#include "cmdline.hpp"
#include "./miniz/miniz.h"
#include "wrappers.hpp"
#include "sequential_algorithms.hpp"
#include <omp.h>

struct CompressionJob{
  std::size_t id;          
  std::uintmax_t start_offset;
  std::uintmax_t size;
};

bool parallel_openMP_compress_stream(ReadOnlyFileMap& in,
									  WriteOnlyFileMap& out, int level) {
  
  /* CHUNK rappresenta la dimensione in byte di ogni lavoro che andranno
   * a svolgere i singoli thread.
   * Un file parallelo ha dimensione superiore al threshold, quindi,
   * definisco la dimensione del CHUNK come un quarto della dimensione del
   * threshold.
   * Mi devo assicurare, però, che non sia minore di 32kB(finestra di contesto
   * dell'algoritmo DEFLATE), che non sia maggiore di 4MB per evitare di avere
   * poche task con file grandi e, per la massima efficienza, che sia un
   * multiplo di 32kB. 
   */
  constexpr std::size_t KB32 = 32 * 1024;
  constexpr std::size_t MB4 = 4 * 1024 * 1024;
  std::size_t CHUNK = (cfg.size_threshold/4);
  if(CHUNK<KB32){
	CHUNK = KB32;
  }else if(CHUNK>MB4){
	CHUNK = MB4;
  }else{
	CHUNK = (CHUNK/KB32) * KB32;
  }

  //Usa divisione intera con ceiling
  std::size_t ntask((in.size() + CHUNK - 1) / CHUNK);
  
  std::size_t max_dest_len = compressBound(CHUNK);
  
  //Vettore che conterrà temporaneamente il file compresso.
  std::vector<unsigned char> arena;
  try{
	arena.resize(ntask*max_dest_len);
  }catch(const std::bad_alloc&){
	std::cerr<<"Memoria non allocata per il buffer Arena";
	return false;
  }
  
  bool ok = true;
  
  //Vettore che conterrà le size di ogni blocco compresso e uno per gli offset
  std::vector<std::size_t> compressed_sizes(ntask);
  std::vector<std::size_t> file_offsets(ntask);
  
#pragma omp parallel for schedule(static) shared(ok)
  //Esegue ogni task definendo i byte che andrà a comprimere
  for(std::size_t i = 0; i < ntask; i++){
	if(!ok)
	  continue;
	std::size_t start = i * CHUNK;
	//Calcolo safe dell'end
	std::size_t end = std::min(in.size(), start + CHUNK);
	std::size_t input_size = end - start;
	
	z_stream s{};
	//Inizializzazione
	if(deflateInit(&s, level) != Z_OK){
#pragma omp atomic write
	  ok = false;
	}else{
	  unsigned char* my_ptr = arena.data() + (i*max_dest_len);
	  //Inserisce i valori necessari in s
	  s.next_in = const_cast<unsigned char*>(in.data() + start);
	  s.avail_in = input_size;
	  s.next_out = my_ptr;
	  s.avail_out = max_dest_len; 

	  //Compressione
	  int zret = deflate(&s, Z_FINISH);
      
	  if(zret == Z_STREAM_ERROR){
#pragma omp atomic write
		ok = false;
	  }else{
		//Ridimensiona alla dimensione reale dei dati compressi
		compressed_sizes[i] = s.total_out;
	  }
	  deflateEnd(&s);
	}
  }
  if(!ok) return false;
  
  // --- CALCOLO OFFSET ---
  
  file_offsets[0] = 0;
  for(std::size_t i = 1; i<ntask; ++i){
	file_offsets[i] = file_offsets[i-1] + compressed_sizes[i-1];
  }
  
  std::size_t total_data_size = file_offsets.back() + compressed_sizes.back();
  std::size_t footer_size = (ntask * sizeof(std::size_t)) +
	sizeof(std::size_t) + sizeof(uint64_t);
  std::size_t final_file_size = total_data_size + footer_size;
  //Controllo spazio disponibile in output
  if(final_file_size > out.capacity()) {
	std::cerr << "Errore: Output map troppo piccola!\n";
	return false;
  }
  
  //--- SCRITTURA DATI COMPRESSI SU FILE DI OUTPUT ---
  
  unsigned char* out_base_ptr = out.data();
  
#pragma omp parallel for schedule(static)
  for(std::size_t i = 0; i<ntask; ++i){
	unsigned char* src = arena.data() + (i*max_dest_len);
	unsigned char* dst = out_base_ptr + file_offsets[i];
	std::memcpy(dst, src, compressed_sizes[i]);
  }
  
  // --- AGGIUNTA FOOTER ---
  
  // Calcolo offset footer e scrittura blocchi dimensioni
  unsigned char* footer_ptr = out_base_ptr + total_data_size;
  std::memcpy(footer_ptr, compressed_sizes.data(), ntask*sizeof(std::size_t));
  footer_ptr += ntask*sizeof(std::size_t);
  //Scrittura numero di blocchi
  std::memcpy(footer_ptr, &ntask, sizeof(std::size_t));
  footer_ptr += sizeof(std::size_t);
  //Scrittura valore identificativo della compressione parallela
  const uint64_t identifier = 0xB045A3;
  std::memcpy(footer_ptr, &identifier, sizeof(uint64_t));
  
  out.commit(final_file_size);
  return true;
}

bool parallel_openMP_decompress_stream(ReadOnlyFileMap& in, std::ostream& out) {
  
  // LETTURA FOOTER
  if (in.size() < sizeof(std::size_t) + sizeof(uint64_t)) return false;
  const unsigned char* data_end = in.data() + in.size();
  
  uint64_t identifier;
  std::memcpy(&identifier, data_end - sizeof(uint64_t), sizeof(uint64_t));
  if (identifier != 0xB045A3) return false;
  
  std::size_t num_blocks;
  std::memcpy(&num_blocks, data_end - sizeof(std::size_t) - sizeof(uint64_t), sizeof(std::size_t));
  //Controllo safety
  if (num_blocks == 0 || num_blocks > 10000000) { 
	std::cerr << "Errore: Numero blocchi corrotto.\n";
	return false;
  }
  //Calcolo footer size
  std::size_t footer_size = (num_blocks * sizeof(std::size_t)) + sizeof(std::size_t) + sizeof(uint64_t);
  if (footer_size > in.size()) return false;
  
  const unsigned char* sizes_ptr = data_end - footer_size;
  std::vector<CompressionJob> lavori(num_blocks);//Vettore dei lavori
  
  std::size_t current_offset = 0;
  std::size_t data_section_size = in.size() - footer_size;//Fine parte dati del file, inizio parte footer

  //Inizializzazione lavori
  for(std::size_t i = 0; i < num_blocks; ++i) {
	std::size_t compressed_size;
	std::memcpy(&compressed_size, sizes_ptr + (i * sizeof(std::size_t)), sizeof(std::size_t));
    
	if (compressed_size == 0 || current_offset + compressed_size > data_section_size) return false;
    
	lavori[i] = {i, current_offset, compressed_size};
	current_offset += compressed_size;
  }

  //PIPELINE CON BATCH
  // 64 blocchi * 4MB (max) = 256MB di picco RAM garantito.
  const std::size_t BATCH_SIZE = 64;
  
  std::vector<std::vector<unsigned char>> buf(num_blocks);
  auto* buf_ptr = buf.data(); //Puntatore grezzo necessario per OpenMP depend
  
  bool ok = true;
  int order_token = 0; //Token per forzare la scrittura sequenziale nelle task writer(0->1->2...)
  
#pragma omp parallel
#pragma omp single //Un solo thread prepara le task
  {
	// Ciclo esterno: processa a finestre (Batch)
	for(std::size_t base_i = 0; base_i < num_blocks; base_i += BATCH_SIZE) {
	  
	  // Se c'è stato un errore nel batch precedente, fermiamoci
	  if(!ok) break;
	  
	  std::size_t end_i = std::min(base_i + BATCH_SIZE, num_blocks);
      
	  // Lancia i task SOLO per questo batch
	  for(std::size_t i = base_i; i < end_i; ++i) {
		
		// --- PRODUCER ---
		// depend(out: buf[i]): Dice "Sto producendo il blocco i"
#pragma omp task firstprivate(i) shared(ok, in, buf, lavori) depend(out: buf_ptr[i])
		{
		  bool global_ok;
#pragma omp atomic read
		  global_ok = ok;
		  
		  if(global_ok) {
			const auto& job = lavori[i];
			z_stream s{};
			//Inizializzazione s
			if (inflateInit(&s) != Z_OK) {
#pragma omp atomic write
			  ok = false;
			} else {
			  s.next_in = const_cast<unsigned char*>(in.data() + job.start_offset);
			  s.avail_in = job.size;
			  
			  //Allocazione dinamica
			  std::vector<unsigned char>& local_buf = buf[i];
			  // Stima iniziale: 4 volte la size compressa o min 2MB
			  size_t cap = std::max((size_t)(job.size * 4), (size_t)(2 * 1024 * 1024));
			  try {
				local_buf.resize(cap);
			  }catch(...){ 
#pragma omp atomic write 
				ok = false; 
			  }
			  
			  if(ok) {
				s.next_out = local_buf.data();
				s.avail_out = local_buf.size();
				
				int zret = Z_OK;
				while(ok) {
				  zret = inflate(&s, Z_NO_FLUSH);
                  
				  if (zret == Z_STREAM_END) {
					local_buf.resize(s.total_out); //Taglia alla size giusta
					break;
					}
				  if (zret != Z_OK && zret != Z_BUF_ERROR) {
#pragma omp atomic write
					ok = false; break;
				  }
				  //Espande il buffer se pieno
				  if (s.avail_out == 0) {
					size_t old_sz = local_buf.size();
					size_t new_sz = old_sz * 2;
					//Safety check: max 512MB per blocco
					if (new_sz > 512*1024*1024) { 
#pragma omp atomic write 
					  ok = false; break; 
					}
					try {
					  local_buf.resize(new_sz);
					  s.next_out = local_buf.data() + old_sz;
					  s.avail_out = new_sz - old_sz;
					}catch(...){
#pragma omp atomic write 
					  ok = false; 
					}
				  }
				}
				inflateEnd(&s);
			  }
			}
		  }
		}
		
		// --- CONSUMER ---
		// depend(in: buf[i]): Aspetta che la decompressione finisca
		// depend(inout: order_token): Aspetta che il Writer del blocco i-1 finisca
#pragma omp task firstprivate(i) shared(ok, out, buf)	\
  depend(in: buf_ptr[i]) depend(inout: order_token)
		{
		  bool local_ok;
#pragma omp atomic read
		  local_ok = ok;

		  //Scrive sul file di output il blocco
		  if(local_ok) {
			out.write(reinterpret_cast<const char*>(buf[i].data()), buf[i].size());
			if(!out) {
#pragma omp atomic write
			  ok = false;
			} else {
			  // Libera la memoria del blocco
			  std::vector<unsigned char>().swap(buf[i]);
			}
		  }
		}
	  }
      
	  // --- BARRIERA BATCH ---
	  // Aspetta che TUTTI i task di questo batch (decompress + write) finiscano.
#pragma omp taskwait
	  
	} // Fine Loop Batch
  } // Fine Single/Parallel
  
  return ok;
}


// --- doWork Specializzata per OpenMP ---
bool doWork(const char* path_cstr) {
  
  const fs::path in(path_cstr);
  if (!fs::exists(in)) {
    std::fprintf(stderr, "File disappear: %s\n", path_cstr);
    return false;
  }
  if (!fs::is_regular_file(in)) {
    if (cfg.verbose) std::fprintf(stderr, "Skip, not a regular file: %s\n", path_cstr);
    return true;
  }
  
  const fs::path out_final = make_output_path(in);
  const fs::path out_tmp   = fs::path(out_final.string() + ".tmp");
  
  if (cfg.verbose) {
    if (cfg.mode == COMP)
      std::fprintf(stderr, "[C] %s -> %s\n", in.string().c_str(), out_final.string().c_str());
    else
      std::fprintf(stderr, "[D] %s -> %s\n", in.string().c_str(), out_final.string().c_str());
  }
  
  std::error_code ec_eq;
  if (fs::equivalent(in, out_final, ec_eq) && !ec_eq) {
    std::fprintf(stderr, "Error: Output coincide with input: %s\n", out_final.string().c_str());
    return false;
  }

  ReadOnlyFileMap mapped_file(in.string()); // Effettua open e mmap
  
  if(mapped_file.size() == 0) {
    std::cout << "File è vuoto.\n";
    return true;
  }
  
  // Crea un output file stream in modalità binaria e, se il file out_tmp è già presente,
  // lo svuota.
  std::ofstream ofs(out_tmp, std::ios::binary | std::ios::trunc);
  if (!ofs) {
    std::fprintf(stderr, "Error: cannot open in output: %s\n", out_tmp.string().c_str());
    return false;
  }
  
  std::size_t size_limit = cfg.size_threshold; //prendo il threshold definito 
  bool ok = true;
  
  if(mapped_file.size() > size_limit) {
	//RAMO PARALLELO OPENMP 
	if (cfg.mode == COMP) {
	  ofs.close(); 
	  //Calcolo max_size...
	  std::size_t max_size = mapped_file.size() + (mapped_file.size() / 10) + (4 * 1024 * 1024);
	  WriteOnlyFileMap out_map(out_tmp.string(), max_size);
      
	  //Chiamata alla versione OMP
	  ok = parallel_openMP_compress_stream(mapped_file, out_map, cfg.level);
	} else {
	  //Chiamata alla versione OMP
	  ok = parallel_openMP_decompress_stream(mapped_file, ofs);
	}
  } else {
	//Ramo sequenziale (File piccoli) 
	ok = (cfg.mode == COMP) 
	  ? sequential_compress_stream(mapped_file, ofs, cfg.level)
	  : sequential_decompress_stream(mapped_file, ofs);
  }

  //Chiude ofs se è ancora aperto(nel caso non siamo passati dal ramo COMP parallelo)
  if (ofs.is_open()) {
	ofs.close();
  }
  
  
  if (!ok) {
    std::error_code ecc;
    fs::remove(out_tmp, ecc);  //remove temporary file
    std::fprintf(stderr, "Error: %s on %s\n",
                 (cfg.mode == COMP ? "compression" : "decompression"),
                 in.string().c_str());
    return false;
  }
  
  
  // atomic rename, if it is not supported by the FS, fallback to copy+remove
  std::error_code ec;
  fs::rename(out_tmp, out_final, ec);
  if (ec) { // rename failed
    std::error_code ecw, ecr;
    fs::copy_file(out_tmp, out_final, fs::copy_options::overwrite_existing, ecw);
    fs::remove(out_tmp, ecr); // remove temporary file
    if (ecw) {
      std::fprintf(stderr, "Error: impossible to write output: %s\n", out_final.string().c_str());
      return false;
    }
  }
  // do we have to remove the input file?
  if (cfg.remove_input) {
    std::error_code ecr;
    fs::remove(in, ecr);
    if (ecr && cfg.verbose)
      std::fprintf(stderr, "Warning: file %s has not been removed (%s)\n",
                   in.string().c_str(), ecr.message().c_str());
  }
  
  return true;
}


//WalkDir fatta con openMP
bool parallel_walkDir(const char* dir_cstr) {
  const fs::path root(dir_cstr);
  if (!fs::exists(root) || !fs::is_directory(root)) return false;
  
  // Recupero threshold
  std::size_t threshold = cfg.size_threshold; // Default 4MB
  
  //Code Separate
  std::vector<fs::path> small_files;
  std::vector<fs::path> large_files;
  
  auto classify = [&](const fs::path& p){
	if(!should_process(p)) return;
    
	std::error_code ec;
	uintmax_t fsize = fs::file_size(p, ec);

	//File piccoli->coda dei file piccoli
	//File grandi->coda dei file grandi
	if (!ec && fsize >= threshold) {
	  large_files.push_back(p);
	} else {
	  small_files.push_back(p);
	}
  };
  
  //Raccolta ricorsiva
  if (cfg.recursive) {
	fs::directory_options opts = fs::directory_options::skip_permission_denied;
	for (auto it = fs::recursive_directory_iterator(root, opts),
		   end = fs::recursive_directory_iterator(); it != end; ++it) {
	  std::error_code ec;
	  if (it->is_symlink(ec) || it->is_directory(ec)) continue;
	  if (it->is_regular_file(ec)) classify(it->path());
	}
  } else {
	for (auto& de : fs::directory_iterator(root, fs::directory_options::skip_permission_denied)) {
	  std::error_code ec;
	  if (de.is_regular_file(ec)) classify(de.path());
	}
  }
  
  bool global_ok = true;
  
  // Eseguiamo i file piccoli in parallelo
  //doWork dentro sarà sequenziale (perché size < threshold).
  if (!small_files.empty()) {
	if(cfg.verbose) std::fprintf(stderr, "Processing %zu small files (Parallel Files)...\n", small_files.size());
	
#pragma omp parallel for schedule(dynamic) reduction(&&:global_ok)
	for(size_t i = 0; i < small_files.size(); ++i) {
	  bool res = doWork(small_files[i].string().c_str());
	  if(!res) {
		std::fprintf(stderr, "Error on file: %s\n", small_files[i].c_str());
		global_ok = false;
	  }
	}
  }
  
  //Processiamo i file grandi, un file alla volta.
  // doWork dentro sfrutterà tutti i core(perché size >= threshold).
  if (!large_files.empty()) {
	if(cfg.verbose) std::fprintf(stderr, "Processing %zu large files (Sequential Files, Parallel Content)...\n", large_files.size());
	
	for(const auto& file_path : large_files) {
	  bool res = doWork(file_path.string().c_str());
	  if(!res) {
		std::fprintf(stderr, "Error on large file: %s\n", file_path.c_str());
		global_ok = false;
	  }
	}
  }
  
  return global_ok;
}

