/**Questo file contiene le versioni parallele(no openMP) di compress e decompress.
 */


#include "cmdline.hpp"
#include "wrappers.hpp"
#include "threadPool.hpp"
#include "./miniz/miniz.h"
#include "sequential_algorithms.hpp"
#include <iostream>
#include <fstream>
#include <atomic>
#include <future>

/**Struttura allineata alla cache line per evitare false sharing.
 * I thread accedono contemporaneamente in scrittura a elementi distinti
 * di un vettore di PaddedFlag, quindi lo allineiamo alla cache line. 
 */
struct alignas(64) PaddedFlag {
  std::atomic<bool> value{false};
};

//Struttura necessaria per memorizzare i dati dei blocchi da elaborare in parallelo
struct CompressionJob{
  std::size_t id; //l'id corrisponde al numero del blocco da elaborare
  std::uintmax_t start_offset;
  std::uintmax_t size;
};

//Funzione rivelatasi necessaria durante i test per limitare il numero di thread
inline int get_thread_count() {
    const char* env_p = std::getenv("OMP_NUM_THREADS");
    if (env_p) {
        try {
            int n = std::stoi(env_p);
            return (n > 0) ? n : 1;
        } catch (...) {}
    }
    //Se non c'è la variabile, usa tutti i core
    return std::thread::hardware_concurrency();
}


inline bool parallel_compress_stream(ReadOnlyFileMap& in, WriteOnlyFileMap& out, int level) {
  /**CHUNK rappresenta la dimensione in byte di ogni lavoro che andranno
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
  
  std::size_t ntask = (in.size() + CHUNK - 1) / CHUNK;
  
  //Definiamo size, offset e id di tutti i chunk da elaborare
  std::vector<CompressionJob> lavori;
  lavori.reserve(ntask);
  for(std::size_t i = 0; i < ntask; ++i) {
    std::size_t start = i * CHUNK;
    std::size_t size = std::min(CHUNK, in.size() - start);
    lavori.push_back({i, start, size});
  }

  //conterrà i vari chunk compressi, prima che vengano scritti su file
  std::vector<std::vector<unsigned char>> compressed_files(ntask);
  std::atomic<bool> ok{true};//Variabile atomica per catturare errori
  auto ready = std::make_unique<PaddedFlag[]>(ntask);//Array di bool su cui aspetterà il writer
  std::size_t max_dest_len = compressBound(CHUNK);//Massima dimensione compressa del file

  //Lambda function dei thread producer che andranno a comprimere il file
  auto producer = [&](std::size_t i){
    if(!ok.load(std::memory_order_relaxed)) return;
	//Prende il chunk su cui lavorare e crea un buffer su cui salvare l'output
    const auto& job = lavori[i];
    std::vector<unsigned char> buffer;
	
    try {
      buffer.resize(max_dest_len);
    } catch(...){
      ok.store(false, std::memory_order_relaxed); 
      return;
    }

	//Inizializza s per la deflate indicando cosa comprimere e dove salvarlo
    z_stream s{};
    if (deflateInit(&s, level) != Z_OK) {
      ok.store(false, std::memory_order_relaxed);
      return;
    }
    s.avail_in = job.size;
    s.next_in = const_cast<unsigned char*>(in.data()+job.start_offset);
    
    s.avail_out = static_cast<uInt>(buffer.size());
    s.next_out = buffer.data();
    //Comprime
    int zret = deflate(&s, Z_FINISH);
    deflateEnd(&s);
    
    if(zret == Z_STREAM_ERROR) {
      ok.store(false, std::memory_order_relaxed);
    } else {
	  //Salva il buffer compresso e avvisa il writer
      buffer.resize(s.total_out);
      compressed_files[i] = std::move(buffer);
      ready[i].value.store(true, std::memory_order_release);
      ready[i].value.notify_one();
    }
  };

  //Lambda function writer che attende la compressione ordinata dei chunk e li scrive su file
  auto writer = [&]() -> bool {
    unsigned char* base_ptr = out.data();
    std::size_t offset{0};//Parte dall'inizio del file di output
    //Vettore per salvare le size dei blocchi compressi
    std::vector<std::size_t> block_sizes;
    block_sizes.reserve(ntask);
	
    for(size_t i = 0; i < ntask; ++i) {
      if (!ok.load(std::memory_order_relaxed)) return false;
      //Attende l'i-esimo blocco
      ready[i].value.wait(false, std::memory_order_acquire);
      
      if (!ok.load(std::memory_order_relaxed)) return false;
      
      auto& block = compressed_files[i];
      std::size_t b_size = block.size();
      
      block_sizes.push_back(b_size);
      
      if (offset + b_size > out.capacity()) {
        std::cerr << "Output buffer too small for data block " << i << "\n";
        ok.store(false, std::memory_order_relaxed); 
        return false;
      }

	  //Copia il blocco sul file di output
      std::memcpy(base_ptr + offset, block.data(), b_size);
      offset += b_size;

	  //Pulisce
      block.clear();
      block.shrink_to_fit();
    }
    
    /* --- SCRITTURA FOOTER --- */

	//Calcola la dimensione del footer
	std::size_t sizes_array_bytes = block_sizes.size() * sizeof(std::size_t);
    std::size_t footer_total_len = sizes_array_bytes + sizeof(std::size_t) + sizeof(uint64_t);
    
    if (offset + footer_total_len > out.capacity()) {
      std::cerr << "Output buffer too small for footer\n";
      return false;
    }

	//Copia in ordine: dimensioni blocchi, numero blocchi, valore 
    unsigned char* footer_ptr = base_ptr + offset;
    std::memcpy(footer_ptr, block_sizes.data(), sizes_array_bytes);
    footer_ptr += sizes_array_bytes;
    
    std::memcpy(footer_ptr, &ntask, sizeof(std::size_t));
    footer_ptr += sizeof(std::size_t);
    
    const uint64_t identifier = 0xB045A3; 
    std::memcpy(footer_ptr, &identifier, sizeof(uint64_t));
    
    offset += footer_total_len;
    out.commit(offset);
    return true;
  };
  
  static threadPool pool(get_thread_count());
  
  auto writer_result = pool.submit(writer);
  
  for(std::size_t i = 0; i<ntask; ++i){
    pool.submit(producer, i);
  }
  
  bool result = pool.wait_future(writer_result);
  return result && ok.load();
}

bool parallel_decompress_stream(ReadOnlyFileMap& in, std::ostream& out) {
  //Controllo dimensione minima
  if (in.size() < sizeof(std::size_t) + sizeof(uint64_t)) {
	return false;
  }
  
  const unsigned char* data_end = in.data() + in.size();
  
  //Lettura identificatore
  uint64_t identifier;
  std::memcpy(&identifier, data_end - sizeof(uint64_t), sizeof(uint64_t));
  
  if (identifier != 0xB045A3) {
	return false;
  }
  
  //Lettura Numero Blocchi
  std::size_t num_blocks;
  std::memcpy(&num_blocks, 
			  data_end - sizeof(std::size_t) - sizeof(uint64_t), 
			  sizeof(std::size_t));
  
  //Sanity Check
  if (num_blocks == 0 || num_blocks > 100000) {
	std::cerr << "Errore: Numero blocchi invalido: " << num_blocks << "\n";
	return false;
  }
  
  std::size_t footer_size = (num_blocks * sizeof(std::size_t)) + 
	sizeof(std::size_t) + sizeof(uint64_t);
  
  if (footer_size > in.size()) {
	std::cerr << "Errore: Footer size maggiore del file\n";
	return false;
  }
  
  //Lettura Array Size
  const unsigned char* sizes_ptr = data_end - footer_size;
  
  //Creazione e inizializzazione vettore dei lavori
  std::vector<CompressionJob> lavori;
  lavori.reserve(num_blocks);
  std::size_t current_offset = 0;
  std::size_t data_section_size = in.size() - footer_size;
  for(std::size_t i{0}; i < num_blocks; i++) {
	std::size_t compressed_size;
	std::memcpy(&compressed_size, 
				sizes_ptr + (i * sizeof(std::size_t)), 
				sizeof(std::size_t));
	
	// Controllo validità
	if (compressed_size == 0 || compressed_size > data_section_size) {
	  std::cerr << "Errore: Blocco " << i << " ha size invalida\n";
	  return false;
	}
	
	if (current_offset + compressed_size > data_section_size) {
	  std::cerr << "Errore: Blocco " << i << " fuori dai limiti\n";
	  return false;
	}
	
	lavori.push_back(CompressionJob{i, current_offset, compressed_size});
	current_offset += compressed_size;
  }
  
  //Setup Decompressione Parallela con ThreadPool
  std::vector<std::vector<unsigned char>> decompressed_blocks(num_blocks);
  std::atomic<bool> ok{true};
  auto ready = std::make_unique<PaddedFlag[]>(num_blocks);
  
  /**
   * Producer: decomprime un blocco
   */
  auto producer = [&](std::size_t i) {
	if(!ok.load(std::memory_order_relaxed)) return;

	//Prende il blocco da decomprimere
	const auto& job = lavori[i];
	std::vector<unsigned char> outbuf;
    //Inizializza s
	z_stream s{};
	if (inflateInit(&s) != Z_OK) { 
	  ok.store(false, std::memory_order_relaxed);
	  return;
	}
	// Buffer temporaneo per inflate
	unsigned char temp_buf[128*1024];
	s.next_in = const_cast<unsigned char*>(in.data() + job.start_offset);
	s.avail_in = job.size;
    
	int zret;
	do {
	  s.next_out = temp_buf;
	  s.avail_out = sizeof(temp_buf);
	  zret = inflate(&s, Z_NO_FLUSH);
      
	  if(zret != Z_OK && zret != Z_STREAM_END && zret != Z_BUF_ERROR) {
		ok.store(false, std::memory_order_relaxed);
		break;
	  }
      
	  std::size_t generated = sizeof(temp_buf) - s.avail_out;
	  if (generated > 0) {
		outbuf.insert(outbuf.end(), temp_buf, temp_buf + generated);
	  }
	} while (zret != Z_STREAM_END && ok.load(std::memory_order_relaxed));
    
	inflateEnd(&s);
    
	if(zret == Z_STREAM_END) {
	  //Avvisa il writer che il blocco è pronto
	  decompressed_blocks[i] = std::move(outbuf);
	  ready[i].value.store(true, std::memory_order_release);
	  ready[i].value.notify_one();
	} else {
	  ok.store(false, std::memory_order_relaxed);
	}
  };
  
  /**
   * Writer: scrive i blocchi decompressi in ordine
   */
  auto writer = [&]() -> bool {
	for(std::size_t i = 0; i < num_blocks; ++i) {
	  
	  //Check errori globali
	  if (!ok.load(std::memory_order_relaxed)) return false;
      
	  //Attesa ordinata dei producer
	  ready[i].value.wait(false, std::memory_order_acquire);
      
	  //Check post-wait
	  if (!ok.load(std::memory_order_relaxed)) return false;
      
	  //Scrittura blocco decompresso
	  auto& block = decompressed_blocks[i];
	  out.write(reinterpret_cast<const char*>(block.data()), block.size());
      
	  if(!out) {
		std::cerr << "Errore: Scrittura output fallita per blocco " << i << "\n";
		ok.store(false, std::memory_order_relaxed);
		return false;
	  }
      
	  //Pulizia RAM
	  block.clear();
	  block.shrink_to_fit();
	}
	
	return true;
  };
  
  static threadPool pool(get_thread_count());
    
  // Lancia il writer
  auto writer_result = pool.submit(writer);
  
  ok.store(true, std::memory_order_relaxed);
  
  // Lancia i producer
  for(std::size_t i = 0; i < num_blocks; ++i) {
	pool.submit(producer, i);
  }
  
  // Aspetta il writer
  bool result = pool.wait_future(writer_result);
  return result && ok.load();
}

// --- doWork Specializzata per parallel C++ ---
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
  
  //Crea un output file stream in modalità binaria e, se il file out_tmp è già presente,
  //lo svuota.
  std::ofstream ofs(out_tmp, std::ios::binary | std::ios::trunc);
  if (!ofs) {
    std::fprintf(stderr, "Error: cannot open in output: %s\n", out_tmp.string().c_str());
    return false;
  }
  
  std::size_t size_limit = cfg.size_threshold;
  bool ok = true;
  
  if(mapped_file.size() > size_limit) {
	//RAMO PARALLELO C++ 
	if (cfg.mode == COMP) {
	  ofs.close();
	  std::size_t max_size = mapped_file.size() + (mapped_file.size() / 10) + (4 * 1024 * 1024);
	  WriteOnlyFileMap out_map(out_tmp.string(), max_size);
	  
	  //Chiamata alla versione parallela della compressione
	  ok = parallel_compress_stream(mapped_file, out_map, cfg.level);
	} else {
	  //Chiamata alla versione parallela della decompress
	  ok = parallel_decompress_stream(mapped_file, ofs);
	}
  } else {
	//Ramo sequenziale
	ok = (cfg.mode == COMP) 
	  ? sequential_compress_stream(mapped_file, ofs, cfg.level)
	  : sequential_decompress_stream(mapped_file, ofs);
  }

  //Chiudiamo ofs se è ancora aperto (nel caso non siamo passati dal ramo COMP parallelo)
  if (ofs.is_open()) {
	ofs.close();
  }

  if (!ok) {
    std::error_code ecc;
    fs::remove(out_tmp, ecc);  // remove temporary file
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

bool parallel_walkDir(const char* dir_cstr) {
  const fs::path root(dir_cstr);
  
  if (!fs::exists(root) || !fs::is_directory(root)) {
    std::fprintf(stderr, "Non è una directory: %s\n", dir_cstr);
    return false;
  }
  
  //Recupero del Threshold
  std::size_t threshold = cfg.size_threshold;
  
  std::vector<fs::path> small_files;
  std::vector<fs::path> large_files;
  
  //Lambda di raccolta e classificazione
  auto collect = [&](const fs::path& p){
    if(should_process(p)){
	  //Recupera dimensione file senza lanciare eccezioni
	  std::error_code ec;
	  uintmax_t fsize = fs::file_size(p, ec);
      
	  if (!ec && fsize >= threshold) {
		large_files.push_back(p);
	  } else {
		small_files.push_back(p);
	  }
    } else {
      if (cfg.verbose)
        std::fprintf(stderr, "Skip: %s\n", p.string().c_str());
    }
  };
  
  //Analisi directory (sequenziale)
  if (cfg.recursive) {
    fs::directory_options opts = fs::directory_options::skip_permission_denied;
    for (auto it = fs::recursive_directory_iterator(root, opts),
           end = fs::recursive_directory_iterator(); it != end; ++it) {
      std::error_code ec;
      if (it->is_symlink(ec) || it->is_directory(ec)) continue;
      if (it->is_regular_file(ec)) collect(it->path());
    }
  } else {
    for (auto& de : fs::directory_iterator(root, fs::directory_options::skip_permission_denied)) {
      std::error_code ec;
      if (de.is_regular_file(ec)) collect(de.path());
    }
  }
  
  if(small_files.empty() && large_files.empty()) {
    if(cfg.verbose) std::fprintf(stderr, "Nessun file da processare\n");
    return true;
  }
  
  static threadPool pool(get_thread_count());
  std::atomic<bool> all_ok{true};
  
  //Esecuzione file piccoli. Qui doWork è sequenziale internamente, quindi usiamo più worker esterni.
  if (!small_files.empty()) {
	std::atomic<std::size_t> next_small_file{0};
    
	int file_workers = std::max(1, get_thread_count()); 
	
	if(cfg.verbose) {
	  std::fprintf(stderr, "Processing %zu small files with %d workers...\n", 
				   small_files.size(), file_workers);
	}
	
	auto worker = [&]() {
	  while(true) {
		std::size_t file_id = next_small_file.fetch_add(1, std::memory_order_relaxed);
        
		if(file_id >= small_files.size()) break;
		if(!all_ok.load(std::memory_order_relaxed)) break;
        
		const auto& file_path = small_files[file_id];
        
		bool ok = doWork(file_path.string().c_str());
        
		if(!ok) {
		  all_ok.store(false, std::memory_order_relaxed);
		  std::fprintf(stderr, "Errore su file piccolo: %s\n", file_path.string().c_str());
		}
	  }
	};
    
	std::vector<std::future<void>> futures;
	for(int i = 0; i < file_workers; ++i) {
	  auto fut = pool.submit(worker);
	  if(fut.valid()) futures.push_back(std::move(fut));
	}
    
	for(auto& fut : futures) pool.wait_future(fut);
  }

  // Fase file grandi: Qui doWork usa il parallelismo interno (tutti i core su un solo file).
  // Eseguiamo i file uno dopo l'altro per evitare oversubscription.
  if (!large_files.empty() && all_ok.load()) {
	if(cfg.verbose) {
	  std::fprintf(stderr, "Processing %zu large files sequentially (Internal Parallelism)...\n", 
				   large_files.size());
	}
	
	for (const auto& file_path : large_files) {
	  if (!all_ok.load(std::memory_order_relaxed)) break;
	  
	  if(cfg.verbose) {
		std::fprintf(stderr, "[Large] %s\n", file_path.string().c_str());
	  }
	  bool ok = doWork(file_path.string().c_str());
	  
	  if(!ok) {
		all_ok.store(false, std::memory_order_relaxed);
		std::fprintf(stderr, "Errore su file grande: %s\n", file_path.string().c_str());
	  }
	}
  }
  
  return all_ok.load();
}
