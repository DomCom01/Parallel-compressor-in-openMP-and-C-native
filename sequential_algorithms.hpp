/**Questo file contiene le versioni sequenziali di compress e decompress
 * Vengono usate, oltre che dalla versione sequenziale del codice, anche
 * dalle versioni parallele quando il file da elaborare è di piccole dimensioni.
 */

#pragma once
#include "wrappers.hpp"
#include "config.hpp"
#include "./miniz/miniz.h"
#include <filesystem>
#include <fstream>
#include <iostream>
#include <vector>

namespace fs = std::filesystem;

// Compressione sequenziale
bool sequential_compress_stream(ReadOnlyFileMap& in, std::ostream& out, int level) {
  // Dimensione del blocco da processare per volta.
  // Con mmap non c'è bisogno di buffer di input, ma zlib lavora meglio 
  // se non gli diamo GB di dati tutti in un colpo.
  constexpr std::size_t CHUNK = 256 * 1024;
  
  z_stream s{};
  //Inizializza "s" preparandola la deflate
  if (deflateInit(&s, level) != Z_OK) return false;
  
  // Serve solo il buffer di output.
  std::vector<unsigned char> outbuf(CHUNK);
  
  bool ok = true;
  int zret = Z_OK;
  std::size_t offset = 0;//offset di partenza
  const std::size_t file_size = in.size();//dimensione file
  const unsigned char* raw_data = in.data();//puntatore all'inizio del file
  
  // Usiamo un do-while per gestire anche il caso di file vuoto (size=0),
  // dove deflate deve essere chiamato almeno una volta con Z_FINISH.
  do {
	// Calcoliamo quanti byte processare a questo giro
	std::size_t remain = file_size - offset;
	std::size_t current_chunk_size = (remain > CHUNK) ? CHUNK : remain;
	
	// Z_NO_FLUSH finché ci sono dati, Z_FINISH all'ultimo blocco
	const int flush = (offset + current_chunk_size == file_size) ? Z_FINISH : Z_NO_FLUSH;
	
	// ZERO-COPY: Puntiamo direttamente alla memoria mappata
	s.next_in = const_cast<unsigned char*>(raw_data + offset);
	s.avail_in = static_cast<uInt>(current_chunk_size);
	
	// Ciclo interno di compressione
	do {
	  //Definiamo dove andrà a scrivere il buffer compresso
	  s.avail_out = static_cast<uInt>(outbuf.size());
	  s.next_out  = outbuf.data();
	  
	  zret = deflate(&s, flush);//Chiamata alla deflate
      
	  if (zret == Z_STREAM_ERROR) {
		ok = false;
		break;
	  }
	  
	  const std::size_t have = outbuf.size() - s.avail_out;
	  if (have > 0) {
		//Scrive ciò che ha compresso fino ad ora nel file di output
		out.write(reinterpret_cast<const char*>(outbuf.data()), static_cast<std::streamsize>(have));
		if (!out) {
		  ok = false;
		  break;
		}
	  }
	  
	} while (s.avail_out == 0); // Continua finché il buffer di output si riempie
	
	if (!ok) break;
	if (flush == Z_FINISH && zret == Z_STREAM_END) break;
	
	// Avanziamo l'offset
	offset += current_chunk_size;
	
  } while (offset < file_size);
  
  deflateEnd(&s);
  return ok;
}

//Decompress sequenziale
bool sequential_decompress_stream(ReadOnlyFileMap& in, std::ostream& out) {
  constexpr std::size_t CHUNK = 256 * 1024;
  std::vector<unsigned char> outbuf(CHUNK);
  
  // Initializes a zlib inflate state (zlib wrapper, not raw/gzip).
  z_stream s{};
  if (inflateInit(&s) != Z_OK)
	return false;
  
  /** --- Fase di controllo presenza del footer ---
   * Se presente va ignorato altrimenti
   * si va a decomprimere una porzione di dati
   * che produrrà spazzatura
   */

  //Prende la dimensione del file compresso
  std::size_t file_size = in.size();
  if (file_size > sizeof(uint64_t)) {
	uint64_t check_value;
	//Copia gli ultimi byte in numero pari alla dimensione di un unsigned integer 64 per
	//controllare se è il valore speciale usato per identificare i file compressi parallelamente
	std::memcpy(&check_value, in.data() + file_size - sizeof(uint64_t), sizeof(uint64_t));
	
	if(check_value == 0xB045A3) {
	  // Footer presente, legge num_blocks in modo sicuro
	  std::size_t num_blocks;
	  const unsigned char* data_end = in.data() + in.size();
	  std::memcpy(&num_blocks, 
				  data_end - sizeof(uint64_t) - sizeof(std::size_t), 
				  sizeof(std::size_t));
	  
	  std::size_t footer_size = (num_blocks * sizeof(std::size_t)) + 
		sizeof(std::size_t) + sizeof(uint64_t);
	  //Cambia la size del file, togliendo il footer
	  if (footer_size <= in.size()) {
		file_size = in.size() - footer_size;
	  }
	}
  }
  
  /* --- Fase di decompressione sequenziale --- */
  
  s.next_in = in.data();
  s.avail_in = file_size;
  int zret = Z_OK;
  bool ok = true;
  
  do {
	
	// prepare the output
	s.avail_out = static_cast<uInt>(outbuf.size());
	s.next_out  = outbuf.data();
	
	// Always calls inflate() once per iteration so it can eventually return Z_STREAM_END
	// even after EOF on the input stream.
	zret = inflate(&s, Z_NO_FLUSH);
	if (zret < 0 && zret != Z_BUF_ERROR) {
	  ok = false;
	  break;
	}
	
	const std::size_t have = outbuf.size() - s.avail_out;
	if (have>0){
	  out.write(reinterpret_cast<const char*>(outbuf.data()),
				static_cast<std::streamsize>(have));
	  if(!out){
		ok = false;
		break;
	  }
	}
	if (zret == Z_STREAM_END) {
	  if(s.avail_in>0){
		inflateEnd(&s);
		if(inflateInit(&s) != Z_OK){
		  ok = false;
		  break;
		}
		zret = Z_OK;
	  }
	}
  }while(s.avail_in > 0 || zret == Z_OK);
  
  inflateEnd(&s);
  return ok; 
}
