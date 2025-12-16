/**Qui sono contenute le funzioni doWork e walkDir sequenziali.
 * La scelta di tenerle separate dagli algoritmi sequenziali di
 * compressione e decompressione si è rivelata necessaria per evitare
 * collisioni con le omologhe versioni parallele.
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

/**
 * Funzione che si occupa della creazione del mapping/stream dei file
 * e poi della chiamata a compressione o decompressione. 
 */
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

  bool ok = true;

  //Chiama compressione o decompressione e ne salva il risultato in una variabile binaria
  ok = (cfg.mode == COMP) 
	? sequential_compress_stream(mapped_file, ofs, cfg.level)
	: sequential_decompress_stream(mapped_file, ofs);

  // Chiudiamo ofs
  if (ofs.is_open()) {
	ofs.close();
  }
  
  
  if(!ok){
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


/**Cerca i file nella cartella specificata, in modo ricorsivo se il flag -r viene impostato ad 1.
 */
// walking the directory recursively (if -r 1) 
bool walkDir(const char* dir_cstr) {
  //Prendiamo il path completo
  const fs::path root(dir_cstr);
  //Se non è una directory terminiamo restituendo un errore
  if (!fs::exists(root) || !fs::is_directory(root)) {
	std::fprintf(stderr, "Non è una directory: %s\n", dir_cstr);
	return false;
  }
  
  bool all_ok = true;
  auto handle = [&](const fs::path& p){
	// Controlla l'estensione del file per vedere se è da processare
	if (!should_process(p)) {// Se non deve essere processato lo salta
	  if (cfg.verbose)
		std::fprintf(stderr, "Skip: %s\n", p.string().c_str());
	  return;
	}
	all_ok &= doWork(p.string().c_str()); // Effettua il calcolo
  };
  
  // Analisi ricorsiva della directory
  if (cfg.recursive) {
	//Controlla se si hanno i permessi per accedere a quel file
	fs::directory_options opts = fs::directory_options::skip_permission_denied;
	
	for (auto it = fs::recursive_directory_iterator(root, opts),
		   end = fs::recursive_directory_iterator();  it != end; ++it) {
	  std::error_code ec;
	  if (it->is_symlink(ec) || it->is_directory(ec)) continue;
	  if (it->is_regular_file(ec)) handle(it->path());
	}
  } else {// Analisi lineare della directory
	for (auto& de : fs::directory_iterator(root, fs::directory_options::skip_permission_denied)) {
	  std::error_code ec;
	  if (de.is_regular_file(ec)) handle(de.path());
	}
  }
  
  return all_ok;
}


