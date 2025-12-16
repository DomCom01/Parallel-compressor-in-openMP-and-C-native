/** In questo file sono contenute funzioni e classi utilizzate da tutte e 3 le implementazioni.
 */

#pragma once
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string>
#include <stdexcept>
#include <cstring>
#include <cerrno>
#include <filesystem>

namespace fs = std::filesystem;

//Funzione per verificare se un path è una directory
bool isDirectory(const char* path) {
  std::error_code ec;
  const fs::path p(path);
  // check symlinks
  const auto st = fs::symlink_status(p, ec);
  if (ec) return false;
  // true only for real directories (a symlink-to-dir returns false)
  if (fs::is_directory(st)) return true;
  return false;
}

/**Controlla se il file è da comprimere/decomprimere
 * Un file non viene compresso in 2 casi:
 *   1) Ha estensione uguale a quella definita per i file compressi
 *   2) Si tratta di un file temporaneo
 * Un file viene decompresso, solo se la sua estensione è uguale
 * a quella definita per i file compressi.
*/
bool should_process(const std::filesystem::path& p) {
  const std::string ext = p.extension().string(); // file extension
  if (cfg.mode == COMP) {
	if (ext == cfg.out_ext || ext == ".tmp")
	  return false;   // already compressed or temporary
	return true;
  } else { // DECOMP
	return (ext == cfg.out_ext);  // decompress only files with proper suffix
  }
}

fs::path make_output_path(const fs::path& in) {
  if (cfg.mode == COMP) {
	return fs::path(in.string() + cfg.out_ext);
  } else {
	const std::string s = in.string();
	const std::string& ext = cfg.out_ext;
	if (s.size() >= ext.size() && s.rfind(ext) == s.size() - ext.size()) {
	  return fs::path{s.substr(0, s.size() - ext.size())};
	}
	return fs::path{s + ".raw"};
  }
}


/**
 * Una classe C++ RAII per mappare un file in memoria in sola lettura.
 * Gestisce open(), mmap(), munmap() e close().
 */
class ReadOnlyFileMap {
private:
  int         fd_ = -1;       // File descriptor
  void* ptr_ = MAP_FAILED; // Puntatore alla memoria
  std::size_t size_ = 0;      // Dimensione del file
  
public:
  // Costruttore: acquisisce tutte le risorse
  ReadOnlyFileMap(const std::string& filename) {
	//Apre file
	fd_ = open(filename.c_str(), O_RDONLY);
	if (fd_ == -1) {
	  throw std::runtime_error("Errore open: " + std::string(strerror(errno)));
	}
	
	//Ottiene la dimensione
	struct stat file_info;
	if (fstat(fd_, &file_info) == -1) {
	  close(fd_); //Pulisce prima di lanciare
	  throw std::runtime_error("Errore fstat: " + std::string(strerror(errno)));
	}
	size_ = file_info.st_size;
	
	if (size_ == 0) {
	  // File vuoto da non mappare.
	  return;
	}
	
	//Fase di mapping
	ptr_ = mmap(nullptr, size_, PROT_READ, MAP_PRIVATE, fd_, 0);
	if (ptr_ == MAP_FAILED) {
	  close(fd_); //Pulisce prima di lanciare l'errore
	  throw std::runtime_error("Errore mmap: " + std::string(strerror(errno)));
	}
  }
  
  // Distruttore: rilascia tutte le risorse
  ~ReadOnlyFileMap() {
	if (ptr_ != MAP_FAILED) {
	  munmap(ptr_, size_);
	}
	if (fd_ != -1) {
	  close(fd_);
	}
  }
  
  //Rende la classe non copiabile 
  ReadOnlyFileMap(const ReadOnlyFileMap&) = delete;
  ReadOnlyFileMap& operator=(const ReadOnlyFileMap&) = delete;
  
  //Rende la classe movable
  ReadOnlyFileMap(ReadOnlyFileMap&& other) noexcept
	: fd_(other.fd_), ptr_(other.ptr_), size_(other.size_) {
	other.fd_ = -1;
	other.ptr_ = MAP_FAILED;
	other.size_ = 0;
  }

  //Restituisce la size del file
  std::size_t size() const { return size_; }
  
  //Restituisce un puntatore ai dati
  const unsigned char* data() const {
	return static_cast<const unsigned char*>(ptr_);
  }
};

/**
 * @brief Una classe C++ RAII per mappare un file in memoria in scrittura.
 * Gestisce la creazione del file, l'allocazione iniziale (ftruncate),
 * il mapping (mmap) e il ridimensionamento finale (commit).
 */
class WriteOnlyFileMap {
private:
  int fd_ = -1;                 
  unsigned char* ptr_ = nullptr;// Puntatore all'area di memoria mappata
  std::size_t capacity_ = 0;    // Capacità massima allocata inizialmente
  std::string filename_;        // Nome del file
  
public:
  // Costruttore: crea il file, alloca lo spazio e mappa la memoria
  WriteOnlyFileMap(const std::string& filename, std::size_t initial_capacity) 
    : fd_(-1), ptr_(nullptr), capacity_(initial_capacity), filename_(filename) 
  {
    //Apre il file in modalità lettura/scrittura, creandolo se non esiste o troncandolo
    fd_ = open(filename.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd_ == -1) {
      throw std::runtime_error("Errore open (write): " + std::string(strerror(errno)));
    }

    //Imposta la dimensione fisica del file alla capacità richiesta
    if (ftruncate(fd_, capacity_) == -1) { 
      close(fd_); 
      throw std::runtime_error("Errore ftruncate: " + std::string(strerror(errno))); 
    }

    //Mappa il file in memoria
    void* p = mmap(nullptr, capacity_, PROT_WRITE | PROT_READ, MAP_SHARED, fd_, 0);
    if (p == MAP_FAILED) { 
      close(fd_); 
      throw std::runtime_error("Errore mmap (write): " + std::string(strerror(errno))); 
    }
    ptr_ = static_cast<unsigned char*>(p);
  }
  
  //Distruttore: rilascia tutte le risorse assicurando la sincronizzazione
  ~WriteOnlyFileMap() { 
    close_resource(); 
  }
  
  //Rende la classe movable
  WriteOnlyFileMap(WriteOnlyFileMap&& other) noexcept 
    : fd_(other.fd_), ptr_(other.ptr_), capacity_(other.capacity_), filename_(std::move(other.filename_)) 
  {
    //"Ruba" le risorse e invalida l'oggetto sorgente
    other.fd_ = -1; 
    other.ptr_ = nullptr; 
    other.capacity_ = 0;
  }
  
  //Operatore di assegnamento move
  WriteOnlyFileMap& operator=(WriteOnlyFileMap&& other) noexcept {
    if (this != &other) {
      close_resource(); // Pulisce le risorse correnti
      // Trasferisce le risorse
      fd_ = other.fd_; 
      ptr_ = other.ptr_; 
      capacity_ = other.capacity_; 
      filename_ = std::move(other.filename_);
      
      // Invalida l'altro oggetto
      other.fd_ = -1; 
      other.ptr_ = nullptr; 
      other.capacity_ = 0;
    }
    return *this;
  }
  
  //Rende la classe non copiabile
  WriteOnlyFileMap(const WriteOnlyFileMap&) = delete;
  WriteOnlyFileMap& operator=(const WriteOnlyFileMap&) = delete;
  
  //Restituisce il puntatore all'area di memoria scrivibile
  unsigned char* data() { return ptr_; }
  
  //Restituisce la capacità totale allocata
  std::size_t capacity() const { return capacity_; }
  
  /**
   * Finalizza il file ridimensionandolo alla dimensione effettiva dei dati scritti.
   * Sincronizza la memoria su disco, rimuove il mapping e tronca il file.
   */
  void commit(std::size_t actual_size) {
    if (ptr_ && actual_size != capacity_) {
      //Sincronizza le modifiche su disco
      msync(ptr_, capacity_, MS_SYNC);
      //Rimuove il mapping
      munmap(ptr_, capacity_);
      ptr_ = nullptr;
      
      //Ridimensiona il file alla grandezza reale dei dati utili
      if (ftruncate(fd_, actual_size) == -1) {
        throw std::runtime_error("Errore ftruncate finale");
      }
      capacity_ = actual_size;
    }
  }
  
private:
  //Helper per la chiusura sicura delle risorse
  void close_resource() {
    if (ptr_) { 
      msync(ptr_, capacity_, MS_SYNC); //Assicura che i dati siano scritti
      munmap(ptr_, capacity_); 
      ptr_ = nullptr; 
    }
    if (fd_ != -1) { 
      close(fd_); 
      fd_ = -1; 
    }
  }
};
