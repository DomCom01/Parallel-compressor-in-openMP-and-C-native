#pragma once

#include <string>
#include <string_view>
#include <charconv>
#include <cstdio>
#include "config.hpp"

inline void usage(const char* prog) {
  std::fprintf(stderr,
R"(Uso: %s [opzioni] <file_o_dir> [altri_file_o_dir...]
Opzioni:
  -r 0|1     Recur into directories (default 1)
  -C 0|1     Compress (0 preserve, 1 remove (default 0)
  -D 0|1     Decompress (0 preserve, 1 remove (default 0)
  -t <bytes> Threshold per la dimensione dei file da considerare piccoli
  -v         Verbose mode
  -h         Help

Esempio:
  %s -r 1 -C 0 -t 4194304 mydir file1.bin file2.txt
)",
        prog, prog);
}

inline long parseCommandLine(int argc, char* argv[]) {
  if (argc < 2) {
	usage(argv[0]); return -1;
  }
  
  // safe convertion from char to int
  auto to_int = [](const char* s, int& out)->bool{
	if (!s) return false;
	const char* begin = s;
	const char* end = s + std::char_traits<char>::length(s);
	auto r = std::from_chars(begin, end, out);
	return r.ec == std::errc{} && r.ptr == end;
  };
  
  bool seenC = false, seenD = false;
  int i = 1;
  while (i < argc && argv[i][0] == '-') {
	std::string_view opt = argv[i];
	if (opt == "-h" || opt == "--help") { usage(argv[0]); return -1; }
	else if (opt == "-v") {
	  cfg.verbose = true;
	  ++i;
	}
	else if (opt == "-t") {
	  if (i + 1 >= argc) {
		usage(argv[0]);
		return -1;
	  }
	  cfg.size_threshold = std::stoull(argv[i+1]);
	  i += 2;
	}
	else if (opt == "-r" || opt == "-C" || opt == "-D") {
	  if (i + 1 >= argc) {
		usage(argv[0]);
		return -1;
	  }
	  int val = 0;
	  if (!to_int(argv[i+1], val)) {
		usage(argv[0]);
		return -1;
	  }
	  
	  if (opt == "-r") {
		cfg.recursive = (val != 0);
	  } else if (opt == "-C") {
		if (seenD) {
		  std::fprintf(stderr,"Error: only one between -C and -D.\n");
		  return -1;
		}
		seenC = true;
		cfg.mode = COMP;
		cfg.remove_input = (val != 0);
	  } else { // -D
		if (seenC) {
		  std::fprintf(stderr,"Error: only one between -C and -D.\n");
		  return -1;
		}
		seenD = true;
		cfg.mode = DECOMP;
		cfg.remove_input = (val != 0);
	  }
	  i += 2;
	} else {
	  usage(argv[0]); return -1;
	}
  }
  
  if (!seenC && !seenD) {
	std::fprintf(stderr, "Error: at least one between -C and -D.\n");
	usage(argv[0]); return -1;
  }
  if (i >= argc) {
	std::fprintf(stderr, "Error: no file or dir to work.\n");
	usage(argv[0]); return -1;
  }
  return i; // index of the first optional args
}
