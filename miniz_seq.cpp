#include <cstdio>
#include <chrono>
#include "config.hpp"
#include "sequential_algorithms.hpp"
#include "cmdline.hpp"
#include "wrappers.hpp"
#include "sequential.hpp"

/**Main per l'esecuzione sequenziale di compressione e decompressione.
 * Le funzioni chiamate da qui eseguono tutte le operazioni in sequenziale.
 */

int main(int argc, char *argv[]) {
  // parse command line arguments and set some global variables
  long start=parseCommandLine(argc, argv);
  if (start<0) return -1;
  
  using clock = std::chrono::steady_clock;
  const auto t0 = clock::now();
  
  bool success = true;
  while(argv[start]) {
	//Controlla se è stata passata una directory o un file
	if (isDirectory(argv[start])) {
	  success &= walkDir(argv[start]);
	} else {
	  success &= doWork(argv[start]);//Il file viene elaborato immediatamente
	}
	start++;
  }
  if (!success) {
	printf("Exiting with (some) Error(s)\n");
	return -1;
  }
  
  
  const auto t1 = clock::now();
  const double secs = std::chrono::duration<double>(t1 - t0).count();
  std::cout << "Time (s): " << secs << "\n";
  
  printf("Exiting with Success\n");
  return 0;
}
