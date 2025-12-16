
#include <cstdio>
#include <chrono>
#include "config.hpp"
#include "cmdline.hpp"
#include "wrappers.hpp"
#include "parallel_omp.hpp"

int main(int argc, char *argv[]) {
    // parse command line arguments and set some global variables
    long start=parseCommandLine(argc, argv);
    if (start<0) return -1;

	using clock = std::chrono::steady_clock;
	const auto t0 = clock::now();
	
	bool success = true;
	while(argv[start]) {
		if (isDirectory(argv[start])) {
			success &= parallel_walkDir(argv[start]);
		} else {
			success &= doWork(argv[start]);
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
