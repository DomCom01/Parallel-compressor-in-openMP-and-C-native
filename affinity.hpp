#pragma once
#include <cerrno>
#include <cstdio>
#include <pthread.h>
#include <sched.h>
#include <cstdint>
#include <unistd.h>
#include <vector>
#include <stdexcept>
#include <sstream>
#include <system_error>

namespace affinity {

std::string get_affinity_from_env(const char* envname= "AFFINITY") {
	const char* p = std::getenv(envname);
	if (p && *p) return std::string(p);
	return {};
}
	
// Parse argument string "0,2,4-10:2" -> {0,2,4,6,8,10}
inline std::vector<unsigned> parse_cpu_list(const std::string& slist) {
	auto trim = [](std::string& s) {
		while (!s.empty() && std::isspace((unsigned char)s.front())) s.erase(s.begin());
		while (!s.empty() && std::isspace((unsigned char)s.back()))  s.pop_back();
	};
	auto parse_uint = [](std::string_view v) {
		if (v.empty()) throw std::invalid_argument("affinity: empty numeric token");
		size_t idx = 0;
		unsigned x = std::stoul(std::string(v), &idx, 0);
		if (idx != v.size()) throw std::invalid_argument("affinity: bad number '" + std::string(v) + "'");
		return x;
	};

	std::vector<unsigned> cpus;
    if (slist.empty()) return cpus;
    std::stringstream ss(slist);
    std::string tok;
    while (std::getline(ss, tok, ',')) {
        trim(tok);
        if (tok.empty()) continue;
        auto dash = tok.find('-');
        auto col  = tok.find(':');
        if (dash == std::string::npos) {
            cpus.push_back(parse_uint(tok));
        } else {
            unsigned a = parse_uint(std::string_view(tok).substr(0, dash));
            unsigned b, s = 1;
            if (col == std::string::npos) {
                b = parse_uint(std::string_view(tok).substr(dash + 1));
            } else {
                b = parse_uint(std::string_view(tok).substr(dash + 1, col - (dash + 1)));
                s = parse_uint(std::string_view(tok).substr(col + 1));
                if (s == 0) throw std::invalid_argument("affinity: step 0");
            }
            if (a > b) std::swap(a, b);
            for (unsigned c = a; c <= b; c += s) cpus.push_back(c);
        }
    }
    return cpus; 
}

	
#if defined(__linux__)
	
inline bool pin_thread_to_core(unsigned cpu) {
	if (cpu >= CPU_SETSIZE) {
		std::printf("affinity::pin_thread_to_core, CPU index (%u) >= CPU_SETSIZE\n", cpu);
		return false;
	}
	// Check against current affinity mask
    cpu_set_t allowed;
    CPU_ZERO(&allowed);
    if (sched_getaffinity(0, sizeof(allowed), &allowed) != 0) {
        std::printf("affinity::pin_thread_to_cpu_list, sched_getaffinity failed errno=%d\n", errno);
        return false;
    }
	if (!CPU_ISSET(cpu, &allowed)) {
		std::printf("affinity::pin_thread_to_cpu_list, CPU %u not allowed by current cpuset\n", cpu);
		return false;
	}
	cpu_set_t set;
	CPU_ZERO(&set);
	CPU_SET(cpu, &set);  
    int rc = pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
    if (rc != 0) {
		std::printf("affinity::pin_thread_to_core, pthread_setaffinity_np failed errno=%d\n",rc);
		return false;
	}
	return true;
}

// here the array is considered as a bitmap	
inline bool pin_thread_to_cores(const std::vector<unsigned>& bitmask) {
    if (bitmask.empty()) {
		std::printf("affinity::pin_thread_to_cores, bitmask vector is empty\n");
		return false;
	}
	// Check against current affinity mask
    cpu_set_t allowed;
    CPU_ZERO(&allowed);
    if (sched_getaffinity(0, sizeof(allowed), &allowed) != 0) {
        std::printf("affinity::pin_thread_to_cpu_list, sched_getaffinity failed errno=%d\n", errno);
        return false;
    }

    cpu_set_t set;
	CPU_ZERO(&set);
	for (unsigned c : bitmask) {
        if (!bitmask[c]) continue;                
        if (c >= CPU_SETSIZE) {
			std::printf("affinity::pin_thread_to_cores, CPU index (%u) >= CPU_SETSIZE\n", c);
			return false;
		}
        if (!CPU_ISSET(c, &allowed)) {
            std::printf("affinity::pin_thread_to_cpu_list, CPU %u not allowed by current cpuset\n", c);
            return false;
        }
        CPU_SET(static_cast<int>(c), &set);  
    }
    int rc = pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
    if (rc != 0) {
		std::printf("affinity::pin_thread_to_cores, pthread_setaffinity_np failed errno=%d\n",rc);
		return false;
	}
	return true;
}

// here the array is considered as a list of cpu ids
inline bool pin_thread_to_cpu_list(const std::vector<unsigned>& cpus) {
    if (cpus.empty()) {
        std::printf("affinity::pin_thread_to_cpu_list, empty list\n");
        return false;
    }

    // Check against current affinity mask
    cpu_set_t allowed;
    CPU_ZERO(&allowed);
    if (sched_getaffinity(0, sizeof(allowed), &allowed) != 0) {
        std::printf("affinity::pin_thread_to_cpu_list, sched_getaffinity failed errno=%d\n", errno);
        return false;
    }

    cpu_set_t set;
    CPU_ZERO(&set);
    for (unsigned c : cpus) {
        if (c >= CPU_SETSIZE) {
            std::printf("affinity::pin_thread_to_cpu_list, CPU index (%u) >= CPU_SETSIZE\n", c);
            return false;
        }
        if (!CPU_ISSET(c, &allowed)) {
            std::printf("affinity::pin_thread_to_cpu_list, CPU %u not allowed by current cpuset\n", c);
            return false;
        }
        CPU_SET(static_cast<int>(c), &set);
    }

    int rc = pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
    if (rc != 0) {
        std::printf("affinity::pin_thread_to_cpu_list, pthread_setaffinity_np failed errno=%d\n", rc);
        return false;
    }
    return true;
}

	
inline bool get_thread_affinity(std::vector<unsigned>& out) {
    // Dimensioniamo la maschera in base ai core configurati
    long nproc = ::sysconf(_SC_NPROCESSORS_CONF);
    if (nproc <= 0) nproc = CPU_SETSIZE;  // fallback

    // cpu_set_t dinamico (GNU extensions)
    size_t setsize = CPU_ALLOC_SIZE(nproc);
    cpu_set_t* set = CPU_ALLOC(nproc);
    if (!set) {
		std::printf("affinity::get_thread_affinity, CPU_ALLOC failed errno=%d\n",errno);
		return false;
	}
    CPU_ZERO_S(setsize, set);

    int rc = ::pthread_getaffinity_np(::pthread_self(), setsize, set);
    if (rc != 0) {
		CPU_FREE(set);
		std::printf("affinity::get_thread_affinity, pthread_setaffinity_np failed errno=%d\n",rc);
		return false;
	}
	out.clear();
    out.reserve(static_cast<size_t>(nproc));
    for (int cpu = 0; cpu < nproc; ++cpu) {
        if (CPU_ISSET_S(cpu, setsize, set))
            out.push_back(static_cast<unsigned>(cpu));
    }
    CPU_FREE(set);
	return true;
}

inline unsigned get_current_cpu() {
	auto cpu = ::sched_getcpu();
	if (cpu==-1) {
		std::printf("affinity::get_current_cpu, sched_getcpu failed errno=%d\n", errno);
	}
	return cpu;
}

#else
#error "thread affinity support only for Linux OS"
#endif 
	
} // namespace affinity
