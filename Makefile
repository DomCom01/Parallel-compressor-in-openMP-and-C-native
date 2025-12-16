# --- Definizioni Variabili e Percorsi ---
TBB_ROOT = /opt/intel/oneapi/tbb/latest
TBB_INC  = -I$(TBB_ROOT)/include
TBB_LIB  = -L$(TBB_ROOT)/lib

# Compilatore e Wrapper per il cluster
CXX      = g++
RUNNER   = srun

# Include directories: corrente, ./include e TBB
INCLUDES = -I. -I./include $(TBB_INC)

# Flags di Compilazione
# Nota: includo -fopenmp per tutti per uniformità, ma è essenziale per miniz_omp
CXXFLAGS = -std=c++20 -O3 -march=native -m64 -pthread -fopenmp $(INCLUDES)

# Flags del Linker
# -Wl,-rpath scrive il percorso della lib dentro l'eseguibile (evita export LD_LIBRARY_PATH)
LDFLAGS  = $(TBB_LIB) -Wl,-rpath,$(TBB_ROOT)/lib
LDLIBS   = -ltbb

# File comune a tutti i progetti
MINIZ_C  = miniz/miniz.c

# --- Target  ---

all: miniz_seq miniz_par miniz_omp

# 1. Sequenziale
miniz_seq: miniz_seq.cpp $(MINIZ_C)
	$(RUNNER) $(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $^ $(LDLIBS)

# 2. Parallelo (Task-based / Threading standard)
miniz_par: miniz_par.cpp $(MINIZ_C)
	$(RUNNER) $(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $^ $(LDLIBS)

# 3. OpenMP
miniz_omp: miniz_omp.cpp $(MINIZ_C)
	$(RUNNER) $(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $^ $(LDLIBS)

# --- Pulizia ---
clean:
	rm -f miniz_seq miniz_par miniz_omp *~
