# C++ compiler (use NVCC for CUDA-related compilation and linking)
NVCC := $(CUDA_HOME)/bin/nvcc
CXX := g++

# Paths from parent Makefile
CUDA_HOME ?= /usr/local/cuda
GOCUVS ?= /home/eric/miniconda3/envs/go
CONDA_PREFIX ?= /home/eric/miniconda3/envs/go

# Common include flags for C++ compilation
CLFLAGS := -I. -I$(CUDA_HOME)/include -I$(CONDA_PREFIX)/include -I$(GOCUVS)/include/rapids -I$(GOCUVS)/include/raft -I$(GOCUVS)/include/cuvs -I../cpp

# Compiler flags for C++ source files
NVCC_COMPILER_FLAGS := -std=c++17 -x cu -Xcompiler "-Wall -Wextra -fPIC -O2" $(CLFLAGS) -DLIBCUDACXX_ENABLE_EXPERIMENTAL_MEMORY_RESOURCE

# Linker flags for creating a shared library
NVCC_LDFLAGS := -L$(CUDA_HOME)/lib64/stubs -lcuda -L$(CUDA_HOME)/lib64 -lcudart -L$(GOCUVS)/lib -lcuvs -lcuvs_c -ldl -lrmm
HOST_LDFLAGS := -lpthread -lm

LDFLAGS := $(NVCC_LDFLAGS) $(addprefix -Xlinker ,$(HOST_LDFLAGS))

# Output library names
BRUTE_FORCE_SHARED := libbrute_force_c.so
IVF_FLAT_SHARED := libivf_flat_c.so

.PHONY: all clean

all: $(BRUTE_FORCE_SHARED) $(IVF_FLAT_SHARED)

$(BRUTE_FORCE_SHARED): brute_force_c.o
	@echo "Linking shared library $@"
	$(NVCC) -shared $< $(LDFLAGS) -o $@

$(IVF_FLAT_SHARED): ivf_flat_c.o
	@echo "Linking shared library $@"
	$(NVCC) -shared $< $(LDFLAGS) -o $@

%.o: %.cpp
	@echo "Compiling $< with NVCC (as CUDA C++)"
	$(NVCC) $(NVCC_COMPILER_FLAGS) -c $< -o $@

clean:
	@echo "Cleaning up..."
	rm -f *.so *.o *.a
