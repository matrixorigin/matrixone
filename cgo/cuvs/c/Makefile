# C++ compiler (use NVCC for CUDA-related compilation and linking)
NVCC := $(CUDA_HOME)/bin/nvcc
CXX := g++

# Paths
CUDA_HOME ?= /usr/local/cuda
GOCUVS ?= /home/eric/miniconda3/envs/go
CONDA_PREFIX ?= /home/eric/miniconda3/envs/go

# Common include flags
CLFLAGS := -I. -I$(CUDA_HOME)/include -I$(CONDA_PREFIX)/include -I$(GOCUVS)/include/rapids -I$(GOCUVS)/include/raft -I$(GOCUVS)/include/cuvs -I../cpp

# Compiler flags
NVCC_COMPILER_FLAGS := -std=c++17 -x cu -Xcompiler "-Wall -Wextra -fPIC -O2" $(CLFLAGS) -DLIBCUDACXX_ENABLE_EXPERIMENTAL_MEMORY_RESOURCE

# Linker flags
NVCC_LDFLAGS := -L$(CUDA_HOME)/lib64/stubs -lcuda -L$(CUDA_HOME)/lib64 -lcudart -L$(GOCUVS)/lib -lcuvs -lcuvs_c -ldl -lrmm
HOST_LDFLAGS := -lpthread -lm
LDFLAGS := $(NVCC_LDFLAGS) $(addprefix -Xlinker ,$(HOST_LDFLAGS))

# Unified library name
TARGET_LIB := libmocuvs.so
SRCS := brute_force_c.cpp ivf_flat_c.cpp sharded_ivf_flat_c.cpp
OBJS := brute_force_c.o ivf_flat_c.o sharded_ivf_flat_c.o

.PHONY: all clean

all: $(TARGET_LIB)

$(TARGET_LIB): $(OBJS)
	@echo "Linking shared library $@"
	$(NVCC) -shared $(OBJS) $(LDFLAGS) -o $@

%.o: %.cpp
	@echo "Compiling $< with NVCC"
	$(NVCC) $(NVCC_COMPILER_FLAGS) -c $< -o $@

clean:
	@echo "Cleaning up..."
	rm -f $(TARGET_LIB) *.o
