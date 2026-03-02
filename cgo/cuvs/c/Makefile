# Makefile for MatrixOne cuVS C Wrapper

CUDA_PATH ?= /usr/local/cuda
NVCC := $(CUDA_PATH)/bin/nvcc

# Compilation flags
# Added --extended-lambda because raft/core/copy.cuh requires it for some internal headers
NVCC_FLAGS := -std=c++17 -x cu -Xcompiler "-Wall -Wextra -fPIC -O2" --extended-lambda --expt-relaxed-constexpr
NVCC_FLAGS += -I. -I$(CUDA_PATH)/include -I$(HOME)/miniconda3/envs/go/include -I$(HOME)/miniconda3/envs/go/include/rapids -I$(HOME)/miniconda3/envs/go/include/raft -I$(HOME)/miniconda3/envs/go/include/cuvs -I../cpp
NVCC_FLAGS += -DLIBCUDACXX_ENABLE_EXPERIMENTAL_MEMORY_RESOURCE -DRAFT_SYSTEM_LITTLE_ENDIAN=1

# Linking flags
LDFLAGS := -shared
LDFLAGS += -L$(CUDA_PATH)/lib64/stubs -lcuda -L$(CUDA_PATH)/lib64 -lcudart
LDFLAGS += -L$(HOME)/miniconda3/envs/go/lib -lcuvs -lcuvs_c -ldl -lrmm
LDFLAGS += -Xlinker -lpthread -Xlinker -lm

# Target library
TARGET := libmocuvs.so

# Source files (sharded_*_c.cpp removed as they are merged)
SRCS := brute_force_c.cpp ivf_flat_c.cpp cagra_c.cpp helper.cpp
OBJS := $(SRCS:.cpp=.o)

.PHONY: all clean

all: $(TARGET)

$(TARGET): $(OBJS)
	@echo "Linking shared library $@"
	$(NVCC) $(LDFLAGS) $^ -o $@

%.o: %.cpp
	@echo "Compiling $< with NVCC"
	$(NVCC) $(NVCC_FLAGS) -c $< -o $@

clean:
	@echo "Cleaning up..."
	rm -f $(TARGET) *.o
