# Makefile for MatrixOne cuVS C Wrapper

UNAME_M := $(shell uname -m)
CUDA_PATH ?= /usr/local/cuda
NVCC := $(CUDA_PATH)/bin/nvcc

ifeq ($(CONDA_PREFIX),)
  $(error CONDA_PREFIX env variable not found. Please activate your conda environment.)
endif

# Compilation flags
# Added --extended-lambda because raft/core/copy.cuh requires it for some internal headers
NVCC_FLAGS := -std=c++17 -x cu -Xcompiler "-Wall -Wextra -fPIC -O2" --extended-lambda --expt-relaxed-constexpr
NVCC_FLAGS += -I. -I$(CUDA_PATH)/include -I$(CONDA_PREFIX)/include -I$(CONDA_PREFIX)/include/rapids -I$(CONDA_PREFIX)/include/raft -I$(CONDA_PREFIX)/include/cuvs
NVCC_FLAGS += -DLIBCUDACXX_ENABLE_EXPERIMENTAL_MEMORY_RESOURCE -DRAFT_SYSTEM_LITTLE_ENDIAN=1

# Linking flags
LDFLAGS := -shared
LDFLAGS += -L$(CUDA_PATH)/lib64/stubs -lcuda -L$(CUDA_PATH)/lib64 -lcudart
LDFLAGS += -L$(CONDA_PREFIX)/lib -lcuvs -lcuvs_c -ldl -lrmm
LDFLAGS += -Xlinker -lpthread -Xlinker -lm

# Target library
TARGET := libmocuvs.so

# Source files
SRCS := brute_force_c.cpp ivf_flat_c.cpp cagra_c.cpp kmeans_c.cpp helper.cpp
OBJS := $(SRCS:.cpp=.o)

# Test configuration
TESTDIR := test
OBJDIR := obj
TEST_EXE := test_cuvs_worker
TEST_SRCS := $(TESTDIR)/main_test.cu \
             $(TESTDIR)/brute_force_test.cu \
             $(TESTDIR)/ivf_flat_test.cu \
             $(TESTDIR)/cagra_test.cu \
             $(TESTDIR)/kmeans_test.cu

TEST_OBJS := $(patsubst $(TESTDIR)/%.cu, $(OBJDIR)/test/%.o, $(TEST_SRCS))

.PHONY: all clean test

all: $(OBJS)

$(TARGET): $(OBJS)
	@echo "Linking shared library $@"
	$(NVCC) $(LDFLAGS) $^ -o $@

%.o: %.cpp
	@echo "Compiling $< with NVCC"
	$(NVCC) $(NVCC_FLAGS) -c $< -o $@

# Test targets
test: $(TEST_EXE)
	@echo "Running tests..."
	./$(TEST_EXE)

$(TEST_EXE): $(TEST_OBJS)
	@echo "NVCCLD $@"
	$(NVCC) $(subst -x cu,,$(NVCC_FLAGS)) $^ $(subst -shared,,$(LDFLAGS)) -o $@

$(OBJDIR)/test/%.o: $(TESTDIR)/%.cu
	@mkdir -p $(@D)
	@echo "NVCC $<"
	$(NVCC) -std=c++17 -Xcompiler "-Wall -Wextra -fPIC -O2" --extended-lambda --expt-relaxed-constexpr -I. -I$(CUDA_PATH)/include -I$(CONDA_PREFIX)/include -I$(CONDA_PREFIX)/include/rapids -I$(CONDA_PREFIX)/include/raft -I$(CONDA_PREFIX)/include/cuvs -DLIBCUDACXX_ENABLE_EXPERIMENTAL_MEMORY_RESOURCE -DRAFT_SYSTEM_LITTLE_ENDIAN=1 -c $< -o $@

clean:
	@echo "Cleaning up..."
	rm -f $(TARGET) *.o $(TEST_EXE)
	rm -rf $(OBJDIR)
