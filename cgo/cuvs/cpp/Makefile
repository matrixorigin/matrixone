# C++ compiler
CXX := g++
NVCC := $(CUDA_HOME)/bin/nvcc

# Compiler flags
CLFLAGS := -I$(CUDA_HOME)/include -I$(HOME)/miniconda3/envs/go/include -I$(HOME)/miniconda3/envs/go/include/rapids -I$(HOME)/miniconda3/envs/go/include/raft -I$(HOME)/miniconda3/envs/go/include/cuvs
NVCCFLAGS := -std=c++17 -Xcompiler "-Wall -Wextra -fPIC -O2" --extended-lambda --expt-relaxed-constexpr -I. $(CLFLAGS) -DLIBCUDACXX_ENABLE_EXPERIMENTAL_MEMORY_RESOURCE -DRAFT_SYSTEM_LITTLE_ENDIAN=1

# Source directory
SRCDIR := .

# Object directory
OBJDIR := obj

# Test directory
TESTDIR := test

# Header files
HEADERS := brute_force.hpp cagra.hpp cuvs_worker.hpp ivf_flat.hpp kmeans.hpp

# Test source files
TEST_SRCS := $(TESTDIR)/main_test.cu \
             $(TESTDIR)/brute_force_test.cu \
             $(TESTDIR)/ivf_flat_test.cu \
             $(TESTDIR)/cagra_test.cu \
             $(TESTDIR)/kmeans_test.cu

# Test object files
TEST_OBJS := $(patsubst $(TESTDIR)/%.cu, $(OBJDIR)/test/%.o, $(TEST_SRCS))

# Test executable
TEST_EXE := test_cuvs_worker

# Libraries to link
LIBS := -L$(CUDA_HOME)/lib64/stubs -lcuda -L$(CUDA_HOME)/lib64 -lcudart -L$(HOME)/miniconda3/envs/go/lib -lcuvs -lcuvs_c -ldl -lrmm -Xlinker -lpthread

# Default target
all: $(TEST_EXE)

# Rule to link the test executable
$(TEST_EXE): $(TEST_OBJS)
	@echo "NVCCLD $@"
	$(NVCC) $(NVCCFLAGS) $^ $(LIBS) -o $@

# Rule to compile test source files
$(OBJDIR)/test/%.o: $(TESTDIR)/%.cu $(HEADERS)
	@mkdir -p $(dir $@)
	@echo "NVCC $<"
	$(NVCC) $(NVCCFLAGS) -c $< -o $@

# Target to run tests
test: $(TEST_EXE)
	@echo "Running tests..."
	./$(TEST_EXE)

# Phony target to clean up build artifacts
clean:
	@echo "Cleaning up..."
	rm -f $(TEST_EXE)
	rm -rf $(OBJDIR)

# Phony targets are not files
.PHONY: all clean test
