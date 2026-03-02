# C++ compiler
CXX := g++
NVCC := $(CUDA_HOME)/bin/nvcc

# Compiler flags
# -std=c++17 is required for std::any
# -pthread is required for std::thread and pthread functions
# -Wall and -Wextra are for good practice warnings
# -O2 is for optimization
# -I. includes the current directory for headers
CLFLAGS := -I$(CUDA_HOME)/include -I$(CONDA_PREFIX)/include -I$(GOCUVS)/include/rapids -I$(GOCUVS)/include/raft -I$(GOCUVS)/include/cuvs
CXXFLAGS := -std=c++17 -pthread -Wall -Wextra -O2 -I. $(CLFLAGS) -DLIBCUDACXX_ENABLE_EXPERIMENTAL_MEMORY_RESOURCE -DRAFT_SYSTEM_LITTLE_ENDIAN=1
NVCCFLAGS := -std=c++17 -Xcompiler "-Wall -Wextra -fPIC -O2" --extended-lambda -I. $(CLFLAGS) -DLIBCUDACXX_ENABLE_EXPERIMENTAL_MEMORY_RESOURCE -DRAFT_SYSTEM_LITTLE_ENDIAN=1

# Source directory
SRCDIR := .

# Object directory
OBJDIR := obj

# Environment Variables for CUDA and GOCUVS (User should set these or adjust)
CUDA_HOME ?= /usr/local/cuda
GOCUVS ?= /home/eric/miniconda3/envs/go # Assuming GOCUVS base path if not specified

# LDFLAGS for linking the test executable
# -L specifies library search paths
# -l specifies libraries to link
# NVCC expects -Xlinker for host linker flags
NVCC_LDFLAGS := -L$(CUDA_HOME)/lib64/stubs -lcuda -L$(CUDA_HOME)/lib64 -lcudart -L$(GOCUVS)/lib -lcuvs -lcuvs_c -ldl -lrmm
HOST_LDFLAGS := -lpthread # For host linker, passed via -Xlinker

LDFLAGS := $(NVCC_LDFLAGS) $(addprefix -Xlinker ,$(HOST_LDFLAGS))

TEST_EXE := test_cuvs_worker
TEST_SRCS := $(SRCDIR)/test/main_test.cu $(SRCDIR)/test/brute_force_test.cu $(SRCDIR)/test/ivf_flat_test.cu $(SRCDIR)/test/sharded_ivf_flat_test.cu $(SRCDIR)/test/cagra_test.cu $(SRCDIR)/test/sharded_cagra_test.cu
TEST_OBJS := $(patsubst $(SRCDIR)/%.cu,$(OBJDIR)/%.o,$(TEST_SRCS))

# The default goal is to build only the test executable, as the library is header-only
all: $(TEST_EXE)

# Rule to build the test executable
$(TEST_EXE): $(TEST_OBJS)
	@echo "NVCCLD $@"
	$(NVCC) $(NVCCFLAGS) $(TEST_OBJS) $(LDFLAGS) -o $@

# Rule to compile the test source files (now .cu files with nvcc)
$(OBJDIR)/test/%.o: $(SRCDIR)/test/%.cu | $(OBJDIR)/test
	@echo "NVCC $<"
	$(NVCC) $(NVCCFLAGS) -c $< -o $@

# Rule to create the object directory for tests
$(OBJDIR)/test:
	mkdir -p $(OBJDIR)/test

# Rule to run the tests
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
