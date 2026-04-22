# Copyright 2021 Matrix Origin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

NVCC := /usr/local/cuda/bin/nvcc
CC   := gcc
CXX  := g++

# Libraries
# -lgomp is needed for OpenMP pragmas in filter.hpp's eval_filter_bitmap_cpu;
# nvcc's host compiler (gcc) emits libgomp calls when -fopenmp is passed via
# -Xcompiler below.
LIBS := -L/usr/local/cuda/lib64/stubs -lcuda -L/usr/local/cuda/lib64/cudart -L$(CONDA_PREFIX)/lib -lcuvs -lcuvs_c -ldl -lrmm -lrapids_logger -Xlinker -lpthread -Xlinker -lm -Xlinker -lgomp

INCLUDES := -I. -I/usr/local/cuda/include -I$(CONDA_PREFIX)/include -I$(CONDA_PREFIX)/include/rapids -I$(CONDA_PREFIX)/include/raft -I$(CONDA_PREFIX)/include/cuvs

# NVCC_FLAGS are for compilation only. -x cu tells nvcc to treat .cpp as .cu.
# -fopenmp is forwarded to the host compiler via -Xcompiler so OpenMP pragmas
# in filter.hpp's eval_filter_bitmap_cpu compile and parallelise instead of
# being dropped with a -Wunknown-pragmas warning.
NVCC_FLAGS := -O3 -std=c++17 -x cu -Xcompiler "-Wall -Wextra -fPIC -fopenmp" --extended-lambda --expt-relaxed-constexpr $(INCLUDES) -DLIBCUDACXX_ENABLE_EXPERIMENTAL_MEMORY_RESOURCE -DRAFT_SYSTEM_LITTLE_ENDIAN=1 \
    -gencode arch=compute_75,code=sm_75 \
    -gencode arch=compute_80,code=sm_80 \
    -gencode arch=compute_86,code=sm_86 \
    -gencode arch=compute_89,code=sm_89 \
    -gencode arch=compute_90,code=sm_90 \
    -gencode arch=compute_90,code=compute_90

# LDFLAGS for linking only. DO NOT include -x cu here.
# -fopenmp ensures the link driver pulls libgomp in when nvcc invokes g++ to
# produce the shared library.
LDFLAGS := -O3 -std=c++17 -Xcompiler "-Wall -Wextra -fPIC -fopenmp"

# Source files
C_SRCS := brute_force_c.cpp ivf_flat_c.cpp ivf_pq_c.cpp cagra_c.cpp kmeans_c.cpp adhoc_c.cpp distance_c.cpp
CPP_SRCS := helper.cpp
TEST_SRCS := test/main_test.cu test/brute_force_test.cu test/ivf_flat_test.cu test/ivf_pq_test.cu test/cagra_test.cu test/kmeans_test.cu test/quantize_test.cu test/distance_test.cu test/batching_test.cu test/snmg_test.cu test/verify_half_conversion.cu test/filter_test.cu

# Object files
OBJS := $(C_SRCS:.cpp=.o) $(CPP_SRCS:.cpp=.o)
TEST_OBJS := $(patsubst test/%.cu,obj/test/%.o,$(TEST_SRCS))

.PHONY: all clean debug release test

all: libmocuvs.so

test: test_cuvs_worker benchmark_cuvs benchmark_filter test_kmeans

release: all

debug: NVCC_FLAGS := $(filter-out -O3,$(NVCC_FLAGS)) -O0 -g -lineinfo
debug: LDFLAGS := $(filter-out -O3,$(LDFLAGS)) -g
debug: all

libmocuvs.so: $(OBJS)
	$(NVCC) $(LDFLAGS) -shared -o $@ $^ $(LIBS)

%.o: %.cpp
	@echo "Compiling $< with NVCC"
	$(NVCC) $(NVCC_FLAGS) -c $< -o $@

obj/test/%.o: test/%.cu
	@mkdir -p $(@D)
	@echo "NVCC $<"
	$(NVCC) $(NVCC_FLAGS) -c $< -o $@

test_cuvs_worker: $(TEST_OBJS) $(OBJS)
	@echo "Linking $@"
	$(NVCC) $(LDFLAGS) $^ $(LIBS) -o $@

benchmark_cuvs: obj/test/benchmark_cuvs.o $(OBJS)
	@echo "Linking $@"
	$(NVCC) $(LDFLAGS) $^ $(LIBS) -o $@

benchmark_filter: obj/test/benchmark_filter.o $(OBJS)
	@echo "Linking $@"
	$(NVCC) $(LDFLAGS) $^ $(LIBS) -o $@

test_kmeans: obj/test/test_kmeans.o $(OBJS)
	@echo "Linking $@"
	$(NVCC) $(LDFLAGS) $^ $(LIBS) -o $@

clean:
	@echo "Cleaning up..."
	rm -f libmocuvs.so *.o test_cuvs_worker benchmark_cuvs benchmark_filter test_kmeans
	rm -rf obj
