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

# Added UCX, UCXX, NCCL libraries
LIBS := -L/usr/local/cuda/lib64/stubs -lcuda -L/usr/local/cuda/lib64/cudart -L/home/ubuntu/miniconda/envs/go/lib -lcuvs -lcuvs_c -lnccl -lucxx -lucp -luct -lucs -lucm -ldl -lrmm -lrapids_logger -Xlinker -lpthread -Xlinker -lm

INCLUDES := -I. -I/usr/local/cuda/include -I/home/ubuntu/miniconda/envs/go/include -I/home/ubuntu/miniconda/envs/go/include/rapids -I/home/ubuntu/miniconda/envs/go/include/raft -I/home/ubuntu/miniconda/envs/go/include/cuvs

# NVCC_FLAGS are for compilation
NVCC_FLAGS := -std=c++17 -x cu -Xcompiler "-Wall -Wextra -fPIC" --extended-lambda --expt-relaxed-constexpr $(INCLUDES) -DLIBCUDACXX_ENABLE_EXPERIMENTAL_MEMORY_RESOURCE -DRAFT_SYSTEM_LITTLE_ENDIAN=1

# LDFLAGS for linking
LDFLAGS := -std=c++17 -Xcompiler "-Wall -Wextra -fPIC"

# Source files
C_SRCS := brute_force_c.cpp ivf_flat_c.cpp ivf_pq_c.cpp cagra_c.cpp kmeans_c.cpp adhoc_c.cpp distance_c.cpp
CPP_SRCS := helper.cpp
TEST_SRCS := test/main_test.cu test/brute_force_test.cu test/ivf_flat_test.cu test/ivf_pq_test.cu test/cagra_test.cu test/kmeans_test.cu test/quantize_test.cu test/distance_test.cu test/batching_test.cu test/snmg_test.cu

# Object files
OBJS := $(C_SRCS:.cpp=.o) $(CPP_SRCS:.cpp=.o)
TEST_OBJS := $(patsubst test/%.cu,obj/test/%.o,$(TEST_SRCS))

.PHONY: all clean debug snmg

all: libmocuvs.so test_cuvs_worker

debug: NVCC_FLAGS += -O0 -g -lineinfo
debug: LDFLAGS += -g
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

snmg: test/test_snmg_init.cu
	@echo "Compiling and linking test_snmg_init"
	$(NVCC) $(NVCC_FLAGS) $(LDFLAGS) $< $(LIBS) -o test_snmg_init

clean:
	@echo "Cleaning up..."
	rm -f libmocuvs.so *.o test_cuvs_worker test_snmg_init
	rm -rf obj
