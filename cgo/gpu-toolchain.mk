# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Include only from GPU builds. Cleaning a native generation must work after
# the original Pixi environment was removed.
ifndef MO_GPU_TOOLCHAIN_INCLUDED
MO_GPU_TOOLCHAIN_INCLUDED := 1
ifneq ($(strip $(filter-out clean clobber,$(if $(MAKECMDGOALS),$(MAKECMDGOALS),all))),)
ifeq ($(strip $(PIXI_PROJECT_ROOT)),)
$(error GPU builds require PIXI_PROJECT_ROOT from pixi run --frozen)
endif
ifeq ($(strip $(PIXI_ENVIRONMENT_NAME)),)
$(error GPU builds require PIXI_ENVIRONMENT_NAME from pixi run --frozen)
endif
ifeq ($(strip $(CONDA_PREFIX)),)
$(error GPU builds require CONDA_PREFIX from pixi run --frozen)
endif
ifneq ($(words $(CONDA_PREFIX) $(PIXI_PROJECT_ROOT)),2)
$(error GPU build paths cannot contain whitespace)
endif
MO_GPU_PREFIX := $(realpath $(CONDA_PREFIX))
ifeq ($(MO_GPU_PREFIX),)
$(error CONDA_PREFIX does not name an installed Pixi environment)
endif
ifneq ($(MO_GPU_PREFIX),$(realpath $(PIXI_PROJECT_ROOT)/.pixi/envs/$(PIXI_ENVIRONMENT_NAME)))
$(error CONDA_PREFIX does not match the activated Pixi environment)
endif
MO_GPU_CUDA_ROOT := $(MO_GPU_PREFIX)/targets/x86_64-linux
MO_GPU_CC := $(MO_GPU_PREFIX)/bin/x86_64-conda-linux-gnu-cc
MO_GPU_CXX := $(MO_GPU_PREFIX)/bin/x86_64-conda-linux-gnu-c++
MO_GPU_NVCC := $(MO_GPU_PREFIX)/bin/nvcc
MO_GPU_CUDA_INCLUDE_DIRS := $(MO_GPU_CUDA_ROOT)/include
MO_GPU_CUDA_LIBRARY_DIRS := $(MO_GPU_CUDA_ROOT)/lib
MO_GPU_CUDA_STUB_DIRS := $(MO_GPU_CUDA_ROOT)/lib/stubs
MO_GPU_RAPIDS_INCLUDE_DIRS := $(MO_GPU_PREFIX)/include
MO_GPU_CFLAGS := -I$(MO_GPU_CUDA_INCLUDE_DIRS) -I$(MO_GPU_RAPIDS_INCLUDE_DIRS)
MO_GPU_LDFLAGS := -L$(MO_GPU_CUDA_STUB_DIRS) -lcuda -L$(MO_GPU_CUDA_LIBRARY_DIRS) -lcudart -L$(MO_GPU_PREFIX)/lib -lcuvs -lcuvs_c -lstdc++
MO_GPU_RUNTIME_PATH := $(MO_GPU_CUDA_LIBRARY_DIRS):$(MO_GPU_PREFIX)/lib
MO_GPU_REQUIRED := $(PIXI_PROJECT_ROOT)/pixi.lock $(MO_GPU_PREFIX)/conda-meta $(MO_GPU_CC) $(MO_GPU_CXX) $(MO_GPU_NVCC) $(MO_GPU_CUDA_INCLUDE_DIRS)/cuda.h $(MO_GPU_CUDA_LIBRARY_DIRS)/libcudart.so $(MO_GPU_CUDA_STUB_DIRS)/libcuda.so $(MO_GPU_RAPIDS_INCLUDE_DIRS)/cuvs/core/c_api.h $(MO_GPU_PREFIX)/lib/libcuvs.so $(MO_GPU_PREFIX)/lib/libcuvs_c.so $(MO_GPU_PREFIX)/lib/librmm.so
ifneq ($(words $(wildcard $(MO_GPU_REQUIRED))),$(words $(MO_GPU_REQUIRED)))
$(error incomplete Pixi GPU environment; run pixi install --frozen)
endif
override CC := $(MO_GPU_CC)
override CXX := $(MO_GPU_CXX)
override HOST_COMPILER := $(MO_GPU_CXX)
override NVCC := $(MO_GPU_NVCC) -ccbin $(MO_GPU_CXX)
export CC CXX
endif
endif
