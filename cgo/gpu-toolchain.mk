# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Include only from GPU builds. Cleaning a CPU/GPU generation must work after
# the original toolchain was removed, and must not require Python or Pixi.
ifndef MO_GPU_TOOLCHAIN_INCLUDED
MO_GPU_TOOLCHAIN_INCLUDED := 1
ifneq ($(strip $(filter-out clean clobber,$(if $(MAKECMDGOALS),$(MAKECMDGOALS),all))),)
# On older GNU Make, command-line variables are not necessarily in the
# environment of a parse-time $(shell ...) call. Export provider selectors
# before invoking the resolver so an explicit manifest cannot be lost.
export GPU_TOOLCHAIN_MANIFEST CONDA_PREFIX CC CXX HOST_COMPILER
ifneq ($(origin CUDA_PATH),undefined)
export CUDA_PATH
endif
MO_GPU_RESOLVER := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))mo_gpu_toolchain.py
MO_GPU_TOOLCHAIN_RECORD := $(shell python3 "$(MO_GPU_RESOLVER)" resolve --format make)
ifeq ($(strip $(MO_GPU_TOOLCHAIN_RECORD)),)
$(error GPU toolchain resolution failed; see diagnostic above)
endif
define MO_GPU_NEWLINE


endef
$(eval $(subst |,$(MO_GPU_NEWLINE),$(MO_GPU_TOOLCHAIN_RECORD)))
# Both thirdparties/CGo and NVCC's host compilation use the selected compilers.
ifeq ($(MO_GPU_PROVIDER),pixi)
override CC := $(MO_GPU_CC)
override CXX := $(MO_GPU_CXX)
override HOST_COMPILER := $(MO_GPU_CXX)
override NVCC := $(MO_GPU_NVCC) -ccbin $(MO_GPU_CXX)
export CC CXX
endif
export GPU_TOOLCHAIN_MANIFEST
endif
endif
