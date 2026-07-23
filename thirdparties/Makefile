# Copyright 2021 - 2022 Matrix Origin
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

PWD=$(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
UNAME_S=$(shell uname -s | tr A-Z a-z)
UNAME_M=$(shell uname -m)
# Extracted third-party sources live in _-prefixed directories: go/build skips
# any directory whose name starts with _ or ., so `go list ./...` (and gopls,
# and `go test ./...`) never walk into them.
#
# Without the prefix, StringZilla-4.2.1/scripts/ breaks the top-level `config`
# target: it ships bench.go alongside bench_*.cpp, and a directory containing a
# .go file IS a package, so `go list` rejects its C++ sources with
#   C++ source files not allowed when not using cgo or SWIG: bench_container.cpp ...
# (a directory of .cpp with no .go is simply skipped, which is why this only
# bites on trees that carry Go bindings). The extracted tree exists only while
# this Makefile runs, so under `make -j` it fails a clean build and vanishes
# before anyone can look -- see the `config` target in ../Makefile.
USEARCH_DIR=_usearch_build
USEARCH_TAR=usearch-2.25.3.tar.gz
STRINGZILLA_DIR=_stringzilla
STRINGZILLA_TAR=StringZilla-4.2.1.tar.gz
SIMSIMD_DIR=_simsimd
SIMSIMD_TAR=SimSIMD-6.5.3.tar.gz
FP16_DIR=_fp16
FP16_TAR=fp16.tar.gz
XXHASH_DIR=xxHash-0.8.3
XXHASH_TAR=$(XXHASH_DIR).tar.gz
XXHASH_TMP=_xxhash_build
CROARING_DIR=CRoaring-4.7.0
CROARING_TAR=$(CROARING_DIR).tar.gz
CROARING_TMP=_croaring_build
CROARING_CFLAGS=-O3 -fPIC -std=c11

# Prebuilt onnxruntime shared library (CPU-only) from the MatrixOne thirdparty
# mirror. GPU (linux_x64_gpu_cuda12) is intentionally not fetched here.
# ONNX_LIB is the local filename our loader resolves; ONNX_SRC is the source path
# in the mirror (whose real files are versioned/symlinked).
#
# The download is pinned to an immutable mirror commit and verified against a
# per-platform SHA-256, so the same MatrixOne commit always produces the same
# native library and a changed mirror artifact fails the build instead of
# silently shipping different code.
ONNX_VERSION=1.26.0
ONNX_REPO_COMMIT=655365097e21c4df0f82337063f9a12b66700593
ONNX_BASE_URL=https://github.com/matrixorigin/thirdparty/raw/$(ONNX_REPO_COMMIT)/onnxrt/$(ONNX_VERSION)
ifeq ($(UNAME_S),darwin)
  ONNX_LIB=onnxruntime_arm64.dylib
  ONNX_SRC=osx_arm64/lib/libonnxruntime.$(ONNX_VERSION).dylib
  ONNX_SHA256=30afadcfc3c704f7671f8430d6252956651c1972373901d2be629da2e6a4d8ee
else ifeq ($(UNAME_M),x86_64)
  ONNX_LIB=onnxruntime.so
  ONNX_SRC=linux_x64/lib/libonnxruntime.so.$(ONNX_VERSION)
  ONNX_SHA256=5bd5bedf736fc501692435d0ec4f6e8b2bdf48cd30af8e6d00d61b3ddc9a7ab8
else
  ONNX_LIB=onnxruntime_arm64.so
  ONNX_SRC=linux_aarch64/lib/libonnxruntime.so.$(ONNX_VERSION)
  ONNX_SHA256=115ecb838e703d390262b8b4d07d5248e6693c67658d4c98c48f94905ab27af4
endif

all: init usearch xxhash croaring onnxruntime

init:
	mkdir -p install
	mkdir -p install/include install/lib

usearch: stringzilla fp16 simsimd install/include/usearch.h

stringzilla: install/include/stringzilla/stringzilla.h

simsimd: install/include/simsimd/simsimd.h

fp16: install/include/fp16.h

simsimd: install/include/simsimd/simsimd.h

xxhash: install/include/xxhash.h

install/include/xxhash.h:
	mkdir -p $(XXHASH_TMP)
	tar zxvf $(XXHASH_TAR) -C $(XXHASH_TMP) --strip-components=1
	cp $(XXHASH_TMP)/xxhash.h install/include
	rm -rf $(XXHASH_TMP)

install/include/fp16.h:
	rm -rf $(FP16_DIR)
	mkdir -p $(FP16_DIR)
	tar zxvf $(FP16_TAR) -C $(FP16_DIR) --strip-components=1
	cp -r $(FP16_DIR)/include/* install/include

install/include/simsimd/simsimd.h:
	rm -rf $(SIMSIMD_DIR)
	mkdir -p $(SIMSIMD_DIR)
	tar zxvf $(SIMSIMD_TAR) -C $(SIMSIMD_DIR) --strip-components=1
	cp -r $(SIMSIMD_DIR)/include/* install/include

install/include/stringzilla/stringzilla.h:
	rm -rf $(STRINGZILLA_DIR)
	mkdir -p $(STRINGZILLA_DIR)
	tar zxvf $(STRINGZILLA_TAR) -C $(STRINGZILLA_DIR) --strip-components=1
	cp -r $(STRINGZILLA_DIR)/include/* install/include

USEARCH_CMAKE_FLAG := -DUSEARCH_BUILD_BENCH_CPP=OFF -DUSEARCH_BUILD_TEST_C=OFF -DUSEARCH_BUILD_TEST_CPP=OFF -DUSEARCH_BUILD_LIB_C=ON -DCMAKE_CXX_FLAGS=-I$(PWD)/install/include -DCMAKE_POLICY_VERSION_MINIMUM=3.5
AVX512FP16 :=

ifeq ($(UNAME_S),linux)
  ifeq ($(MUSL),)
	USEARCH_CMAKE_FLAG += -DUSEARCH_USE_OPENMP=1
  endif
endif

ifeq ($(MO_CL_SIMSIMD),1)
  ifeq ($(MUSL),)
	USEARCH_CMAKE_FLAG += -DUSEARCH_USE_SIMSIMD=1 -DSIMSIMD_DYNAMIC_DISPATCH=1
	SIMSIMD_TARGET := HASWELL SKYLAKE ICE GENOA SAPPHIRE TURIN SIERRA NEON NEON_I8 NEON_F16 NEON_BF16 SVE SVE_I8 SVE_F16 SVE_BF16 SVE2
	USEARCH_CMAKE_FLAG += $(foreach target,$(SIMSIMD_TARGET),-DSIMSIMD_TARGET_$(target)=1)
  endif
endif

install/include/usearch.h:
	rm -rf $(USEARCH_DIR)
	mkdir -p $(USEARCH_DIR)
	tar zxvf $(USEARCH_TAR) -C $(USEARCH_DIR) --strip-components=1
	mkdir -p $(USEARCH_DIR)/fp16 $(USEARCH_DIR)/simsimd $(USEARCH_DIR)/stringzilla
	cp -r $(FP16_DIR)/* $(USEARCH_DIR)/fp16
	cp -r $(SIMSIMD_DIR)/* $(USEARCH_DIR)/simsimd
	cp -r $(STRINGZILLA_DIR)/* $(USEARCH_DIR)/stringzilla
	@if $(CC) -march=haswell -E - < /dev/null > /dev/null 2>&1; then \
		sed -i -e "s/march=native/march=haswell/g" $(USEARCH_DIR)/CMakeLists.txt; \
	fi
	(cd $(USEARCH_DIR) && cmake -B build $(USEARCH_CMAKE_FLAG) && cmake --build build --config Release)
ifeq ($(UNAME_S),darwin)
	cp $(USEARCH_DIR)/build/*.dylib install/lib
endif
ifeq ($(UNAME_S),linux)
	cp $(USEARCH_DIR)/build/*.so install/lib
endif
	cp $(USEARCH_DIR)/build/libusearch_static_c.a install/lib/libusearch_c.a
	cp $(USEARCH_DIR)/c/usearch.h install/include
	rm -rf $(FP16_DIR)
	rm -rf $(SIMSIMD_DIR)
	rm -rf $(STRINGZILLA_DIR)
	rm -rf $(USEARCH_DIR)

croaring: install/lib/libroaring.a

# Build CRoaring as a static lib from its single-file amalgamation, and install
# the amalgamated header. Used by the cgo doc_id filter (roaring64 bitset).
install/lib/libroaring.a:
	rm -rf $(CROARING_TMP)
	mkdir -p $(CROARING_TMP)
	tar zxf $(CROARING_TAR) -C $(CROARING_TMP) --strip-components=1
	(cd $(CROARING_TMP) && bash amalgamation.sh)
	$(CC) $(CROARING_CFLAGS) -I$(CROARING_TMP) -c $(CROARING_TMP)/roaring.c -o $(CROARING_TMP)/roaring.o
	ar rcs install/lib/libroaring.a $(CROARING_TMP)/roaring.o
	cp $(CROARING_TMP)/roaring.h install/include/roaring.h
	rm -rf $(CROARING_TMP)

# Download the prebuilt onnxruntime $(ONNX_VERSION) shared library (CPU) for the
# current platform, saved under the name our loader resolves:
#   linux amd64  -> onnxruntime.so
#   linux arm64  -> onnxruntime_arm64.so
#   macOS arm64  -> onnxruntime_arm64.dylib
onnxruntime: install/lib/$(ONNX_LIB)

# Download to a temp file, verify the pinned SHA-256, then move into place.
# sha256sum on linux, shasum -a 256 on macOS.
install/lib/$(ONNX_LIB): | init
	curl -fSL -o $@.tmp $(ONNX_BASE_URL)/$(ONNX_SRC)
	@echo "$(ONNX_SHA256)  $@.tmp" | (command -v sha256sum > /dev/null && sha256sum -c - || shasum -a 256 -c -) \
		|| { echo "onnxruntime checksum mismatch for $(ONNX_SRC)"; rm -f $@.tmp; exit 1; }
	mv $@.tmp $@

clean:
	rm -rf install $(USEARCH_DIR) $(STRINGZILLA_DIR) $(SIMSIMD_DIR) $(FP16_DIR) $(CROARING_TMP)

.PHONY: usearch stringzilla fp16 simsimd init croaring onnxruntime
