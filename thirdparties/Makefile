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

PWD=$(shell pwd)
UNAME=$(shell uname -s | tr A-Z a-z)
USEARCH_DIR=usearch-2.16.10
USEARCH_TAR=$(USEARCH_DIR).tar.gz
STRINGZILLA_DIR=StringZilla-3.11.3
STRINGZILLA_TAR=$(STRINGZILLA_DIR).tar.gz
SIMSIMD_DIR=SimSIMD-6.2.3
SIMSIMD_TAR=$(SIMSIMD_DIR).tar.gz
FP16_DIR=fp16
FP16_TAR=$(FP16_DIR).tar.gz

all: init stringzilla fp16 simsimd usearch

init:
	mkdir -p install
	mkdir -p install/include install/lib

usearch: install/include/usearch.h

stringzilla: install/include/stringzilla/stringzilla.h

simsimd: install/include/simsimd/simsimd.h

fp16: install/include/fp16.h

simsimd: install/include/simsimd/simsimd.h

install/include/fp16.h:
	tar zxvf $(FP16_TAR)
	cp -r $(FP16_DIR)/include/* install/include

install/include/simsimd/simsimd.h:
	tar zxvf $(SIMSIMD_TAR)
	cp -r $(SIMSIMD_DIR)/include/* install/include

install/include/stringzilla/stringzilla.h:
	tar zxvf $(STRINGZILLA_TAR)
	cp -r $(STRINGZILLA_DIR)/include/* install/include

install/include/usearch.h:
	rm -rf $(USEARCH_DIR)
	tar zxvf $(USEARCH_TAR)
	(cd $(USEARCH_DIR) && mkdir build && cd build && cmake -DUSEARCH_BUILD_LIB_C=ON -DCMAKE_CXX_FLAGS=-I\ $(PWD)/install/include .. && make)
ifeq ($(UNAME),darwin)
	cp $(USEARCH_DIR)/build/*.dylib install/lib
endif
ifeq ($(UNAME),linux)
	cp $(USEARCH_DIR)/build/*.so install/lib
endif
	cp $(USEARCH_DIR)/c/usearch.h install/include

clean:
	rm -rf install $(USEARCH_DIR) $(STRINGZILLA_DIR) $(SIMSIMD_DIR) $(FP16_DIR)

.PHONY: usearch stringzilla fp16 simsimd init
