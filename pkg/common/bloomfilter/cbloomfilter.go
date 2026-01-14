// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bloomfilter

/*
#include <stdlib.h>
#include <string.h>
#include "../../../cgo/bloom.h"
*/
import "C"
import (
	"runtime"
	"unsafe"

	_ "github.com/matrixorigin/matrixone/cgo"
)

type CBloomFilter struct {
	ptr *C.bloomfilter_t
}

func NewCBloomFilter(nbits uint64) *CBloomFilter {
	ptr := C.bloomfilter_init(C.uint64_t(nbits))
	if ptr == nil {
		return nil
	}
	return &CBloomFilter{ptr: ptr}
}

func (bf *CBloomFilter) Free() {
	if bf != nil && bf.ptr != nil {
		C.bloomfilter_free(bf.ptr)
		bf.ptr = nil
	}
}

func (bf *CBloomFilter) Add(data []byte) {
	if bf == nil || bf.ptr == nil || len(data) == 0 {
		return
	}
	C.bloomfilter_add(bf.ptr, unsafe.Pointer(&data[0]), C.size_t(len(data)))
	runtime.KeepAlive(data)
}

func (bf *CBloomFilter) Test(data []byte) bool {
	if bf == nil || bf.ptr == nil || len(data) == 0 {
		return false
	}
	return bool(C.bloomfilter_test(bf.ptr, unsafe.Pointer(&data[0]), C.size_t(len(data))))
}

func (bf *CBloomFilter) Marshal() []byte {
	if bf == nil || bf.ptr == nil {
		return nil
	}
	var clen C.size_t
	dataPtr := C.bloomfilter_marshal(bf.ptr, &clen)
	if dataPtr == nil {
		return nil
	}
	return C.GoBytes(unsafe.Pointer(dataPtr), C.int(clen))
}

func UnmarshalCBloomFilter(data []byte) *CBloomFilter {
	if len(data) == 0 {
		return nil
	}
	// Allocate C memory and copy data to it, because bloomfilter_unmarshal
	// just casts the pointer and we want a stable C allocation that we can free.
	cData := C.malloc(C.size_t(len(data)))
	C.memcpy(cData, unsafe.Pointer(&data[0]), C.size_t(len(data)))
	runtime.KeepAlive(data)

	ptr := C.bloomfilter_unmarshal((*C.uint8_t)(cData), C.size_t(len(data)))
	if ptr == nil {
		C.free(cData)
		return nil
	}
	return &CBloomFilter{ptr: ptr}
}
