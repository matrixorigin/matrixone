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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// CBloomFilter is a wrapper around the C implementation of a bloom filter.
type CBloomFilter struct {
	ptr *C.bloomfilter_t
}

// NewCBloomFilterWithProbaility creates a new CBloomFilter with optimal parameters
// derived from the expected number of elements (rowcnt) and the desired false positive probability.
func NewCBloomFilterWithProbaility(rowcnt int64, probability float64) *CBloomFilter {
	nbit, k := computeMemAndHashCount(rowcnt, probability)
	return NewCBloomFilter(uint64(nbit), uint32(k))
}

// NewCBloomFilter creates a new CBloomFilter with a specific number of bits (nbits)
// and number of hash functions (k).
func NewCBloomFilter(nbits uint64, k uint32) *CBloomFilter {
	ptr := C.bloomfilter_init(C.uint64_t(nbits), C.uint32_t(k))
	if ptr == nil {
		return nil
	}
	return &CBloomFilter{ptr: ptr}
}

// Free releases the C memory allocated for the bloom filter.
func (bf *CBloomFilter) Free() {
	if bf != nil && bf.ptr != nil {
		C.bloomfilter_free(bf.ptr)
		bf.ptr = nil
	}
}

// Add inserts a byte slice into the bloom filter.
func (bf *CBloomFilter) Add(data []byte) {
	if bf == nil || bf.ptr == nil || len(data) == 0 {
		return
	}
	C.bloomfilter_add(bf.ptr, unsafe.Pointer(&data[0]), C.size_t(len(data)))
	runtime.KeepAlive(data)
}

// Test checks if a byte slice is possibly in the bloom filter.
func (bf *CBloomFilter) Test(data []byte) bool {
	if bf == nil || bf.ptr == nil || len(data) == 0 {
		return false
	}
	return bool(C.bloomfilter_test(bf.ptr, unsafe.Pointer(&data[0]), C.size_t(len(data))))
}

// TestAndAdd checks if a byte slice is in the bloom filter and adds it if it's not.
func (bf *CBloomFilter) TestAndAdd(data []byte) bool {
	if bf == nil || bf.ptr == nil || len(data) == 0 {
		return false
	}
	return bool(C.bloomfilter_test_and_add(bf.ptr, unsafe.Pointer(&data[0]), C.size_t(len(data))))
}

// Marshal serializes the bloom filter into a byte slice.
func (bf *CBloomFilter) Marshal() ([]byte, error) {
	if bf == nil || bf.ptr == nil {
		return nil, nil
	}
	var clen C.size_t
	dataPtr := C.bloomfilter_marshal(bf.ptr, &clen)
	if dataPtr == nil {
		return nil, moerr.NewInternalErrorNoCtx("failed to marhsal CBloomFilter")
	}
	return C.GoBytes(unsafe.Pointer(dataPtr), C.int(clen)), nil
}

// Unmarshal reconstructs the bloom filter from a byte slice.
func (bf *CBloomFilter) Unmarshal(data []byte) error {
	if bf.ptr != nil {
		return moerr.NewInternalErrorNoCtx("CBloomFilter:Unmarshal ptr is not nil")
	}

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
		panic("failed to alloc memory for CBloomFilter")
	}
	bf.ptr = ptr
	return nil
}

// TestAndAddVector tests and adds all elements in the vector to the bloom filter.
// It invokes the callback function for all elements in the vector.
func (bf *CBloomFilter) TestAndAddVector(v *vector.Vector, callBack func(bool, bool, int)) {
	if bf == nil || bf.ptr == nil {
		return
	}

	isFixed := v.GetType().IsFixedLen()
	var fixedData []byte
	var typeSize int
	var varlenData []types.Varlena
	var area []byte

	if isFixed {
		fixedData = v.GetData()
		typeSize = v.GetType().TypeSize()
	} else {
		varlenData = vector.MustFixedColWithTypeCheck[types.Varlena](v)
		area = v.GetArea()
	}

	length := v.Length()
	const chunkSize = 256
	nulls := v.GetNulls()
	nullbm := nulls.GetBitmap()

	var nullptr *uint64
	var nulllen int
	if nullbm != nil {
		nullptr = nullbm.Ptr()
		nulllen = nullbm.Size()
	}

	if isFixed {
		results := make([]uint8, chunkSize)
		for i := 0; i < length; i += chunkSize {
			end := i + chunkSize
			if end > length {
				end = length
			}
			nitem := end - i
			buf := fixedData[i*typeSize : end*typeSize : end*typeSize]
			var curNullPtr unsafe.Pointer
			var curNullLen C.size_t
			if nullptr != nil {
				offset := (i / 64) * 8
				if offset < nulllen {
					curNullPtr = unsafe.Pointer(uintptr(unsafe.Pointer(nullptr)) + uintptr(offset))
					curNullLen = C.size_t(nulllen - offset)
				}
			}
			C.bloomfilter_test_and_add_fixed(bf.ptr, unsafe.Pointer(&buf[0]), C.size_t(len(buf)), C.size_t(typeSize), C.size_t(nitem), curNullPtr, curNullLen, unsafe.Pointer(&results[0]))
			runtime.KeepAlive(buf)
			if callBack != nil {
				for j := 0; j < nitem; j++ {
					callBack(results[j] != 0, nulls.Contains(uint64(i+j)), i+j)
				}
			}
		}
	} else {
		// Estimated size: length * (avg_data_len + overhead)
		// Overhead: size(4) + valid_flag(1)
		buf := make([]byte, 0, length*20)
		results := make([]uint8, chunkSize)

		for i := 0; i < length; i += chunkSize {
			end := i + chunkSize
			if end > length {
				end = length
			}
			nitem := end - i
			buf = buf[:0]

			for j := i; j < end; j++ {
				if nulls.Contains(uint64(j)) {
					// For nulls, we just need to advance the C pointer correctly.
					// We write size=0.
					// C code: uint32_t elemsz = *((uint32_t*)k); k += sizeof(uint32_t); ... k+= elemsz;
					// So if elemsz=0, k moves 4 bytes.
					buf = append(buf, 0, 0, 0, 0)
				} else {
					// Format: [size:4] [data...]
					data := varlenData[j].GetByteSlice(area)
					sz := uint32(len(data))
					buf = append(buf, byte(sz), byte(sz>>8), byte(sz>>16), byte(sz>>24))
					buf = append(buf, data...)
				}
			}

			var curNullPtr unsafe.Pointer
			var curNullLen C.size_t
			if nullptr != nil {
				offset := (i / 64) * 8
				if offset < nulllen {
					curNullPtr = unsafe.Pointer(uintptr(unsafe.Pointer(nullptr)) + uintptr(offset))
					curNullLen = C.size_t(nulllen - offset)
				}
			}

			C.bloomfilter_test_and_add_varlena(bf.ptr, unsafe.Pointer(&buf[0]), C.size_t(len(buf)), C.size_t(nitem), curNullPtr, curNullLen, unsafe.Pointer(&results[0]))

			if callBack != nil {
				for j := 0; j < nitem; j++ {
					callBack(results[j] != 0, nulls.Contains(uint64(i+j)), i+j)
				}
			}
		}
		runtime.KeepAlive(buf)
	}
	runtime.KeepAlive(nullptr)
}

// TestVector tests all elements in the vector against the bloom filter.
// It invokes the callback function for all elements in the vector.
func (bf *CBloomFilter) TestVector(v *vector.Vector, callBack func(bool, bool, int)) {
	if bf == nil || bf.ptr == nil {
		return
	}

	isFixed := v.GetType().IsFixedLen()
	var fixedData []byte
	var typeSize int
	var varlenData []types.Varlena
	var area []byte

	if isFixed {
		fixedData = v.GetData()
		typeSize = v.GetType().TypeSize()
	} else {
		varlenData = vector.MustFixedColWithTypeCheck[types.Varlena](v)
		area = v.GetArea()
	}

	length := v.Length()
	const chunkSize = 256
	nulls := v.GetNulls()
	nullbm := nulls.GetBitmap()

	var nullptr *uint64
	var nulllen int
	if nullbm != nil {
		nullptr = nullbm.Ptr()
		nulllen = nullbm.Size()
	}

	if isFixed {
		results := make([]uint8, chunkSize)
		for i := 0; i < length; i += chunkSize {
			end := i + chunkSize
			if end > length {
				end = length
			}
			nitem := end - i
			buf := fixedData[i*typeSize : end*typeSize : end*typeSize]

			var curNullPtr unsafe.Pointer
			var curNullLen C.size_t
			if nullptr != nil {
				offset := (i / 64) * 8
				if offset < nulllen {
					curNullPtr = unsafe.Pointer(uintptr(unsafe.Pointer(nullptr)) + uintptr(offset))
					curNullLen = C.size_t(nulllen - offset)
				}
			}
			C.bloomfilter_test_fixed(bf.ptr, unsafe.Pointer(&buf[0]), C.size_t(len(buf)), C.size_t(typeSize), C.size_t(nitem), curNullPtr, curNullLen, unsafe.Pointer(&results[0]))
			runtime.KeepAlive(buf)
			if callBack != nil {
				for j := 0; j < nitem; j++ {
					callBack(results[j] != 0, nulls.Contains(uint64(i+j)), i+j)
				}
			}
		}
	} else {
		// Estimated size
		buf := make([]byte, 0, length*20)
		results := make([]uint8, chunkSize)

		for i := 0; i < length; i += chunkSize {
			end := i + chunkSize
			if end > length {
				end = length
			}
			nitem := end - i
			buf = buf[:0]

			for j := i; j < end; j++ {
				if nulls.Contains(uint64(j)) {
					buf = append(buf, 0, 0, 0, 0)
				} else {
					data := varlenData[j].GetByteSlice(area)
					sz := uint32(len(data))
					buf = append(buf, byte(sz), byte(sz>>8), byte(sz>>16), byte(sz>>24))
					buf = append(buf, data...)
				}
			}

			var curNullPtr unsafe.Pointer
			var curNullLen C.size_t
			if nullptr != nil {
				offset := (i / 64) * 8
				if offset < nulllen {
					curNullPtr = unsafe.Pointer(uintptr(unsafe.Pointer(nullptr)) + uintptr(offset))
					curNullLen = C.size_t(nulllen - offset)
				}
			}

			C.bloomfilter_test_varlena(bf.ptr, unsafe.Pointer(&buf[0]), C.size_t(len(buf)), C.size_t(nitem), curNullPtr, curNullLen, unsafe.Pointer(&results[0]))

			if callBack != nil {
				for j := 0; j < nitem; j++ {
					callBack(results[j] != 0, nulls.Contains(uint64(i+j)), i+j)
				}
			}
		}
		runtime.KeepAlive(buf)
	}
	runtime.KeepAlive(nullptr)
}

// AddVector adds all elements in the vector to the bloom filter.
func (bf *CBloomFilter) AddVector(v *vector.Vector) {
	if bf == nil || bf.ptr == nil {
		return
	}

	isFixed := v.GetType().IsFixedLen()
	var fixedData []byte
	var typeSize int
	var varlenData []types.Varlena
	var area []byte

	if isFixed {
		fixedData = v.GetData()
		typeSize = v.GetType().TypeSize()
	} else {
		varlenData = vector.MustFixedColWithTypeCheck[types.Varlena](v)
		area = v.GetArea()
	}

	length := v.Length()
	const chunkSize = 256
	nulls := v.GetNulls()
	nullbm := nulls.GetBitmap()

	var nullptr *uint64
	var nulllen int
	if nullbm != nil {
		nullptr = nullbm.Ptr()
		nulllen = nullbm.Size()
	}

	if isFixed {
		for i := 0; i < length; i += chunkSize {
			end := i + chunkSize
			if end > length {
				end = length
			}
			nitem := end - i
			buf := fixedData[i*typeSize : end*typeSize : end*typeSize]

			var curNullPtr unsafe.Pointer
			var curNullLen C.size_t
			if nullptr != nil {
				offset := (i / 64) * 8
				if offset < nulllen {
					curNullPtr = unsafe.Pointer(uintptr(unsafe.Pointer(nullptr)) + uintptr(offset))
					curNullLen = C.size_t(nulllen - offset)
				}
			}
			C.bloomfilter_add_fixed(bf.ptr, unsafe.Pointer(&buf[0]), C.size_t(len(buf)), C.size_t(typeSize), C.size_t(nitem), curNullPtr, curNullLen)
			runtime.KeepAlive(buf)
		}
	} else {
		buf := make([]byte, 0, length*20)
		nulls := v.GetNulls()
		for i := 0; i < length; i += chunkSize {
			end := i + chunkSize
			if end > length {
				end = length
			}
			nitem := end - i
			buf = buf[:0]

			for j := i; j < end; j++ {
				if nulls.Contains(uint64(j)) {
					buf = append(buf, 0, 0, 0, 0)
				} else {
					data := varlenData[j].GetByteSlice(area)
					sz := uint32(len(data))
					buf = append(buf, byte(sz), byte(sz>>8), byte(sz>>16), byte(sz>>24))
					buf = append(buf, data...)
				}
			}

			var curNullPtr unsafe.Pointer
			var curNullLen C.size_t
			if nullptr != nil {
				offset := (i / 64) * 8
				if offset < nulllen {
					curNullPtr = unsafe.Pointer(uintptr(unsafe.Pointer(nullptr)) + uintptr(offset))
					curNullLen = C.size_t(nulllen - offset)
				}
			}

			C.bloomfilter_add_varlena(bf.ptr, unsafe.Pointer(&buf[0]), C.size_t(len(buf)), C.size_t(nitem), curNullPtr, curNullLen)
		}
		runtime.KeepAlive(buf)
	}
	runtime.KeepAlive(nullptr)
}
