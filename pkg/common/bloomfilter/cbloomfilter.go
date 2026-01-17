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

func (bf *CBloomFilter) Ptr() *C.bloomfilter_t {
	return bf.ptr
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
	if bf == nil || bf.ptr == nil || data == nil {
		return
	}
	C.bloomfilter_add(bf.ptr, unsafe.Pointer(unsafe.SliceData(data)), C.size_t(len(data)))
	runtime.KeepAlive(data)
}

// Test checks if a byte slice is possibly in the bloom filter.
func (bf *CBloomFilter) Test(data []byte) bool {
	if bf == nil || bf.ptr == nil || data == nil {
		return false
	}
	return bool(C.bloomfilter_test(bf.ptr, unsafe.Pointer(unsafe.SliceData(data)), C.size_t(len(data))))
}

// TestAndAdd checks if a byte slice is in the bloom filter and adds it if it's not.
func (bf *CBloomFilter) TestAndAdd(data []byte) bool {
	if bf == nil || bf.ptr == nil || data == nil {
		return false
	}
	return bool(C.bloomfilter_test_and_add(bf.ptr, unsafe.Pointer(unsafe.SliceData(data)), C.size_t(len(data))))
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
		return moerr.NewInternalErrorNoCtx("Invalid bloomfilter data: empty slice")
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

	if v.GetType().IsFixedLen() {
		bf.testAndAddFixedVector(v, callBack)
	} else {
		bf.testAndAddVarlenaVector(v, callBack)
	}
}

// TestVector tests all elements in the vector against the bloom filter.
// It invokes the callback function for all elements in the vector.
func (bf *CBloomFilter) TestVector(v *vector.Vector, callBack func(bool, bool, int)) {
	if bf == nil || bf.ptr == nil {
		return
	}

	if v.GetType().IsFixedLen() {
		bf.testFixedVector(v, callBack)
	} else {
		bf.testVarlenaVector(v, callBack)
	}
}

// AddVector adds all elements in the vector to the bloom filter.
func (bf *CBloomFilter) AddVector(v *vector.Vector) {
	if bf == nil || bf.ptr == nil {
		return
	}

	if v.GetType().IsFixedLen() {
		bf.addFixedVector(v)
	} else {
		bf.addVarlenaVector(v)
	}
}

func (bf *CBloomFilter) testAndAddFixedVector(v *vector.Vector, callBack func(bool, bool, int)) {
	fixedData := v.GetData()
	typeSize := v.GetType().TypeSize()
	length := v.Length()
	nulls := v.GetNulls()
	nullbm := nulls.GetBitmap()

	var nullptr unsafe.Pointer
	var nulllen C.size_t
	if nullbm != nil {
		nullptr = unsafe.Pointer(nullbm.Ptr())
		nulllen = C.size_t(nullbm.Size())
	}

	results := make([]uint8, length)
	C.bloomfilter_test_and_add_fixed(bf.ptr, unsafe.Pointer(&fixedData[0]), C.size_t(len(fixedData)), C.size_t(typeSize), C.size_t(length), nullptr, nulllen, unsafe.Pointer(&results[0]))
	runtime.KeepAlive(fixedData)
	runtime.KeepAlive(nullptr)

	if callBack != nil {
		for j := 0; j < length; j++ {
			callBack(results[j] != 0, nulls.Contains(uint64(j)), j)
		}
	}
}

func (bf *CBloomFilter) testAndAddVarlenaVector(v *vector.Vector, callBack func(bool, bool, int)) {
	if v.Length() == 0 {
		return
	}
	varlenData := vector.MustFixedColWithTypeCheck[types.Varlena](v)
	area := v.GetArea()
	length := v.Length()
	nulls := v.GetNulls()
	nullbm := nulls.GetBitmap()

	var nullptr *uint64
	var nulllen int
	if nullbm != nil {
		nullptr = nullbm.Ptr()
		nulllen = nullbm.Size()
	}

	results := make([]uint8, length)
	C.bloomfilter_test_and_add_varlena(bf.ptr, unsafe.Pointer(&varlenData[0]), C.size_t(length), unsafe.Pointer(unsafe.SliceData(area)), C.size_t(len(area)), unsafe.Pointer(nullptr), C.size_t(nulllen), unsafe.Pointer(&results[0]))

	if callBack != nil {
		for j := 0; j < length; j++ {
			callBack(results[j] != 0, nulls.Contains(uint64(j)), j)
		}
	}
	runtime.KeepAlive(varlenData)
	runtime.KeepAlive(area)
	runtime.KeepAlive(nullptr)
}

func (bf *CBloomFilter) testFixedVector(v *vector.Vector, callBack func(bool, bool, int)) {
	fixedData := v.GetData()
	typeSize := v.GetType().TypeSize()
	length := v.Length()
	nulls := v.GetNulls()
	nullbm := nulls.GetBitmap()

	var nullptr unsafe.Pointer
	var nulllen C.size_t
	if nullbm != nil {
		nullptr = unsafe.Pointer(nullbm.Ptr())
		nulllen = C.size_t(nullbm.Size())
	}

	results := make([]uint8, length)
	C.bloomfilter_test_fixed(bf.ptr, unsafe.Pointer(&fixedData[0]), C.size_t(len(fixedData)), C.size_t(typeSize), C.size_t(length), nullptr, nulllen, unsafe.Pointer(&results[0]))
	runtime.KeepAlive(fixedData)
	runtime.KeepAlive(nullptr)

	if callBack != nil {
		for j := 0; j < length; j++ {
			callBack(results[j] != 0, nulls.Contains(uint64(j)), j)
		}
	}
}

func (bf *CBloomFilter) testVarlenaVector(v *vector.Vector, callBack func(bool, bool, int)) {
	if v.Length() == 0 {
		return
	}
	varlenData := vector.MustFixedColWithTypeCheck[types.Varlena](v)
	area := v.GetArea()
	length := v.Length()
	nulls := v.GetNulls()
	nullbm := nulls.GetBitmap()

	var nullptr *uint64
	var nulllen int
	if nullbm != nil {
		nullptr = nullbm.Ptr()
		nulllen = nullbm.Size()
	}

	results := make([]uint8, length)
	C.bloomfilter_test_varlena(bf.ptr, unsafe.Pointer(&varlenData[0]), C.size_t(length), unsafe.Pointer(unsafe.SliceData(area)), C.size_t(len(area)), unsafe.Pointer(nullptr), C.size_t(nulllen), unsafe.Pointer(&results[0]))

	if callBack != nil {
		for j := 0; j < length; j++ {
			callBack(results[j] != 0, nulls.Contains(uint64(j)), j)
		}
	}

	runtime.KeepAlive(varlenData)
	runtime.KeepAlive(area)
	runtime.KeepAlive(nullptr)
}

func (bf *CBloomFilter) addFixedVector(v *vector.Vector) {
	fixedData := v.GetData()
	typeSize := v.GetType().TypeSize()
	length := v.Length()
	nulls := v.GetNulls()
	nullbm := nulls.GetBitmap()

	var nullptr unsafe.Pointer
	var nulllen C.size_t
	if nullbm != nil {
		nullptr = unsafe.Pointer(nullbm.Ptr())
		nulllen = C.size_t(nullbm.Size())
	}

	C.bloomfilter_add_fixed(bf.ptr, unsafe.Pointer(&fixedData[0]), C.size_t(len(fixedData)), C.size_t(typeSize), C.size_t(length), nullptr, nulllen)
	runtime.KeepAlive(fixedData)
	runtime.KeepAlive(nullptr)
}
func (bf *CBloomFilter) addVarlenaVector(v *vector.Vector) {
	if v.Length() == 0 {
		return
	}
	varlenData := vector.MustFixedColWithTypeCheck[types.Varlena](v)
	area := v.GetArea()
	nulls := v.GetNulls()
	nullbm := nulls.GetBitmap()

	var nullptr *uint64
	var nulllen int
	if nullbm != nil {
		nullptr = nullbm.Ptr()
		nulllen = nullbm.Size()
	}

	C.bloomfilter_add_varlena(bf.ptr, unsafe.Pointer(&varlenData[0]), C.size_t(v.Length()), unsafe.Pointer(unsafe.SliceData(area)), C.size_t(len(area)), unsafe.Pointer(nullptr), C.size_t(nulllen))
	runtime.KeepAlive(varlenData)
	runtime.KeepAlive(area)
	runtime.KeepAlive(nullptr)
}
