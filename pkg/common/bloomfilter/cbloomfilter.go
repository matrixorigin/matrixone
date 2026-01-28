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
	"math"
	"runtime"
	"sync/atomic"
	"unsafe"

	_ "github.com/matrixorigin/matrixone/cgo"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// CBloomFilter is a wrapper around the C implementation of a bloom filter.
type CBloomFilter struct {
	ptr    *C.bloomfilter_t
	refcnt int32
}

func computeMemAndHashCountC(rowCount int64, probability float64) (int64, int) {
	k := 1
	if rowCount < 10001 {
		k = 1
	} else if rowCount < 10_0001 {
		k = 1
	} else if rowCount < 100_0001 {
		k = 1
	} else if rowCount < 1000_0001 {
		k = 2
	} else if rowCount < 1_0000_0001 {
		k = 3
	} else if rowCount < 10_0000_0001 {
		k = 3
	} else if rowCount < 100_0000_0001 {
		k = 3
	} else {
		panic("unsupport rowCount")
	}
	k *= 3
	m_float := -float64(k) * float64(rowCount) / math.Log(1-math.Pow(probability, 1.0/float64(k)))
	m := int64(m_float)

	/*
		m_float = -float64(rowCount) * math.Log(probability) / math.Pow(math.Log(2), 2)
		m := uint64(math.Ceil(m_float))
		k_float := (float64(m) / float64(rowCount)) * math.Log2(2)
		k = int(math.Ceil(k_float))
	*/

	//fmt.Printf("m %d k %d\n", m, k)
	return m, k
}

// NewCBloomFilterWithProbaility creates a new CBloomFilter with optimal parameters
// derived from the expected number of elements (rowcnt) and the desired false positive probability.
func NewCBloomFilterWithProbability(rowcnt int64, probability float64) *CBloomFilter {
	if rowcnt <= 0 {
		rowcnt = 2
	}
	nbit, k := computeMemAndHashCountC(rowcnt, probability)
	//os.Stderr.WriteString(fmt.Sprintf("bloom k %d m %d\n", k, nbit))
	return NewCBloomFilter(uint64(nbit), uint32(k))
}

// NewCBloomFilter creates a new CBloomFilter with a specific number of bits (nbits)
// and number of hash functions (k).
func NewCBloomFilter(nbits uint64, k uint32) *CBloomFilter {
	ptr := C.bloomfilter_init(C.uint64_t(nbits), C.uint32_t(k))
	if ptr == nil {
		return nil
	}
	return &CBloomFilter{ptr: ptr, refcnt: 1}
}

func (bf *CBloomFilter) Ptr() *C.bloomfilter_t {
	return bf.ptr
}

// Free releases the C memory allocated for the bloom filter.
func (bf *CBloomFilter) Free() {
	if bf != nil && bf.ptr != nil {
		if atomic.AddInt32(&bf.refcnt, -1) == 0 {
			C.bloomfilter_free(bf.ptr)
			bf.ptr = nil
		}
	}
}

// Share the CBloomfilter and increment the reference counter
func (bf *CBloomFilter) SharePointer() *CBloomFilter {
	atomic.AddInt32(&bf.refcnt, 1)
	return bf
}

// Check CBloomFilter is Valid
func (bf *CBloomFilter) Valid() bool {
	return (bf != nil && bf.ptr != nil)
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
	atomic.StoreInt32(&bf.refcnt, 1)
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
func (bf *CBloomFilter) TestVector(v *vector.Vector, callBack func(bool, bool, int)) []uint8 {
	if bf == nil || bf.ptr == nil {
		return nil
	}

	if v.GetType().IsFixedLen() {
		return bf.testFixedVector(v, callBack)
	} else {
		return bf.testVarlenaVector(v, callBack)
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
	typeSize := v.GetType().TypeSize()
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
	C.bloomfilter_test_and_add_varlena(bf.ptr, unsafe.Pointer(&varlenData[0]), C.size_t(len(varlenData)*typeSize), C.size_t(typeSize), C.size_t(length), unsafe.Pointer(unsafe.SliceData(area)), C.size_t(len(area)), unsafe.Pointer(nullptr), C.size_t(nulllen), unsafe.Pointer(&results[0]))

	if callBack != nil {
		for j := 0; j < length; j++ {
			callBack(results[j] != 0, nulls.Contains(uint64(j)), j)
		}
	}
	runtime.KeepAlive(varlenData)
	runtime.KeepAlive(area)
	runtime.KeepAlive(nullptr)
}

func (bf *CBloomFilter) testFixedVector(v *vector.Vector, callBack func(bool, bool, int)) []uint8 {
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

	return results
}

func (bf *CBloomFilter) testVarlenaVector(v *vector.Vector, callBack func(bool, bool, int)) []uint8 {
	if v.Length() == 0 {
		return []uint8{}
	}
	varlenData := vector.MustFixedColWithTypeCheck[types.Varlena](v)
	typeSize := v.GetType().TypeSize()
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
	C.bloomfilter_test_varlena(bf.ptr, unsafe.Pointer(&varlenData[0]), C.size_t(len(varlenData)*typeSize), C.size_t(typeSize), C.size_t(length), unsafe.Pointer(unsafe.SliceData(area)), C.size_t(len(area)), unsafe.Pointer(nullptr), C.size_t(nulllen), unsafe.Pointer(&results[0]))

	if callBack != nil {
		for j := 0; j < length; j++ {
			callBack(results[j] != 0, nulls.Contains(uint64(j)), j)
		}
	}

	runtime.KeepAlive(varlenData)
	runtime.KeepAlive(area)
	runtime.KeepAlive(nullptr)

	return results
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
	typeSize := v.GetType().TypeSize()
	area := v.GetArea()
	nulls := v.GetNulls()
	nullbm := nulls.GetBitmap()

	var nullptr *uint64
	var nulllen int
	if nullbm != nil {
		nullptr = nullbm.Ptr()
		nulllen = nullbm.Size()
	}

	C.bloomfilter_add_varlena(bf.ptr, unsafe.Pointer(&varlenData[0]), C.size_t(len(varlenData)*typeSize), C.size_t(typeSize), C.size_t(v.Length()), unsafe.Pointer(unsafe.SliceData(area)), C.size_t(len(area)), unsafe.Pointer(nullptr), C.size_t(nulllen))
	runtime.KeepAlive(varlenData)
	runtime.KeepAlive(area)
	runtime.KeepAlive(nullptr)
}
