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
	"context"
	"runtime"
	"unsafe"

	_ "github.com/matrixorigin/matrixone/cgo"
	"github.com/matrixorigin/matrixone/pkg/common/concurrent"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

type CBloomFilter struct {
	ptr *C.bloomfilter_t
}

func NewCBloomFilterWithProbaility(rowcnt int64, probability float64) *CBloomFilter {
	nbit, k := computeMemAndHashCount(rowcnt, probability)
	return NewCBloomFilter(uint64(nbit), uint32(k))
}

func NewCBloomFilter(nbits uint64, k uint32) *CBloomFilter {
	ptr := C.bloomfilter_init(C.uint64_t(nbits), C.uint32_t(k))
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

func (bf *CBloomFilter) TestAndAdd(data []byte) bool {
	if bf == nil || bf.ptr == nil || len(data) == 0 {
		return false
	}
	return bool(C.bloomfilter_test_and_add(bf.ptr, unsafe.Pointer(&data[0]), C.size_t(len(data))))
}

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

// test and add all element in the vector.Vector to the bloom filter
// call the callback function for all elements in the vector.Vector
// use concurrent.ThreadPoolExecutor to process the vector.Vector
func (bf *CBloomFilter) TestAndAddVector(v *vector.Vector, callBack func(bool, int)) {
	if bf == nil || bf.ptr == nil {
		return
	}
	executor := concurrent.NewThreadPoolExecutor(0)

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

	_ = executor.Execute(context.TODO(), v.Length(), func(ctx context.Context, thread_id int, start, end int) error {
		buf := make([]byte, 0, 64)
		if isFixed {
			if !v.GetNulls().Any() {
				for i := start; i < end; i++ {
					buf = buf[:0]
					buf = append(buf, 0)
					idx := i * typeSize
					buf = append(buf, fixedData[idx:idx+typeSize]...)
					found := C.bloomfilter_test_and_add(bf.ptr, unsafe.Pointer(&buf[0]), C.size_t(len(buf)))
					if callBack != nil {
						callBack(bool(found), i)
					}
				}
			} else {
				nulls := v.GetNulls()
				for i := start; i < end; i++ {
					buf = buf[:0]
					if nulls.Contains(uint64(i)) {
						buf = append(buf, 1)
					} else {
						buf = append(buf, 0)
						idx := i * typeSize
						buf = append(buf, fixedData[idx:idx+typeSize]...)
					}
					found := C.bloomfilter_test_and_add(bf.ptr, unsafe.Pointer(&buf[0]), C.size_t(len(buf)))
					if callBack != nil {
						callBack(bool(found), i)
					}
				}
			}
		} else {
			if !v.GetNulls().Any() {
				for i := start; i < end; i++ {
					buf = buf[:0]
					buf = append(buf, 0)
					buf = append(buf, varlenData[i].GetByteSlice(area)...)
					found := C.bloomfilter_test_and_add(bf.ptr, unsafe.Pointer(&buf[0]), C.size_t(len(buf)))
					if callBack != nil {
						callBack(bool(found), i)
					}
				}
			} else {
				nulls := v.GetNulls()
				for i := start; i < end; i++ {
					buf = buf[:0]
					if nulls.Contains(uint64(i)) {
						buf = append(buf, 1)
					} else {
						buf = append(buf, 0)
						buf = append(buf, varlenData[i].GetByteSlice(area)...)
					}
					found := C.bloomfilter_test_and_add(bf.ptr, unsafe.Pointer(&buf[0]), C.size_t(len(buf)))
					if callBack != nil {
						callBack(bool(found), i)
					}
				}
			}
		}
		runtime.KeepAlive(buf)
		return nil
	})
}

// test all element in the vector.Vector to the bloom filter
// call the callback function for all elements in the vector.Vector
// use concurrent.ThreadPoolExecutor to process the vector.Vector
func (bf *CBloomFilter) TestVector(v *vector.Vector, callBack func(bool, int)) {
	if bf == nil || bf.ptr == nil {
		return
	}
	executor := concurrent.NewThreadPoolExecutor(0)

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

	_ = executor.Execute(context.TODO(), v.Length(), func(ctx context.Context, thread_id int, start, end int) error {
		buf := make([]byte, 0, 64)
		if isFixed {
			if !v.GetNulls().Any() {
				for i := start; i < end; i++ {
					buf = buf[:0]
					buf = append(buf, 0)
					idx := i * typeSize
					buf = append(buf, fixedData[idx:idx+typeSize]...)
					found := C.bloomfilter_test(bf.ptr, unsafe.Pointer(&buf[0]), C.size_t(len(buf)))
					if callBack != nil {
						callBack(bool(found), i)
					}
				}
			} else {
				nulls := v.GetNulls()
				for i := start; i < end; i++ {
					buf = buf[:0]
					if nulls.Contains(uint64(i)) {
						buf = append(buf, 1)
					} else {
						buf = append(buf, 0)
						idx := i * typeSize
						buf = append(buf, fixedData[idx:idx+typeSize]...)
					}
					found := C.bloomfilter_test(bf.ptr, unsafe.Pointer(&buf[0]), C.size_t(len(buf)))
					if callBack != nil {
						callBack(bool(found), i)
					}
				}
			}
		} else {
			if !v.GetNulls().Any() {
				for i := start; i < end; i++ {
					buf = buf[:0]
					buf = append(buf, 0)
					buf = append(buf, varlenData[i].GetByteSlice(area)...)
					found := C.bloomfilter_test(bf.ptr, unsafe.Pointer(&buf[0]), C.size_t(len(buf)))
					if callBack != nil {
						callBack(bool(found), i)
					}
				}
			} else {
				nulls := v.GetNulls()
				for i := start; i < end; i++ {
					buf = buf[:0]
					if nulls.Contains(uint64(i)) {
						buf = append(buf, 1)
					} else {
						buf = append(buf, 0)
						buf = append(buf, varlenData[i].GetByteSlice(area)...)
					}
					found := C.bloomfilter_test(bf.ptr, unsafe.Pointer(&buf[0]), C.size_t(len(buf)))
					if callBack != nil {
						callBack(bool(found), i)
					}
				}
			}
		}
		runtime.KeepAlive(buf)
		return nil
	})
}

// add all element in the vector.Vector to the bloom filter
// use concurrent.ThreadPoolExecutor to process the vector.Vector
func (bf *CBloomFilter) AddVector(v *vector.Vector) {
	if bf == nil || bf.ptr == nil {
		return
	}
	executor := concurrent.NewThreadPoolExecutor(0)

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

	_ = executor.Execute(context.TODO(), v.Length(), func(ctx context.Context, thread_id int, start, end int) error {
		buf := make([]byte, 0, 64)
		if isFixed {
			if !v.GetNulls().Any() {
				for i := start; i < end; i++ {
					buf = buf[:0]
					buf = append(buf, 0)
					idx := i * typeSize
					buf = append(buf, fixedData[idx:idx+typeSize]...)
					C.bloomfilter_add(bf.ptr, unsafe.Pointer(&buf[0]), C.size_t(len(buf)))
				}
			} else {
				nulls := v.GetNulls()
				for i := start; i < end; i++ {
					buf = buf[:0]
					if nulls.Contains(uint64(i)) {
						buf = append(buf, 1)
					} else {
						buf = append(buf, 0)
						idx := i * typeSize
						buf = append(buf, fixedData[idx:idx+typeSize]...)
					}
					C.bloomfilter_add(bf.ptr, unsafe.Pointer(&buf[0]), C.size_t(len(buf)))
				}
			}
		} else {
			if !v.GetNulls().Any() {
				for i := start; i < end; i++ {
					buf = buf[:0]
					buf = append(buf, 0)
					buf = append(buf, varlenData[i].GetByteSlice(area)...)
					C.bloomfilter_add(bf.ptr, unsafe.Pointer(&buf[0]), C.size_t(len(buf)))
				}
			} else {
				nulls := v.GetNulls()
				for i := start; i < end; i++ {
					buf = buf[:0]
					if nulls.Contains(uint64(i)) {
						buf = append(buf, 1)
					} else {
						buf = append(buf, 0)
						buf = append(buf, varlenData[i].GetByteSlice(area)...)
					}
					C.bloomfilter_add(bf.ptr, unsafe.Pointer(&buf[0]), C.size_t(len(buf)))
				}
			}
		}
		runtime.KeepAlive(buf)
		return nil
	})
}
