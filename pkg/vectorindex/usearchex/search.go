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

package usearchex

/*
#include <stdlib.h>
#include <string.h>
#include "../../../cgo/usearchex.h"
*/
import "C"
import (
	"runtime"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/bloomfilter"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	usearch "github.com/unum-cloud/usearch/golang"
)

func FilteredSearchUnsafeWithBloomFilter(
	index *usearch.Index,
	query unsafe.Pointer,
	limit uint,
	bf *bloomfilter.CBloomFilter,
) (keys []usearch.Key, distances []float32, err error) {
	var errorMessage *C.char

	if index.GetHandle() == nil {
		panic("index is uninitialized")
	}
	handle := C.usearch_index_t(index.GetHandle())

	var bfptr unsafe.Pointer
	if bf != nil {
		bfptr = unsafe.Pointer(bf.Ptr())
	}

	if query == nil {
		return nil, nil, moerr.NewInternalErrorNoCtx("query pointer cannot be nil")
	}

	if limit == 0 {
		return []usearch.Key{}, []float32{}, nil
	}

	keys = make([]usearch.Key, limit)
	distances = make([]float32, limit)

	resultCount := uint(C.usearchex_filtered_search_with_bloomfilter(
		handle,
		query,
		C.usearch_scalar_kind_t(index.GetConfig().Quantization.CValue()),
		C.size_t(limit),
		bfptr,
		(*C.usearch_key_t)(&keys[0]),
		(*C.usearch_distance_t)(&distances[0]),
		(*C.usearch_error_t)(&errorMessage)))

	if errorMessage != nil {
		return nil, nil, moerr.NewInternalErrorNoCtx(C.GoString(errorMessage))
	}

	keys = keys[:resultCount]
	distances = distances[:resultCount]
	return keys, distances, nil
}

func CreateBitSetFromInt64Vector(vec *vector.Vector) (*bitmap.Bitmap, error) {

	if vec.GetType().Oid != types.T_int64 {
		return nil, moerr.NewInternalErrorNoCtx("CreateBitSetFromInt64Vector: vector type is not int64")
	}

	var bm bitmap.Bitmap
	if vec.Length() > 0 {
		maxID := int64(0)
		for i := 0; i < vec.Length(); i++ {
			if vec.IsNull(uint64(i)) {
				continue
			}
			id := vector.GetFixedAtNoTypeCheck[int64](vec, i)
			if id > maxID {
				maxID = id
			}
		}

		// create bitmap
		bm.InitWithSize(maxID + 1)
		for i := 0; i < vec.Length(); i++ {
			if vec.IsNull(uint64(i)) {
				continue
			}
			id := vector.GetFixedAtNoTypeCheck[int64](vec, i)
			bm.Add(uint64(id))
		}
	}

	return &bm, nil
}

func FilteredSearchUnsafeWithBitmap(
	index *usearch.Index,
	query unsafe.Pointer,
	limit uint,
	bm *bitmap.Bitmap,
) (keys []usearch.Key, distances []float32, err error) {
	var errorMessage *C.char

	if index.GetHandle() == nil {
		panic("index is uninitialized")
	}
	handle := C.usearch_index_t(index.GetHandle())

	var bmptr *C.uint64_t
	var bmsize uint64

	if bm != nil {
		bmptr = (*C.uint64_t)(unsafe.Pointer(bm.Ptr()))
		bmsize = uint64(bm.Size() / 8) // number of uint64
	}

	if query == nil {
		return nil, nil, moerr.NewInternalErrorNoCtx("query pointer cannot be nil")
	}

	if limit == 0 {
		return []usearch.Key{}, []float32{}, nil
	}

	keys = make([]usearch.Key, limit)
	distances = make([]float32, limit)

	resultCount := uint(C.usearchex_filtered_search_with_bitmap(
		handle,
		query,
		C.usearch_scalar_kind_t(index.GetConfig().Quantization.CValue()),
		C.size_t(limit),
		bmptr,
		C.size_t(bmsize),
		(*C.usearch_key_t)(&keys[0]),
		(*C.usearch_distance_t)(&distances[0]),
		(*C.usearch_error_t)(&errorMessage)))

	if errorMessage != nil {
		return nil, nil, moerr.NewInternalErrorNoCtx(C.GoString(errorMessage))
	}

	runtime.KeepAlive(bm)
	keys = keys[:resultCount]
	distances = distances[:resultCount]
	return keys, distances, nil
}
