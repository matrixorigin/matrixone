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

	"github.com/matrixorigin/matrixone/pkg/common/docfilter"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	usearch "github.com/unum-cloud/usearch/golang"
)

// FilteredSearchUnsafeWithMembership runs a usearch search that keeps only the
// candidate keys present in the doc_id membership filter f. f must be built in C
// memory (docfilter.CMembershipOf: cbitmap, CRoaring, C sorted64 or bloom); the C
// predicate tests each candidate key through its handle and kind. A nil filter
// passes all keys.
func FilteredSearchUnsafeWithMembership(
	index *usearch.Index,
	query unsafe.Pointer,
	limit uint,
	f docfilter.MembershipFilter,
) (keys []usearch.Key, distances []float32, err error) {
	var errorMessage *C.char

	if index.GetHandle() == nil {
		panic("index is uninitialized")
	}
	handle := C.usearch_index_t(index.GetHandle())

	if query == nil {
		return nil, nil, moerr.NewInternalErrorNoCtx("query pointer cannot be nil")
	}

	if limit == 0 {
		return []usearch.Key{}, []float32{}, nil
	}

	// The filter's C handle + kind. A nil filter means "unfiltered" and leaves the
	// handle NULL (the C predicate keeps all); an unusable non-nil filter is an error.
	membership, err := docfilter.CMembershipOf(f)
	if err != nil {
		return nil, nil, err
	}
	fptr, kind := membership.Handle, C.int(membership.Kind)

	keys = make([]usearch.Key, limit)
	distances = make([]float32, limit)

	resultCount := uint(C.usearchex_filtered_search_with_membership(
		handle,
		query,
		C.usearch_scalar_kind_t(index.GetConfig().Quantization.CValue()),
		C.size_t(limit),
		fptr,
		kind,
		(*C.usearch_key_t)(&keys[0]),
		(*C.usearch_distance_t)(&distances[0]),
		(*C.usearch_error_t)(&errorMessage)))

	if errorMessage != nil {
		return nil, nil, moerr.NewInternalErrorNoCtx(C.GoString(errorMessage))
	}

	runtime.KeepAlive(f)
	keys = keys[:resultCount]
	distances = distances[:resultCount]
	if err := metric.CheckFiniteDists(distances, "vector index search"); err != nil {
		return nil, nil, err
	}
	return keys, distances, nil
}
