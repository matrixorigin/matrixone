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
	usearch "github.com/unum-cloud/usearch/golang"
)

// FilteredSearchUnsafeWithMembership runs a usearch search that keeps only the
// candidate keys present in the doc_id membership filter f. f may be any
// docfilter.MembershipFilter (an exact cbitmap / CRoaring bitset, or a bloom
// filter); the C predicate tests each candidate key against f's underlying
// structure via its CHandle/CKind cgo bridge. A nil filter passes all keys.
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

	// Extract the filter's C handle + kind. A nil filter means "unfiltered" and
	// leaves fptr NULL (the C predicate keeps all). But a non-nil filter that is
	// invalid, of an unknown kind, or has a nil handle is an ERROR — silently
	// treating any of those as keep-all would skip intended filtering and return
	// extra rows.
	var fptr unsafe.Pointer
	var kind C.int
	if f != nil {
		if !f.Valid() {
			return nil, nil, moerr.NewInternalErrorNoCtx("usearchex: invalid (non-nil) membership filter")
		}
		// The C search predicate needs the underlying handle + kind, which live
		// on the C-bridge adapter contract (not the general probe interface).
		cf, ok := f.(docfilter.CFilter)
		if !ok {
			return nil, nil, moerr.NewInternalErrorNoCtx("usearchex: membership filter does not support the C search bridge")
		}
		switch cf.CKind() {
		case docfilter.TagBloom, docfilter.TagCRoaring, docfilter.TagCbitmap:
		default:
			return nil, nil, moerr.NewInternalErrorNoCtx("usearchex: unknown membership filter kind")
		}
		fptr = cf.CHandle()
		if fptr == nil {
			return nil, nil, moerr.NewInternalErrorNoCtx("usearchex: membership filter has nil handle")
		}
		kind = C.int(cf.CKind())
	}

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
	return keys, distances, nil
}
