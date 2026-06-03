// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package docfilter

/*
#include <stdlib.h>
#include "../../../cgo/croaring.h"
*/
import "C"

import (
	"sync/atomic"
	"unsafe"

	_ "github.com/matrixorigin/matrixone/cgo"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// TagCRoaring marks a payload serialized by the CRoaring (C, roaring64) filter,
// alongside docfilter.TagBitset (Go roaring) and TagBloom in the shared
// reader-side transport. CRoaring is the C-backed, compact, exact integer-PK
// filter (build/probe in C, one cgo call per vector).
const TagCRoaring byte = 2

// CRoaringFilter wraps a C roaring64_bitmap_t (via cgo/croaring) and
// implements engine.MembershipFilter. It uses CBloomFilter-style refcounting so the
// same C bitmap can be shared across parallel readers and freed exactly once.
type CRoaringFilter struct {
	ptr    unsafe.Pointer
	refcnt int32
}

// vecFixedArgs extracts the (data ptr, byte len, elemsz, nitem, nullmap ptr,
// nullmap len) that the C *_fixed entry points expect, mirroring
// cbloomfilter's fixed-vector path.
func vecFixedArgs(v *vector.Vector) (data unsafe.Pointer, dataLen C.size_t, elemsz C.size_t, nitem C.size_t, nullPtr unsafe.Pointer, nullLen C.size_t) {
	fixed := v.GetData()
	nitem = C.size_t(v.Length())
	elemsz = C.size_t(v.GetType().TypeSize())
	if len(fixed) > 0 {
		data = unsafe.Pointer(&fixed[0])
		dataLen = C.size_t(len(fixed))
	}
	if nb := v.GetNulls().GetBitmap(); nb != nil {
		nullPtr = unsafe.Pointer(nb.Ptr())
		nullLen = C.size_t(nb.Size())
	}
	return
}

// BuildCRoaringBytes builds a roaring64 bitset from an integer doc_id vector
// (read directly in C) and returns its portable serialization (no tag prefix).
func BuildCRoaringBytes(v *vector.Vector) ([]byte, error) {
	return buildCRoaringBytes(v, false)
}

// buildCRoaringBytes builds a roaring64 bitset; when runOpt is true it adds a
// run_optimize pass that converts consecutive runs to run-length containers —
// far smaller for clustered/contiguous id sets (e.g. a BETWEEN range or
// sequential PKs), at the cost of an extra pass over the containers.
func buildCRoaringBytes(v *vector.Vector, runOpt bool) ([]byte, error) {
	r := C.mo_croaring_create()
	if r == nil {
		return nil, moerr.NewInternalErrorNoCtx("croaring: create failed")
	}
	defer C.mo_croaring_free(r)

	data, dataLen, elemsz, nitem, nullPtr, nullLen := vecFixedArgs(v)
	if data != nil {
		C.mo_croaring_add_fixed(r, data, dataLen, elemsz, nitem, nullPtr, nullLen)
	}
	if runOpt {
		C.mo_croaring_run_optimize(r)
	}

	var clen C.size_t
	buf := C.mo_croaring_serialize(r, &clen)
	if buf == nil {
		return nil, moerr.NewInternalErrorNoCtx("croaring: serialize failed")
	}
	defer C.mo_croaring_free_buf(buf)
	return C.GoBytes(unsafe.Pointer(buf), C.int(clen)), nil
}

// NewCRoaringFilter deserializes a portable roaring64 payload (no tag prefix).
func NewCRoaringFilter(data []byte) (*CRoaringFilter, error) {
	if len(data) == 0 {
		return nil, moerr.NewInternalErrorNoCtx("croaring: empty payload")
	}
	ptr := C.mo_croaring_deserialize((*C.uint8_t)(unsafe.Pointer(&data[0])), C.size_t(len(data)))
	if ptr == nil {
		return nil, moerr.NewInternalErrorNoCtx("croaring: deserialize failed")
	}
	return &CRoaringFilter{ptr: ptr, refcnt: 1}, nil
}

// Test reports whether the raw fixed bytes of a single doc_id are present.
func (f *CRoaringFilter) Test(data []byte) bool {
	if f == nil || f.ptr == nil {
		return false
	}
	return bool(C.mo_croaring_contains(f.ptr, C.uint64_t(rawIntToUint64(data))))
}

// TestVector tests every row of an integer doc_id vector in one cgo call.
func (f *CRoaringFilter) TestVector(v *vector.Vector, cb func(bool, bool, int)) []uint8 {
	if f == nil || f.ptr == nil {
		return nil
	}
	length := v.Length()
	res := make([]uint8, length)
	if length == 0 {
		return res
	}
	data, dataLen, elemsz, nitem, nullPtr, nullLen := vecFixedArgs(v)
	if data != nil {
		C.mo_croaring_test_fixed(f.ptr, data, dataLen, elemsz, nitem, nullPtr, nullLen, unsafe.Pointer(&res[0]))
	}
	if cb != nil {
		nulls := v.GetNulls()
		for i := 0; i < length; i++ {
			cb(res[i] != 0, nulls.Contains(uint64(i)), i)
		}
	}
	return res
}

// Valid reports whether the filter is usable.
func (f *CRoaringFilter) Valid() bool {
	return f != nil && f.ptr != nil
}

// SharePointer increments the refcount and returns the same filter, so each
// parallel reader holds a share and the C bitmap is freed exactly once.
func (f *CRoaringFilter) SharePointer() *CRoaringFilter {
	atomic.AddInt32(&f.refcnt, 1)
	return f
}

// Share implements MembershipFilter (refcounted; returns the same C bitmap).
func (f *CRoaringFilter) Share() MembershipFilter {
	return f.SharePointer()
}

// Free drops one reference; the C bitmap is released when the last is freed.
func (f *CRoaringFilter) Free() {
	if f != nil && f.ptr != nil {
		if atomic.AddInt32(&f.refcnt, -1) == 0 {
			C.mo_croaring_free(f.ptr)
			f.ptr = nil
		}
	}
}

// Exact is true: a roaring bitset is an exact membership test (no false positives).
func (f *CRoaringFilter) Exact() bool { return true }

// CHandle returns the underlying C roaring64 handle for the cgo search bridge.
func (f *CRoaringFilter) CHandle() unsafe.Pointer { return f.ptr }

// CKind reports the roaring64 structure tag.
func (f *CRoaringFilter) CKind() byte { return TagCRoaring }
