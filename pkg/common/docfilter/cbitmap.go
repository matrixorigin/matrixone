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
#include "../../../cgo/cbitmap.h"
*/
import "C"

import (
	"sync/atomic"
	"unsafe"

	_ "github.com/matrixorigin/matrixone/cgo"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// TagCbitmap marks a payload serialized by the dense cbitmap filter, alongside
// TagBloom / TagBitset / TagCRoaring in the shared reader-side transport.
const TagCbitmap byte = 3

// MaxCbitmapBits caps the dense bitset size. A dense bitmap is indexed by the
// doc_id value, so its size is O(max value), not O(count) — only viable when
// the max id is bounded. Above this the caller falls back to CRoaring.
//
// 2^23 bits = 1 MB, sized to stay within a typical per-core L2 cache so the
// random per-row membership probe hits L2 rather than L3/DRAM, and to bound
// per-query memory under concurrency (N concurrent queries * up to 1 MB).
// Covers dense integer PKs up to ~8.4M; sparser/larger id ranges fall back to
// the compact CRoaring bitset.
const MaxCbitmapBits = uint64(1) << 23

// CbitmapFeasible reports whether a max doc_id value is small enough that a
// dense cbitmap is worthwhile (vs the compact CRoaring filter). This is the
// value-indexed (offset-off) bound; with CbitmapUseOffset the actual build
// gates on the value SPAN (max-min), which is never larger.
func CbitmapFeasible(maxVal uint64) bool {
	// Written as maxVal < MaxCbitmapBits (not maxVal+1 <= MaxCbitmapBits) so that
	// maxVal == math.MaxUint64 — what a negative/maxed integer PK zero-extends to
	// — does not wrap maxVal+1 to 0 and wrongly report a 2^64-wide range as
	// feasible. Equivalent to "nbits = maxVal+1 fits within MaxCbitmapBits".
	return maxVal < MaxCbitmapBits
}

// BuildIntegerFilter builds the best exact filter for an integer doc_id
// vector and returns the tag byte to prepend + the serialized payload: a dense
// cbitmap when the id range is bounded (fastest), else a compact CRoaring
// bitset (sparse-safe). The reader picks the structure from the tag.
func BuildIntegerFilter(v *vector.Vector) (byte, []byte, error) {
	if data, ok, err := BuildCbitmapBytes(v); err != nil {
		return 0, nil, err
	} else if ok {
		return TagCbitmap, data, nil
	}
	data, err := BuildCRoaringBytes(v)
	if err != nil {
		return 0, nil, err
	}
	return TagCRoaring, data, nil
}

// CbitmapFilter wraps a C dense bitset (cgo/cbitmap) and implements
// engine.MembershipFilter. Build and probe run entirely in C over the raw column
// buffer (one cgo call per vector), and it uses CRoaring-style refcounting so
// the same C bitset can be shared across parallel readers and freed once. It is
// the fastest exact filter for dense, bounded integer doc_ids.
type CbitmapFilter struct {
	ptr    unsafe.Pointer
	refcnt int32
}

// cbitmapSerialize serializes a C bitset handle to bytes (no tag prefix).
func cbitmapSerialize(f unsafe.Pointer) ([]byte, error) {
	var clen C.size_t
	buf := C.mo_cbitmap_serialize(f, &clen)
	if buf == nil {
		return nil, moerr.NewInternalErrorNoCtx("cbitmap: serialize failed")
	}
	defer C.mo_cbitmap_free_buf(buf)
	return C.GoBytes(unsafe.Pointer(buf), C.int(clen)), nil
}

// CbitmapUseOffset, when true, bases the dense bitset at min(values) so its size
// is the value SPAN (max-min) rather than the max value. This lets high-but-
// narrow id sets — recent rows of a large table, BETWEEN ranges, or
// signed/negative PKs (which zero-extend to huge uint64) — stay within
// MaxCbitmapBits instead of falling back to CRoaring. On by default: it strictly
// shrinks the bitset (size = span, never larger than the value-indexed layout)
// at no probe cost (BenchmarkTestVectorOffset: identical probe time), so a
// bounded-span set uses the fast dense path even when its absolute ids are
// large. The base is carried in the serialized payload, so the reader is
// agnostic to this flag (a payload built with it on is probed correctly either
// way).
var CbitmapUseOffset = true

// BuildCbitmapBytes builds a dense bitset from an integer doc_id vector (read
// directly in C) and returns its serialization (no tag). ok=false means the id
// range is too large for a dense bitmap (caller should use CRoaring instead).
func BuildCbitmapBytes(v *vector.Vector) (data []byte, ok bool, err error) {
	return buildCbitmapBytesCap(v, MaxCbitmapBits, CbitmapUseOffset)
}

// buildCbitmapBytesCap is BuildCbitmapBytes with an explicit bit cap and offset
// flag. Production callers use BuildCbitmapBytes (MaxCbitmapBits +
// CbitmapUseOffset); benchmarks pass these explicitly to measure each variant.
func buildCbitmapBytesCap(v *vector.Vector, maxBits uint64, useOffset bool) (data []byte, ok bool, err error) {
	cdata, dataLen, elemsz, nitem, nullPtr, nullLen := vecFixedArgs(v)
	off := C.int(0)
	if useOffset {
		off = 1
	}
	var f unsafe.Pointer
	status := C.mo_cbitmap_build_fixed(cdata, dataLen, elemsz, nitem, nullPtr, nullLen,
		C.uint64_t(maxBits), off, &f)
	switch status {
	case C.MO_CBITMAP_RANGE_TOO_LARGE:
		// The ONLY status that means "fall back to CRoaring": ok=false, no error.
		return nil, false, nil
	case C.MO_CBITMAP_OOM:
		return nil, false, moerr.NewInternalErrorNoCtx("cbitmap: build out of memory")
	case C.MO_CBITMAP_OK:
		// proceed below
	default: // MO_CBITMAP_INVALID_INPUT or any unexpected status
		return nil, false, moerr.NewInternalErrorNoCtx("cbitmap: invalid build input")
	}
	defer C.mo_cbitmap_free(f)
	b, err := cbitmapSerialize(f)
	if err != nil {
		return nil, false, err
	}
	return b, true, nil
}

// NewCbitmapFilter deserializes a dense bitset payload (no tag prefix).
func NewCbitmapFilter(data []byte) (*CbitmapFilter, error) {
	if len(data) == 0 {
		return nil, moerr.NewInternalErrorNoCtx("cbitmap: empty payload")
	}
	ptr := C.mo_cbitmap_deserialize((*C.uint8_t)(unsafe.Pointer(&data[0])), C.size_t(len(data)))
	if ptr == nil {
		return nil, moerr.NewInternalErrorNoCtx("cbitmap: deserialize failed")
	}
	return &CbitmapFilter{ptr: ptr, refcnt: 1}, nil
}

// Test reports whether the raw fixed bytes of a single doc_id are present.
func (f *CbitmapFilter) Test(data []byte) bool {
	if f == nil || f.ptr == nil {
		return false
	}
	return bool(C.mo_cbitmap_contain(f.ptr, C.uint64_t(rawIntToUint64(data))))
}

// TestVector tests every row of an integer doc_id vector in one cgo call.
func (f *CbitmapFilter) TestVector(v *vector.Vector, cb func(bool, bool, int)) []uint8 {
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
		C.mo_cbitmap_test_fixed(f.ptr, data, dataLen, elemsz, nitem, nullPtr, nullLen, unsafe.Pointer(&res[0]))
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
func (f *CbitmapFilter) Valid() bool {
	return f != nil && f.ptr != nil
}

// SharePointer increments the refcount and returns the same filter, so each
// parallel reader holds a share and the C bitset is freed exactly once.
func (f *CbitmapFilter) SharePointer() *CbitmapFilter {
	atomic.AddInt32(&f.refcnt, 1)
	return f
}

// Share implements MembershipFilter (refcounted; returns the same C bitset).
func (f *CbitmapFilter) Share() MembershipFilter {
	return f.SharePointer()
}

// Free drops one reference; the C bitset is released when the last is freed.
func (f *CbitmapFilter) Free() {
	if f != nil && f.ptr != nil {
		if atomic.AddInt32(&f.refcnt, -1) == 0 {
			C.mo_cbitmap_free(f.ptr)
			f.ptr = nil
		}
	}
}

// Exact is true: a dense bitset is an exact membership test (no false positives).
func (f *CbitmapFilter) Exact() bool { return true }

// CHandle returns the underlying C dense-bitset handle for the cgo search bridge.
func (f *CbitmapFilter) CHandle() unsafe.Pointer { return f.ptr }

// CKind reports the dense-bitset structure tag.
func (f *CbitmapFilter) CKind() byte { return TagCbitmap }
