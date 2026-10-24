// Copyright 2026 Matrix Origin
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

package docfilter

/*
#include <stdlib.h>
#include "../../../cgo/cbitmap.h"
#include "../../../cgo/croaring.h"
#include "../../../cgo/sorted64.h"
*/
import "C"

import (
	"math"
	"sync/atomic"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// Bytes a C-built roaring64 may allocate: per container (struct, array header, ART
// nodes, allocator overhead; measured at most 96) and per key (2 in an array
// container, doubled for array growth while building).
const (
	croaringContainerBytes = uint64(128)
	croaringKeyBytes       = uint64(4)
	croaringFixedBytes     = uint64(1 << 10)
	croaringContainerBits  = 16
)

// NewForC builds the exact membership of an integer key vector in C memory, for a
// consumer that tests it from C or C++ (usearch, cuVS). A cbitmap when its span
// bitmap is eligible, else whichever of a CRoaring bitmap and a C sorted64 array is
// estimated smaller. The estimate is reserved with admission before the build and
// released when the filter is freed.
func NewForC(v *vector.Vector, admission MemoryAdmission) (MembershipFilter, error) {
	rng, ok := integerValueRange(v)
	if !ok {
		return nil, moerr.NewInternalErrorNoCtx("docfilter: C membership needs integer keys")
	}
	data, dataLen, elemsz, nitem, nullPtr, nullLen := vecFixedArgs(v)

	base := uint64(0)
	if CbitmapUseOffset {
		base = rng.min
	}
	if bitCap := cbitmapBitCapForCount(rng.count); bitCap != 0 && rng.count != 0 && rng.max-base < bitCap {
		span := rng.max - base + 1
		release, err := acquireMemory(admission, int64(3*8+((span+63)/64)*8))
		if err != nil {
			return nil, err
		}
		off := C.int(0)
		if CbitmapUseOffset {
			off = 1
		}
		var f unsafe.Pointer
		if status := C.mo_cbitmap_build_fixed(data, dataLen, elemsz, nitem, nullPtr, nullLen,
			C.uint64_t(bitCap), off, &f); status != C.MO_CBITMAP_OK {
			if release != nil {
				release()
			}
			return nil, moerr.NewInternalErrorNoCtxf("cbitmap: build failed with status %d", int(status))
		}
		return &CbitmapFilter{ptr: f, refcnt: 1, memoryRelease: release}, nil
	}

	roaringBytes, roaringOK := croaringBuildUpperBound(rng)
	sortedBytes, sortedOK := csorted64BuildUpperBound(uint64(nitem))
	if !sortedOK {
		return nil, moerr.NewInternalErrorNoCtx("docfilter: membership size overflow")
	}
	if roaringOK && roaringBytes < sortedBytes {
		release, err := acquireMemory(admission, roaringBytes)
		if err != nil {
			return nil, err
		}
		r := C.mo_croaring_create()
		if r == nil || (data != nil && !bool(C.mo_croaring_add_fixed(r, data, dataLen, elemsz, nitem, nullPtr, nullLen))) {
			if r != nil {
				C.mo_croaring_free(r)
			}
			if release != nil {
				release()
			}
			return nil, moerr.NewInternalErrorNoCtx("croaring: build failed")
		}
		C.mo_croaring_run_optimize(r)
		return &CRoaringFilter{ptr: r, refcnt: 1, memoryRelease: release}, nil
	}
	release, err := acquireMemory(admission, sortedBytes)
	if err != nil {
		return nil, err
	}
	p := C.mo_sorted64_build_fixed(data, dataLen, elemsz, nitem, nullPtr, nullLen)
	if p == nil {
		if release != nil {
			release()
		}
		return nil, moerr.NewInternalErrorNoCtx("sorted64: build failed")
	}
	return &CSorted64Filter{ptr: p, refcnt: 1, memoryRelease: release}, nil
}

// croaringBuildUpperBound bounds the C memory of a roaring64 over rng: containers
// are the 2^16-value chunks between min and max, at most one per key.
func croaringBuildUpperBound(rng integerRange) (int64, bool) {
	containers := rng.count
	if rng.count != 0 {
		containers = min(containers, (rng.max>>croaringContainerBits)-(rng.min>>croaringContainerBits)+1)
	}
	if containers > (math.MaxInt64/2)/croaringContainerBytes || rng.count > (math.MaxInt64/2)/croaringKeyBytes {
		return 0, false
	}
	return int64(containers*croaringContainerBytes + rng.count*croaringKeyBytes + croaringFixedBytes), true
}

// csorted64BuildUpperBound is the C allocation of mo_sorted64_build_fixed over rows
// physical rows: the count word and one word per row.
func csorted64BuildUpperBound(rows uint64) (int64, bool) {
	if rows > uint64(math.MaxInt64/8)-1 {
		return 0, false
	}
	return int64((rows + 1) * 8), true
}

var _ CFilter = (*CSorted64Filter)(nil)

// CSorted64Filter is a sorted64 membership whose [count][sorted uint64 keys] payload
// is C memory (mo_sorted64_build_fixed), for C and C++ consumers. It is refcounted
// like CbitmapFilter; the payload and its admission are released by the last Free.
type CSorted64Filter struct {
	ptr           *C.uint64_t
	refcnt        int32
	memoryRelease func()
}

// Test reports whether the raw fixed bytes of a single key are present.
func (f *CSorted64Filter) Test(data []byte) bool {
	if f == nil || f.ptr == nil {
		return false
	}
	return bool(C.mo_sorted64_contains(unsafe.Pointer(f.ptr), C.uint64_t(rawIntToUint64(data))))
}

// TestVector tests every row of an integer key vector in one cgo call.
func (f *CSorted64Filter) TestVector(v *vector.Vector, cb func(bool, bool, int)) []uint8 {
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
		C.mo_sorted64_test_fixed(unsafe.Pointer(f.ptr), data, dataLen, elemsz, nitem, nullPtr, nullLen, unsafe.Pointer(&res[0]))
	}
	finalizeVecResults(v, res, cb)
	return res
}

// Valid reports whether the filter is usable.
func (f *CSorted64Filter) Valid() bool { return f != nil && f.ptr != nil }

// Share implements MembershipFilter (refcounted; returns the same C payload).
func (f *CSorted64Filter) Share() MembershipFilter {
	atomic.AddInt32(&f.refcnt, 1)
	return f
}

// Free drops one reference; the C payload is released when the last is freed.
func (f *CSorted64Filter) Free() {
	if f != nil && f.ptr != nil {
		if atomic.AddInt32(&f.refcnt, -1) == 0 {
			C.mo_sorted64_free(f.ptr)
			f.ptr = nil
			if f.memoryRelease != nil {
				f.memoryRelease()
				f.memoryRelease = nil
			}
		}
	}
}

// Exact is true: a sorted key set has no false positives.
func (f *CSorted64Filter) Exact() bool { return true }

// CHandle returns the C payload for the cgo search bridge.
func (f *CSorted64Filter) CHandle() unsafe.Pointer { return unsafe.Pointer(f.ptr) }

// CKind reports the sorted64 structure tag.
func (f *CSorted64Filter) CKind() byte { return TagSorted64 }
