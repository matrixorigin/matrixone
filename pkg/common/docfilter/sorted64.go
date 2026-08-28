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
#include "../../../cgo/sorted64.h"
*/
import "C"

import (
	"encoding/binary"
	"runtime"
	"slices"
	"sort"
	"sync/atomic"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// TagSorted64 marks an exact, sorted uint64 membership set. The payload is
// [count uint64][count sorted uint64 values], all little-endian. Unlike
// CRoaring, this representation has no opaque C allocation: the transported
// payload is also the live probe structure. That makes memory linear and
// visible to Go while still allowing the C vector-search callback to binary
// search it directly.
const TagSorted64 byte = 4

// Sorted64Filter is an exact integer membership filter backed directly by its
// canonical serialized payload. Shares keep the payload alive until the last
// reader releases it; no per-reader reconstruction or C heap is required.
type Sorted64Filter struct {
	data          []byte
	refcnt        int32
	memoryRelease func()
}

var (
	_ MembershipFilter = (*Sorted64Filter)(nil)
	_ CFilter          = (*Sorted64Filter)(nil)
)

// BuildSorted64Bytes builds a canonical exact integer set. The returned slice
// is allocated as uint64 words, so its first byte is suitably aligned for the
// C search bridge; the wire format remains explicitly little-endian on every
// supported MO target.
func BuildSorted64Bytes(v *vector.Vector) ([]byte, error) {
	return buildSorted64Bytes(v, false)
}

// buildSorted64TaggedBytes builds the transport form without allocating a
// second payload-sized Go slice merely to prepend the tag.
func buildSorted64TaggedBytes(v *vector.Vector) ([]byte, error) {
	return buildSorted64Bytes(v, true)
}

func buildSorted64Bytes(v *vector.Vector, tagged bool) ([]byte, error) {
	if v == nil || !SupportsBitset(*v.GetType()) {
		return nil, moerr.NewInternalErrorNoCtx("sorted64: integer vector required")
	}

	physicalRows := v.Length()
	if v.IsConst() {
		if v.Length() == 0 || v.IsConstNull() {
			physicalRows = 0
		} else {
			physicalRows = 1
		}
	}
	valueOffset := 1
	if tagged {
		// Reserve one aligned word. After sorting, the canonical payload is
		// shifted left seven bytes in the same allocation to make room for the
		// one-byte tag.
		valueOffset = 2
	}
	nonNullCount := integerValueCountUpperBound(v)
	maxInt := uint64(^uint(0) >> 1)
	if nonNullCount > maxInt/8-uint64(valueOffset) {
		return nil, moerr.NewInternalErrorNoCtx(
			"sorted64: payload size overflow")
	}
	nonNullRows := int(nonNullCount)
	words := make([]uint64, nonNullRows+valueOffset)
	count := 0
	for i := 0; i < physicalRows; i++ {
		if v.IsNull(uint64(i)) {
			continue
		}
		count++
		words[valueOffset+count-1] = rawIntToUint64(v.GetRawBytesAt(i))
	}

	values := words[valueOffset : valueOffset+count]
	slices.Sort(values)
	unique := 0
	for _, value := range values {
		if unique == 0 || value != values[unique-1] {
			values[unique] = value
			unique++
		}
	}
	countWord := valueOffset - 1
	words[countWord] = uint64(unique)
	words = words[:valueOffset+unique]

	data := unsafe.Slice(
		(*byte)(unsafe.Pointer(unsafe.SliceData(words))),
		len(words)*int(unsafe.Sizeof(uint64(0))),
	)
	// The package has a compile-time little-endian guard in cbitmap.c. Keep the
	// format declaration local too, so a future portable-endian conversion does
	// not accidentally retain native-order words here.
	payloadOffset := countWord * 8
	if binary.LittleEndian.Uint64(data[payloadOffset:payloadOffset+8]) != uint64(unique) {
		return nil, moerr.NewInternalErrorNoCtx("sorted64: unsupported byte order")
	}
	if tagged {
		payloadBytes := (unique + 1) * 8
		copy(data[1:1+payloadBytes], data[8:8+payloadBytes])
		data[0] = TagSorted64
		data = data[:1+payloadBytes]
	} else {
		data = data[payloadOffset:]
	}
	runtime.KeepAlive(words)
	return data, nil
}

// NewSorted64Filter validates and aliases a canonical payload. The caller must
// keep using the returned filter (rather than mutating data); the filter itself
// owns the slice reference for its complete shared lifetime.
func NewSorted64Filter(data []byte) (*Sorted64Filter, error) {
	if len(data) < 8 || len(data)%8 != 0 {
		return nil, moerr.NewInternalErrorNoCtx("sorted64: invalid payload length")
	}
	count := binary.LittleEndian.Uint64(data[:8])
	if count > uint64((len(data)-8)/8) || 8+count*8 != uint64(len(data)) {
		return nil, moerr.NewInternalErrorNoCtx("sorted64: invalid cardinality")
	}
	for i := uint64(1); i < count; i++ {
		previous := binary.LittleEndian.Uint64(data[8+i*8-8 : 8+i*8])
		current := binary.LittleEndian.Uint64(data[8+i*8 : 8+i*8+8])
		if previous >= current {
			return nil, moerr.NewInternalErrorNoCtx("sorted64: values are not strictly ordered")
		}
	}
	return &Sorted64Filter{data: data, refcnt: 1}, nil
}

func (f *Sorted64Filter) cardinality() int {
	if f == nil || len(f.data) < 8 {
		return 0
	}
	return int(binary.LittleEndian.Uint64(f.data[:8]))
}

func (f *Sorted64Filter) valueAt(i int) uint64 {
	offset := 8 + i*8
	return binary.LittleEndian.Uint64(f.data[offset : offset+8])
}

func (f *Sorted64Filter) Test(data []byte) bool {
	if !f.Valid() {
		return false
	}
	value := rawIntToUint64(data)
	count := f.cardinality()
	idx := sort.Search(count, func(i int) bool { return f.valueAt(i) >= value })
	return idx < count && f.valueAt(idx) == value
}

func (f *Sorted64Filter) TestVector(v *vector.Vector, cb func(bool, bool, int)) []uint8 {
	if !f.Valid() {
		return nil
	}
	result := make([]uint8, v.Length())
	if len(result) > 0 {
		data, dataLen, elemsz, nitem, nullPtr, nullLen := vecFixedArgs(v)
		C.mo_sorted64_test_fixed(
			f.CHandle(), data, dataLen, elemsz, nitem, nullPtr, nullLen,
			unsafe.Pointer(unsafe.SliceData(result)),
		)
	}
	finalizeVecResults(v, result, cb)
	runtime.KeepAlive(f)
	runtime.KeepAlive(v)
	runtime.KeepAlive(result)
	return result
}

func (f *Sorted64Filter) Valid() bool {
	return f != nil && len(f.data) >= 8 && atomic.LoadInt32(&f.refcnt) > 0
}

func (f *Sorted64Filter) Exact() bool { return true }

func (f *Sorted64Filter) SharePointer() *Sorted64Filter {
	atomic.AddInt32(&f.refcnt, 1)
	return f
}

func (f *Sorted64Filter) Share() MembershipFilter { return f.SharePointer() }

func (f *Sorted64Filter) Free() {
	if f == nil {
		return
	}
	for {
		refs := atomic.LoadInt32(&f.refcnt)
		if refs <= 0 {
			return
		}
		if atomic.CompareAndSwapInt32(&f.refcnt, refs, refs-1) {
			if refs == 1 {
				f.data = nil
				if f.memoryRelease != nil {
					f.memoryRelease()
					f.memoryRelease = nil
				}
			}
			return
		}
	}
}

// CHandle points at the complete [count][values] payload. C only borrows it for
// the duration of one cgo call; FilteredSearchUnsafeWithMembership keeps f
// alive until that call returns.
func (f *Sorted64Filter) CHandle() unsafe.Pointer {
	if !f.Valid() {
		return nil
	}
	return unsafe.Pointer(unsafe.SliceData(f.data))
}

func (f *Sorted64Filter) CKind() byte { return TagSorted64 }
