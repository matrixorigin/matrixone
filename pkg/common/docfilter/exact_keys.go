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

import (
	"bytes"
	"cmp"
	"context"
	"math"
	"reflect"
	"slices"
	"sort"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap/keycodec"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// exactKeys retains immutable key bytes and one globally sorted, chunked row
// directory. The directory avoids copying variable-width keys or creating a
// whole-build Vector. Two-level binary search probes one complete exact set.
type exactKeys struct {
	keys    *keycodec.KeyPayload
	views   []vector.Vector
	rows    [][]uint64
	typ     types.Type
	refs    atomic.Int32
	release func()
}

func (f *exactKeys) value(address uint64) []byte {
	return f.views[address>>13].GetRawBytesAt(int(address & 8191))
}

func (f *exactKeys) compare(left, right []byte) int {
	if f.typ.Oid.IsMySQLString() {
		return types.CompareStringValues(f.typ, left, right)
	}
	if f.typ.Oid == types.T_float32 {
		codec := keycodec.NewFloat32Codec(f.typ.Scale)
		return cmp.Compare(codec.CanonicalBits(types.DecodeFixed[float32](left)), codec.CanonicalBits(types.DecodeFixed[float32](right)))
	}
	if f.typ.Oid == types.T_float64 {
		return cmp.Compare(keycodec.CanonicalFloat64Bits(types.DecodeFixed[float64](left)), keycodec.CanonicalFloat64Bits(types.DecodeFixed[float64](right)))
	}
	return bytes.Compare(left, right)
}

func (f *exactKeys) notJoinable(value []byte) bool {
	return f.typ.Oid == types.T_float32 && math.IsNaN(float64(types.DecodeFixed[float32](value))) || f.typ.Oid == types.T_float64 && math.IsNaN(types.DecodeFixed[float64](value))
}

// NewExactKeys creates a host-reader filter for non-integer primary keys.
// Native integer consumers use NewForCKeys/BuildKeysWithMemoryAdmission instead.
// The payload must remain owned by the caller throughout construction.
func NewExactKeys(ctx context.Context, keys *keycodec.KeyPayload, typ types.Type, admission MemoryAdmission) (MembershipFilter, error) {
	// typeswitch:partial exact scalar runtime filters support float32/64, not the low-precision float family.
	if keys == nil || !types.MembershipKeyTypeCompatible(keys.Type().Oid, typ.Oid) || (!keycodec.SupportsExactRawRuntimeFilter(typ.Oid) && typ.Oid != types.T_float32 && typ.Oid != types.T_float64) {
		return nil, moerr.NewInvalidInputNoCtx("docfilter: unsupported exact key domain")
	}
	n, parts := keys.Rows(), uint64(keys.ChunkCount())
	rowParts := (n + 8191) / 8192
	// Include Go allocation rounding and both descriptor directories. Scratch
	// has its own lease and is released after the bounded-run merge completes.
	finalBytes := n*8 + parts*uint64(reflect.TypeFor[vector.Vector]().Size()) + rowParts*40 + 512
	scratchBytes := n*8 + parts*64 + 512
	if finalBytes > math.MaxInt64 || scratchBytes > math.MaxInt64 {
		return nil, moerr.NewInvalidInputNoCtx("docfilter: exact key size overflow")
	}
	release, err := acquireMemory(admission, int64(finalBytes))
	if err != nil {
		return nil, err
	}
	f := &exactKeys{keys: keys, typ: typ, release: release}
	if !keys.Retain() {
		if release != nil {
			release()
		}
		return nil, moerr.NewInvalidStateNoCtx("docfilter: exact keys were released")
	}
	f.refs.Store(1)
	complete := false
	defer func() {
		if !complete {
			f.Free()
		}
	}()
	scratchRelease, err := acquireMemory(admission, int64(scratchBytes))
	if err != nil {
		return nil, err
	}
	if scratchRelease != nil {
		defer scratchRelease()
	}
	f.views = make([]vector.Vector, keys.ChunkCount())
	runs := make([][]uint64, len(f.views))
	part := 0
	err = keys.VisitEncoded(ctx, func(data []byte) error {
		v := &f.views[part]
		if err := v.UnmarshalBinaryTrusted(data); err != nil {
			return err
		}
		run := make([]uint64, 0, v.Length())
		for row := 0; row < v.Length(); row++ {
			if !v.IsNull(uint64(row)) && !f.notJoinable(v.GetRawBytesAt(row)) {
				run = append(run, uint64(part)<<13|uint64(row))
			}
		}
		slices.SortFunc(run, func(a, b uint64) int { return f.compare(f.value(a), f.value(b)) })
		runs[part] = run
		part++
		return nil
	})
	if err != nil {
		return nil, err
	}
	positions := make([]int, len(runs))
	heap := make([]int, 0, len(runs))
	less := func(a, b int) bool {
		return f.compare(f.value(runs[a][positions[a]]), f.value(runs[b][positions[b]])) < 0
	}
	down := func(root int) {
		for {
			child := root*2 + 1
			if child >= len(heap) {
				return
			}
			if child+1 < len(heap) && less(heap[child+1], heap[child]) {
				child++
			}
			if !less(heap[child], heap[root]) {
				return
			}
			heap[root], heap[child] = heap[child], heap[root]
			root = child
		}
	}
	for i, run := range runs {
		if len(run) != 0 {
			heap = append(heap, i)
		}
	}
	for i := len(heap)/2 - 1; i >= 0; i-- {
		down(i)
	}
	f.rows = make([][]uint64, 0, int(rowParts))
	var tail []uint64
	for len(heap) != 0 {
		if len(tail) == cap(tail) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			tail = make([]uint64, 0, int(min(n, 8192)))
			f.rows = append(f.rows, tail)
		}
		i := heap[0]
		tail = append(tail, runs[i][positions[i]])
		f.rows[len(f.rows)-1] = tail
		n--
		positions[i]++
		if positions[i] == len(runs[i]) {
			runs[i] = nil
			heap[0] = heap[len(heap)-1]
			heap = heap[:len(heap)-1]
		}
		if len(heap) != 0 {
			down(0)
		}
	}
	complete = true
	return f, nil
}

func (f *exactKeys) Test(value []byte) bool {
	if !f.Valid() || f.notJoinable(value) {
		return false
	}
	i := sort.Search(len(f.rows), func(i int) bool { row := f.rows[i]; return f.compare(f.value(row[len(row)-1]), value) >= 0 })
	if i == len(f.rows) {
		return false
	}
	rows := f.rows[i]
	j := sort.Search(len(rows), func(j int) bool { return f.compare(f.value(rows[j]), value) >= 0 })
	return j < len(rows) && f.compare(f.value(rows[j]), value) == 0
}

func (f *exactKeys) TestVector(v *vector.Vector, cb func(bool, bool, int)) []uint8 {
	result := make([]uint8, v.Length())
	for row := 0; row < v.Length(); row++ {
		null := v.IsNull(uint64(row))
		found := !null && f.Test(v.GetRawBytesAt(row))
		if found {
			result[row] = 1
		}
		if cb != nil {
			cb(found, null, row)
		}
	}
	return result
}

func (f *exactKeys) Valid() bool { return f != nil && f.refs.Load() > 0 }
func (f *exactKeys) Exact() bool { return true }
func (f *exactKeys) Share() MembershipFilter {
	for {
		n := f.refs.Load()
		if n <= 0 {
			return nil
		}
		if f.refs.CompareAndSwap(n, n+1) {
			return f
		}
	}
}
func (f *exactKeys) Free() {
	if f == nil {
		return
	}
	for {
		n := f.refs.Load()
		if n <= 0 {
			return
		}
		if !f.refs.CompareAndSwap(n, n-1) {
			continue
		}
		if n == 1 {
			for i := range f.views {
				f.views[i].Free(nil)
			}
			f.views = nil
			f.rows = nil
			f.keys.Release()
			f.keys = nil
			if f.release != nil {
				f.release()
				f.release = nil
			}
		}
		return
	}
}
