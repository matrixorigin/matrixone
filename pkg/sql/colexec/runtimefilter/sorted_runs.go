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

package runtimefilter

import (
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"reflect"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/compare"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type mergeRun struct {
	vec *vector.Vector
	row int
}

// MarshalExactFilterRuns merges sorted, null-free runs directly into the
// existing IN-vector wire format. There is no concatenated temporary Vector.
// Scratch is O(number of runs); the only O(number of keys) allocation is the
// final wire payload. The single-run path is exactly the existing encoder.
func MarshalExactFilterRuns(ctx context.Context, runs []*vector.Vector, mp *mpool.MPool, account *mpool.AllocationAccount, owner mpool.AllocationOwner, site mpool.AllocationSite) (data []byte, release func(), err error) {
	if len(runs) == 1 {
		return MarshalExactFilterVector(runs[0], mp, account, owner, site)
	}
	if len(runs) == 0 || account == nil {
		return nil, nil, process.ErrExecutionResourceInvalid
	}
	typ := *runs[0].GetType()
	width := typ.TypeSize()
	var rows, areaBytes uint64
	for _, v := range runs {
		if v == nil || v.IsConst() || v.HasNull() || !typ.Eq(*v.GetType()) {
			return nil, nil, process.ErrExecutionResourceInvalid
		}
		rows += uint64(v.Length())
		if typ.IsVarlen() {
			for row := 0; row < v.Length(); row++ {
				n := len(v.GetRawBytesAt(row))
				if n > types.VarlenaInlineSize {
					areaBytes += uint64(n)
				}
			}
		}
	}
	dataBytes := rows * uint64(width)
	const header = 1 + types.TSize + 4 + 4
	total := uint64(header+4+4+1) + dataBytes + areaBytes
	if rows > uint64(^uint32(0)>>1) || total > uint64(mpool.MaxAllocationSize()) {
		return nil, nil, ErrOptionalKeySize
	}
	reservation, err := account.ReserveCapacity(uint64(len(runs))*uint64(reflect.TypeFor[mergeRun]().Size()), owner, site)
	if err != nil {
		return nil, nil, err
	}
	defer reservation.Abort()
	lease, err := reservation.Commit(reservation.UpperBound())
	if err != nil {
		return nil, nil, err
	}
	defer lease.Release()
	heap := make([]mergeRun, 0, len(runs))
	comparator := compare.New(typ, false, false)
	less := func(a, b mergeRun) bool {
		// IN wire vectors use InplaceSort's raw-byte string order and Go's
		// NaN-first floating order, not SQL collation/ORDER BY comparators.
		if typ.IsVarlen() {
			return bytes.Compare(a.vec.GetRawBytesAt(a.row), b.vec.GetRawBytesAt(b.row)) < 0
		}
		if typ.Oid == types.T_float32 {
			return cmp.Compare(vector.MustFixedColNoTypeCheck[float32](a.vec)[a.row], vector.MustFixedColNoTypeCheck[float32](b.vec)[b.row]) < 0
		}
		if typ.Oid == types.T_float64 {
			return cmp.Compare(vector.MustFixedColNoTypeCheck[float64](a.vec)[a.row], vector.MustFixedColNoTypeCheck[float64](b.vec)[b.row]) < 0
		}
		comparator.Set(0, a.vec)
		comparator.Set(1, b.vec)
		return comparator.Compare(0, 1, int64(a.row), int64(b.row)) < 0
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
	for _, v := range runs {
		if v.Length() != 0 {
			heap = append(heap, mergeRun{vec: v})
		}
	}
	for i := len(heap)/2 - 1; i >= 0; i-- {
		down(i)
	}
	buf, err := mpool.NewAccountedBuffer(mp, account, owner, site)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		if err != nil {
			buf.Free()
		}
	}()
	if err = buf.Resize(int(total)); err != nil {
		return nil, nil, MarkOptionalAllocationError(err)
	}
	out := buf.Bytes()
	out[0] = byte(vector.FLAT)
	copy(out[1:], types.EncodeType(&typ))
	binary.NativeEndian.PutUint32(out[1+types.TSize:], uint32(rows))
	binary.NativeEndian.PutUint32(out[1+types.TSize+4:], uint32(dataBytes))
	areaStart := header + int(dataBytes) + 4
	binary.NativeEndian.PutUint32(out[areaStart-4:], uint32(areaBytes))
	dataPos, areaPos := header, 0
	for row := uint64(0); row < rows; row++ {
		if row&8191 == 0 {
			if err = ctx.Err(); err != nil {
				return nil, nil, err
			}
		}
		run := &heap[0]
		if typ.IsVarlen() {
			value := run.vec.GetRawBytesAt(run.row)
			var descriptor types.Varlena
			if len(value) <= types.VarlenaInlineSize {
				descriptor[0] = byte(len(value))
				copy(descriptor[1:], value)
			} else {
				descriptor.SetOffsetLen(uint32(areaPos), uint32(len(value)))
				copy(out[areaStart+areaPos:], value)
				areaPos += len(value)
			}
			copy(out[dataPos:], descriptor[:])
		} else {
			copy(out[dataPos:dataPos+width], run.vec.GetData()[run.row*width:(run.row+1)*width])
		}
		dataPos += width
		run.row++
		if run.row == run.vec.Length() {
			heap[0] = heap[len(heap)-1]
			heap = heap[:len(heap)-1]
		}
		if len(heap) != 0 {
			down(0)
		}
	}
	// Empty null bitmap followed by the sorted flag, as in Vector.MarshalTo.
	binary.NativeEndian.PutUint32(out[areaStart+int(areaBytes):], 0)
	out[len(out)-1] = 1
	return out, buf.Free, nil
}
