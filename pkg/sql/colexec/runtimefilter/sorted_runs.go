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
	"slices"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/compare"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type mergeRun struct {
	vec      *vector.Vector
	row      int
	key      []byte
	areaBase uint32
}

// ExactFilterDataSorter selects the native IN ordering for fixed-width
// encodings. Sorting the final admitted buffer avoids an intermediate vector
// and the generic per-row heap/comparator overhead on the common numeric path.
func ExactFilterDataSorter(typ types.Type) func([]byte) {
	// typeswitch:partial exact runtime filters support ordinary float32/64, not low-precision float types.
	switch typ.Oid {
	case types.T_bool:
		return func(data []byte) { slices.SortFunc(types.DecodeSlice[bool](data), types.BoolAscCompare) }
	case types.T_int8:
		return sortExactData[int8]
	case types.T_int16:
		return sortExactData[int16]
	case types.T_int32:
		return sortExactData[int32]
	case types.T_int64:
		return sortExactData[int64]
	case types.T_uint8:
		return sortExactData[uint8]
	case types.T_uint16:
		return sortExactData[uint16]
	case types.T_uint32:
		return sortExactData[uint32]
	case types.T_uint64, types.T_bit:
		return sortExactData[uint64]
	case types.T_float32:
		return sortExactData[float32]
	case types.T_float64:
		return sortExactData[float64]
	case types.T_date:
		return sortExactData[types.Date]
	case types.T_time:
		return sortExactData[types.Time]
	case types.T_datetime:
		return sortExactData[types.Datetime]
	case types.T_timestamp:
		return sortExactData[types.Timestamp]
	case types.T_year:
		return sortExactData[types.MoYear]
	case types.T_enum:
		return sortExactData[types.Enum]
	case types.T_decimal64:
		return func(data []byte) { slices.SortFunc(types.DecodeSlice[types.Decimal64](data), types.Decimal64.Compare) }
	case types.T_decimal128:
		return func(data []byte) {
			slices.SortFunc(types.DecodeSlice[types.Decimal128](data), types.Decimal128.Compare)
		}
	case types.T_decimal256:
		return func(data []byte) {
			slices.SortFunc(types.DecodeSlice[types.Decimal256](data), types.Decimal256.Compare)
		}
	case types.T_uuid:
		return func(data []byte) { slices.SortFunc(types.DecodeSlice[types.Uuid](data), types.Uuid.Compare) }
	default:
		return nil
	}
}

func sortExactData[T cmp.Ordered](data []byte) { slices.Sort(types.DecodeSlice[T](data)) }

// MarshalExactFilterRuns writes null-free runs directly into the
// existing IN-vector wire format. There is no concatenated temporary Vector.
// Scratch is O(number of runs); the only O(number of keys) allocation is the
// final wire payload. Numeric data is sorted in that final buffer; other runs
// must already be sorted and are merged. The single-run path is unchanged.
func MarshalExactFilterRuns(ctx context.Context, runs []*vector.Vector, mp *mpool.MPool, account *mpool.AllocationAccount, owner mpool.AllocationOwner, site mpool.AllocationSite) (data []byte, release func(), err error) {
	if len(runs) == 1 {
		return MarshalExactFilterVector(runs[0], mp, account, owner, site)
	}
	if len(runs) == 0 || runs[0] == nil || account == nil {
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
			areaBytes += uint64(len(v.GetArea()))
		}
	}
	dataBytes := rows * uint64(width)
	const header = 1 + types.TSize + 4 + 4
	total := uint64(header+4+4+1) + dataBytes + areaBytes
	if rows > uint64(^uint32(0)>>1) || total > uint64(mpool.MaxAllocationSize()) {
		return nil, nil, ErrOptionalKeySize
	}
	if sorter := ExactFilterDataSorter(typ); sorter != nil {
		buf, out, err := newExactRunBuffer(typ, rows, 0, mp, account, owner, site)
		if err != nil {
			return nil, nil, err
		}
		pos := header
		for _, v := range runs {
			if err := ctx.Err(); err != nil {
				buf.Free()
				return nil, nil, err
			}
			n := v.Length() * width
			copy(out[pos:pos+n], v.GetData()[:n])
			pos += n
		}
		sorter(out[header : header+int(dataBytes)])
		if err := ctx.Err(); err != nil {
			buf.Free()
			return nil, nil, err
		}
		return out, buf.Free, nil
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
	// Compare heap positions rather than copying two run descriptors per
	// comparison. Choose the key domain once, outside the per-row merge loop.
	less := func(a, b int) bool {
		// IN encodings use raw-byte string order, not SQL collation order.
		return bytes.Compare(heap[a].key, heap[b].key) < 0
	}
	if !typ.IsVarlen() {
		comparator := compare.New(typ, false, false)
		less = func(a, b int) bool {
			left, right := &heap[a], &heap[b]
			comparator.Set(0, left.vec)
			comparator.Set(1, right.vec)
			return comparator.Compare(0, 1, int64(left.row), int64(right.row)) < 0
		}
	}
	down := func(root int) {
		for {
			child := root*2 + 1
			if child >= len(heap) {
				return
			}
			if child+1 < len(heap) && less(child+1, child) {
				child++
			}
			if !less(child, root) {
				return
			}
			heap[root], heap[child] = heap[child], heap[root]
			root = child
		}
	}
	var areaBase uint32
	for _, v := range runs {
		if v.Length() != 0 {
			run := mergeRun{vec: v, areaBase: areaBase}
			if typ.IsVarlen() {
				run.key = v.GetRawBytesAt(0)
			}
			heap = append(heap, run)
		}
		if typ.IsVarlen() {
			areaBase += uint32(len(v.GetArea()))
		}
	}
	for i := len(heap)/2 - 1; i >= 0; i-- {
		down(i)
	}
	buf, out, err := newExactRunBuffer(typ, rows, areaBytes, mp, account, owner, site)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		if err != nil {
			buf.Free()
		}
	}()
	areaStart := header + int(dataBytes) + 4
	// Keep each run's area layout, including shared values. Bulk copies avoid
	// a sizing scan and one tiny payload copy per key; the merge only rebases
	// the sorted descriptors into these admitted backing spans.
	if typ.IsVarlen() {
		areaPos := areaStart
		for _, v := range runs {
			if err = ctx.Err(); err != nil {
				return nil, nil, err
			}
			areaPos += copy(out[areaPos:], v.GetArea())
		}
	}
	dataPos := header
	for row := uint64(0); row < rows; row++ {
		if row&8191 == 0 {
			if err = ctx.Err(); err != nil {
				return nil, nil, err
			}
		}
		run := &heap[0]
		if typ.IsVarlen() {
			descriptor := vector.MustFixedColNoTypeCheck[types.Varlena](run.vec)[run.row]
			if !descriptor.IsSmall() {
				offset, length := descriptor.OffsetLen()
				descriptor.SetOffsetLen(run.areaBase+offset, length)
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
		} else if typ.IsVarlen() {
			run.key = run.vec.GetRawBytesAt(run.row)
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

func newExactRunBuffer(typ types.Type, rows, areaBytes uint64, mp *mpool.MPool, account *mpool.AllocationAccount, owner mpool.AllocationOwner, site mpool.AllocationSite) (*mpool.AccountedBuffer, []byte, error) {
	const header = 1 + types.TSize + 4 + 4
	dataBytes := rows * uint64(typ.TypeSize())
	total := uint64(header+4+4+1) + dataBytes + areaBytes
	// Align the numeric data inside the otherwise unchanged wire frame.
	// Keep the original capacity class: padding must not double a buffer or
	// turn a representable final encoding into a size refusal.
	padding := uint64((8 - header%8) % 8)
	before, okBefore := mpool.GrowCapacity(0, int64(total))
	after, okAfter := mpool.GrowCapacity(0, int64(total+padding))
	if !okBefore || !okAfter || after != before {
		padding = 0
	}
	buf, err := mpool.NewAccountedBuffer(mp, account, owner, site)
	if err != nil {
		return nil, nil, err
	}
	if err = buf.Resize(int(total + padding)); err != nil {
		buf.Free()
		return nil, nil, MarkOptionalAllocationError(err)
	}
	out := buf.Bytes()[int(padding):]
	out[0] = byte(vector.FLAT)
	copy(out[1:], types.EncodeType(&typ))
	binary.NativeEndian.PutUint32(out[1+types.TSize:], uint32(rows))
	binary.NativeEndian.PutUint32(out[1+types.TSize+4:], uint32(dataBytes))
	binary.NativeEndian.PutUint32(out[header+int(dataBytes):], uint32(areaBytes))
	binary.NativeEndian.PutUint32(out[header+int(dataBytes)+4+int(areaBytes):], 0)
	out[len(out)-1] = 1
	return buf, out, nil
}
