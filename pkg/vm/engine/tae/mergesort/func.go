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

package mergesort

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

/// sort things

func sort[T any](col containers.Vector, lessFunc lessFunc[T], n int) SortSlice[T] {
	dataWithIdx := NewSortSlice(n, lessFunc)
	if col.HasNull() {
		for i := 0; i < n; i++ {
			var item SortElem[T]
			if col.IsNull(i) {
				item = SortElem[T]{isNull: true, idx: int32(i)}
			} else {
				item = SortElem[T]{data: col.Get(i).(T), idx: int32(i)}
			}
			dataWithIdx.Append(item)
		}
	} else {
		for i := 0; i < n; i++ {
			dataWithIdx.Append(SortElem[T]{data: col.Get(i).(T), idx: int32(i)})
		}
	}
	sortUnstable(dataWithIdx)
	return dataWithIdx
}

func Sort[T any](col containers.Vector, lessFunc lessFunc[T], idx []int32) (ret containers.Vector) {
	dataWithIdx := sort(col, lessFunc, len(idx))

	// make col sorted
	for i, v := range dataWithIdx.AsSlice() {
		idx[i] = v.idx
		if v.isNull {
			col.Update(i, nil, true)
		} else {
			// FIXME: memory waste for varlen type
			col.Update(i, v.data, false)
		}
	}

	ret = col
	return
}

func Shuffle(
	col containers.Vector, idx []int32, pool *containers.VectorPool,
) containers.Vector {
	var err error
	ret := pool.GetVector(col.GetType())
	if err = ret.PreExtend(len(idx)); err != nil {
		panic(err)
	}
	retVec := ret.GetDownstreamVector()
	srcVec := col.GetDownstreamVector()

	if err = retVec.Union(srcVec, idx, ret.GetAllocator()); err != nil {
		panic(err)
	}

	col.Close()
	return ret
}

/// merge things

// mergeVarlen merge varlen column, and do not consider lifetime of `column` and `ret`
func mergeVarlen(
	col, ret []*vector.Vector,
	sortidx, mapping []uint32,
	fromLayout, toLayout []uint32,
	m *mpool.MPool,
) {
	offset := make([]uint32, len(fromLayout))
	offset[0] = 0
	for i := 1; i < len(fromLayout); i++ {
		offset[i] = offset[i-1] + fromLayout[i-1]
	}

	nBlk := len(col)
	heap := newHeapSlice(nBlk, bytesLess)
	for i := 0; i < nBlk; i++ {
		if col[i].IsNull(0) {
			heap.Append(heapElem[[]byte]{isNull: true, src: uint32(i), next: 1})
		} else {
			heap.Append(heapElem[[]byte]{data: col[i].GetBytesAt(0), src: uint32(i), next: 1})
		}
	}
	heapInit(heap)
	k := 0
	for i := 0; i < len(toLayout); i++ {
		for j := 0; j < int(toLayout[i]); j++ {
			top := heapPop(heap)
			// update sortidx and mapping for k-th sorted element
			sortidx[k] = top.src
			mapping[offset[top.src]+top.next-1] = uint32(k)
			// bump k
			k++
			// insert value to ret
			if top.isNull {
				vector.AppendBytes(ret[i], nil, true, m)
			} else {
				vector.AppendBytes(ret[i], top.data, false, m)
			}
			// find next element to heap
			if int(top.next) < int(fromLayout[top.src]) {
				if col[top.src].IsNull(uint64(top.next)) {
					heapPush(heap, heapElem[[]byte]{isNull: true, src: top.src, next: top.next + 1})
				} else {
					heapPush(heap, heapElem[[]byte]{data: col[top.src].GetBytesAt(int(top.next)), src: top.src, next: top.next + 1})
				}
			}
		}
	}
}

// mergeVarlen merge fixed type column, and do not consider lifetime of `column` and `ret`
func mergeFixed[T any](
	col, ret []*vector.Vector,
	sortidx, mapping []uint32,
	lessFunc lessFunc[T],
	fromLayout, toLayout []uint32,
	m *mpool.MPool,
) {
	offset := make([]uint32, len(fromLayout))
	offset[0] = 0
	for i := 1; i < len(fromLayout); i++ {
		offset[i] = offset[i-1] + fromLayout[i-1]
	}

	nBlk := len(col)
	heap := newHeapSlice(nBlk, lessFunc)
	for i := 0; i < nBlk; i++ {
		if col[i].IsNull(0) {
			heap.Append(heapElem[T]{isNull: true, src: uint32(i), next: 1})
		} else {
			heap.Append(heapElem[T]{data: vector.GetFixedAt[T](col[i], 0), src: uint32(i), next: 1})
		}
	}
	heapInit(heap)
	var nullVal T
	k := 0
	for i := 0; i < len(toLayout); i++ {
		for j := 0; j < int(toLayout[i]); j++ {
			top := heapPop(heap)
			// update sortidx and mapping for k-th sorted element
			sortidx[k] = top.src
			mapping[offset[top.src]+top.next-1] = uint32(k)
			// bump k
			k++
			// insert value to ret
			if top.isNull {
				vector.AppendFixed(ret[i], nullVal, true, m)
			} else {
				vector.AppendFixed(ret[i], top.data, false, m)
			}
			// find next element to heap
			if int(top.next) < int(fromLayout[top.src]) {
				if col[top.src].IsNull(uint64(top.next)) {
					heapPush(heap, heapElem[T]{isNull: true, src: top.src, next: top.next + 1})
				} else {
					heapPush(heap, heapElem[T]{data: vector.GetFixedAt[T](col[top.src], int(top.next)), src: top.src, next: top.next + 1})
				}
			}
		}
	}
}

// Merge merge column, and do not consider lifetime of `column` and `ret`
//
// ret, sortidx, mapping are output, they will be modified in this function.
// Caller should initialize them properly.
//
// sortidx[k] means k-th element in the flatten toLayout array,
// which is the merged outcome, is from sortedidx[k]-th column in fromLayout array
// For fromLayout [5, 4] and toLayout [3, 3, 3], sortidx maybe like [0,0,0,1,1,0,1,1,0]
//
// mapping[k] means k-th element in the flatten fromLayout array is moved
// to the mapping[k]-th element in the flatten toLayout array
func Merge(
	column, ret []*vector.Vector,
	sortidx, mapping []uint32,
	fromLayout, toLayout []uint32,
	m *mpool.MPool,
) {
	if len(column) == 0 {
		return
	}

	typ := column[0].GetType()
	if typ.IsVarlen() {
		mergeVarlen(column, ret, sortidx, mapping, fromLayout, toLayout, m)
	} else {
		switch typ.Oid {
		case types.T_bool:
			mergeFixed[bool](column, ret, sortidx, mapping, boolLess, fromLayout, toLayout, m)
		case types.T_bit:
			mergeFixed[uint64](column, ret, sortidx, mapping, numericLess[uint64], fromLayout, toLayout, m)
		case types.T_int8:
			mergeFixed[int8](column, ret, sortidx, mapping, numericLess[int8], fromLayout, toLayout, m)
		case types.T_int16:
			mergeFixed[int16](column, ret, sortidx, mapping, numericLess[int16], fromLayout, toLayout, m)
		case types.T_int32:
			mergeFixed[int32](column, ret, sortidx, mapping, numericLess[int32], fromLayout, toLayout, m)
		case types.T_int64:
			mergeFixed[int64](column, ret, sortidx, mapping, numericLess[int64], fromLayout, toLayout, m)
		case types.T_float32:
			mergeFixed[float32](column, ret, sortidx, mapping, numericLess[float32], fromLayout, toLayout, m)
		case types.T_float64:
			mergeFixed[float64](column, ret, sortidx, mapping, numericLess[float64], fromLayout, toLayout, m)
		case types.T_uint8:
			mergeFixed[uint8](column, ret, sortidx, mapping, numericLess[uint8], fromLayout, toLayout, m)
		case types.T_uint16:
			mergeFixed[uint16](column, ret, sortidx, mapping, numericLess[uint16], fromLayout, toLayout, m)
		case types.T_uint32:
			mergeFixed[uint32](column, ret, sortidx, mapping, numericLess[uint32], fromLayout, toLayout, m)
		case types.T_uint64:
			mergeFixed[uint64](column, ret, sortidx, mapping, numericLess[uint64], fromLayout, toLayout, m)
		case types.T_date:
			mergeFixed[types.Date](column, ret, sortidx, mapping, numericLess[types.Date], fromLayout, toLayout, m)
		case types.T_timestamp:
			mergeFixed[types.Timestamp](column, ret, sortidx, mapping, numericLess[types.Timestamp], fromLayout, toLayout, m)
		case types.T_datetime:
			mergeFixed[types.Datetime](column, ret, sortidx, mapping, numericLess[types.Datetime], fromLayout, toLayout, m)
		case types.T_time:
			mergeFixed[types.Time](column, ret, sortidx, mapping, numericLess[types.Time], fromLayout, toLayout, m)
		case types.T_enum:
			mergeFixed[types.Enum](column, ret, sortidx, mapping, numericLess[types.Enum], fromLayout, toLayout, m)
		case types.T_decimal64:
			mergeFixed[types.Decimal64](column, ret, sortidx, mapping, ltTypeLess[types.Decimal64], fromLayout, toLayout, m)
		case types.T_decimal128:
			mergeFixed[types.Decimal128](column, ret, sortidx, mapping, ltTypeLess[types.Decimal128], fromLayout, toLayout, m)
		case types.T_uuid:
			mergeFixed[types.Uuid](column, ret, sortidx, mapping, ltTypeLess[types.Uuid], fromLayout, toLayout, m)
		case types.T_TS:
			mergeFixed[types.TS](column, ret, sortidx, mapping, tsLess, fromLayout, toLayout, m)
		case types.T_Rowid:
			mergeFixed[types.Rowid](column, ret, sortidx, mapping, rowidLess, fromLayout, toLayout, m)
		case types.T_Blockid:
			mergeFixed[types.Blockid](column, ret, sortidx, mapping, blockidLess, fromLayout, toLayout, m)
		default:
			panic(fmt.Sprintf("unsupported type %s", typ.String()))
		}
	}

}

func multiplexVarlen(
	cols, ret []*vector.Vector, src []uint32, fromLayout, toLayout []uint32, m *mpool.MPool,
) {
	cursors := make([]int, len(fromLayout))
	k := 0
	for i := 0; i < len(toLayout); i++ {
		ret[i].PreExtend(int(toLayout[i]), m)
		for j := 0; j < int(toLayout[i]); j++ {
			s := src[k]
			if cols[s].IsNull(uint64(cursors[s])) {
				vector.AppendBytes(ret[i], nil, true, m)
			} else {
				vector.AppendBytes(ret[i], cols[s].GetBytesAt(cursors[s]), false, m)
			}
			cursors[s]++
			k++
		}
	}

}

func multiplexFixed[T any](
	cols, ret []*vector.Vector, src []uint32, fromLayout, toLayout []uint32, m *mpool.MPool,
) {
	cursors := make([]int, len(fromLayout))
	k := 0
	var nullVal T
	for i := 0; i < len(toLayout); i++ {
		ret[i].PreExtend(int(toLayout[i]), m)
		for j := 0; j < int(toLayout[i]); j++ {
			s := src[k]
			if cols[s].IsNull(uint64(cursors[s])) {
				vector.AppendFixed(ret[i], nullVal, true, m)
			} else {
				vector.AppendFixed(ret[i], vector.GetFixedAt[T](cols[s], cursors[s]), false, m)
			}
			cursors[s]++
			k++
		}
	}
}

// Multiplex do not consider lifetime of `column` and `ret`
func Multiplex(
	column, ret []*vector.Vector, src []uint32, fromLayout, toLayout []uint32, m *mpool.MPool,
) {
	if len(column) == 0 {
		return
	}

	typ := column[0].GetType()
	if typ.IsVarlen() {
		multiplexVarlen(column, ret, src, fromLayout, toLayout, m)
	} else {
		switch typ.Oid {
		case types.T_bool:
			multiplexFixed[bool](column, ret, src, fromLayout, toLayout, m)
		case types.T_bit:
			multiplexFixed[uint64](column, ret, src, fromLayout, toLayout, m)
		case types.T_int8:
			multiplexFixed[int8](column, ret, src, fromLayout, toLayout, m)
		case types.T_int16:
			multiplexFixed[int16](column, ret, src, fromLayout, toLayout, m)
		case types.T_int32:
			multiplexFixed[int32](column, ret, src, fromLayout, toLayout, m)
		case types.T_int64:
			multiplexFixed[int64](column, ret, src, fromLayout, toLayout, m)
		case types.T_float32:
			multiplexFixed[float32](column, ret, src, fromLayout, toLayout, m)
		case types.T_float64:
			multiplexFixed[float64](column, ret, src, fromLayout, toLayout, m)
		case types.T_uint8:
			multiplexFixed[uint8](column, ret, src, fromLayout, toLayout, m)
		case types.T_uint16:
			multiplexFixed[uint16](column, ret, src, fromLayout, toLayout, m)
		case types.T_uint32:
			multiplexFixed[uint32](column, ret, src, fromLayout, toLayout, m)
		case types.T_uint64:
			multiplexFixed[uint64](column, ret, src, fromLayout, toLayout, m)
		case types.T_date:
			multiplexFixed[types.Date](column, ret, src, fromLayout, toLayout, m)
		case types.T_timestamp:
			multiplexFixed[types.Timestamp](column, ret, src, fromLayout, toLayout, m)
		case types.T_datetime:
			multiplexFixed[types.Datetime](column, ret, src, fromLayout, toLayout, m)
		case types.T_time:
			multiplexFixed[types.Time](column, ret, src, fromLayout, toLayout, m)
		case types.T_enum:
			multiplexFixed[types.Enum](column, ret, src, fromLayout, toLayout, m)
		case types.T_decimal64:
			multiplexFixed[types.Decimal64](column, ret, src, fromLayout, toLayout, m)
		case types.T_decimal128:
			multiplexFixed[types.Decimal128](column, ret, src, fromLayout, toLayout, m)
		case types.T_uuid:
			multiplexFixed[types.Uuid](column, ret, src, fromLayout, toLayout, m)
		case types.T_TS:
			multiplexFixed[types.TS](column, ret, src, fromLayout, toLayout, m)
		case types.T_Rowid:
			multiplexFixed[types.Rowid](column, ret, src, fromLayout, toLayout, m)
		case types.T_Blockid:
			multiplexFixed[types.Blockid](column, ret, src, fromLayout, toLayout, m)
		default:
			panic(fmt.Sprintf("unsupported type %s", typ.String()))
		}
	}
}

// Reshape do not consider lifetime of `column` and `ret`
func Reshape(column []*vector.Vector, ret []*vector.Vector, fromLayout, toLayout []uint32, m *mpool.MPool) {
	fromIdx := 0
	fromOffset := 0
	typ := column[0].GetType()
	for i := 0; i < len(toLayout); i++ {
		toOffset := 0
		for toOffset < int(toLayout[i]) {
			// find offset to fill a full block
			fromLeft := int(fromLayout[fromIdx]) - fromOffset
			if fromLeft == 0 {
				fromIdx++
				fromOffset = 0
				fromLeft = int(fromLayout[fromIdx])
			}
			length := 0
			if fromLeft < int(toLayout[i])-toOffset {
				length = fromLeft
			} else {
				length = int(toLayout[i]) - toOffset
			}

			// clone from src and append to dest
			// FIXME: is clone necessary? can we just feed the original vector window to GetUnionAllFunction?
			// TODO: use vector pool for the cloned vector
			cloned, err := column[fromIdx].CloneWindow(fromOffset, fromOffset+length, m)
			if err != nil {
				panic(err)
			}
			err = vector.GetUnionAllFunction(*typ, m)(ret[i], cloned)
			if err != nil {
				panic(err)
			}

			// release temporary coloned vector
			cloned.Free(m)

			// update offset
			fromOffset += length
			toOffset += length
		}
	}
}
