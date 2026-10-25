// Copyright 2022 Matrix Origin
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

package mergeutil

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sort"
)

// SinkerT takes ownership of the full buffer and returns a clean, empty
// replacement batch (with capacity retained) for MergeSortBatches to fill.
type SinkerT func(*batch.Batch) (*batch.Batch, error)

func MergeSortBatches(
	batches []*batch.Batch,
	sortKeyIdx int,
	buffer *batch.Batch,
	sinker SinkerT,
	mp *mpool.MPool,
	putBack func(int),
	byteLimits ...int64,
) (*batch.Batch, error) {
	byteLimit := int64(64 << 20)
	if len(byteLimits) > 0 && byteLimits[0] > 0 {
		byteLimit = byteLimits[0]
	}
	var merge mergeInterface
	nulls := make([]*nulls.Nulls, len(batches))
	for i, b := range batches {
		nulls[i] = b.Vecs[sortKeyIdx].GetNulls()
	}
	switch batches[0].Vecs[sortKeyIdx].GetType().Oid {
	case types.T_bool:
		ds := &fixedDataSlice[bool]{getFixedCols[bool](batches, sortKeyIdx)}
		merge = newMerge(sort.BoolLess, ds, nulls)
	case types.T_bit:
		ds := &fixedDataSlice[uint64]{getFixedCols[uint64](batches, sortKeyIdx)}
		merge = newMerge(sort.GenericLess[uint64], ds, nulls)
	case types.T_int8:
		ds := &fixedDataSlice[int8]{getFixedCols[int8](batches, sortKeyIdx)}
		merge = newMerge(sort.GenericLess[int8], ds, nulls)
	case types.T_int16:
		ds := &fixedDataSlice[int16]{getFixedCols[int16](batches, sortKeyIdx)}
		merge = newMerge(sort.GenericLess[int16], ds, nulls)
	case types.T_int32:
		ds := &fixedDataSlice[int32]{getFixedCols[int32](batches, sortKeyIdx)}
		merge = newMerge(sort.GenericLess[int32], ds, nulls)
	case types.T_int64:
		ds := &fixedDataSlice[int64]{getFixedCols[int64](batches, sortKeyIdx)}
		merge = newMerge(sort.GenericLess[int64], ds, nulls)
	case types.T_uint8:
		ds := &fixedDataSlice[uint8]{getFixedCols[uint8](batches, sortKeyIdx)}
		merge = newMerge(sort.GenericLess[uint8], ds, nulls)
	case types.T_uint16:
		ds := &fixedDataSlice[uint16]{getFixedCols[uint16](batches, sortKeyIdx)}
		merge = newMerge(sort.GenericLess[uint16], ds, nulls)
	case types.T_uint32:
		ds := &fixedDataSlice[uint32]{getFixedCols[uint32](batches, sortKeyIdx)}
		merge = newMerge(sort.GenericLess[uint32], ds, nulls)
	case types.T_uint64:
		ds := &fixedDataSlice[uint64]{getFixedCols[uint64](batches, sortKeyIdx)}
		merge = newMerge(sort.GenericLess[uint64], ds, nulls)
	case types.T_float32:
		ds := &fixedDataSlice[float32]{getFixedCols[float32](batches, sortKeyIdx)}
		merge = newMerge(sort.GenericLess[float32], ds, nulls)
	case types.T_float64:
		ds := &fixedDataSlice[float64]{getFixedCols[float64](batches, sortKeyIdx)}
		merge = newMerge(sort.GenericLess[float64], ds, nulls)
	case types.T_date:
		ds := &fixedDataSlice[types.Date]{getFixedCols[types.Date](batches, sortKeyIdx)}
		merge = newMerge(sort.GenericLess[types.Date], ds, nulls)
	case types.T_datetime:
		ds := &fixedDataSlice[types.Datetime]{getFixedCols[types.Datetime](batches, sortKeyIdx)}
		merge = newMerge(sort.GenericLess[types.Datetime], ds, nulls)
	case types.T_time:
		ds := &fixedDataSlice[types.Time]{getFixedCols[types.Time](batches, sortKeyIdx)}
		merge = newMerge(sort.GenericLess[types.Time], ds, nulls)
	case types.T_timestamp:
		ds := &fixedDataSlice[types.Timestamp]{getFixedCols[types.Timestamp](batches, sortKeyIdx)}
		merge = newMerge(sort.GenericLess[types.Timestamp], ds, nulls)
	case types.T_enum:
		ds := &fixedDataSlice[types.Enum]{getFixedCols[types.Enum](batches, sortKeyIdx)}
		merge = newMerge(sort.GenericLess[types.Enum], ds, nulls)
	case types.T_decimal64:
		ds := &fixedDataSlice[types.Decimal64]{getFixedCols[types.Decimal64](batches, sortKeyIdx)}
		merge = newMerge(sort.Decimal64Less, ds, nulls)
	case types.T_decimal128:
		ds := &fixedDataSlice[types.Decimal128]{getFixedCols[types.Decimal128](batches, sortKeyIdx)}
		merge = newMerge(sort.Decimal128Less, ds, nulls)
	case types.T_uuid:
		ds := &fixedDataSlice[types.Uuid]{getFixedCols[types.Uuid](batches, sortKeyIdx)}
		merge = newMerge(sort.UuidLess, ds, nulls)
	case types.T_char, types.T_varchar, types.T_blob, types.T_text, types.T_datalink:
		ds := &varlenaDataSlice{getVarlenaCols(batches, sortKeyIdx)}
		merge = newMerge(sort.GenericLess[string], ds, nulls)
	case types.T_Rowid:
		ds := &fixedDataSlice[types.Rowid]{getFixedCols[types.Rowid](batches, sortKeyIdx)}
		merge = newMerge(sort.RowidLess, ds, nulls)
	default:
		panic(fmt.Sprintf("invalid type: %s", batches[0].Vecs[sortKeyIdx].GetType()))
	}
	var (
		batchIndex int
		rowIndex   int
		lens       int
	)
	size := len(batches)
	buffer.CleanOnlyData()
	areas := make([]int, len(buffer.Vecs))
	var varlenCols []int
	for col, vec := range buffer.Vecs {
		if vec.GetType().IsVarlen() {
			varlenCols = append(varlenCols, col)
		}
	}
	// Bitmap-bearing sources retain the exact null/grouping growth forecast.
	var hasBitmaps bool
	for _, b := range batches {
		for _, vec := range b.Vecs {
			hasBitmaps = hasBitmaps || !vec.GetNulls().EmptyByFlag() || !vec.GetGrouping().EmptyByFlag()
		}
	}
	allocated, dataRows := 0, 0
	refreshCapacity := func() {
		allocated, dataRows = buffer.Allocated(), objectio.BlockMaxRows
		for _, vec := range buffer.Vecs {
			width := vec.GetType().TypeSize()
			if width <= 0 {
				dataRows = 0
				break
			}
			dataRows = min(dataRows, cap(vec.GetData())/width)
		}
	}
	refreshCapacity()
	for size > 0 {
		batchIndex, rowIndex, size = merge.getNextPos()
		mayGrow := lens >= dataRows
		for _, col := range varlenCols {
			src := batches[batchIndex].Vecs[col]
			area := 0
			if !src.IsNull(uint64(rowIndex)) {
				if n := len(src.GetBytesAt(rowIndex)); n > types.VarlenaInlineSize {
					area = n
				}
			}
			areas[col] = area
			backing := buffer.Vecs[col].GetArea()
			mayGrow = mayGrow || area > cap(backing)-len(backing)
		}
		var growth int64
		if mayGrow || hasBitmaps {
			for i, v := range buffer.Vecs {
				growth += v.AppendWindowAllocation(batches[batchIndex].Vecs[i], rowIndex, rowIndex+1)
			}
		}
		if lens > 0 && int64(allocated)+growth > byteLimit {
			buffer.SetRowCount(lens)
			var err error
			buffer, err = sinker(buffer)
			if err != nil {
				return buffer, err
			}
			lens = 0
			refreshCapacity()
			mayGrow = true
		}
		if lens == 0 && int64(allocated) > byteLimit {
			buffer.FreeColumns(mp)
			mayGrow = true
		}
		for i := range buffer.Vecs {
			if mayGrow {
				if err := buffer.Vecs[i].PreExtendWithArea(1, areas[i], mp); err != nil {
					return buffer, err
				}
			}
			err := buffer.Vecs[i].UnionOne(batches[batchIndex].Vecs[i], int64(rowIndex), mp)
			if err != nil {
				return buffer, err
			}
		}
		if mayGrow {
			refreshCapacity()
		}
		// all data in batches[batchIndex] are used. Clean it.
		if putBack != nil && rowIndex+1 == batches[batchIndex].RowCount() {
			putBack(batchIndex)
		}
		lens++
		if lens == objectio.BlockMaxRows {
			lens = 0
			buffer.SetRowCount(objectio.BlockMaxRows)
			var err error
			if buffer, err = sinker(buffer); err != nil {
				return buffer, err
			}
			refreshCapacity()
		}
	}
	if lens > 0 {
		buffer.SetRowCount(lens)
		var err error
		if buffer, err = sinker(buffer); err != nil {
			return buffer, err
		}
	}
	return buffer, nil
}

func SortColumnsByIndex(
	cols []*vector.Vector, sortIdx int, mp *mpool.MPool,
) (err error) {
	var idxBuf []int64
	var shuffleBuf []byte
	return SortColumnsByIndexWithBuf(cols, sortIdx, mp, &idxBuf, &shuffleBuf)
}

// SortColumnsByIndexWithBuf sorts all columns by the sort key column at sortIdx.
// idxBuf is a scratch buffer for the permutation index array; shuffleBuf is a
// scratch buffer for the shuffle operation. Both are grown as needed and
// retained across calls for reuse.
func SortColumnsByIndexWithBuf(
	cols []*vector.Vector, sortIdx int, mp *mpool.MPool, idxBuf *[]int64, shuffleBuf *[]byte,
) (err error) {
	sortKey := cols[sortIdx]
	n := sortKey.Length()
	if cap(*idxBuf) < n {
		*idxBuf = make([]int64, n)
	} else {
		*idxBuf = (*idxBuf)[:n]
	}
	sortedIdx := *idxBuf
	for i := 0; i < n; i++ {
		sortedIdx[i] = int64(i)
	}
	sort.Sort(false, false, true, sortedIdx, sortKey)
	for i := 0; i < len(cols); i++ {
		err = cols[i].ShuffleWithBuf(sortedIdx, mp, shuffleBuf)
		if err != nil {
			return
		}

		if i == sortIdx {
			cols[i].SetSorted(true)
		} else {
			cols[i].SetSorted(false)
		}
	}

	return
}
