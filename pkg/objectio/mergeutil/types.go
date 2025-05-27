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

type SinkerT func(*batch.Batch) error

func MergeSortBatches(
	batches []*batch.Batch,
	sortKeyIdx int,
	buffer *batch.Batch,
	sinker SinkerT,
	mp *mpool.MPool,
	cleanBatch bool,
) error {
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
	for size > 0 {
		batchIndex, rowIndex, size = merge.getNextPos()
		for i := range buffer.Vecs {
			err := buffer.Vecs[i].UnionOne(batches[batchIndex].Vecs[i], int64(rowIndex), mp)
			if err != nil {
				return err
			}
		}
		// all data in batches[batchIndex] are used. Clean it.
		if cleanBatch {
			if rowIndex+1 == batches[batchIndex].RowCount() {
				batches[batchIndex].Clean(mp)
			}
		}
		lens++
		if lens == objectio.BlockMaxRows {
			lens = 0
			buffer.SetRowCount(objectio.BlockMaxRows)
			if err := sinker(buffer); err != nil {
				return err
			}
			// force clean
			buffer.CleanOnlyData()
		}
	}
	if lens > 0 {
		buffer.SetRowCount(lens)
		if err := sinker(buffer); err != nil {
			return err
		}
		buffer.CleanOnlyData()
	}
	return nil
}

func SortColumnsByIndex(
	cols []*vector.Vector, sortIdx int, mp *mpool.MPool,
) (err error) {
	sortKey := cols[sortIdx]
	sortedIdx := make([]int64, sortKey.Length())
	for i := 0; i < len(sortedIdx); i++ {
		sortedIdx[i] = int64(i)
	}
	sort.Sort(false, false, true, sortedIdx, sortKey)
	for i := 0; i < len(cols); i++ {
		err = cols[i].Shuffle(sortedIdx, mp)
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
