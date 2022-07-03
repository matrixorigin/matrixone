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

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort/bools"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort/decimal128s"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort/numerics"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort/varchar"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/types"
)

func SortBlockColumns(cols []containers.Vector, pk int) ([]uint32, error) {
	sortedIdx := make([]uint32, cols[pk].Length())

	switch cols[pk].GetType().Oid {
	case types.Type_BOOL:
		bools.Sort(cols[pk], sortedIdx)
	case types.Type_INT8:
		numerics.Sort[int8](cols[pk], sortedIdx)
	case types.Type_INT16:
		numerics.Sort[int16](cols[pk], sortedIdx)
	case types.Type_INT32:
		numerics.Sort[int32](cols[pk], sortedIdx)
	case types.Type_INT64:
		numerics.Sort[int64](cols[pk], sortedIdx)
	case types.Type_UINT8:
		numerics.Sort[uint8](cols[pk], sortedIdx)
	case types.Type_UINT16:
		numerics.Sort[uint16](cols[pk], sortedIdx)
	case types.Type_UINT32:
		numerics.Sort[uint32](cols[pk], sortedIdx)
	case types.Type_UINT64:
		numerics.Sort[uint64](cols[pk], sortedIdx)
	case types.Type_FLOAT32:
		numerics.Sort[float32](cols[pk], sortedIdx)
	case types.Type_FLOAT64:
		numerics.Sort[float64](cols[pk], sortedIdx)
	case types.Type_DATE:
		numerics.Sort[types.Date](cols[pk], sortedIdx)
	case types.Type_DATETIME:
		numerics.Sort[types.Datetime](cols[pk], sortedIdx)
	case types.Type_DECIMAL64:
		numerics.Sort[types.Decimal64](cols[pk], sortedIdx)
	case types.Type_DECIMAL128:
		decimal128s.Sort(cols[pk], sortedIdx)
	case types.Type_TIMESTAMP:
		numerics.Sort[types.Timestamp](cols[pk], sortedIdx)
	case types.Type_CHAR, types.Type_JSON, types.Type_VARCHAR:
		varchar.Sort(cols[pk], sortedIdx)
	default:
		panic(fmt.Sprintf("%s not supported", cols[pk].GetType().String()))
	}

	for i := 0; i < len(cols); i++ {
		if i == pk {
			continue
		}
		cols[i] = Shuffle(cols[i], sortedIdx)
	}
	return sortedIdx, nil
}

func MergeSortedColumn(column []containers.Vector, sortedIdx *[]uint32, fromLayout, toLayout []uint32) (ret []containers.Vector, mapping []uint32) {
	switch column[0].GetType().Oid {
	case types.Type_BOOL:
		ret, mapping = bools.Merge(column, sortedIdx, fromLayout, toLayout)
	case types.Type_INT8:
		ret, mapping = numerics.Merge[int8](column, sortedIdx, fromLayout, toLayout)
	case types.Type_INT16:
		ret, mapping = numerics.Merge[int16](column, sortedIdx, fromLayout, toLayout)
	case types.Type_INT32:
		ret, mapping = numerics.Merge[int32](column, sortedIdx, fromLayout, toLayout)
	case types.Type_INT64:
		ret, mapping = numerics.Merge[int64](column, sortedIdx, fromLayout, toLayout)
	case types.Type_UINT8:
		ret, mapping = numerics.Merge[uint8](column, sortedIdx, fromLayout, toLayout)
	case types.Type_UINT16:
		ret, mapping = numerics.Merge[uint16](column, sortedIdx, fromLayout, toLayout)
	case types.Type_UINT32:
		ret, mapping = numerics.Merge[uint32](column, sortedIdx, fromLayout, toLayout)
	case types.Type_UINT64:
		ret, mapping = numerics.Merge[uint64](column, sortedIdx, fromLayout, toLayout)
	case types.Type_FLOAT32:
		ret, mapping = numerics.Merge[float32](column, sortedIdx, fromLayout, toLayout)
	case types.Type_FLOAT64:
		ret, mapping = numerics.Merge[float64](column, sortedIdx, fromLayout, toLayout)
	case types.Type_DATE:
		ret, mapping = numerics.Merge[types.Date](column, sortedIdx, fromLayout, toLayout)
	case types.Type_DATETIME:
		ret, mapping = numerics.Merge[types.Datetime](column, sortedIdx, fromLayout, toLayout)
	case types.Type_DECIMAL64:
		ret, mapping = numerics.Merge[types.Decimal64](column, sortedIdx, fromLayout, toLayout)
	case types.Type_DECIMAL128:
		ret, mapping = decimal128s.Merge(column, sortedIdx, fromLayout, toLayout)
	case types.Type_TIMESTAMP:
		ret, mapping = numerics.Merge[types.Timestamp](column, sortedIdx, fromLayout, toLayout)
	case types.Type_CHAR, types.Type_JSON, types.Type_VARCHAR:
		ret, mapping = varchar.Merge(column, sortedIdx, fromLayout, toLayout)
	default:
		panic(fmt.Sprintf("%s not supported", column[0].GetType().String()))
	}
	return
}

func Reshape(column []containers.Vector, fromLayout, toLayout []uint32) (ret []containers.Vector) {
	ret = make([]containers.Vector, len(toLayout))
	fromIdx := 0
	fromOffset := 0
	for i := 0; i < len(toLayout); i++ {
		ret[i] = containers.MakeVector(column[0].GetType(), column[0].Nullable())
		toOffset := 0
		for toOffset < int(toLayout[i]) {
			fromLeft := fromLayout[fromIdx] - uint32(fromOffset)
			if fromLeft == 0 {
				fromIdx++
				fromOffset = 0
				fromLeft = fromLayout[fromIdx]
			}
			length := 0
			if fromLeft < toLayout[i]-uint32(toOffset) {
				length = int(fromLeft)
			} else {
				length = int(toLayout[i]) - toOffset
			}
			cloned := column[fromIdx].CloneWindow(fromOffset, length)
			defer cloned.Close()
			ret[i].Extend(cloned)
			fromOffset += length
			toOffset += length
		}
	}
	for _, v := range column {
		v.Close()
	}
	return
}

func ShuffleColumn(column []containers.Vector, sortedIdx []uint32, fromLayout, toLayout []uint32) (ret []containers.Vector) {
	ret = Multiplex(column, sortedIdx, fromLayout, toLayout)
	return
}

//func MergeBlocksToSegment(blks []*batch.Batch, pk int) error {
//	n := len(blks) * blks[0].Vecs[pk].Length()
//	mergedSrc := make([]uint16, n)
//
//	col := make([]*vector.Vector, len(blks))
//	for i := 0; i < len(blks); i++ {
//		col[i] = blks[i].Vecs[pk]
//	}
//
//	switch blks[0].Vecs[pk].Typ.Oid {
//	case types.Type_INT8:
//		int8s.Merge(col, mergedSrc)
//	case types.Type_INT16:
//		int16s.Merge(col, mergedSrc)
//	case types.Type_INT32:
//		int32s.Merge(col, mergedSrc)
//	case types.Type_INT64:
//		int64s.Merge(col, mergedSrc)
//	case types.Type_UINT8:
//		uint8s.Merge(col, mergedSrc)
//	case types.Type_UINT16:
//		uint16s.Merge(col, mergedSrc)
//	case types.Type_UINT32:
//		uint32s.Merge(col, mergedSrc)
//	case types.Type_UINT64:
//		uint64s.Merge(col, mergedSrc)
//	case types.Type_FLOAT32:
//		float32s.Merge(col, mergedSrc)
//	case types.Type_FLOAT64:
//		float64s.Merge(col, mergedSrc)
//	case types.Type_DATE:
//		dates.Merge(col, mergedSrc)
//	case types.Type_DATETIME:
//		datetimes.Merge(col, mergedSrc)
//	case types.Type_CHAR, types.Type_JSON, types.Type_VARCHAR:
//		varchar.Merge(col, mergedSrc)
//	}
//
//	for j := 0; j < len(blks[0].Vecs); j++ {
//		if j == pk {
//			continue
//		}
//		for i := 0; i < len(blks); i++ {
//			col[i] = blks[i].Vecs[j]
//		}
//
//		switch blks[0].Vecs[j].Typ.Oid {
//		case types.Type_INT8:
//			int8s.Multiplex(col, mergedSrc)
//		case types.Type_INT16:
//			int16s.Multiplex(col, mergedSrc)
//		case types.Type_INT32:
//			int32s.Multiplex(col, mergedSrc)
//		case types.Type_INT64:
//			int64s.Multiplex(col, mergedSrc)
//		case types.Type_UINT8:
//			uint8s.Multiplex(col, mergedSrc)
//		case types.Type_UINT16:
//			uint16s.Multiplex(col, mergedSrc)
//		case types.Type_UINT32:
//			uint32s.Multiplex(col, mergedSrc)
//		case types.Type_UINT64:
//			uint64s.Multiplex(col, mergedSrc)
//		case types.Type_FLOAT32:
//			float32s.Multiplex(col, mergedSrc)
//		case types.Type_FLOAT64:
//			float64s.Multiplex(col, mergedSrc)
//		case types.Type_DATE:
//			dates.Multiplex(col, mergedSrc)
//		case types.Type_DATETIME:
//			datetimes.Multiplex(col, mergedSrc)
//		case types.Type_CHAR, types.Type_JSON, types.Type_VARCHAR:
//			varchar.Multiplex(col, mergedSrc)
//		}
//	}
//
//	return nil
//}
