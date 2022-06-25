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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort/dates"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort/datetimes"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort/decimal128s"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort/decimal64s"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort/float32s"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort/float64s"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort/int16s"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort/int32s"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort/int64s"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort/int8s"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort/timestamps"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort/uint16s"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort/uint32s"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort/uint64s"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort/uint8s"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort/varchar"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/types"
)

func SortBlockColumns(cols []containers.Vector, pk int) error {
	sortedIdx := make([]uint32, cols[pk].Length())

	switch cols[pk].GetType().Oid {
	case types.Type_BOOL:
		bools.Sort(cols[pk], sortedIdx)
	case types.Type_INT8:
		int8s.Sort(cols[pk], sortedIdx)
	case types.Type_INT16:
		int16s.Sort(cols[pk], sortedIdx)
	case types.Type_INT32:
		int32s.Sort(cols[pk], sortedIdx)
	case types.Type_INT64:
		int64s.Sort(cols[pk], sortedIdx)
	case types.Type_UINT8:
		uint8s.Sort(cols[pk], sortedIdx)
	case types.Type_UINT16:
		uint16s.Sort(cols[pk], sortedIdx)
	case types.Type_UINT32:
		uint32s.Sort(cols[pk], sortedIdx)
	case types.Type_UINT64:
		uint64s.Sort(cols[pk], sortedIdx)
	case types.Type_FLOAT32:
		float32s.Sort(cols[pk], sortedIdx)
	case types.Type_FLOAT64:
		float64s.Sort(cols[pk], sortedIdx)
	case types.Type_DATE:
		dates.Sort(cols[pk], sortedIdx)
	case types.Type_DATETIME:
		datetimes.Sort(cols[pk], sortedIdx)
	case types.Type_DECIMAL64:
		decimal64s.Sort(cols[pk], sortedIdx)
	case types.Type_DECIMAL128:
		decimal128s.Sort(cols[pk], sortedIdx)
	case types.Type_TIMESTAMP:
		timestamps.Sort(cols[pk], sortedIdx)
	case types.Type_CHAR, types.Type_JSON, types.Type_VARCHAR:
		varchar.Sort(cols[pk], sortedIdx)
	default:
		panic(fmt.Sprintf("%s not supported", cols[pk].GetType().String()))
	}

	for i := 0; i < len(cols); i++ {
		if i == pk {
			continue
		}
		switch cols[i].GetType().Oid {
		case types.Type_BOOL:
			bools.Shuffle(cols[i], sortedIdx)
		case types.Type_INT8:
			int8s.Shuffle(cols[i], sortedIdx)
		case types.Type_INT16:
			int16s.Shuffle(cols[i], sortedIdx)
		case types.Type_INT32:
			int32s.Shuffle(cols[i], sortedIdx)
		case types.Type_INT64:
			int64s.Shuffle(cols[i], sortedIdx)
		case types.Type_UINT8:
			uint8s.Shuffle(cols[i], sortedIdx)
		case types.Type_UINT16:
			uint16s.Shuffle(cols[i], sortedIdx)
		case types.Type_UINT32:
			uint32s.Shuffle(cols[i], sortedIdx)
		case types.Type_UINT64:
			uint64s.Shuffle(cols[i], sortedIdx)
		case types.Type_FLOAT32:
			float32s.Shuffle(cols[i], sortedIdx)
		case types.Type_FLOAT64:
			float64s.Shuffle(cols[i], sortedIdx)
		case types.Type_DATE:
			dates.Shuffle(cols[i], sortedIdx)
		case types.Type_DATETIME:
			datetimes.Shuffle(cols[i], sortedIdx)
		case types.Type_DECIMAL64:
			decimal64s.Shuffle(cols[i], sortedIdx)
		case types.Type_DECIMAL128:
			decimal128s.Shuffle(cols[i], sortedIdx)
		case types.Type_TIMESTAMP:
			timestamps.Shuffle(cols[i], sortedIdx)
		case types.Type_CHAR, types.Type_JSON, types.Type_VARCHAR:
			varchar.Shuffle(cols[i], sortedIdx)
		default:
			panic(fmt.Sprintf("%s not supported", cols[i].GetType().String()))
		}
	}

	return nil
}

func MergeSortedColumn(column []containers.Vector, sortedIdx *[]uint32, fromLayout, toLayout []uint32) (ret []containers.Vector, mapping []uint32) {
	switch column[0].GetType().Oid {
	case types.Type_BOOL:
		ret, mapping = bools.Merge(column, sortedIdx, fromLayout, toLayout)
	case types.Type_INT8:
		ret, mapping = int8s.Merge(column, sortedIdx, fromLayout, toLayout)
	case types.Type_INT16:
		ret, mapping = int16s.Merge(column, sortedIdx, fromLayout, toLayout)
	case types.Type_INT32:
		ret, mapping = int32s.Merge(column, sortedIdx, fromLayout, toLayout)
	case types.Type_INT64:
		ret, mapping = int64s.Merge(column, sortedIdx, fromLayout, toLayout)
	case types.Type_UINT8:
		ret, mapping = uint8s.Merge(column, sortedIdx, fromLayout, toLayout)
	case types.Type_UINT16:
		ret, mapping = uint16s.Merge(column, sortedIdx, fromLayout, toLayout)
	case types.Type_UINT32:
		ret, mapping = uint32s.Merge(column, sortedIdx, fromLayout, toLayout)
	case types.Type_UINT64:
		ret, mapping = uint64s.Merge(column, sortedIdx, fromLayout, toLayout)
	case types.Type_FLOAT32:
		ret, mapping = float32s.Merge(column, sortedIdx, fromLayout, toLayout)
	case types.Type_FLOAT64:
		ret, mapping = float64s.Merge(column, sortedIdx, fromLayout, toLayout)
	case types.Type_DATE:
		ret, mapping = dates.Merge(column, sortedIdx, fromLayout, toLayout)
	case types.Type_DATETIME:
		ret, mapping = datetimes.Merge(column, sortedIdx, fromLayout, toLayout)
	case types.Type_DECIMAL64:
		ret, mapping = decimal64s.Merge(column, sortedIdx, fromLayout, toLayout)
	case types.Type_DECIMAL128:
		ret, mapping = decimal128s.Merge(column, sortedIdx, fromLayout, toLayout)
	case types.Type_TIMESTAMP:
		ret, mapping = timestamps.Merge(column, sortedIdx, fromLayout, toLayout)
	case types.Type_CHAR, types.Type_JSON, types.Type_VARCHAR:
		ret, mapping = varchar.Merge(column, sortedIdx, fromLayout, toLayout)
	default:
		panic(fmt.Sprintf("%s not supported", column[0].GetType().String()))
	}
	return
}

func Reshape(column []containers.Vector, fromLayout, toLayout []uint32) (ret []containers.Vector) {
	switch column[0].GetType().Oid {
	case types.Type_BOOL:
		ret = bools.Reshape(column, fromLayout, toLayout)
	case types.Type_INT8:
		ret = int8s.Reshape(column, fromLayout, toLayout)
	case types.Type_INT16:
		ret = int16s.Reshape(column, fromLayout, toLayout)
	case types.Type_INT32:
		ret = int32s.Reshape(column, fromLayout, toLayout)
	case types.Type_INT64:
		ret = int64s.Reshape(column, fromLayout, toLayout)
	case types.Type_UINT8:
		ret = uint8s.Reshape(column, fromLayout, toLayout)
	case types.Type_UINT16:
		ret = uint16s.Reshape(column, fromLayout, toLayout)
	case types.Type_UINT32:
		ret = uint32s.Reshape(column, fromLayout, toLayout)
	case types.Type_UINT64:
		ret = uint64s.Reshape(column, fromLayout, toLayout)
	case types.Type_FLOAT32:
		ret = float32s.Reshape(column, fromLayout, toLayout)
	case types.Type_FLOAT64:
		ret = float64s.Reshape(column, fromLayout, toLayout)
	case types.Type_DATE:
		ret = dates.Reshape(column, fromLayout, toLayout)
	case types.Type_DATETIME:
		ret = datetimes.Reshape(column, fromLayout, toLayout)
	case types.Type_DECIMAL64:
		ret = decimal64s.Reshape(column, fromLayout, toLayout)
	case types.Type_DECIMAL128:
		ret = decimal128s.Reshape(column, fromLayout, toLayout)
	case types.Type_TIMESTAMP:
		ret = timestamps.Reshape(column, fromLayout, toLayout)
	case types.Type_CHAR, types.Type_JSON, types.Type_VARCHAR:
		ret = varchar.Reshape(column, fromLayout, toLayout)
	}
	return
}

func ShuffleColumn(column []containers.Vector, sortedIdx []uint32, fromLayout, toLayout []uint32) (ret []containers.Vector) {
	switch column[0].GetType().Oid {
	case types.Type_BOOL:
		ret = bools.Multiplex(column, sortedIdx, fromLayout, toLayout)
	case types.Type_INT8:
		ret = int8s.Multiplex(column, sortedIdx, fromLayout, toLayout)
	case types.Type_INT16:
		ret = int16s.Multiplex(column, sortedIdx, fromLayout, toLayout)
	case types.Type_INT32:
		ret = int32s.Multiplex(column, sortedIdx, fromLayout, toLayout)
	case types.Type_INT64:
		ret = int64s.Multiplex(column, sortedIdx, fromLayout, toLayout)
	case types.Type_UINT8:
		ret = uint8s.Multiplex(column, sortedIdx, fromLayout, toLayout)
	case types.Type_UINT16:
		ret = uint16s.Multiplex(column, sortedIdx, fromLayout, toLayout)
	case types.Type_UINT32:
		ret = uint32s.Multiplex(column, sortedIdx, fromLayout, toLayout)
	case types.Type_UINT64:
		ret = uint64s.Multiplex(column, sortedIdx, fromLayout, toLayout)
	case types.Type_FLOAT32:
		ret = float32s.Multiplex(column, sortedIdx, fromLayout, toLayout)
	case types.Type_FLOAT64:
		ret = float64s.Multiplex(column, sortedIdx, fromLayout, toLayout)
	case types.Type_DATE:
		ret = dates.Multiplex(column, sortedIdx, fromLayout, toLayout)
	case types.Type_DATETIME:
		ret = datetimes.Multiplex(column, sortedIdx, fromLayout, toLayout)
	case types.Type_DECIMAL64:
		ret = decimal64s.Multiplex(column, sortedIdx, fromLayout, toLayout)
	case types.Type_DECIMAL128:
		ret = decimal128s.Multiplex(column, sortedIdx, fromLayout, toLayout)
	case types.Type_TIMESTAMP:
		ret = timestamps.Multiplex(column, sortedIdx, fromLayout, toLayout)
	case types.Type_CHAR, types.Type_JSON, types.Type_VARCHAR:
		ret = varchar.Multiplex(column, sortedIdx, fromLayout, toLayout)
	default:
		panic(fmt.Sprintf("%s not supported", column[0].GetType().String()))
	}
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
