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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort/dates"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort/datetimes"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort/float32s"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort/float64s"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort/int16s"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort/int32s"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort/int64s"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort/int8s"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort/uint16s"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort/uint32s"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort/uint64s"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort/uint8s"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort/varchar"
)

func SortBlockColumns(cols []*vector.Vector, pk int) error {
	sortedIdx := make([]uint32, vector.Length(cols[pk]))

	switch cols[pk].Typ.Oid {
	case types.T_int8:
		int8s.Sort(cols[pk], sortedIdx)
	case types.T_int16:
		int16s.Sort(cols[pk], sortedIdx)
	case types.T_int32:
		int32s.Sort(cols[pk], sortedIdx)
	case types.T_int64:
		int64s.Sort(cols[pk], sortedIdx)
	case types.T_uint8:
		uint8s.Sort(cols[pk], sortedIdx)
	case types.T_uint16:
		uint16s.Sort(cols[pk], sortedIdx)
	case types.T_uint32:
		uint32s.Sort(cols[pk], sortedIdx)
	case types.T_uint64:
		uint64s.Sort(cols[pk], sortedIdx)
	case types.T_float32:
		float32s.Sort(cols[pk], sortedIdx)
	case types.T_float64:
		float64s.Sort(cols[pk], sortedIdx)
	case types.T_date:
		dates.Sort(cols[pk], sortedIdx)
	case types.T_datetime:
		datetimes.Sort(cols[pk], sortedIdx)
	case types.T_char, types.T_json, types.T_varchar:
		varchar.Sort(cols[pk], sortedIdx)
	}

	for i := 0; i < len(cols); i++ {
		if i == pk {
			continue
		}
		switch cols[i].Typ.Oid {
		case types.T_int8:
			int8s.Shuffle(cols[i], sortedIdx)
		case types.T_int16:
			int16s.Shuffle(cols[i], sortedIdx)
		case types.T_int32:
			int32s.Shuffle(cols[i], sortedIdx)
		case types.T_int64:
			int64s.Shuffle(cols[i], sortedIdx)
		case types.T_uint8:
			uint8s.Shuffle(cols[i], sortedIdx)
		case types.T_uint16:
			uint16s.Shuffle(cols[i], sortedIdx)
		case types.T_uint32:
			uint32s.Shuffle(cols[i], sortedIdx)
		case types.T_uint64:
			uint64s.Shuffle(cols[i], sortedIdx)
		case types.T_float32:
			float32s.Shuffle(cols[i], sortedIdx)
		case types.T_float64:
			float64s.Shuffle(cols[i], sortedIdx)
		case types.T_date:
			dates.Shuffle(cols[i], sortedIdx)
		case types.T_datetime:
			datetimes.Shuffle(cols[i], sortedIdx)
		case types.T_char, types.T_json, types.T_varchar:
			varchar.Shuffle(cols[i], sortedIdx)
		}
	}

	return nil
}

func MergeSortedColumn(column []*vector.Vector, sortedIdx *[]uint32, fromLayout, toLayout []uint32) (ret []*vector.Vector, mapping []uint32) {
	switch column[0].Typ.Oid {
	case types.T_int8:
		ret, mapping = int8s.Merge(column, sortedIdx, fromLayout, toLayout)
	case types.T_int16:
		ret, mapping = int16s.Merge(column, sortedIdx, fromLayout, toLayout)
	case types.T_int32:
		ret, mapping = int32s.Merge(column, sortedIdx, fromLayout, toLayout)
	case types.T_int64:
		ret, mapping = int64s.Merge(column, sortedIdx, fromLayout, toLayout)
	case types.T_uint8:
		ret, mapping = uint8s.Merge(column, sortedIdx, fromLayout, toLayout)
	case types.T_uint16:
		ret, mapping = uint16s.Merge(column, sortedIdx, fromLayout, toLayout)
	case types.T_uint32:
		ret, mapping = uint32s.Merge(column, sortedIdx, fromLayout, toLayout)
	case types.T_uint64:
		ret, mapping = uint64s.Merge(column, sortedIdx, fromLayout, toLayout)
	case types.T_float32:
		ret, mapping = float32s.Merge(column, sortedIdx, fromLayout, toLayout)
	case types.T_float64:
		ret, mapping = float64s.Merge(column, sortedIdx, fromLayout, toLayout)
	case types.T_date:
		ret, mapping = dates.Merge(column, sortedIdx, fromLayout, toLayout)
	case types.T_datetime:
		ret, mapping = datetimes.Merge(column, sortedIdx, fromLayout, toLayout)
	case types.T_char, types.T_json, types.T_varchar:
		ret, mapping = varchar.Merge(column, sortedIdx, fromLayout, toLayout)
	}
	return
}

func ShuffleColumn(column []*vector.Vector, sortedIdx []uint32, fromLayout, toLayout []uint32) (ret []*vector.Vector) {
	switch column[0].Typ.Oid {
	case types.T_int8:
		ret = int8s.Multiplex(column, sortedIdx, fromLayout, toLayout)
	case types.T_int16:
		ret = int16s.Multiplex(column, sortedIdx, fromLayout, toLayout)
	case types.T_int32:
		ret = int32s.Multiplex(column, sortedIdx, fromLayout, toLayout)
	case types.T_int64:
		ret = int64s.Multiplex(column, sortedIdx, fromLayout, toLayout)
	case types.T_uint8:
		ret = uint8s.Multiplex(column, sortedIdx, fromLayout, toLayout)
	case types.T_uint16:
		ret = uint16s.Multiplex(column, sortedIdx, fromLayout, toLayout)
	case types.T_uint32:
		ret = uint32s.Multiplex(column, sortedIdx, fromLayout, toLayout)
	case types.T_uint64:
		ret = uint64s.Multiplex(column, sortedIdx, fromLayout, toLayout)
	case types.T_float32:
		ret = float32s.Multiplex(column, sortedIdx, fromLayout, toLayout)
	case types.T_float64:
		ret = float64s.Multiplex(column, sortedIdx, fromLayout, toLayout)
	case types.T_date:
		ret = dates.Multiplex(column, sortedIdx, fromLayout, toLayout)
	case types.T_datetime:
		ret = datetimes.Multiplex(column, sortedIdx, fromLayout, toLayout)
	case types.T_char, types.T_json, types.T_varchar:
		ret = varchar.Multiplex(column, sortedIdx, fromLayout, toLayout)
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
//	case types.T_int8:
//		int8s.Merge(col, mergedSrc)
//	case types.T_int16:
//		int16s.Merge(col, mergedSrc)
//	case types.T_int32:
//		int32s.Merge(col, mergedSrc)
//	case types.T_int64:
//		int64s.Merge(col, mergedSrc)
//	case types.T_uint8:
//		uint8s.Merge(col, mergedSrc)
//	case types.T_uint16:
//		uint16s.Merge(col, mergedSrc)
//	case types.T_uint32:
//		uint32s.Merge(col, mergedSrc)
//	case types.T_uint64:
//		uint64s.Merge(col, mergedSrc)
//	case types.T_float32:
//		float32s.Merge(col, mergedSrc)
//	case types.T_float64:
//		float64s.Merge(col, mergedSrc)
//	case types.T_date:
//		dates.Merge(col, mergedSrc)
//	case types.T_datetime:
//		datetimes.Merge(col, mergedSrc)
//	case types.T_char, types.T_json, types.T_varchar:
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
//		case types.T_int8:
//			int8s.Multiplex(col, mergedSrc)
//		case types.T_int16:
//			int16s.Multiplex(col, mergedSrc)
//		case types.T_int32:
//			int32s.Multiplex(col, mergedSrc)
//		case types.T_int64:
//			int64s.Multiplex(col, mergedSrc)
//		case types.T_uint8:
//			uint8s.Multiplex(col, mergedSrc)
//		case types.T_uint16:
//			uint16s.Multiplex(col, mergedSrc)
//		case types.T_uint32:
//			uint32s.Multiplex(col, mergedSrc)
//		case types.T_uint64:
//			uint64s.Multiplex(col, mergedSrc)
//		case types.T_float32:
//			float32s.Multiplex(col, mergedSrc)
//		case types.T_float64:
//			float64s.Multiplex(col, mergedSrc)
//		case types.T_date:
//			dates.Multiplex(col, mergedSrc)
//		case types.T_datetime:
//			datetimes.Multiplex(col, mergedSrc)
//		case types.T_char, types.T_json, types.T_varchar:
//			varchar.Multiplex(col, mergedSrc)
//		}
//	}
//
//	return nil
//}
