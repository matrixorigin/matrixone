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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

func SortBlockColumns(cols []containers.Vector, pk int) ([]uint32, error) {
	sortedIdx := make([]uint32, cols[pk].Length())

	// switch cols[pk].GetType().Oid {
	// case types.T_bool:
	// 	bools.Sort(cols[pk], sortedIdx)
	// case types.T_int8:
	// 	numerics.Sort[int8](cols[pk], sortedIdx)
	// case types.T_int16:
	// 	numerics.Sort[int16](cols[pk], sortedIdx)
	// case types.T_int32:
	// 	numerics.Sort[int32](cols[pk], sortedIdx)
	// case types.T_int64:
	// 	numerics.Sort[int64](cols[pk], sortedIdx)
	// case types.T_uint8:
	// 	numerics.Sort[uint8](cols[pk], sortedIdx)
	// case types.T_uint16:
	// 	numerics.Sort[uint16](cols[pk], sortedIdx)
	// case types.T_uint32:
	// 	numerics.Sort[uint32](cols[pk], sortedIdx)
	// case types.T_uint64:
	// 	numerics.Sort[uint64](cols[pk], sortedIdx)
	// case types.T_float32:
	// 	numerics.Sort[float32](cols[pk], sortedIdx)
	// case types.T_float64:
	// 	numerics.Sort[float64](cols[pk], sortedIdx)
	// case types.T_date:
	// 	numerics.Sort[types.Date](cols[pk], sortedIdx)
	// case types.T_time:
	// 	numerics.Sort[types.Time](cols[pk], sortedIdx)
	// case types.T_datetime:
	// 	numerics.Sort[types.Datetime](cols[pk], sortedIdx)
	// case types.T_decimal64:
	// 	decimal64s.Sort(cols[pk], sortedIdx)
	// case types.T_decimal128:
	// 	decimal128s.Sort(cols[pk], sortedIdx)
	// case types.T_timestamp:
	// 	numerics.Sort[types.Timestamp](cols[pk], sortedIdx)
	// case types.T_uuid:
	// 	uuids.Sort(cols[pk], sortedIdx)
	// case types.T_TS:
	// 	txnts.Sort(cols[pk], sortedIdx)
	// case types.T_Rowid:
	// 	rowid.Sort(cols[pk], sortedIdx)
	// case types.T_char, types.T_json, types.T_varchar, types.T_blob, types.T_text:
	// 	varchar.Sort(cols[pk], sortedIdx)
	// default:
	// 	panic(fmt.Sprintf("%s not supported", cols[pk].GetType().String()))
	// }

	switch cols[pk].GetType().Oid {
	case types.T_bool:
		Sort(cols[pk], boolLess, sortedIdx)
	case types.T_int8:
		Sort(cols[pk], numericLess[int8], sortedIdx)
	case types.T_int16:
		Sort(cols[pk], numericLess[int16], sortedIdx)
	case types.T_int32:
		Sort(cols[pk], numericLess[int32], sortedIdx)
	case types.T_int64:
		Sort(cols[pk], numericLess[int64], sortedIdx)
	case types.T_uint8:
		Sort(cols[pk], numericLess[uint8], sortedIdx)
	case types.T_uint16:
		Sort(cols[pk], numericLess[uint16], sortedIdx)
	case types.T_uint32:
		Sort(cols[pk], numericLess[uint32], sortedIdx)
	case types.T_uint64:
		Sort(cols[pk], numericLess[uint64], sortedIdx)
	case types.T_float32:
		Sort(cols[pk], numericLess[float32], sortedIdx)
	case types.T_float64:
		Sort(cols[pk], numericLess[float64], sortedIdx)
	case types.T_date:
		Sort(cols[pk], numericLess[types.Date], sortedIdx)
	case types.T_time:
		Sort(cols[pk], numericLess[types.Time], sortedIdx)
	case types.T_datetime:
		Sort(cols[pk], numericLess[types.Datetime], sortedIdx)
	case types.T_timestamp:
		Sort(cols[pk], numericLess[types.Timestamp], sortedIdx)
	case types.T_decimal64:
		Sort(cols[pk], ltTypeLess[types.Decimal64], sortedIdx)
	case types.T_decimal128:
		Sort(cols[pk], ltTypeLess[types.Decimal128], sortedIdx)
	case types.T_uuid:
		Sort(cols[pk], ltTypeLess[types.Uuid], sortedIdx)
	case types.T_TS:
		Sort(cols[pk], tsLess, sortedIdx)
	case types.T_Rowid:
		Sort(cols[pk], rowidLess, sortedIdx)
	case types.T_char, types.T_json, types.T_varchar,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_text:
		Sort(cols[pk], bytesLess, sortedIdx)
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
	// switch column[0].GetType().Oid {
	// case types.T_bool:
	// 	ret, mapping = bools.Merge(column, sortedIdx, fromLayout, toLayout)
	// case types.T_int8:
	// 	ret, mapping = numerics.Merge[int8](column, sortedIdx, fromLayout, toLayout)
	// case types.T_int16:
	// 	ret, mapping = numerics.Merge[int16](column, sortedIdx, fromLayout, toLayout)
	// case types.T_int32:
	// 	ret, mapping = numerics.Merge[int32](column, sortedIdx, fromLayout, toLayout)
	// case types.T_int64:
	// 	ret, mapping = numerics.Merge[int64](column, sortedIdx, fromLayout, toLayout)
	// case types.T_uint8:
	// 	ret, mapping = numerics.Merge[uint8](column, sortedIdx, fromLayout, toLayout)
	// case types.T_uint16:
	// 	ret, mapping = numerics.Merge[uint16](column, sortedIdx, fromLayout, toLayout)
	// case types.T_uint32:
	// 	ret, mapping = numerics.Merge[uint32](column, sortedIdx, fromLayout, toLayout)
	// case types.T_uint64:
	// 	ret, mapping = numerics.Merge[uint64](column, sortedIdx, fromLayout, toLayout)
	// case types.T_float32:
	// 	ret, mapping = numerics.Merge[float32](column, sortedIdx, fromLayout, toLayout)
	// case types.T_float64:
	// 	ret, mapping = numerics.Merge[float64](column, sortedIdx, fromLayout, toLayout)
	// case types.T_date:
	// 	ret, mapping = numerics.Merge[types.Date](column, sortedIdx, fromLayout, toLayout)
	// case types.T_time:
	// 	ret, mapping = numerics.Merge[types.Time](column, sortedIdx, fromLayout, toLayout)
	// case types.T_datetime:
	// 	ret, mapping = numerics.Merge[types.Datetime](column, sortedIdx, fromLayout, toLayout)
	// case types.T_decimal64:
	// 	ret, mapping = decimal64s.Merge(column, sortedIdx, fromLayout, toLayout)
	// case types.T_decimal128:
	// 	ret, mapping = decimal128s.Merge(column, sortedIdx, fromLayout, toLayout)
	// case types.T_uuid:
	// 	ret, mapping = uuids.Merge(column, sortedIdx, fromLayout, toLayout)
	// case types.T_timestamp:
	// 	ret, mapping = numerics.Merge[types.Timestamp](column, sortedIdx, fromLayout, toLayout)
	// case types.T_TS:
	// 	ret, mapping = txnts.Merge(column, sortedIdx, fromLayout, toLayout)
	// case types.T_Rowid:
	// 	ret, mapping = rowid.Merge(column, sortedIdx, fromLayout, toLayout)
	// case types.T_char, types.T_json, types.T_varchar, types.T_blob, types.T_text:
	// 	ret, mapping = varchar.Merge(column, sortedIdx, fromLayout, toLayout)
	// default:
	// 	panic(fmt.Sprintf("%s not supported", column[0].GetType().String()))
	// }
	// return
	switch column[0].GetType().Oid {
	case types.T_bool:
		ret, mapping = Merge(column, sortedIdx, boolLess, fromLayout, toLayout)
	case types.T_int8:
		ret, mapping = Merge(column, sortedIdx, numericLess[int8], fromLayout, toLayout)
	case types.T_int16:
		ret, mapping = Merge(column, sortedIdx, numericLess[int16], fromLayout, toLayout)
	case types.T_int32:
		ret, mapping = Merge(column, sortedIdx, numericLess[int32], fromLayout, toLayout)
	case types.T_int64:
		ret, mapping = Merge(column, sortedIdx, numericLess[int64], fromLayout, toLayout)
	case types.T_uint8:
		ret, mapping = Merge(column, sortedIdx, numericLess[uint8], fromLayout, toLayout)
	case types.T_uint16:
		ret, mapping = Merge(column, sortedIdx, numericLess[uint16], fromLayout, toLayout)
	case types.T_uint32:
		ret, mapping = Merge(column, sortedIdx, numericLess[uint32], fromLayout, toLayout)
	case types.T_uint64:
		ret, mapping = Merge(column, sortedIdx, numericLess[uint64], fromLayout, toLayout)
	case types.T_float32:
		ret, mapping = Merge(column, sortedIdx, numericLess[float32], fromLayout, toLayout)
	case types.T_float64:
		ret, mapping = Merge(column, sortedIdx, numericLess[float64], fromLayout, toLayout)
	case types.T_date:
		ret, mapping = Merge(column, sortedIdx, numericLess[types.Date], fromLayout, toLayout)
	case types.T_time:
		ret, mapping = Merge(column, sortedIdx, numericLess[types.Time], fromLayout, toLayout)
	case types.T_datetime:
		ret, mapping = Merge(column, sortedIdx, numericLess[types.Datetime], fromLayout, toLayout)
	case types.T_timestamp:
		ret, mapping = Merge(column, sortedIdx, numericLess[types.Timestamp], fromLayout, toLayout)
	case types.T_decimal64:
		ret, mapping = Merge(column, sortedIdx, ltTypeLess[types.Decimal64], fromLayout, toLayout)
	case types.T_decimal128:
		ret, mapping = Merge(column, sortedIdx, ltTypeLess[types.Decimal128], fromLayout, toLayout)
	case types.T_uuid:
		ret, mapping = Merge(column, sortedIdx, ltTypeLess[types.Uuid], fromLayout, toLayout)
	case types.T_TS:
		ret, mapping = Merge(column, sortedIdx, tsLess, fromLayout, toLayout)
	case types.T_Rowid:
		ret, mapping = Merge(column, sortedIdx, rowidLess, fromLayout, toLayout)
	case types.T_char, types.T_json, types.T_varchar,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_text:
		ret, mapping = Merge(column, sortedIdx, bytesLess, fromLayout, toLayout)
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
//	case types.T_char, types.T_json, types.T_varchar, types.T_blob:
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
//		case types.T_char, types.T_json, types.T_varchar, types.T_blob:
//			varchar.Multiplex(col, mergedSrc)
//		}
//	}
//
//	return nil
//}
