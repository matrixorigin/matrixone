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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

func SortBlockColumns(
	cols []containers.Vector, pk int, pool *containers.VectorPool,
) ([]int32, error) {
	sortedIdx := make([]int32, cols[pk].Length())

	switch cols[pk].GetType().Oid {
	case types.T_bool:
		Sort(cols[pk], boolLess, sortedIdx)
	case types.T_bit:
		Sort(cols[pk], numericLess[uint64], sortedIdx)
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
	case types.T_enum:
		Sort(cols[pk], numericLess[types.Enum], sortedIdx)
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
	case types.T_Blockid:
		Sort(cols[pk], blockidLess, sortedIdx)
	case types.T_char, types.T_json, types.T_varchar,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_text,
		types.T_array_float32, types.T_array_float64:
		Sort(cols[pk], bytesLess, sortedIdx)
	//TODO: check if I should add T_array here? Is bytesLess enough?
	default:
		panic(fmt.Sprintf("%s not supported", cols[pk].GetType().String()))
	}

	for i := 0; i < len(cols); i++ {
		if i == pk {
			continue
		}
		cols[i] = Shuffle(cols[i], sortedIdx, pool)
	}
	return sortedIdx, nil
}

// MergeColumn merge sorted column. It modify sortidx and mapping as merging record. After that, vector in `column` will be closed. Used by Tn only
func MergeColumn(
	column []containers.Vector,
	sortidx, mapping []uint32,
	fromLayout, toLayout []uint32,
	pool *containers.VectorPool,
) (ret []containers.Vector) {

	columns := make([]*vector.Vector, len(column))
	for i := range column {
		columns[i] = column[i].GetDownstreamVector()
	}

	ret = make([]containers.Vector, len(toLayout))
	retvec := make([]*vector.Vector, len(toLayout))
	for i := 0; i < len(toLayout); i++ {
		ret[i] = pool.GetVector(column[0].GetType())
		retvec[i] = ret[i].GetDownstreamVector()
	}

	Merge(columns, retvec, sortidx, mapping, fromLayout, toLayout, ret[0].GetAllocator())

	for _, v := range column {
		v.Close()
	}

	return
}

// ShuffleColumn shuffle column according to sortedIdx.  After that, vector in `column` will be closed. Used by Tn only
func ShuffleColumn(
	column []containers.Vector, sortedIdx []uint32, fromLayout, toLayout []uint32, pool *containers.VectorPool,
) (ret []containers.Vector) {

	columns := make([]*vector.Vector, len(column))
	for i := range column {
		columns[i] = column[i].GetDownstreamVector()
	}

	ret = make([]containers.Vector, len(toLayout))
	retvec := make([]*vector.Vector, len(toLayout))
	for i := 0; i < len(toLayout); i++ {
		ret[i] = pool.GetVector(column[0].GetType())
		retvec[i] = ret[i].GetDownstreamVector()
	}

	Multiplex(columns, retvec, sortedIdx, fromLayout, toLayout, ret[0].GetAllocator())

	for _, v := range column {
		v.Close()
	}

	return
}

// ReshapeColumn rearrange array according to toLayout. After that, vector in `column` will be closed. Used by Tn only
func ReshapeColumn(
	column []containers.Vector, fromLayout, toLayout []uint32, pool *containers.VectorPool,
) (ret []containers.Vector) {
	columns := make([]*vector.Vector, len(column))
	for i := range column {
		columns[i] = column[i].GetDownstreamVector()
	}
	ret = make([]containers.Vector, len(toLayout))
	retvec := make([]*vector.Vector, len(toLayout))
	for i := 0; i < len(toLayout); i++ {
		ret[i] = pool.GetVector(column[0].GetType())
		retvec[i] = ret[i].GetDownstreamVector()
	}

	Reshape(columns, retvec, fromLayout, toLayout, ret[0].GetAllocator())

	for _, v := range column {
		v.Close()
	}
	return
}
