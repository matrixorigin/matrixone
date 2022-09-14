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

package txnstorage

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
)

type BatchIter func() (tuple []any, isNulls []bool)

func NewBatchIter(b *batch.Batch) BatchIter {
	i := 0
	iter := func() (tuple []any, isNulls []bool) {
		if i >= b.Vecs[0].Length() {
			return
		}
		for _, vec := range b.Vecs {
			value, isNull := vectorAt(vec, i)
			tuple = append(tuple, value)
			isNulls = append(isNulls, isNull)
		}
		i++
		return
	}
	return iter
}

func vectorAt(vec *vector.Vector, i int) (value any, isNull bool) {
	if vec.IsConst() {
		i = 0
	}
	switch vec.Typ.Oid {

	case types.T_bool:
		if vec.IsScalarNull() {
			return false, true
		}
		return vec.Col.([]bool)[i], vec.GetNulls().Contains(uint64(i))

	case types.T_int8:
		if vec.IsScalarNull() {
			return int8(0), true
		}
		return vec.Col.([]int8)[i], vec.GetNulls().Contains(uint64(i))

	case types.T_int16:
		if vec.IsScalarNull() {
			return int16(0), true
		}
		return vec.Col.([]int16)[i], vec.GetNulls().Contains(uint64(i))

	case types.T_int32:
		if vec.IsScalarNull() {
			return int32(0), true
		}
		slice := vec.Col.([]int32)
		return slice[i], vec.GetNulls().Contains(uint64(i))

	case types.T_int64:
		if vec.IsScalarNull() {
			return int64(0), true
		}
		return vec.Col.([]int64)[i], vec.GetNulls().Contains(uint64(i))

	case types.T_uint8:
		if vec.IsScalarNull() {
			return uint8(0), true
		}
		return vec.Col.([]uint8)[i], vec.GetNulls().Contains(uint64(i))

	case types.T_uint16:
		if vec.IsScalarNull() {
			return uint16(0), true
		}
		return vec.Col.([]uint16)[i], vec.GetNulls().Contains(uint64(i))

	case types.T_uint32:
		if vec.IsScalarNull() {
			return uint32(0), true
		}
		return vec.Col.([]uint32)[i], vec.GetNulls().Contains(uint64(i))

	case types.T_uint64:
		if vec.IsScalarNull() {
			return uint64(0), true
		}
		return vec.Col.([]uint64)[i], vec.GetNulls().Contains(uint64(i))

	case types.T_float32:
		if vec.IsScalarNull() {
			return float32(0), true
		}
		return vec.Col.([]float32)[i], vec.GetNulls().Contains(uint64(i))

	case types.T_float64:
		if vec.IsScalarNull() {
			return float64(0), true
		}
		return vec.Col.([]float64)[i], vec.GetNulls().Contains(uint64(i))

	case types.T_tuple:
		if vec.IsScalarNull() {
			return []any{}, true
		}
		return vec.Col.([][]any)[i], vec.GetNulls().Contains(uint64(i))

	case types.T_char, types.T_varchar, types.T_json, types.T_blob:
		if vec.IsScalarNull() {
			return []byte{}, true
		}
		return vec.GetBytes(int64(i)), vec.GetNulls().Contains(uint64(i))

	case types.T_date:
		if vec.IsScalarNull() {
			var zero types.Date
			return zero, true
		}
		return vec.Col.([]types.Date)[i], vec.GetNulls().Contains(uint64(i))

	case types.T_datetime:
		if vec.IsScalarNull() {
			var zero types.Datetime
			return zero, true
		}
		return vec.Col.([]types.Datetime)[i], vec.GetNulls().Contains(uint64(i))

	case types.T_timestamp:
		if vec.IsScalarNull() {
			var zero types.Timestamp
			return zero, true
		}
		return vec.Col.([]types.Timestamp)[i], vec.GetNulls().Contains(uint64(i))

	case types.T_decimal64:
		if vec.IsScalarNull() {
			var zero types.Decimal64
			return zero, true
		}
		return vec.Col.([]types.Decimal64)[i], vec.GetNulls().Contains(uint64(i))

	case types.T_decimal128:
		if vec.IsScalarNull() {
			var zero types.Decimal128
			return zero, true
		}
		return vec.Col.([]types.Decimal128)[i], vec.GetNulls().Contains(uint64(i))

	case types.T_Rowid:
		if vec.IsScalarNull() {
			var zero types.Rowid
			return zero, true
		}
		return vec.Col.([]types.Rowid)[i], vec.GetNulls().Contains(uint64(i))

	case types.T_uuid:
		if vec.IsScalarNull() {
			var zero types.Uuid
			return zero, true
		}
		return vec.Col.([]types.Uuid)[i], vec.GetNulls().Contains(uint64(i))

	}

	panic(fmt.Errorf("unknown column type: %v", vec.Typ))
}

func vectorAppend(vec *vector.Vector, value any, heap *mheap.Mheap) {
	str, ok := value.(string)
	if ok {
		value = []byte(str)
	}
	vec.Append(value, false, heap)
}
