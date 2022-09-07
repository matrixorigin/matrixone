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
)

type BatchIter func() (tuple []any)

func NewBatchIter(b *batch.Batch) BatchIter {
	i := 0

	iter := func() (tuple []any) {

		for _, vec := range b.Vecs {
			switch vec.Typ.Oid {

			case types.T_bool:
				col := vec.Col.([]bool)
				if i < len(col) {
					tuple = append(tuple, col[i])
				} else {
					return
				}

			case types.T_int8:
				col := vec.Col.([]int8)
				if i < len(col) {
					tuple = append(tuple, col[i])
				} else {
					return
				}

			case types.T_int16:
				col := vec.Col.([]int16)
				if i < len(col) {
					tuple = append(tuple, col[i])
				} else {
					return
				}

			case types.T_int32:
				col := vec.Col.([]int32)
				if i < len(col) {
					tuple = append(tuple, col[i])
				} else {
					return
				}

			case types.T_int64:
				col := vec.Col.([]int64)
				if i < len(col) {
					tuple = append(tuple, col[i])
				} else {
					return
				}

			case types.T_uint8:
				col := vec.Col.([]uint8)
				if i < len(col) {
					tuple = append(tuple, col[i])
				} else {
					return
				}

			case types.T_uint16:
				col := vec.Col.([]uint16)
				if i < len(col) {
					tuple = append(tuple, col[i])
				} else {
					return
				}

			case types.T_uint32:
				col := vec.Col.([]uint32)
				if i < len(col) {
					tuple = append(tuple, col[i])
				} else {
					return
				}

			case types.T_uint64:
				col := vec.Col.([]uint64)
				if i < len(col) {
					tuple = append(tuple, col[i])
				} else {
					return
				}

			case types.T_float32:
				col := vec.Col.([]float32)
				if i < len(col) {
					tuple = append(tuple, col[i])
				} else {
					return
				}

			case types.T_float64:
				col := vec.Col.([]float64)
				if i < len(col) {
					tuple = append(tuple, col[i])
				} else {
					return
				}

			case types.T_tuple:
				col := vec.Col.([][]any)
				if i < len(col) {
					tuple = append(tuple, col[i])
				} else {
					return
				}

			case types.T_char, types.T_varchar, types.T_json, types.T_blob:
				if i < vec.Length() {
					tuple = append(tuple, vec.GetBytes(int64(i)))
				} else {
					return
				}

			case types.T_date:
				col := vec.Col.([]types.Date)
				if i < len(col) {
					tuple = append(tuple, col[i])
				} else {
					return
				}

			case types.T_datetime:
				col := vec.Col.([]types.Datetime)
				if i < len(col) {
					tuple = append(tuple, col[i])
				} else {
					return
				}

			case types.T_timestamp:
				col := vec.Col.([]types.Timestamp)
				if i < len(col) {
					tuple = append(tuple, col[i])
				} else {
					return
				}

			case types.T_decimal64:
				col := vec.Col.([]types.Decimal64)
				if i < len(col) {
					tuple = append(tuple, col[i])
				} else {
					return
				}

			case types.T_decimal128:
				col := vec.Col.([]types.Decimal128)
				if i < len(col) {
					tuple = append(tuple, col[i])
				} else {
					return
				}

			}
		}

		i++

		return
	}

	return iter
}

func vectorAt(vec *vector.Vector, i int) any {
	switch vec.Typ.Oid {

	case types.T_bool:
		return vec.Col.([]bool)[i]

	case types.T_int8:
		return vec.Col.([]int8)[i]

	case types.T_int16:
		return vec.Col.([]int16)[i]

	case types.T_int32:
		return vec.Col.([]int32)[i]

	case types.T_int64:
		return vec.Col.([]int64)[i]

	case types.T_uint8:
		return vec.Col.([]uint8)[i]

	case types.T_uint16:
		return vec.Col.([]uint16)[i]

	case types.T_uint32:
		return vec.Col.([]uint32)[i]

	case types.T_uint64:
		return vec.Col.([]uint64)[i]

	case types.T_float32:
		return vec.Col.([]float32)[i]

	case types.T_float64:
		return vec.Col.([]float64)[i]

	case types.T_tuple:
		return vec.Col.([][]any)[i]

	case types.T_char, types.T_varchar, types.T_json, types.T_blob:
		return vec.GetBytes(int64(i))

	case types.T_date:
		return vec.Col.([]types.Date)[i]

	case types.T_datetime:
		return vec.Col.([]types.Datetime)[i]

	case types.T_timestamp:
		return vec.Col.([]types.Timestamp)[i]

	case types.T_decimal64:
		return vec.Col.([]types.Decimal64)[i]

	case types.T_decimal128:
		return vec.Col.([]types.Decimal128)[i]

	}

	panic(fmt.Errorf("unknown column type: %v", vec.Typ))
}
