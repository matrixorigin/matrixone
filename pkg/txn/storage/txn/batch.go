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
	"github.com/matrixorigin/matrixone/pkg/common/container/batch"
	"github.com/matrixorigin/matrixone/pkg/common/container/types"
)

type BatchIter func() (cols []any)

func NewBatchIter(b *batch.Batch) BatchIter {
	i := 0

	iter := func() (cols []any) {
		for _, vec := range b.Vecs {
			switch vec.Typ.Oid {

			case types.T_bool:
				cols = append(cols, vec.Col.([]bool)[i])

			case types.T_int8:
				cols = append(cols, vec.Col.([]int8)[i])

			case types.T_int16:
				cols = append(cols, vec.Col.([]int16)[i])

			case types.T_int32:
				cols = append(cols, vec.Col.([]int32)[i])

			case types.T_int64:
				cols = append(cols, vec.Col.([]int64)[i])

			case types.T_uint8:
				cols = append(cols, vec.Col.([]uint8)[i])

			case types.T_uint16:
				cols = append(cols, vec.Col.([]uint16)[i])

			case types.T_uint32:
				cols = append(cols, vec.Col.([]uint32)[i])

			case types.T_uint64:
				cols = append(cols, vec.Col.([]uint64)[i])

			case types.T_float32:
				cols = append(cols, vec.Col.([]float32)[i])

			case types.T_float64:
				cols = append(cols, vec.Col.([]float64)[i])

			case types.T_sel:
				cols = append(cols, vec.Col.([]int64)[i])

			case types.T_tuple:
				cols = append(cols, vec.Col.([][]any)[i])

			case types.T_char, types.T_varchar, types.T_json, types.T_blob:
				info := vec.Col.(*types.Bytes)
				str := vec.Data[info.Offsets[i] : info.Offsets[i]+info.Lengths[i]]
				cols = append(cols, str)

			case types.T_date:
				cols = append(cols, vec.Col.([]types.Date)[i])

			case types.T_datetime:
				cols = append(cols, vec.Col.([]types.Datetime)[i])

			case types.T_timestamp:
				cols = append(cols, vec.Col.([]types.Timestamp)[i])

			case types.T_decimal64:
				cols = append(cols, vec.Col.([]types.Decimal64)[i])

			case types.T_decimal128:
				cols = append(cols, vec.Col.([]types.Decimal128)[i])

			}
		}

		i++

		return
	}

	return iter
}
