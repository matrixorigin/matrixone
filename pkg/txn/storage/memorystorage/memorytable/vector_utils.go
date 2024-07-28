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

package memorytable

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// VectorAt returns a nullable value at specifed index
func VectorAt(vec *vector.Vector, i int) (value Nullable) {
	if vec.IsConst() {
		i = 0
	}

	switch vec.GetType().Oid {

	case types.T_bool:
		return vectorAtFixed[bool](vec, i)

	case types.T_bit:
		return vectorAtFixed[uint64](vec, i)

	case types.T_int8:
		return vectorAtFixed[int8](vec, i)

	case types.T_int16:
		return vectorAtFixed[int16](vec, i)

	case types.T_int32:
		return vectorAtFixed[int32](vec, i)

	case types.T_int64:
		return vectorAtFixed[int64](vec, i)

	case types.T_uint8:
		return vectorAtFixed[uint8](vec, i)

	case types.T_uint16:
		return vectorAtFixed[uint16](vec, i)

	case types.T_uint32:
		return vectorAtFixed[uint32](vec, i)

	case types.T_uint64:
		return vectorAtFixed[uint64](vec, i)

	case types.T_float32:
		return vectorAtFixed[float32](vec, i)

	case types.T_float64:
		return vectorAtFixed[float64](vec, i)

	case types.T_tuple:
		return vectorAtFixed[[]any](vec, i)

	case types.T_date:
		return vectorAtFixed[types.Date](vec, i)

	case types.T_time:
		return vectorAtFixed[types.Time](vec, i)

	case types.T_datetime:
		return vectorAtFixed[types.Datetime](vec, i)

	case types.T_enum:
		return vectorAtFixed[types.Enum](vec, i)

	case types.T_timestamp:
		return vectorAtFixed[types.Timestamp](vec, i)

	case types.T_decimal64:
		return vectorAtFixed[types.Decimal64](vec, i)

	case types.T_decimal128:
		return vectorAtFixed[types.Decimal128](vec, i)

	case types.T_TS:
		return vectorAtFixed[types.TS](vec, i)

	case types.T_Rowid:
		return vectorAtFixed[types.Rowid](vec, i)
	case types.T_Blockid:
		return vectorAtFixed[types.Blockid](vec, i)
	case types.T_uuid:
		return vectorAtFixed[types.Uuid](vec, i)

	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_json, types.T_blob, types.T_text,
		types.T_array_float32, types.T_array_float64, types.T_datalink:
		if vec.IsConstNull() {
			value = Nullable{
				IsNull: true,
				Value:  []byte{},
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vec.GetBytesAt(i),
		}
		return

	}

	panic(fmt.Sprintf("unknown column type: %v", *vec.GetType()))
}

func vectorAtFixed[T any](vec *vector.Vector, i int) (value Nullable) {
	if vec.IsConstNull() {
		var zero T
		value = Nullable{
			IsNull: true,
			Value:  zero,
		}
		return
	}

	slice := vector.MustFixedCol[T](vec)
	if len(slice) != vec.Length() {
		panic(fmt.Sprintf(
			"bad vector length, expected %d, got %d",
			vec.Length(),
			len(slice),
		))
	}
	value = Nullable{
		IsNull: vec.GetNulls().Contains(uint64(i)),
		Value:  slice[i],
	}
	return
}
