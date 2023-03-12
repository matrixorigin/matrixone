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

//TODO move to vector?

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
		if vec.IsConstNull() {
			value = Nullable{
				IsNull: true,
				Value:  false,
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vector.MustFixedCol[bool](vec)[i],
		}
		return

	case types.T_int8:
		if vec.IsConstNull() {
			value = Nullable{
				IsNull: true,
				Value:  int8(0),
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vector.MustFixedCol[int8](vec)[i],
		}
		return

	case types.T_int16:
		if vec.IsConstNull() {
			value = Nullable{
				IsNull: true,
				Value:  int16(0),
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vector.MustFixedCol[int16](vec)[i],
		}
		return

	case types.T_int32:
		if vec.IsConstNull() {
			value = Nullable{
				IsNull: true,
				Value:  int32(0),
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vector.MustFixedCol[int32](vec)[i],
		}
		return

	case types.T_int64:
		if vec.IsConstNull() {
			value = Nullable{
				IsNull: true,
				Value:  int64(0),
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vector.MustFixedCol[int64](vec)[i],
		}
		return

	case types.T_uint8:
		if vec.IsConstNull() {
			value = Nullable{
				IsNull: true,
				Value:  uint8(0),
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vector.MustFixedCol[uint8](vec)[i],
		}
		return

	case types.T_uint16:
		if vec.IsConstNull() {
			value = Nullable{
				IsNull: true,
				Value:  uint16(0),
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vector.MustFixedCol[uint16](vec)[i],
		}
		return

	case types.T_uint32:
		if vec.IsConstNull() {
			value = Nullable{
				IsNull: true,
				Value:  uint32(0),
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vector.MustFixedCol[uint32](vec)[i],
		}
		return

	case types.T_uint64:
		if vec.IsConstNull() {
			value = Nullable{
				IsNull: true,
				Value:  uint64(0),
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vector.MustFixedCol[uint64](vec)[i],
		}
		return

	case types.T_float32:
		if vec.IsConstNull() {
			value = Nullable{
				IsNull: true,
				Value:  float32(0),
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vector.MustFixedCol[float32](vec)[i],
		}
		return

	case types.T_float64:
		if vec.IsConstNull() {
			value = Nullable{
				IsNull: true,
				Value:  float64(0),
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vector.MustFixedCol[float64](vec)[i],
		}
		return

	case types.T_tuple:
		if vec.IsConstNull() {
			value = Nullable{
				IsNull: true,
				Value:  []any{},
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vector.MustFixedCol[[]any](vec)[i],
		}
		return

	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_json, types.T_blob, types.T_text:
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

	case types.T_date:
		if vec.IsConstNull() {
			var zero types.Date
			value = Nullable{
				IsNull: true,
				Value:  zero,
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vector.MustFixedCol[types.Date](vec)[i],
		}
		return

	case types.T_time:
		if vec.IsConstNull() {
			var zero types.Time
			value = Nullable{
				IsNull: true,
				Value:  zero,
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vector.MustFixedCol[types.Time](vec)[i],
		}
		return

	case types.T_datetime:
		if vec.IsConstNull() {
			var zero types.Datetime
			value = Nullable{
				IsNull: true,
				Value:  zero,
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vector.MustFixedCol[types.Datetime](vec)[i],
		}
		return

	case types.T_timestamp:
		if vec.IsConstNull() {
			var zero types.Timestamp
			value = Nullable{
				IsNull: true,
				Value:  zero,
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vector.MustFixedCol[types.Timestamp](vec)[i],
		}
		return

	case types.T_decimal64:
		if vec.IsConstNull() {
			var zero types.Decimal64
			value = Nullable{
				IsNull: true,
				Value:  zero,
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vector.MustFixedCol[types.Decimal64](vec)[i],
		}
		return

	case types.T_decimal128:
		if vec.IsConstNull() {
			var zero types.Decimal128
			value = Nullable{
				IsNull: true,
				Value:  zero,
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vector.MustFixedCol[types.Decimal128](vec)[i],
		}
		return

	case types.T_TS:
		if vec.IsConstNull() {
			var zero types.TS
			value = Nullable{
				IsNull: true,
				Value:  zero,
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vector.MustFixedCol[types.TS](vec)[i],
		}
		return

	case types.T_Rowid:
		if vec.IsConstNull() {
			var zero types.Rowid
			value = Nullable{
				IsNull: true,
				Value:  zero,
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vector.MustFixedCol[types.Rowid](vec)[i],
		}
		return

	case types.T_uuid:
		if vec.IsConstNull() {
			var zero types.Uuid
			value = Nullable{
				IsNull: true,
				Value:  zero,
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vector.MustFixedCol[types.Uuid](vec)[i],
		}
		return

	}

	panic(fmt.Sprintf("unknown column type: %v", *vec.GetType()))
}
