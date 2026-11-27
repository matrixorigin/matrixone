// Copyright 2026 Matrix Origin
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

package containers

// The switch implementations of getNonNullValue and UpdateValue before they
// used the vector type-ops table, kept verbatim as the oracle for
// TestValueAccessMatchesLegacy. Delete this file once the table has been the
// only implementation for a release.

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func legacyGetNonNullValue(col *vector.Vector, row uint32) any {

	switch col.GetType().Oid {
	case types.T_bool:
		return vector.GetFixedAtNoTypeCheck[bool](col, int(row))
	case types.T_bit:
		return vector.GetFixedAtNoTypeCheck[uint64](col, int(row))
	case types.T_int8:
		return vector.GetFixedAtNoTypeCheck[int8](col, int(row))
	case types.T_int16:
		return vector.GetFixedAtNoTypeCheck[int16](col, int(row))
	case types.T_int32:
		return vector.GetFixedAtNoTypeCheck[int32](col, int(row))
	case types.T_int64:
		return vector.GetFixedAtNoTypeCheck[int64](col, int(row))
	case types.T_uint8:
		return vector.GetFixedAtNoTypeCheck[uint8](col, int(row))
	case types.T_uint16:
		return vector.GetFixedAtNoTypeCheck[uint16](col, int(row))
	case types.T_uint32:
		return vector.GetFixedAtNoTypeCheck[uint32](col, int(row))
	case types.T_uint64:
		return vector.GetFixedAtNoTypeCheck[uint64](col, int(row))
	case types.T_decimal64:
		return vector.GetFixedAtNoTypeCheck[types.Decimal64](col, int(row))
	case types.T_decimal128:
		return vector.GetFixedAtNoTypeCheck[types.Decimal128](col, int(row))
	case types.T_uuid:
		return vector.GetFixedAtNoTypeCheck[types.Uuid](col, int(row))
	case types.T_float32:
		return vector.GetFixedAtNoTypeCheck[float32](col, int(row))
	case types.T_float64:
		return vector.GetFixedAtNoTypeCheck[float64](col, int(row))
	case types.T_bf16:
		return vector.GetFixedAtNoTypeCheck[types.BF16](col, int(row))
	case types.T_float16:
		return vector.GetFixedAtNoTypeCheck[types.Float16](col, int(row))
	case types.T_float8:
		return vector.GetFixedAtNoTypeCheck[types.Float8](col, int(row))
	case types.T_float4:
		return vector.GetFixedAtNoTypeCheck[types.Float4](col, int(row))
	case types.T_date:
		return vector.GetFixedAtNoTypeCheck[types.Date](col, int(row))
	case types.T_time:
		return vector.GetFixedAtNoTypeCheck[types.Time](col, int(row))
	case types.T_datetime:
		return vector.GetFixedAtNoTypeCheck[types.Datetime](col, int(row))
	case types.T_timestamp:
		return vector.GetFixedAtNoTypeCheck[types.Timestamp](col, int(row))
	case types.T_enum:
		return vector.GetFixedAtNoTypeCheck[types.Enum](col, int(row))
	case types.T_year:
		return vector.GetFixedAtNoTypeCheck[types.MoYear](col, int(row))
	case types.T_TS:
		return vector.GetFixedAtNoTypeCheck[types.TS](col, int(row))
	case types.T_Rowid:
		return vector.GetFixedAtNoTypeCheck[types.Rowid](col, int(row))
	case types.T_Blockid:
		return vector.GetFixedAtNoTypeCheck[types.Blockid](col, int(row))
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_json, types.T_blob, types.T_text,
		types.T_array_float32, types.T_array_float64,
		types.T_array_bf16, types.T_array_float16, types.T_array_int8, types.T_array_uint8,
		types.T_array_float8, types.T_array_float4, types.T_datalink:
		return col.GetBytesAt(int(row))
	default:
		//return vector.ErrVecTypeNotSupport
		panic(any("No Support"))
	}
}

func legacyGenericUpdateFixedValue[T types.FixedSizeT](
	vec *vector.Vector, row uint32, v any, isNull bool, _ *mpool.MPool,
) {
	if isNull {
		vec.GetNulls().Add(uint64(row))
	} else {
		err := vector.SetFixedAtNoTypeCheck(vec, int(row), v.(T))
		if err != nil {
			panic(err)
		}
		if vec.GetNulls().Contains(uint64(row)) {
			vec.GetNulls().Unset(uint64(row))
		}
	}
}

func legacyGenericUpdateBytes(
	vec *vector.Vector, row uint32, v any, isNull bool, mp *mpool.MPool,
) {
	if isNull {
		vec.GetNulls().Add(uint64(row))
	} else {
		err := vector.SetBytesAt(vec, int(row), v.([]byte), mp)
		if err != nil {
			panic(err)
		}
		if vec.GetNulls().Contains(uint64(row)) {
			vec.GetNulls().Unset(uint64(row))
		}
	}
}

func legacyUpdateValue(col *vector.Vector, row uint32, val any, isNull bool, mp *mpool.MPool) {
	switch col.GetType().Oid {
	case types.T_bool:
		legacyGenericUpdateFixedValue[bool](col, row, val, isNull, mp)
	case types.T_bit:
		legacyGenericUpdateFixedValue[uint64](col, row, val, isNull, mp)
	case types.T_int8:
		legacyGenericUpdateFixedValue[int8](col, row, val, isNull, mp)
	case types.T_int16:
		legacyGenericUpdateFixedValue[int16](col, row, val, isNull, mp)
	case types.T_int32:
		legacyGenericUpdateFixedValue[int32](col, row, val, isNull, mp)
	case types.T_int64:
		legacyGenericUpdateFixedValue[int64](col, row, val, isNull, mp)
	case types.T_uint8:
		legacyGenericUpdateFixedValue[uint8](col, row, val, isNull, mp)
	case types.T_uint16:
		legacyGenericUpdateFixedValue[uint16](col, row, val, isNull, mp)
	case types.T_uint32:
		legacyGenericUpdateFixedValue[uint32](col, row, val, isNull, mp)
	case types.T_uint64:
		legacyGenericUpdateFixedValue[uint64](col, row, val, isNull, mp)
	case types.T_decimal64:
		legacyGenericUpdateFixedValue[types.Decimal64](col, row, val, isNull, mp)
	case types.T_decimal128:
		legacyGenericUpdateFixedValue[types.Decimal128](col, row, val, isNull, mp)
	case types.T_float32:
		legacyGenericUpdateFixedValue[float32](col, row, val, isNull, mp)
	case types.T_float64:
		legacyGenericUpdateFixedValue[float64](col, row, val, isNull, mp)
	case types.T_bf16:
		legacyGenericUpdateFixedValue[types.BF16](col, row, val, isNull, mp)
	case types.T_float16:
		legacyGenericUpdateFixedValue[types.Float16](col, row, val, isNull, mp)
	case types.T_float8:
		legacyGenericUpdateFixedValue[types.Float8](col, row, val, isNull, mp)
	case types.T_float4:
		legacyGenericUpdateFixedValue[types.Float4](col, row, val, isNull, mp)
	case types.T_date:
		legacyGenericUpdateFixedValue[types.Date](col, row, val, isNull, mp)
	case types.T_time:
		legacyGenericUpdateFixedValue[types.Time](col, row, val, isNull, mp)
	case types.T_datetime:
		legacyGenericUpdateFixedValue[types.Datetime](col, row, val, isNull, mp)
	case types.T_timestamp:
		legacyGenericUpdateFixedValue[types.Timestamp](col, row, val, isNull, mp)
	case types.T_enum:
		legacyGenericUpdateFixedValue[types.Enum](col, row, val, isNull, mp)
	case types.T_uuid:
		legacyGenericUpdateFixedValue[types.Uuid](col, row, val, isNull, mp)
	case types.T_TS:
		legacyGenericUpdateFixedValue[types.TS](col, row, val, isNull, mp)
	case types.T_Rowid:
		legacyGenericUpdateFixedValue[types.Rowid](col, row, val, isNull, mp)
	case types.T_Blockid:
		legacyGenericUpdateFixedValue[types.Blockid](col, row, val, isNull, mp)
	case types.T_varchar, types.T_char, types.T_json,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_text,
		types.T_array_float32, types.T_array_float64,
		types.T_array_bf16, types.T_array_float16, types.T_array_int8, types.T_array_uint8,
		types.T_array_float8, types.T_array_float4, types.T_datalink:
		legacyGenericUpdateBytes(col, row, val, isNull, mp)
	default:
		panic(moerr.NewInternalErrorNoCtxf("%v not supported", col.GetType()))
	}
}
