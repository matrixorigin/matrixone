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

package types

// The implementations of the T methods before they moved onto the traits
// table (traits_table.go), kept verbatim as the oracle for
// TestTraitsMatchLegacy. Delete this file once the table has been the only
// implementation for a release.

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

func legacyString(t T) string {
	switch t {
	case T_any:
		return "ANY"
	case T_bool:
		return "BOOL"
	case T_bit:
		return "BIT"
	case T_int8:
		return "TINYINT"
	case T_int16:
		return "SMALLINT"
	case T_int32:
		return "INT"
	case T_int64:
		return "BIGINT"
	case T_uint8:
		return "TINYINT UNSIGNED"
	case T_uint16:
		return "SMALLINT UNSIGNED"
	case T_uint32:
		return "INT UNSIGNED"
	case T_uint64:
		return "BIGINT UNSIGNED"
	case T_float32:
		return "FLOAT"
	case T_float64:
		return "DOUBLE"
	case T_bf16:
		return "BF16"
	case T_float16:
		return "FLOAT16"
	case T_float8:
		return "FLOAT8"
	case T_float4:
		return "FLOAT4"
	case T_date:
		return "DATE"
	case T_datetime:
		return "DATETIME"
	case T_time:
		return "TIME"
	case T_timestamp:
		return "TIMESTAMP"
	case T_char:
		return "CHAR"
	case T_varchar:
		return "VARCHAR"
	case T_binary:
		return "BINARY"
	case T_varbinary:
		return "VARBINARY"
	case T_json:
		return "JSON"
	case T_geometry:
		return "GEOMETRY"
	case T_geometry32:
		return "GEOMETRY32"
	case T_tuple:
		return "TUPLE"
	case T_decimal64:
		return "DECIMAL64"
	case T_decimal128:
		return "DECIMAL128"
	case T_decimal256:
		return "DECIMAL256"
	case T_blob:
		return "BLOB"
	case T_text:
		return "TEXT"
	case T_datalink:
		return "DATALINK"
	case T_TS:
		return "TRANSACTION TIMESTAMP"
	case T_Objectid:
		return "OBJECTID"
	case T_Rowid:
		return "ROWID"
	case T_uuid:
		return "UUID"
	case T_Blockid:
		return "BLOCKID"
	case T_interval:
		return "INTERVAL"
	case T_year:
		return "YEAR"
	case T_array_float32:
		return "VECF32"
	case T_array_float64:
		return "VECF64"
	case T_array_bf16:
		return "VECBF16"
	case T_array_float16:
		return "VECF16"
	case T_array_int8:
		return "VECINT8"
	case T_array_uint8:
		return "VECUINT8"
	case T_array_float8:
		return "VECF8"
	case T_array_float4:
		return "VECF4"
	case T_enum:
		return "ENUM"
	}
	return fmt.Sprintf("unexpected type: %d", t)
}

func legacyOidString(t T) string {
	switch t {
	case T_uuid:
		return "T_uuid"
	case T_json:
		return "T_json"
	case T_geometry:
		return "T_geometry"
	case T_geometry32:
		return "T_geometry32"
	case T_bool:
		return "T_bool"
	case T_bit:
		return "T_bit"
	case T_int64:
		return "T_int64"
	case T_int32:
		return "T_int32"
	case T_int16:
		return "T_int16"
	case T_int8:
		return "T_int8"
	case T_float64:
		return "T_float64"
	case T_float32:
		return "T_float32"
	case T_bf16:
		return "T_bf16"
	case T_float16:
		return "T_float16"
	case T_float8:
		return "T_float8"
	case T_float4:
		return "T_float4"
	case T_uint8:
		return "T_uint8"
	case T_uint16:
		return "T_uint16"
	case T_uint32:
		return "T_uint32"
	case T_uint64:
		return "T_uint64"
	case T_char:
		return "T_char"
	case T_varchar:
		return "T_varchar"
	case T_binary:
		return "T_binary"
	case T_varbinary:
		return "T_varbinary"
	case T_date:
		return "T_date"
	case T_datetime:
		return "T_datetime"
	case T_time:
		return "T_time"
	case T_timestamp:
		return "T_timestamp"
	case T_decimal64:
		return "T_decimal64"
	case T_decimal128:
		return "T_decimal128"
	case T_decimal256:
		return "T_decimal256"
	case T_blob:
		return "T_blob"
	case T_text:
		return "T_text"
	case T_datalink:
		return "T_datalink"
	case T_TS:
		return "T_TS"
	case T_Rowid:
		return "T_Rowid"
	case T_Blockid:
		return "T_Blockid"
	case T_interval:
		return "T_interval"
	case T_year:
		return "T_year"
	case T_enum:
		return "T_enum"
	case T_array_float32:
		return "T_array_float32"
	case T_array_float64:
		return "T_array_float64"
	case T_array_bf16:
		return "T_array_bf16"
	case T_array_float16:
		return "T_array_float16"
	case T_array_int8:
		return "T_array_int8"
	case T_array_uint8:
		return "T_array_uint8"
	case T_array_float8:
		return "T_array_float8"
	case T_array_float4:
		return "T_array_float4"
	}
	return "unknown_type"
}

func legacyTypeLen(t T) int {
	switch t {
	case T_any:
		return 0
	case T_bit:
		return 8
	case T_int8, T_bool, T_float8, T_float4:
		return 1
	case T_int16, T_bf16, T_float16:
		return 2
	case T_int32, T_date:
		return 4
	case T_int64, T_datetime, T_time, T_timestamp:
		return 8
	case T_year:
		return 2
	case T_uint8:
		return 1
	case T_uint16:
		return 2
	case T_uint32:
		return 4
	case T_uint64:
		return 8
	case T_float32:
		return 4
	case T_float64:
		return 8
	case T_char, T_varchar, T_json, T_blob, T_text, T_binary, T_varbinary, T_array_float32, T_array_float64, T_array_bf16, T_array_float16, T_array_int8, T_array_uint8, T_array_float8, T_array_float4, T_datalink, T_geometry, T_geometry32:
		return VarlenaSize
	case T_decimal64:
		return 8
	case T_decimal128:
		return 16
	case T_decimal256:
		return 32
	case T_uuid:
		return 16
	case T_TS:
		return TxnTsSize
	case T_Rowid:
		return RowidSize
	case T_Blockid:
		return BlockidSize
	case T_tuple, T_interval:
		return 0
	case T_enum:
		return 2
	}
	panic(fmt.Sprintf("unknown type %d", t))
}

func legacyFixedLength(t T) int {
	switch t {
	case T_any:
		return 0
	case T_bit:
		return 8
	case T_int8, T_uint8, T_bool, T_float8, T_float4:
		return 1
	case T_int16, T_uint16, T_bf16, T_float16:
		return 2
	case T_year:
		return 2
	case T_int32, T_uint32, T_date, T_float32:
		return 4
	case T_int64, T_uint64, T_datetime, T_time, T_float64, T_timestamp:
		return 8
	case T_decimal64:
		return 8
	case T_decimal128:
		return 16
	case T_decimal256:
		return 32
	case T_uuid:
		return 16
	case T_TS:
		return TxnTsSize
	case T_Rowid:
		return RowidSize
	case T_Blockid:
		return BlockidSize
	case T_char, T_varchar, T_blob, T_json, T_text, T_binary, T_varbinary, T_array_float32, T_array_float64, T_array_bf16, T_array_float16, T_array_int8, T_array_uint8, T_array_float8, T_array_float4, T_datalink, T_geometry, T_geometry32:
		return -24
	case T_enum:
		return 2
	}
	panic(moerr.NewInternalErrorNoCtx(fmt.Sprintf("unknown type %d", t)))
}

func legacyIsFixedLen(t T) bool {
	return legacyFixedLength(t) > 0
}

func legacyIsOrdered(t T) bool {
	switch t {
	case T_int8, T_int16, T_int32, T_int64,
		T_uint8, T_uint16, T_uint32, T_uint64,
		T_float32, T_float64,
		T_date, T_time, T_datetime, T_timestamp, T_year,
		T_bit:
		return true
	default:
		return false
	}
}

func legacyIsUnsignedInt(t T) bool {
	if t == T_uint8 || t == T_uint16 || t == T_uint32 || t == T_uint64 {
		return true
	}
	return false
}

func legacyIsSignedInt(t T) bool {
	if t == T_int8 || t == T_int16 || t == T_int32 || t == T_int64 {
		return true
	}
	return false
}

func legacyIsInteger(t T) bool {
	if legacyIsUnsignedInt(t) || legacyIsSignedInt(t) {
		return true
	}
	return false
}

func legacyIsFloat(t T) bool {
	if t == T_float32 || t == T_float64 || t == T_bf16 || t == T_float16 || t == T_float8 || t == T_float4 {
		return true
	}
	return false
}

func legacyIsLowPrecisionFloat(t T) bool {
	return t == T_bf16 || t == T_float16 || t == T_float8 || t == T_float4
}

func legacyIsMySQLString(t T) bool {
	// NOTE: Don't replace this with legacyFixedLength(t)<0
	// The legacyFixedLength(t)<0 logic includes T_Json, which is not a MySQL string type.
	if t == T_char || t == T_varchar || t == T_blob || t == T_text || t == T_binary || t == T_varbinary {
		return true
	}
	return false
}

func legacyIsDateRelate(t T) bool {
	if t == T_date || t == T_datetime || t == T_timestamp || t == T_time || t == T_year {
		return true
	}
	return false
}

func legacyIsArrayRelate(t T) bool {
	if t == T_array_float32 || t == T_array_float64 ||
		t == T_array_bf16 || t == T_array_float16 || t == T_array_int8 || t == T_array_uint8 {
		return true
	}
	return false
}

func legacyIsBlockScaledArray(t T) bool {
	return t == T_array_float8 || t == T_array_float4
}

func legacyIsArray(t T) bool {
	return legacyIsArrayRelate(t) || legacyIsBlockScaledArray(t)
}

func legacyIsDecimal(t T) bool {
	if t == T_decimal64 || t == T_decimal128 || t == T_decimal256 {
		return true
	}
	return false
}

func legacyTryToType(t T) (Type, bool) {
	var typ Type

	typ.Oid = t
	switch t {
	case T_bool:
		typ.Size = 1
	case T_bit:
		typ.Size = 8
		typ.Width = MaxBitLen
	case T_int8:
		typ.Size = 1
	case T_int16:
		typ.Size = 2
	case T_int32, T_date:
		typ.Size = 4
	case T_int64, T_datetime, T_time, T_timestamp:
		typ.Size = 8
	case T_year:
		typ.Size = 2
		typ.Width = 4
	case T_uint8:
		typ.Size = 1
	case T_uint16:
		typ.Size = 2
	case T_uint32:
		typ.Size = 4
	case T_uint64:
		typ.Size = 8
	case T_float32:
		typ.Size = 4
	case T_float64:
		typ.Size = 8
	case T_bf16, T_float16:
		typ.Size = 2
	case T_float8, T_float4:
		typ.Size = 1
	case T_decimal64:
		typ.Size = 8
		typ.Width = 18
	case T_decimal128:
		typ.Size = 16
		typ.Width = 38
	case T_decimal256:
		typ.Size = 32
		typ.Width = 76
	case T_uuid:
		typ.Size = 16
	case T_TS:
		typ.Size = TxnTsSize
	case T_Rowid:
		typ.Size = RowidSize
	case T_Blockid:
		typ.Size = BlockidSize
	case T_json, T_blob, T_text, T_datalink, T_geometry, T_geometry32:
		typ.Size = VarlenaSize
	case T_char:
		typ.Size = VarlenaSize
		typ.Width = MaxCharLen
	case T_varchar:
		typ.Size = VarlenaSize
		typ.Width = MaxVarcharLen
	case T_array_float32, T_array_float64, T_array_bf16, T_array_float16, T_array_int8, T_array_uint8,
		T_array_float8, T_array_float4:
		typ.Size = VarlenaSize
		typ.Width = MaxArrayDimension
	case T_binary:
		typ.Size = VarlenaSize
		typ.Width = MaxBinaryLen
	case T_varbinary:
		typ.Size = VarlenaSize
		typ.Width = MaxVarBinaryLen
	case T_enum:
		typ.Size = 2
	case T_any:
		// XXX I don't know about this one ...
		typ.Size = 0
	default:
		return Type{}, false
	}
	typ.Charset = CharsetType(t)
	return typ, true
}

func legacyTypeIsInt(t Type) bool {
	switch t.Oid {
	case T_int8, T_int16, T_int32, T_int64:
		return true
	default:
		return false
	}
}

func legacyTypeIsUInt(t Type) bool {
	switch t.Oid {
	case T_uint8, T_uint16, T_uint32, T_uint64:
		return true
	default:
		return false
	}
}

func legacyTypeIsFloat(t Type) bool {
	switch t.Oid {
	case T_float32, T_float64, T_bf16, T_float16, T_float8, T_float4:
		return true
	default:
		return false
	}
}

func legacyTypeIsDecimal(t Type) bool {
	switch t.Oid {
	case T_decimal64, T_decimal128, T_decimal256:
		return true
	default:
		return false
	}
}
