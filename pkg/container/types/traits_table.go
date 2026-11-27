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

// traitRows has one row per T_ constant (see traits.go). Adding a type means
// adding its row here.
//
// Fields left out keep their zero value, which means:
//   - flags: in none of the IsXxx families;
//   - typeLen, fixedLen: no size, TypeLen / FixedLength panic;
//   - deflt: TryToType reports false (ToType panics);
//   - name: String reports "unexpected type: <n>";
//   - oidName: OidString reports "unknown_type".
//
// Some rows leave fields out on purpose, matching the behavior these methods
// had as switches: for example T_int128 and T_uint128 have no size and no
// name, and T_tuple and T_interval have a TypeLen of 0 but no FixedLength.
var traitRows = []traitRow{
	{T_any, typeTraits{name: "ANY",
		typeLen: sz(0), fixedLen: sz(0), deflt: def(0, 0)}},
	{T_star, typeTraits{}},

	{T_bool, typeTraits{name: "BOOL", oidName: "T_bool",
		typeLen: sz(1), fixedLen: sz(1), deflt: def(1, 0)}},
	{T_bit, typeTraits{name: "BIT", oidName: "T_bit", flags: flagOrdered,
		typeLen: sz(8), fixedLen: sz(8), deflt: def(8, MaxBitLen)}},

	// integers
	{T_int8, typeTraits{name: "TINYINT", oidName: "T_int8", flags: flagSignedInt | flagOrdered,
		typeLen: sz(1), fixedLen: sz(1), deflt: def(1, 0)}},
	{T_int16, typeTraits{name: "SMALLINT", oidName: "T_int16", flags: flagSignedInt | flagOrdered,
		typeLen: sz(2), fixedLen: sz(2), deflt: def(2, 0)}},
	{T_int32, typeTraits{name: "INT", oidName: "T_int32", flags: flagSignedInt | flagOrdered,
		typeLen: sz(4), fixedLen: sz(4), deflt: def(4, 0)}},
	{T_int64, typeTraits{name: "BIGINT", oidName: "T_int64", flags: flagSignedInt | flagOrdered,
		typeLen: sz(8), fixedLen: sz(8), deflt: def(8, 0)}},
	{T_int128, typeTraits{}},
	{T_uint8, typeTraits{name: "TINYINT UNSIGNED", oidName: "T_uint8", flags: flagUnsignedInt | flagOrdered,
		typeLen: sz(1), fixedLen: sz(1), deflt: def(1, 0)}},
	{T_uint16, typeTraits{name: "SMALLINT UNSIGNED", oidName: "T_uint16", flags: flagUnsignedInt | flagOrdered,
		typeLen: sz(2), fixedLen: sz(2), deflt: def(2, 0)}},
	{T_uint32, typeTraits{name: "INT UNSIGNED", oidName: "T_uint32", flags: flagUnsignedInt | flagOrdered,
		typeLen: sz(4), fixedLen: sz(4), deflt: def(4, 0)}},
	{T_uint64, typeTraits{name: "BIGINT UNSIGNED", oidName: "T_uint64", flags: flagUnsignedInt | flagOrdered,
		typeLen: sz(8), fixedLen: sz(8), deflt: def(8, 0)}},
	{T_uint128, typeTraits{}},

	// floats
	{T_float32, typeTraits{name: "FLOAT", oidName: "T_float32", flags: flagFloat | flagOrdered,
		typeLen: sz(4), fixedLen: sz(4), deflt: def(4, 0)}},
	{T_float64, typeTraits{name: "DOUBLE", oidName: "T_float64", flags: flagFloat | flagOrdered,
		typeLen: sz(8), fixedLen: sz(8), deflt: def(8, 0)}},
	{T_bf16, typeTraits{name: "BF16", oidName: "T_bf16", flags: flagFloat | flagLowPrecisionFloat,
		typeLen: sz(2), fixedLen: sz(2), deflt: def(2, 0)}},
	{T_float16, typeTraits{name: "FLOAT16", oidName: "T_float16", flags: flagFloat | flagLowPrecisionFloat,
		typeLen: sz(2), fixedLen: sz(2), deflt: def(2, 0)}},
	{T_float8, typeTraits{name: "FLOAT8", oidName: "T_float8", flags: flagFloat | flagLowPrecisionFloat,
		typeLen: sz(1), fixedLen: sz(1), deflt: def(1, 0)}},
	{T_float4, typeTraits{name: "FLOAT4", oidName: "T_float4", flags: flagFloat | flagLowPrecisionFloat,
		typeLen: sz(1), fixedLen: sz(1), deflt: def(1, 0)}},

	// decimals
	{T_decimal64, typeTraits{name: "DECIMAL64", oidName: "T_decimal64", flags: flagDecimal,
		typeLen: sz(8), fixedLen: sz(8), deflt: def(8, 18)}},
	{T_decimal128, typeTraits{name: "DECIMAL128", oidName: "T_decimal128", flags: flagDecimal,
		typeLen: sz(16), fixedLen: sz(16), deflt: def(16, 38)}},
	{T_decimal256, typeTraits{name: "DECIMAL256", oidName: "T_decimal256", flags: flagDecimal,
		typeLen: sz(32), fixedLen: sz(32), deflt: def(32, 76)}},

	// date and time
	{T_date, typeTraits{name: "DATE", oidName: "T_date", flags: flagDateRelate | flagOrdered,
		typeLen: sz(4), fixedLen: sz(4), deflt: def(4, 0)}},
	{T_time, typeTraits{name: "TIME", oidName: "T_time", flags: flagDateRelate | flagOrdered,
		typeLen: sz(8), fixedLen: sz(8), deflt: def(8, 0)}},
	{T_datetime, typeTraits{name: "DATETIME", oidName: "T_datetime", flags: flagDateRelate | flagOrdered,
		typeLen: sz(8), fixedLen: sz(8), deflt: def(8, 0)}},
	{T_timestamp, typeTraits{name: "TIMESTAMP", oidName: "T_timestamp", flags: flagDateRelate | flagOrdered,
		typeLen: sz(8), fixedLen: sz(8), deflt: def(8, 0)}},
	{T_interval, typeTraits{name: "INTERVAL", oidName: "T_interval",
		typeLen: sz(0)}},
	{T_year, typeTraits{name: "YEAR", oidName: "T_year", flags: flagDateRelate | flagOrdered,
		typeLen: sz(2), fixedLen: sz(2), deflt: def(2, 4)}},

	// strings and other variable-length types
	{T_char, typeTraits{name: "CHAR", oidName: "T_char", flags: flagMySQLString,
		typeLen: sz(VarlenaSize), fixedLen: sz(-VarlenaSize), deflt: def(VarlenaSize, MaxCharLen)}},
	{T_varchar, typeTraits{name: "VARCHAR", oidName: "T_varchar", flags: flagMySQLString,
		typeLen: sz(VarlenaSize), fixedLen: sz(-VarlenaSize), deflt: def(VarlenaSize, MaxVarcharLen)}},
	{T_json, typeTraits{name: "JSON", oidName: "T_json",
		typeLen: sz(VarlenaSize), fixedLen: sz(-VarlenaSize), deflt: def(VarlenaSize, 0)}},
	{T_uuid, typeTraits{name: "UUID", oidName: "T_uuid",
		typeLen: sz(16), fixedLen: sz(16), deflt: def(16, 0)}},
	{T_binary, typeTraits{name: "BINARY", oidName: "T_binary", flags: flagMySQLString,
		typeLen: sz(VarlenaSize), fixedLen: sz(-VarlenaSize), deflt: def(VarlenaSize, MaxBinaryLen)}},
	{T_varbinary, typeTraits{name: "VARBINARY", oidName: "T_varbinary", flags: flagMySQLString,
		typeLen: sz(VarlenaSize), fixedLen: sz(-VarlenaSize), deflt: def(VarlenaSize, MaxVarBinaryLen)}},
	{T_enum, typeTraits{name: "ENUM", oidName: "T_enum",
		typeLen: sz(2), fixedLen: sz(2), deflt: def(2, 0)}},
	{T_geometry, typeTraits{name: "GEOMETRY", oidName: "T_geometry",
		typeLen: sz(VarlenaSize), fixedLen: sz(-VarlenaSize), deflt: def(VarlenaSize, 0)}},
	{T_geometry32, typeTraits{name: "GEOMETRY32", oidName: "T_geometry32",
		typeLen: sz(VarlenaSize), fixedLen: sz(-VarlenaSize), deflt: def(VarlenaSize, 0)}},
	{T_blob, typeTraits{name: "BLOB", oidName: "T_blob", flags: flagMySQLString,
		typeLen: sz(VarlenaSize), fixedLen: sz(-VarlenaSize), deflt: def(VarlenaSize, 0)}},
	{T_text, typeTraits{name: "TEXT", oidName: "T_text", flags: flagMySQLString,
		typeLen: sz(VarlenaSize), fixedLen: sz(-VarlenaSize), deflt: def(VarlenaSize, 0)}},
	{T_datalink, typeTraits{name: "DATALINK", oidName: "T_datalink",
		typeLen: sz(VarlenaSize), fixedLen: sz(-VarlenaSize), deflt: def(VarlenaSize, 0)}},

	// transaction and storage internals
	{T_TS, typeTraits{name: "TRANSACTION TIMESTAMP", oidName: "T_TS",
		typeLen: sz(TxnTsSize), fixedLen: sz(TxnTsSize), deflt: def(TxnTsSize, 0)}},
	{T_Rowid, typeTraits{name: "ROWID", oidName: "T_Rowid",
		typeLen: sz(RowidSize), fixedLen: sz(RowidSize), deflt: def(RowidSize, 0)}},
	{T_Blockid, typeTraits{name: "BLOCKID", oidName: "T_Blockid",
		typeLen: sz(BlockidSize), fixedLen: sz(BlockidSize), deflt: def(BlockidSize, 0)}},
	{T_Objectid, typeTraits{name: "OBJECTID"}},

	{T_tuple, typeTraits{name: "TUPLE",
		typeLen: sz(0)}},

	// vectors
	{T_array_float32, typeTraits{name: "VECF32", oidName: "T_array_float32", flags: flagArrayRelate,
		typeLen: sz(VarlenaSize), fixedLen: sz(-VarlenaSize), deflt: def(VarlenaSize, MaxArrayDimension)}},
	{T_array_float64, typeTraits{name: "VECF64", oidName: "T_array_float64", flags: flagArrayRelate,
		typeLen: sz(VarlenaSize), fixedLen: sz(-VarlenaSize), deflt: def(VarlenaSize, MaxArrayDimension)}},
	{T_array_bf16, typeTraits{name: "VECBF16", oidName: "T_array_bf16", flags: flagArrayRelate,
		typeLen: sz(VarlenaSize), fixedLen: sz(-VarlenaSize), deflt: def(VarlenaSize, MaxArrayDimension)}},
	{T_array_float16, typeTraits{name: "VECF16", oidName: "T_array_float16", flags: flagArrayRelate,
		typeLen: sz(VarlenaSize), fixedLen: sz(-VarlenaSize), deflt: def(VarlenaSize, MaxArrayDimension)}},
	{T_array_int8, typeTraits{name: "VECINT8", oidName: "T_array_int8", flags: flagArrayRelate,
		typeLen: sz(VarlenaSize), fixedLen: sz(-VarlenaSize), deflt: def(VarlenaSize, MaxArrayDimension)}},
	{T_array_uint8, typeTraits{name: "VECUINT8", oidName: "T_array_uint8", flags: flagArrayRelate,
		typeLen: sz(VarlenaSize), fixedLen: sz(-VarlenaSize), deflt: def(VarlenaSize, MaxArrayDimension)}},
	{T_array_float8, typeTraits{name: "VECF8", oidName: "T_array_float8", flags: flagBlockScaledArray,
		typeLen: sz(VarlenaSize), fixedLen: sz(-VarlenaSize), deflt: def(VarlenaSize, MaxArrayDimension)}},
	{T_array_float4, typeTraits{name: "VECF4", oidName: "T_array_float4", flags: flagBlockScaledArray,
		typeLen: sz(VarlenaSize), fixedLen: sz(-VarlenaSize), deflt: def(VarlenaSize, MaxArrayDimension)}},
}
