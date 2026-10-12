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

package types

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestFormatSQLColumnType(t *testing.T) {
	for _, tc := range []struct {
		typ  Type
		want string
	}{
		{New(T_bool, 64, 3), "BOOLEAN"},
		{New(T_bit, 1, 0), "BIT(1)"},
		{New(T_char, 0, 0), "CHAR(0)"}, {New(T_varchar, 20, 0), "VARCHAR(20)"},
		{New(T_binary, 8, 0), "BINARY(8)"}, {New(T_varbinary, 32, 0), "VARBINARY(32)"},
		{New(T_decimal64, 10, 2), "DECIMAL(10,2)"}, {New(T_decimal128, 38, 0), "DECIMAL(38,0)"},
		{New(T_decimal256, 76, 18), "DECIMAL(76,18)"},
		{New(T_int32, 24, -1), "MEDIUMINT"},
		{New(T_uint32, 24, -1), "MEDIUMINT UNSIGNED"},
		{New(T_float32, 0, -1), "FLOAT"}, {New(T_float32, 5, 0), "FLOAT(5,0)"},
		{New(T_float64, 8, 2), "DOUBLE(8,2)"}, {New(T_float64, 8, -1), "DOUBLE"},
		{New(T_date, 0, 0), "DATE"}, {New(T_year, 4, 0), "YEAR"}, {New(T_json, 0, 0), "JSON"},
		{New(T_time, 0, 0), "TIME"}, {New(T_time, 6, 6), "TIME(6)"},
		{New(T_datetime, 0, 0), "DATETIME"}, {New(T_datetime, 3, 3), "DATETIME(3)"},
		{New(T_timestamp, 0, 0), "TIMESTAMP"}, {New(T_timestamp, 6, 6), "TIMESTAMP(6)"},
		{New(T_uuid, 0, 0), "UUID"}, {New(T_datalink, 0, 0), "DATALINK"},
		{New(T_text, 0, 0), "TEXT"}, {New(T_text, 20, 0), "TEXT"}, {New(T_text, MaxStringSize, 0), "TEXT"},
		{New(T_text, MaxTinyTextLen, 0), "TINYTEXT"}, {New(T_text, MaxMediumTextLen, 0), "MEDIUMTEXT"},
		{New(T_text, MaxLongTextLen, 0), "LONGTEXT"},
		{New(T_blob, 0, 0), "LONGBLOB"}, {New(T_blob, 1, 0), "TINYBLOB"},
		{New(T_blob, MaxTinyTextLen, 0), "TINYBLOB"}, {New(T_blob, MaxTinyTextLen+1, 0), "BLOB"},
		{New(T_blob, MaxStringSize, 0), "BLOB"}, {New(T_blob, MaxStringSize+1, 0), "MEDIUMBLOB"},
		{New(T_blob, MaxMediumTextLen, 0), "MEDIUMBLOB"}, {New(T_blob, MaxMediumTextLen+1, 0), "LONGBLOB"},
	} {
		t.Run(tc.want, func(t *testing.T) {
			got, ok := FormatSQLColumnType(tc.typ)
			require.True(t, ok)
			require.Equal(t, tc.want, got)
		})
	}
	for _, tc := range []struct {
		oid  T
		want string
	}{
		{T_int8, "TINYINT"}, {T_int16, "SMALLINT"}, {T_int32, "INT"}, {T_int64, "BIGINT"},
		{T_uint8, "TINYINT UNSIGNED"}, {T_uint16, "SMALLINT UNSIGNED"}, {T_uint32, "INT UNSIGNED"}, {T_uint64, "BIGINT UNSIGNED"},
	} {
		for _, width := range []int32{0, 8, 16, 32, 64} {
			got, ok := FormatSQLColumnType(New(tc.oid, width, 0))
			require.True(t, ok)
			require.Equal(t, tc.want, got)
		}
	}
	for _, oid := range []T{T_any, T_tuple, T_Rowid, T_enum, T_geometry, T_geometry32, T_array_float32, T_array_float64, T_array_bf16, T_array_float16, T_array_int8, T_array_uint8} {
		_, ok := FormatSQLColumnType(New(oid, 0, 0))
		require.False(t, ok)
	}
}
