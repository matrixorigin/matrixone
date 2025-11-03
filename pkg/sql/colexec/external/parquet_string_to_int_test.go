// Copyright 2025 Matrix Origin
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

package external

import (
	"bytes"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

// TestParquet_StringToInt_BasicTypes tests STRING → INT conversions for all integer types
func TestParquet_StringToInt_BasicTypes(t *testing.T) {
	proc := testutil.NewProc(t)

	tests := []struct {
		name      string
		dt        types.T
		strValues []string
		expected  interface{} // slice of expected values
	}{
		{
			name:      "STRING → INT8",
			dt:        types.T_int8,
			strValues: []string{"1", "-1", "127", "-128", "0"},
			expected:  []int8{1, -1, 127, -128, 0},
		},
		{
			name:      "STRING → UINT8",
			dt:        types.T_uint8,
			strValues: []string{"0", "1", "255", "128", "100"},
			expected:  []uint8{0, 1, 255, 128, 100},
		},
		{
			name:      "STRING → INT16",
			dt:        types.T_int16,
			strValues: []string{"1", "-1", "32767", "-32768", "0"},
			expected:  []int16{1, -1, 32767, -32768, 0},
		},
		{
			name:      "STRING → UINT16",
			dt:        types.T_uint16,
			strValues: []string{"0", "1", "65535", "32768", "1000"},
			expected:  []uint16{0, 1, 65535, 32768, 1000},
		},
		{
			name:      "STRING → INT32",
			dt:        types.T_int32,
			strValues: []string{"1", "-1", "2147483647", "-2147483648", "0"},
			expected:  []int32{1, -1, 2147483647, -2147483648, 0},
		},
		{
			name:      "STRING → UINT32",
			dt:        types.T_uint32,
			strValues: []string{"0", "1", "4294967295", "2147483648", "1000000"},
			expected:  []uint32{0, 1, 4294967295, 2147483648, 1000000},
		},
		{
			name:      "STRING → INT64",
			dt:        types.T_int64,
			strValues: []string{"1", "-1", "9223372036854775807", "-9223372036854775808", "0"},
			expected:  []int64{1, -1, 9223372036854775807, -9223372036854775808, 0},
		},
		{
			name:      "STRING → UINT64",
			dt:        types.T_uint64,
			strValues: []string{"0", "1", "18446744073709551615", "9223372036854775808", "1000000"},
			expected:  []uint64{0, 1, 18446744073709551615, 9223372036854775808, 1000000},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Create parquet file with one row per value
			st := parquet.String().Type()
			var buf bytes.Buffer
			schema := parquet.NewSchema("x", parquet.Group{"c": parquet.Leaf(st)})
			w := parquet.NewWriter(&buf, schema)

			// Write each string as a separate row
			for _, s := range tc.strValues {
				row := parquet.Row{parquet.ByteArrayValue([]byte(s))}
				_, err := w.WriteRows([]parquet.Row{row})
				require.NoError(t, err)
			}
			require.NoError(t, w.Close())

			f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
			require.NoError(t, err)
			col := f.Root().Column("c")
			page, err := col.Pages().ReadPage()
			require.NoError(t, err)

			// Test conversion
			vec := vector.NewVec(types.New(tc.dt, 0, 0))
			var h ParquetHandler
			mp := h.getMapper(col, plan.Type{Id: int32(tc.dt), NotNullable: true})
			require.NotNil(t, mp, "STRING → %s conversion should be supported", tc.dt)
			err = mp.mapping(page, proc, vec)
			require.NoError(t, err)

			// Verify results
			require.Equal(t, len(tc.strValues), vec.Length())
			switch tc.dt {
			case types.T_int8:
				got := vector.MustFixedColWithTypeCheck[int8](vec)
				require.Equal(t, tc.expected.([]int8), got)
			case types.T_uint8:
				got := vector.MustFixedColWithTypeCheck[uint8](vec)
				require.Equal(t, tc.expected.([]uint8), got)
			case types.T_int16:
				got := vector.MustFixedColWithTypeCheck[int16](vec)
				require.Equal(t, tc.expected.([]int16), got)
			case types.T_uint16:
				got := vector.MustFixedColWithTypeCheck[uint16](vec)
				require.Equal(t, tc.expected.([]uint16), got)
			case types.T_int32:
				got := vector.MustFixedColWithTypeCheck[int32](vec)
				require.Equal(t, tc.expected.([]int32), got)
			case types.T_uint32:
				got := vector.MustFixedColWithTypeCheck[uint32](vec)
				require.Equal(t, tc.expected.([]uint32), got)
			case types.T_int64:
				got := vector.MustFixedColWithTypeCheck[int64](vec)
				require.Equal(t, tc.expected.([]int64), got)
			case types.T_uint64:
				got := vector.MustFixedColWithTypeCheck[uint64](vec)
				require.Equal(t, tc.expected.([]uint64), got)
			}
		})
	}
}

// TestParquet_StringToInt_InvalidValues tests error handling for invalid string values
func TestParquet_StringToInt_InvalidValues(t *testing.T) {
	proc := testutil.NewProc(t)

	tests := []struct {
		name     string
		dt       types.T
		strValue string
	}{
		{"STRING(not_a_number) → INT32", types.T_int32, "not_a_number"},
		{"STRING(abc) → INT64", types.T_int64, "abc"},
		{"STRING(12.34) → INT32", types.T_int32, "12.34"}, // decimal not valid for ParseInt
		{"STRING(-1) → UINT32", types.T_uint32, "-1"},     // negative not valid for unsigned
		{"STRING(256) → INT8", types.T_int8, "256"},       // out of range
		{"STRING(256) → UINT8", types.T_uint8, "256"},     // out of range
		{"STRING(32768) → INT16", types.T_int16, "32768"}, // out of range
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Build parquet file with invalid string
			values := []parquet.Value{parquet.ByteArrayValue([]byte(tc.strValue))}
			st := parquet.String().Type()
			var buf bytes.Buffer
			schema := parquet.NewSchema("x", parquet.Group{"c": parquet.Leaf(st)})
			w := parquet.NewWriter(&buf, schema)
			for _, val := range values {
				row := parquet.Row{val}
				_, err := w.WriteRows([]parquet.Row{row})
				require.NoError(t, err)
			}
			require.NoError(t, w.Close())

			f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
			require.NoError(t, err)
			col := f.Root().Column("c")
			page, err := col.Pages().ReadPage()
			require.NoError(t, err)

			// Test conversion - should fail
			vec := vector.NewVec(types.New(tc.dt, 0, 0))
			var h ParquetHandler
			mp := h.getMapper(col, plan.Type{Id: int32(tc.dt), NotNullable: true})
			require.NotNil(t, mp)
			err = mp.mapping(page, proc, vec)
			require.Error(t, err, "Should fail to parse invalid value '%s'", tc.strValue)
		})
	}
}

// TestParquet_StringToInt_ExtremeValues tests conversion of extreme values
func TestParquet_StringToInt_ExtremeValues(t *testing.T) {
	proc := testutil.NewProc(t)

	tests := []struct {
		name     string
		dt       types.T
		strValue string
		expected interface{}
	}{
		{"MaxInt8", types.T_int8, "127", int8(127)},
		{"MinInt8", types.T_int8, "-128", int8(-128)},
		{"MaxUint8", types.T_uint8, "255", uint8(255)},
		{"MaxInt16", types.T_int16, "32767", int16(32767)},
		{"MinInt16", types.T_int16, "-32768", int16(-32768)},
		{"MaxUint16", types.T_uint16, "65535", uint16(65535)},
		{"MaxInt32", types.T_int32, "2147483647", int32(2147483647)},
		{"MinInt32", types.T_int32, "-2147483648", int32(-2147483648)},
		{"MaxUint32", types.T_uint32, "4294967295", uint32(4294967295)},
		{"MaxInt64", types.T_int64, "9223372036854775807", int64(9223372036854775807)},
		{"MinInt64", types.T_int64, "-9223372036854775808", int64(math.MinInt64)},
		{"MaxUint64", types.T_uint64, "18446744073709551615", uint64(18446744073709551615)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Build parquet file
			values := []parquet.Value{parquet.ByteArrayValue([]byte(tc.strValue))}
			st := parquet.String().Type()
			var buf bytes.Buffer
			schema := parquet.NewSchema("x", parquet.Group{"c": parquet.Leaf(st)})
			w := parquet.NewWriter(&buf, schema)
			for _, val := range values {
				row := parquet.Row{val}
				_, err := w.WriteRows([]parquet.Row{row})
				require.NoError(t, err)
			}
			require.NoError(t, w.Close())

			f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
			require.NoError(t, err)
			col := f.Root().Column("c")
			page, err := col.Pages().ReadPage()
			require.NoError(t, err)

			// Test conversion
			vec := vector.NewVec(types.New(tc.dt, 0, 0))
			var h ParquetHandler
			mp := h.getMapper(col, plan.Type{Id: int32(tc.dt), NotNullable: true})
			require.NotNil(t, mp)
			err = mp.mapping(page, proc, vec)
			require.NoError(t, err)

			// Verify result
			require.Equal(t, 1, vec.Length())
			switch tc.dt {
			case types.T_int8:
				got := vector.MustFixedColWithTypeCheck[int8](vec)
				require.Equal(t, tc.expected.(int8), got[0])
			case types.T_uint8:
				got := vector.MustFixedColWithTypeCheck[uint8](vec)
				require.Equal(t, tc.expected.(uint8), got[0])
			case types.T_int16:
				got := vector.MustFixedColWithTypeCheck[int16](vec)
				require.Equal(t, tc.expected.(int16), got[0])
			case types.T_uint16:
				got := vector.MustFixedColWithTypeCheck[uint16](vec)
				require.Equal(t, tc.expected.(uint16), got[0])
			case types.T_int32:
				got := vector.MustFixedColWithTypeCheck[int32](vec)
				require.Equal(t, tc.expected.(int32), got[0])
			case types.T_uint32:
				got := vector.MustFixedColWithTypeCheck[uint32](vec)
				require.Equal(t, tc.expected.(uint32), got[0])
			case types.T_int64:
				got := vector.MustFixedColWithTypeCheck[int64](vec)
				require.Equal(t, tc.expected.(int64), got[0])
			case types.T_uint64:
				got := vector.MustFixedColWithTypeCheck[uint64](vec)
				require.Equal(t, tc.expected.(uint64), got[0])
			}
		})
	}
}

// TestParquet_StringToInt8_Negative tests negative values for signed types
func TestParquet_StringToInt8_Negative(t *testing.T) {
	proc := testutil.NewProc(t)

	strValues := []string{"-128", "-127", "-1", "0", "1", "126", "127"}
	values := make([]parquet.Value, len(strValues))
	for i, s := range strValues {
		values[i] = parquet.ByteArrayValue([]byte(s))
	}

	st := parquet.String().Type()
	var buf bytes.Buffer
	schema := parquet.NewSchema("x", parquet.Group{"c": parquet.Leaf(st)})
	w := parquet.NewWriter(&buf, schema)
	for _, val := range values {
		row := parquet.Row{val}
		_, err := w.WriteRows([]parquet.Row{row})
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())

	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	col := f.Root().Column("c")
	page, err := col.Pages().ReadPage()
	require.NoError(t, err)

	vec := vector.NewVec(types.New(types.T_int8, 0, 0))
	var h ParquetHandler
	mp := h.getMapper(col, plan.Type{Id: int32(types.T_int8), NotNullable: true})
	require.NotNil(t, mp)
	err = mp.mapping(page, proc, vec)
	require.NoError(t, err)

	got := vector.MustFixedColWithTypeCheck[int8](vec)
	require.Equal(t, []int8{-128, -127, -1, 0, 1, 126, 127}, got)
}

// TestParquet_StringToInt64_Dictionary tests STRING → INT64 with dictionary encoding
func TestParquet_StringToInt64_Dictionary(t *testing.T) {
	proc := testutil.NewProc(t)

	// Create dictionary-encoded string column
	node := parquet.Encoded(parquet.Leaf(parquet.String().Type()), &parquet.RLEDictionary)
	vals := []parquet.Value{
		parquet.ByteArrayValue([]byte("100")),
		parquet.ByteArrayValue([]byte("-200")),
		parquet.ByteArrayValue([]byte("100")),
		parquet.ByteArrayValue([]byte("300")),
		parquet.ByteArrayValue([]byte("-200")),
	}
	f, page := writeDictAndGetPage(t, node, vals)

	vec := vector.NewVec(types.New(types.T_int64, 0, 0))
	var h ParquetHandler
	mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_int64), NotNullable: true})
	require.NotNil(t, mp, "STRING → INT64 with dictionary encoding should be supported")
	require.NoError(t, mp.mapping(page, proc, vec))

	got := vector.MustFixedColWithTypeCheck[int64](vec)
	require.Equal(t, []int64{100, -200, 100, 300, -200}, got)
}

// TestParquet_StringToUInt32_Dictionary tests STRING → UINT32 with dictionary encoding
func TestParquet_StringToUInt32_Dictionary(t *testing.T) {
	proc := testutil.NewProc(t)

	// Create dictionary-encoded string column
	node := parquet.Encoded(parquet.Leaf(parquet.String().Type()), &parquet.RLEDictionary)
	vals := []parquet.Value{
		parquet.ByteArrayValue([]byte("100")),
		parquet.ByteArrayValue([]byte("200")),
		parquet.ByteArrayValue([]byte("100")),
		parquet.ByteArrayValue([]byte("4294967295")),
		parquet.ByteArrayValue([]byte("200")),
	}
	f, page := writeDictAndGetPage(t, node, vals)

	vec := vector.NewVec(types.New(types.T_uint32, 0, 0))
	var h ParquetHandler
	mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_uint32), NotNullable: true})
	require.NotNil(t, mp, "STRING → UINT32 with dictionary encoding should be supported")
	require.NoError(t, mp.mapping(page, proc, vec))

	got := vector.MustFixedColWithTypeCheck[uint32](vec)
	require.Equal(t, []uint32{100, 200, 100, 4294967295, 200}, got)
}

// TestParquet_StringToInt_WithNulls tests STRING → INT with NULL values
func TestParquet_StringToInt_WithNulls(t *testing.T) {
	proc := testutil.NewProc(t)

	// Create optional string column with NULLs
	var buf bytes.Buffer
	schema := parquet.NewSchema("x", parquet.Group{
		"c": parquet.Optional(parquet.Leaf(parquet.String().Type())),
	})
	w := parquet.NewWriter(&buf, schema)
	rows := []parquet.Row{
		{parquet.ByteArrayValue([]byte("42")).Level(0, 1, 0)},  // present
		{parquet.NullValue().Level(0, 0, 0)},                   // null
		{parquet.ByteArrayValue([]byte("-99")).Level(0, 1, 0)}, // present
	}
	_, err := w.WriteRows(rows)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	col := f.Root().Column("c")
	page, err := col.Pages().ReadPage()
	require.NoError(t, err)

	vec := vector.NewVec(types.New(types.T_int64, 0, 0))
	var h ParquetHandler
	mp := h.getMapper(col, plan.Type{Id: int32(types.T_int64) /* nullable */})
	require.NotNil(t, mp, "STRING → INT64 with NULL values should be supported")
	require.NoError(t, mp.mapping(page, proc, vec))

	require.Equal(t, 3, vec.Length())
	require.True(t, vec.GetNulls().Contains(1), "NULL should be at index 1")
	got := vector.MustFixedColWithTypeCheck[int64](vec)
	require.Equal(t, int64(42), got[0])
	require.Equal(t, int64(-99), got[2])
}

// TestParquet_StringToInt_MixedSizes tests various sizes in one test
func TestParquet_StringToInt_MixedSizes(t *testing.T) {
	proc := testutil.NewProc(t)

	// INT32 with various string values including leading/trailing spaces handling
	strValues := []string{"0", "1", "-1", "100", "-100", "1000000", "-1000000"}
	values := make([]parquet.Value, len(strValues))
	for i, s := range strValues {
		values[i] = parquet.ByteArrayValue([]byte(s))
	}

	st := parquet.String().Type()
	var buf bytes.Buffer
	schema := parquet.NewSchema("x", parquet.Group{"c": parquet.Leaf(st)})
	w := parquet.NewWriter(&buf, schema)
	for _, val := range values {
		row := parquet.Row{val}
		_, err := w.WriteRows([]parquet.Row{row})
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())

	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	col := f.Root().Column("c")
	page, err := col.Pages().ReadPage()
	require.NoError(t, err)

	vec := vector.NewVec(types.New(types.T_int32, 0, 0))
	var h ParquetHandler
	mp := h.getMapper(col, plan.Type{Id: int32(types.T_int32), NotNullable: true})
	require.NotNil(t, mp)
	err = mp.mapping(page, proc, vec)
	require.NoError(t, err)

	got := vector.MustFixedColWithTypeCheck[int32](vec)
	require.Equal(t, []int32{0, 1, -1, 100, -100, 1000000, -1000000}, got)
}
