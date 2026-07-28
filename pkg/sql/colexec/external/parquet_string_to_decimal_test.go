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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

// TestParquet_StringToDecimal64_BasicConversions tests basic STRING → DECIMAL64 conversions
func TestParquet_StringToDecimal64_BasicConversions(t *testing.T) {
	proc := testutil.NewProc(t)

	tests := []struct {
		name      string
		strValues []string
		width     int32
		scale     int32
		expected  []types.Decimal64
	}{
		{
			name:      "Integer strings to DECIMAL(10,0)",
			strValues: []string{"123", "456", "0", "-789"},
			width:     10,
			scale:     0,
			expected: []types.Decimal64{
				mustParseDecimal64(t, "123", 10, 0),
				mustParseDecimal64(t, "456", 10, 0),
				mustParseDecimal64(t, "0", 10, 0),
				mustParseDecimal64(t, "-789", 10, 0),
			},
		},
		{
			name:      "Decimal strings to DECIMAL(10,2)",
			strValues: []string{"123.45", "0.01", "99.99", "-12.34"},
			width:     10,
			scale:     2,
			expected: []types.Decimal64{
				mustParseDecimal64(t, "123.45", 10, 2),
				mustParseDecimal64(t, "0.01", 10, 2),
				mustParseDecimal64(t, "99.99", 10, 2),
				mustParseDecimal64(t, "-12.34", 10, 2),
			},
		},
		{
			name:      "Scientific notation to DECIMAL(18,2)",
			strValues: []string{"1e5", "1E5", "1e2", "1E2", "1.5e-1"},
			width:     18,
			scale:     2,
			expected: []types.Decimal64{
				mustParseDecimal64(t, "100000", 18, 2),
				mustParseDecimal64(t, "100000", 18, 2), // 1E5 (uppercase E)
				mustParseDecimal64(t, "100", 18, 2),
				mustParseDecimal64(t, "100", 18, 2), // 1E2 (uppercase E)
				mustParseDecimal64(t, "0.15", 18, 2),
			},
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
			vec := vector.NewVec(types.New(types.T_decimal64, tc.width, tc.scale))
			var h ParquetHandler
			mp := h.getMapper(col, plan.Type{
				Id:          int32(types.T_decimal64),
				Width:       tc.width,
				Scale:       tc.scale,
				NotNullable: true,
			})
			require.NotNil(t, mp, "STRING → DECIMAL64 should be supported")
			require.NoError(t, mp.mapping(page, proc, vec))

			// Verify results
			got := vector.MustFixedColWithTypeCheck[types.Decimal64](vec)
			require.Equal(t, len(tc.expected), len(got), "length mismatch")
			for i := range tc.expected {
				require.Equal(t, tc.expected[i], got[i], "value mismatch at index %d", i)
			}
		})
	}
}

// TestParquet_StringToDecimal64_SpecialFormats tests special string formats
func TestParquet_StringToDecimal64_SpecialFormats(t *testing.T) {
	proc := testutil.NewProc(t)

	tests := []struct {
		name     string
		strValue string
		width    int32
		scale    int32
		expected types.Decimal64
	}{
		{
			name:     "Leading spaces",
			strValue: " 123.45",
			width:    10,
			scale:    2,
			expected: mustParseDecimal64(t, "123.45", 10, 2),
		},
		{
			name:     "Trailing spaces",
			strValue: "123.45 ",
			width:    10,
			scale:    2,
			expected: mustParseDecimal64(t, "123.45", 10, 2),
		},
		{
			name:     "Both spaces",
			strValue: " 123.45 ",
			width:    10,
			scale:    2,
			expected: mustParseDecimal64(t, "123.45", 10, 2),
		},
		{
			name:     "Tab and newline",
			strValue: "\t123.45\n",
			width:    10,
			scale:    2,
			expected: mustParseDecimal64(t, "123.45", 10, 2),
		},
		{
			name:     "Leading zeros",
			strValue: "0123.45",
			width:    10,
			scale:    2,
			expected: mustParseDecimal64(t, "123.45", 10, 2),
		},
		{
			name:     "Positive sign",
			strValue: "+123.45",
			width:    10,
			scale:    2,
			expected: mustParseDecimal64(t, "123.45", 10, 2),
		},
		{
			name:     "Uppercase E scientific",
			strValue: "1.23E5",
			width:    18,
			scale:    2,
			expected: mustParseDecimal64(t, "123000", 18, 2),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			st := parquet.String().Type()
			var buf bytes.Buffer
			schema := parquet.NewSchema("x", parquet.Group{"c": parquet.Leaf(st)})
			w := parquet.NewWriter(&buf, schema)
			row := parquet.Row{parquet.ByteArrayValue([]byte(tc.strValue))}
			_, err := w.WriteRows([]parquet.Row{row})
			require.NoError(t, err)
			require.NoError(t, w.Close())

			f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
			require.NoError(t, err)
			col := f.Root().Column("c")
			page, err := col.Pages().ReadPage()
			require.NoError(t, err)

			vec := vector.NewVec(types.New(types.T_decimal64, tc.width, tc.scale))
			var h ParquetHandler
			mp := h.getMapper(col, plan.Type{
				Id:          int32(types.T_decimal64),
				Width:       tc.width,
				Scale:       tc.scale,
				NotNullable: true,
			})
			require.NotNil(t, mp)
			require.NoError(t, mp.mapping(page, proc, vec))

			got := vector.MustFixedColWithTypeCheck[types.Decimal64](vec)
			require.Equal(t, 1, len(got))
			require.Equal(t, tc.expected, got[0])
		})
	}
}

// TestParquet_StringToDecimal64_BoundaryValues tests boundary values for DECIMAL64
func TestParquet_StringToDecimal64_BoundaryValues(t *testing.T) {
	proc := testutil.NewProc(t)

	tests := []struct {
		name     string
		strValue string
		width    int32
		scale    int32
		expected types.Decimal64
	}{
		{
			name:     "Max precision DECIMAL(18,0)",
			strValue: "999999999999999999",
			width:    18,
			scale:    0,
			expected: mustParseDecimal64(t, "999999999999999999", 18, 0),
		},
		{
			name:     "Min precision DECIMAL(18,0)",
			strValue: "-999999999999999999",
			width:    18,
			scale:    0,
			expected: mustParseDecimal64(t, "-999999999999999999", 18, 0),
		},
		{
			name:     "Max with scale DECIMAL(18,2)",
			strValue: "9999999999999999.99",
			width:    18,
			scale:    2,
			expected: mustParseDecimal64(t, "9999999999999999.99", 18, 2),
		},
		{
			name:     "High scale DECIMAL(10,6)",
			strValue: "123.456789",
			width:    10,
			scale:    6,
			expected: mustParseDecimal64(t, "123.456789", 10, 6),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			st := parquet.String().Type()
			var buf bytes.Buffer
			schema := parquet.NewSchema("x", parquet.Group{"c": parquet.Leaf(st)})
			w := parquet.NewWriter(&buf, schema)
			row := parquet.Row{parquet.ByteArrayValue([]byte(tc.strValue))}
			_, err := w.WriteRows([]parquet.Row{row})
			require.NoError(t, err)
			require.NoError(t, w.Close())

			f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
			require.NoError(t, err)
			col := f.Root().Column("c")
			page, err := col.Pages().ReadPage()
			require.NoError(t, err)

			vec := vector.NewVec(types.New(types.T_decimal64, tc.width, tc.scale))
			var h ParquetHandler
			mp := h.getMapper(col, plan.Type{
				Id:          int32(types.T_decimal64),
				Width:       tc.width,
				Scale:       tc.scale,
				NotNullable: true,
			})
			require.NotNil(t, mp)
			require.NoError(t, mp.mapping(page, proc, vec))

			got := vector.MustFixedColWithTypeCheck[types.Decimal64](vec)
			require.Equal(t, 1, len(got))
			require.Equal(t, tc.expected, got[0])
		})
	}
}

// TestParquet_StringToDecimal64_InvalidFormats tests error handling
func TestParquet_StringToDecimal64_InvalidFormats(t *testing.T) {
	proc := testutil.NewProc(t)

	tests := []struct {
		name     string
		strValue string
		width    int32
		scale    int32
	}{
		{name: "Letters", strValue: "abc", width: 10, scale: 2},
		{name: "Mixed", strValue: "12abc34", width: 10, scale: 2},
		{name: "Empty string", strValue: "", width: 10, scale: 2},
		{name: "Only spaces", strValue: "   ", width: 10, scale: 2},
		{name: "Special chars", strValue: "12@34", width: 10, scale: 2},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			st := parquet.String().Type()
			var buf bytes.Buffer
			schema := parquet.NewSchema("x", parquet.Group{"c": parquet.Leaf(st)})
			w := parquet.NewWriter(&buf, schema)
			row := parquet.Row{parquet.ByteArrayValue([]byte(tc.strValue))}
			_, err := w.WriteRows([]parquet.Row{row})
			require.NoError(t, err)
			require.NoError(t, w.Close())

			f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
			require.NoError(t, err)
			col := f.Root().Column("c")
			page, err := col.Pages().ReadPage()
			require.NoError(t, err)

			vec := vector.NewVec(types.New(types.T_decimal64, tc.width, tc.scale))
			var h ParquetHandler
			mp := h.getMapper(col, plan.Type{
				Id:          int32(types.T_decimal64),
				Width:       tc.width,
				Scale:       tc.scale,
				NotNullable: true,
			})
			require.NotNil(t, mp)

			// Should return error
			err = mp.mapping(page, proc, vec)
			require.Error(t, err, "Expected error for invalid format: %s", tc.strValue)
		})
	}
}

// TestParquet_StringToDecimal128_BasicConversions tests DECIMAL128 conversions
func TestParquet_StringToDecimal128_BasicConversions(t *testing.T) {
	proc := testutil.NewProc(t)

	tests := []struct {
		name      string
		strValues []string
		width     int32
		scale     int32
		expected  []types.Decimal128
	}{
		{
			name:      "Large numbers to DECIMAL(38,0)",
			strValues: []string{"12345678901234567890", "-9876543210987654321"},
			width:     38,
			scale:     0,
			expected: []types.Decimal128{
				mustParseDecimal128(t, "12345678901234567890", 38, 0),
				mustParseDecimal128(t, "-9876543210987654321", 38, 0),
			},
		},
		{
			name:      "High precision DECIMAL(38,20)",
			strValues: []string{"123456789012345678.12345678901234567890"},
			width:     38,
			scale:     20,
			expected: []types.Decimal128{
				mustParseDecimal128(t, "123456789012345678.12345678901234567890", 38, 20),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			st := parquet.String().Type()
			var buf bytes.Buffer
			schema := parquet.NewSchema("x", parquet.Group{"c": parquet.Leaf(st)})
			w := parquet.NewWriter(&buf, schema)

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

			vec := vector.NewVec(types.New(types.T_decimal128, tc.width, tc.scale))
			var h ParquetHandler
			mp := h.getMapper(col, plan.Type{
				Id:          int32(types.T_decimal128),
				Width:       tc.width,
				Scale:       tc.scale,
				NotNullable: true,
			})
			require.NotNil(t, mp, "STRING → DECIMAL128 should be supported")
			require.NoError(t, mp.mapping(page, proc, vec))

			got := vector.MustFixedColWithTypeCheck[types.Decimal128](vec)
			require.Equal(t, len(tc.expected), len(got))
			for i := range tc.expected {
				require.Equal(t, tc.expected[i], got[i], "value mismatch at index %d", i)
			}
		})
	}
}

// Helper functions
func mustParseDecimal64(t *testing.T, s string, width, scale int32) types.Decimal64 {
	result, err := types.ParseDecimal64(s, width, scale)
	require.NoError(t, err, "Failed to parse decimal64: %s", s)
	return result
}

func mustParseDecimal128(t *testing.T, s string, width, scale int32) types.Decimal128 {
	result, err := types.ParseDecimal128(s, width, scale)
	require.NoError(t, err, "Failed to parse decimal128: %s", s)
	return result
}
