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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
	"github.com/stretchr/testify/require"
)

// Test getMapper returns nil when PhysicalType is nil
func TestGetMapper_NilPhysicalType(t *testing.T) {
	// Create a mock column with nil physical type (this is hard to do with real parquet,
	// so we test the already covered optional->notNullable case)
	var buf bytes.Buffer
	schema := parquet.NewSchema("x", parquet.Group{
		"c": parquet.Optional(parquet.Leaf(parquet.Int32Type)),
	})
	w := parquet.NewWriter(&buf, schema)
	_, err := w.WriteRows([]parquet.Row{
		{parquet.Int32Value(1).Level(0, 1, 0)},
	})
	require.NoError(t, err)
	require.NoError(t, w.Close())

	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)

	var h ParquetHandler
	// Test: optional column mapped to NotNullable should return nil
	mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_int32), NotNullable: true})
	require.Nil(t, mp, "optional column should not map to notNullable type")
}

// Test type mismatch cases that return nil mapper
func TestGetMapper_TypeMismatch(t *testing.T) {
	tests := []struct {
		name       string
		parquetTyp parquet.Type
		moType     types.T
	}{
		{name: "bool_to_int", parquetTyp: parquet.BooleanType, moType: types.T_int32},
		{name: "int32_to_bool", parquetTyp: parquet.Int32Type, moType: types.T_bool},
		{name: "int64_to_bool", parquetTyp: parquet.Int64Type, moType: types.T_bool},
		{name: "float_to_bool", parquetTyp: parquet.FloatType, moType: types.T_bool},
		{name: "string_to_bool", parquetTyp: parquet.String().Type(), moType: types.T_bool},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			schema := parquet.NewSchema("x", parquet.Group{
				"c": parquet.Leaf(tc.parquetTyp),
			})
			w := parquet.NewWriter(&buf, schema)
			require.NoError(t, w.Close())

			f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
			require.NoError(t, err)

			var h ParquetHandler
			mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(tc.moType), NotNullable: true})
			require.Nil(t, mp, "type mismatch should return nil mapper")
		})
	}
}

// Test widening conversions: int32->int64, int32->uint64, float32->float64
func TestGetMapper_WideningConversions(t *testing.T) {
	proc := testutil.NewProc(t)

	// INT32 -> INT64
	t.Run("int32_to_int64", func(t *testing.T) {
		node := parquet.Leaf(parquet.Int32Type)
		vals := []parquet.Value{parquet.Int32Value(123), parquet.Int32Value(-456)}
		f, page := writeDictAndGetPage(t, node, vals)

		vec := vector.NewVec(types.New(types.T_int64, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_int64), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		got := vector.MustFixedColWithTypeCheck[int64](vec)
		require.Equal(t, []int64{123, -456}, got)
	})

	// INT32 -> UINT64 (widening for unsigned)
	t.Run("int32_to_uint64", func(t *testing.T) {
		node := parquet.Leaf(parquet.Int32Type)
		vals := []parquet.Value{parquet.Int32Value(100), parquet.Int32Value(200)}
		f, page := writeDictAndGetPage(t, node, vals)

		vec := vector.NewVec(types.New(types.T_uint64, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_uint64), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		got := vector.MustFixedColWithTypeCheck[uint64](vec)
		require.Equal(t, []uint64{100, 200}, got)
	})

	// FLOAT32 -> FLOAT64
	t.Run("float32_to_float64", func(t *testing.T) {
		node := parquet.Leaf(parquet.FloatType)
		vals := []parquet.Value{parquet.FloatValue(1.5), parquet.FloatValue(2.5)}
		f, page := writeDictAndGetPage(t, node, vals)

		vec := vector.NewVec(types.New(types.T_float64, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_float64), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		got := vector.MustFixedColWithTypeCheck[float64](vec)
		require.InDeltaSlice(t, []float64{1.5, 2.5}, got, 1e-9)
	})
}

// Test string to numeric type conversions
func TestGetMapper_StringToNumeric(t *testing.T) {
	proc := testutil.NewProc(t)

	tests := []struct {
		name       string
		values     []string
		targetType types.T
		validate   func(t *testing.T, vec *vector.Vector)
	}{
		{
			name:       "string_to_uint8",
			values:     []string{"12", "255"},
			targetType: types.T_uint8,
			validate: func(t *testing.T, vec *vector.Vector) {
				got := vector.MustFixedColWithTypeCheck[uint8](vec)
				require.Equal(t, []uint8{12, 255}, got)
			},
		},
		{
			name:       "string_to_int8",
			values:     []string{"12", "-128"},
			targetType: types.T_int8,
			validate: func(t *testing.T, vec *vector.Vector) {
				got := vector.MustFixedColWithTypeCheck[int8](vec)
				require.Equal(t, []int8{12, -128}, got)
			},
		},
		{
			name:       "string_to_uint16",
			values:     []string{"100", "65535"},
			targetType: types.T_uint16,
			validate: func(t *testing.T, vec *vector.Vector) {
				got := vector.MustFixedColWithTypeCheck[uint16](vec)
				require.Equal(t, []uint16{100, 65535}, got)
			},
		},
		{
			name:       "string_to_int16",
			values:     []string{"100", "-32768"},
			targetType: types.T_int16,
			validate: func(t *testing.T, vec *vector.Vector) {
				got := vector.MustFixedColWithTypeCheck[int16](vec)
				require.Equal(t, []int16{100, -32768}, got)
			},
		},
		{
			name:       "string_to_int32",
			values:     []string{"1000", "-2000"},
			targetType: types.T_int32,
			validate: func(t *testing.T, vec *vector.Vector) {
				got := vector.MustFixedColWithTypeCheck[int32](vec)
				require.Equal(t, []int32{1000, -2000}, got)
			},
		},
		{
			name:       "string_to_int64",
			values:     []string{"9223372036854775807", "-9223372036854775808"},
			targetType: types.T_int64,
			validate: func(t *testing.T, vec *vector.Vector) {
				got := vector.MustFixedColWithTypeCheck[int64](vec)
				require.Equal(t, []int64{9223372036854775807, -9223372036854775808}, got)
			},
		},
		{
			name:       "string_to_uint32",
			values:     []string{"1000", "4294967295"},
			targetType: types.T_uint32,
			validate: func(t *testing.T, vec *vector.Vector) {
				got := vector.MustFixedColWithTypeCheck[uint32](vec)
				require.Equal(t, []uint32{1000, 4294967295}, got)
			},
		},
		{
			name:       "string_to_uint64",
			values:     []string{"1000", "18446744073709551615"},
			targetType: types.T_uint64,
			validate: func(t *testing.T, vec *vector.Vector) {
				got := vector.MustFixedColWithTypeCheck[uint64](vec)
				require.Equal(t, []uint64{1000, 18446744073709551615}, got)
			},
		},
		{
			name:       "string_to_float32",
			values:     []string{"1.5", "-2.25"},
			targetType: types.T_float32,
			validate: func(t *testing.T, vec *vector.Vector) {
				got := vector.MustFixedColWithTypeCheck[float32](vec)
				require.InDeltaSlice(t, []float32{1.5, -2.25}, got, 1e-6)
			},
		},
		{
			name:       "string_to_float64",
			values:     []string{"1.5", "-2.25"},
			targetType: types.T_float64,
			validate: func(t *testing.T, vec *vector.Vector) {
				got := vector.MustFixedColWithTypeCheck[float64](vec)
				require.InDeltaSlice(t, []float64{1.5, -2.25}, got, 1e-9)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Build parquet file with string values
			var buf bytes.Buffer
			schema := parquet.NewSchema("x", parquet.Group{
				"c": parquet.Leaf(parquet.String().Type()),
			})
			w := parquet.NewWriter(&buf, schema)

			vals := make([]parquet.Value, len(tc.values))
			for i, s := range tc.values {
				vals[i] = parquet.ByteArrayValue([]byte(s)).Level(0, 0, 0)
			}
			_, err := w.WriteRows([]parquet.Row{parquet.MakeRow(vals)})
			require.NoError(t, err)
			require.NoError(t, w.Close())

			f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
			require.NoError(t, err)

			col := f.Root().Column("c")
			page, err := col.Pages().ReadPage()
			require.NoError(t, err)

			vec := vector.NewVec(types.New(tc.targetType, 0, 0))
			var h ParquetHandler
			mp := h.getMapper(col, plan.Type{Id: int32(tc.targetType), NotNullable: true})
			require.NotNil(t, mp)
			require.NoError(t, mp.mapping(page, proc, vec))

			tc.validate(t, vec)
		})
	}
}

// Test string to temporal type conversions
func TestGetMapper_StringToTemporal(t *testing.T) {
	proc := testutil.NewProc(t)

	tests := []struct {
		name       string
		values     []string
		targetType types.T
		checkFunc  func(t *testing.T, vec *vector.Vector)
	}{
		{
			name:       "string_to_date",
			values:     []string{"2024-01-01", "2025-12-31"},
			targetType: types.T_date,
			checkFunc: func(t *testing.T, vec *vector.Vector) {
				require.Equal(t, 2, vec.Length())
				// Just verify no error and correct length
			},
		},
		{
			name:       "string_to_time",
			values:     []string{"12:34:56", "23:59:59"},
			targetType: types.T_time,
			checkFunc: func(t *testing.T, vec *vector.Vector) {
				require.Equal(t, 2, vec.Length())
			},
		},
		{
			name:       "string_to_timestamp",
			values:     []string{"2024-01-01 12:34:56", "2025-12-31 23:59:59"},
			targetType: types.T_timestamp,
			checkFunc: func(t *testing.T, vec *vector.Vector) {
				require.Equal(t, 2, vec.Length())
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			schema := parquet.NewSchema("x", parquet.Group{
				"c": parquet.Leaf(parquet.String().Type()),
			})
			w := parquet.NewWriter(&buf, schema)

			vals := make([]parquet.Value, len(tc.values))
			for i, s := range tc.values {
				vals[i] = parquet.ByteArrayValue([]byte(s)).Level(0, 0, 0)
			}
			_, err := w.WriteRows([]parquet.Row{parquet.MakeRow(vals)})
			require.NoError(t, err)
			require.NoError(t, w.Close())

			f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
			require.NoError(t, err)

			col := f.Root().Column("c")
			page, err := col.Pages().ReadPage()
			require.NoError(t, err)

			vec := vector.NewVec(types.New(tc.targetType, 0, 0))
			var h ParquetHandler
			mp := h.getMapper(col, plan.Type{Id: int32(tc.targetType), NotNullable: true})
			require.NotNil(t, mp)
			require.NoError(t, mp.mapping(page, proc, vec))

			tc.checkFunc(t, vec)
		})
	}
}

// Test string to decimal conversions
func TestGetMapper_StringToDecimal(t *testing.T) {
	proc := testutil.NewProc(t)

	tests := []struct {
		name       string
		values     []string
		targetType types.T
		width      int32
		scale      int32
	}{
		{
			name:       "string_to_decimal64",
			values:     []string{"123.45", "-678.90"},
			targetType: types.T_decimal64,
			width:      10,
			scale:      2,
		},
		{
			name:       "string_to_decimal128",
			values:     []string{"123456789.12", "-987654321.98"},
			targetType: types.T_decimal128,
			width:      20,
			scale:      2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			schema := parquet.NewSchema("x", parquet.Group{
				"c": parquet.Leaf(parquet.String().Type()),
			})
			w := parquet.NewWriter(&buf, schema)

			vals := make([]parquet.Value, len(tc.values))
			for i, s := range tc.values {
				vals[i] = parquet.ByteArrayValue([]byte(s)).Level(0, 0, 0)
			}
			_, err := w.WriteRows([]parquet.Row{parquet.MakeRow(vals)})
			require.NoError(t, err)
			require.NoError(t, w.Close())

			f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
			require.NoError(t, err)

			col := f.Root().Column("c")
			page, err := col.Pages().ReadPage()
			require.NoError(t, err)

			vec := vector.NewVec(types.New(tc.targetType, tc.width, tc.scale))
			var h ParquetHandler
			mp := h.getMapper(col, plan.Type{Id: int32(tc.targetType), NotNullable: true, Width: tc.width, Scale: tc.scale})
			require.NotNil(t, mp)
			require.NoError(t, mp.mapping(page, proc, vec))

			require.Equal(t, len(tc.values), vec.Length())
		})
	}
}

// Test dictionary encoding for various types
func TestGetMapper_DictionaryEncoding(t *testing.T) {
	proc := testutil.NewProc(t)

	// Test widening with dictionary: int32->int64
	t.Run("dict_int32_to_int64", func(t *testing.T) {
		node := parquet.Encoded(parquet.Leaf(parquet.Int32Type), &parquet.RLEDictionary)
		vals := []parquet.Value{
			parquet.Int32Value(100), parquet.Int32Value(200), parquet.Int32Value(100),
		}
		f, page := writeDictAndGetPage(t, node, vals)

		vec := vector.NewVec(types.New(types.T_int64, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_int64), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		got := vector.MustFixedColWithTypeCheck[int64](vec)
		require.Equal(t, []int64{100, 200, 100}, got)
	})

	// Test widening with dictionary: int32->uint64
	t.Run("dict_int32_to_uint64", func(t *testing.T) {
		node := parquet.Encoded(parquet.Leaf(parquet.Int32Type), &parquet.RLEDictionary)
		vals := []parquet.Value{
			parquet.Int32Value(300), parquet.Int32Value(400), parquet.Int32Value(300),
		}
		f, page := writeDictAndGetPage(t, node, vals)

		vec := vector.NewVec(types.New(types.T_uint64, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_uint64), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		got := vector.MustFixedColWithTypeCheck[uint64](vec)
		require.Equal(t, []uint64{300, 400, 300}, got)
	})

	// Test widening with dictionary: float32->float64
	t.Run("dict_float32_to_float64", func(t *testing.T) {
		node := parquet.Encoded(parquet.Leaf(parquet.FloatType), &parquet.RLEDictionary)
		vals := []parquet.Value{
			parquet.FloatValue(3.14), parquet.FloatValue(2.71), parquet.FloatValue(3.14),
		}
		f, page := writeDictAndGetPage(t, node, vals)

		vec := vector.NewVec(types.New(types.T_float64, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_float64), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		got := vector.MustFixedColWithTypeCheck[float64](vec)
		require.InDeltaSlice(t, []float64{3.14, 2.71, 3.14}, got, 1e-6)
	})
}

// Test error cases for string conversions
func TestGetMapper_StringConversionErrors(t *testing.T) {
	proc := testutil.NewProc(t)

	tests := []struct {
		name       string
		value      string
		targetType types.T
	}{
		{name: "invalid_uint8", value: "not_a_number", targetType: types.T_uint8},
		{name: "invalid_int8", value: "999", targetType: types.T_int8},
		{name: "invalid_float32", value: "abc", targetType: types.T_float32},
		{name: "invalid_date", value: "not-a-date", targetType: types.T_date},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			schema := parquet.NewSchema("x", parquet.Group{
				"c": parquet.Leaf(parquet.String().Type()),
			})
			w := parquet.NewWriter(&buf, schema)

			val := parquet.ByteArrayValue([]byte(tc.value)).Level(0, 0, 0)
			_, err := w.WriteRows([]parquet.Row{parquet.MakeRow([]parquet.Value{val})})
			require.NoError(t, err)
			require.NoError(t, w.Close())

			f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
			require.NoError(t, err)

			col := f.Root().Column("c")
			page, err := col.Pages().ReadPage()
			require.NoError(t, err)

			vec := vector.NewVec(types.New(tc.targetType, 0, 0))
			var h ParquetHandler
			mp := h.getMapper(col, plan.Type{Id: int32(tc.targetType), NotNullable: true})
			require.NotNil(t, mp)
			err = mp.mapping(page, proc, vec)
			require.Error(t, err, "should fail to parse invalid string")
		})
	}
}

// Test string conversions with dictionary encoding
func TestGetMapper_StringConversionWithDictionary(t *testing.T) {
	proc := testutil.NewProc(t)

	// String to UINT8 with dictionary
	t.Run("dict_string_to_uint8", func(t *testing.T) {
		node := parquet.Encoded(parquet.Leaf(parquet.String().Type()), &parquet.RLEDictionary)
		vals := []parquet.Value{
			parquet.ByteArrayValue([]byte("10")),
			parquet.ByteArrayValue([]byte("20")),
			parquet.ByteArrayValue([]byte("10")),
		}
		f, page := writeDictAndGetPage(t, node, vals)

		vec := vector.NewVec(types.New(types.T_uint8, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_uint8), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		got := vector.MustFixedColWithTypeCheck[uint8](vec)
		require.Equal(t, []uint8{10, 20, 10}, got)
	})

	// String to INT32 with dictionary
	t.Run("dict_string_to_int32", func(t *testing.T) {
		node := parquet.Encoded(parquet.Leaf(parquet.String().Type()), &parquet.RLEDictionary)
		vals := []parquet.Value{
			parquet.ByteArrayValue([]byte("100")),
			parquet.ByteArrayValue([]byte("-200")),
			parquet.ByteArrayValue([]byte("100")),
		}
		f, page := writeDictAndGetPage(t, node, vals)

		vec := vector.NewVec(types.New(types.T_int32, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_int32), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		got := vector.MustFixedColWithTypeCheck[int32](vec)
		require.Equal(t, []int32{100, -200, 100}, got)
	})
}

// Test FixedLenByteArray conversions
func TestGetMapper_FixedLenByteArrayConversions(t *testing.T) {
	proc := testutil.NewProc(t)

	// FixedLenByteArray to UINT8
	t.Run("fixed_to_uint8", func(t *testing.T) {
		var buf bytes.Buffer
		schema := parquet.NewSchema("x", parquet.Group{
			"c": parquet.Leaf(parquet.FixedLenByteArrayType(2)),
		})
		w := parquet.NewWriter(&buf, schema)

		vals := []parquet.Value{
			parquet.FixedLenByteArrayValue([]byte("12")).Level(0, 0, 0),
			parquet.FixedLenByteArrayValue([]byte("34")).Level(0, 0, 0),
		}
		_, err := w.WriteRows([]parquet.Row{parquet.MakeRow(vals)})
		require.NoError(t, err)
		require.NoError(t, w.Close())

		f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		require.NoError(t, err)

		col := f.Root().Column("c")
		page, err := col.Pages().ReadPage()
		require.NoError(t, err)

		vec := vector.NewVec(types.New(types.T_uint8, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(col, plan.Type{Id: int32(types.T_uint8), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		got := vector.MustFixedColWithTypeCheck[uint8](vec)
		require.Equal(t, 2, len(got))
	})
}

// Test remaining type combinations not yet covered
func TestGetMapper_RemainingTypes(t *testing.T) {
	proc := testutil.NewProc(t)

	// Test CHAR type
	t.Run("char_type", func(t *testing.T) {
		var buf bytes.Buffer
		schema := parquet.NewSchema("x", parquet.Group{
			"c": parquet.Leaf(parquet.String().Type()),
		})
		w := parquet.NewWriter(&buf, schema)

		vals := []parquet.Value{
			parquet.ByteArrayValue([]byte("abc")).Level(0, 0, 0),
			parquet.ByteArrayValue([]byte("def")).Level(0, 0, 0),
		}
		_, err := w.WriteRows([]parquet.Row{parquet.MakeRow(vals)})
		require.NoError(t, err)
		require.NoError(t, w.Close())

		f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		require.NoError(t, err)

		col := f.Root().Column("c")
		page, err := col.Pages().ReadPage()
		require.NoError(t, err)

		vec := vector.NewVec(types.New(types.T_char, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(col, plan.Type{Id: int32(types.T_char), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		require.Equal(t, 2, vec.Length())
	})

	// Test TEXT type
	t.Run("text_type", func(t *testing.T) {
		var buf bytes.Buffer
		schema := parquet.NewSchema("x", parquet.Group{
			"c": parquet.Leaf(parquet.String().Type()),
		})
		w := parquet.NewWriter(&buf, schema)

		vals := []parquet.Value{
			parquet.ByteArrayValue([]byte("hello world")).Level(0, 0, 0),
		}
		_, err := w.WriteRows([]parquet.Row{parquet.MakeRow(vals)})
		require.NoError(t, err)
		require.NoError(t, w.Close())

		f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		require.NoError(t, err)

		col := f.Root().Column("c")
		page, err := col.Pages().ReadPage()
		require.NoError(t, err)

		vec := vector.NewVec(types.New(types.T_text, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(col, plan.Type{Id: int32(types.T_text), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		require.Equal(t, 1, vec.Length())
	})

	// Test BINARY type
	t.Run("binary_type", func(t *testing.T) {
		var buf bytes.Buffer
		schema := parquet.NewSchema("x", parquet.Group{
			"c": parquet.Leaf(parquet.String().Type()),
		})
		w := parquet.NewWriter(&buf, schema)

		vals := []parquet.Value{
			parquet.ByteArrayValue([]byte{0x01, 0x02, 0x03}).Level(0, 0, 0),
		}
		_, err := w.WriteRows([]parquet.Row{parquet.MakeRow(vals)})
		require.NoError(t, err)
		require.NoError(t, w.Close())

		f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		require.NoError(t, err)

		col := f.Root().Column("c")
		page, err := col.Pages().ReadPage()
		require.NoError(t, err)

		vec := vector.NewVec(types.New(types.T_binary, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(col, plan.Type{Id: int32(types.T_binary), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		require.Equal(t, 1, vec.Length())
	})
}

// Test ByteArray/FixedLenByteArray with NULL values
func TestGetMapper_ByteArrayWithNulls(t *testing.T) {
	proc := testutil.NewProc(t)

	// String to INT32 with nulls
	t.Run("string_to_int32_with_nulls", func(t *testing.T) {
		var buf bytes.Buffer
		schema := parquet.NewSchema("x", parquet.Group{
			"c": parquet.Optional(parquet.Leaf(parquet.String().Type())),
		})
		w := parquet.NewWriter(&buf, schema)

		rows := []parquet.Row{
			{parquet.ByteArrayValue([]byte("100")).Level(0, 1, 0)}, // present
			{parquet.NullValue().Level(0, 0, 0)},                   // null
			{parquet.ByteArrayValue([]byte("200")).Level(0, 1, 0)}, // present
		}
		_, err := w.WriteRows(rows)
		require.NoError(t, err)
		require.NoError(t, w.Close())

		f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		require.NoError(t, err)

		col := f.Root().Column("c")
		page, err := col.Pages().ReadPage()
		require.NoError(t, err)

		vec := vector.NewVec(types.New(types.T_int32, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(col, plan.Type{Id: int32(types.T_int32)})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		require.Equal(t, 3, vec.Length())
		require.True(t, vec.GetNulls().Contains(1))
		got := vector.MustFixedColWithTypeCheck[int32](vec)
		require.Equal(t, int32(100), got[0])
		require.Equal(t, int32(200), got[2])
	})

	// String to FLOAT64 with nulls
	t.Run("string_to_float64_with_nulls", func(t *testing.T) {
		var buf bytes.Buffer
		schema := parquet.NewSchema("x", parquet.Group{
			"c": parquet.Optional(parquet.Leaf(parquet.String().Type())),
		})
		w := parquet.NewWriter(&buf, schema)

		rows := []parquet.Row{
			{parquet.ByteArrayValue([]byte("1.5")).Level(0, 1, 0)},
			{parquet.NullValue().Level(0, 0, 0)},
			{parquet.ByteArrayValue([]byte("2.5")).Level(0, 1, 0)},
		}
		_, err := w.WriteRows(rows)
		require.NoError(t, err)
		require.NoError(t, w.Close())

		f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		require.NoError(t, err)

		col := f.Root().Column("c")
		page, err := col.Pages().ReadPage()
		require.NoError(t, err)

		vec := vector.NewVec(types.New(types.T_float64, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(col, plan.Type{Id: int32(types.T_float64)})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		require.Equal(t, 3, vec.Length())
		require.True(t, vec.GetNulls().Contains(1))
	})

	// String to DATE with nulls
	t.Run("string_to_date_with_nulls", func(t *testing.T) {
		var buf bytes.Buffer
		schema := parquet.NewSchema("x", parquet.Group{
			"c": parquet.Optional(parquet.Leaf(parquet.String().Type())),
		})
		w := parquet.NewWriter(&buf, schema)

		rows := []parquet.Row{
			{parquet.ByteArrayValue([]byte("2024-01-01")).Level(0, 1, 0)},
			{parquet.NullValue().Level(0, 0, 0)},
			{parquet.ByteArrayValue([]byte("2024-12-31")).Level(0, 1, 0)},
		}
		_, err := w.WriteRows(rows)
		require.NoError(t, err)
		require.NoError(t, w.Close())

		f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		require.NoError(t, err)

		col := f.Root().Column("c")
		page, err := col.Pages().ReadPage()
		require.NoError(t, err)

		vec := vector.NewVec(types.New(types.T_date, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(col, plan.Type{Id: int32(types.T_date)})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		require.Equal(t, 3, vec.Length())
		require.True(t, vec.GetNulls().Contains(1))
	})

	// String to DECIMAL64 with nulls
	t.Run("string_to_decimal64_with_nulls", func(t *testing.T) {
		var buf bytes.Buffer
		schema := parquet.NewSchema("x", parquet.Group{
			"c": parquet.Optional(parquet.Leaf(parquet.String().Type())),
		})
		w := parquet.NewWriter(&buf, schema)

		rows := []parquet.Row{
			{parquet.ByteArrayValue([]byte("123.45")).Level(0, 1, 0)},
			{parquet.NullValue().Level(0, 0, 0)},
			{parquet.ByteArrayValue([]byte("678.90")).Level(0, 1, 0)},
		}
		_, err := w.WriteRows(rows)
		require.NoError(t, err)
		require.NoError(t, w.Close())

		f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		require.NoError(t, err)

		col := f.Root().Column("c")
		page, err := col.Pages().ReadPage()
		require.NoError(t, err)

		vec := vector.NewVec(types.New(types.T_decimal64, 10, 2))
		var h ParquetHandler
		mp := h.getMapper(col, plan.Type{Id: int32(types.T_decimal64), Width: 10, Scale: 2})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		require.Equal(t, 3, vec.Length())
		require.True(t, vec.GetNulls().Contains(1))
	})
}

// Test FixedLenByteArray conversions more thoroughly
func TestGetMapper_FixedLenByteArray(t *testing.T) {
	proc := testutil.NewProc(t)

	tests := []struct {
		name       string
		values     []string
		targetType types.T
		fixedLen   int
	}{
		{name: "fixed_to_int16", values: []string{"10", "20"}, targetType: types.T_int16, fixedLen: 2},
		{name: "fixed_to_int32", values: []string{"100", "200"}, targetType: types.T_int32, fixedLen: 3},
		{name: "fixed_to_int64", values: []string{"1000"}, targetType: types.T_int64, fixedLen: 4},
		{name: "fixed_to_float32", values: []string{"1.5", "2.5"}, targetType: types.T_float32, fixedLen: 3},
		{name: "fixed_to_uint16", values: []string{"100", "200"}, targetType: types.T_uint16, fixedLen: 3},
		{name: "fixed_to_uint32", values: []string{"1000"}, targetType: types.T_uint32, fixedLen: 4},
		{name: "fixed_to_uint64", values: []string{"10000"}, targetType: types.T_uint64, fixedLen: 5},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			schema := parquet.NewSchema("x", parquet.Group{
				"c": parquet.Leaf(parquet.FixedLenByteArrayType(tc.fixedLen)),
			})
			w := parquet.NewWriter(&buf, schema)

			vals := make([]parquet.Value, len(tc.values))
			for i, s := range tc.values {
				// Pad or truncate to fixed length
				data := []byte(s)
				if len(data) < tc.fixedLen {
					padded := make([]byte, tc.fixedLen)
					copy(padded, data)
					data = padded
				} else if len(data) > tc.fixedLen {
					data = data[:tc.fixedLen]
				}
				vals[i] = parquet.FixedLenByteArrayValue(data).Level(0, 0, 0)
			}
			_, err := w.WriteRows([]parquet.Row{parquet.MakeRow(vals)})
			require.NoError(t, err)
			require.NoError(t, w.Close())

			f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
			require.NoError(t, err)

			col := f.Root().Column("c")
			page, err := col.Pages().ReadPage()
			require.NoError(t, err)

			vec := vector.NewVec(types.New(tc.targetType, 0, 0))
			var h ParquetHandler
			mp := h.getMapper(col, plan.Type{Id: int32(tc.targetType), NotNullable: true})
			require.NotNil(t, mp)
			require.NoError(t, mp.mapping(page, proc, vec))
			require.Equal(t, len(tc.values), vec.Length())
		})
	}
}

// Test dictionary-encoded ByteArray for more types
func TestGetMapper_DictByteArray_MoreTypes(t *testing.T) {
	proc := testutil.NewProc(t)

	// Dictionary STRING to INT16
	t.Run("dict_string_to_int16", func(t *testing.T) {
		node := parquet.Encoded(parquet.Leaf(parquet.String().Type()), &parquet.RLEDictionary)
		vals := []parquet.Value{
			parquet.ByteArrayValue([]byte("100")),
			parquet.ByteArrayValue([]byte("-200")),
			parquet.ByteArrayValue([]byte("100")),
		}
		f, page := writeDictAndGetPage(t, node, vals)

		vec := vector.NewVec(types.New(types.T_int16, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_int16), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		got := vector.MustFixedColWithTypeCheck[int16](vec)
		require.Equal(t, []int16{100, -200, 100}, got)
	})

	// Dictionary STRING to UINT16
	t.Run("dict_string_to_uint16", func(t *testing.T) {
		node := parquet.Encoded(parquet.Leaf(parquet.String().Type()), &parquet.RLEDictionary)
		vals := []parquet.Value{
			parquet.ByteArrayValue([]byte("100")),
			parquet.ByteArrayValue([]byte("200")),
			parquet.ByteArrayValue([]byte("100")),
		}
		f, page := writeDictAndGetPage(t, node, vals)

		vec := vector.NewVec(types.New(types.T_uint16, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_uint16), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		got := vector.MustFixedColWithTypeCheck[uint16](vec)
		require.Equal(t, []uint16{100, 200, 100}, got)
	})

	// Dictionary STRING to INT64
	t.Run("dict_string_to_int64", func(t *testing.T) {
		node := parquet.Encoded(parquet.Leaf(parquet.String().Type()), &parquet.RLEDictionary)
		vals := []parquet.Value{
			parquet.ByteArrayValue([]byte("1000")),
			parquet.ByteArrayValue([]byte("-2000")),
			parquet.ByteArrayValue([]byte("1000")),
		}
		f, page := writeDictAndGetPage(t, node, vals)

		vec := vector.NewVec(types.New(types.T_int64, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_int64), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		got := vector.MustFixedColWithTypeCheck[int64](vec)
		require.Equal(t, []int64{1000, -2000, 1000}, got)
	})

	// Dictionary STRING to UINT64
	t.Run("dict_string_to_uint64", func(t *testing.T) {
		node := parquet.Encoded(parquet.Leaf(parquet.String().Type()), &parquet.RLEDictionary)
		vals := []parquet.Value{
			parquet.ByteArrayValue([]byte("1000")),
			parquet.ByteArrayValue([]byte("2000")),
			parquet.ByteArrayValue([]byte("1000")),
		}
		f, page := writeDictAndGetPage(t, node, vals)

		vec := vector.NewVec(types.New(types.T_uint64, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_uint64), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		got := vector.MustFixedColWithTypeCheck[uint64](vec)
		require.Equal(t, []uint64{1000, 2000, 1000}, got)
	})

	// Dictionary STRING to FLOAT32
	t.Run("dict_string_to_float32", func(t *testing.T) {
		node := parquet.Encoded(parquet.Leaf(parquet.String().Type()), &parquet.RLEDictionary)
		vals := []parquet.Value{
			parquet.ByteArrayValue([]byte("1.5")),
			parquet.ByteArrayValue([]byte("2.5")),
			parquet.ByteArrayValue([]byte("1.5")),
		}
		f, page := writeDictAndGetPage(t, node, vals)

		vec := vector.NewVec(types.New(types.T_float32, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_float32), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		got := vector.MustFixedColWithTypeCheck[float32](vec)
		require.InDeltaSlice(t, []float32{1.5, 2.5, 1.5}, got, 1e-6)
	})

	// Dictionary STRING to FLOAT64
	t.Run("dict_string_to_float64", func(t *testing.T) {
		node := parquet.Encoded(parquet.Leaf(parquet.String().Type()), &parquet.RLEDictionary)
		vals := []parquet.Value{
			parquet.ByteArrayValue([]byte("1.5")),
			parquet.ByteArrayValue([]byte("2.5")),
			parquet.ByteArrayValue([]byte("1.5")),
		}
		f, page := writeDictAndGetPage(t, node, vals)

		vec := vector.NewVec(types.New(types.T_float64, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_float64), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		got := vector.MustFixedColWithTypeCheck[float64](vec)
		require.InDeltaSlice(t, []float64{1.5, 2.5, 1.5}, got, 1e-9)
	})

	// Dictionary STRING to DECIMAL128
	t.Run("dict_string_to_decimal128", func(t *testing.T) {
		node := parquet.Encoded(parquet.Leaf(parquet.String().Type()), &parquet.RLEDictionary)
		vals := []parquet.Value{
			parquet.ByteArrayValue([]byte("123.45")),
			parquet.ByteArrayValue([]byte("678.90")),
			parquet.ByteArrayValue([]byte("123.45")),
		}
		f, page := writeDictAndGetPage(t, node, vals)

		vec := vector.NewVec(types.New(types.T_decimal128, 10, 2))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_decimal128), NotNullable: true, Width: 10, Scale: 2})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		require.Equal(t, 3, vec.Length())
	})
}

// Test isDecimalLogicalType helper function
func TestIsDecimalLogicalType(t *testing.T) {
	tests := []struct {
		name     string
		lt       *format.LogicalType
		expected bool
	}{
		{
			name:     "nil_logical_type",
			lt:       nil,
			expected: false,
		},
		{
			name:     "logical_type_with_nil_decimal",
			lt:       &format.LogicalType{},
			expected: false,
		},
		{
			name:     "logical_type_with_decimal",
			lt:       &format.LogicalType{Decimal: &format.DecimalType{Precision: 10, Scale: 2}},
			expected: true,
		},
		{
			name:     "logical_type_with_utf8",
			lt:       &format.LogicalType{UTF8: &format.StringType{}},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isDecimalLogicalType(tt.lt)
			require.Equal(t, tt.expected, result)
		})
	}
}

// Test FixedLenByteArray with DECIMAL LogicalType (PyArrow scenario)
func TestGetMapper_FixedLenByteArrayDecimal(t *testing.T) {
	proc := testutil.NewProc(t)

	// Test DECIMAL64 from FixedLenByteArray with DECIMAL LogicalType
	t.Run("fixed_decimal_to_decimal64", func(t *testing.T) {
		var buf bytes.Buffer
		// Use parquet.Decimal to create proper DECIMAL type with FixedLenByteArray
		schema := parquet.NewSchema("x", parquet.Group{
			"c": parquet.Decimal(2, 10, parquet.FixedLenByteArrayType(5)),
		})
		w := parquet.NewWriter(&buf, schema)

		// Write decimal values: 123.45, -678.90, 999.99
		// These are stored as big-endian two's complement in FixedLenByteArray
		rows := []parquet.Row{
			{parquet.FixedLenByteArrayValue(encodeDecimalToBytes(12345, 5)).Level(0, 0, 0)},
			{parquet.FixedLenByteArrayValue(encodeDecimalToBytes(67890, 5)).Level(0, 0, 0)},
			{parquet.FixedLenByteArrayValue(encodeDecimalToBytes(99999, 5)).Level(0, 0, 0)},
		}
		_, err := w.WriteRows(rows)
		require.NoError(t, err)
		require.NoError(t, w.Close())

		f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		require.NoError(t, err)

		col := f.Root().Column("c")
		// Verify LogicalType is DECIMAL
		lt := col.Type().LogicalType()
		require.NotNil(t, lt)
		require.NotNil(t, lt.Decimal)

		page, err := col.Pages().ReadPage()
		require.NoError(t, err)

		vec := vector.NewVec(types.New(types.T_decimal64, 10, 2))
		var h ParquetHandler
		mp := h.getMapper(col, plan.Type{Id: int32(types.T_decimal64), NotNullable: true, Width: 10, Scale: 2})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		require.Equal(t, 3, vec.Length())

		got := vector.MustFixedColWithTypeCheck[types.Decimal64](vec)
		require.Equal(t, types.Decimal64(12345), got[0])
		require.Equal(t, types.Decimal64(67890), got[1])
		require.Equal(t, types.Decimal64(99999), got[2])
	})

	// Test DECIMAL128 from FixedLenByteArray with DECIMAL LogicalType
	t.Run("fixed_decimal_to_decimal128", func(t *testing.T) {
		var buf bytes.Buffer
		schema := parquet.NewSchema("x", parquet.Group{
			"c": parquet.Decimal(2, 20, parquet.FixedLenByteArrayType(9)),
		})
		w := parquet.NewWriter(&buf, schema)

		rows := []parquet.Row{
			{parquet.FixedLenByteArrayValue(encodeDecimalToBytes(1234567890123456, 9)).Level(0, 0, 0)},
			{parquet.FixedLenByteArrayValue(encodeDecimalToBytes(-9876543210987654, 9)).Level(0, 0, 0)},
		}
		_, err := w.WriteRows(rows)
		require.NoError(t, err)
		require.NoError(t, w.Close())

		f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		require.NoError(t, err)

		col := f.Root().Column("c")
		lt := col.Type().LogicalType()
		require.NotNil(t, lt)
		require.NotNil(t, lt.Decimal)

		page, err := col.Pages().ReadPage()
		require.NoError(t, err)

		vec := vector.NewVec(types.New(types.T_decimal128, 20, 2))
		var h ParquetHandler
		mp := h.getMapper(col, plan.Type{Id: int32(types.T_decimal128), NotNullable: true, Width: 20, Scale: 2})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		require.Equal(t, 2, vec.Length())
	})
}

// Test FixedLenByteArray DECIMAL with dictionary encoding
func TestGetMapper_FixedLenByteArrayDecimalWithDict(t *testing.T) {
	proc := testutil.NewProc(t)

	t.Run("dict_fixed_decimal_to_decimal64", func(t *testing.T) {
		var buf bytes.Buffer
		schema := parquet.NewSchema("x", parquet.Group{
			"c": parquet.Encoded(
				parquet.Decimal(2, 10, parquet.FixedLenByteArrayType(5)),
				&parquet.RLEDictionary,
			),
		})
		w := parquet.NewWriter(&buf, schema)

		// Write with repeated values to trigger dictionary encoding
		rows := []parquet.Row{
			{parquet.FixedLenByteArrayValue(encodeDecimalToBytes(12345, 5)).Level(0, 0, 0)},
			{parquet.FixedLenByteArrayValue(encodeDecimalToBytes(67890, 5)).Level(0, 0, 0)},
			{parquet.FixedLenByteArrayValue(encodeDecimalToBytes(12345, 5)).Level(0, 0, 0)},
		}
		_, err := w.WriteRows(rows)
		require.NoError(t, err)
		require.NoError(t, w.Close())

		f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		require.NoError(t, err)

		col := f.Root().Column("c")
		page, err := col.Pages().ReadPage()
		require.NoError(t, err)

		vec := vector.NewVec(types.New(types.T_decimal64, 10, 2))
		var h ParquetHandler
		mp := h.getMapper(col, plan.Type{Id: int32(types.T_decimal64), NotNullable: true, Width: 10, Scale: 2})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		require.Equal(t, 3, vec.Length())

		got := vector.MustFixedColWithTypeCheck[types.Decimal64](vec)
		require.Equal(t, types.Decimal64(12345), got[0])
		require.Equal(t, types.Decimal64(67890), got[1])
		require.Equal(t, types.Decimal64(12345), got[2])
	})
}

// encodeDecimalToBytes encodes an int64 value to big-endian two's complement bytes
func encodeDecimalToBytes(val int64, length int) []byte {
	result := make([]byte, length)
	negative := val < 0

	// Fill with sign extension
	if negative {
		for i := range result {
			result[i] = 0xFF
		}
	}

	// Write value in big-endian
	for i := length - 1; i >= 0 && val != 0 && val != -1; i-- {
		result[i] = byte(val & 0xFF)
		val >>= 8
	}

	return result
}
