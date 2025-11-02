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
	"github.com/parquet-go/parquet-go/encoding"
	"github.com/stretchr/testify/require"
)

// TestParquet_WideningConversion_Int32ToInt64 tests INT32 → INT64 widening conversion
func TestParquet_WideningConversion_Int32ToInt64(t *testing.T) {
	proc := testutil.NewProc(t)

	// Test 1: Plain encoding INT32 → INT64
	{
		st := parquet.Int32Type
		// Test with various int32 values including extremes
		values := []int32{1, -1, 100, -100, math.MaxInt32, math.MinInt32, 0}
		page := st.NewPage(0, len(values), encoding.Int32Values(values))

		var buf bytes.Buffer
		schema := parquet.NewSchema("x", parquet.Group{"c": parquet.Leaf(st)})
		w := parquet.NewWriter(&buf, schema)
		vals := make([]parquet.Value, page.NumRows())
		_, _ = page.Values().ReadValues(vals)
		_, err := w.WriteRows([]parquet.Row{parquet.MakeRow(vals)})
		require.NoError(t, err)
		require.NoError(t, w.Close())

		f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		require.NoError(t, err)

		vec := vector.NewVec(types.New(types.T_int64, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_int64), NotNullable: true})
		require.NotNil(t, mp, "INT32 → INT64 widening conversion should be supported")
		err = mp.mapping(page, proc, vec)
		require.NoError(t, err)

		got := vector.MustFixedColWithTypeCheck[int64](vec)
		require.Equal(t, len(values), len(got))
		for i, v := range values {
			require.Equal(t, int64(v), got[i], "Value at index %d should be correctly widened", i)
		}
	}

	// Test 2: Dictionary encoding INT32 → INT64
	{
		node := parquet.Encoded(parquet.Leaf(parquet.Int32Type), &parquet.RLEDictionary)
		vals := []parquet.Value{
			parquet.Int32Value(100),
			parquet.Int32Value(-200),
			parquet.Int32Value(100),
			parquet.Int32Value(300),
			parquet.Int32Value(-200),
		}
		f, page := writeDictAndGetPage(t, node, vals)

		vec := vector.NewVec(types.New(types.T_int64, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_int64), NotNullable: true})
		require.NotNil(t, mp, "INT32 → INT64 with dictionary encoding should be supported")
		require.NoError(t, mp.mapping(page, proc, vec))

		got := vector.MustFixedColWithTypeCheck[int64](vec)
		require.Equal(t, []int64{100, -200, 100, 300, -200}, got)
	}

	// Test 3: Direct mapping INT64 → INT64 still works (backward compatibility)
	{
		st := parquet.Int64Type
		values := []int64{1000, -2000, 3000}
		page := st.NewPage(0, len(values), encoding.Int64Values(values))

		var buf bytes.Buffer
		schema := parquet.NewSchema("x", parquet.Group{"c": parquet.Leaf(st)})
		w := parquet.NewWriter(&buf, schema)
		vals := make([]parquet.Value, page.NumRows())
		_, _ = page.Values().ReadValues(vals)
		_, err := w.WriteRows([]parquet.Row{parquet.MakeRow(vals)})
		require.NoError(t, err)
		require.NoError(t, w.Close())

		f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		require.NoError(t, err)

		vec := vector.NewVec(types.New(types.T_int64, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_int64), NotNullable: true})
		require.NotNil(t, mp, "INT64 → INT64 direct mapping should still work")
		err = mp.mapping(page, proc, vec)
		require.NoError(t, err)

		got := vector.MustFixedColWithTypeCheck[int64](vec)
		require.Equal(t, values, got)
	}
}

// TestParquet_WideningConversion_UInt32ToUInt64 tests UINT32 → UINT64 widening conversion
func TestParquet_WideningConversion_UInt32ToUInt64(t *testing.T) {
	proc := testutil.NewProc(t)

	// Test 1: Plain encoding UINT32 → UINT64
	{
		st := parquet.Int32Type
		// Test with various uint32 values including max
		values := []uint32{0, 1, 100, 1000, math.MaxUint32, math.MaxUint32 - 1}
		page := st.NewPage(0, len(values), encoding.Uint32Values(values))

		var buf bytes.Buffer
		schema := parquet.NewSchema("x", parquet.Group{"c": parquet.Leaf(st)})
		w := parquet.NewWriter(&buf, schema)
		vals := make([]parquet.Value, page.NumRows())
		_, _ = page.Values().ReadValues(vals)
		_, err := w.WriteRows([]parquet.Row{parquet.MakeRow(vals)})
		require.NoError(t, err)
		require.NoError(t, w.Close())

		f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		require.NoError(t, err)

		vec := vector.NewVec(types.New(types.T_uint64, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_uint64), NotNullable: true})
		require.NotNil(t, mp, "UINT32 → UINT64 widening conversion should be supported")
		err = mp.mapping(page, proc, vec)
		require.NoError(t, err)

		got := vector.MustFixedColWithTypeCheck[uint64](vec)
		require.Equal(t, len(values), len(got))
		for i, v := range values {
			require.Equal(t, uint64(v), got[i], "Value at index %d should be correctly widened", i)
		}
	}

	// Test 2: Dictionary encoding UINT32 → UINT64
	{
		node := parquet.Encoded(parquet.Leaf(parquet.Int32Type), &parquet.RLEDictionary)
		vals := []parquet.Value{
			parquet.Int32Value(100),
			parquet.Int32Value(200),
			parquet.Int32Value(100),
			parquet.Int32Value(-1), // Will be interpreted as MaxUint32 (4294967295) when read as uint32
			parquet.Int32Value(200),
		}
		f, page := writeDictAndGetPage(t, node, vals)

		vec := vector.NewVec(types.New(types.T_uint64, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_uint64), NotNullable: true})
		require.NotNil(t, mp, "UINT32 → UINT64 with dictionary encoding should be supported")
		require.NoError(t, mp.mapping(page, proc, vec))

		got := vector.MustFixedColWithTypeCheck[uint64](vec)
		require.Equal(t, []uint64{100, 200, 100, 4294967295, 200}, got)
	}

	// Test 3: Direct mapping UINT64 → UINT64 still works (backward compatibility)
	{
		st := parquet.Int64Type
		values := []uint64{0, 1000, 10000, math.MaxUint32 + 1, math.MaxUint64 - 1}
		page := st.NewPage(0, len(values), encoding.Uint64Values(values))

		var buf bytes.Buffer
		schema := parquet.NewSchema("x", parquet.Group{"c": parquet.Leaf(st)})
		w := parquet.NewWriter(&buf, schema)
		vals := make([]parquet.Value, page.NumRows())
		_, _ = page.Values().ReadValues(vals)
		_, err := w.WriteRows([]parquet.Row{parquet.MakeRow(vals)})
		require.NoError(t, err)
		require.NoError(t, w.Close())

		f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		require.NoError(t, err)

		vec := vector.NewVec(types.New(types.T_uint64, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_uint64), NotNullable: true})
		require.NotNil(t, mp, "UINT64 → UINT64 direct mapping should still work")
		err = mp.mapping(page, proc, vec)
		require.NoError(t, err)

		got := vector.MustFixedColWithTypeCheck[uint64](vec)
		require.Equal(t, values, got)
	}
}

// TestParquet_WideningConversion_WithNulls tests widening conversion with NULL values
func TestParquet_WideningConversion_WithNulls(t *testing.T) {
	proc := testutil.NewProc(t)

	// INT32 → INT64 with NULL values
	{
		var buf bytes.Buffer
		schema := parquet.NewSchema("x", parquet.Group{
			"c": parquet.Optional(parquet.Leaf(parquet.Int32Type)),
		})
		w := parquet.NewWriter(&buf, schema)
		rows := []parquet.Row{
			{parquet.Int32Value(42).Level(0, 1, 0)},  // present
			{parquet.NullValue().Level(0, 0, 0)},     // null
			{parquet.Int32Value(-99).Level(0, 1, 0)}, // present
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
		require.NotNil(t, mp, "INT32 → INT64 with NULL values should be supported")
		require.NoError(t, mp.mapping(page, proc, vec))

		require.Equal(t, 3, vec.Length())
		require.True(t, vec.GetNulls().Contains(1), "NULL should be at index 1")
		got := vector.MustFixedColWithTypeCheck[int64](vec)
		require.Equal(t, int64(42), got[0])
		require.Equal(t, int64(-99), got[2])
	}
}

// TestParquet_WideningConversion_ExtremeValues tests conversion of extreme values
func TestParquet_WideningConversion_ExtremeValues(t *testing.T) {
	proc := testutil.NewProc(t)

	// INT32 extreme values → INT64
	{
		st := parquet.Int32Type
		values := []int32{
			math.MaxInt32,
			math.MinInt32,
			math.MaxInt32 - 1,
			math.MinInt32 + 1,
			0,
		}
		page := st.NewPage(0, len(values), encoding.Int32Values(values))

		var buf bytes.Buffer
		schema := parquet.NewSchema("x", parquet.Group{"c": parquet.Leaf(st)})
		w := parquet.NewWriter(&buf, schema)
		vals := make([]parquet.Value, page.NumRows())
		_, _ = page.Values().ReadValues(vals)
		_, err := w.WriteRows([]parquet.Row{parquet.MakeRow(vals)})
		require.NoError(t, err)
		require.NoError(t, w.Close())

		f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		require.NoError(t, err)

		vec := vector.NewVec(types.New(types.T_int64, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_int64), NotNullable: true})
		require.NotNil(t, mp)
		err = mp.mapping(page, proc, vec)
		require.NoError(t, err)

		got := vector.MustFixedColWithTypeCheck[int64](vec)
		// Verify extreme values are preserved correctly
		require.Equal(t, int64(math.MaxInt32), got[0])
		require.Equal(t, int64(math.MinInt32), got[1])
		require.Equal(t, int64(math.MaxInt32-1), got[2])
		require.Equal(t, int64(math.MinInt32+1), got[3])
		require.Equal(t, int64(0), got[4])
	}

	// UINT32 extreme values → UINT64
	{
		st := parquet.Int32Type
		values := []uint32{
			math.MaxUint32,
			math.MaxUint32 - 1,
			0,
			1,
			math.MaxUint32 / 2,
		}
		page := st.NewPage(0, len(values), encoding.Uint32Values(values))

		var buf bytes.Buffer
		schema := parquet.NewSchema("x", parquet.Group{"c": parquet.Leaf(st)})
		w := parquet.NewWriter(&buf, schema)
		vals := make([]parquet.Value, page.NumRows())
		_, _ = page.Values().ReadValues(vals)
		_, err := w.WriteRows([]parquet.Row{parquet.MakeRow(vals)})
		require.NoError(t, err)
		require.NoError(t, w.Close())

		f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		require.NoError(t, err)

		vec := vector.NewVec(types.New(types.T_uint64, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_uint64), NotNullable: true})
		require.NotNil(t, mp)
		err = mp.mapping(page, proc, vec)
		require.NoError(t, err)

		got := vector.MustFixedColWithTypeCheck[uint64](vec)
		// Verify extreme values are preserved correctly
		require.Equal(t, uint64(math.MaxUint32), got[0])
		require.Equal(t, uint64(math.MaxUint32-1), got[1])
		require.Equal(t, uint64(0), got[2])
		require.Equal(t, uint64(1), got[3])
		require.Equal(t, uint64(math.MaxUint32/2), got[4])
	}
}

// TestParquet_WideningConversion_BackwardCompatibility tests backward compatibility
func TestParquet_WideningConversion_BackwardCompatibility(t *testing.T) {
	proc := testutil.NewProc(t)

	// INT32 → INT32 still works
	{
		st := parquet.Int32Type
		values := []int32{1, 2, 3}
		page := st.NewPage(0, len(values), encoding.Int32Values(values))

		var buf bytes.Buffer
		schema := parquet.NewSchema("x", parquet.Group{"c": parquet.Leaf(st)})
		w := parquet.NewWriter(&buf, schema)
		vals := make([]parquet.Value, page.NumRows())
		_, _ = page.Values().ReadValues(vals)
		_, err := w.WriteRows([]parquet.Row{parquet.MakeRow(vals)})
		require.NoError(t, err)
		require.NoError(t, w.Close())

		f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		require.NoError(t, err)

		vec := vector.NewVec(types.New(types.T_int32, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_int32), NotNullable: true})
		require.NotNil(t, mp, "INT32 → INT32 direct mapping should still work")
		err = mp.mapping(page, proc, vec)
		require.NoError(t, err)

		got := vector.MustFixedColWithTypeCheck[int32](vec)
		require.Equal(t, values, got)
	}

	// UINT32 → UINT32 still works
	{
		st := parquet.Int32Type
		values := []uint32{1, 2, 3}
		page := st.NewPage(0, len(values), encoding.Uint32Values(values))

		var buf bytes.Buffer
		schema := parquet.NewSchema("x", parquet.Group{"c": parquet.Leaf(st)})
		w := parquet.NewWriter(&buf, schema)
		vals := make([]parquet.Value, page.NumRows())
		_, _ = page.Values().ReadValues(vals)
		_, err := w.WriteRows([]parquet.Row{parquet.MakeRow(vals)})
		require.NoError(t, err)
		require.NoError(t, w.Close())

		f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		require.NoError(t, err)

		vec := vector.NewVec(types.New(types.T_uint32, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_uint32), NotNullable: true})
		require.NotNil(t, mp, "UINT32 → UINT32 direct mapping should still work")
		err = mp.mapping(page, proc, vec)
		require.NoError(t, err)

		got := vector.MustFixedColWithTypeCheck[uint32](vec)
		require.Equal(t, values, got)
	}
}

// TestParquet_WideningConversion_Float32ToFloat64 tests FLOAT32 → FLOAT64 widening conversion
func TestParquet_WideningConversion_Float32ToFloat64(t *testing.T) {
	proc := testutil.NewProc(t)

	// Test 1: Plain encoding FLOAT32 → FLOAT64
	{
		st := parquet.FloatType
		values := []float32{1.5, -2.75, 3.14159, 0.0, 123.456}
		page := st.NewPage(0, len(values), encoding.FloatValues(values))

		var buf bytes.Buffer
		schema := parquet.NewSchema("x", parquet.Group{"c": parquet.Leaf(st)})
		w := parquet.NewWriter(&buf, schema)
		vals := make([]parquet.Value, page.NumRows())
		_, _ = page.Values().ReadValues(vals)
		_, err := w.WriteRows([]parquet.Row{parquet.MakeRow(vals)})
		require.NoError(t, err)
		require.NoError(t, w.Close())

		f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		require.NoError(t, err)
		col := f.Root().Column("c")
		page2, err := col.Pages().ReadPage()
		require.NoError(t, err)

		vec := vector.NewVec(types.New(types.T_float64, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(col, plan.Type{Id: int32(types.T_float64), NotNullable: true})
		require.NotNil(t, mp, "FLOAT32 → FLOAT64 should be supported")
		require.NoError(t, mp.mapping(page2, proc, vec))

		got := vector.MustFixedColWithTypeCheck[float64](vec)
		require.Equal(t, len(values), len(got))
		for i, v := range values {
			require.InDelta(t, float64(v), got[i], 0.0001, "Value at index %d should be correctly widened", i)
		}
	}

	// Test 2: Dictionary encoding FLOAT32 → FLOAT64
	{
		node := parquet.Encoded(parquet.Leaf(parquet.FloatType), &parquet.RLEDictionary)
		vals := []parquet.Value{
			parquet.FloatValue(1.5),
			parquet.FloatValue(2.75),
			parquet.FloatValue(1.5), // Repeat
			parquet.FloatValue(3.14),
			parquet.FloatValue(2.75), // Repeat
		}
		f, page := writeDictAndGetPage(t, node, vals)

		vec := vector.NewVec(types.New(types.T_float64, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_float64), NotNullable: true})
		require.NotNil(t, mp, "FLOAT32 → FLOAT64 with dictionary encoding should be supported")
		require.NoError(t, mp.mapping(page, proc, vec))

		got := vector.MustFixedColWithTypeCheck[float64](vec)
		expected := []float64{1.5, 2.75, 1.5, 3.14, 2.75}
		require.Equal(t, len(expected), len(got))
		for i := range expected {
			require.InDelta(t, expected[i], got[i], 0.0001)
		}
	}

	// Test 3: Extreme float values
	{
		st := parquet.FloatType
		values := []float32{
			1.401298464324817e-45,   // Smallest positive float32
			3.4028234663852886e+38,  // Largest positive float32
			-3.4028234663852886e+38, // Largest negative float32
		}
		page := st.NewPage(0, len(values), encoding.FloatValues(values))

		var buf bytes.Buffer
		schema := parquet.NewSchema("x", parquet.Group{"c": parquet.Leaf(st)})
		w := parquet.NewWriter(&buf, schema)
		vals := make([]parquet.Value, page.NumRows())
		_, _ = page.Values().ReadValues(vals)
		_, err := w.WriteRows([]parquet.Row{parquet.MakeRow(vals)})
		require.NoError(t, err)
		require.NoError(t, w.Close())

		f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		require.NoError(t, err)
		col := f.Root().Column("c")
		page2, err := col.Pages().ReadPage()
		require.NoError(t, err)

		vec := vector.NewVec(types.New(types.T_float64, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(col, plan.Type{Id: int32(types.T_float64), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page2, proc, vec))

		got := vector.MustFixedColWithTypeCheck[float64](vec)
		require.Equal(t, len(values), len(got))
		for i, v := range values {
			require.InDelta(t, float64(v), got[i], 1e-30)
		}
	}
}
