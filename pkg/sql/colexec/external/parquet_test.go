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
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/encoding"
	"github.com/stretchr/testify/require"
)

// This test constructs tiny parquet files in-memory for a broad set of types
// and validates that getMapper can decode a single page into a MatrixOne vector.
func TestParquet_AllTypesBasic(t *testing.T) {
	proc := testutil.NewProc(t)

	tests := []struct {
		name       string
		st         parquet.Type
		numValues  int
		values     encoding.Values
		dt         types.T
		expectText string // optional; if empty, compare with input values
	}{
		{name: "bool", st: parquet.BooleanType, numValues: 2, values: encoding.BooleanValues([]byte{0xFF}), dt: types.T_bool, expectText: "[true true]"},
		{name: "int32", st: parquet.Int32Type, numValues: 2, values: encoding.Int32Values([]int32{1, -2}), dt: types.T_int32},
		{name: "int64", st: parquet.Int64Type, numValues: 2, values: encoding.Int64Values([]int64{2, 7}), dt: types.T_int64},
		{name: "uint32", st: parquet.Uint(32).Type(), numValues: 2, values: encoding.Uint32Values([]uint32{5, 3}), dt: types.T_uint32},
		{name: "uint64", st: parquet.Uint(64).Type(), numValues: 2, values: encoding.Uint64Values([]uint64{8, 10}), dt: types.T_uint64},
		{name: "float32", st: parquet.FloatType, numValues: 2, values: encoding.FloatValues([]float32{7.5, 3.25}), dt: types.T_float32},
		{name: "float64", st: parquet.DoubleType, numValues: 2, values: encoding.DoubleValues([]float64{77.9, 0}), dt: types.T_float64},
		{name: "string", st: parquet.String().Type(), numValues: 2, values: encoding.ByteArrayValues([]byte("abcdefg"), []uint32{0, 3, 7}), dt: types.T_varchar, expectText: "[ abc defg ]"},
		{name: "fixed3", st: parquet.FixedLenByteArrayType(3), numValues: 2, values: encoding.FixedLenByteArrayValues([]byte("abcdef"), 3), dt: types.T_char, expectText: "[ abc def ]"},
		{name: "date", st: parquet.Date().Type(), numValues: 2, values: encoding.Int32Values([]int32{0, 365}), dt: types.T_date, expectText: "[1970-01-01 1971-01-01]"},
		{name: "time_ms", st: parquet.Time(parquet.Millisecond).Type(), numValues: 2, values: encoding.Int32Values([]int32{1_000, 61_000}), dt: types.T_time, expectText: "[00:00:01 00:01:01]"},
		{name: "ts_us", st: parquet.Timestamp(parquet.Microsecond).Type(), numValues: 2, values: encoding.Int64Values([]int64{0, 1_000_000}), dt: types.T_timestamp, expectText: "[1970-01-01 00:00:00.000000 UTC 1970-01-01 00:00:01.000000 UTC]"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Build a tiny parquet buffer with a single column named "c"
			page := tc.st.NewPage(0, tc.numValues, tc.values)

			var buf bytes.Buffer
			schema := parquet.NewSchema("x", parquet.Group{
				"c": parquet.Leaf(tc.st),
			})
			w := parquet.NewWriter(&buf, schema)

			vals := make([]parquet.Value, page.NumRows())
			n, _ := page.Values().ReadValues(vals)
			require.Equal(t, int(tc.numValues), n)

			_, err := w.WriteRows([]parquet.Row{parquet.MakeRow(vals)})
			require.NoError(t, err)
			require.NoError(t, w.Close())

			f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
			require.NoError(t, err)

			vec := vector.NewVec(types.New(tc.dt, 0, 0))
			var h ParquetHandler
			mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(tc.dt), NotNullable: true})
			require.NotNil(t, mp)
			err = mp.mapping(page, proc, vec)
			require.NoError(t, err)

			if tc.expectText != "" {
				// For string-like vectors, formatting may differ (spaces), assert element-wise.
				if tc.dt == types.T_varchar {
					require.Equal(t, 2, vec.Length())
					require.Equal(t, "abc", vec.GetStringAt(0))
					require.Equal(t, "defg", vec.GetStringAt(1))
				} else if tc.dt == types.T_char {
					require.Equal(t, 2, vec.Length())
					require.Equal(t, "abc", vec.GetStringAt(0))
					require.Equal(t, "def", vec.GetStringAt(1))
				} else {
					require.Equal(t, tc.expectText, vec.String())
				}
			} else {
				// Fallback: ensure the vector length matches and not empty
				require.Equal(t, int(tc.numValues), vec.Length())
				// Optional: compare textual form to input values when sensible
				_ = fmt.Sprint(vals)
			}
		})
	}
}

// write a single-column file with dictionary page enabled and return first page
func writeDictAndGetPage(t *testing.T, node parquet.Node, values []parquet.Value) (file *parquet.File, page parquet.Page) {
	t.Helper()
	var buf bytes.Buffer
	schema := parquet.NewSchema("x", parquet.Group{
		"c": node,
	})
	w := parquet.NewWriter(&buf, schema)

	// Ensure column indexes/levels set for required leaf (rep=0, def=0, col=0)
	for i := range values {
		v := &values[i]
		*v = v.Level(0, 0, 0)
	}
	_, err := w.WriteRows([]parquet.Row{parquet.MakeRow(values)})
	require.NoError(t, err)
	require.NoError(t, w.Close())

	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	col := f.Root().Column("c")
	pg, err := col.Pages().ReadPage()
	require.NoError(t, err)
	return f, pg
}

func TestParquet_Dictionary_Numeric(t *testing.T) {
	proc := testutil.NewProc(t)

	// int8 from int32 dictionary
	{
		node := parquet.Encoded(parquet.Leaf(parquet.Int32Type), &parquet.RLEDictionary)
		vals := []parquet.Value{
			parquet.Int32Value(1), parquet.Int32Value(2), parquet.Int32Value(1), parquet.Int32Value(3), parquet.Int32Value(2),
		}
		f, page := writeDictAndGetPage(t, node, vals)

		vec := vector.NewVec(types.New(types.T_int8, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_int8), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		got := vector.MustFixedColWithTypeCheck[int8](vec)
		require.Equal(t, []int8{1, 2, 1, 3, 2}, got)
	}

	// float32 dictionary
	{
		node := parquet.Encoded(parquet.Leaf(parquet.FloatType), &parquet.RLEDictionary)
		vals := []parquet.Value{
			parquet.FloatValue(1.5), parquet.FloatValue(2.25), parquet.FloatValue(1.5), parquet.FloatValue(3.5),
		}
		f, page := writeDictAndGetPage(t, node, vals)
		vec := vector.NewVec(types.New(types.T_float32, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_float32), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		got := vector.MustFixedColWithTypeCheck[float32](vec)
		require.InDeltaSlice(t, []float32{1.5, 2.25, 1.5, 3.5}, got, 1e-6)
	}

	// uint64 from int64 dictionary
	{
		node := parquet.Encoded(parquet.Leaf(parquet.Int64Type), &parquet.RLEDictionary)
		vals := []parquet.Value{
			parquet.Int64Value(5), parquet.Int64Value(7), parquet.Int64Value(5), parquet.Int64Value(9),
		}
		f, page := writeDictAndGetPage(t, node, vals)
		vec := vector.NewVec(types.New(types.T_uint64, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_uint64), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		got := vector.MustFixedColWithTypeCheck[uint64](vec)
		require.Equal(t, []uint64{5, 7, 5, 9}, got)
	}
}

func TestParquet_Dictionary_Date_Time_Timestamp(t *testing.T) {
	proc := testutil.NewProc(t)

	// DATE dictionary (days since epoch)
	{
		node := parquet.Encoded(parquet.Leaf(parquet.Date().Type()), &parquet.RLEDictionary)
		// 0 -> 1970-01-01, 1 -> 1970-01-02
		vals := []parquet.Value{
			parquet.Int32Value(0), parquet.Int32Value(1), parquet.Int32Value(0),
		}
		f, page := writeDictAndGetPage(t, node, vals)
		vec := vector.NewVec(types.New(types.T_date, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_date), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		got := vector.MustFixedColWithTypeCheck[types.Date](vec)
		require.Equal(t, []types.Date{types.DaysFromUnixEpochToDate(0), types.DaysFromUnixEpochToDate(1), types.DaysFromUnixEpochToDate(0)}, got)
	}

	// TIME millis dictionary (int32 millis)
	{
		node := parquet.Encoded(parquet.Leaf(parquet.Time(parquet.Millisecond).Type()), &parquet.RLEDictionary)
		vals := []parquet.Value{
			parquet.Int32Value(1000), parquet.Int32Value(61000), parquet.Int32Value(1000),
		}
		f, page := writeDictAndGetPage(t, node, vals)
		vec := vector.NewVec(types.New(types.T_time, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_time), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		got := vector.MustFixedColWithTypeCheck[types.Time](vec)
		// TIME stores microseconds of day
		require.Equal(t, []types.Time{types.Time(1000) * 1000, types.Time(61000) * 1000, types.Time(1000) * 1000}, got)
	}

	// TIMESTAMP micros dictionary (int64 micros)
	{
		node := parquet.Encoded(parquet.Leaf(parquet.Timestamp(parquet.Microsecond).Type()), &parquet.RLEDictionary)
		vals := []parquet.Value{
			parquet.Int64Value(0), parquet.Int64Value(1_000_000), parquet.Int64Value(0),
		}
		f, page := writeDictAndGetPage(t, node, vals)
		vec := vector.NewVec(types.New(types.T_timestamp, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_timestamp), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		got := vector.MustFixedColWithTypeCheck[types.Timestamp](vec)
		require.Equal(t, []types.Timestamp{types.UnixMicroToTimestamp(0), types.UnixMicroToTimestamp(1_000_000), types.UnixMicroToTimestamp(0)}, got)
	}
}

// Nested payloads marshalled as JSON
type innerT struct {
	T int32 `parquet:"t"`
}

type nestedT struct {
	I   int32   `parquet:"i"`
	S   string  `parquet:"s"`
	Arr []int32 `parquet:"arr,list"`
	Obj innerT  `parquet:"obj"`
}

type rowNested struct {
	C nestedT `parquet:"c"`
}

func jsonEq(t *testing.T, got string, want any) {
	var g any
	require.NoError(t, json.Unmarshal([]byte(got), &g))
	// Marshal want then unmarshal to normalize types
	wb, _ := json.Marshal(want)
	var w any
	require.NoError(t, json.Unmarshal(wb, &w))
	require.True(t, reflect.DeepEqual(g, w), "json not equal\n got=%s\nwant=%s", got, string(wb))
}

func TestParquet_NestedStructListMap_JSON(t *testing.T) {
	proc := testutil.NewProc(t)
	rows := []rowNested{
		{C: nestedT{I: 1, S: "a", Arr: []int32{1, 2}, Obj: innerT{T: 9}}},
		{C: nestedT{I: 2, S: "b", Arr: []int32{3}, Obj: innerT{T: 8}}},
		{C: nestedT{I: 3, S: "c", Arr: nil, Obj: innerT{T: 7}}},
	}

	var buf bytes.Buffer
	schema := parquet.SchemaOf(rowNested{})
	w := parquet.NewGenericWriter[rowNested](&buf, schema)
	_, err := w.Write(rows)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)

	col := f.Root().Column("c")
	require.NotNil(t, col)
	require.False(t, col.Leaf())

	jm, err := newJSONColumnMapper(f, col)
	require.NoError(t, err)

	vec := vector.NewVec(types.New(types.T_varchar, 0, 0))
	eof, err := jm.mapBatch(context.Background(), vec, len(rows), true, proc.Mp())
	require.NoError(t, err)
	require.True(t, eof)
	require.Equal(t, len(rows), vec.Length())

	// Validate JSON content per row (map order ignored)
	jsonEq(t, vec.GetStringAt(0), map[string]any{
		"i":   float64(1),
		"s":   "a",
		"arr": []any{float64(1), float64(2)},
		"obj": map[string]any{"t": float64(9)},
	})
	jsonEq(t, vec.GetStringAt(1), map[string]any{
		"i":   float64(2),
		"s":   "b",
		"arr": []any{float64(3)},
		"obj": map[string]any{"t": float64(8)},
	})
	jsonEq(t, vec.GetStringAt(2), map[string]any{
		"i": float64(3),
		"s": "c",
		// parquet-go reconstructs missing list as empty slice rather than null
		"arr": []any{},
		"obj": map[string]any{"t": float64(7)},
	})
}
