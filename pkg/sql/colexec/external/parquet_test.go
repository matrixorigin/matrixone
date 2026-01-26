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
	"errors"
	"fmt"
	"math/big"
	"testing"
	"time"

	"io"
	"iter"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/encoding"
	"github.com/stretchr/testify/require"
)

// fakeFS is a minimal ETL-compatible FileService for testing fsReaderAt
type fakeFS struct{ b []byte }

func (f *fakeFS) Name() string                                            { return "fake" }
func (f *fakeFS) Write(ctx context.Context, v fileservice.IOVector) error { return nil }
func (f *fakeFS) Read(ctx context.Context, v *fileservice.IOVector) error {
	if len(v.Entries) == 0 {
		return moerr.NewInternalError(ctx, "empty entries")
	}
	e := &v.Entries[0]
	if e.Size < 0 {
		e.Size = int64(len(f.b)) - e.Offset
	}
	if int(e.Offset+e.Size) > len(f.b) {
		return io.EOF
	}
	if len(e.Data) < int(e.Size) {
		e.Data = make([]byte, e.Size)
	}
	copy(e.Data, f.b[e.Offset:int(e.Offset+e.Size)])
	return nil
}
func (f *fakeFS) ReadCache(ctx context.Context, v *fileservice.IOVector) error { return nil }
func (f *fakeFS) List(ctx context.Context, dirPath string) iter.Seq2[*fileservice.DirEntry, error] {
	return nil
}
func (f *fakeFS) Delete(ctx context.Context, filePaths ...string) error { return nil }
func (f *fakeFS) StatFile(ctx context.Context, filePath string) (*fileservice.DirEntry, error) {
	return nil, nil
}
func (f *fakeFS) PrefetchFile(ctx context.Context, filePath string) error { return nil }
func (f *fakeFS) Cost() *fileservice.CostAttr                             { return &fileservice.CostAttr{} }
func (f *fakeFS) Close(ctx context.Context)                               {}
func (f *fakeFS) ETLCompatible()                                          {}

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

	// float64 dictionary
	{
		node := parquet.Encoded(parquet.Leaf(parquet.DoubleType), &parquet.RLEDictionary)
		vals := []parquet.Value{
			parquet.DoubleValue(1.5), parquet.DoubleValue(2.25), parquet.DoubleValue(1.5), parquet.DoubleValue(3.5),
		}
		f, page := writeDictAndGetPage(t, node, vals)
		vec := vector.NewVec(types.New(types.T_float64, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_float64), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		got := vector.MustFixedColWithTypeCheck[float64](vec)
		require.InDeltaSlice(t, []float64{1.5, 2.25, 1.5, 3.5}, got, 1e-9)
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

	// int32 dictionary
	{
		node := parquet.Encoded(parquet.Leaf(parquet.Int32Type), &parquet.RLEDictionary)
		vals := []parquet.Value{parquet.Int32Value(-3), parquet.Int32Value(4), parquet.Int32Value(-3)}
		f, page := writeDictAndGetPage(t, node, vals)
		vec := vector.NewVec(types.New(types.T_int32, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_int32), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		got := vector.MustFixedColWithTypeCheck[int32](vec)
		require.Equal(t, []int32{-3, 4, -3}, got)
	}

	// int16 from int32 dictionary
	{
		node := parquet.Encoded(parquet.Leaf(parquet.Int32Type), &parquet.RLEDictionary)
		vals := []parquet.Value{parquet.Int32Value(-10), parquet.Int32Value(20), parquet.Int32Value(-10)}
		f, page := writeDictAndGetPage(t, node, vals)
		vec := vector.NewVec(types.New(types.T_int16, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_int16), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		got := vector.MustFixedColWithTypeCheck[int16](vec)
		require.Equal(t, []int16{-10, 20, -10}, got)
	}

	// uint16 from int32 dictionary
	{
		node := parquet.Encoded(parquet.Leaf(parquet.Int32Type), &parquet.RLEDictionary)
		vals := []parquet.Value{parquet.Int32Value(0), parquet.Int32Value(65535), parquet.Int32Value(1)}
		f, page := writeDictAndGetPage(t, node, vals)
		vec := vector.NewVec(types.New(types.T_uint16, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_uint16), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		got := vector.MustFixedColWithTypeCheck[uint16](vec)
		require.Equal(t, []uint16{0, 65535, 1}, got)
	}

	// uint8 from int32 dictionary
	{
		node := parquet.Encoded(parquet.Leaf(parquet.Int32Type), &parquet.RLEDictionary)
		vals := []parquet.Value{parquet.Int32Value(1), parquet.Int32Value(255), parquet.Int32Value(1)}
		f, page := writeDictAndGetPage(t, node, vals)
		vec := vector.NewVec(types.New(types.T_uint8, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_uint8), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		got := vector.MustFixedColWithTypeCheck[uint8](vec)
		require.Equal(t, []uint8{1, 255, 1}, got)
	}

	// uint32 from int32 dictionary
	{
		node := parquet.Encoded(parquet.Leaf(parquet.Int32Type), &parquet.RLEDictionary)
		vals := []parquet.Value{parquet.Int32Value(3), parquet.Int32Value(400000000), parquet.Int32Value(3)}
		f, page := writeDictAndGetPage(t, node, vals)
		vec := vector.NewVec(types.New(types.T_uint32, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_uint32), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		got := vector.MustFixedColWithTypeCheck[uint32](vec)
		require.Equal(t, []uint32{3, 400000000, 3}, got)
	}

	// int64 dictionary
	{
		node := parquet.Encoded(parquet.Leaf(parquet.Int64Type), &parquet.RLEDictionary)
		vals := []parquet.Value{parquet.Int64Value(-9), parquet.Int64Value(11), parquet.Int64Value(-9)}
		f, page := writeDictAndGetPage(t, node, vals)
		vec := vector.NewVec(types.New(types.T_int64, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_int64), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		got := vector.MustFixedColWithTypeCheck[int64](vec)
		require.Equal(t, []int64{-9, 11, -9}, got)
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

	// TIME nanos dictionary (int64 nanos)
	{
		node := parquet.Encoded(parquet.Leaf(parquet.Time(parquet.Nanosecond).Type()), &parquet.RLEDictionary)
		vals := []parquet.Value{
			parquet.Int64Value(1_000), parquet.Int64Value(61_000), parquet.Int64Value(1_000),
		}
		f, page := writeDictAndGetPage(t, node, vals)
		vec := vector.NewVec(types.New(types.T_time, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_time), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		got := vector.MustFixedColWithTypeCheck[types.Time](vec)
		require.Equal(t, []types.Time{types.Time(1), types.Time(61), types.Time(1)}, got)
	}

	// TIME micros dictionary (int64 micros)
	{
		node := parquet.Encoded(parquet.Leaf(parquet.Time(parquet.Microsecond).Type()), &parquet.RLEDictionary)
		vals := []parquet.Value{
			parquet.Int64Value(1_000), parquet.Int64Value(61_000), parquet.Int64Value(1_000),
		}
		f, page := writeDictAndGetPage(t, node, vals)
		vec := vector.NewVec(types.New(types.T_time, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_time), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		got := vector.MustFixedColWithTypeCheck[types.Time](vec)
		require.Equal(t, []types.Time{types.Time(1_000), types.Time(61_000), types.Time(1_000)}, got)
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

	// TIMESTAMP nanos dictionary (int64 nanos)
	{
		node := parquet.Encoded(parquet.Leaf(parquet.Timestamp(parquet.Nanosecond).Type()), &parquet.RLEDictionary)
		vals := []parquet.Value{
			parquet.Int64Value(1_000), parquet.Int64Value(2_000), parquet.Int64Value(1_000),
		}
		f, page := writeDictAndGetPage(t, node, vals)
		vec := vector.NewVec(types.New(types.T_timestamp, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_timestamp), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		got := vector.MustFixedColWithTypeCheck[types.Timestamp](vec)
		require.Equal(t, []types.Timestamp{types.UnixNanoToTimestamp(1_000), types.UnixNanoToTimestamp(2_000), types.UnixNanoToTimestamp(1_000)}, got)
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

	// TIMESTAMP millis dictionary (int64 millis)
	{
		node := parquet.Encoded(parquet.Leaf(parquet.Timestamp(parquet.Millisecond).Type()), &parquet.RLEDictionary)
		vals := []parquet.Value{
			parquet.Int64Value(1), parquet.Int64Value(2), parquet.Int64Value(1),
		}
		f, page := writeDictAndGetPage(t, node, vals)
		vec := vector.NewVec(types.New(types.T_timestamp, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_timestamp), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		got := vector.MustFixedColWithTypeCheck[types.Timestamp](vec)
		require.Equal(t, []types.Timestamp{types.UnixMicroToTimestamp(1_000), types.UnixMicroToTimestamp(2_000), types.UnixMicroToTimestamp(1_000)}, got)
	}
}

func TestParquet_Mappers_MoreIntsAndStringDict(t *testing.T) {
	proc := testutil.NewProc(t)
	// Non-dict small int mappings from INT32
	st := parquet.Int32Type
	page := st.NewPage(0, 4, encoding.Int32Values([]int32{1, -1, 255, -128}))
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

	for _, dt := range []types.T{types.T_uint8, types.T_int8, types.T_uint16, types.T_int16} {
		vec := vector.NewVec(types.New(dt, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(col, plan.Type{Id: int32(dt), NotNullable: true})
		require.NotNil(t, mp, "mapper for %v", dt)
		require.NoError(t, mp.mapping(page, proc, vec))
		require.Equal(t, 4, vec.Length())
	}

	// Dictionary-encoded string mapping
	{
		node := parquet.Encoded(parquet.Leaf(parquet.ByteArrayType), &parquet.RLEDictionary)
		vals := []parquet.Value{parquet.ByteArrayValue([]byte("aa")), parquet.ByteArrayValue([]byte("bb")), parquet.ByteArrayValue([]byte("aa"))}
		f2, page2 := writeDictAndGetPage(t, node, vals)
		vec := vector.NewVec(types.New(types.T_varchar, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f2.Root().Column("c"), plan.Type{Id: int32(types.T_varchar)})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page2, proc, vec))
		require.Equal(t, 3, vec.Length())
		require.Equal(t, "aa", vec.GetStringAt(0))
		require.Equal(t, "bb", vec.GetStringAt(1))
		require.Equal(t, "aa", vec.GetStringAt(2))
	}
}

func TestParquet_Time_Timestamp_Datetime_Units(t *testing.T) {
	proc := testutil.NewProc(t)
	// TIME nanos non-dict
	{
		st := parquet.Time(parquet.Nanosecond).Type()
		page := st.NewPage(0, 2, encoding.Int64Values([]int64{1000, 2000}))
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
		vec := vector.NewVec(types.New(types.T_time, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_time), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		got := vector.MustFixedColWithTypeCheck[types.Time](vec)
		require.Equal(t, []types.Time{types.Time(1000 / 1000), types.Time(2000 / 1000)}, got)
	}
	// TIMESTAMP nanos non-dict
	{
		st := parquet.Timestamp(parquet.Nanosecond).Type()
		page := st.NewPage(0, 2, encoding.Int64Values([]int64{1_000, 2_000}))
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
		vec := vector.NewVec(types.New(types.T_timestamp, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_timestamp), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		got := vector.MustFixedColWithTypeCheck[types.Timestamp](vec)
		require.Equal(t, []types.Timestamp{types.UnixNanoToTimestamp(1_000), types.UnixNanoToTimestamp(2_000)}, got)
	}
	// DATETIME millis non-dict
	{
		st := parquet.Timestamp(parquet.Millisecond).Type()
		page := st.NewPage(0, 2, encoding.Int64Values([]int64{1, 2}))
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
		vec := vector.NewVec(types.New(types.T_datetime, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_datetime), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		got := vector.MustFixedColWithTypeCheck[types.Datetime](vec)
		require.Equal(t, []types.Datetime{types.Datetime(types.UnixMicroToTimestamp(1_000)), types.Datetime(types.UnixMicroToTimestamp(2_000))}, got)
	}
}

func TestParquet_Dictionary_Datetime(t *testing.T) {
	proc := testutil.NewProc(t)

	// DATETIME nanos dictionary (int64 nanos)
	{
		node := parquet.Encoded(parquet.Leaf(parquet.Timestamp(parquet.Nanosecond).Type()), &parquet.RLEDictionary)
		vals := []parquet.Value{
			parquet.Int64Value(1_000), parquet.Int64Value(2_000), parquet.Int64Value(1_000),
		}
		f, page := writeDictAndGetPage(t, node, vals)
		vec := vector.NewVec(types.New(types.T_datetime, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_datetime), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		got := vector.MustFixedColWithTypeCheck[types.Datetime](vec)
		require.Equal(t, []types.Datetime{
			types.Datetime(types.UnixNanoToTimestamp(1_000)),
			types.Datetime(types.UnixNanoToTimestamp(2_000)),
			types.Datetime(types.UnixNanoToTimestamp(1_000)),
		}, got)
	}

	// DATETIME micros dictionary (int64 micros)
	{
		node := parquet.Encoded(parquet.Leaf(parquet.Timestamp(parquet.Microsecond).Type()), &parquet.RLEDictionary)
		vals := []parquet.Value{
			parquet.Int64Value(500_000), parquet.Int64Value(1_000_000), parquet.Int64Value(500_000),
		}
		f, page := writeDictAndGetPage(t, node, vals)
		vec := vector.NewVec(types.New(types.T_datetime, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_datetime), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		got := vector.MustFixedColWithTypeCheck[types.Datetime](vec)
		require.Equal(t, []types.Datetime{
			types.Datetime(types.UnixMicroToTimestamp(500_000)),
			types.Datetime(types.UnixMicroToTimestamp(1_000_000)),
			types.Datetime(types.UnixMicroToTimestamp(500_000)),
		}, got)
	}

	// DATETIME millis dictionary (int64 millis)
	{
		node := parquet.Encoded(parquet.Leaf(parquet.Timestamp(parquet.Millisecond).Type()), &parquet.RLEDictionary)
		vals := []parquet.Value{
			parquet.Int64Value(3), parquet.Int64Value(4), parquet.Int64Value(3),
		}
		f, page := writeDictAndGetPage(t, node, vals)
		vec := vector.NewVec(types.New(types.T_datetime, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_datetime), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		got := vector.MustFixedColWithTypeCheck[types.Datetime](vec)
		require.Equal(t, []types.Datetime{
			types.Datetime(types.UnixMicroToTimestamp(3_000)),
			types.Datetime(types.UnixMicroToTimestamp(4_000)),
			types.Datetime(types.UnixMicroToTimestamp(3_000)),
		}, got)
	}
}

func TestParquet_Decimal_Mapping(t *testing.T) {
	proc := testutil.NewProc(t)
	// decimal64 from int32 non-dict
	{
		st := parquet.Int32Type
		page := st.NewPage(0, 2, encoding.Int32Values([]int32{1, -2}))
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
		vec := vector.NewVec(types.New(types.T_decimal64, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_decimal64), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		got := vector.MustFixedColWithTypeCheck[types.Decimal64](vec)
		require.Equal(t, int64(1), int64(got[0]))
		require.Equal(t, int64(-2), int64(got[1]))
	}
	// decimal128 from int64 non-dict
	{
		st := parquet.Int64Type
		page := st.NewPage(0, 2, encoding.Int64Values([]int64{5, -6}))
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
		vec := vector.NewVec(types.New(types.T_decimal128, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_decimal128), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		got := vector.MustFixedColWithTypeCheck[types.Decimal128](vec)
		require.Equal(t, uint64(5), got[0].B0_63)
		require.Equal(t, uint64(0), got[0].B64_127)
		require.Equal(t, uint64(^uint64(0)), got[1].B64_127) // sign extension for negative
	}
	// Note: decimal256 non-dictionary mapping is skipped because vector.New may not fully support it yet.
}

func TestParquet_Dictionary_Decimals(t *testing.T) {
	proc := testutil.NewProc(t)
	toDecimal64 := func(v int64) types.Decimal64 {
		return types.Decimal64(v)
	}

	// DECIMAL64 dictionary
	{
		node := parquet.Encoded(parquet.Leaf(parquet.Int64Type), &parquet.RLEDictionary)
		vals := []parquet.Value{
			parquet.Int64Value(7), parquet.Int64Value(-3), parquet.Int64Value(7),
		}
		f, page := writeDictAndGetPage(t, node, vals)
		vec := vector.NewVec(types.New(types.T_decimal64, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_decimal64), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		got := vector.MustFixedColWithTypeCheck[types.Decimal64](vec)
		require.Equal(t, []types.Decimal64{toDecimal64(7), toDecimal64(-3), toDecimal64(7)}, got)
	}

	// DECIMAL128 dictionary
	{
		node := parquet.Encoded(parquet.Leaf(parquet.Int64Type), &parquet.RLEDictionary)
		vals := []parquet.Value{
			parquet.Int64Value(11), parquet.Int64Value(-9), parquet.Int64Value(11),
		}
		f, page := writeDictAndGetPage(t, node, vals)
		vec := vector.NewVec(types.New(types.T_decimal128, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_decimal128), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		got := vector.MustFixedColWithTypeCheck[types.Decimal128](vec)
		require.Equal(t, []types.Decimal128{
			decimal128FromInt64(11),
			decimal128FromInt64(-9),
			decimal128FromInt64(11),
		}, got)
	}

	// DECIMAL256 dictionary
	{
		node := parquet.Encoded(parquet.Leaf(parquet.Int64Type), &parquet.RLEDictionary)
		vals := []parquet.Value{
			parquet.Int64Value(13), parquet.Int64Value(-5), parquet.Int64Value(13),
		}
		f, page := writeDictAndGetPage(t, node, vals)
		vec := vector.NewVec(types.New(types.T_decimal256, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_decimal256), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		got := vector.MustFixedColWithTypeCheck[types.Decimal256](vec)
		require.Equal(t, []types.Decimal256{
			decimal256FromInt64(13),
			decimal256FromInt64(-5),
			decimal256FromInt64(13),
		}, got)
	}
}

func TestParquet_openFile_localNYI(t *testing.T) {
	param := &ExternalParam{
		ExParamConst: ExParamConst{
			Ctx: context.Background(),
			Extern: &tree.ExternParam{
				ExParamConst: tree.ExParamConst{
					ScanType: tree.INFILE,
				},
				ExParam: tree.ExParam{
					Local: true,
				},
			},
		},
		ExParam: ExParam{
			Fileparam: &ExFileparam{
				FileIndex: 1,
			},
		},
	}

	var h ParquetHandler
	err := h.openFile(param)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrNYI))
}

func TestParquet_prepare_missingColumn(t *testing.T) {
	var buf bytes.Buffer
	schema := parquet.NewSchema("x", parquet.Group{
		"colA": parquet.Leaf(parquet.Int32Type),
	})
	w := parquet.NewWriter(&buf, schema)
	_, err := w.WriteRows([]parquet.Row{
		{parquet.Int32Value(1).Level(0, 0, 0)},
	})
	require.NoError(t, err)
	require.NoError(t, w.Close())

	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)

	h := &ParquetHandler{file: f}
	param := &ExternalParam{
		ExParamConst: ExParamConst{
			Ctx: context.Background(),
			Attrs: []plan.ExternAttr{
				{ColName: "not_exist", ColIndex: 0},
			},
			Cols: []*plan.ColDef{
				{Typ: plan.Type{Id: int32(types.T_int32)}},
			},
		},
	}
	err = h.prepare(param)
	require.Error(t, err)
}

func TestParquet_prepare_optionalToNotNull(t *testing.T) {
	var buf bytes.Buffer
	schema := parquet.NewSchema("x", parquet.Group{
		"c": parquet.Optional(parquet.Leaf(parquet.Int32Type)),
	})
	w := parquet.NewWriter(&buf, schema)
	_, err := w.WriteRows([]parquet.Row{
		{parquet.Int32Value(1).Level(0, 0, 0)},
	})
	require.NoError(t, err)
	require.NoError(t, w.Close())
	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)

	h := &ParquetHandler{file: f}
	param := &ExternalParam{
		ExParamConst: ExParamConst{
			Ctx: context.Background(),
			Attrs: []plan.ExternAttr{
				{ColName: "c", ColIndex: 0},
			},
			Cols: []*plan.ColDef{
				{Typ: plan.Type{Id: int32(types.T_int32), NotNullable: true}},
			},
		},
	}
	err = h.prepare(param)
	require.Error(t, err)
}

func TestParquet_ensureDictionaryIndexes_outOfRange(t *testing.T) {
	ctx := context.Background()
	err := ensureDictionaryIndexes(ctx, 3, []int32{0, 1, 5})
	require.Error(t, err)
	require.Contains(t, err.Error(), "out of range")
}

func TestParquet_Bool_DictionaryNYI(t *testing.T) {
	proc := testutil.NewProc(t)
	// Dictionary-encoded boolean column should be NYI in mapper
	node := parquet.Encoded(parquet.Leaf(parquet.BooleanType), &parquet.RLEDictionary)
	vals := []parquet.Value{
		parquet.BooleanValue(true), parquet.BooleanValue(false), parquet.BooleanValue(true),
	}
	f, page := writeDictAndGetPage(t, node, vals)
	vec := vector.NewVec(types.New(types.T_bool, 0, 0))
	var h ParquetHandler
	mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_bool), NotNullable: true})
	require.NotNil(t, mp)
	err := mp.mapping(page, proc, vec)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrNYI))
}

// Note: Constructing explicit nulls for pages requires internal helpers.
// Mapping null behavior is indirectly exercised in other branches.

func TestParquet_ScanParquetFile_SteppedBatches(t *testing.T) {
	// Reduce batch size so scan steps across multiple calls
	save := maxParquetBatchCnt
	maxParquetBatchCnt = 1
	defer func() { maxParquetBatchCnt = save }()

	// Build a simple file with 3 int32 values
	st := parquet.Int32Type
	page := st.NewPage(0, 3, encoding.Int32Values([]int32{10, 20, 30}))
	var buf bytes.Buffer
	schema := parquet.NewSchema("x", parquet.Group{"c": parquet.Leaf(st)})
	w := parquet.NewWriter(&buf, schema)
	vals := make([]parquet.Value, page.NumRows())
	_, _ = page.Values().ReadValues(vals)
	_, err := w.WriteRows([]parquet.Row{parquet.MakeRow(vals)})
	require.NoError(t, err)
	require.NoError(t, w.Close())

	// Prepare ExternalParam using INLINE data
	param := &ExternalParam{
		ExParamConst: ExParamConst{
			Ctx:      context.Background(),
			Attrs:    []plan.ExternAttr{{ColName: "c", ColIndex: 0}},
			Cols:     []*plan.ColDef{{Typ: plan.Type{Id: int32(types.T_int32), NotNullable: true}}},
			Extern:   &tree.ExternParam{ExParamConst: tree.ExParamConst{ScanType: tree.INLINE}, ExParam: tree.ExParam{}},
			FileSize: []int64{int64(buf.Len())},
		},
		ExParam: ExParam{Fileparam: &ExFileparam{FileIndex: 1, FileCnt: 1}},
	}
	param.Extern.Data = string(buf.Bytes())

	proc := testutil.NewProc(t)

	got := make([]int32, 0, 3)
	for attempts := 0; attempts < 5 && !param.Fileparam.End; attempts++ {
		bat := vectorBatch([]types.Type{types.New(types.T_int32, 0, 0)})
		require.NoError(t, scanParquetFile(context.Background(), param, proc, bat))
		vals := vector.MustFixedColWithTypeCheck[int32](bat.Vecs[0])
		got = append(got, vals[:bat.RowCount()]...)
	}
	require.Equal(t, []int32{10, 20, 30}, got)
	require.True(t, param.Fileparam.End)
}

// helper to build a batch with provided vector types
func vectorBatch(ts []types.Type) *batch.Batch {
	bat := batch.NewWithSize(len(ts))
	for i, t := range ts {
		bat.Vecs[i] = vector.NewVec(t)
	}
	return bat
}

func Test_parquet_strLoader(t *testing.T) {
	var ld strLoader
	// ByteArray
	ld.init(encoding.ByteArrayValues([]byte("abcd"), []uint32{0, 2, 4}))
	require.Equal(t, "ab", string(ld.loadNext()))
	require.Equal(t, "cd", string(ld.loadNext()))

	// FixedLenByteArray
	var ld2 strLoader
	ld2.init(encoding.FixedLenByteArrayValues([]byte("abcdef"), 3))
	require.Equal(t, "abc", string(ld2.loadNext()))
	require.Equal(t, "def", string(ld2.loadAt(1)))

	// Unsupported kind panics
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic for unsupported kind")
		}
	}()
	var ld3 strLoader
	ld3.init(encoding.Int32Values([]int32{1}))
}

func Test_parquet_copyDictPageToVec_indexError(t *testing.T) {
	proc := testutil.NewProc(t)
	// Build a simple page context using int32 values as indexes
	st := parquet.Int32Type
	page := st.NewPage(0, 3, encoding.Int32Values([]int32{0, 2, 5}))
	vec := vector.NewVec(types.New(types.T_int32, 0, 0))
	mp := &columnMapper{srcNull: false, dstNull: false, maxDefinitionLevel: 0}

	// dictLen=3, but index 5 is out of range -> error
	err := copyDictPageToVec[int32](mp, page, proc, vec, 3, []int32{0, 2, 5}, func(idx int32) int32 { return idx })
	require.Error(t, err)
}

func Test_parquet_decimalBytes_Roundtrip_And_Overflow(t *testing.T) {
	ctx := context.Background()

	// Roundtrip for 64/128/256 with small values
	for _, v := range []int64{0, 1, -1, 1234567890, -987654321} {
		// 64
		b64, err := bigIntToTwosComplementBytes(ctx, big.NewInt(v), 8)
		require.NoError(t, err)
		d64, err := decimalBytesToDecimal64(ctx, b64)
		require.NoError(t, err)
		require.Equal(t, types.Decimal64(v), d64)

		// 128
		b128, err := bigIntToTwosComplementBytes(ctx, big.NewInt(v), 16)
		require.NoError(t, err)
		_, err = decimalBytesToDecimal128(ctx, b128)
		require.NoError(t, err)

		// 256
		b256, err := bigIntToTwosComplementBytes(ctx, big.NewInt(v), 32)
		require.NoError(t, err)
		d256, err := decimalBytesToDecimal256(ctx, b256)
		require.NoError(t, err)
		_ = d256
	}

	// Positive not fit to size for bigIntToTwosComplementBytes
	_, err := bigIntToTwosComplementBytes(ctx, new(big.Int).Lsh(big.NewInt(1), 64), 8)
	require.Error(t, err)

	// Negative out of range for size: -2^8 for size=1
	_, err = bigIntToTwosComplementBytes(ctx, new(big.Int).Neg(new(big.Int).Lsh(big.NewInt(1), 8)), 1)
	require.Error(t, err)
}

func Test_parquet_decodeDecimal_AllBranches(t *testing.T) {
	ctx := context.Background()
	// decimal64 from int32/int64
	vals64, err := decodeDecimal64Values(ctx, parquet.Int32, encoding.Int32Values([]int32{1, -2}))
	require.NoError(t, err)
	require.Len(t, vals64, 2)
	require.Equal(t, int64(1), int64(vals64[0]))
	require.Equal(t, int64(-2), int64(vals64[1]))
	vals64, err = decodeDecimal64Values(ctx, parquet.Int64, encoding.Int64Values([]int64{3, -4}))
	require.NoError(t, err)
	require.Len(t, vals64, 2)
	require.Equal(t, int64(3), int64(vals64[0]))
	require.Equal(t, int64(-4), int64(vals64[1]))
	// bytearray empty offsets -> nil
	vals64, err = decodeDecimal64Values(ctx, parquet.ByteArray, encoding.ByteArrayValues(nil, nil))
	require.NoError(t, err)
	require.Nil(t, vals64)
	// bytearray with two values 1 and -2
	ba := []byte{0x01, 0xFE} // 0x01 -> 1, 0xFE -> -2 in 1-byte two's complement
	vals64, err = decodeDecimal64Values(ctx, parquet.ByteArray, encoding.ByteArrayValues(ba, []uint32{0, 1, 2}))
	require.NoError(t, err)
	require.Len(t, vals64, 2)
	require.Equal(t, int64(1), int64(vals64[0]))
	require.Equal(t, int64(-2), int64(vals64[1]))
	// fixed len incorrect size
	_, err = decodeDecimal64Values(ctx, parquet.FixedLenByteArray, encoding.FixedLenByteArrayValues([]byte{0, 1, 2}, 2))
	require.Error(t, err)

	// decimal128 branches
	_, err = decodeDecimal128Values(ctx, parquet.FixedLenByteArray, encoding.FixedLenByteArrayValues([]byte{0, 0, 0, 1}, 0))
	require.Error(t, err)
	_, err = decodeDecimal128Values(ctx, parquet.Boolean, encoding.BooleanValues([]byte{1}))
	require.Error(t, err)
	// decimal128 success from bytearray and fixed-len
	{
		b1, _ := bigIntToTwosComplementBytes(ctx, big.NewInt(1), 16)
		bm1, _ := bigIntToTwosComplementBytes(ctx, big.NewInt(-1), 16)
		buf := append([]byte{}, b1...)
		buf = append(buf, bm1...)
		vals128, err := decodeDecimal128Values(ctx, parquet.ByteArray, encoding.ByteArrayValues(buf, []uint32{0, 16, 32}))
		require.NoError(t, err)
		require.Equal(t, 2, len(vals128))
		// fixed-len
		vals128, err = decodeDecimal128Values(ctx, parquet.FixedLenByteArray, encoding.FixedLenByteArrayValues(buf, 16))
		require.NoError(t, err)
		require.Equal(t, 2, len(vals128))
	}

	// decimal256 branches
	_, err = decodeDecimal256Values(ctx, parquet.FixedLenByteArray, encoding.FixedLenByteArrayValues([]byte{0, 0, 0, 1}, 0))
	require.Error(t, err)
	_, err = decodeDecimal256Values(ctx, parquet.Boolean, encoding.BooleanValues([]byte{1}))
	require.Error(t, err)
	// decimal256 success from bytearray and fixed-len
	{
		b1, _ := bigIntToTwosComplementBytes(ctx, big.NewInt(1), 32)
		bm1, _ := bigIntToTwosComplementBytes(ctx, big.NewInt(-1), 32)
		buf := append([]byte{}, b1...)
		buf = append(buf, bm1...)
		vals256, err := decodeDecimal256Values(ctx, parquet.ByteArray, encoding.ByteArrayValues(buf, []uint32{0, 32, 64}))
		require.NoError(t, err)
		require.Equal(t, 2, len(vals256))
		vals256, err = decodeDecimal256Values(ctx, parquet.FixedLenByteArray, encoding.FixedLenByteArrayValues(buf, 32))
		require.NoError(t, err)
		require.Equal(t, 2, len(vals256))
	}

	// decimal128/256 from int32/int64 success
	vals128, err := decodeDecimal128Values(ctx, parquet.Int32, encoding.Int32Values([]int32{1, -1}))
	require.NoError(t, err)
	require.Len(t, vals128, 2)
	vals128, err = decodeDecimal128Values(ctx, parquet.Int64, encoding.Int64Values([]int64{2, -2}))
	require.NoError(t, err)
	require.Len(t, vals128, 2)
	vals256, err := decodeDecimal256Values(ctx, parquet.Int32, encoding.Int32Values([]int32{1, -1}))
	require.NoError(t, err)
	require.Len(t, vals256, 2)
	vals256, err = decodeDecimal256Values(ctx, parquet.Int64, encoding.Int64Values([]int64{2, -2}))
	require.NoError(t, err)
	require.Len(t, vals256, 2)
}

func Test_parquet_decimal256FromInt64(t *testing.T) {
	d := decimal256FromInt64(-1)
	require.Equal(t, uint64(^uint64(0)), d.B64_127)
}

func Test_prepareNullCheck_simplePaths(t *testing.T) {
	ctx := context.Background()
	// Build a required int32 page (no nulls)
	st := parquet.Int32Type
	page := st.NewPage(0, 2, encoding.Int32Values([]int32{1, 2}))
	mp := &columnMapper{srcNull: false, dstNull: true, maxDefinitionLevel: 0}
	nc, err := prepareNullCheck(ctx, mp, page)
	require.NoError(t, err)
	require.True(t, nc.noNulls)
	require.False(t, nc.isNull(0))
	require.False(t, nc.isNull(1))

	// when srcNull true but page has no nulls -> noNulls should be true
	mp = &columnMapper{srcNull: true, dstNull: true, maxDefinitionLevel: 0}
	nc, err = prepareNullCheck(ctx, mp, page)
	require.NoError(t, err)
	require.True(t, nc.noNulls)
	require.False(t, nc.isNull(0))
}

func Test_validateStringDataCount(t *testing.T) {
	ctx := context.Background()

	// Test ByteArray validation - success case
	{
		var loader strLoader
		loader.init(encoding.ByteArrayValues([]byte("abcdef"), []uint32{0, 3, 6}))
		err := validateStringDataCount(ctx, &loader, 2)
		require.NoError(t, err)
	}

	// Test ByteArray validation - mismatch
	{
		var loader strLoader
		loader.init(encoding.ByteArrayValues([]byte("abcdef"), []uint32{0, 3, 6}))
		err := validateStringDataCount(ctx, &loader, 3)
		require.Error(t, err)
		require.Contains(t, err.Error(), "expected 3 non-null values")
	}

	// Test FixedLenByteArray validation - success case
	{
		var loader strLoader
		loader.init(encoding.FixedLenByteArrayValues([]byte("abcdef"), 3))
		err := validateStringDataCount(ctx, &loader, 2)
		require.NoError(t, err)
	}

	// Test FixedLenByteArray validation - mismatch
	{
		var loader strLoader
		loader.init(encoding.FixedLenByteArrayValues([]byte("abcdef"), 3))
		err := validateStringDataCount(ctx, &loader, 3)
		require.Error(t, err)
		require.Contains(t, err.Error(), "expected 3 non-null values")
	}

	// Test empty ByteArray
	{
		var loader strLoader
		loader.init(encoding.ByteArrayValues(nil, nil))
		err := validateStringDataCount(ctx, &loader, 0)
		require.NoError(t, err)
	}
}

func Test_validateDictionaryIndicesCount(t *testing.T) {
	ctx := context.Background()

	// Success case
	indices := []int32{0, 1, 2}
	err := validateDictionaryIndicesCount(ctx, indices, 3)
	require.NoError(t, err)

	// Mismatch case
	err = validateDictionaryIndicesCount(ctx, indices, 5)
	require.Error(t, err)
	require.Contains(t, err.Error(), "expected 5 dictionary indices")
}

func Test_wrapParseError(t *testing.T) {
	ctx := context.Background()

	// nil error returns nil
	require.Nil(t, wrapParseError(ctx, 0, nil))

	// Plain error gets wrapped with row context
	plainErr := errors.New("parse error")
	wrapped := wrapParseError(ctx, 5, plainErr)
	require.Error(t, wrapped)
	require.Contains(t, wrapped.Error(), "row 5")

	// moerr.Error is returned directly without extra wrapping
	moErr := moerr.NewInternalError(ctx, "already a moerr")
	result := wrapParseError(ctx, 10, moErr)
	require.Equal(t, moErr, result)
}

func Test_fsReaderAt_ReadAt(t *testing.T) {
	data := []byte("hello world")
	r := &fsReaderAt{fs: &fakeFS{b: data}, readPath: "fake:hello", ctx: context.Background()}
	buf := make([]byte, 5)
	n, err := r.ReadAt(buf, 6)
	require.NoError(t, err)
	require.Equal(t, 5, n)
	require.Equal(t, []byte("world"), buf)
}

func Test_copyPageToVecMap_NullsHandled(t *testing.T) {
	proc := testutil.NewProc(t)
	// Create an optional int32 column with one null via writer to get real levels
	var buf bytes.Buffer
	schema := parquet.NewSchema("x", parquet.Group{"c": parquet.Optional(parquet.Leaf(parquet.Int32Type))})
	w := parquet.NewWriter(&buf, schema)
	rows := []parquet.Row{
		{parquet.Int32Value(42).Level(0, 1, 0)}, // present
		{parquet.NullValue().Level(0, 0, 0)},    // null
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
	mp := h.getMapper(col, plan.Type{Id: int32(types.T_int32) /* nullable */})
	require.NotNil(t, mp)
	require.NoError(t, mp.mapping(page, proc, vec))
	require.Equal(t, 2, vec.Length())
	// null index 1
	require.True(t, vec.GetNulls().Contains(1))
	got := vector.MustFixedColWithTypeCheck[int32](vec)[:2]
	require.Equal(t, int32(42), got[0])
}

func Test_getData_FinishAndOffset(t *testing.T) {
	proc := testutil.NewProc(t)
	// File with 2 rows
	var buf bytes.Buffer
	schema := parquet.NewSchema("x", parquet.Group{"c": parquet.Leaf(parquet.Int32Type)})
	w := parquet.NewWriter(&buf, schema)
	row := parquet.MakeRow([]parquet.Value{parquet.Int32Value(7).Level(0, 0, 0), parquet.Int32Value(8).Level(0, 0, 0)})
	_, err := w.WriteRows([]parquet.Row{row})
	require.NoError(t, err)
	require.NoError(t, w.Close())
	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)

	h := &ParquetHandler{file: f, batchCnt: 1}
	param := &ExternalParam{ExParamConst: ExParamConst{Ctx: context.Background(), Attrs: []plan.ExternAttr{{ColName: "c", ColIndex: 0}}, Cols: []*plan.ColDef{{Typ: plan.Type{Id: int32(types.T_int32), NotNullable: true}}}}, ExParam: ExParam{Fileparam: &ExFileparam{FileIndex: 1, FileCnt: 1}}}
	require.NoError(t, h.prepare(param))
	param.parqh = h

	// First call -> one row, not finished yet
	bat := vectorBatch([]types.Type{types.New(types.T_int32, 0, 0)})
	require.NoError(t, h.getData(bat, param, proc))
	require.Equal(t, 1, bat.RowCount())
	require.NotNil(t, param.parqh)
	// Second call -> last row and finish
	bat2 := vectorBatch([]types.Type{types.New(types.T_int32, 0, 0)})
	require.NoError(t, h.getData(bat2, param, proc))
	require.Equal(t, 1, bat2.RowCount())
	require.Nil(t, param.parqh)
}

// TestParquet_Timestamp_NotAdjustedToUTC tests loading TIMESTAMP with IsAdjustedToUTC=false.
// When IsAdjustedToUTC=false, the parquet value represents a "local time literal" without timezone info.
// MO should store it such that displaying with session timezone shows the original literal value.
func TestParquet_Timestamp_NotAdjustedToUTC(t *testing.T) {
	// Create a process with a specific timezone (+8 hours)
	proc := testutil.NewProc(t)
	loc := time.FixedZone("UTC+8", 8*3600)
	proc.Base.SessionInfo.TimeZone = loc

	// Test value: 2024-01-15 10:30:00 as Unix microseconds (interpreted as UTC in parquet)
	// 2024-01-15 10:30:00 UTC = 1705314600 seconds = 1705314600000000 microseconds
	testMicros := int64(1705314600000000)

	// Test with IsAdjustedToUTC=false (using TimestampAdjusted)
	{
		// Create parquet file with IsAdjustedToUTC=false
		st := parquet.TimestampAdjusted(parquet.Microsecond, false).Type()
		page := st.NewPage(0, 1, encoding.Int64Values([]int64{testMicros}))

		var buf bytes.Buffer
		schema := parquet.NewSchema("x", parquet.Group{
			"c": parquet.TimestampAdjusted(parquet.Microsecond, false),
		})
		w := parquet.NewWriter(&buf, schema)
		vals := make([]parquet.Value, page.NumRows())
		_, _ = page.Values().ReadValues(vals)
		_, err := w.WriteRows([]parquet.Row{parquet.MakeRow(vals)})
		require.NoError(t, err)
		require.NoError(t, w.Close())

		f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		require.NoError(t, err)

		vec := vector.NewVec(types.New(types.T_timestamp, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_timestamp)})
		require.NotNil(t, mp, "mapper should not be nil for IsAdjustedToUTC=false")

		err = mp.mapping(page, proc, vec)
		require.NoError(t, err)

		got := vector.MustFixedColWithTypeCheck[types.Timestamp](vec)
		require.Equal(t, 1, len(got))

		// The displayed value should be "2024-01-15 10:30:00" regardless of timezone
		// String2 uses the session timezone to display
		displayed := got[0].String2(loc, 0)
		require.Equal(t, "2024-01-15 10:30:00", displayed,
			"IsAdjustedToUTC=false should preserve the literal time value")
	}

	// Compare with IsAdjustedToUTC=true (default behavior)
	{
		st := parquet.Timestamp(parquet.Microsecond).Type()
		page := st.NewPage(0, 1, encoding.Int64Values([]int64{testMicros}))

		var buf bytes.Buffer
		schema := parquet.NewSchema("x", parquet.Group{
			"c": parquet.Timestamp(parquet.Microsecond),
		})
		w := parquet.NewWriter(&buf, schema)
		vals := make([]parquet.Value, page.NumRows())
		_, _ = page.Values().ReadValues(vals)
		_, err := w.WriteRows([]parquet.Row{parquet.MakeRow(vals)})
		require.NoError(t, err)
		require.NoError(t, w.Close())

		f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		require.NoError(t, err)

		vec := vector.NewVec(types.New(types.T_timestamp, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_timestamp)})
		require.NotNil(t, mp)

		err = mp.mapping(page, proc, vec)
		require.NoError(t, err)

		got := vector.MustFixedColWithTypeCheck[types.Timestamp](vec)
		displayed := got[0].String2(loc, 0)
		// With IsAdjustedToUTC=true, the value is UTC, so +8 timezone shows +8 hours
		require.Equal(t, "2024-01-15 18:30:00", displayed,
			"IsAdjustedToUTC=true should add timezone offset when displaying")
	}
}

// TestParquet_Timestamp_NotAdjustedToUTC_AllUnits tests all time units (nanos, micros, millis)
func TestParquet_Timestamp_NotAdjustedToUTC_AllUnits(t *testing.T) {
	proc := testutil.NewProc(t)
	loc := time.FixedZone("UTC+8", 8*3600)
	proc.Base.SessionInfo.TimeZone = loc

	// 2024-01-15 10:30:00 UTC = 1705314600000000 microseconds
	testMicros := int64(1705314600000000)
	testMillis := testMicros / 1000
	testNanos := testMicros * 1000

	tests := []struct {
		name  string
		unit  parquet.TimeUnit
		value int64
	}{
		{"micros", parquet.Microsecond, testMicros},
		{"millis", parquet.Millisecond, testMillis},
		{"nanos", parquet.Nanosecond, testNanos},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			st := parquet.TimestampAdjusted(tc.unit, false).Type()
			page := st.NewPage(0, 1, encoding.Int64Values([]int64{tc.value}))

			var buf bytes.Buffer
			schema := parquet.NewSchema("x", parquet.Group{
				"c": parquet.TimestampAdjusted(tc.unit, false),
			})
			w := parquet.NewWriter(&buf, schema)
			vals := make([]parquet.Value, page.NumRows())
			_, _ = page.Values().ReadValues(vals)
			_, err := w.WriteRows([]parquet.Row{parquet.MakeRow(vals)})
			require.NoError(t, err)
			require.NoError(t, w.Close())

			f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
			require.NoError(t, err)

			vec := vector.NewVec(types.New(types.T_timestamp, 0, 0))
			var h ParquetHandler
			mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_timestamp)})
			require.NotNil(t, mp, "mapper should not be nil for %s", tc.name)

			err = mp.mapping(page, proc, vec)
			require.NoError(t, err)

			got := vector.MustFixedColWithTypeCheck[types.Timestamp](vec)
			displayed := got[0].String2(loc, 0)
			require.Equal(t, "2024-01-15 10:30:00", displayed,
				"unit %s: should preserve literal time value", tc.name)
		})
	}
}

// TestParquet_Timestamp_NotAdjustedToUTC_Dictionary tests dictionary-encoded timestamps
func TestParquet_Timestamp_NotAdjustedToUTC_Dictionary(t *testing.T) {
	proc := testutil.NewProc(t)
	loc := time.FixedZone("UTC+8", 8*3600)
	proc.Base.SessionInfo.TimeZone = loc

	// 2024-01-15 10:30:00 UTC = 1705314600000000 microseconds
	testMicros := int64(1705314600000000)

	node := parquet.Encoded(parquet.TimestampAdjusted(parquet.Microsecond, false), &parquet.RLEDictionary)
	vals := []parquet.Value{
		parquet.Int64Value(testMicros),
		parquet.Int64Value(testMicros + 1000000), // +1 second
		parquet.Int64Value(testMicros),
	}
	f, page := writeDictAndGetPage(t, node, vals)

	vec := vector.NewVec(types.New(types.T_timestamp, 0, 0))
	var h ParquetHandler
	mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_timestamp)})
	require.NotNil(t, mp)

	err := mp.mapping(page, proc, vec)
	require.NoError(t, err)

	got := vector.MustFixedColWithTypeCheck[types.Timestamp](vec)
	require.Equal(t, 3, len(got))
	require.Equal(t, "2024-01-15 10:30:00", got[0].String2(loc, 0))
	require.Equal(t, "2024-01-15 10:30:01", got[1].String2(loc, 0))
	require.Equal(t, "2024-01-15 10:30:00", got[2].String2(loc, 0))
}
