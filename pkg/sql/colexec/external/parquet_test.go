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
	"fmt"
	"math/big"
	"testing"

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

func TestParquetDecimalMappingRegression(t *testing.T) {
	proc := testutil.NewProc(t)
	ctx := context.Background()
	decimalBytes := func(v int64) []byte {
		b, err := bigIntToTwosComplementBytes(ctx, big.NewInt(v), 8)
		require.NoError(t, err)
		return b
	}

	values := []parquet.Value{
		parquet.FixedLenByteArrayValue(decimalBytes(12345)).Level(0, 0, 0),
		parquet.FixedLenByteArrayValue(decimalBytes(-6789)).Level(0, 0, 0),
	}

	var buf bytes.Buffer
	schema := parquet.NewSchema("x", parquet.Group{
		"c": parquet.Decimal(2, 12, parquet.FixedLenByteArrayType(8)),
	})
	w := parquet.NewWriter(&buf, schema)
	_, err := w.WriteRows([]parquet.Row{parquet.MakeRow(values)})
	require.NoError(t, err)
	require.NoError(t, w.Close())

	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	col := f.Root().Column("c")
	page, err := col.Pages().ReadPage()
	require.NoError(t, err)

	vec := vector.NewVec(types.New(types.T_decimal64, 12, 2))
	var h ParquetHandler
	mp := h.getMapper(col, plan.Type{
		Id:          int32(types.T_decimal64),
		Width:       12,
		Scale:       2,
		NotNullable: true,
	})
	require.NotNil(t, mp)
	require.NoError(t, mp.mapping(page, proc, vec))

	neg := int64(-6789)
	got := vector.MustFixedColWithTypeCheck[types.Decimal64](vec)
	require.Equal(t, []types.Decimal64{
		types.Decimal64(int64(12345)),
		types.Decimal64(neg),
	}, got)
}

func TestParquetStringToDecimalMapping(t *testing.T) {
	proc := testutil.NewProc(t)
	values := []parquet.Value{
		parquet.ByteArrayValue([]byte(" +123.45 ")).Level(0, 0, 0),
		parquet.ByteArrayValue([]byte("-6.70")).Level(0, 0, 0),
	}

	var buf bytes.Buffer
	schema := parquet.NewSchema("x", parquet.Group{
		"c": parquet.String(),
	})
	w := parquet.NewWriter(&buf, schema)
	_, err := w.WriteRows([]parquet.Row{parquet.MakeRow(values)})
	require.NoError(t, err)
	require.NoError(t, w.Close())

	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	col := f.Root().Column("c")
	page, err := col.Pages().ReadPage()
	require.NoError(t, err)

	vec := vector.NewVec(types.New(types.T_decimal64, 12, 2))
	var h ParquetHandler
	mp := h.getMapper(col, plan.Type{
		Id:          int32(types.T_decimal64),
		Width:       12,
		Scale:       2,
		NotNullable: true,
	})
	require.NotNil(t, mp)
	require.NoError(t, mp.mapping(page, proc, vec))

	expected0, err := types.ParseDecimal64("123.45", 12, 2)
	require.NoError(t, err)
	expected1, err := types.ParseDecimal64("-6.70", 12, 2)
	require.NoError(t, err)
	got := vector.MustFixedColWithTypeCheck[types.Decimal64](vec)
	require.Equal(t, []types.Decimal64{expected0, expected1}, got)
}

func TestParquetStringToJsonMapping(t *testing.T) {
	proc := testutil.NewProc(t)
	requireJSONAt := func(t *testing.T, vec *vector.Vector, row int, expected string) {
		t.Helper()
		want, err := types.ParseStringToByteJson(expected)
		require.NoError(t, err)
		got := types.DecodeJson(vec.GetBytesAt(row))
		require.Equal(t, want.String(), got.String())
	}

	t.Run("plain string page", func(t *testing.T) {
		f, page := writeDictAndGetPage(t, parquet.String(), []parquet.Value{
			parquet.ByteArrayValue([]byte(`{"k":"v0","n":0}`)),
			parquet.ByteArrayValue([]byte(` {"k":"v1","n":1} `)),
		})

		vec := vector.NewVec(types.T_json.ToType())
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_json), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))

		require.Equal(t, 2, vec.Length())
		requireJSONAt(t, vec, 0, `{"k":"v0","n":0}`)
		requireJSONAt(t, vec, 1, `{"k":"v1","n":1}`)
	})

	t.Run("dictionary string page", func(t *testing.T) {
		f, page := writeDictAndGetPage(t, parquet.Encoded(parquet.String(), &parquet.RLEDictionary), []parquet.Value{
			parquet.ByteArrayValue([]byte(`{"k":"v0","n":0}`)),
			parquet.ByteArrayValue([]byte(`{"k":"v1","n":1}`)),
			parquet.ByteArrayValue([]byte(`{"k":"v0","n":0}`)),
		})

		vec := vector.NewVec(types.T_json.ToType())
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_json), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))

		require.Equal(t, 3, vec.Length())
		requireJSONAt(t, vec, 0, `{"k":"v0","n":0}`)
		requireJSONAt(t, vec, 1, `{"k":"v1","n":1}`)
		requireJSONAt(t, vec, 2, `{"k":"v0","n":0}`)
	})

	t.Run("invalid json", func(t *testing.T) {
		f, page := writeDictAndGetPage(t, parquet.String(), []parquet.Value{
			parquet.ByteArrayValue([]byte(`not-json`)),
		})

		vec := vector.NewVec(types.T_json.ToType())
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_json), NotNullable: true})
		require.NotNil(t, mp)
		require.ErrorContains(t, mp.mapping(page, proc, vec), "json text not-json")
	})
}

func TestParquetListToVectorMapping(t *testing.T) {
	proc := testutil.NewProc(t)

	t.Run("float list to vecf32", func(t *testing.T) {
		f, page := writeListAndGetPage(t, parquet.Leaf(parquet.FloatType), []parquet.Row{
			{
				parquet.FloatValue(1).Level(0, 1, 0),
				parquet.FloatValue(2).Level(1, 1, 0),
				parquet.FloatValue(3).Level(1, 1, 0),
			},
			{
				parquet.FloatValue(4.5).Level(0, 1, 0),
				parquet.FloatValue(5.5).Level(1, 1, 0),
				parquet.FloatValue(6.5).Level(1, 1, 0),
			},
		})

		var h ParquetHandler
		leaf, mp := h.getNestedMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_array_float32), Width: 3})
		require.NotNil(t, leaf)
		require.NotNil(t, mp)
		require.True(t, mp.allowRepetition)

		vec := vector.NewVec(types.New(types.T_array_float32, 3, 0))
		require.NoError(t, mp.mapping(page, proc, vec))
		require.Equal(t, [][]float32{{1, 2, 3}, {4.5, 5.5, 6.5}}, vector.MustArrayCol[float32](vec))
	})

	t.Run("double list to vecf64", func(t *testing.T) {
		f, page := writeListAndGetPage(t, parquet.Leaf(parquet.DoubleType), []parquet.Row{
			{
				parquet.DoubleValue(1.25).Level(0, 1, 0),
				parquet.DoubleValue(2.25).Level(1, 1, 0),
				parquet.DoubleValue(3.25).Level(1, 1, 0),
			},
			{
				parquet.DoubleValue(4.25).Level(0, 1, 0),
				parquet.DoubleValue(5.25).Level(1, 1, 0),
				parquet.DoubleValue(6.25).Level(1, 1, 0),
			},
		})

		var h ParquetHandler
		leaf, mp := h.getNestedMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_array_float64), Width: 3})
		require.NotNil(t, leaf)
		require.NotNil(t, mp)

		vec := vector.NewVec(types.New(types.T_array_float64, 3, 0))
		require.NoError(t, mp.mapping(page, proc, vec))
		require.Equal(t, [][]float64{{1.25, 2.25, 3.25}, {4.25, 5.25, 6.25}}, vector.MustArrayCol[float64](vec))
	})

	t.Run("dimension mismatch", func(t *testing.T) {
		f, page := writeListAndGetPage(t, parquet.Leaf(parquet.FloatType), []parquet.Row{
			{
				parquet.FloatValue(1).Level(0, 1, 0),
				parquet.FloatValue(2).Level(1, 1, 0),
			},
		})

		var h ParquetHandler
		_, mp := h.getNestedMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_array_float32), Width: 3})
		require.NotNil(t, mp)

		vec := vector.NewVec(types.New(types.T_array_float32, 3, 0))
		require.ErrorContains(t, mp.mapping(page, proc, vec), "expected vector dimension 3 != actual dimension 2")
	})

	t.Run("empty lists", func(t *testing.T) {
		f, page := writeListAndGetPage(t, parquet.Leaf(parquet.FloatType), []parquet.Row{
			{
				parquet.NullValue().Level(0, 0, 0),
			},
			{
				parquet.FloatValue(1).Level(0, 1, 0),
				parquet.FloatValue(2).Level(1, 1, 0),
			},
			{
				parquet.NullValue().Level(0, 0, 0),
			},
		})

		var h ParquetHandler
		_, mp := h.getNestedMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_array_float32)})
		require.NotNil(t, mp)

		vec := vector.NewVec(types.T_array_float32.ToType())
		require.NoError(t, mp.mapping(page, proc, vec))
		rows := vector.MustArrayCol[float32](vec)
		require.Empty(t, rows[0])
		require.Equal(t, []float32{1, 2}, rows[1])
		require.Empty(t, rows[2])
	})

	t.Run("nullable list with empty row", func(t *testing.T) {
		f, page := writeListNodeAndGetPage(t, parquet.Optional(parquet.List(parquet.Leaf(parquet.FloatType))), []parquet.Row{
			{
				parquet.NullValue().Level(0, 0, 0),
			},
			{
				parquet.NullValue().Level(0, 1, 0),
			},
			{
				parquet.FloatValue(7).Level(0, 2, 0),
			},
		})

		var h ParquetHandler
		_, mp := h.getNestedMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_array_float32)})
		require.NotNil(t, mp)

		vec := vector.NewVec(types.T_array_float32.ToType())
		require.NoError(t, mp.mapping(page, proc, vec))
		require.Equal(t, 3, vec.Length())
		require.True(t, vec.GetNulls().Contains(0))
		require.False(t, vec.GetNulls().Contains(1))
		require.False(t, vec.GetNulls().Contains(2))
		rows := vector.MustArrayCol[float32](vec)
		require.Empty(t, rows[1])
		require.Equal(t, []float32{7}, rows[2])
	})

	t.Run("optional list elements rejected", func(t *testing.T) {
		f, _ := writeListNodeAndGetPage(t, parquet.List(parquet.Optional(parquet.Leaf(parquet.FloatType))), []parquet.Row{
			{
				parquet.NullValue().Level(0, 1, 0),
			},
		})

		var h ParquetHandler
		leaf, mp := h.getNestedMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_array_float32)})
		require.Nil(t, leaf)
		require.Nil(t, mp)
	})

	t.Run("fixed width optional list elements without nulls", func(t *testing.T) {
		f, page := writeListNodeAndGetPage(t, parquet.List(parquet.Optional(parquet.Leaf(parquet.FloatType))), []parquet.Row{
			{
				parquet.FloatValue(1).Level(0, 2, 0),
				parquet.FloatValue(2).Level(1, 2, 0),
				parquet.FloatValue(3).Level(1, 2, 0),
			},
		})

		var h ParquetHandler
		_, mp := h.getNestedMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_array_float32), Width: 3})
		require.NotNil(t, mp)

		vec := vector.NewVec(types.New(types.T_array_float32, 3, 0))
		require.NoError(t, mp.mapping(page, proc, vec))
		require.Equal(t, [][]float32{{1, 2, 3}}, vector.MustArrayCol[float32](vec))
	})

	t.Run("fixed width optional list null element rejected", func(t *testing.T) {
		f, page := writeListNodeAndGetPage(t, parquet.List(parquet.Optional(parquet.Leaf(parquet.FloatType))), []parquet.Row{
			{
				parquet.NullValue().Level(0, 1, 0),
				parquet.FloatValue(2).Level(1, 2, 0),
				parquet.FloatValue(3).Level(1, 2, 0),
			},
		})

		var h ParquetHandler
		_, mp := h.getNestedMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_array_float32), Width: 3})
		require.NotNil(t, mp)

		vec := vector.NewVec(types.New(types.T_array_float32, 3, 0))
		require.ErrorContains(t, mp.mapping(page, proc, vec), "parquet list NULL elements are not supported")
	})
}

func TestParquetCrossTypeMappings(t *testing.T) {
	proc := testutil.NewProc(t)
	ctx := context.Background()

	t.Run("bool to tinyint and varchar", func(t *testing.T) {
		values := []parquet.Value{parquet.BooleanValue(true), parquet.BooleanValue(false)}
		f, page := writeDictAndGetPage(t, parquet.Leaf(parquet.BooleanType), values)
		col := f.Root().Column("c")

		vecInt := vector.NewVec(types.T_int8.ToType())
		var h ParquetHandler
		mp := h.getMapper(col, plan.Type{Id: int32(types.T_int8), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vecInt))
		require.Equal(t, []int8{1, 0}, vector.MustFixedColWithTypeCheck[int8](vecInt))

		vecStr := vector.NewVec(types.T_varchar.ToType())
		mp = h.getMapper(col, plan.Type{Id: int32(types.T_varchar), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vecStr))
		require.Equal(t, "true", vecStr.GetStringAt(0))
		require.Equal(t, "false", vecStr.GetStringAt(1))
	})

	t.Run("int64 to int32 and varchar", func(t *testing.T) {
		f, page := writeDictAndGetPage(t, parquet.Int(64), []parquet.Value{
			parquet.Int64Value(100),
			parquet.Int64Value(-200),
		})
		col := f.Root().Column("c")

		vecInt := vector.NewVec(types.T_int32.ToType())
		var h ParquetHandler
		mp := h.getMapper(col, plan.Type{Id: int32(types.T_int32), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vecInt))
		require.Equal(t, []int32{100, -200}, vector.MustFixedColWithTypeCheck[int32](vecInt))

		vecStr := vector.NewVec(types.T_varchar.ToType())
		mp = h.getMapper(col, plan.Type{Id: int32(types.T_varchar), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vecStr))
		require.Equal(t, "100", vecStr.GetStringAt(0))
		require.Equal(t, "-200", vecStr.GetStringAt(1))
	})

	t.Run("int64 to int32 overflow", func(t *testing.T) {
		f, page := writeDictAndGetPage(t, parquet.Int(64), []parquet.Value{
			parquet.Int64Value(1 << 40),
		})

		vec := vector.NewVec(types.T_int32.ToType())
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_int32), NotNullable: true})
		require.NotNil(t, mp)
		require.ErrorContains(t, mp.mapping(page, proc, vec), "overflows INT")
	})

	t.Run("signed int32 to int8 overflow", func(t *testing.T) {
		f, page := writeDictAndGetPage(t, parquet.Leaf(parquet.Int32Type), []parquet.Value{
			parquet.Int32Value(128),
		})

		vec := vector.NewVec(types.T_int8.ToType())
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_int8), NotNullable: true})
		require.NotNil(t, mp)
		require.ErrorContains(t, mp.mapping(page, proc, vec), "overflows TINYINT")
	})

	t.Run("negative signed int32 to uint8", func(t *testing.T) {
		f, page := writeDictAndGetPage(t, parquet.Leaf(parquet.Int32Type), []parquet.Value{
			parquet.Int32Value(-1),
		})

		vec := vector.NewVec(types.T_uint8.ToType())
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_uint8), NotNullable: true})
		require.NotNil(t, mp)
		require.ErrorContains(t, mp.mapping(page, proc, vec), "negative parquet value")
	})

	t.Run("negative signed int32 to uint32", func(t *testing.T) {
		f, page := writeDictAndGetPage(t, parquet.Leaf(parquet.Int32Type), []parquet.Value{
			parquet.Int32Value(-1),
		})

		vec := vector.NewVec(types.T_uint32.ToType())
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_uint32), NotNullable: true})
		require.NotNil(t, mp)
		require.ErrorContains(t, mp.mapping(page, proc, vec), "negative parquet value")
	})

	t.Run("unsigned int32 to int32 overflow", func(t *testing.T) {
		f, page := writeDictAndGetPage(t, parquet.Uint(32), []parquet.Value{
			parquet.ValueOf(uint32(1 << 31)),
		})

		vec := vector.NewVec(types.T_int32.ToType())
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_int32), NotNullable: true})
		require.NotNil(t, mp)
		require.ErrorContains(t, mp.mapping(page, proc, vec), "overflows INT")
	})

	t.Run("unsigned int64 to int64 overflow", func(t *testing.T) {
		f, page := writeDictAndGetPage(t, parquet.Uint(64), []parquet.Value{
			parquet.ValueOf(uint64(1 << 63)),
		})

		vec := vector.NewVec(types.T_int64.ToType())
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_int64), NotNullable: true})
		require.NotNil(t, mp)
		require.ErrorContains(t, mp.mapping(page, proc, vec), "overflows BIGINT")
	})

	t.Run("double to float", func(t *testing.T) {
		f, page := writeDictAndGetPage(t, parquet.Leaf(parquet.DoubleType), []parquet.Value{
			parquet.DoubleValue(1.5),
			parquet.DoubleValue(2.25),
		})

		vec := vector.NewVec(types.T_float32.ToType())
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_float32), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		require.InDeltaSlice(t, []float32{1.5, 2.25}, vector.MustFixedColWithTypeCheck[float32](vec), 1e-6)
	})

	t.Run("decimal to double and varchar", func(t *testing.T) {
		dec := func(v int64) []byte {
			b, err := bigIntToTwosComplementBytes(ctx, big.NewInt(v), 8)
			require.NoError(t, err)
			return b
		}
		f, page := writeDictAndGetPage(t, parquet.Decimal(2, 10, parquet.FixedLenByteArrayType(8)), []parquet.Value{
			parquet.FixedLenByteArrayValue(dec(10000)),
			parquet.FixedLenByteArrayValue(dec(20050)),
		})
		col := f.Root().Column("c")

		vecFloat := vector.NewVec(types.T_float64.ToType())
		var h ParquetHandler
		mp := h.getMapper(col, plan.Type{Id: int32(types.T_float64), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vecFloat))
		require.InDeltaSlice(t, []float64{100, 200.5}, vector.MustFixedColWithTypeCheck[float64](vecFloat), 1e-9)

		vecStr := vector.NewVec(types.T_varchar.ToType())
		mp = h.getMapper(col, plan.Type{Id: int32(types.T_varchar), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vecStr))
		require.Equal(t, "100.00", vecStr.GetStringAt(0))
		require.Equal(t, "200.50", vecStr.GetStringAt(1))
	})

	t.Run("date to varchar", func(t *testing.T) {
		f, page := writeDictAndGetPage(t, parquet.Date(), []parquet.Value{
			parquet.Int32Value(19723),
			parquet.Int32Value(19875),
		})

		vec := vector.NewVec(types.T_varchar.ToType())
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_varchar), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		require.Equal(t, "2024-01-01", vec.GetStringAt(0))
		require.Equal(t, "2024-06-01", vec.GetStringAt(1))
	})
}

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

func TestParquetPrepareAllocatesByColumnIndex(t *testing.T) {
	f, _ := writeDictAndGetPage(t, parquet.Leaf(parquet.Int32Type), []parquet.Value{parquet.Int32Value(1)})

	h := &ParquetHandler{file: f}
	param := &ExternalParam{
		ExParamConst: ExParamConst{
			Ctx: context.Background(),
			Attrs: []plan.ExternAttr{
				{ColName: "c", ColIndex: 1},
			},
			Cols: []*plan.ColDef{
				{Name: "__mo_rowid", Hidden: true},
				{Name: "c", Typ: plan.Type{Id: int32(types.T_int32), NotNullable: true}},
			},
		},
	}
	require.NoError(t, h.prepare(param))
	require.Len(t, h.cols, 2)
	require.Nil(t, h.cols[0])
	require.NotNil(t, h.cols[1])
	require.Nil(t, h.mappers[0])
	require.NotNil(t, h.mappers[1])
}

func writeListAndGetPage(t *testing.T, elem parquet.Node, rows []parquet.Row) (file *parquet.File, page parquet.Page) {
	t.Helper()
	return writeListNodeAndGetPage(t, parquet.List(elem), rows)
}

func writeListNodeAndGetPage(t *testing.T, listNode parquet.Node, rows []parquet.Row) (file *parquet.File, page parquet.Page) {
	t.Helper()
	var buf bytes.Buffer
	schema := parquet.NewSchema("x", parquet.Group{
		"c": listNode,
	})
	w := parquet.NewWriter(&buf, schema)
	_, err := w.WriteRows(rows)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	col := f.Root().Column("c")
	require.NotNil(t, col)
	require.False(t, col.Leaf())
	leaf, ok := parquetListElementLeaf(col)
	require.True(t, ok)
	pg, err := leaf.Pages().ReadPage()
	require.NoError(t, err)
	return f, pg
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
	for _, tc := range []struct {
		name   string
		dt     types.T
		values []int32
	}{
		{name: "uint8", dt: types.T_uint8, values: []int32{0, 1, 128, 255}},
		{name: "int8", dt: types.T_int8, values: []int32{-128, -1, 0, 127}},
		{name: "uint16", dt: types.T_uint16, values: []int32{0, 1, 255, 65535}},
		{name: "int16", dt: types.T_int16, values: []int32{-32768, -1, 255, 32767}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := parquet.Int32Type
			page := st.NewPage(0, len(tc.values), encoding.Int32Values(tc.values))
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

			vec := vector.NewVec(types.New(tc.dt, 0, 0))
			var h ParquetHandler
			mp := h.getMapper(col, plan.Type{Id: int32(tc.dt), NotNullable: true})
			require.NotNil(t, mp, "mapper for %v", tc.dt)
			require.NoError(t, mp.mapping(page, proc, vec))
			require.Equal(t, len(tc.values), vec.Length())
		})
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
	err := h.openFile(param, false)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrNYI))
}

func TestParquetShouldPrefetchS3Parquet(t *testing.T) {
	require.True(t, shouldPrefetchS3Parquet(tree.S3, true, maxParquetS3PrefetchSize))
	require.False(t, shouldPrefetchS3Parquet(tree.S3, true, maxParquetS3PrefetchSize+1))
	require.False(t, shouldPrefetchS3Parquet(tree.S3, false, maxParquetS3PrefetchSize))
	require.False(t, shouldPrefetchS3Parquet(tree.INFILE, true, maxParquetS3PrefetchSize))
	require.False(t, shouldPrefetchS3Parquet(tree.S3, true, -1))
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

func TestParquet_ScanParquetFile_MappingErrorClosesPages(t *testing.T) {
	var buf bytes.Buffer
	schema := parquet.NewSchema("x", parquet.Group{"c": parquet.Uint(64)})
	w := parquet.NewWriter(&buf, schema)
	_, err := w.WriteRows([]parquet.Row{
		{parquet.ValueOf(uint64(1<<63)).Level(0, 0, 0)},
	})
	require.NoError(t, err)
	require.NoError(t, w.Close())

	param := &ExternalParam{
		ExParamConst: ExParamConst{
			Ctx:      context.Background(),
			Attrs:    []plan.ExternAttr{{ColName: "c", ColIndex: 0}},
			Cols:     []*plan.ColDef{{Typ: plan.Type{Id: int32(types.T_int64), NotNullable: true}}},
			Extern:   &tree.ExternParam{ExParamConst: tree.ExParamConst{ScanType: tree.INLINE}},
			FileSize: []int64{int64(buf.Len())},
		},
		ExParam: ExParam{Fileparam: &ExFileparam{FileIndex: 1, FileCnt: 1}},
	}
	param.Extern.Data = string(buf.Bytes())

	proc := testutil.NewProc(t)
	bat := vectorBatch([]types.Type{types.T_int64.ToType()})
	err = scanParquetFile(context.Background(), param, proc, bat)
	require.ErrorContains(t, err, "overflows BIGINT")
	require.NotNil(t, param.parqh)
	require.Len(t, param.parqh.pages, 1)
	require.Nil(t, param.parqh.pages[0])
	require.Nil(t, param.parqh.currentPage[0])
	require.Zero(t, param.parqh.pageOffset[0])
}

func TestParquet_ScanParquetFile_CountStarNoAttrs(t *testing.T) {
	save := maxParquetBatchCnt
	maxParquetBatchCnt = 2
	defer func() { maxParquetBatchCnt = save }()

	var buf bytes.Buffer
	st := parquet.Int32Type
	page := st.NewPage(0, 3, encoding.Int32Values([]int32{1, 2, 3}))
	schema := parquet.NewSchema("x", parquet.Group{"c": parquet.Leaf(st)})
	w := parquet.NewWriter(&buf, schema)
	values := make([]parquet.Value, page.NumRows())
	_, _ = page.Values().ReadValues(values)
	_, err := w.WriteRows([]parquet.Row{parquet.MakeRow(values)})
	require.NoError(t, err)
	require.NoError(t, w.Close())

	param := &ExternalParam{
		ExParamConst: ExParamConst{
			Ctx:      context.Background(),
			Cols:     []*plan.ColDef{{Typ: plan.Type{Id: int32(types.T_int32), NotNullable: true}}},
			Extern:   &tree.ExternParam{ExParamConst: tree.ExParamConst{ScanType: tree.INLINE}, ExParam: tree.ExParam{}},
			FileSize: []int64{int64(buf.Len())},
		},
		ExParam: ExParam{Fileparam: &ExFileparam{FileIndex: 1, FileCnt: 1}},
	}
	param.Extern.Data = string(buf.Bytes())

	proc := testutil.NewProc(t)

	var total int
	for attempts := 0; attempts < 5 && !param.Fileparam.End; attempts++ {
		bat := batch.NewWithSize(0)
		require.NoError(t, scanParquetFile(context.Background(), param, proc, bat))
		require.LessOrEqual(t, bat.RowCount(), int(maxParquetBatchCnt))
		total += bat.RowCount()
	}
	require.Equal(t, 3, total)
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

func TestParquetDecimalMapperRescalesAndChecksPrecision(t *testing.T) {
	proc := testutil.NewProc(t)
	ctx := context.Background()

	t.Run("byte_array_ascii_decimal64_uses_string_parse", func(t *testing.T) {
		node := parquet.Leaf(parquet.ByteArrayType)
		vals := []parquet.Value{parquet.ByteArrayValue([]byte("12.34")), parquet.ByteArrayValue([]byte("-5.67"))}
		f, page := writeDictAndGetPage(t, node, vals)

		vec := vector.NewVec(types.New(types.T_decimal64, 5, 2))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_decimal64), NotNullable: true, Width: 5, Scale: 2})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))

		got := vector.MustFixedColWithTypeCheck[types.Decimal64](vec)
		require.Equal(t, "12.34", got[0].Format(2))
		require.Equal(t, "-5.67", got[1].Format(2))
	})

	t.Run("dict_byte_array_ascii_decimal128_uses_string_parse", func(t *testing.T) {
		node := parquet.Encoded(parquet.Leaf(parquet.ByteArrayType), &parquet.RLEDictionary)
		vals := []parquet.Value{
			parquet.ByteArrayValue([]byte("123.45")),
			parquet.ByteArrayValue([]byte("-67.89")),
			parquet.ByteArrayValue([]byte("123.45")),
		}
		f, page := writeDictAndGetPage(t, node, vals)

		vec := vector.NewVec(types.New(types.T_decimal128, 10, 2))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_decimal128), NotNullable: true, Width: 10, Scale: 2})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))

		got := vector.MustFixedColWithTypeCheck[types.Decimal128](vec)
		require.Equal(t, "123.45", got[0].Format(2))
		require.Equal(t, "-67.89", got[1].Format(2))
		require.Equal(t, "123.45", got[2].Format(2))
	})

	t.Run("byte_array_ascii_decimal256_uses_string_parse", func(t *testing.T) {
		node := parquet.Leaf(parquet.ByteArrayType)
		vals := []parquet.Value{parquet.ByteArrayValue([]byte("12345678901234567890.123"))}
		f, page := writeDictAndGetPage(t, node, vals)

		vec := vector.NewVec(types.New(types.T_decimal256, 40, 3))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_decimal256), NotNullable: true, Width: 40, Scale: 3})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))

		got := vector.MustFixedColWithTypeCheck[types.Decimal256](vec)
		require.Equal(t, "12345678901234567890.123", got[0].Format(3))
	})

	t.Run("decimal64_source_scale_zero_to_target_scale_two", func(t *testing.T) {
		node := parquet.Decimal(0, 9, parquet.Int32Type)
		vals := []parquet.Value{parquet.Int32Value(123), parquet.Int32Value(-5)}
		f, page := writeDictAndGetPage(t, node, vals)

		vec := vector.NewVec(types.New(types.T_decimal64, 5, 2))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_decimal64), NotNullable: true, Width: 5, Scale: 2})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))

		got := vector.MustFixedColWithTypeCheck[types.Decimal64](vec)
		require.Equal(t, "123.00", got[0].Format(2))
		require.Equal(t, "-5.00", got[1].Format(2))
	})

	t.Run("decimal128_fixed_len_source_scale_two_to_target_scale_four", func(t *testing.T) {
		node := parquet.Decimal(2, 20, parquet.FixedLenByteArrayType(9))
		b, err := bigIntToTwosComplementBytes(ctx, big.NewInt(12345), 9)
		require.NoError(t, err)
		f, page := writeDictAndGetPage(t, node, []parquet.Value{parquet.FixedLenByteArrayValue(b)})

		vec := vector.NewVec(types.New(types.T_decimal128, 20, 4))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_decimal128), NotNullable: true, Width: 20, Scale: 4})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))

		got := vector.MustFixedColWithTypeCheck[types.Decimal128](vec)
		require.Equal(t, "123.4500", got[0].Format(4))
	})

	t.Run("decimal256_fixed_len_source_scale_two_to_target_scale_three", func(t *testing.T) {
		node := parquet.Decimal(2, 40, parquet.FixedLenByteArrayType(16))
		b, err := bigIntToTwosComplementBytes(ctx, big.NewInt(12345), 16)
		require.NoError(t, err)
		f, page := writeDictAndGetPage(t, node, []parquet.Value{parquet.FixedLenByteArrayValue(b)})

		vec := vector.NewVec(types.New(types.T_decimal256, 40, 3))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_decimal256), NotNullable: true, Width: 40, Scale: 3})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))

		got := vector.MustFixedColWithTypeCheck[types.Decimal256](vec)
		require.Equal(t, "123.450", got[0].Format(3))
	})

	t.Run("reject_destination_precision_overflow", func(t *testing.T) {
		node := parquet.Decimal(0, 9, parquet.Int32Type)
		f, page := writeDictAndGetPage(t, node, []parquet.Value{parquet.Int32Value(123)})

		vec := vector.NewVec(types.New(types.T_decimal64, 4, 2))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_decimal64), NotNullable: true, Width: 4, Scale: 2})
		require.NotNil(t, mp)
		require.Error(t, mp.mapping(page, proc, vec))
	})
}

func Test_parquet_decimal256FromInt64(t *testing.T) {
	d := decimal256FromInt64(-1)
	require.Equal(t, uint64(^uint64(0)), d.B64_127)
}

func Test_pageIsNull_simplePaths(t *testing.T) {
	// Build a required int32 page (no nulls)
	st := parquet.Int32Type
	page := st.NewPage(0, 2, encoding.Int32Values([]int32{1, 2}))
	mp := &columnMapper{srcNull: false, dstNull: true, maxDefinitionLevel: 0}
	b, err := mp.pageIsNull(context.Background(), page, 0)
	require.NoError(t, err)
	require.False(t, b)
	// when srcNull true but page has no nulls -> false
	mp = &columnMapper{srcNull: true, dstNull: true, maxDefinitionLevel: 0}
	b, err = mp.pageIsNull(context.Background(), page, 0)
	require.NoError(t, err)
	require.False(t, b)
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
	// Second call -> last row and the handler can finish immediately without
	// requiring another empty read to hit EOF.
	bat2 := vectorBatch([]types.Type{types.New(types.T_int32, 0, 0)})
	require.NoError(t, h.getData(bat2, param, proc))
	require.Equal(t, 1, bat2.RowCount())
	require.Nil(t, param.parqh)
	require.True(t, param.Fileparam.End)
}
