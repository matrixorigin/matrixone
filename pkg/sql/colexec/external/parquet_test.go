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
	"math"
	"math/big"
	"sync"
	"testing"
	"time"

	"io"
	"iter"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	icebergio "github.com/matrixorigin/matrixone/pkg/iceberg/io"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/encoding"
	"github.com/parquet-go/parquet-go/format"
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

func TestParquetDecimalSameScalePrecisionOverflow(t *testing.T) {
	proc := testutil.NewProc(t)
	ctx := context.Background()
	encodings := []struct {
		name       string
		dictionary bool
	}{
		{name: "plain"},
		{name: "dictionary", dictionary: true},
	}
	targets := []struct {
		name            string
		oid             types.T
		targetPrecision int32
		sourcePrecision int
		byteWidth       int
	}{
		{name: "decimal64", oid: types.T_decimal64, targetPrecision: 4, sourcePrecision: 5, byteWidth: 8},
		{name: "decimal128", oid: types.T_decimal128, targetPrecision: 19, sourcePrecision: 20, byteWidth: 16},
		{name: "decimal256", oid: types.T_decimal256, targetPrecision: 39, sourcePrecision: 40, byteWidth: 32},
	}

	for _, encoding := range encodings {
		for _, target := range targets {
			t.Run(encoding.name+"/"+target.name, func(t *testing.T) {
				overflow := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(target.targetPrecision)), nil)
				encoded, err := bigIntToTwosComplementBytes(ctx, overflow, target.byteWidth)
				require.NoError(t, err)
				node := parquet.Decimal(2, target.sourcePrecision, parquet.FixedLenByteArrayType(target.byteWidth))
				if encoding.dictionary {
					node = parquet.Encoded(node, &parquet.RLEDictionary)
				}
				f, page := writeDictAndGetPage(t, node, []parquet.Value{
					parquet.FixedLenByteArrayValue(encoded),
				})
				if encoding.dictionary {
					require.NotNil(t, page.Dictionary())
				}

				vec := vector.NewVec(types.New(target.oid, target.targetPrecision, 2))
				var h ParquetHandler
				mp := h.getMapper(f.Root().Column("c"), plan.Type{
					Id:          int32(target.oid),
					Width:       target.targetPrecision,
					Scale:       2,
					NotNullable: true,
				})
				require.NotNil(t, mp)
				err = mp.mapping(page, proc, vec)
				require.ErrorContains(t, err, fmt.Sprintf("DECIMAL(%d,2)", target.targetPrecision))
			})
		}
	}
}

func TestParquetDecimalScaleConversion(t *testing.T) {
	proc := testutil.NewProc(t)
	ctx := context.Background()
	decimalBytes := func(v int64) []byte {
		b, err := bigIntToTwosComplementBytes(ctx, big.NewInt(v), 8)
		require.NoError(t, err)
		return b
	}

	encodings := []struct {
		name       string
		dictionary bool
	}{
		{name: "plain"},
		{name: "dictionary", dictionary: true},
	}
	targets := []struct {
		name  string
		oid   types.T
		width int32
	}{
		{name: "decimal64", oid: types.T_decimal64, width: 10},
		{name: "decimal128", oid: types.T_decimal128, width: 20},
		{name: "decimal256", oid: types.T_decimal256, width: 40},
	}

	for _, encoding := range encodings {
		for _, target := range targets {
			t.Run(encoding.name+"/"+target.name, func(t *testing.T) {
				node := parquet.Decimal(3, 10, parquet.FixedLenByteArrayType(8))
				if encoding.dictionary {
					node = parquet.Encoded(node, &parquet.RLEDictionary)
				}
				f, page := writeDictAndGetPage(t, node, []parquet.Value{
					parquet.FixedLenByteArrayValue(decimalBytes(1235)),
					parquet.FixedLenByteArrayValue(decimalBytes(-1235)),
					parquet.FixedLenByteArrayValue(decimalBytes(12345)),
					parquet.FixedLenByteArrayValue(decimalBytes(1)),
				})
				if encoding.dictionary {
					require.NotNil(t, page.Dictionary())
				}

				vec := vector.NewVec(types.New(target.oid, target.width, 2))
				var h ParquetHandler
				mp := h.getMapper(f.Root().Column("c"), plan.Type{
					Id:          int32(target.oid),
					Width:       target.width,
					Scale:       2,
					NotNullable: true,
				})
				require.NotNil(t, mp)
				require.NoError(t, mp.mapping(page, proc, vec))

				neg124 := int64(-124)
				switch target.oid {
				case types.T_decimal64:
					require.Equal(t, []types.Decimal64{124, types.Decimal64(neg124), 1235, 0},
						vector.MustFixedColWithTypeCheck[types.Decimal64](vec))
				case types.T_decimal128:
					require.Equal(t, []types.Decimal128{
						decimal128FromInt64(124), decimal128FromInt64(neg124), decimal128FromInt64(1235), {},
					}, vector.MustFixedColWithTypeCheck[types.Decimal128](vec))
				case types.T_decimal256:
					require.Equal(t, []types.Decimal256{
						decimal256FromInt64(124), decimal256FromInt64(neg124), decimal256FromInt64(1235), {},
					}, vector.MustFixedColWithTypeCheck[types.Decimal256](vec))
				}
			})
		}
	}

	t.Run("source precision exceeds decimal256", func(t *testing.T) {
		tenth := new(big.Int).Exp(big.NewInt(10), big.NewInt(99), nil)
		encode := func(value *big.Int) []byte {
			b, err := bigIntToTwosComplementBytes(ctx, value, 43)
			require.NoError(t, err)
			return b
		}
		f, page := writeDictAndGetPage(t,
			parquet.Decimal(100, 100, parquet.FixedLenByteArrayType(43)),
			[]parquet.Value{
				parquet.FixedLenByteArrayValue(encode(big.NewInt(0))),
				parquet.FixedLenByteArrayValue(encode(tenth)),
				parquet.FixedLenByteArrayValue(encode(new(big.Int).Neg(tenth))),
			})

		vec := vector.NewVec(types.New(types.T_decimal64, 10, 2))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{
			Id:          int32(types.T_decimal64),
			Width:       10,
			Scale:       2,
			NotNullable: true,
		})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		neg10 := int64(-10)
		require.Equal(t, []types.Decimal64{0, 10, types.Decimal64(neg10)},
			vector.MustFixedColWithTypeCheck[types.Decimal64](vec))
	})
}

func TestParquetDecimalScaleConversionBounds(t *testing.T) {
	ctx := context.Background()
	sourceType := parquet.Decimal(2, 9, parquet.Int64Type).Type()

	t.Run("scale up", func(t *testing.T) {
		got, err := parquetDecimalValueToTargetBigInt(ctx, sourceType, parquet.Int64Value(123), 7, 4)
		require.NoError(t, err)
		require.Equal(t, "12300", got.String())
	})

	t.Run("unchanged scale", func(t *testing.T) {
		got, err := parquetDecimalValueToTargetBigInt(ctx, sourceType, parquet.Int64Value(-123), 3, 2)
		require.NoError(t, err)
		require.Equal(t, "-123", got.String())
	})

	t.Run("invalid scale", func(t *testing.T) {
		_, err := parquetDecimalValueToTargetBigInt(ctx, sourceType, parquet.Int64Value(1), 9, -1)
		require.Error(t, err)
	})

	t.Run("unsupported physical type", func(t *testing.T) {
		_, err := parquetDecimalValueToTargetBigInt(ctx, parquet.BooleanType, parquet.BooleanValue(true), 9, 0)
		require.Error(t, err)
	})

	t.Run("scale up precision overflow", func(t *testing.T) {
		_, err := parquetDecimalValueToTargetBigInt(ctx, sourceType, parquet.Int64Value(123), 4, 4)
		require.Error(t, err)
	})

	t.Run("rounding precision overflow", func(t *testing.T) {
		st := parquet.Decimal(1, 3, parquet.Int32Type).Type()
		_, err := parquetDecimalValueToTargetBigInt(ctx, st, parquet.Int32Value(999), 2, 0)
		require.Error(t, err)
	})

	encode := func(value *big.Int) parquet.Value {
		b, err := bigIntToTwosComplementBytes(ctx, value, 43)
		require.NoError(t, err)
		return parquet.FixedLenByteArrayValue(b)
	}
	wideSourceType := parquet.Decimal(0, 100, parquet.FixedLenByteArrayType(43)).Type()

	t.Run("decimal64 storage overflow", func(t *testing.T) {
		_, err := parquetDecimalValueToDecimal64(ctx, wideSourceType,
			encode(new(big.Int).Lsh(big.NewInt(1), 63)), 100, 0)
		require.Error(t, err)
	})

	t.Run("decimal128 storage overflow", func(t *testing.T) {
		_, err := parquetDecimalValueToDecimal128(ctx, wideSourceType,
			encode(new(big.Int).Lsh(big.NewInt(1), 127)), 100, 0)
		require.Error(t, err)
	})

	t.Run("decimal256 storage overflow", func(t *testing.T) {
		_, err := parquetDecimalValueToDecimal256(ctx, wideSourceType,
			encode(new(big.Int).Lsh(big.NewInt(1), 255)), 100, 0)
		require.Error(t, err)
	})

	t.Run("target conversion error", func(t *testing.T) {
		_, err := parquetDecimalValueToDecimal64(ctx, parquet.BooleanType, parquet.BooleanValue(true), 9, 0)
		require.Error(t, err)
		_, err = parquetDecimalValueToDecimal128(ctx, parquet.BooleanType, parquet.BooleanValue(true), 9, 0)
		require.Error(t, err)
		_, err = parquetDecimalValueToDecimal256(ctx, parquet.BooleanType, parquet.BooleanValue(true), 9, 0)
		require.Error(t, err)
	})
}

func TestParquetOpenFileUsesIcebergObjectIORef(t *testing.T) {
	ctx := context.Background()
	var buf bytes.Buffer
	schema := parquet.NewSchema("orders", parquet.Group{
		"id": parquet.FieldID(parquet.Leaf(parquet.Int32Type), 1),
	})
	w := parquet.NewWriter(&buf, schema)
	_, err := w.WriteRows([]parquet.Row{{
		parquet.Int32Value(7).Level(0, 0, 0),
	}})
	require.NoError(t, err)
	require.NoError(t, w.Close())

	fs, err := fileservice.NewMemoryFS("iceberg-data-file-reader", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	readPath := "data/orders.parquet"
	require.NoError(t, fs.Write(ctx, fileservice.IOVector{
		FilePath: readPath,
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   int64(buf.Len()),
			Data:   append([]byte(nil), buf.Bytes()...),
		}},
	}))

	ref, err := icebergio.RegisterObjectIOProvider(ctx, icebergio.ScopedProvider{FileService: fs}, func(location string) icebergio.ObjectScope {
		return icebergio.ObjectScope{
			AccountID:       42,
			CatalogID:       7,
			StorageLocation: readPath,
			Endpoint:        "s3.me-central-1.amazonaws.com",
			Region:          "me-central-1",
			Bucket:          "warehouse",
			Principal:       "ksa-analytics",
		}
	}, time.Minute)
	require.NoError(t, err)
	defer icebergio.ReleaseObjectIORef(ref)

	param := &ExternalParam{
		ExParamConst: ExParamConst{
			Ctx:                ctx,
			FileSize:           []int64{int64(buf.Len())},
			IcebergObjectIORef: ref,
			Attrs: []plan.ExternAttr{
				{ColName: "id", ColIndex: 0},
			},
			Cols: []*plan.ColDef{
				{Name: "id", Typ: plan.Type{Id: int32(types.T_int32)}},
			},
			IcebergColumns: []*pipeline.IcebergColumnMapping{
				{
					MoColIndex:        0,
					IcebergFieldId:    1,
					SnapshotFieldName: "id",
					CurrentFieldName:  "id",
				},
			},
			IcebergSnapshot: &pipeline.IcebergSnapshotRuntime{SnapshotId: 123},
			Extern: &tree.ExternParam{
				ExParamConst: tree.ExParamConst{ScanType: tree.S3, Format: tree.PARQUET},
				ExParam:      tree.ExParam{ExternType: int32(plan.ExternType_ICEBERG_TB)},
			},
		},
		ExParam: ExParam{Fileparam: &ExFileparam{
			FileIndex: 1,
			FileCnt:   1,
			Filepath:  "s3://warehouse/orders.parquet",
		}},
	}

	h, err := newParquetHandler(param)
	require.NoError(t, err)
	require.NotNil(t, h)
	require.Equal(t, int64(1), h.file.NumRows())
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

func TestParquetStringToVectorMapping(t *testing.T) {
	proc := testutil.NewProc(t)

	t.Run("optional string to vecf32", func(t *testing.T) {
		f, page := writeColumnAndGetPage(t, parquet.Optional(parquet.String()), []parquet.Row{
			{parquet.ByteArrayValue([]byte("[0.1,0.2,0.3]")).Level(0, 1, 0)},
			{parquet.NullValue().Level(0, 0, 0)},
			{parquet.ByteArrayValue([]byte(" [1, 2, 3] ")).Level(0, 1, 0)},
		})

		vec := vector.NewVec(types.New(types.T_array_float32, 3, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_array_float32), Width: 3})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))

		require.Equal(t, 3, vec.Length())
		require.False(t, vec.GetNulls().Contains(0))
		require.True(t, vec.GetNulls().Contains(1))
		require.False(t, vec.GetNulls().Contains(2))
		require.Equal(t, []float32{0.1, 0.2, 0.3}, vector.GetArrayAt[float32](vec, 0))
		require.Equal(t, []float32{1, 2, 3}, vector.GetArrayAt[float32](vec, 2))
	})

	t.Run("dictionary string to vecf64", func(t *testing.T) {
		f, page := writeDictAndGetPage(t, parquet.Encoded(parquet.String(), &parquet.RLEDictionary), []parquet.Value{
			parquet.ByteArrayValue([]byte("[1.25,2.25,3.25]")),
			parquet.ByteArrayValue([]byte("[4.5,5.5,6.5]")),
			parquet.ByteArrayValue([]byte("[1.25,2.25,3.25]")),
		})

		vec := vector.NewVec(types.New(types.T_array_float64, 3, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_array_float64), Width: 3, NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))

		require.Equal(t, [][]float64{
			{1.25, 2.25, 3.25},
			{4.5, 5.5, 6.5},
			{1.25, 2.25, 3.25},
		}, vector.MustArrayCol[float64](vec))
	})

	t.Run("raw byte array to vecf32", func(t *testing.T) {
		f, page := writeColumnAndGetPage(t, parquet.Leaf(parquet.ByteArrayType), []parquet.Row{
			{parquet.ByteArrayValue([]byte("[7,8,9]")).Level(0, 0, 0)},
		})

		vec := vector.NewVec(types.New(types.T_array_float32, 3, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_array_float32), Width: 3, NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		require.Equal(t, []float32{7, 8, 9}, vector.GetArrayAt[float32](vec, 0))
	})

	t.Run("fixed len byte array to vecf64", func(t *testing.T) {
		f, page := writeColumnAndGetPage(t, parquet.Leaf(parquet.FixedLenByteArrayType(7)), []parquet.Row{
			{parquet.FixedLenByteArrayValue([]byte("[1,2,3]")).Level(0, 0, 0)},
		})

		vec := vector.NewVec(types.New(types.T_array_float64, 3, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_array_float64), Width: 3, NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		require.Equal(t, []float64{1, 2, 3}, vector.GetArrayAt[float64](vec, 0))
	})

	t.Run("empty page", func(t *testing.T) {
		page := parquet.ByteArrayType.NewPage(0, 0, encoding.ByteArrayValues(nil, []uint32{0}))
		vec := vector.NewVec(types.New(types.T_array_float32, 3, 0))
		require.NoError(t, processStringToArray[float32](context.Background(), &columnMapper{}, page, proc, vec, 3))
		require.Equal(t, 0, vec.Length())
	})

	t.Run("invalid target width", func(t *testing.T) {
		_, page := writeColumnAndGetPage(t, parquet.String(), []parquet.Row{
			{parquet.ByteArrayValue([]byte("[1,2,3]")).Level(0, 0, 0)},
		})

		vec := vector.NewVec(types.T_array_float32.ToType())
		require.ErrorContains(t, processStringToArray[float32](context.Background(), &columnMapper{}, page, proc, vec, 0), "invalid vector dimension 0")
	})

	t.Run("non string source is not vector input", func(t *testing.T) {
		f, _ := writeColumnAndGetPage(t, parquet.Leaf(parquet.Int32Type), []parquet.Row{
			{parquet.Int32Value(1).Level(0, 0, 0)},
		})

		var h ParquetHandler
		require.Nil(t, h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_array_float64), Width: 3, NotNullable: true}))
	})

	t.Run("dimension mismatch", func(t *testing.T) {
		f, page := writeColumnAndGetPage(t, parquet.String(), []parquet.Row{
			{parquet.ByteArrayValue([]byte("[1,2]")).Level(0, 0, 0)},
		})

		vec := vector.NewVec(types.New(types.T_array_float32, 3, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_array_float32), Width: 3, NotNullable: true})
		require.NotNil(t, mp)
		require.ErrorContains(t, mp.mapping(page, proc, vec), "expected vector dimension 3 != actual dimension 2")
	})

	t.Run("malformed vector text", func(t *testing.T) {
		f, page := writeColumnAndGetPage(t, parquet.String(), []parquet.Row{
			{parquet.ByteArrayValue([]byte("not-a-vector")).Level(0, 0, 0)},
		})

		vec := vector.NewVec(types.New(types.T_array_float64, 3, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_array_float64), Width: 3, NotNullable: true})
		require.NotNil(t, mp)
		require.ErrorContains(t, mp.mapping(page, proc, vec), "malformed vector input")
	})

	t.Run("json logical type vector text", func(t *testing.T) {
		f, page := writeColumnAndGetPage(t, parquet.JSON(), []parquet.Row{
			{parquet.ByteArrayValue([]byte("[1,2,3]")).Level(0, 0, 0)},
		})

		vec := vector.NewVec(types.New(types.T_array_float32, 3, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_array_float32), Width: 3, NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		require.Equal(t, []float32{1, 2, 3}, vector.GetArrayAt[float32](vec, 0))
	})

	t.Run("empty vector text with variable width", func(t *testing.T) {
		f, page := writeColumnAndGetPage(t, parquet.String(), []parquet.Row{
			{parquet.ByteArrayValue([]byte("[]")).Level(0, 0, 0)},
			{parquet.ByteArrayValue([]byte("[1,2]")).Level(0, 0, 0)},
		})

		vec := vector.NewVec(types.T_array_float64.ToType())
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_array_float64), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		rows := vector.MustArrayCol[float64](vec)
		require.Empty(t, rows[0])
		require.Equal(t, []float64{1, 2}, rows[1])
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
		leaf, mp := h.getNestedListMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_array_float32), Width: 3})
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
		leaf, mp := h.getNestedListMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_array_float64), Width: 3})
		require.NotNil(t, leaf)
		require.NotNil(t, mp)

		vec := vector.NewVec(types.New(types.T_array_float64, 3, 0))
		require.NoError(t, mp.mapping(page, proc, vec))
		require.Equal(t, [][]float64{{1.25, 2.25, 3.25}, {4.25, 5.25, 6.25}}, vector.MustArrayCol[float64](vec))
	})

	t.Run("float list to vecf64", func(t *testing.T) {
		f, page := writeListAndGetPage(t, parquet.Leaf(parquet.FloatType), []parquet.Row{{
			parquet.FloatValue(1.5).Level(0, 1, 0),
			parquet.FloatValue(2.5).Level(1, 1, 0),
			parquet.FloatValue(3.5).Level(1, 1, 0),
		}})

		var h ParquetHandler
		leaf, mp := h.getNestedListMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_array_float64), Width: 3})
		require.NotNil(t, leaf)
		require.NotNil(t, mp)

		vec := vector.NewVec(types.New(types.T_array_float64, 3, 0))
		require.NoError(t, mp.mapping(page, proc, vec))
		require.Equal(t, [][]float64{{1.5, 2.5, 3.5}}, vector.MustArrayCol[float64](vec))
	})

	t.Run("double list to vecf32", func(t *testing.T) {
		f, page := writeListAndGetPage(t, parquet.Leaf(parquet.DoubleType), []parquet.Row{{
			parquet.DoubleValue(1.25).Level(0, 1, 0),
			parquet.DoubleValue(2.25).Level(1, 1, 0),
			parquet.DoubleValue(3.25).Level(1, 1, 0),
		}})

		var h ParquetHandler
		leaf, mp := h.getNestedListMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_array_float32), Width: 3})
		require.NotNil(t, leaf)
		require.NotNil(t, mp)

		vec := vector.NewVec(types.New(types.T_array_float32, 3, 0))
		require.NoError(t, mp.mapping(page, proc, vec))
		require.Equal(t, [][]float32{{1.25, 2.25, 3.25}}, vector.MustArrayCol[float32](vec))
	})

	t.Run("double list to vecf32 rejects overflow", func(t *testing.T) {
		for _, tc := range []struct {
			name  string
			value float64
		}{
			{name: "positive", value: 1e100},
			{name: "negative", value: -1e100},
		} {
			t.Run(tc.name, func(t *testing.T) {
				f, page := writeListAndGetPage(t, parquet.Leaf(parquet.DoubleType), []parquet.Row{{
					parquet.DoubleValue(tc.value).Level(0, 1, 0),
				}})

				var h ParquetHandler
				_, mp := h.getNestedListMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_array_float32), Width: 1})
				require.NotNil(t, mp)

				vec := vector.NewVec(types.New(types.T_array_float32, 1, 0))
				require.ErrorContains(t, mp.mapping(page, proc, vec), "overflows FLOAT")
			})
		}
	})

	t.Run("dimension mismatch", func(t *testing.T) {
		f, page := writeListAndGetPage(t, parquet.Leaf(parquet.FloatType), []parquet.Row{
			{
				parquet.FloatValue(1).Level(0, 1, 0),
				parquet.FloatValue(2).Level(1, 1, 0),
			},
		})

		var h ParquetHandler
		_, mp := h.getNestedListMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_array_float32), Width: 3})
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
		_, mp := h.getNestedListMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_array_float32)})
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
		_, mp := h.getNestedListMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_array_float32)})
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
		leaf, mp := h.getNestedListMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_array_float32)})
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
		_, mp := h.getNestedListMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_array_float32), Width: 3})
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
		_, mp := h.getNestedListMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_array_float32), Width: 3})
		require.NotNil(t, mp)

		vec := vector.NewVec(types.New(types.T_array_float32, 3, 0))
		require.ErrorContains(t, mp.mapping(page, proc, vec), "parquet list NULL elements are not supported")
	})

	// Narrow vector targets: bf16/f16 decode from FLOAT leaves, int8/uint8 from
	// INT32 leaves. Values are chosen exactly representable so the round-trip is
	// loss-free and can be asserted by exact equality.
	t.Run("float list to vecbf16", func(t *testing.T) {
		f, page := writeListAndGetPage(t, parquet.Leaf(parquet.FloatType), []parquet.Row{
			{
				parquet.FloatValue(1).Level(0, 1, 0),
				parquet.FloatValue(2).Level(1, 1, 0),
				parquet.FloatValue(-0.5).Level(1, 1, 0),
			},
		})

		var h ParquetHandler
		leaf, mp := h.getNestedListMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_array_bf16), Width: 3})
		require.NotNil(t, leaf)
		require.NotNil(t, mp)

		vec := vector.NewVec(types.New(types.T_array_bf16, 3, 0))
		require.NoError(t, mp.mapping(page, proc, vec))
		require.Equal(t, 1, vec.Length())
		require.Equal(t, []types.BF16{
			types.BF16FromFloat32(1), types.BF16FromFloat32(2), types.BF16FromFloat32(-0.5),
		}, vector.GetArrayAt[types.BF16](vec, 0))
	})

	t.Run("float list to vecf16", func(t *testing.T) {
		f, page := writeListAndGetPage(t, parquet.Leaf(parquet.FloatType), []parquet.Row{
			{
				parquet.FloatValue(0.5).Level(0, 1, 0),
				parquet.FloatValue(0.25).Level(1, 1, 0),
				parquet.FloatValue(4).Level(1, 1, 0),
			},
		})

		var h ParquetHandler
		leaf, mp := h.getNestedListMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_array_float16), Width: 3})
		require.NotNil(t, leaf)
		require.NotNil(t, mp)

		vec := vector.NewVec(types.New(types.T_array_float16, 3, 0))
		require.NoError(t, mp.mapping(page, proc, vec))
		require.Equal(t, 1, vec.Length())
		require.Equal(t, []types.Float16{
			types.Float16FromFloat32(0.5), types.Float16FromFloat32(0.25), types.Float16FromFloat32(4),
		}, vector.GetArrayAt[types.Float16](vec, 0))
	})

	t.Run("int32 list to vecint8", func(t *testing.T) {
		f, page := writeListAndGetPage(t, parquet.Leaf(parquet.Int32Type), []parquet.Row{
			{
				parquet.Int32Value(-128).Level(0, 1, 0),
				parquet.Int32Value(0).Level(1, 1, 0),
				parquet.Int32Value(127).Level(1, 1, 0),
			},
		})

		var h ParquetHandler
		leaf, mp := h.getNestedListMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_array_int8), Width: 3})
		require.NotNil(t, leaf)
		require.NotNil(t, mp)

		vec := vector.NewVec(types.New(types.T_array_int8, 3, 0))
		require.NoError(t, mp.mapping(page, proc, vec))
		require.Equal(t, 1, vec.Length())
		require.Equal(t, []int8{-128, 0, 127}, vector.GetArrayAt[int8](vec, 0))
	})

	t.Run("int32 list to vecuint8", func(t *testing.T) {
		f, page := writeListAndGetPage(t, parquet.Leaf(parquet.Int32Type), []parquet.Row{
			{
				parquet.Int32Value(0).Level(0, 1, 0),
				parquet.Int32Value(128).Level(1, 1, 0),
				parquet.Int32Value(255).Level(1, 1, 0),
			},
		})

		var h ParquetHandler
		leaf, mp := h.getNestedListMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_array_uint8), Width: 3})
		require.NotNil(t, leaf)
		require.NotNil(t, mp)

		vec := vector.NewVec(types.New(types.T_array_uint8, 3, 0))
		require.NoError(t, mp.mapping(page, proc, vec))
		require.Equal(t, 1, vec.Length())
		require.Equal(t, []uint8{0, 128, 255}, vector.GetArrayAt[uint8](vec, 0))
	})

	t.Run("vecint8 out of range rejected", func(t *testing.T) {
		f, page := writeListAndGetPage(t, parquet.Leaf(parquet.Int32Type), []parquet.Row{
			{
				parquet.Int32Value(200).Level(0, 1, 0),
				parquet.Int32Value(0).Level(1, 1, 0),
				parquet.Int32Value(0).Level(1, 1, 0),
			},
		})

		var h ParquetHandler
		_, mp := h.getNestedListMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_array_int8), Width: 3})
		require.NotNil(t, mp)

		vec := vector.NewVec(types.New(types.T_array_int8, 3, 0))
		require.ErrorContains(t, mp.mapping(page, proc, vec), "out of range")
	})

	t.Run("vecuint8 out of range rejected", func(t *testing.T) {
		f, page := writeListAndGetPage(t, parquet.Leaf(parquet.Int32Type), []parquet.Row{
			{
				parquet.Int32Value(-1).Level(0, 1, 0),
				parquet.Int32Value(0).Level(1, 1, 0),
				parquet.Int32Value(0).Level(1, 1, 0),
			},
		})

		var h ParquetHandler
		_, mp := h.getNestedListMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_array_uint8), Width: 3})
		require.NotNil(t, mp)

		vec := vector.NewVec(types.New(types.T_array_uint8, 3, 0))
		require.ErrorContains(t, mp.mapping(page, proc, vec), "out of range")
	})

	t.Run("vecbf16 rejects int32 leaf", func(t *testing.T) {
		f, _ := writeListAndGetPage(t, parquet.Leaf(parquet.Int32Type), []parquet.Row{
			{parquet.Int32Value(1).Level(0, 1, 0)},
		})

		var h ParquetHandler
		leaf, mp := h.getNestedListMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_array_bf16), Width: 1})
		require.Nil(t, leaf)
		require.Nil(t, mp)
	})

	t.Run("vecint8 rejects float leaf", func(t *testing.T) {
		f, _ := writeListAndGetPage(t, parquet.Leaf(parquet.FloatType), []parquet.Row{
			{parquet.FloatValue(1).Level(0, 1, 0)},
		})

		var h ParquetHandler
		leaf, mp := h.getNestedListMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_array_int8), Width: 1})
		require.Nil(t, leaf)
		require.Nil(t, mp)
	})
}

func TestParquetListMapperRejectsValueKindMismatch(t *testing.T) {
	proc := testutil.NewProc(t)
	f, page := writeListAndGetPage(t, parquet.Leaf(parquet.FloatType), []parquet.Row{
		{parquet.FloatValue(1).Level(0, 1, 0)},
	})
	var h ParquetHandler
	_, mp := h.getNestedListMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_array_float32), Width: 1})
	require.NotNil(t, mp)

	badPage := &parquetPageWithData{Page: page, data: encoding.Int32Values([]int32{1})}
	vec := vector.NewVec(types.New(types.T_array_float32, 0, 0))
	err := mp.mapping(badPage, proc, vec)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
	require.Contains(t, err.Error(), "expected FLOAT")
	require.Zero(t, vec.Length())
}

func TestParquetListMapperRejectsRepetitionLevelOverflow(t *testing.T) {
	proc := testutil.NewProc(t)
	f, page := writeListAndGetPage(t, parquet.Leaf(parquet.FloatType), []parquet.Row{
		{
			parquet.FloatValue(1).Level(0, 1, 0),
			parquet.FloatValue(2).Level(1, 1, 0),
		},
	})
	var h ParquetHandler
	_, mp := h.getNestedListMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_array_float32), Width: 2})
	require.NotNil(t, mp)

	badValues := []parquet.Value{
		parquet.FloatValue(1).Level(0, 1, 0),
		parquet.FloatValue(2).Level(2, 1, 0),
	}
	badPage := &parquetPageWithValues{
		Page: page,
		values: parquet.ValueReaderFunc(func(dst []parquet.Value) (int, error) {
			return copy(dst, badValues), nil
		}),
	}
	vec := vector.NewVec(types.New(types.T_array_float32, 0, 0))
	err := mp.mapping(badPage, proc, vec)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
	require.Contains(t, err.Error(), "repetition level 2 exceeds maximum 1")
	require.Zero(t, vec.Length())
}

func TestParquetListMapperRejectsDefinitionLevelNullnessMismatch(t *testing.T) {
	proc := testutil.NewProc(t)
	f, page := writeListAndGetPage(t, parquet.Leaf(parquet.FloatType), []parquet.Row{
		{parquet.FloatValue(1).Level(0, 1, 0)},
	})
	var h ParquetHandler
	_, mp := h.getNestedListMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_array_float32), Width: 1})
	require.NotNil(t, mp)

	badValues := []parquet.Value{parquet.FloatValue(1).Level(0, 0, 0)}
	badPage := &parquetPageWithValues{
		Page: page,
		values: parquet.ValueReaderFunc(func(dst []parquet.Value) (int, error) {
			return copy(dst, badValues), nil
		}),
	}
	vec := vector.NewVec(types.New(types.T_array_float32, 0, 0))
	err := mp.mapping(badPage, proc, vec)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
	require.Contains(t, err.Error(), "NULL status")
	require.Zero(t, vec.Length())
}

func TestParquetValueNullnessMustMatchDefinitionLevels(t *testing.T) {
	proc := testutil.NewProc(t)
	page := parquet.Int32Type.NewPage(0, 1, encoding.Int32Values([]int32{1}))
	badPage := &parquetPageWithValues{
		Page: page,
		values: parquet.ValueReaderFunc(func(dst []parquet.Value) (int, error) {
			dst[0] = parquet.NullValue().Level(0, 0, 0)
			return 1, nil
		}),
	}
	vec := vector.NewVec(types.T_int32.ToType())
	err := processParquetValuesToFixed[int32](context.Background(),
		&columnMapper{srcNull: false, dstNull: true}, badPage, proc, vec, 0,
		func(v parquet.Value) (int32, error) { return v.Int32(), nil })
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
	require.Contains(t, err.Error(), "value NULL status disagrees")
	require.Zero(t, vec.Length())
}

func TestParquetOptionalNoNullPageRetainsDefinitionLevel(t *testing.T) {
	proc := testutil.NewProc(t)
	f, page := writeColumnAndGetPage(t, parquet.Optional(parquet.Leaf(parquet.BooleanType)), []parquet.Row{
		{parquet.BooleanValue(true).Level(0, 1, 0)},
		{parquet.BooleanValue(false).Level(0, 1, 0)},
	})

	var h ParquetHandler
	mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_bool)})
	require.NotNil(t, mp)
	vec := vector.NewVec(types.T_bool.ToType())
	require.NoError(t, mp.mapping(page, proc, vec))
	require.Equal(t, []bool{true, false}, vector.MustFixedColWithTypeCheck[bool](vec))
	require.False(t, vec.GetNulls().Contains(0))
	require.False(t, vec.GetNulls().Contains(1))
}

func TestParquetGenericMappingRejectsValueKindMismatch(t *testing.T) {
	proc := testutil.NewProc(t)
	page := parquet.Int32Type.NewPage(0, 1, encoding.Int32Values([]int32{1}))
	badPage := &parquetPageWithValues{
		Page: page,
		values: parquet.ValueReaderFunc(func(dst []parquet.Value) (int, error) {
			dst[0] = parquet.BooleanValue(true)
			return 1, nil
		}),
	}
	vec := vector.NewVec(types.T_int32.ToType())
	err := processParquetValuesToFixed[int32](context.Background(), &columnMapper{}, badPage, proc, vec, 0,
		func(v parquet.Value) (int32, error) { return v.Int32(), nil })
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
	require.Contains(t, err.Error(), "value kind BOOLEAN")
	require.Zero(t, vec.Length())
}

func TestParquetGenericMappingRejectsValueLevelMismatch(t *testing.T) {
	proc := testutil.NewProc(t)
	page := parquet.Int32Type.NewPage(0, 1, encoding.Int32Values([]int32{1}))
	for _, tc := range []struct {
		name  string
		value parquet.Value
		want  string
	}{
		{name: "definition", value: parquet.Int32Value(1).Level(0, 1, 0), want: "definition level"},
		{name: "repetition", value: parquet.Int32Value(1).Level(1, 0, 0), want: "repetition level"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			badPage := &parquetPageWithValues{
				Page: page,
				values: parquet.ValueReaderFunc(func(dst []parquet.Value) (int, error) {
					dst[0] = tc.value
					return 1, nil
				}),
			}
			vec := vector.NewVec(types.T_int32.ToType())
			err := processParquetValuesToFixed[int32](context.Background(), &columnMapper{}, badPage, proc, vec, 0,
				func(v parquet.Value) (int32, error) { return v.Int32(), nil })
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
			require.Contains(t, err.Error(), tc.want)
			require.Zero(t, vec.Length())
		})
	}
}

func TestParquetGenericDictionaryMappingRejectsOutOfRangeIndex(t *testing.T) {
	proc := testutil.NewProc(t)
	f, page := writeColumnAndGetPage(t, parquet.Leaf(parquet.FloatType), []parquet.Row{
		{parquet.FloatValue(1).Level(0, 0, 0)},
	})
	dict := parquet.FloatType.NewDictionary(0, 1, encoding.FloatValues([]float32{1}))
	badData := &parquetPageWithData{Page: page, data: encoding.Int32Values([]int32{1})}
	badValues := &parquetPageWithValues{
		Page: badData,
		values: parquet.ValueReaderFunc(func(dst []parquet.Value) (int, error) {
			dict.Lookup([]int32{1}, dst[:1])
			return 1, nil
		}),
	}
	badPage := &parquetPageWithDictionary{Page: badValues, dictionary: dict}

	var h ParquetHandler
	mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_int32), NotNullable: true})
	require.NotNil(t, mp)
	vec := vector.NewVec(types.T_int32.ToType())
	require.NotPanics(t, func() {
		err := mp.mapping(badPage, proc, vec)
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
		require.Contains(t, err.Error(), "dictionary index 1 out of range")
	})
	require.Zero(t, vec.Length())
}

func TestParquetCrossTypeMappings(t *testing.T) {
	proc := testutil.NewProc(t, testutil.WithFileService(nil))
	t.Cleanup(func() {
		bytes, objects := proc.Mp().OnHeapOutstanding()
		require.Equal(t, [3]int64{}, [3]int64{proc.Mp().CurrNB(), bytes, objects}, "mapping vectors must be released before process cleanup")
	})
	mapScalar := func(t *testing.T, col *parquet.Column, page parquet.Page, target plan.Type, vecType types.Type) *vector.Vector {
		t.Helper()
		vec := vector.NewVec(vecType)
		t.Cleanup(func() { vec.Free(proc.Mp()) })
		var h ParquetHandler
		mapper := h.getMapper(col, target)
		require.NotNil(t, mapper)
		require.NoError(t, mapper.mapping(page, proc, vec))
		require.Equal(t, vecType, *vec.GetType())
		require.True(t, vec.GetNulls().IsEmpty(), "required mapping produced NULL")
		require.Equal(t, int(page.NumValues()), vec.Length())
		return vec
	}
	requireJSONAt := func(t *testing.T, vec *vector.Vector, row int, expected string) {
		t.Helper()
		got := types.DecodeJson(vec.GetBytesAt(row))
		require.Equal(t, expected, got.String())
	}

	t.Run("bool to tinyint and varchar", func(t *testing.T) {
		values := []parquet.Value{parquet.BooleanValue(true), parquet.BooleanValue(false)}
		f, page := writeDictAndGetPage(t, parquet.Leaf(parquet.BooleanType), values)
		require.Equal(t, false, page.Dictionary() != nil, "fixture encoding changed")
		col := f.Root().Column("c")

		vecInt := mapScalar(t, col, page, plan.Type{Id: int32(types.T_int8), NotNullable: true}, types.T_int8.ToType())
		require.Equal(t, []int8{1, 0}, vector.MustFixedColWithTypeCheck[int8](vecInt))

		vecStr := mapScalar(t, col, page, plan.Type{Id: int32(types.T_varchar), NotNullable: true}, types.T_varchar.ToType())
		require.Equal(t, "true", vecStr.GetStringAt(0))
		require.Equal(t, "false", vecStr.GetStringAt(1))
	})
	t.Run("bool to float json and decimal", func(t *testing.T) {
		f, page := writeDictAndGetPage(t, parquet.Leaf(parquet.BooleanType), []parquet.Value{
			parquet.BooleanValue(true),
			parquet.BooleanValue(false),
		})
		require.Equal(t, false, page.Dictionary() != nil, "fixture encoding changed")
		col := f.Root().Column("c")

		vecFloat32 := mapScalar(t, col, page, plan.Type{Id: int32(types.T_float32), NotNullable: true}, types.T_float32.ToType())
		require.Equal(t, []float32{1, 0}, vector.MustFixedColWithTypeCheck[float32](vecFloat32))

		vecFloat := mapScalar(t, col, page, plan.Type{Id: int32(types.T_float64), NotNullable: true}, types.T_float64.ToType())
		require.Equal(t, []float64{1, 0}, vector.MustFixedColWithTypeCheck[float64](vecFloat))

		vecJSON := mapScalar(t, col, page, plan.Type{Id: int32(types.T_json), NotNullable: true}, types.T_json.ToType())
		requireJSONAt(t, vecJSON, 0, `true`)
		requireJSONAt(t, vecJSON, 1, `false`)

		vecDec := mapScalar(t, col, page, plan.Type{Id: int32(types.T_decimal128), Width: 10, Scale: 2, NotNullable: true}, types.New(types.T_decimal128, 10, 2))
		wantTrue := types.Decimal128{B0_63: 100}
		wantFalse := types.Decimal128{B0_63: 0}
		require.Equal(t, []types.Decimal128{wantTrue, wantFalse}, vector.MustFixedColWithTypeCheck[types.Decimal128](vecDec))
	})
	t.Run("int32 to bool json and enum", func(t *testing.T) {
		f, page := writeDictAndGetPage(t, parquet.Leaf(parquet.Int32Type), []parquet.Value{
			parquet.Int32Value(1),
			parquet.Int32Value(0),
			parquet.Int32Value(2),
		})
		require.Equal(t, false, page.Dictionary() != nil, "fixture encoding changed")
		col := f.Root().Column("c")

		vecBool := mapScalar(t, col, page, plan.Type{Id: int32(types.T_bool), NotNullable: true}, types.T_bool.ToType())
		require.Equal(t, []bool{true, false, true}, vector.MustFixedColWithTypeCheck[bool](vecBool))

		vecJSON := mapScalar(t, col, page, plan.Type{Id: int32(types.T_json), NotNullable: true}, types.T_json.ToType())
		requireJSONAt(t, vecJSON, 0, `1`)
		requireJSONAt(t, vecJSON, 1, `0`)
		requireJSONAt(t, vecJSON, 2, `2`)

		fEnum, pageEnum := writeDictAndGetPage(t, parquet.Leaf(parquet.Int32Type), []parquet.Value{
			parquet.Int32Value(1),
			parquet.Int32Value(2),
			parquet.Int32Value(3),
		})
		require.Equal(t, false, pageEnum.Dictionary() != nil, "fixture encoding changed")
		vecEnum := mapScalar(t, fEnum.Root().Column("c"), pageEnum, plan.Type{Id: int32(types.T_enum), Enumvalues: "red,green,blue", NotNullable: true}, types.T_enum.ToType())
		require.Equal(t, []types.Enum{1, 2, 3}, vector.MustFixedColWithTypeCheck[types.Enum](vecEnum))
		require.True(t, vecEnum.GetNulls().IsEmpty())
	})
	t.Run("float to bool int64 json and decimal", func(t *testing.T) {
		f, page := writeDictAndGetPage(t, parquet.Leaf(parquet.FloatType), []parquet.Value{
			parquet.FloatValue(1.4),
			parquet.FloatValue(1.5),
			parquet.FloatValue(0),
		})
		require.Equal(t, false, page.Dictionary() != nil, "fixture encoding changed")
		col := f.Root().Column("c")

		vecBool := mapScalar(t, col, page, plan.Type{Id: int32(types.T_bool), NotNullable: true}, types.T_bool.ToType())
		require.Equal(t, []bool{true, true, false}, vector.MustFixedColWithTypeCheck[bool](vecBool))

		vecInt := mapScalar(t, col, page, plan.Type{Id: int32(types.T_int64), NotNullable: true}, types.T_int64.ToType())
		require.Equal(t, []int64{1, 2, 0}, vector.MustFixedColWithTypeCheck[int64](vecInt))

		vecJSON := mapScalar(t, col, page, plan.Type{Id: int32(types.T_json), NotNullable: true}, types.T_json.ToType())
		requireJSONAt(t, vecJSON, 0, `1.4`)
		requireJSONAt(t, vecJSON, 1, `1.5`)
		requireJSONAt(t, vecJSON, 2, `0`)

		vecDec := mapScalar(t, col, page, plan.Type{Id: int32(types.T_decimal128), Width: 10, Scale: 2, NotNullable: true}, types.New(types.T_decimal128, 10, 2))
		want0 := types.Decimal128{B0_63: 140}
		want1 := types.Decimal128{B0_63: 150}
		want2 := types.Decimal128{B0_63: 0}
		require.Equal(t, []types.Decimal128{want0, want1, want2}, vector.MustFixedColWithTypeCheck[types.Decimal128](vecDec))
	})
	t.Run("rounded numeric to integer widths", func(t *testing.T) {
		fSigned, pageSigned := writeDictAndGetPage(t, parquet.Leaf(parquet.DoubleType), []parquet.Value{
			parquet.DoubleValue(1.5),
			parquet.DoubleValue(-2.5),
		})
		require.Equal(t, false, pageSigned.Dictionary() != nil, "fixture encoding changed")
		colSigned := fSigned.Root().Column("c")

		vecInt8 := mapScalar(t, colSigned, pageSigned, plan.Type{Id: int32(types.T_int8), NotNullable: true}, types.T_int8.ToType())
		require.Equal(t, []int8{2, -3}, vector.MustFixedColWithTypeCheck[int8](vecInt8))

		vecInt16 := mapScalar(t, colSigned, pageSigned, plan.Type{Id: int32(types.T_int16), NotNullable: true}, types.T_int16.ToType())
		require.Equal(t, []int16{2, -3}, vector.MustFixedColWithTypeCheck[int16](vecInt16))

		vecInt32 := mapScalar(t, colSigned, pageSigned, plan.Type{Id: int32(types.T_int32), NotNullable: true}, types.T_int32.ToType())
		require.Equal(t, []int32{2, -3}, vector.MustFixedColWithTypeCheck[int32](vecInt32))

		vecInt64 := mapScalar(t, colSigned, pageSigned, plan.Type{Id: int32(types.T_int64), NotNullable: true}, types.T_int64.ToType())
		require.Equal(t, []int64{2, -3}, vector.MustFixedColWithTypeCheck[int64](vecInt64))

		fUnsigned, pageUnsigned := writeDictAndGetPage(t, parquet.Leaf(parquet.FloatType), []parquet.Value{
			parquet.FloatValue(1.4),
			parquet.FloatValue(1.5),
		})
		require.Equal(t, false, pageUnsigned.Dictionary() != nil, "fixture encoding changed")
		colUnsigned := fUnsigned.Root().Column("c")

		vecUint8 := mapScalar(t, colUnsigned, pageUnsigned, plan.Type{Id: int32(types.T_uint8), NotNullable: true}, types.T_uint8.ToType())
		require.Equal(t, []uint8{1, 2}, vector.MustFixedColWithTypeCheck[uint8](vecUint8))

		vecUint16 := mapScalar(t, colUnsigned, pageUnsigned, plan.Type{Id: int32(types.T_uint16), NotNullable: true}, types.T_uint16.ToType())
		require.Equal(t, []uint16{1, 2}, vector.MustFixedColWithTypeCheck[uint16](vecUint16))

		vecUint32 := mapScalar(t, colUnsigned, pageUnsigned, plan.Type{Id: int32(types.T_uint32), NotNullable: true}, types.T_uint32.ToType())
		require.Equal(t, []uint32{1, 2}, vector.MustFixedColWithTypeCheck[uint32](vecUint32))

		vecUint64 := mapScalar(t, colUnsigned, pageUnsigned, plan.Type{Id: int32(types.T_uint64), NotNullable: true}, types.T_uint64.ToType())
		require.Equal(t, []uint64{1, 2}, vector.MustFixedColWithTypeCheck[uint64](vecUint64))

		fDecimal, pageDecimal := writeDictAndGetPage(t, parquet.Decimal(2, 9, parquet.Int32Type), []parquet.Value{
			parquet.Int32Value(12345),
		})
		require.Equal(t, false, pageDecimal.Dictionary() != nil, "fixture encoding changed")
		vecDecInt := mapScalar(t, fDecimal.Root().Column("c"), pageDecimal, plan.Type{Id: int32(types.T_int16), NotNullable: true}, types.T_int16.ToType())
		require.Equal(t, []int16{123}, vector.MustFixedColWithTypeCheck[int16](vecDecInt))
	})
	t.Run("rounded numeric to integer rejects invalid values", func(t *testing.T) {
		var h ParquetHandler

		fSignedOverflow, pageSignedOverflow := writeDictAndGetPage(t, parquet.Leaf(parquet.FloatType), []parquet.Value{
			parquet.FloatValue(128),
		})
		require.Equal(t, false, pageSignedOverflow.Dictionary() != nil, "fixture encoding changed")
		mp := h.getMapper(fSignedOverflow.Root().Column("c"), plan.Type{Id: int32(types.T_int8), NotNullable: true})
		require.NotNil(t, mp)
		vecInt8 := vector.NewVec(types.T_int8.ToType())
		t.Cleanup(func() { vecInt8.Free(proc.Mp()) })
		require.ErrorContains(t, mp.mapping(pageSignedOverflow, proc, vecInt8), "overflows TINYINT")
		require.Zero(t, vecInt8.Length(), "rejected mapping appended output")
		require.True(t, vecInt8.GetNulls().IsEmpty(), "rejected mapping changed NULL state")

		fNegativeUnsigned, pageNegativeUnsigned := writeDictAndGetPage(t, parquet.Leaf(parquet.DoubleType), []parquet.Value{
			parquet.DoubleValue(-1),
		})
		require.Equal(t, false, pageNegativeUnsigned.Dictionary() != nil, "fixture encoding changed")
		mp = h.getMapper(fNegativeUnsigned.Root().Column("c"), plan.Type{Id: int32(types.T_uint8), NotNullable: true})
		require.NotNil(t, mp)
		vecUint8 := vector.NewVec(types.T_uint8.ToType())
		t.Cleanup(func() { vecUint8.Free(proc.Mp()) })
		require.ErrorContains(t, mp.mapping(pageNegativeUnsigned, proc, vecUint8), "overflows unsigned integer")
		require.Zero(t, vecUint8.Length(), "rejected mapping appended output")
		require.True(t, vecUint8.GetNulls().IsEmpty(), "rejected mapping changed NULL state")

		fUnsignedOverflow, pageUnsignedOverflow := writeDictAndGetPage(t, parquet.Leaf(parquet.DoubleType), []parquet.Value{
			parquet.DoubleValue(256),
		})
		require.Equal(t, false, pageUnsignedOverflow.Dictionary() != nil, "fixture encoding changed")
		mp = h.getMapper(fUnsignedOverflow.Root().Column("c"), plan.Type{Id: int32(types.T_uint8), NotNullable: true})
		require.NotNil(t, mp)
		vecUint8Overflow := vector.NewVec(types.T_uint8.ToType())
		t.Cleanup(func() { vecUint8Overflow.Free(proc.Mp()) })
		require.ErrorContains(t, mp.mapping(pageUnsignedOverflow, proc, vecUint8Overflow), "overflows TINYINT UNSIGNED")
		require.Zero(t, vecUint8Overflow.Length(), "rejected mapping appended output")
		require.True(t, vecUint8Overflow.GetNulls().IsEmpty(), "rejected mapping changed NULL state")
	})
	t.Run("decimal int32 to int64 and json", func(t *testing.T) {
		f, page := writeDictAndGetPage(t, parquet.Decimal(2, 9, parquet.Int32Type), []parquet.Value{
			parquet.Int32Value(12345),
			parquet.Int32Value(-6789),
		})
		require.Equal(t, false, page.Dictionary() != nil, "fixture encoding changed")
		col := f.Root().Column("c")

		vecInt := mapScalar(t, col, page, plan.Type{Id: int32(types.T_int64), NotNullable: true}, types.T_int64.ToType())
		require.Equal(t, []int64{123, -68}, vector.MustFixedColWithTypeCheck[int64](vecInt))

		vecJSON := mapScalar(t, col, page, plan.Type{Id: int32(types.T_json), NotNullable: true}, types.T_json.ToType())
		requireJSONAt(t, vecJSON, 0, `123.45`)
		requireJSONAt(t, vecJSON, 1, `-67.89`)
	})
	t.Run("decimal logical to integer keeps exact boundaries", func(t *testing.T) {

		f, page := writeDictAndGetPage(t, parquet.Decimal(0, 19, parquet.FixedLenByteArrayType(8)), []parquet.Value{
			parquet.FixedLenByteArrayValue([]byte{0x00, 0x20, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}),
			parquet.FixedLenByteArrayValue([]byte{0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}),
		})
		require.Equal(t, false, page.Dictionary() != nil, "fixture encoding changed")
		vecInt := mapScalar(t, f.Root().Column("c"), page, plan.Type{Id: int32(types.T_int64), NotNullable: true}, types.T_int64.ToType())
		require.Equal(t, []int64{9007199254740993, math.MaxInt64}, vector.MustFixedColWithTypeCheck[int64](vecInt))

		fRound, pageRound := writeDictAndGetPage(t, parquet.Decimal(1, 19, parquet.FixedLenByteArrayType(8)), []parquet.Value{
			parquet.FixedLenByteArrayValue([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0f}),
			parquet.FixedLenByteArrayValue([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xf1}),
			parquet.FixedLenByteArrayValue([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfc}),
		})
		require.Equal(t, false, pageRound.Dictionary() != nil, "fixture encoding changed")
		vecRound := mapScalar(t, fRound.Root().Column("c"), pageRound, plan.Type{Id: int32(types.T_int64), NotNullable: true}, types.T_int64.ToType())
		require.Equal(t, []int64{2, -2, 0}, vector.MustFixedColWithTypeCheck[int64](vecRound))

		fUint, pageUint := writeDictAndGetPage(t, parquet.Decimal(0, 20, parquet.FixedLenByteArrayType(9)), []parquet.Value{
			parquet.FixedLenByteArrayValue([]byte{0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}),
		})
		require.Equal(t, false, pageUint.Dictionary() != nil, "fixture encoding changed")
		vecUint := mapScalar(t, fUint.Root().Column("c"), pageUint, plan.Type{Id: int32(types.T_uint64), NotNullable: true}, types.T_uint64.ToType())
		require.Equal(t, []uint64{math.MaxUint64}, vector.MustFixedColWithTypeCheck[uint64](vecUint))
	})
	t.Run("decimal logical to integer rejects exact overflows", func(t *testing.T) {
		var h ParquetHandler

		fIntOverflow, pageIntOverflow := writeDictAndGetPage(t, parquet.Decimal(0, 20, parquet.FixedLenByteArrayType(9)), []parquet.Value{
			parquet.FixedLenByteArrayValue([]byte{0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}),
		})
		require.Equal(t, false, pageIntOverflow.Dictionary() != nil, "fixture encoding changed")
		mp := h.getMapper(fIntOverflow.Root().Column("c"), plan.Type{Id: int32(types.T_int64), NotNullable: true})
		require.NotNil(t, mp)
		vecInt := vector.NewVec(types.T_int64.ToType())
		t.Cleanup(func() { vecInt.Free(proc.Mp()) })
		require.ErrorContains(t, mp.mapping(pageIntOverflow, proc, vecInt), "overflows BIGINT")
		require.Zero(t, vecInt.Length(), "rejected mapping appended output")
		require.True(t, vecInt.GetNulls().IsEmpty(), "rejected mapping changed NULL state")

		fUintOverflow, pageUintOverflow := writeDictAndGetPage(t, parquet.Decimal(0, 21, parquet.FixedLenByteArrayType(10)), []parquet.Value{
			parquet.FixedLenByteArrayValue([]byte{0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}),
		})
		require.Equal(t, false, pageUintOverflow.Dictionary() != nil, "fixture encoding changed")
		mp = h.getMapper(fUintOverflow.Root().Column("c"), plan.Type{Id: int32(types.T_uint64), NotNullable: true})
		require.NotNil(t, mp)
		vecUint := vector.NewVec(types.T_uint64.ToType())
		t.Cleanup(func() { vecUint.Free(proc.Mp()) })
		require.ErrorContains(t, mp.mapping(pageUintOverflow, proc, vecUint), "overflows BIGINT UNSIGNED")
		require.Zero(t, vecUint.Length(), "rejected mapping appended output")
		require.True(t, vecUint.GetNulls().IsEmpty(), "rejected mapping changed NULL state")

		fNegativeUint, pageNegativeUint := writeDictAndGetPage(t, parquet.Decimal(1, 9, parquet.FixedLenByteArrayType(4)), []parquet.Value{
			parquet.FixedLenByteArrayValue([]byte{0xff, 0xff, 0xff, 0xfb}),
		})
		require.Equal(t, false, pageNegativeUint.Dictionary() != nil, "fixture encoding changed")
		mp = h.getMapper(fNegativeUint.Root().Column("c"), plan.Type{Id: int32(types.T_uint64), NotNullable: true})
		require.NotNil(t, mp)
		vecNegativeUint := vector.NewVec(types.T_uint64.ToType())
		t.Cleanup(func() { vecNegativeUint.Free(proc.Mp()) })
		require.ErrorContains(t, mp.mapping(pageNegativeUint, proc, vecNegativeUint), "overflows unsigned integer")
		require.Zero(t, vecNegativeUint.Length(), "rejected mapping appended output")
		require.True(t, vecNegativeUint.GetNulls().IsEmpty(), "rejected mapping changed NULL state")
	})
	t.Run("string to bool and bit", func(t *testing.T) {
		f, page := writeDictAndGetPage(t, parquet.String(), []parquet.Value{
			parquet.ByteArrayValue([]byte("true")),
			parquet.ByteArrayValue([]byte("0")),
		})
		require.Equal(t, false, page.Dictionary() != nil, "fixture encoding changed")
		col := f.Root().Column("c")

		vecBool := mapScalar(t, col, page, plan.Type{Id: int32(types.T_bool), NotNullable: true}, types.T_bool.ToType())
		require.Equal(t, []bool{true, false}, vector.MustFixedColWithTypeCheck[bool](vecBool))

		vecBit := mapScalar(t, col, page, plan.Type{Id: int32(types.T_bit), Width: 1, NotNullable: true}, types.New(types.T_bit, 1, 0))
		require.Equal(t, []uint64{1, 0}, vector.MustFixedColWithTypeCheck[uint64](vecBit))
	})
	t.Run("string to bit parses decimal", func(t *testing.T) {
		f, page := writeDictAndGetPage(t, parquet.String(), []parquet.Value{
			parquet.ByteArrayValue([]byte("010")),
			parquet.ByteArrayValue([]byte("15")),
		})
		require.Equal(t, false, page.Dictionary() != nil, "fixture encoding changed")

		vec := mapScalar(t, f.Root().Column("c"), page, plan.Type{Id: int32(types.T_bit), Width: 4, NotNullable: true}, types.New(types.T_bit, 4, 0))
		require.Equal(t, []uint64{10, 15}, vector.MustFixedColWithTypeCheck[uint64](vec))
	})
	t.Run("string to bit rejects invalid and overflow", func(t *testing.T) {
		var h ParquetHandler

		fInvalid, pageInvalid := writeDictAndGetPage(t, parquet.String(), []parquet.Value{
			parquet.ByteArrayValue([]byte("not-a-bit")),
		})
		require.Equal(t, false, pageInvalid.Dictionary() != nil, "fixture encoding changed")
		vecInvalid := vector.NewVec(types.New(types.T_bit, 4, 0))
		t.Cleanup(func() { vecInvalid.Free(proc.Mp()) })
		mp := h.getMapper(fInvalid.Root().Column("c"), plan.Type{Id: int32(types.T_bit), Width: 4, NotNullable: true})
		require.NotNil(t, mp)
		require.Error(t, mp.mapping(pageInvalid, proc, vecInvalid))
		require.Zero(t, vecInvalid.Length(), "rejected mapping appended output")
		require.True(t, vecInvalid.GetNulls().IsEmpty(), "rejected mapping changed NULL state")

		fOverflow, pageOverflow := writeDictAndGetPage(t, parquet.String(), []parquet.Value{
			parquet.ByteArrayValue([]byte("16")),
		})
		require.Equal(t, false, pageOverflow.Dictionary() != nil, "fixture encoding changed")
		vecOverflow := vector.NewVec(types.New(types.T_bit, 4, 0))
		t.Cleanup(func() { vecOverflow.Free(proc.Mp()) })
		mp = h.getMapper(fOverflow.Root().Column("c"), plan.Type{Id: int32(types.T_bit), Width: 4, NotNullable: true})
		require.NotNil(t, mp)
		require.ErrorContains(t, mp.mapping(pageOverflow, proc, vecOverflow), "overflows BIT(4)")
		require.Zero(t, vecOverflow.Length(), "rejected mapping appended output")
		require.True(t, vecOverflow.GetNulls().IsEmpty(), "rejected mapping changed NULL state")
	})
	t.Run("int8 to bit", func(t *testing.T) {
		f, page := writeDictAndGetPage(t, parquet.Int(8), []parquet.Value{
			parquet.Int32Value(0),
			parquet.Int32Value(1),
			parquet.Int32Value(127),
		})
		require.Equal(t, false, page.Dictionary() != nil, "fixture encoding changed")

		vec := mapScalar(t, f.Root().Column("c"), page, plan.Type{Id: int32(types.T_bit), Width: 8, NotNullable: true}, types.New(types.T_bit, 8, 0))
		require.Equal(t, []uint64{0, 1, 127}, vector.MustFixedColWithTypeCheck[uint64](vec))
	})
	t.Run("bool to bit", func(t *testing.T) {
		f, page := writeDictAndGetPage(t, parquet.Leaf(parquet.BooleanType), []parquet.Value{
			parquet.BooleanValue(true),
			parquet.BooleanValue(false),
		})
		require.Equal(t, false, page.Dictionary() != nil, "fixture encoding changed")

		vec := mapScalar(t, f.Root().Column("c"), page, plan.Type{Id: int32(types.T_bit), Width: 1, NotNullable: true}, types.New(types.T_bit, 1, 0))
		require.Equal(t, []uint64{1, 0}, vector.MustFixedColWithTypeCheck[uint64](vec))
	})
	t.Run("negative signed int8 to bit", func(t *testing.T) {
		f, page := writeDictAndGetPage(t, parquet.Int(8), []parquet.Value{
			parquet.Int32Value(-1),
		})
		require.Equal(t, false, page.Dictionary() != nil, "fixture encoding changed")

		vec := vector.NewVec(types.New(types.T_bit, 8, 0))
		t.Cleanup(func() { vec.Free(proc.Mp()) })
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_bit), Width: 8, NotNullable: true})
		require.NotNil(t, mp)
		require.ErrorContains(t, mp.mapping(page, proc, vec), "negative parquet value")
		require.Zero(t, vec.Length(), "rejected mapping appended output")
		require.True(t, vec.GetNulls().IsEmpty(), "rejected mapping changed NULL state")
	})
	t.Run("int16 to bit overflow", func(t *testing.T) {
		f, page := writeDictAndGetPage(t, parquet.Int(16), []parquet.Value{
			parquet.Int32Value(256),
		})
		require.Equal(t, false, page.Dictionary() != nil, "fixture encoding changed")

		vec := vector.NewVec(types.New(types.T_bit, 8, 0))
		t.Cleanup(func() { vec.Free(proc.Mp()) })
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_bit), Width: 8, NotNullable: true})
		require.NotNil(t, mp)
		require.ErrorContains(t, mp.mapping(page, proc, vec), "overflows BIT(8)")
		require.Zero(t, vec.Length(), "rejected mapping appended output")
		require.True(t, vec.GetNulls().IsEmpty(), "rejected mapping changed NULL state")
	})
	t.Run("string to uuid", func(t *testing.T) {
		f, page := writeDictAndGetPage(t, parquet.String(), []parquet.Value{
			parquet.ByteArrayValue([]byte("c8477387-eb5b-4d97-af1b-48d9db74856d")),
			parquet.ByteArrayValue([]byte("00000000-0000-0000-0000-000000000001")),
		})
		require.Equal(t, false, page.Dictionary() != nil, "fixture encoding changed")

		vec := mapScalar(t, f.Root().Column("c"), page, plan.Type{Id: int32(types.T_uuid), NotNullable: true}, types.T_uuid.ToType())

		want0 := types.Uuid{0xc8, 0x47, 0x73, 0x87, 0xeb, 0x5b, 0x4d, 0x97, 0xaf, 0x1b, 0x48, 0xd9, 0xdb, 0x74, 0x85, 0x6d}
		want1 := types.Uuid{15: 1}
		require.Equal(t, []types.Uuid{want0, want1}, vector.MustFixedColWithTypeCheck[types.Uuid](vec))
	})
	t.Run("plain int64 micros to time", func(t *testing.T) {
		f, page := writeDictAndGetPage(t, parquet.Leaf(parquet.Int64Type), []parquet.Value{
			parquet.Int64Value(0),
			parquet.Int64Value(45_296_123_456),
		})
		require.Equal(t, false, page.Dictionary() != nil, "fixture encoding changed")

		vec := mapScalar(t, f.Root().Column("c"), page, plan.Type{Id: int32(types.T_time), Scale: 6, NotNullable: true}, types.New(types.T_time, 0, 6))
		require.Equal(t, []types.Time{0, types.Time(45_296_123_456)}, vector.MustFixedColWithTypeCheck[types.Time](vec))
	})
	t.Run("string to enum", func(t *testing.T) {
		f, page := writeDictAndGetPage(t, parquet.Encoded(parquet.String(), &parquet.RLEDictionary), []parquet.Value{
			parquet.ByteArrayValue([]byte("red")),
			parquet.ByteArrayValue([]byte("green")),
			parquet.ByteArrayValue([]byte("red")),
		})
		require.Equal(t, true, page.Dictionary() != nil, "fixture encoding changed")

		require.NotNil(t, page.Dictionary())
		vec := mapScalar(t, f.Root().Column("c"), page, plan.Type{Id: int32(types.T_enum), Enumvalues: "red,green,blue", NotNullable: true}, types.T_enum.ToType())
		require.Equal(t, []types.Enum{1, 2, 1}, vector.MustFixedColWithTypeCheck[types.Enum](vec))
		alternate := mapScalar(t, f.Root().Column("c"), page, plan.Type{Id: int32(types.T_enum), Enumvalues: "blue,red,green", NotNullable: true}, types.T_enum.ToType())
		require.Equal(t, []types.Enum{2, 3, 2}, vector.MustFixedColWithTypeCheck[types.Enum](alternate))
	})
	t.Run("int64 to int32 and varchar", func(t *testing.T) {
		f, page := writeDictAndGetPage(t, parquet.Int(64), []parquet.Value{
			parquet.Int64Value(100),
			parquet.Int64Value(-200),
		})
		require.Equal(t, false, page.Dictionary() != nil, "fixture encoding changed")
		col := f.Root().Column("c")

		vecInt := mapScalar(t, col, page, plan.Type{Id: int32(types.T_int32), NotNullable: true}, types.T_int32.ToType())
		require.Equal(t, []int32{100, -200}, vector.MustFixedColWithTypeCheck[int32](vecInt))

		vecStr := mapScalar(t, col, page, plan.Type{Id: int32(types.T_varchar), NotNullable: true}, types.T_varchar.ToType())
		require.Equal(t, "100", vecStr.GetStringAt(0))
		require.Equal(t, "-200", vecStr.GetStringAt(1))
	})
	t.Run("int64 to int32 overflow", func(t *testing.T) {
		f, page := writeDictAndGetPage(t, parquet.Int(64), []parquet.Value{
			parquet.Int64Value(1 << 40),
		})
		require.Equal(t, false, page.Dictionary() != nil, "fixture encoding changed")

		vec := vector.NewVec(types.T_int32.ToType())
		t.Cleanup(func() { vec.Free(proc.Mp()) })
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_int32), NotNullable: true})
		require.NotNil(t, mp)
		require.ErrorContains(t, mp.mapping(page, proc, vec), "overflows INT")
		require.Zero(t, vec.Length(), "rejected mapping appended output")
		require.True(t, vec.GetNulls().IsEmpty(), "rejected mapping changed NULL state")
	})
	t.Run("signed int32 to int8 overflow", func(t *testing.T) {
		f, page := writeDictAndGetPage(t, parquet.Leaf(parquet.Int32Type), []parquet.Value{
			parquet.Int32Value(128),
		})
		require.Equal(t, false, page.Dictionary() != nil, "fixture encoding changed")

		vec := vector.NewVec(types.T_int8.ToType())
		t.Cleanup(func() { vec.Free(proc.Mp()) })
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_int8), NotNullable: true})
		require.NotNil(t, mp)
		require.ErrorContains(t, mp.mapping(page, proc, vec), "overflows TINYINT")
		require.Zero(t, vec.Length(), "rejected mapping appended output")
		require.True(t, vec.GetNulls().IsEmpty(), "rejected mapping changed NULL state")
	})
	t.Run("negative signed int32 to uint8", func(t *testing.T) {
		f, page := writeDictAndGetPage(t, parquet.Leaf(parquet.Int32Type), []parquet.Value{
			parquet.Int32Value(-1),
		})
		require.Equal(t, false, page.Dictionary() != nil, "fixture encoding changed")

		vec := vector.NewVec(types.T_uint8.ToType())
		t.Cleanup(func() { vec.Free(proc.Mp()) })
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_uint8), NotNullable: true})
		require.NotNil(t, mp)
		require.ErrorContains(t, mp.mapping(page, proc, vec), "negative parquet value")
		require.Zero(t, vec.Length(), "rejected mapping appended output")
		require.True(t, vec.GetNulls().IsEmpty(), "rejected mapping changed NULL state")
	})
	t.Run("negative signed int32 to uint32", func(t *testing.T) {
		f, page := writeDictAndGetPage(t, parquet.Leaf(parquet.Int32Type), []parquet.Value{
			parquet.Int32Value(-1),
		})
		require.Equal(t, false, page.Dictionary() != nil, "fixture encoding changed")

		vec := vector.NewVec(types.T_uint32.ToType())
		t.Cleanup(func() { vec.Free(proc.Mp()) })
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_uint32), NotNullable: true})
		require.NotNil(t, mp)
		require.ErrorContains(t, mp.mapping(page, proc, vec), "negative parquet value")
		require.Zero(t, vec.Length(), "rejected mapping appended output")
		require.True(t, vec.GetNulls().IsEmpty(), "rejected mapping changed NULL state")
	})
	t.Run("unsigned int32 to int32 overflow", func(t *testing.T) {
		f, page := writeDictAndGetPage(t, parquet.Uint(32), []parquet.Value{
			parquet.ValueOf(uint32(1 << 31)),
		})
		require.Equal(t, false, page.Dictionary() != nil, "fixture encoding changed")

		vec := vector.NewVec(types.T_int32.ToType())
		t.Cleanup(func() { vec.Free(proc.Mp()) })
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_int32), NotNullable: true})
		require.NotNil(t, mp)
		require.ErrorContains(t, mp.mapping(page, proc, vec), "overflows INT")
		require.Zero(t, vec.Length(), "rejected mapping appended output")
		require.True(t, vec.GetNulls().IsEmpty(), "rejected mapping changed NULL state")
	})
	t.Run("unsigned int64 to int64 overflow", func(t *testing.T) {
		f, page := writeDictAndGetPage(t, parquet.Uint(64), []parquet.Value{
			parquet.ValueOf(uint64(1 << 63)),
		})
		require.Equal(t, false, page.Dictionary() != nil, "fixture encoding changed")

		vec := vector.NewVec(types.T_int64.ToType())
		t.Cleanup(func() { vec.Free(proc.Mp()) })
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_int64), NotNullable: true})
		require.NotNil(t, mp)
		require.ErrorContains(t, mp.mapping(page, proc, vec), "overflows BIGINT")
		require.Zero(t, vec.Length(), "rejected mapping appended output")
		require.True(t, vec.GetNulls().IsEmpty(), "rejected mapping changed NULL state")
	})
	t.Run("unsigned int64 above int64 max to floats", func(t *testing.T) {
		f, page := writeDictAndGetPage(t, parquet.Uint(64), []parquet.Value{
			parquet.ValueOf(uint64(1 << 63)),
		})
		require.Equal(t, false, page.Dictionary() != nil, "fixture encoding changed")
		col := f.Root().Column("c")

		vecFloat32 := mapScalar(t, col, page, plan.Type{Id: int32(types.T_float32), NotNullable: true}, types.T_float32.ToType())
		require.InDeltaSlice(t, []float32{float32(uint64(1 << 63))}, vector.MustFixedColWithTypeCheck[float32](vecFloat32), 1)

		vecFloat64 := mapScalar(t, col, page, plan.Type{Id: int32(types.T_float64), NotNullable: true}, types.T_float64.ToType())
		require.InDeltaSlice(t, []float64{float64(uint64(1 << 63))}, vector.MustFixedColWithTypeCheck[float64](vecFloat64), 1)
	})
	t.Run("double to float", func(t *testing.T) {
		f, page := writeDictAndGetPage(t, parquet.Leaf(parquet.DoubleType), []parquet.Value{
			parquet.DoubleValue(1.5),
			parquet.DoubleValue(2.25),
		})
		require.Equal(t, false, page.Dictionary() != nil, "fixture encoding changed")

		vec := mapScalar(t, f.Root().Column("c"), page, plan.Type{Id: int32(types.T_float32), NotNullable: true}, types.T_float32.ToType())
		require.InDeltaSlice(t, []float32{1.5, 2.25}, vector.MustFixedColWithTypeCheck[float32](vec), 1e-6)
	})
	t.Run("decimal to double and varchar", func(t *testing.T) {
		f, page := writeDictAndGetPage(t, parquet.Decimal(2, 10, parquet.FixedLenByteArrayType(8)), []parquet.Value{
			parquet.FixedLenByteArrayValue([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x27, 0x10}),
			parquet.FixedLenByteArrayValue([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x4e, 0x52}),
		})
		require.Equal(t, false, page.Dictionary() != nil, "fixture encoding changed")
		col := f.Root().Column("c")

		vecFloat := mapScalar(t, col, page, plan.Type{Id: int32(types.T_float64), NotNullable: true}, types.T_float64.ToType())
		require.InDeltaSlice(t, []float64{100, 200.5}, vector.MustFixedColWithTypeCheck[float64](vecFloat), 1e-9)

		vecStr := mapScalar(t, col, page, plan.Type{Id: int32(types.T_varchar), NotNullable: true}, types.T_varchar.ToType())
		require.Equal(t, "100.00", vecStr.GetStringAt(0))
		require.Equal(t, "200.50", vecStr.GetStringAt(1))

		vecInt := mapScalar(t, col, page, plan.Type{Id: int32(types.T_int64), NotNullable: true}, types.T_int64.ToType())
		require.Equal(t, []int64{100, 201}, vector.MustFixedColWithTypeCheck[int64](vecInt))

		vecJSON := mapScalar(t, col, page, plan.Type{Id: int32(types.T_json), NotNullable: true}, types.T_json.ToType())
		requireJSONAt(t, vecJSON, 0, `100`)
		requireJSONAt(t, vecJSON, 1, `200.5`)
	})
	t.Run("date to varchar", func(t *testing.T) {
		f, page := writeDictAndGetPage(t, parquet.Date(), []parquet.Value{
			parquet.Int32Value(19723),
			parquet.Int32Value(19875),
		})
		require.Equal(t, false, page.Dictionary() != nil, "fixture encoding changed")

		vec := mapScalar(t, f.Root().Column("c"), page, plan.Type{Id: int32(types.T_varchar), NotNullable: true}, types.T_varchar.ToType())
		require.Equal(t, "2024-01-01", vec.GetStringAt(0))
		require.Equal(t, "2024-06-01", vec.GetStringAt(1))
	})
	t.Run("date to timestamp", func(t *testing.T) {
		previousZone := proc.Base.SessionInfo.TimeZone
		t.Cleanup(func() { proc.Base.SessionInfo.TimeZone = previousZone })
		proc.Base.SessionInfo.TimeZone = time.UTC
		f, page := writeDictAndGetPage(t, parquet.Date(), []parquet.Value{
			parquet.Int32Value(19723),
		})
		require.Equal(t, false, page.Dictionary() != nil, "fixture encoding changed")

		vec := mapScalar(t, f.Root().Column("c"), page, plan.Type{Id: int32(types.T_timestamp), Scale: 6, NotNullable: true}, types.New(types.T_timestamp, 0, 6))
		require.Equal(t, []types.Timestamp{63839664000000000}, vector.MustFixedColWithTypeCheck[types.Timestamp](vec))
	})
	t.Run("timestamp to date and time", func(t *testing.T) {
		previousZone := proc.Base.SessionInfo.TimeZone
		t.Cleanup(func() { proc.Base.SessionInfo.TimeZone = previousZone })
		proc.Base.SessionInfo.TimeZone = time.UTC
		micros := time.Date(2024, 1, 1, 12, 30, 45, 123456000, time.UTC).UnixMicro()
		f, page := writeDictAndGetPage(t, parquet.Timestamp(parquet.Microsecond), []parquet.Value{
			parquet.Int64Value(micros),
		})
		require.Equal(t, false, page.Dictionary() != nil, "fixture encoding changed")
		col := f.Root().Column("c")

		vecDate := mapScalar(t, col, page, plan.Type{Id: int32(types.T_date), NotNullable: true}, types.T_date.ToType())
		wantDate, err := types.ParseDateCast("2024-01-01")
		require.NoError(t, err)
		require.Equal(t, []types.Date{wantDate}, vector.MustFixedColWithTypeCheck[types.Date](vecDate))

		vecTime := mapScalar(t, col, page, plan.Type{Id: int32(types.T_time), Scale: 6, NotNullable: true}, types.New(types.T_time, 0, 6))
		wantTime, err := types.ParseTime("12:30:45.123456", 6)
		require.NoError(t, err)
		require.Equal(t, []types.Time{wantTime}, vector.MustFixedColWithTypeCheck[types.Time](vecTime))
	})
	t.Run("time logical to datetime", func(t *testing.T) {
		previousZone := proc.Base.SessionInfo.TimeZone
		t.Cleanup(func() { proc.Base.SessionInfo.TimeZone = previousZone })
		proc.Base.SessionInfo.TimeZone = time.UTC

		row := parquet.Row{parquet.Int64Value(45045123456).Level(0, 0, 0)}
		f, page := writeColumnAndGetPage(t, parquet.Time(parquet.Microsecond), []parquet.Row{row})
		require.Equal(t, false, page.Dictionary() != nil, "fixture encoding changed")

		before := time.Now().UTC()
		vec := mapScalar(t, f.Root().Column("c"), page, plan.Type{Id: int32(types.T_datetime), Scale: 6, NotNullable: true}, types.New(types.T_datetime, 0, 6))
		after := time.Now().UTC()
		// Accept the date on either side of the call if UTC midnight intervenes.
		midnight := func(now time.Time) types.Datetime {
			return types.Datetime(time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC).UnixMicro() + 62135596800000000 + 45045123456)
		}
		require.Contains(t, []types.Datetime{midnight(before), midnight(after)}, vector.MustFixedColWithTypeCheck[types.Datetime](vec)[0])
	})
	t.Run("time logical dictionary to datetime", func(t *testing.T) {
		previousZone := proc.Base.SessionInfo.TimeZone
		t.Cleanup(func() { proc.Base.SessionInfo.TimeZone = previousZone })
		proc.Base.SessionInfo.TimeZone = time.UTC

		f, page := writeDictAndGetPage(t, parquet.Encoded(parquet.Time(parquet.Microsecond), &parquet.RLEDictionary), []parquet.Value{
			parquet.Int64Value(3723000000),
		})
		require.Equal(t, true, page.Dictionary() != nil, "fixture encoding changed")

		before := time.Now().UTC()
		vec := mapScalar(t, f.Root().Column("c"), page, plan.Type{Id: int32(types.T_datetime), Scale: 6, NotNullable: true}, types.New(types.T_datetime, 0, 6))
		after := time.Now().UTC()
		// Accept the date on either side of the call if UTC midnight intervenes.
		midnight := func(now time.Time) types.Datetime {
			return types.Datetime(time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC).UnixMicro() + 62135596800000000 + 3723000000)
		}
		require.Contains(t, []types.Datetime{midnight(before), midnight(after)}, vector.MustFixedColWithTypeCheck[types.Datetime](vec)[0])
	})
	t.Run("constructed scalars", func(t *testing.T) {
		previousZone := proc.Base.SessionInfo.TimeZone
		t.Cleanup(func() { proc.Base.SessionInfo.TimeZone = previousZone })
		proc.Base.SessionInfo.TimeZone = time.UTC

		tests := []struct {
			name      string
			st        parquet.Type
			numValues int
			values    encoding.Values
			dt        types.T
			want      any
		}{
			{name: "bool", st: parquet.BooleanType, numValues: 2, values: encoding.BooleanValues([]byte{0xFF}), dt: types.T_bool, want: []bool{true, true}},
			{name: "int32", st: parquet.Int32Type, numValues: 2, values: encoding.Int32Values([]int32{1, -2}), dt: types.T_int32, want: []int32{1, -2}},
			{name: "int64", st: parquet.Int64Type, numValues: 2, values: encoding.Int64Values([]int64{2, 7}), dt: types.T_int64, want: []int64{2, 7}},
			{name: "uint32", st: parquet.Uint(32).Type(), numValues: 2, values: encoding.Uint32Values([]uint32{5, 3}), dt: types.T_uint32, want: []uint32{5, 3}},
			{name: "uint64", st: parquet.Uint(64).Type(), numValues: 2, values: encoding.Uint64Values([]uint64{8, 10}), dt: types.T_uint64, want: []uint64{8, 10}},
			{name: "float32", st: parquet.FloatType, numValues: 2, values: encoding.FloatValues([]float32{7.5, 3.25}), dt: types.T_float32, want: []float32{7.5, 3.25}},
			{name: "float64", st: parquet.DoubleType, numValues: 2, values: encoding.DoubleValues([]float64{77.9, 0}), dt: types.T_float64, want: []float64{77.9, 0}},
			{name: "string", st: parquet.String().Type(), numValues: 2, values: encoding.ByteArrayValues([]byte("abcdefg"), []uint32{0, 3, 7}), dt: types.T_varchar, want: []string{"abc", "defg"}},
			{name: "fixed3", st: parquet.FixedLenByteArrayType(3), numValues: 2, values: encoding.FixedLenByteArrayValues([]byte("abcdef"), 3), dt: types.T_char, want: []string{"abc", "def"}},
			// Gregorian epoch: 719162 days from 0001-01-01 to 1970-01-01.
			{name: "date", st: parquet.Date().Type(), numValues: 2, values: encoding.Int32Values([]int32{0, 365}), dt: types.T_date, want: []types.Date{719162, 719527}},
			{name: "time_ms", st: parquet.Time(parquet.Millisecond).Type(), numValues: 2, values: encoding.Int32Values([]int32{1_000, 61_000}), dt: types.T_time, want: []types.Time{1000000, 61000000}},
			{name: "ts_us", st: parquet.Timestamp(parquet.Microsecond).Type(), numValues: 2, values: encoding.Int64Values([]int64{0, 1_000_000}), dt: types.T_timestamp, want: []types.Timestamp{62135596800000000, 62135596801000000}},
			{name: "time_ns", st: parquet.Time(parquet.Nanosecond).Type(), numValues: 2, values: encoding.Int64Values([]int64{1000, 2000}), dt: types.T_time, want: []types.Time{1, 2}},
			{name: "timestamp_ns", st: parquet.Timestamp(parquet.Nanosecond).Type(), numValues: 2, values: encoding.Int64Values([]int64{1000, 2000}), dt: types.T_timestamp, want: []types.Timestamp{62135596800000001, 62135596800000002}},
			{name: "datetime_ms", st: parquet.Timestamp(parquet.Millisecond).Type(), numValues: 2, values: encoding.Int64Values([]int64{1, 2}), dt: types.T_datetime, want: []types.Datetime{62135596800001000, 62135596800002000}},
			{name: "decimal64_int32", st: parquet.Int32Type, numValues: 2, values: encoding.Int32Values([]int32{1, -2}), dt: types.T_decimal64, want: []types.Decimal64{1, 18446744073709551614}},
			{name: "decimal128_int64", st: parquet.Int64Type, numValues: 2, values: encoding.Int64Values([]int64{5, -6}), dt: types.T_decimal128, want: []types.Decimal128{{B0_63: 5}, {B0_63: 18446744073709551610, B64_127: 18446744073709551615}}},
			{name: "uint8_boundaries", st: parquet.Int32Type, numValues: 4, values: encoding.Int32Values([]int32{0, 1, 128, 255}), dt: types.T_uint8, want: []uint8{0, 1, 128, 255}},
			{name: "int8_boundaries", st: parquet.Int32Type, numValues: 4, values: encoding.Int32Values([]int32{-128, -1, 0, 127}), dt: types.T_int8, want: []int8{-128, -1, 0, 127}},
			{name: "uint16_boundaries", st: parquet.Int32Type, numValues: 4, values: encoding.Int32Values([]int32{0, 1, 255, 65535}), dt: types.T_uint16, want: []uint16{0, 1, 255, 65535}},
			{name: "int16_boundaries", st: parquet.Int32Type, numValues: 4, values: encoding.Int32Values([]int32{-32768, -1, 255, 32767}), dt: types.T_int16, want: []int16{-32768, -1, 255, 32767}},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				// Build a tiny parquet buffer with a single column named "c"
				page := tc.st.NewPage(0, tc.numValues, tc.values)

				vals := make([]parquet.Value, tc.numValues)
				n, err := page.Values().ReadValues(vals)
				require.True(t, err == nil || err == io.EOF)
				require.Equal(t, tc.numValues, n)
				f := writeParquetFixture(t, parquet.Leaf(tc.st), []parquet.Row{parquet.MakeRow(vals)})

				vec := vector.NewVec(types.New(tc.dt, 0, 0))
				t.Cleanup(func() { vec.Free(proc.Mp()) })
				var h ParquetHandler
				mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(tc.dt), NotNullable: true})
				require.NotNil(t, mp)
				err = mp.mapping(page, proc, vec)
				require.NoError(t, err)

				require.Equal(t, tc.numValues, vec.Length())
				require.Equal(t, types.New(tc.dt, 0, 0), *vec.GetType())
				require.True(t, vec.GetNulls().IsEmpty())
				requireParquetScalarResult(t, vec, tc.want, 0)
			})
		}

	})
	t.Run("dictionary scalars", func(t *testing.T) {
		previousZone := proc.Base.SessionInfo.TimeZone
		t.Cleanup(func() { proc.Base.SessionInfo.TimeZone = previousZone })
		proc.Base.SessionInfo.TimeZone = time.UTC
		for _, tc := range []struct {
			name     string
			node     parquet.Node
			values   []parquet.Value
			target   types.T
			want     any
			delta    float64
			nullable bool
		}{
			{name: "int8 from int32 dictionary", node: parquet.Encoded(parquet.Leaf(parquet.Int32Type), &parquet.RLEDictionary), values: []parquet.Value{
				parquet.Int32Value(1), parquet.Int32Value(2), parquet.Int32Value(1), parquet.Int32Value(3), parquet.Int32Value(2),
			}, target: types.T_int8, want: []int8{1, 2, 1, 3, 2}, delta: 0},
			{name: "float32 dictionary", node: parquet.Encoded(parquet.Leaf(parquet.FloatType), &parquet.RLEDictionary), values: []parquet.Value{
				parquet.FloatValue(1.5), parquet.FloatValue(2.25), parquet.FloatValue(1.5), parquet.FloatValue(3.5),
			}, target: types.T_float32, want: []float32{1.5, 2.25, 1.5, 3.5}, delta: 1e-6},
			{name: "float64 dictionary", node: parquet.Encoded(parquet.Leaf(parquet.DoubleType), &parquet.RLEDictionary), values: []parquet.Value{
				parquet.DoubleValue(1.5), parquet.DoubleValue(2.25), parquet.DoubleValue(1.5), parquet.DoubleValue(3.5),
			}, target: types.T_float64, want: []float64{1.5, 2.25, 1.5, 3.5}, delta: 1e-9},
			{name: "uint64 from int64 dictionary", node: parquet.Encoded(parquet.Leaf(parquet.Int64Type), &parquet.RLEDictionary), values: []parquet.Value{
				parquet.Int64Value(5), parquet.Int64Value(7), parquet.Int64Value(5), parquet.Int64Value(9),
			}, target: types.T_uint64, want: []uint64{5, 7, 5, 9}, delta: 0},
			{name: "int32 dictionary", node: parquet.Encoded(parquet.Leaf(parquet.Int32Type), &parquet.RLEDictionary), values: []parquet.Value{parquet.Int32Value(-3), parquet.Int32Value(4), parquet.Int32Value(-3)}, target: types.T_int32, want: []int32{-3, 4, -3}, delta: 0},
			{name: "int16 from int32 dictionary", node: parquet.Encoded(parquet.Leaf(parquet.Int32Type), &parquet.RLEDictionary), values: []parquet.Value{parquet.Int32Value(-10), parquet.Int32Value(20), parquet.Int32Value(-10)}, target: types.T_int16, want: []int16{-10, 20, -10}, delta: 0},
			{name: "uint16 from int32 dictionary", node: parquet.Encoded(parquet.Leaf(parquet.Int32Type), &parquet.RLEDictionary), values: []parquet.Value{parquet.Int32Value(0), parquet.Int32Value(65535), parquet.Int32Value(1)}, target: types.T_uint16, want: []uint16{0, 65535, 1}, delta: 0},
			{name: "uint8 from int32 dictionary", node: parquet.Encoded(parquet.Leaf(parquet.Int32Type), &parquet.RLEDictionary), values: []parquet.Value{parquet.Int32Value(1), parquet.Int32Value(255), parquet.Int32Value(1)}, target: types.T_uint8, want: []uint8{1, 255, 1}, delta: 0},
			{name: "uint32 from int32 dictionary", node: parquet.Encoded(parquet.Leaf(parquet.Int32Type), &parquet.RLEDictionary), values: []parquet.Value{parquet.Int32Value(3), parquet.Int32Value(400000000), parquet.Int32Value(3)}, target: types.T_uint32, want: []uint32{3, 400000000, 3}, delta: 0},
			{name: "int64 dictionary", node: parquet.Encoded(parquet.Leaf(parquet.Int64Type), &parquet.RLEDictionary), values: []parquet.Value{parquet.Int64Value(-9), parquet.Int64Value(11), parquet.Int64Value(-9)}, target: types.T_int64, want: []int64{-9, 11, -9}, delta: 0},
			{name: "Date_Time_Timestamp/DATE dictionary (days since epoch)", node: parquet.Encoded(parquet.Leaf(parquet.Date().Type()), &parquet.RLEDictionary), values: []parquet.Value{
				parquet.Int32Value(0), parquet.Int32Value(1), parquet.Int32Value(0),
			}, target: types.T_date, want: []types.Date{719162, 719163, 719162}},
			{name: "Date_Time_Timestamp/TIME nanos dictionary (int64 nanos)", node: parquet.Encoded(parquet.Leaf(parquet.Time(parquet.Nanosecond).Type()), &parquet.RLEDictionary), values: []parquet.Value{
				parquet.Int64Value(1_000), parquet.Int64Value(61_000), parquet.Int64Value(1_000),
			}, target: types.T_time, want: []types.Time{types.Time(1), types.Time(61), types.Time(1)}},
			{name: "Date_Time_Timestamp/TIME micros dictionary (int64 micros)", node: parquet.Encoded(parquet.Leaf(parquet.Time(parquet.Microsecond).Type()), &parquet.RLEDictionary), values: []parquet.Value{
				parquet.Int64Value(1_000), parquet.Int64Value(61_000), parquet.Int64Value(1_000),
			}, target: types.T_time, want: []types.Time{types.Time(1_000), types.Time(61_000), types.Time(1_000)}},
			{name: "Date_Time_Timestamp/TIME millis dictionary (int32 millis)", node: parquet.Encoded(parquet.Leaf(parquet.Time(parquet.Millisecond).Type()), &parquet.RLEDictionary), values: []parquet.Value{
				parquet.Int32Value(1000), parquet.Int32Value(61000), parquet.Int32Value(1000),
			}, target: types.T_time, want: []types.Time{types.Time(1000) * 1000, types.Time(61000) * 1000, types.Time(1000) * 1000}},
			{name: "Date_Time_Timestamp/TIMESTAMP nanos dictionary (int64 nanos)", node: parquet.Encoded(parquet.Leaf(parquet.Timestamp(parquet.Nanosecond).Type()), &parquet.RLEDictionary), values: []parquet.Value{
				parquet.Int64Value(1_000), parquet.Int64Value(2_000), parquet.Int64Value(1_000),
			}, target: types.T_timestamp, want: []types.Timestamp{62135596800000001, 62135596800000002, 62135596800000001}},
			{name: "Date_Time_Timestamp/TIMESTAMP micros dictionary (int64 micros)", node: parquet.Encoded(parquet.Leaf(parquet.Timestamp(parquet.Microsecond).Type()), &parquet.RLEDictionary), values: []parquet.Value{
				parquet.Int64Value(0), parquet.Int64Value(1_000_000), parquet.Int64Value(0),
			}, target: types.T_timestamp, want: []types.Timestamp{62135596800000000, 62135596801000000, 62135596800000000}},
			{name: "Date_Time_Timestamp/TIMESTAMP millis dictionary (int64 millis)", node: parquet.Encoded(parquet.Leaf(parquet.Timestamp(parquet.Millisecond).Type()), &parquet.RLEDictionary), values: []parquet.Value{
				parquet.Int64Value(1), parquet.Int64Value(2), parquet.Int64Value(1),
			}, target: types.T_timestamp, want: []types.Timestamp{62135596800001000, 62135596800002000, 62135596800001000}},
			{name: "Datetime/DATETIME nanos dictionary (int64 nanos)", node: parquet.Encoded(parquet.Leaf(parquet.Timestamp(parquet.Nanosecond).Type()), &parquet.RLEDictionary), values: []parquet.Value{
				parquet.Int64Value(1_000), parquet.Int64Value(2_000), parquet.Int64Value(1_000),
			}, target: types.T_datetime, want: []types.Datetime{
				62135596800000001,
				62135596800000002,
				62135596800000001,
			}},
			{name: "Datetime/DATETIME micros dictionary (int64 micros)", node: parquet.Encoded(parquet.Leaf(parquet.Timestamp(parquet.Microsecond).Type()), &parquet.RLEDictionary), values: []parquet.Value{
				parquet.Int64Value(500_000), parquet.Int64Value(1_000_000), parquet.Int64Value(500_000),
			}, target: types.T_datetime, want: []types.Datetime{
				62135596800500000,
				62135596801000000,
				62135596800500000,
			}},
			{name: "Datetime/DATETIME millis dictionary (int64 millis)", node: parquet.Encoded(parquet.Leaf(parquet.Timestamp(parquet.Millisecond).Type()), &parquet.RLEDictionary), values: []parquet.Value{
				parquet.Int64Value(3), parquet.Int64Value(4), parquet.Int64Value(3),
			}, target: types.T_datetime, want: []types.Datetime{
				62135596800003000,
				62135596800004000,
				62135596800003000,
			}},
			{name: "Decimals/DECIMAL64 dictionary", node: parquet.Encoded(parquet.Leaf(parquet.Int64Type), &parquet.RLEDictionary), values: []parquet.Value{
				parquet.Int64Value(7), parquet.Int64Value(-3), parquet.Int64Value(7),
			}, target: types.T_decimal64, want: []types.Decimal64{7, 18446744073709551613, 7}},
			{name: "Decimals/DECIMAL128 dictionary", node: parquet.Encoded(parquet.Leaf(parquet.Int64Type), &parquet.RLEDictionary), values: []parquet.Value{
				parquet.Int64Value(11), parquet.Int64Value(-9), parquet.Int64Value(11),
			}, target: types.T_decimal128, want: []types.Decimal128{
				{B0_63: 11, B64_127: 0},
				{B0_63: 18446744073709551607, B64_127: 18446744073709551615},
				{B0_63: 11, B64_127: 0},
			}},
			{name: "Decimals/DECIMAL256 dictionary", node: parquet.Encoded(parquet.Leaf(parquet.Int64Type), &parquet.RLEDictionary), values: []parquet.Value{
				parquet.Int64Value(13), parquet.Int64Value(-5), parquet.Int64Value(13),
			}, target: types.T_decimal256, want: []types.Decimal256{
				{B0_63: 13, B64_127: 0, B128_191: 0, B192_255: 0},
				{B0_63: 18446744073709551611, B64_127: 18446744073709551615, B128_191: 18446744073709551615, B192_255: 18446744073709551615},
				{B0_63: 13, B64_127: 0, B128_191: 0, B192_255: 0},
			}},
			{name: "raw bytearray varchar", node: parquet.Encoded(parquet.Leaf(parquet.ByteArrayType), &parquet.RLEDictionary), values: []parquet.Value{parquet.ByteArrayValue([]byte("aa")), parquet.ByteArrayValue([]byte("bb")), parquet.ByteArrayValue([]byte("aa"))}, target: types.T_varchar, want: []string{"aa", "bb", "aa"}, nullable: true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				f, page := writeDictAndGetPage(t, tc.node, tc.values)
				require.NotNil(t, page.Dictionary(), "fixture must exercise dictionary decoding")
				vecType := types.New(tc.target, 0, 0)
				vec := vector.NewVec(vecType)
				t.Cleanup(func() { vec.Free(proc.Mp()) })
				var h ParquetHandler
				mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(tc.target), NotNullable: !tc.nullable})
				require.NotNil(t, mp)
				require.NoError(t, mp.mapping(page, proc, vec))
				require.Equal(t, vecType, *vec.GetType())
				require.Equal(t, len(tc.values), vec.Length())
				require.True(t, vec.GetNulls().IsEmpty())
				requireParquetScalarResult(t, vec, tc.want, tc.delta)
			})
		}

	})
}
func TestParquetFloat32Overflow(t *testing.T) {
	proc := testutil.NewProc(t)
	assertOverflow := func(t *testing.T, f *parquet.File, page parquet.Page) {
		t.Helper()
		vec := vector.NewVec(types.T_float32.ToType())
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_float32), NotNullable: true})
		require.NotNil(t, mp)
		require.ErrorContains(t, mp.mapping(page, proc, vec), "overflows FLOAT")
	}

	for _, tc := range []struct {
		name       string
		value      float64
		dictionary bool
	}{
		{name: "plain positive", value: 1e100},
		{name: "plain negative", value: -1e100},
		{name: "dictionary positive", value: 1e100, dictionary: true},
		{name: "dictionary negative", value: -1e100, dictionary: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.dictionary {
				f, page := writeDictAndGetPage(t,
					parquet.Encoded(parquet.Leaf(parquet.DoubleType), &parquet.RLEDictionary),
					[]parquet.Value{parquet.DoubleValue(tc.value)},
				)
				assertOverflow(t, f, page)
				return
			}
			f, page := writeColumnAndGetPage(t, parquet.Leaf(parquet.DoubleType), []parquet.Row{{
				parquet.DoubleValue(tc.value).Level(0, 0, 0),
			}})
			assertOverflow(t, f, page)
		})
	}

	t.Run("decimal positive and negative", func(t *testing.T) {
		magnitude := new(big.Int).Exp(big.NewInt(10), big.NewInt(39), nil)
		for _, tc := range []struct {
			name  string
			value *big.Int
		}{
			{name: "positive", value: magnitude},
			{name: "negative", value: new(big.Int).Neg(magnitude)},
		} {
			t.Run(tc.name, func(t *testing.T) {
				data, err := bigIntToTwosComplementBytes(context.Background(), tc.value, 17)
				require.NoError(t, err)
				f, page := writeColumnAndGetPage(t, parquet.Decimal(0, 40, parquet.FixedLenByteArrayType(17)), []parquet.Row{{
					parquet.FixedLenByteArrayValue(data).Level(0, 0, 0),
				}})
				assertOverflow(t, f, page)
			})
		}
	})

	t.Run("accepts finite boundaries", func(t *testing.T) {
		for _, value := range []float64{math.MaxFloat32, -math.MaxFloat32} {
			converted, err := parquetFloat64ToFloat32(context.Background(), value)
			require.NoError(t, err)
			require.Equal(t, float32(value), converted)
		}
	})

	t.Run("accepts infinities and NaN", func(t *testing.T) {
		for _, value := range []float64{math.Inf(1), math.Inf(-1), math.NaN()} {
			converted, err := parquetFloat64ToFloat32(context.Background(), value)
			require.NoError(t, err)
			if math.IsNaN(value) {
				require.True(t, math.IsNaN(float64(converted)))
			} else {
				require.True(t, math.IsInf(float64(converted), int(math.Copysign(1, value))))
			}
		}
	})
}

func TestParquetFloat32InfinityMapping(t *testing.T) {
	proc := testutil.NewProc(t)
	for _, tc := range []struct {
		name       string
		value      float64
		dictionary bool
		sign       int
	}{
		{name: "plain positive", value: math.Inf(1), sign: 1},
		{name: "plain negative", value: math.Inf(-1), sign: -1},
		{name: "dictionary positive", value: math.Inf(1), dictionary: true, sign: 1},
		{name: "dictionary negative", value: math.Inf(-1), dictionary: true, sign: -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var f *parquet.File
			var page parquet.Page
			if tc.dictionary {
				f, page = writeDictAndGetPage(t, parquet.Encoded(parquet.Leaf(parquet.DoubleType), &parquet.RLEDictionary), []parquet.Value{parquet.DoubleValue(tc.value)})
			} else {
				f, page = writeColumnAndGetPage(t, parquet.Leaf(parquet.DoubleType), []parquet.Row{{parquet.DoubleValue(tc.value).Level(0, 0, 0)}})
			}
			vec := vector.NewVec(types.T_float32.ToType())
			t.Cleanup(func() { vec.Free(proc.Mp()) })
			var h ParquetHandler
			mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_float32), NotNullable: true})
			require.NotNil(t, mp)
			require.NoError(t, mp.mapping(page, proc, vec))
			got := vector.MustFixedColWithTypeCheck[float32](vec)
			require.Len(t, got, 1)
			require.True(t, math.IsInf(float64(got[0]), tc.sign))
		})
	}
}

func TestParquetBoundedVarlenMapping(t *testing.T) {
	proc := testutil.NewProc(t)
	for _, dictionary := range []bool{false, true} {
		for _, tc := range []struct {
			name   string
			target types.T
			want   []byte
		}{
			{name: "binary", target: types.T_binary, want: []byte{'a', 'b', 0, 0}},
			{name: "varbinary", target: types.T_varbinary, want: []byte{'a', 'b'}},
			{name: "char", target: types.T_char, want: []byte{'a', 'b'}},
			{name: "varchar", target: types.T_varchar, want: []byte{'a', 'b'}},
		} {
			t.Run(fmt.Sprintf("%s/dictionary=%t", tc.name, dictionary), func(t *testing.T) {
				var f *parquet.File
				var page parquet.Page
				values := []parquet.Value{parquet.ByteArrayValue([]byte("ab"))}
				if dictionary {
					f, page = writeDictAndGetPage(t, parquet.Encoded(parquet.Leaf(parquet.String().Type()), &parquet.RLEDictionary), values)
					require.NotNil(t, page.Dictionary())
				} else {
					f, page = writeColumnAndGetPage(t, parquet.Leaf(parquet.String().Type()), []parquet.Row{{values[0].Level(0, 0, 0)}})
				}
				vec := vector.NewVec(types.New(tc.target, 4, 0))
				t.Cleanup(func() { vec.Free(proc.Mp()) })
				var h ParquetHandler
				mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(tc.target), Width: 4, NotNullable: true})
				require.NotNil(t, mp)
				require.NoError(t, mp.mapping(page, proc, vec))
				require.Equal(t, tc.want, vec.GetBytesAt(0))
			})
		}
	}

	for _, dictionary := range []bool{false, true} {
		t.Run(fmt.Sprintf("rejects over-width/dictionary=%t", dictionary), func(t *testing.T) {
			values := []parquet.Value{parquet.ByteArrayValue([]byte("ok")), parquet.ByteArrayValue([]byte("toolong"))}
			var f *parquet.File
			var page parquet.Page
			if dictionary {
				f, page = writeDictAndGetPage(t, parquet.Encoded(parquet.Leaf(parquet.String().Type()), &parquet.RLEDictionary), values)
			} else {
				f, page = writeColumnAndGetPage(t, parquet.Leaf(parquet.String().Type()), []parquet.Row{{values[0].Level(0, 0, 0)}, {values[1].Level(0, 0, 0)}})
			}
			vec := vector.NewVec(types.New(types.T_varchar, 4, 0))
			t.Cleanup(func() { vec.Free(proc.Mp()) })
			var h ParquetHandler
			mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_varchar), Width: 4, NotNullable: true})
			require.NotNil(t, mp)
			require.ErrorContains(t, mp.mapping(page, proc, vec), "Dest length 4")
			require.Zero(t, vec.Length(), "page append must roll back after width failure")
		})
	}

	t.Run("row mode uses the same semantics", func(t *testing.T) {
		f, _ := writeColumnAndGetPage(t, parquet.Leaf(parquet.ByteArrayType), []parquet.Row{{parquet.ByteArrayValue([]byte("ab")).Level(0, 0, 0)}})
		col := f.Root().Column("c")
		for _, tc := range []struct {
			target types.T
			want   []byte
		}{
			{target: types.T_binary, want: []byte{'a', 'b', 0, 0}},
			{target: types.T_varbinary, want: []byte{'a', 'b'}},
		} {
			vec := vector.NewVec(types.New(tc.target, 4, 0))
			def := &plan.ColDef{Typ: plan.Type{Id: int32(tc.target), Width: 4}}
			require.NoError(t, appendLeafValue(parquet.ByteArrayValue([]byte("ab")), col, vec, def, proc))
			require.Equal(t, tc.want, vec.GetBytesAt(0))
			vec.Free(proc.Mp())
		}
	})

	t.Run("text width counts UTF-8 runes", func(t *testing.T) {
		f, page := writeColumnAndGetPage(t, parquet.Leaf(parquet.String().Type()), []parquet.Row{{parquet.ByteArrayValue([]byte("你好")).Level(0, 0, 0)}})
		vec := vector.NewVec(types.New(types.T_varchar, 2, 0))
		t.Cleanup(func() { vec.Free(proc.Mp()) })
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_varchar), Width: 2, NotNullable: true})
		require.NoError(t, mp.mapping(page, proc, vec))
		require.Equal(t, []byte("你好"), vec.GetBytesAt(0))
	})

	t.Run("nullable and unbounded values keep their semantics", func(t *testing.T) {
		f, page := writeColumnAndGetPage(t, parquet.Optional(parquet.Leaf(parquet.String().Type())), []parquet.Row{
			{parquet.NullValue().Level(0, 0, 0)},
			{parquet.ByteArrayValue([]byte("a very long value")).Level(0, 1, 0)},
		})
		vec := vector.NewVec(types.New(types.T_blob, 0, 0))
		t.Cleanup(func() { vec.Free(proc.Mp()) })
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_blob), NotNullable: false})
		require.NoError(t, mp.mapping(page, proc, vec))
		require.Equal(t, []uint64{0}, vec.GetNulls().ToArray())
		require.Equal(t, []byte("a very long value"), vec.GetBytesAt(1))
	})
}

func TestParquetCrossTypeHelperCoverage(t *testing.T) {
	ctx := context.Background()

	decimalInt32 := parquet.Decimal(2, 9, parquet.Int32Type).Type()
	require.True(t, isParquetScalarBoolConvertibleSource(decimalInt32))
	require.False(t, isParquetScalarBoolConvertibleSource(parquet.String().Type()))
	require.True(t, isParquetDecimalCastSource(parquet.BooleanType))
	require.False(t, isParquetDecimalCastSource(parquet.String().Type()))
	require.True(t, useRawIntegerDecimalMapping(parquet.Int32Type, 0, 0))
	require.False(t, useRawIntegerDecimalMapping(parquet.BooleanType, 0, 0))
	require.False(t, useRawIntegerDecimalMapping(decimalInt32, 0, 0))
	require.True(t, canUseRawDecimalMapping(decimalInt32, 9, 2))
	require.False(t, canUseRawDecimalMapping(decimalInt32, 8, 2))
	require.False(t, canUseRawDecimalMapping(decimalInt32, 9, 3))

	boolVal, err := parquetValueToBool(ctx, decimalInt32, parquet.Int32Value(100))
	require.NoError(t, err)
	require.True(t, boolVal)
	boolVal, err = parquetValueToBool(ctx, parquet.Int64Type, parquet.Int64Value(0))
	require.NoError(t, err)
	require.False(t, boolVal)
	boolVal, err = parquetValueToBool(ctx, parquet.FloatType, parquet.FloatValue(1))
	require.NoError(t, err)
	require.True(t, boolVal)
	boolVal, err = parquetValueToBool(ctx, parquet.DoubleType, parquet.DoubleValue(0))
	require.NoError(t, err)
	require.False(t, boolVal)
	_, err = parquetValueToBool(ctx, parquet.String().Type(), parquet.ByteArrayValue([]byte("true")))
	require.ErrorContains(t, err, "cannot convert parquet")

	rounded, err := parquetValueToRoundedInt64(ctx, decimalInt32, parquet.Int32Value(12345))
	require.NoError(t, err)
	require.Equal(t, int64(123), rounded)
	rounded, err = parquetValueToRoundedInt64(ctx, parquet.Int64Type, parquet.Int64Value(7))
	require.NoError(t, err)
	require.Equal(t, int64(7), rounded)
	require.Equal(t, big.NewInt(0), roundScaledDecimalBigInt(big.NewInt(999), 4))
	_, err = roundParquetFloatToInt64(ctx, math.NaN())
	require.ErrorContains(t, err, "cannot convert parquet floating point")
	_, err = roundParquetFloatToInt64(ctx, float64(uint64(1)<<63))
	require.ErrorContains(t, err, "overflows BIGINT")

	text, err := parquetValueToDecimalString(ctx, decimalInt32, parquet.Int32Value(12345))
	require.NoError(t, err)
	require.Equal(t, "123.45", text)
	text, err = parquetValueToDecimalString(ctx, parquet.BooleanType, parquet.BooleanValue(false))
	require.NoError(t, err)
	require.Equal(t, "0", text)
	text, err = parquetValueToDecimalString(ctx, parquet.Uint(32).Type(), parquet.ValueOf(uint32(42)))
	require.NoError(t, err)
	require.Equal(t, "42", text)
	text, err = parquetValueToDecimalString(ctx, parquet.Int64Type, parquet.Int64Value(-7))
	require.NoError(t, err)
	require.Equal(t, "-7", text)
	text, err = parquetValueToDecimalString(ctx, parquet.FloatType, parquet.FloatValue(1.5))
	require.NoError(t, err)
	require.Equal(t, "1.5", text)
	text, err = parquetValueToDecimalString(ctx, parquet.DoubleType, parquet.DoubleValue(2.25))
	require.NoError(t, err)
	require.Equal(t, "2.25", text)
	_, err = parquetValueToDecimalString(ctx, parquet.String().Type(), parquet.ByteArrayValue([]byte("1")))
	require.ErrorContains(t, err, "cannot convert parquet")

	jsonVal, err := parquetValueToByteJson(ctx, parquet.Date().Type(), parquet.Int32Value(19723))
	require.NoError(t, err)
	require.Equal(t, `"2024-01-01"`, jsonVal.String())
	jsonVal, err = parquetValueToByteJson(ctx, parquet.Int32Type, parquet.Int32Value(3))
	require.NoError(t, err)
	require.Equal(t, "3", jsonVal.String())
	_, err = parquetValueToByteJson(ctx, parquet.String().Type(), parquet.ByteArrayValue([]byte("bad")))
	require.ErrorContains(t, err, "cannot convert parquet")

	micros, err := parquetTimestampValueToMicros(ctx, parquet.Int64Value(1_000), parquet.Timestamp(parquet.Nanosecond).Type().LogicalType())
	require.NoError(t, err)
	require.Equal(t, int64(1), micros)
	micros, err = parquetTimestampValueToMicros(ctx, parquet.Int64Value(2), parquet.Timestamp(parquet.Microsecond).Type().LogicalType())
	require.NoError(t, err)
	require.Equal(t, int64(2), micros)
	micros, err = parquetTimestampValueToMicros(ctx, parquet.Int64Value(3), parquet.Timestamp(parquet.Millisecond).Type().LogicalType())
	require.NoError(t, err)
	require.Equal(t, int64(3000), micros)
	_, err = parquetTimestampValueToMicros(ctx, parquet.Int64Value(math.MaxInt64), parquet.Timestamp(parquet.Millisecond).Type().LogicalType())
	require.ErrorContains(t, err, "overflows microseconds")
	_, err = parquetTimestampValueToMicros(ctx, parquet.Int64Value(math.MinInt64), parquet.Timestamp(parquet.Millisecond).Type().LogicalType())
	require.ErrorContains(t, err, "overflows microseconds")

	_, err = parquetTimestampValueToDatetime(ctx, parquet.TimestampAdjusted(parquet.Microsecond, false).Type(), parquet.Int64Value(1), time.UTC)
	require.NoError(t, err)
	_, err = parquetTimestampValueToDatetime(ctx, parquet.TimestampAdjusted(parquet.Microsecond, true).Type(), parquet.Int64Value(1), time.UTC)
	require.NoError(t, err)

	proc := testutil.NewProc(t)
	proc.Base.SessionInfo.TimeZone = nil
	require.Equal(t, time.Local, parquetSessionLocation(proc))
	proc.Base.SessionInfo.TimeZone = time.UTC
	require.Equal(t, time.UTC, parquetSessionLocation(proc))

	dec256, err := parseStringToDecimal256("12.34", 10, 2)
	require.NoError(t, err)
	wantDec256, err := types.ParseDecimal256("12.34", 10, 2)
	require.NoError(t, err)
	require.Equal(t, wantDec256, dec256)
	_, err = parseStringToDecimal256("", 10, 2)
	require.ErrorContains(t, err, "empty string")
}

func TestParquetTimestampLogicalMissingUnit(t *testing.T) {
	ctx := context.Background()
	lt := &format.LogicalType{Timestamp: &format.TimestampType{}}
	v := parquet.Int64Value(1)

	_, err := parquetTimestampValueToMicros(ctx, v, lt)
	require.ErrorContains(t, err, "missing parquet timestamp unit")

	_, err = parquetTimestampValueToString(ctx, v, lt)
	require.ErrorContains(t, err, "missing parquet timestamp unit")
}

func TestParquetTimestampMillisOverflowInFastPath(t *testing.T) {
	proc := testutil.NewProc(t)
	node := parquet.Timestamp(parquet.Millisecond)
	cases := []struct {
		name string
	}{
		{
			name: "plain",
		},
		{
			name: "dictionary",
		},
	}
	for i := range cases {
		t.Run(cases[i].name, func(t *testing.T) {
			var page parquet.Page
			var file *parquet.File
			if cases[i].name == "dictionary" {
				file, page = writeDictAndGetPage(t, parquet.Encoded(node, &parquet.RLEDictionary), []parquet.Value{
					parquet.Int64Value(math.MaxInt64),
				})
			} else {
				file, page = writeColumnAndGetPage(t, node, []parquet.Row{
					{parquet.Int64Value(math.MaxInt64).Level(0, 0, 0)},
				})
			}
			vec := vector.NewVec(types.T_timestamp.ToType())
			var h ParquetHandler
			mp := h.getMapper(file.Root().Column("c"), plan.Type{Id: int32(types.T_timestamp), NotNullable: true})
			require.NotNil(t, mp)
			err := mp.mapping(page, proc, vec)
			require.ErrorContains(t, err, "overflows microseconds")
			require.Zero(t, vec.Length())
		})
	}
}

// fakeFS is a minimal ETL-compatible FileService for testing fsReaderAt.
type fakeFS struct {
	b           []byte
	readErr     error
	readLatency time.Duration
	shortRead   bool

	lastPolicy          fileservice.Policy
	lastOffset          int64
	lastSize            int64
	readCount           int64
	logicalRead         int64
	simulatedRemoteRead int64
}

func (f *fakeFS) Name() string                                            { return "fake" }
func (f *fakeFS) Write(ctx context.Context, v fileservice.IOVector) error { return nil }
func (f *fakeFS) Read(ctx context.Context, v *fileservice.IOVector) error {
	if len(v.Entries) == 0 {
		return moerr.NewInternalError(ctx, "empty entries")
	}
	if f.readErr != nil {
		return f.readErr
	}
	if f.readLatency > 0 {
		time.Sleep(f.readLatency)
	}
	e := &v.Entries[0]
	f.lastPolicy = v.Policy
	f.lastOffset = e.Offset
	if e.Size < 0 {
		e.Size = int64(len(f.b)) - e.Offset
	}
	if f.shortRead && e.Size > 0 {
		e.Size--
	}
	f.lastSize = e.Size
	if int(e.Offset+e.Size) > len(f.b) {
		return io.EOF
	}
	f.readCount++
	f.logicalRead += e.Size
	if v.Policy.CacheFullFile() && !v.Policy.Any(fileservice.SkipDiskCache) {
		f.simulatedRemoteRead += int64(len(f.b))
	} else {
		f.simulatedRemoteRead += e.Size
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

func writeParquetFixture(t *testing.T, node parquet.Node, rows []parquet.Row) *parquet.File {
	t.Helper()
	var buf bytes.Buffer
	schema := parquet.NewSchema("x", parquet.Group{"c": node})
	// The lexical defer also closes a partially written fixture on FailNow.
	func() {
		w := parquet.NewWriter(&buf, schema)
		defer func() { require.NoError(t, w.Close()) }()
		_, err := w.WriteRows(rows)
		require.NoError(t, err)
	}()
	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	return f
}

func readParquetFixturePage(t *testing.T, col *parquet.Column) parquet.Page {
	t.Helper()
	require.NotNil(t, col)
	pages := col.Pages()
	defer func() { require.NoError(t, pages.Close()) }()
	page, err := pages.ReadPage()
	if page != nil {
		t.Cleanup(func() { parquet.Release(page) })
	}
	require.NoError(t, err)
	require.NotNil(t, page)
	return page
}

func writeListNodeAndGetPage(t *testing.T, listNode parquet.Node, rows []parquet.Row) (file *parquet.File, page parquet.Page) {
	t.Helper()
	f := writeParquetFixture(t, listNode, rows)
	col := f.Root().Column("c")
	require.NotNil(t, col)
	require.False(t, col.Leaf())
	leaf, ok := parquetListElementLeaf(col)
	require.True(t, ok)
	return f, readParquetFixturePage(t, leaf)
}

func writeColumnAndGetPage(t *testing.T, node parquet.Node, rows []parquet.Row) (file *parquet.File, page parquet.Page) {
	t.Helper()
	f := writeParquetFixture(t, node, rows)
	return f, readParquetFixturePage(t, f.Root().Column("c"))
}

// Write a required single column; the node determines its encoding.
func writeDictAndGetPage(t *testing.T, node parquet.Node, values []parquet.Value) (file *parquet.File, page parquet.Page) {
	t.Helper()
	normalized := make([]parquet.Value, len(values))
	for i, value := range values {
		normalized[i] = value.Level(0, 0, 0)
	}
	return writeColumnAndGetPage(t, node, []parquet.Row{parquet.MakeRow(normalized)})
}

func requireParquetScalarResult(t *testing.T, vec *vector.Vector, expected any, delta float64) {
	t.Helper()
	switch want := expected.(type) {
	case []int8:
		require.Equal(t, want, vector.MustFixedColWithTypeCheck[int8](vec))
	case []int16:
		require.Equal(t, want, vector.MustFixedColWithTypeCheck[int16](vec))
	case []int32:
		require.Equal(t, want, vector.MustFixedColWithTypeCheck[int32](vec))
	case []int64:
		require.Equal(t, want, vector.MustFixedColWithTypeCheck[int64](vec))
	case []uint8:
		require.Equal(t, want, vector.MustFixedColWithTypeCheck[uint8](vec))
	case []uint16:
		require.Equal(t, want, vector.MustFixedColWithTypeCheck[uint16](vec))
	case []uint32:
		require.Equal(t, want, vector.MustFixedColWithTypeCheck[uint32](vec))
	case []uint64:
		require.Equal(t, want, vector.MustFixedColWithTypeCheck[uint64](vec))
	case []float32:
		got := vector.MustFixedColWithTypeCheck[float32](vec)
		if delta == 0 {
			require.Equal(t, want, got)
		} else {
			require.InDeltaSlice(t, want, got, delta)
		}
	case []float64:
		got := vector.MustFixedColWithTypeCheck[float64](vec)
		if delta == 0 {
			require.Equal(t, want, got)
		} else {
			require.InDeltaSlice(t, want, got, delta)
		}
	case []bool:
		require.Equal(t, want, vector.MustFixedColWithTypeCheck[bool](vec))
	case []types.Date:
		require.Equal(t, want, vector.MustFixedColWithTypeCheck[types.Date](vec))
	case []types.Time:
		require.Equal(t, want, vector.MustFixedColWithTypeCheck[types.Time](vec))
	case []types.Timestamp:
		require.Equal(t, want, vector.MustFixedColWithTypeCheck[types.Timestamp](vec))
	case []types.Datetime:
		require.Equal(t, want, vector.MustFixedColWithTypeCheck[types.Datetime](vec))
	case []types.Decimal64:
		require.Equal(t, want, vector.MustFixedColWithTypeCheck[types.Decimal64](vec))
	case []types.Decimal128:
		require.Equal(t, want, vector.MustFixedColWithTypeCheck[types.Decimal128](vec))
	case []types.Decimal256:
		require.Equal(t, want, vector.MustFixedColWithTypeCheck[types.Decimal256](vec))
	case []string:
		require.Equal(t, len(want), vec.Length())
		for i, value := range want {
			require.Equal(t, value, vec.GetStringAt(i))
		}
	default:
		t.Fatalf("unsupported scalar expectation %T", expected)
	}
}

func TestParquetTimestampToDatetimeUsesSessionTimeZone(t *testing.T) {
	loc, err := time.LoadLocation("Asia/Shanghai")
	require.NoError(t, err)
	proc := testutil.NewProc(t)
	proc.Base.SessionInfo.TimeZone = loc

	micros := time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC).UnixMicro()
	for _, tc := range []struct {
		name  string
		unit  parquet.TimeUnit
		value int64
	}{
		{name: "nanos", unit: parquet.Nanosecond, value: micros * 1000},
		{name: "micros", unit: parquet.Microsecond, value: micros},
		{name: "millis", unit: parquet.Millisecond, value: micros / 1000},
	} {
		for _, adjusted := range []bool{true, false} {
			for _, dictionary := range []bool{false, true} {
				name := fmt.Sprintf("%s/adjusted=%t/dictionary=%t", tc.name, adjusted, dictionary)
				t.Run(name, func(t *testing.T) {
					node := parquet.TimestampAdjusted(tc.unit, adjusted)
					var f *parquet.File
					var page parquet.Page
					if dictionary {
						f, page = writeDictAndGetPage(t, parquet.Encoded(node, &parquet.RLEDictionary), []parquet.Value{
							parquet.Int64Value(tc.value),
							parquet.Int64Value(tc.value),
						})
					} else {
						f, page = writeColumnAndGetPage(t, node, []parquet.Row{{
							parquet.Int64Value(tc.value).Level(0, 0, 0),
						}})
					}

					vec := vector.NewVec(types.T_datetime.ToType())
					var h ParquetHandler
					mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_datetime), NotNullable: true})
					require.NotNil(t, mp)
					require.NoError(t, mp.mapping(page, proc, vec))

					wantText := "2024-01-01 00:00:00"
					if adjusted {
						wantText = "2024-01-01 08:00:00"
					}
					want, err := types.ParseDatetime(wantText, 0)
					require.NoError(t, err)
					got := vector.MustFixedColWithTypeCheck[types.Datetime](vec)
					require.NotEmpty(t, got)
					for _, value := range got {
						require.Equal(t, want, value)
					}
				})
			}
		}
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
	require.True(t, shouldPrefetchS3Parquet(tree.S3, true, maxParquetS3PrefetchSize, false, false))
	require.False(t, shouldPrefetchS3Parquet(tree.S3, true, maxParquetS3PrefetchSize+1, false, false))
	require.False(t, shouldPrefetchS3Parquet(tree.S3, false, maxParquetS3PrefetchSize, false, false))
	require.False(t, shouldPrefetchS3Parquet(tree.INFILE, true, maxParquetS3PrefetchSize, false, false))
	require.False(t, shouldPrefetchS3Parquet(tree.S3, true, -1, false, false))
	require.False(t, shouldPrefetchS3Parquet(tree.S3, true, 80*1024*1024, true, false))
	// A whole-file fanout can run up to the bounded execution DOP concurrently.
	// It must use direct ReaderAt reads rather than retain one complete object
	// per active scope.
	require.False(t, shouldPrefetchS3Parquet(tree.S3, true, maxParquetS3PrefetchSize, false, true))
}

func TestParquetWholeFileFanoutPrepareRequiresCompatibleProtocol(t *testing.T) {
	proc := testutil.NewProc(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	previous, hadPrevious := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if hadPrevious {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, previous)
		} else {
			rt.CompareAndDeleteGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion44)
		}
	})

	external := NewArgument().WithEs(&ExternalParam{ExParamConst: ExParamConst{ParquetWholeFileFanout: true}})
	proc.Ctx = context.WithValue(proc.Ctx, defines.RemoteRunContext{}, true)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion44)
	err := external.Prepare(proc)
	require.ErrorContains(t, err, "MORPC protocol version 45")

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion45)
	require.NoError(t, validateParquetWholeFileFanoutProtocol(proc, external.Es))
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
	// Test that optional column can be prepared to map to NOT NULL column
	// The NULL constraint violation will be checked at runtime when actual NULLs are encountered
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
	// prepare should succeed - NULL constraint is checked at runtime, not prepare time
	err = h.prepare(param)
	require.NoError(t, err)
}

func TestParquet_ensureDictionaryIndexes_outOfRange(t *testing.T) {
	ctx := context.Background()
	err := ensureDictionaryIndexes(ctx, 3, []int32{0, 1, 5})
	require.Error(t, err)
	require.Contains(t, err.Error(), "out of range")
	err = ensureDictionaryIndexes(ctx, 3, []int32{-1})
	require.Error(t, err)
	require.Contains(t, err.Error(), "out of range")
	err = ensureDictionaryIndexes(ctx, -1, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "dictionary length -1 is invalid")
}

func TestParquet_Dictionary_Bool(t *testing.T) {
	proc := testutil.NewProc(t)

	t.Run("required", func(t *testing.T) {
		node := parquet.Encoded(parquet.Leaf(parquet.BooleanType), &parquet.RLEDictionary)
		vals := []parquet.Value{
			parquet.BooleanValue(true), parquet.BooleanValue(false), parquet.BooleanValue(true),
		}
		f, page := writeDictAndGetPage(t, node, vals)
		require.NotNil(t, page.Dictionary())

		vec := vector.NewVec(types.New(types.T_bool, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_bool), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		require.Equal(t, []bool{true, false, true}, vector.MustFixedColWithTypeCheck[bool](vec))
	})

	t.Run("nullable", func(t *testing.T) {
		node := parquet.Optional(parquet.Encoded(parquet.Leaf(parquet.BooleanType), &parquet.RLEDictionary))
		rows := []parquet.Row{
			{parquet.BooleanValue(true).Level(0, 1, 0)},
			{parquet.NullValue().Level(0, 0, 0)},
			{parquet.BooleanValue(false).Level(0, 1, 0)},
			{parquet.BooleanValue(true).Level(0, 1, 0)},
		}
		f, page := writeColumnAndGetPage(t, node, rows)
		require.NotNil(t, page.Dictionary())

		vec := vector.NewVec(types.New(types.T_bool, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_bool)})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		require.Equal(t, []bool{true, false, false, true}, vector.MustFixedColWithTypeCheck[bool](vec))
		require.True(t, vec.GetNulls().Contains(1))
		require.False(t, vec.GetNulls().Contains(0))
		require.False(t, vec.GetNulls().Contains(2))
		require.False(t, vec.GetNulls().Contains(3))
	})
}

func TestParquet_Plain_Bool(t *testing.T) {
	proc := testutil.NewProc(t)

	t.Run("required sliced page", func(t *testing.T) {
		node := parquet.Leaf(parquet.BooleanType)
		rows := []parquet.Row{
			{parquet.BooleanValue(true).Level(0, 0, 0)},
			{parquet.BooleanValue(false).Level(0, 0, 0)},
			{parquet.BooleanValue(true).Level(0, 0, 0)},
			{parquet.BooleanValue(false).Level(0, 0, 0)},
		}
		f, page := writeColumnAndGetPage(t, node, rows)
		require.Nil(t, page.Dictionary())
		page = page.Slice(1, 4)
		slicedPage := page
		t.Cleanup(func() { parquet.Release(slicedPage) })

		vec := vector.NewVec(types.New(types.T_bool, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_bool), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		require.Equal(t, []bool{false, true, false}, vector.MustFixedColWithTypeCheck[bool](vec))
	})

	t.Run("nullable sliced page", func(t *testing.T) {
		node := parquet.Optional(parquet.Leaf(parquet.BooleanType))
		rows := []parquet.Row{
			{parquet.BooleanValue(true).Level(0, 1, 0)},
			{parquet.NullValue().Level(0, 0, 0)},
			{parquet.BooleanValue(false).Level(0, 1, 0)},
			{parquet.BooleanValue(true).Level(0, 1, 0)},
			{parquet.NullValue().Level(0, 0, 0)},
		}
		f, page := writeColumnAndGetPage(t, node, rows)
		require.Nil(t, page.Dictionary())
		page = page.Slice(1, 5)
		slicedPage := page
		t.Cleanup(func() { parquet.Release(slicedPage) })

		vec := vector.NewVec(types.New(types.T_bool, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_bool)})
		require.NotNil(t, mp)
		seedFile, seedPage := writeColumnAndGetPage(t, parquet.Leaf(parquet.BooleanType), []parquet.Row{
			{parquet.BooleanValue(true).Level(0, 0, 0)},
			{parquet.BooleanValue(true).Level(0, 0, 0)},
			{parquet.BooleanValue(true).Level(0, 0, 0)},
			{parquet.BooleanValue(true).Level(0, 0, 0)},
		})
		seedMapper := h.getMapper(seedFile.Root().Column("c"), plan.Type{Id: int32(types.T_bool), NotNullable: true})
		require.NoError(t, seedMapper.mapping(seedPage, proc, vec))
		vec.ResetWithSameType()
		require.NoError(t, mp.mapping(page, proc, vec))
		require.Equal(t, []bool{false, false, true, false}, vector.MustFixedColWithTypeCheck[bool](vec))
		require.True(t, vec.GetNulls().Contains(0))
		require.False(t, vec.GetNulls().Contains(1))
		require.False(t, vec.GetNulls().Contains(2))
		require.True(t, vec.GetNulls().Contains(3))
	})
}

type parquetPageWithData struct {
	parquet.Page
	data encoding.Values
}

func (p *parquetPageWithData) Data() encoding.Values {
	return p.data
}

type parquetPageWithValues struct {
	parquet.Page
	values parquet.ValueReader
}

func (p *parquetPageWithValues) Values() parquet.ValueReader {
	return p.values
}

type parquetBooleanReaderFunc func([]bool) (int, error)

func (f parquetBooleanReaderFunc) ReadBooleans(values []bool) (int, error) {
	return f(values)
}

func (f parquetBooleanReaderFunc) ReadValues([]parquet.Value) (int, error) {
	return 0, io.EOF
}

type parquetPageWithDefinitionLevels struct {
	parquet.Page
	levels   []byte
	numNulls int64
}

func (p *parquetPageWithDefinitionLevels) DefinitionLevels() []byte {
	return p.levels
}

func (p *parquetPageWithDefinitionLevels) NumNulls() int64 {
	return p.numNulls
}

type parquetPageWithNumValues struct {
	parquet.Page
	numValues int64
}

func (p *parquetPageWithNumValues) NumValues() int64 {
	return p.numValues
}

type parquetPageWithNumRows struct {
	parquet.Page
	numRows int64
}

func (p *parquetPageWithNumRows) NumRows() int64 {
	return p.numRows
}

type parquetPageWithDictionary struct {
	parquet.Page
	dictionary parquet.Dictionary
}

func (p *parquetPageWithDictionary) Dictionary() parquet.Dictionary {
	return p.dictionary
}

type parquetRowGroupWithNumRows struct {
	parquet.RowGroup
	numRows int64
}

func (r *parquetRowGroupWithNumRows) NumRows() int64 {
	return r.numRows
}

func TestParquetRowCountOnlyRejectsInvalidRowGroup(t *testing.T) {
	param := &ExternalParam{ExParamConst: ExParamConst{Ctx: context.Background()}}
	bat := batch.NewWithSize(0)
	h := &ParquetHandler{
		batchCnt: 1,
		rowGroups: []parquet.RowGroup{
			&parquetRowGroupWithNumRows{numRows: -1},
		},
	}

	err := h.getDataRowCountOnly(bat, param)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
	require.Contains(t, err.Error(), "row group: NumRows() -1 is negative")
	require.Zero(t, bat.RowCount())
}

func TestParquetPageModeEOFRequiresCompleteRowGroup(t *testing.T) {
	ctx := context.Background()
	require.ErrorContains(t,
		validateParquetPageModeEOF(ctx, 1, 2),
		"page columns ended after 1 rows, expected 2")
	require.NoError(t, validateParquetPageModeEOF(ctx, 2, 2))
	require.ErrorContains(t, validateParquetPageModeEOF(ctx, 0, -1), "NumRows() -1 is negative")
}

func TestParquetRowModeEOFRequiresCompleteRowGroup(t *testing.T) {
	ctx := context.Background()
	require.ErrorContains(t,
		validateParquetRowModeEOF(ctx, 1, 2),
		"row reader ended after 1 rows, expected 2")
	require.NoError(t, validateParquetRowModeEOF(ctx, 2, 2))
	require.ErrorContains(t, validateParquetRowModeEOF(ctx, 3, 2), "row reader position 3")
	require.ErrorContains(t, validateParquetRowModeEOF(ctx, 0, -1), "NumRows() -1 is negative")
}

func TestParquetRowModeZeroBatchCountDoesNotAllocateNegativeBuffer(t *testing.T) {
	proc := testutil.NewProc(t)
	param := &ExternalParam{ExParamConst: ExParamConst{Ctx: context.Background()}}
	bat := batch.NewWithSize(0)
	h := &ParquetHandler{batchCnt: -1}

	require.NoError(t, h.getDataByRow(bat, param, proc))
	require.Zero(t, bat.RowCount())
}

func TestParquetPageRowsStayWithinRowGroup(t *testing.T) {
	ctx := context.Background()
	require.NoError(t, validateParquetPageRows(ctx, 2, 1, 3, 5))
	require.ErrorContains(t, validateParquetPageRows(ctx, 3, 0, 3, 5), "row group has 5 rows")
	require.ErrorContains(t, validateParquetPageRows(ctx, 2, 3, 0, 5), "page offset 3")
	require.ErrorContains(t, validateParquetPageRows(ctx, 1, 0, 6, 5), "row position 6")
}

func TestParquetRowModeLeafConversionsRejectOverflow(t *testing.T) {
	proc := testutil.NewProc(t)
	int32File, _ := writeColumnAndGetPage(t, parquet.Leaf(parquet.Int32Type), []parquet.Row{
		{parquet.Int32Value(256).Level(0, 0, 0)},
	})
	int64File, _ := writeColumnAndGetPage(t, parquet.Leaf(parquet.Int64Type), []parquet.Row{
		{parquet.Int64Value(math.MaxInt64).Level(0, 0, 0)},
	})

	cases := []struct {
		name   string
		col    *parquet.Column
		value  parquet.Value
		target types.T
		want   string
	}{
		{name: "int8", col: int32File.Root().Column("c"), value: parquet.Int32Value(128), target: types.T_int8, want: "overflows TINYINT"},
		{name: "uint8", col: int32File.Root().Column("c"), value: parquet.Int32Value(-1), target: types.T_uint8, want: "negative parquet value"},
		{name: "int32 from int64", col: int64File.Root().Column("c"), value: parquet.Int64Value(math.MaxInt64), target: types.T_int32, want: "overflows INT"},
		{name: "uint32", col: int64File.Root().Column("c"), value: parquet.Int64Value(1 << 32), target: types.T_uint32, want: "overflows INT UNSIGNED"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			vec := vector.NewVec(types.New(tc.target, 0, 0))
			def := &plan.ColDef{Typ: plan.Type{Id: int32(tc.target)}}
			err := appendLeafValue(tc.value, tc.col, vec, def, proc)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.want)
			require.Zero(t, vec.Length())
		})
	}
}

func TestParquetRowModeLeafDefinitionLevelMatchesNullness(t *testing.T) {
	proc := testutil.NewProc(t)
	f, _ := writeColumnAndGetPage(t, parquet.Optional(parquet.Leaf(parquet.Int32Type)), []parquet.Row{
		{parquet.Int32Value(1).Level(0, 1, 0)},
	})
	col := f.Root().Column("c")
	def := &plan.ColDef{Typ: plan.Type{Id: int32(types.T_int32)}}

	for _, tc := range []struct {
		name  string
		value parquet.Value
		want  string
	}{
		{name: "value at null level", value: parquet.Int32Value(1).Level(0, 0, 0), want: "NULL status disagrees"},
		{name: "null at value level", value: parquet.NullValue().Level(0, 1, 0), want: "NULL status disagrees"},
		{name: "definition level overflow", value: parquet.Int32Value(1).Level(0, 2, 0), want: "exceeds maximum"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			vec := vector.NewVec(types.T_int32.ToType())
			h := &ParquetHandler{}
			err := h.processLeafValue(parquet.Row{tc.value}, col, vec, def, proc)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.want)
			require.Zero(t, vec.Length())
		})
	}

	t.Run("missing nullable leaf appends null", func(t *testing.T) {
		vec := vector.NewVec(types.T_int32.ToType())
		t.Cleanup(func() { vec.Free(proc.Mp()) })
		h := &ParquetHandler{}
		require.NoError(t, h.processLeafValue(parquet.Row{}, col, vec, def, proc))
		require.Equal(t, 1, vec.Length())
		require.True(t, vec.GetNulls().Contains(0))
	})
}

func TestParquetNestedNullUsesColumnDefinitionLevel(t *testing.T) {
	schema := parquet.NewSchema("x", parquet.Group{
		"outer": parquet.Optional(parquet.Group{
			"nested": parquet.Optional(parquet.Group{
				"value": parquet.Leaf(parquet.Int32Type),
			}),
		}),
	})
	var buf bytes.Buffer
	w := parquet.NewWriter(&buf, schema)
	require.NoError(t, w.Close())
	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	col := f.Root().Column("outer").Column("nested")
	leaf := col.Column("value")
	require.True(t, col.Optional())
	require.Greater(t, col.MaxDefinitionLevel(), 1)

	nullAtOuterLevel := parquet.Int32Value(1).Level(0, 1, leaf.Index())
	require.True(t, isNestedColumnNull([]parquet.Value{nullAtOuterLevel}, col))

	valueAtMaxLevel := parquet.Int32Value(1).Level(0, col.MaxDefinitionLevel(), leaf.Index())
	require.False(t, isNestedColumnNull([]parquet.Value{valueAtMaxLevel}, col))
}

func TestParquetNestedValuesRejectInvalidLevels(t *testing.T) {
	schema := parquet.NewSchema("x", parquet.Group{
		"outer": parquet.Group{
			"value": parquet.Leaf(parquet.Int32Type),
		},
	})
	var buf bytes.Buffer
	w := parquet.NewWriter(&buf, schema)
	require.NoError(t, w.Close())
	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	col := f.Root().Column("outer")
	leaf := col.Column("value")

	for _, tc := range []struct {
		name  string
		value parquet.Value
		want  string
	}{
		{name: "definition level", value: parquet.Int32Value(1).Level(0, 1, leaf.Index()), want: "exceeds maximum"},
		{name: "null mismatch", value: parquet.NullValue().Level(0, 0, leaf.Index()), want: "NULL status disagrees"},
		{name: "repetition level", value: parquet.Int32Value(1).Level(1, 0, leaf.Index()), want: "repetition level"},
		{name: "value kind", value: parquet.BooleanValue(true).Level(0, 0, leaf.Index()), want: "value kind BOOLEAN"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateParquetNestedValues(context.Background(), col, []parquet.Value{tc.value})
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
			require.Contains(t, err.Error(), tc.want)
		})
	}
}

func TestParquetNestedOptionalGroupNullIsPreserved(t *testing.T) {
	schema := parquet.NewSchema("x", parquet.Group{
		"outer": parquet.Group{
			"nested": parquet.Optional(parquet.Group{
				"value": parquet.Leaf(parquet.Int32Type),
			}),
		},
	})
	var buf bytes.Buffer
	w := parquet.NewWriter(&buf, schema)
	require.NoError(t, w.Close())
	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	outer := f.Root().Column("outer")
	leaf := outer.Column("nested").Column("value")

	value, err := reconstructNestedByType(context.Background(), outer, []parquet.Value{
		parquet.NullValue().Level(0, 0, leaf.Index()),
	})
	require.NoError(t, err)
	result, ok := value.(map[string]any)
	require.True(t, ok)
	require.Contains(t, result, "nested")
	require.Nil(t, result["nested"])
}

func TestParquetListOfNestedAlignsOptionalFieldsByElement(t *testing.T) {
	schema := parquet.NewSchema("x", parquet.Group{
		"items": parquet.List(parquet.Group{
			"a": parquet.Optional(parquet.Leaf(parquet.Int32Type)),
			"b": parquet.Optional(parquet.Leaf(parquet.Int32Type)),
		}),
	})
	var buf bytes.Buffer
	w := parquet.NewWriter(&buf, schema)
	require.NoError(t, w.Close())
	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	element := f.Root().Column("items").Column("list").Column("element")
	items := f.Root().Column("items")
	a := element.Column("a")
	b := element.Column("b")

	got, err := reconstructNestedValue(context.Background(), items, []parquet.Value{
		parquet.Int32Value(10).Level(0, a.MaxDefinitionLevel(), a.Index()),
		parquet.NullValue().Level(1, a.MaxDefinitionLevel()-1, a.Index()),
		parquet.NullValue().Level(0, b.MaxDefinitionLevel()-1, b.Index()),
		parquet.Int32Value(20).Level(1, b.MaxDefinitionLevel(), b.Index()),
	})
	require.NoError(t, err)
	require.Equal(t, []any{
		map[string]any{"a": int64(10), "b": nil},
		map[string]any{"a": nil, "b": int64(20)},
	}, got)
}

func TestParquetMapOfNestedAlignsOptionalValuesByEntry(t *testing.T) {
	schema := parquet.NewSchema("x", parquet.Group{
		"m": parquet.Map(parquet.String(), parquet.Group{
			"a": parquet.Optional(parquet.Leaf(parquet.Int32Type)),
			"b": parquet.Optional(parquet.Leaf(parquet.Int32Type)),
		}),
	})
	var buf bytes.Buffer
	w := parquet.NewWriter(&buf, schema)
	require.NoError(t, w.Close())
	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	kv := f.Root().Column("m").Column("key_value")
	key := kv.Column("key")
	value := kv.Column("value")
	a := value.Column("a")
	b := value.Column("b")

	got, err := reconstructMap(context.Background(), kv, []parquet.Value{
		parquet.ByteArrayValue([]byte("first")).Level(0, key.MaxDefinitionLevel(), key.Index()),
		parquet.ByteArrayValue([]byte("second")).Level(1, key.MaxDefinitionLevel(), key.Index()),
		parquet.Int32Value(10).Level(0, a.MaxDefinitionLevel(), a.Index()),
		parquet.NullValue().Level(1, a.MaxDefinitionLevel()-1, a.Index()),
		parquet.NullValue().Level(0, b.MaxDefinitionLevel()-1, b.Index()),
		parquet.Int32Value(20).Level(1, b.MaxDefinitionLevel(), b.Index()),
	})
	require.NoError(t, err)
	require.Equal(t, map[string]any{
		"first":  map[string]any{"a": int64(10), "b": nil},
		"second": map[string]any{"a": nil, "b": int64(20)},
	}, got)
}

func TestParquetListPreservesNullElement(t *testing.T) {
	schema := parquet.NewSchema("x", parquet.Group{
		"items": parquet.List(parquet.Optional(parquet.Leaf(parquet.Int32Type))),
	})
	var buf bytes.Buffer
	w := parquet.NewWriter(&buf, schema)
	require.NoError(t, w.Close())
	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	items := f.Root().Column("items")
	element := items.Column("list").Column("element")
	emptyLevel := listEmptyDefinitionLevel(element)

	empty, err := reconstructNestedValue(context.Background(), items, []parquet.Value{
		parquet.NullValue().Level(0, emptyLevel, element.Index()),
	})
	require.NoError(t, err)
	require.Equal(t, []any{}, empty)

	withNull, err := reconstructNestedValue(context.Background(), items, []parquet.Value{
		parquet.NullValue().Level(0, emptyLevel+1, element.Index()),
	})
	require.NoError(t, err)
	require.Equal(t, []any{nil}, withNull)
}

func TestParquetNestedEmptyListMarkers(t *testing.T) {
	t.Run("logical list of optional struct", func(t *testing.T) {
		schema := parquet.NewSchema("x", parquet.Group{
			"items": parquet.List(parquet.Optional(parquet.Group{
				"value": parquet.Optional(parquet.Leaf(parquet.Int32Type)),
				"other": parquet.Optional(parquet.Leaf(parquet.Int32Type)),
			})),
		})
		var buf bytes.Buffer
		w := parquet.NewWriter(&buf, schema)
		require.NoError(t, w.Close())
		f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		require.NoError(t, err)
		items := f.Root().Column("items")
		element := items.Column("list").Column("element")
		leaf := element.Column("value")
		other := element.Column("other")

		got, err := reconstructNestedValue(context.Background(), items, []parquet.Value{
			parquet.NullValue().Level(0, listEmptyDefinitionLevel(element), leaf.Index()),
			parquet.NullValue().Level(0, listEmptyDefinitionLevel(element), other.Index()),
		})
		require.NoError(t, err)
		require.Equal(t, []any{}, got)
	})

	t.Run("unannotated list pattern", func(t *testing.T) {
		schema := parquet.NewSchema("x", parquet.Group{
			"items": parquet.Group{
				"list": parquet.Repeated(parquet.Group{
					"element": parquet.Optional(parquet.Group{
						"value": parquet.Leaf(parquet.Int32Type),
					}),
				}),
			},
		})
		var buf bytes.Buffer
		w := parquet.NewWriter(&buf, schema)
		require.NoError(t, w.Close())
		f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		require.NoError(t, err)
		items := f.Root().Column("items")
		element := items.Column("list").Column("element")
		leaf := element.Column("value")

		got, err := reconstructNestedValue(context.Background(), items, []parquet.Value{
			parquet.NullValue().Level(0, listEmptyDefinitionLevel(element), leaf.Index()),
		})
		require.NoError(t, err)
		require.Equal(t, []any{}, got)
	})
}

func TestParquetMapOfNestedListAndScalarAlignsByEntry(t *testing.T) {
	schema := parquet.NewSchema("x", parquet.Group{
		"m": parquet.Map(parquet.String(), parquet.Group{
			"a": parquet.List(parquet.Leaf(parquet.Int32Type)),
			"b": parquet.Optional(parquet.Leaf(parquet.Int32Type)),
		}),
	})
	var buf bytes.Buffer
	w := parquet.NewWriter(&buf, schema)
	require.NoError(t, w.Close())
	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	m := f.Root().Column("m")
	kv := m.Column("key_value")
	key := kv.Column("key")
	value := kv.Column("value")
	a := value.Column("a").Column("list").Column("element")
	b := value.Column("b")

	got, err := reconstructNestedValue(context.Background(), m, []parquet.Value{
		parquet.ByteArrayValue([]byte("first")).Level(0, key.MaxDefinitionLevel(), key.Index()),
		parquet.ByteArrayValue([]byte("second")).Level(1, key.MaxDefinitionLevel(), key.Index()),
		parquet.Int32Value(1).Level(0, a.MaxDefinitionLevel(), a.Index()),
		parquet.Int32Value(2).Level(2, a.MaxDefinitionLevel(), a.Index()),
		parquet.Int32Value(3).Level(1, a.MaxDefinitionLevel(), a.Index()),
		parquet.Int32Value(10).Level(0, b.MaxDefinitionLevel(), b.Index()),
		parquet.Int32Value(20).Level(1, b.MaxDefinitionLevel(), b.Index()),
	})
	require.NoError(t, err)
	require.Equal(t, map[string]any{
		"first":  map[string]any{"a": []any{int64(1), int64(2)}, "b": int64(10)},
		"second": map[string]any{"a": []any{int64(3)}, "b": int64(20)},
	}, got)
}

func TestParquetEmptyMapMarkers(t *testing.T) {
	t.Run("scalar value", func(t *testing.T) {
		schema := parquet.NewSchema("x", parquet.Group{
			"m": parquet.Map(parquet.String(), parquet.Optional(parquet.String())),
		})
		var buf bytes.Buffer
		w := parquet.NewWriter(&buf, schema)
		require.NoError(t, w.Close())
		f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		require.NoError(t, err)
		m := f.Root().Column("m")
		kv := m.Column("key_value")
		key := kv.Column("key")
		value := kv.Column("value")
		emptyLevel := key.MaxDefinitionLevel() - 1

		got, err := reconstructNestedValue(context.Background(), m, []parquet.Value{
			parquet.NullValue().Level(0, emptyLevel, key.Index()),
			parquet.NullValue().Level(0, emptyLevel, value.Index()),
		})
		require.NoError(t, err)
		require.Equal(t, map[string]any{}, got)
	})

	t.Run("nested list value", func(t *testing.T) {
		schema := parquet.NewSchema("x", parquet.Group{
			"m": parquet.Map(parquet.String(), parquet.List(parquet.Leaf(parquet.Int32Type))),
		})
		var buf bytes.Buffer
		w := parquet.NewWriter(&buf, schema)
		require.NoError(t, w.Close())
		f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		require.NoError(t, err)
		m := f.Root().Column("m")
		kv := m.Column("key_value")
		key := kv.Column("key")
		value := kv.Column("value").Column("list").Column("element")
		emptyLevel := key.MaxDefinitionLevel() - 1

		got, err := reconstructNestedValue(context.Background(), m, []parquet.Value{
			parquet.NullValue().Level(0, emptyLevel, key.Index()),
			parquet.NullValue().Level(0, emptyLevel, value.Index()),
		})
		require.NoError(t, err)
		require.Equal(t, map[string]any{}, got)
	})
}

func TestParquetMapNestedEmptyListByEntry(t *testing.T) {
	schema := parquet.NewSchema("x", parquet.Group{
		"m": parquet.Map(parquet.String(), parquet.List(parquet.Leaf(parquet.Int32Type))),
	})
	var buf bytes.Buffer
	w := parquet.NewWriter(&buf, schema)
	require.NoError(t, w.Close())
	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	m := f.Root().Column("m")
	kv := m.Column("key_value")
	key := kv.Column("key")
	element := kv.Column("value").Column("list").Column("element")

	got, err := reconstructNestedValue(context.Background(), m, []parquet.Value{
		parquet.ByteArrayValue([]byte("first")).Level(0, key.MaxDefinitionLevel(), key.Index()),
		parquet.ByteArrayValue([]byte("second")).Level(1, key.MaxDefinitionLevel(), key.Index()),
		parquet.Int32Value(1).Level(0, element.MaxDefinitionLevel(), element.Index()),
		parquet.NullValue().Level(1, listEmptyDefinitionLevel(element), element.Index()),
	})
	require.NoError(t, err)
	require.Equal(t, map[string]any{
		"first":  []any{int64(1)},
		"second": []any{},
	}, got)
}

func TestParquetLogicalListOfMapsReconstructsOuterElements(t *testing.T) {
	schema := parquet.NewSchema("x", parquet.Group{
		"items": parquet.List(parquet.Map(parquet.String(), parquet.String())),
	})
	var buf bytes.Buffer
	w := parquet.NewWriter(&buf, schema)
	require.NoError(t, w.Close())
	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	items := f.Root().Column("items")
	element := items.Column("list").Column("element")
	key := element.Column("key_value").Column("key")
	value := element.Column("key_value").Column("value")

	got, err := reconstructNestedValue(context.Background(), items, []parquet.Value{
		parquet.ByteArrayValue([]byte("first")).Level(0, key.MaxDefinitionLevel(), key.Index()),
		parquet.ByteArrayValue([]byte("second")).Level(1, key.MaxDefinitionLevel(), key.Index()),
		parquet.ByteArrayValue([]byte("one")).Level(0, value.MaxDefinitionLevel(), value.Index()),
		parquet.ByteArrayValue([]byte("two")).Level(1, value.MaxDefinitionLevel(), value.Index()),
	})
	require.NoError(t, err)
	require.Equal(t, []any{
		map[string]any{"first": "one"},
		map[string]any{"second": "two"},
	}, got)
}

func TestParquetMapRejectsNullKey(t *testing.T) {
	schema := parquet.NewSchema("x", parquet.Group{
		"m": parquet.Map(parquet.String(), parquet.Optional(parquet.String())),
	})
	var buf bytes.Buffer
	w := parquet.NewWriter(&buf, schema)
	require.NoError(t, w.Close())
	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	m := f.Root().Column("m")
	kv := m.Column("key_value")
	key := kv.Column("key")
	value := kv.Column("value")

	_, err = reconstructMap(context.Background(), kv, []parquet.Value{
		parquet.NullValue().Level(0, key.MaxDefinitionLevel()-1, key.Index()),
		parquet.ByteArrayValue([]byte("v")).Level(0, value.MaxDefinitionLevel(), value.Index()),
	})
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
	require.Contains(t, err.Error(), "map key cannot be NULL")
}

func TestParquet_Plain_Bool_ReadErrorRollsBack(t *testing.T) {
	proc := testutil.NewProc(t)
	node := parquet.Leaf(parquet.BooleanType)
	rows := []parquet.Row{
		{parquet.BooleanValue(true).Level(0, 0, 0)},
		{parquet.BooleanValue(false).Level(0, 0, 0)},
		{parquet.BooleanValue(true).Level(0, 0, 0)},
	}
	f, page := writeColumnAndGetPage(t, node, rows)

	vec := vector.NewVec(types.New(types.T_bool, 0, 0))
	var h ParquetHandler
	mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_bool), NotNullable: true})
	require.NotNil(t, mp)
	require.NoError(t, vector.AppendFixed(vec, true, false, proc.Mp()))

	badPage := &parquetPageWithValues{
		Page: page,
		values: parquetBooleanReaderFunc(func(values []bool) (int, error) {
			values[0] = false
			return 1, io.EOF
		}),
	}
	err := mp.mapping(badPage, proc, vec)
	require.Error(t, err)
	require.Contains(t, err.Error(), "short read bool")
	require.Equal(t, []bool{true}, vector.MustFixedColWithTypeCheck[bool](vec))
}

func TestParquet_Plain_Bool_UnexpectedNullRollsBack(t *testing.T) {
	proc := testutil.NewProc(t)
	node := parquet.Leaf(parquet.BooleanType)
	rows := []parquet.Row{
		{parquet.BooleanValue(true).Level(0, 0, 0)},
	}
	f, page := writeColumnAndGetPage(t, node, rows)

	var h ParquetHandler
	mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_bool), NotNullable: true})
	require.NotNil(t, mp)
	badPage := &parquetPageWithValues{
		Page: page,
		values: parquet.ValueReaderFunc(func(values []parquet.Value) (int, error) {
			values[0] = parquet.NullValue()
			return 1, io.EOF
		}),
	}
	vec := vector.NewVec(types.New(types.T_bool, 0, 0))
	err := mp.mapping(badPage, proc, vec)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
	require.Contains(t, err.Error(), "NULL status disagrees")
	require.Zero(t, vec.Length())
}

func TestParquet_Plain_Bool_UnexpectedValueKindRollsBack(t *testing.T) {
	proc := testutil.NewProc(t)
	node := parquet.Leaf(parquet.BooleanType)
	rows := []parquet.Row{
		{parquet.BooleanValue(true).Level(0, 0, 0)},
	}
	f, page := writeColumnAndGetPage(t, node, rows)

	var h ParquetHandler
	mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_bool), NotNullable: true})
	require.NotNil(t, mp)
	badPage := &parquetPageWithValues{
		Page: page,
		values: parquet.ValueReaderFunc(func(values []parquet.Value) (int, error) {
			values[0] = parquet.Int32Value(1)
			return 1, io.EOF
		}),
	}
	vec := vector.NewVec(types.New(types.T_bool, 0, 0))
	err := mp.mapping(badPage, proc, vec)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
	require.Contains(t, err.Error(), "reader returned INT32 value")
	require.Zero(t, vec.Length())
}

func TestParquet_Plain_Bool_DefinitionLevelValueMismatchRollsBack(t *testing.T) {
	proc := testutil.NewProc(t)
	node := parquet.Optional(parquet.Leaf(parquet.BooleanType))
	rows := []parquet.Row{
		{parquet.BooleanValue(true).Level(0, 1, 0)},
		{parquet.NullValue().Level(0, 0, 0)},
	}
	f, page := writeColumnAndGetPage(t, node, rows)

	var h ParquetHandler
	mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_bool)})
	require.NotNil(t, mp)
	badPage := &parquetPageWithValues{
		Page: page,
		values: parquet.ValueReaderFunc(func(values []parquet.Value) (int, error) {
			values[0] = parquet.BooleanValue(true)
			values[1] = parquet.BooleanValue(false)
			return 2, io.EOF
		}),
	}
	vec := vector.NewVec(types.New(types.T_bool, 0, 0))
	err := mp.mapping(badPage, proc, vec)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
	require.Contains(t, err.Error(), "value definition level")
	require.Zero(t, vec.Length())
}

func TestParquet_Plain_Bool_DefinitionLevelMismatch(t *testing.T) {
	proc := testutil.NewProc(t)
	node := parquet.Optional(parquet.Leaf(parquet.BooleanType))
	rows := []parquet.Row{
		{parquet.BooleanValue(true).Level(0, 1, 0)},
		{parquet.NullValue().Level(0, 0, 0)},
	}
	f, page := writeColumnAndGetPage(t, node, rows)

	var h ParquetHandler
	mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_bool)})
	require.NotNil(t, mp)
	badPage := &parquetPageWithDefinitionLevels{
		Page:     page,
		levels:   []byte{1, 1},
		numNulls: 1,
	}
	vec := vector.NewVec(types.New(types.T_bool, 0, 0))
	err := mp.mapping(badPage, proc, vec)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
	require.Contains(t, err.Error(), "NumNulls() indicates")
	require.Zero(t, vec.Length())
}

func TestParquet_Plain_Bool_ValueCountMismatch(t *testing.T) {
	proc := testutil.NewProc(t)
	node := parquet.Leaf(parquet.BooleanType)
	rows := []parquet.Row{
		{parquet.BooleanValue(true).Level(0, 0, 0)},
		{parquet.BooleanValue(false).Level(0, 0, 0)},
		{parquet.BooleanValue(true).Level(0, 0, 0)},
	}
	f, page := writeColumnAndGetPage(t, node, rows)

	var h ParquetHandler
	mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_bool), NotNullable: true})
	require.NotNil(t, mp)
	badPage := &parquetPageWithNumValues{Page: page, numValues: 2}
	vec := vector.NewVec(types.New(types.T_bool, 0, 0))
	err := mp.mapping(badPage, proc, vec)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
	require.Contains(t, err.Error(), "NumValues() 2 does not match NumRows() 3")
	require.Zero(t, vec.Length())
}

func TestParquet_Dictionary_Bool_IndexError(t *testing.T) {
	proc := testutil.NewProc(t)
	node := parquet.Encoded(parquet.Leaf(parquet.BooleanType), &parquet.RLEDictionary)
	vals := []parquet.Value{
		parquet.BooleanValue(true), parquet.BooleanValue(false), parquet.BooleanValue(true),
	}
	f, page := writeDictAndGetPage(t, node, vals)

	vec := vector.NewVec(types.New(types.T_bool, 0, 0))
	var h ParquetHandler
	mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_bool), NotNullable: true})
	require.NotNil(t, mp)
	badPage := &parquetPageWithData{
		Page: page,
		data: encoding.Int32Values([]int32{0, 2, 1}),
	}
	err := mp.mapping(badPage, proc, vec)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
	require.Contains(t, err.Error(), "out of range")
	require.Zero(t, vec.Length())
}

func TestParquet_Dictionary_Bool_IndexCountError(t *testing.T) {
	proc := testutil.NewProc(t)
	node := parquet.Encoded(parquet.Leaf(parquet.BooleanType), &parquet.RLEDictionary)
	vals := []parquet.Value{
		parquet.BooleanValue(true), parquet.BooleanValue(false), parquet.BooleanValue(true),
	}
	f, page := writeDictAndGetPage(t, node, vals)

	vec := vector.NewVec(types.New(types.T_bool, 0, 0))
	var h ParquetHandler
	mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_bool), NotNullable: true})
	require.NotNil(t, mp)
	badPage := &parquetPageWithData{
		Page: page,
		data: encoding.Int32Values([]int32{0, 1}),
	}
	err := mp.mapping(badPage, proc, vec)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
	require.Contains(t, err.Error(), "dictionary indices")
	require.Zero(t, vec.Length())
}

func TestParquet_Dictionary_Bool_NullToNotNull(t *testing.T) {
	proc := testutil.NewProc(t)
	node := parquet.Optional(parquet.Encoded(parquet.Leaf(parquet.BooleanType), &parquet.RLEDictionary))
	rows := []parquet.Row{
		{parquet.BooleanValue(true).Level(0, 1, 0)},
		{parquet.NullValue().Level(0, 0, 0)},
	}
	f, page := writeColumnAndGetPage(t, node, rows)

	vec := vector.NewVec(types.New(types.T_bool, 0, 0))
	var h ParquetHandler
	mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_bool), NotNullable: true})
	require.NotNil(t, mp)
	err := mp.mapping(page, proc, vec)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrConstraintViolation), "unexpected error: %v", err)
	require.Contains(t, err.Error(), "cannot load NULL value")
	require.Zero(t, vec.Length())
}

func TestParquet_Dictionary_Bool_ValueKindMismatch(t *testing.T) {
	proc := testutil.NewProc(t)
	node := parquet.Encoded(parquet.Leaf(parquet.BooleanType), &parquet.RLEDictionary)
	f, page := writeDictAndGetPage(t, node, []parquet.Value{parquet.BooleanValue(true)})
	badDictionary := parquet.Int32Type.NewDictionary(0, 1, encoding.Int32Values([]int32{1}))
	badPage := &parquetPageWithDictionary{Page: page, dictionary: badDictionary}

	var h ParquetHandler
	mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_bool), NotNullable: true})
	require.NotNil(t, mp)
	vec := vector.NewVec(types.New(types.T_bool, 0, 0))
	err := mp.mapping(badPage, proc, vec)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
	require.Contains(t, err.Error(), "BOOLEAN dictionary with type INT32")
	require.Zero(t, vec.Length())
}

func TestParquet_Dictionary_Bool_IndexKindMismatch(t *testing.T) {
	proc := testutil.NewProc(t)
	node := parquet.Encoded(parquet.Leaf(parquet.BooleanType), &parquet.RLEDictionary)
	f, page := writeDictAndGetPage(t, node, []parquet.Value{parquet.BooleanValue(true)})
	badPage := &parquetPageWithData{
		Page: page,
		data: encoding.BooleanValues([]byte{1}),
	}

	var h ParquetHandler
	mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_bool), NotNullable: true})
	require.NotNil(t, mp)
	vec := vector.NewVec(types.New(types.T_bool, 0, 0))
	err := mp.mapping(badPage, proc, vec)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
	require.Contains(t, err.Error(), "BOOLEAN dictionary indexes with type BOOLEAN")
	require.Zero(t, vec.Length())
}

func TestParquet_Dictionary_Numeric_IndexKindMismatch(t *testing.T) {
	proc := testutil.NewProc(t)
	node := parquet.Encoded(parquet.Leaf(parquet.Int32Type), &parquet.RLEDictionary)
	f, page := writeDictAndGetPage(t, node, []parquet.Value{parquet.Int32Value(1)})
	badPage := &parquetPageWithData{
		Page: page,
		data: encoding.BooleanValues([]byte{1}),
	}

	var h ParquetHandler
	mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_int32), NotNullable: true})
	require.NotNil(t, mp)
	vec := vector.NewVec(types.New(types.T_int32, 0, 0))
	err := mp.mapping(badPage, proc, vec)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
	require.Contains(t, err.Error(), "dictionary indexes with type BOOLEAN")
	require.Zero(t, vec.Length())
}

func TestParquet_Dictionary_Numeric_ValueKindMismatch(t *testing.T) {
	proc := testutil.NewProc(t)
	node := parquet.Encoded(parquet.Leaf(parquet.Int32Type), &parquet.RLEDictionary)
	f, page := writeDictAndGetPage(t, node, []parquet.Value{parquet.Int32Value(1)})
	badDictionary := parquet.BooleanType.NewDictionary(0, 1, encoding.BooleanValues([]byte{1}))
	badPage := &parquetPageWithDictionary{Page: page, dictionary: badDictionary}

	var h ParquetHandler
	mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_int32), NotNullable: true})
	require.NotNil(t, mp)
	vec := vector.NewVec(types.New(types.T_int32, 0, 0))
	err := mp.mapping(badPage, proc, vec)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
	require.Contains(t, err.Error(), "dictionary values with type BOOLEAN")
	require.Zero(t, vec.Length())
}

func TestParquet_Dictionary_String_ValueKindMismatch(t *testing.T) {
	proc := testutil.NewProc(t)
	node := parquet.Encoded(parquet.String(), &parquet.RLEDictionary)
	f, page := writeDictAndGetPage(t, node, []parquet.Value{parquet.ByteArrayValue([]byte("value"))})
	badDictionary := parquet.Int32Type.NewDictionary(0, 1, encoding.Int32Values([]int32{1}))
	badPage := &parquetPageWithDictionary{Page: page, dictionary: badDictionary}

	var h ParquetHandler
	mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_varchar), NotNullable: true})
	require.NotNil(t, mp)
	vec := vector.NewVec(types.T_varchar.ToType())
	err := mp.mapping(badPage, proc, vec)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
	require.Contains(t, err.Error(), "string values with type INT32")
	require.Zero(t, vec.Length())
}

func TestParquet_Dictionary_StringMalformedOffsets(t *testing.T) {
	proc := testutil.NewProc(t)
	f, page := writeDictAndGetPage(t, parquet.Encoded(parquet.String(), &parquet.RLEDictionary), []parquet.Value{
		parquet.ByteArrayValue([]byte("1")),
	})
	badDictionary := parquet.String().Type().NewDictionary(0, 1,
		encoding.ByteArrayValues([]byte("1"), []uint32{0, 2}))
	badPage := &parquetPageWithDictionary{Page: page, dictionary: badDictionary}

	cases := []struct {
		name string
		dt   plan.Type
		vec  *vector.Vector
	}{
		{name: "fixed", dt: plan.Type{Id: int32(types.T_int32), NotNullable: true}, vec: vector.NewVec(types.T_int32.ToType())},
		{name: "json", dt: plan.Type{Id: int32(types.T_json), NotNullable: true}, vec: vector.NewVec(types.T_json.ToType())},
		{name: "array", dt: plan.Type{Id: int32(types.T_array_float32), Width: 1, NotNullable: true}, vec: vector.NewVec(types.New(types.T_array_float32, 1, 0))},
	}
	var h ParquetHandler
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mp := h.getMapper(f.Root().Column("c"), tc.dt)
			require.NotNil(t, mp)
			var err error
			require.NotPanics(t, func() { err = mp.mapping(badPage, proc, tc.vec) })
			require.ErrorContains(t, err, "exceeds buffer length")
			require.Zero(t, tc.vec.Length())
		})
	}
}

func TestParquet_Plain_Numeric_ValueKindMismatch(t *testing.T) {
	proc := testutil.NewProc(t)
	f, page := writeColumnAndGetPage(t, parquet.Leaf(parquet.Int32Type), []parquet.Row{
		{parquet.Int32Value(1).Level(0, 0, 0)},
	})
	badPage := &parquetPageWithData{
		Page: page,
		data: encoding.BooleanValues([]byte{1}),
	}

	var h ParquetHandler
	mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_int32), NotNullable: true})
	require.NotNil(t, mp)
	vec := vector.NewVec(types.New(types.T_int32, 0, 0))
	err := mp.mapping(badPage, proc, vec)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
	require.Contains(t, err.Error(), "page values with type BOOLEAN")
	require.Zero(t, vec.Length())
}

func TestParquetDecodedPageSize_InvalidDictionaryIndexes(t *testing.T) {
	page := parquet.Int32Type.NewPage(0, 1, encoding.Int32Values([]int32{0}))
	dict := parquet.Int32Type.NewDictionary(0, 1, encoding.Int32Values([]int32{1}))
	badData := &parquetPageWithData{
		Page: page,
		data: encoding.BooleanValues([]byte{1}),
	}
	badPage := &parquetPageWithDictionary{Page: badData, dictionary: dict}

	require.NotPanics(t, func() {
		require.Positive(t, parquetDecodedPageSize(badPage))
	})
}

func TestParquetDecodedPageSize_FixedWidthDictionary(t *testing.T) {
	page := parquet.Int32Type.NewPage(0, 3, encoding.Int32Values([]int32{0, 1, 0}))
	dict := parquet.Int32Type.NewDictionary(0, 2, encoding.Int32Values([]int32{10, 20}))
	withDict := &parquetPageWithDictionary{Page: page, dictionary: dict}

	require.Equal(t, uint64(12), parquetDecodedPageSize(withDict))
}

func TestParquetDecodedPageSize_InvalidDictionaryStringOffsets(t *testing.T) {
	_, page := writeDictAndGetPage(t, parquet.Encoded(parquet.String(), &parquet.RLEDictionary), []parquet.Value{
		parquet.ByteArrayValue([]byte("value")),
	})
	badDictionary := parquet.String().Type().NewDictionary(0, 1,
		encoding.ByteArrayValues([]byte("value"), []uint32{0, 6}))
	badPage := &parquetPageWithDictionary{Page: page, dictionary: badDictionary}

	require.NotPanics(t, func() {
		require.Positive(t, parquetDecodedPageSize(badPage))
	})
}

func TestParquet_Dictionary_Bool_NullableSlicedPage(t *testing.T) {
	proc := testutil.NewProc(t)
	node := parquet.Optional(parquet.Encoded(parquet.Leaf(parquet.BooleanType), &parquet.RLEDictionary))
	rows := []parquet.Row{
		{parquet.BooleanValue(true).Level(0, 1, 0)},
		{parquet.NullValue().Level(0, 0, 0)},
		{parquet.BooleanValue(false).Level(0, 1, 0)},
		{parquet.BooleanValue(true).Level(0, 1, 0)},
		{parquet.NullValue().Level(0, 0, 0)},
	}
	f, page := writeColumnAndGetPage(t, node, rows)
	require.NotNil(t, page.Dictionary())
	page = page.Slice(1, 5)
	slicedPage := page
	t.Cleanup(func() { parquet.Release(slicedPage) })

	var h ParquetHandler
	mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_bool)})
	require.NotNil(t, mp)
	vec := vector.NewVec(types.New(types.T_bool, 0, 0))
	require.NoError(t, mp.mapping(page, proc, vec))
	require.Equal(t, []bool{false, false, true, false}, vector.MustFixedColWithTypeCheck[bool](vec))
	require.True(t, vec.GetNulls().Contains(0))
	require.False(t, vec.GetNulls().Contains(1))
	require.False(t, vec.GetNulls().Contains(2))
	require.True(t, vec.GetNulls().Contains(3))
}

func TestParquet_NullableMappingWithAllocationAccount(t *testing.T) {
	proc := testutil.NewProc(t)
	defer proc.Free()

	registry, err := mpool.NewAllocationAccountRegistry(1, 16)
	require.NoError(t, err)
	account, err := registry.Open(1 << 20)
	require.NoError(t, err)
	selection, err := vector.NewAllocationAccountSelection(account, 1, 1, 2, 3, 4)
	require.NoError(t, err)
	vec, err := vector.NewOffHeapVecWithTypeAndAllocation(types.New(types.T_bool, 0, 0), selection)
	require.NoError(t, err)
	defer func() {
		vec.Free(proc.Mp())
		require.Zero(t, account.Snapshot().Used)
		account.Seal()
		_, err := registry.Finalize(account)
		require.NoError(t, err)
	}()

	rows := []parquet.Row{
		{parquet.BooleanValue(true).Level(0, 1, 0)},
		{parquet.NullValue().Level(0, 0, 0)},
		{parquet.BooleanValue(false).Level(0, 1, 0)},
	}
	cases := []struct {
		name string
		node parquet.Node
	}{
		{name: "plain", node: parquet.Optional(parquet.Leaf(parquet.BooleanType))},
		{name: "dictionary", node: parquet.Optional(parquet.Encoded(parquet.Leaf(parquet.BooleanType), &parquet.RLEDictionary))},
	}

	var h ParquetHandler
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f, page := writeColumnAndGetPage(t, tc.node, rows)
			mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_bool)})
			require.NotNil(t, mp)
			vec.ResetWithSameType()
			require.NoError(t, mp.mapping(page, proc, vec))
			require.Equal(t, []bool{true, false, false}, vector.MustFixedColWithTypeCheck[bool](vec))
			require.True(t, vec.GetNulls().Contains(1))
		})
	}
}

func TestParquet_NullableStringFixedMappingWithAllocationAccount(t *testing.T) {
	proc := testutil.NewProc(t)
	defer proc.Free()

	registry, err := mpool.NewAllocationAccountRegistry(1, 16)
	require.NoError(t, err)
	account, err := registry.Open(1 << 20)
	require.NoError(t, err)
	selection, err := vector.NewAllocationAccountSelection(account, 1, 1, 2, 3, 4)
	require.NoError(t, err)
	vec, err := vector.NewOffHeapVecWithTypeAndAllocation(types.New(types.T_int32, 0, 0), selection)
	require.NoError(t, err)
	defer func() {
		vec.Free(proc.Mp())
		require.Zero(t, account.Snapshot().Used)
		account.Seal()
		_, err := registry.Finalize(account)
		require.NoError(t, err)
	}()

	rows := []parquet.Row{
		{parquet.ByteArrayValue([]byte("1")).Level(0, 1, 0)},
		{parquet.NullValue().Level(0, 0, 0)},
		{parquet.ByteArrayValue([]byte("2")).Level(0, 1, 0)},
	}
	f, page := writeColumnAndGetPage(t, parquet.Optional(parquet.String()), rows)
	var h ParquetHandler
	mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_int32)})
	require.NotNil(t, mp)
	require.NoError(t, mp.mapping(page, proc, vec))
	require.Equal(t, []int32{1, 0, 2}, vector.MustFixedColWithTypeCheck[int32](vec))
	require.True(t, vec.GetNulls().Contains(1))
}

func TestParquet_StringFixedMappingRollsBackOnParseError(t *testing.T) {
	proc := testutil.NewProc(t)
	f, page := writeColumnAndGetPage(t, parquet.String(), []parquet.Row{
		{parquet.ByteArrayValue([]byte("1")).Level(0, 0, 0)},
		{parquet.ByteArrayValue([]byte("not-an-int")).Level(0, 0, 0)},
	})

	var h ParquetHandler
	mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_int32), NotNullable: true})
	require.NotNil(t, mp)
	vec := vector.NewVec(types.New(types.T_int32, 0, 0))
	require.NoError(t, vector.AppendFixed(vec, int32(99), false, proc.Mp()))

	err := mp.mapping(page, proc, vec)
	require.Error(t, err)
	require.Equal(t, []int32{99}, vector.MustFixedColWithTypeCheck[int32](vec))
}

func TestParquet_StringJsonMappingRollsBackOnParseError(t *testing.T) {
	proc := testutil.NewProc(t)
	f, page := writeColumnAndGetPage(t, parquet.String(), []parquet.Row{
		{parquet.ByteArrayValue([]byte(`{"seed":1}`)).Level(0, 0, 0)},
		{parquet.ByteArrayValue([]byte(`not-json`)).Level(0, 0, 0)},
	})

	var h ParquetHandler
	mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_json), NotNullable: true})
	require.NotNil(t, mp)
	vec := vector.NewVec(types.T_json.ToType())
	seed, err := types.ParseStringToByteJson(`{"existing":true}`)
	require.NoError(t, err)
	require.NoError(t, vector.AppendByteJson(vec, seed, false, proc.Mp()))

	err = mp.mapping(page, proc, vec)
	require.Error(t, err)
	require.Equal(t, 1, vec.Length())
	require.Equal(t, seed.String(), types.DecodeJson(vec.GetBytesAt(0)).String())
}

func TestParquet_StringArrayMappingRollsBackOnParseError(t *testing.T) {
	proc := testutil.NewProc(t)
	f, page := writeColumnAndGetPage(t, parquet.String(), []parquet.Row{
		{parquet.ByteArrayValue([]byte("[1,2,3]")).Level(0, 0, 0)},
		{parquet.ByteArrayValue([]byte("not-a-vector")).Level(0, 0, 0)},
	})

	var h ParquetHandler
	mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_array_float32), Width: 3, NotNullable: true})
	require.NotNil(t, mp)
	vec := vector.NewVec(types.New(types.T_array_float32, 3, 0))
	seed := []float32{9, 8, 7}
	require.NoError(t, vector.AppendArray(vec, seed, false, proc.Mp()))

	err := mp.mapping(page, proc, vec)
	require.Error(t, err)
	require.Equal(t, 1, vec.Length())
	require.Equal(t, seed, vector.GetArrayAt[float32](vec, 0))
}

func TestParquetStringMappingValidatesBeforeAllocating(t *testing.T) {
	proc := testutil.NewProc(t)
	registry, err := mpool.NewAllocationAccountRegistry(1, 16)
	require.NoError(t, err)
	account, err := registry.Open(1 << 20)
	require.NoError(t, err)
	selection, err := vector.NewAllocationAccountSelection(account, 1, 1, 2, 3, 4)
	require.NoError(t, err)
	vec, err := vector.NewOffHeapVecWithTypeAndAllocation(types.T_varchar.ToType(), selection)
	require.NoError(t, err)
	defer func() {
		vec.Free(proc.Mp())
		require.Zero(t, account.Snapshot().Used)
		account.Seal()
		_, err := registry.Finalize(account)
		require.NoError(t, err)
	}()

	f, page := writeColumnAndGetPage(t, parquet.String(), []parquet.Row{
		{parquet.ByteArrayValue([]byte("value")).Level(0, 0, 0)},
	})
	badPage := &parquetPageWithData{
		Page: page,
		data: encoding.ByteArrayValues([]byte("value"), []uint32{0, 6}),
	}
	var h ParquetHandler
	mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_varchar), NotNullable: true})
	require.NotNil(t, mp)
	usedBefore := account.Snapshot().Used
	err = mp.mapping(badPage, proc, vec)
	require.ErrorContains(t, err, "exceeds buffer length")
	require.Zero(t, vec.Length())
	require.Equal(t, usedBefore, account.Snapshot().Used)
}

func TestParquetValuesToFixedRollsBackOnConversionError(t *testing.T) {
	proc := testutil.NewProc(t, testutil.WithFileService(nil))
	t.Cleanup(func() {
		bytes, objects := proc.Mp().OnHeapOutstanding()
		require.Equal(t, [3]int64{}, [3]int64{proc.Mp().CurrNB(), bytes, objects})
	})
	page := parquet.Int32Type.NewPage(0, 2, encoding.Int32Values([]int32{1, 2}))
	vec := vector.NewVec(types.T_int32.ToType())
	t.Cleanup(func() { vec.Free(proc.Mp()) })
	require.NoError(t, vector.AppendFixed(vec, int32(99), false, proc.Mp()))

	err := processParquetValuesToFixed[int32](context.Background(), &columnMapper{}, page, proc, vec, 0,
		func(v parquet.Value) (int32, error) {
			if v.Int32() == 2 {
				return 0, errors.New("conversion failed")
			}
			return v.Int32(), nil
		})
	require.ErrorContains(t, err, "row 1: conversion failed")
	require.Equal(t, []int32{99}, vector.MustFixedColWithTypeCheck[int32](vec))
	require.True(t, vec.GetNulls().IsEmpty())
	t.Run("public partial overflow preserves prefix and NULL", func(t *testing.T) {
		f, page := writeDictAndGetPage(t, parquet.Leaf(parquet.FloatType), []parquet.Value{parquet.FloatValue(1.5), parquet.FloatValue(128)})
		output := vector.NewVec(types.T_int8.ToType())
		t.Cleanup(func() { output.Free(proc.Mp()) })
		require.NoError(t, vector.AppendFixed(output, int8(99), false, proc.Mp()))
		require.NoError(t, vector.AppendFixed(output, int8(0), true, proc.Mp()))
		var h ParquetHandler
		mapper := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_int8), NotNullable: true})
		require.NotNil(t, mapper)
		require.ErrorContains(t, mapper.mapping(page, proc, output), "overflows TINYINT")
		require.Equal(t, []int8{99, 0}, vector.MustFixedColWithTypeCheck[int8](output))
		require.Equal(t, 2, output.Length())
		require.Equal(t, []uint64{1}, output.GetNulls().ToArray())
	})
}

func TestParquetValuesToBytesAndJsonRollBackOnConversionError(t *testing.T) {
	proc := testutil.NewProc(t)
	bytesPage := parquet.ByteArrayType.NewPage(0, 2,
		encoding.ByteArrayValues([]byte("onetwo"), []uint32{0, 3, 6}))
	bytesVec := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(bytesVec, []byte("seed"), false, proc.Mp()))

	err := processParquetValuesToBytes(context.Background(), &columnMapper{}, bytesPage, proc, bytesVec,
		func(v parquet.Value) ([]byte, error) {
			if string(v.ByteArray()) == "two" {
				return nil, errors.New("conversion failed")
			}
			return v.ByteArray(), nil
		})
	require.ErrorContains(t, err, "row 1: conversion failed")
	require.Equal(t, 1, bytesVec.Length())
	require.Equal(t, "seed", string(bytesVec.GetBytesAt(0)))

	jsonFirst := []byte(`{"a":1}`)
	jsonSecond := []byte(`not-json`)
	jsonData := append(append([]byte{}, jsonFirst...), jsonSecond...)
	jsonPage := parquet.ByteArrayType.NewPage(0, 2,
		encoding.ByteArrayValues(jsonData, []uint32{0, uint32(len(jsonFirst)), uint32(len(jsonData))}))
	jsonVec := vector.NewVec(types.T_json.ToType())
	seed, err := types.ParseStringToByteJson(`{"existing":true}`)
	require.NoError(t, err)
	require.NoError(t, vector.AppendByteJson(jsonVec, seed, false, proc.Mp()))

	err = processParquetValuesToJson(context.Background(), &columnMapper{}, jsonPage, proc, jsonVec,
		func(v parquet.Value) (bytejson.ByteJson, error) {
			return types.ParseSliceToByteJson(v.ByteArray())
		})
	require.Error(t, err)
	require.Equal(t, 1, jsonVec.Length())
	require.Equal(t, seed.String(), types.DecodeJson(jsonVec.GetBytesAt(0)).String())
}

func TestParquetListToArrayRollsBackOnConversionError(t *testing.T) {
	proc := testutil.NewProc(t)
	_, page := writeListAndGetPage(t, parquet.Leaf(parquet.FloatType), []parquet.Row{
		{parquet.FloatValue(1).Level(0, 1, 0)},
		{parquet.FloatValue(2).Level(0, 1, 0)},
	})

	vec := vector.NewVec(types.New(types.T_array_float32, 1, 0))
	seed := []float32{9}
	require.NoError(t, vector.AppendArray(vec, seed, false, proc.Mp()))

	err := processParquetListToArray[float32](context.Background(), &columnMapper{maxDefinitionLevel: 1}, page, proc, vec, 1,
		func(v parquet.Value) (float32, error) {
			if v.Float() == 2 {
				return 0, errors.New("conversion failed")
			}
			return v.Float(), nil
		})
	require.ErrorContains(t, err, "row 1")
	require.Equal(t, 1, vec.Length())
	require.Equal(t, seed, vector.GetArrayAt[float32](vec, 0))
}

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

	r := NewParquetReader(param, proc)
	_, err = r.Open(param, proc)
	require.NoError(t, err)
	defer r.Close()

	got := make([]int32, 0, 3)
	for attempts := 0; attempts < 5; attempts++ {
		bat := vectorBatch([]types.Type{types.New(types.T_int32, 0, 0)})
		finished, rerr := r.ReadBatch(context.Background(), bat, proc, nil)
		require.NoError(t, rerr)
		vals := vector.MustFixedColWithTypeCheck[int32](bat.Vecs[0])
		got = append(got, vals[:bat.RowCount()]...)
		if finished {
			break
		}
	}
	require.Equal(t, []int32{10, 20, 30}, got)
}

func TestParquet_ScanParquetFile_SteppedDictionaryBoolBatches(t *testing.T) {
	save := maxParquetBatchCnt
	maxParquetBatchCnt = 1
	defer func() { maxParquetBatchCnt = save }()

	var buf bytes.Buffer
	schema := parquet.NewSchema("x", parquet.Group{
		"c": parquet.Encoded(parquet.Leaf(parquet.BooleanType), &parquet.RLEDictionary),
	})
	w := parquet.NewWriter(&buf, schema)
	rows := []parquet.Row{
		{parquet.BooleanValue(true).Level(0, 0, 0)},
		{parquet.BooleanValue(false).Level(0, 0, 0)},
		{parquet.BooleanValue(true).Level(0, 0, 0)},
	}
	_, err := w.WriteRows(rows)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	param := &ExternalParam{
		ExParamConst: ExParamConst{
			Ctx:      context.Background(),
			Attrs:    []plan.ExternAttr{{ColName: "c", ColIndex: 0}},
			Cols:     []*plan.ColDef{{Typ: plan.Type{Id: int32(types.T_bool), NotNullable: true}}},
			Extern:   &tree.ExternParam{ExParamConst: tree.ExParamConst{ScanType: tree.INLINE}},
			FileSize: []int64{int64(buf.Len())},
		},
		ExParam: ExParam{Fileparam: &ExFileparam{FileIndex: 1, FileCnt: 1}},
	}
	param.Extern.Data = string(buf.Bytes())

	proc := testutil.NewProc(t)
	r := NewParquetReader(param, proc)
	_, err = r.Open(param, proc)
	require.NoError(t, err)
	defer r.Close()

	got := make([]bool, 0, len(rows))
	for attempts := 0; attempts < 5; attempts++ {
		bat := vectorBatch([]types.Type{types.New(types.T_bool, 0, 0)})
		finished, rerr := r.ReadBatch(context.Background(), bat, proc, nil)
		require.NoError(t, rerr)
		vals := vector.MustFixedColWithTypeCheck[bool](bat.Vecs[0])
		got = append(got, vals[:bat.RowCount()]...)
		if finished {
			break
		}
	}
	require.Equal(t, []bool{true, false, true}, got)
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

	r := NewParquetReader(param, proc)
	fileEmpty, err := r.Open(param, proc)
	require.NoError(t, err)
	require.False(t, fileEmpty)
	_, err = r.ReadBatch(context.Background(), bat, proc, nil)
	require.ErrorContains(t, err, "overflows BIGINT")
	require.NotNil(t, r.h)
	require.Len(t, r.h.pages, 1)
	require.Nil(t, r.h.pages[0])
	require.Nil(t, r.h.currentPage[0])
	require.Zero(t, r.h.pageOffset[0])
	require.NoError(t, r.Close())
}

func TestParquet_RowGroupSelection_MappingErrorClosesPages(t *testing.T) {
	var buf bytes.Buffer
	schema := parquet.NewSchema("x", parquet.Group{"c": parquet.Uint(64)})
	w := parquet.NewWriter(&buf, schema, parquet.MaxRowsPerRowGroup(1))
	_, err := w.WriteRows([]parquet.Row{
		{parquet.ValueOf(uint64(1)).Level(0, 0, 0)},
		{parquet.ValueOf(uint64(1<<63)).Level(0, 0, 0)},
	})
	require.NoError(t, err)
	require.NoError(t, w.Close())

	param := &ExternalParam{
		ExParamConst: ExParamConst{
			Ctx:                   context.Background(),
			Attrs:                 []plan.ExternAttr{{ColName: "c", ColIndex: 0}},
			Cols:                  []*plan.ColDef{{Typ: plan.Type{Id: int32(types.T_int64), NotNullable: true}}},
			Extern:                &tree.ExternParam{ExParamConst: tree.ExParamConst{ScanType: tree.INLINE, Format: tree.PARQUET}},
			FileSize:              []int64{int64(buf.Len())},
			ParquetRowGroupShards: []*pipeline.ParquetRowGroupShard{{FileIndex: 0, RowGroupStart: 1, RowGroupEnd: 2}},
		},
		ExParam: ExParam{Fileparam: &ExFileparam{FileIndex: 1, FileCnt: 1}},
	}
	param.Extern.Data = string(buf.Bytes())

	proc := testutil.NewProc(t)
	bat := vectorBatch([]types.Type{types.T_int64.ToType()})

	r := NewParquetReader(param, proc)
	fileEmpty, err := r.Open(param, proc)
	require.NoError(t, err)
	require.False(t, fileEmpty)
	_, err = r.ReadBatch(context.Background(), bat, proc, nil)
	require.ErrorContains(t, err, "overflows BIGINT")
	require.NotNil(t, r.h)
	require.Len(t, r.h.pages, 1)
	require.Nil(t, r.h.pages[0])
	require.Nil(t, r.h.currentPage[0])
	require.Zero(t, r.h.pageOffset[0])
	require.NoError(t, r.Close())
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

	r := NewParquetReader(param, proc)
	fileEmpty, err := r.Open(param, proc)
	require.NoError(t, err)
	require.False(t, fileEmpty)
	defer r.Close()

	var total int
	for attempts := 0; attempts < 5; attempts++ {
		bat := batch.NewWithSize(0)
		finished, rerr := r.ReadBatch(context.Background(), bat, proc, nil)
		require.NoError(t, rerr)
		require.LessOrEqual(t, bat.RowCount(), int(maxParquetBatchCnt))
		total += bat.RowCount()
		if finished {
			break
		}
	}
	require.Equal(t, 3, total)
}

func TestParquet_RowGroupSelection_PagePath(t *testing.T) {
	save := maxParquetBatchCnt
	maxParquetBatchCnt = 3
	defer func() { maxParquetBatchCnt = save }()

	data := writeInt32ParquetWithRowGroups(t, []int32{0, 1, 2, 3, 4, 5}, 2)
	param := &ExternalParam{
		ExParamConst: ExParamConst{
			Ctx:      context.Background(),
			Attrs:    []plan.ExternAttr{{ColName: "c", ColIndex: 0}},
			Cols:     []*plan.ColDef{{Typ: plan.Type{Id: int32(types.T_int32), NotNullable: true}}},
			Extern:   &tree.ExternParam{ExParamConst: tree.ExParamConst{ScanType: tree.INLINE}},
			FileSize: []int64{int64(len(data))},
			ParquetRowGroupShards: []*pipeline.ParquetRowGroupShard{
				{FileIndex: 0, RowGroupStart: 1, RowGroupEnd: 3},
			},
		},
		ExParam: ExParam{Fileparam: &ExFileparam{FileIndex: 1, FileCnt: 1}},
	}
	param.Extern.Data = string(data)

	proc := testutil.NewProc(t)
	r := NewParquetReader(param, proc)
	fileEmpty, err := r.Open(param, proc)
	require.NoError(t, err)
	require.False(t, fileEmpty)
	defer r.Close()

	got := make([]int32, 0, 4)
	for attempts := 0; attempts < 4; attempts++ {
		bat := vectorBatch([]types.Type{types.New(types.T_int32, 0, 0)})
		finished, rerr := r.ReadBatch(context.Background(), bat, proc, nil)
		require.NoError(t, rerr)
		values := vector.MustFixedColWithTypeCheck[int32](bat.Vecs[0])
		got = append(got, values[:bat.RowCount()]...)
		if finished {
			break
		}
	}
	require.Equal(t, []int32{2, 3, 4, 5}, got)
}

func TestParquet_RowGroupSelection_RowCountOnly(t *testing.T) {
	save := maxParquetBatchCnt
	maxParquetBatchCnt = 3
	defer func() { maxParquetBatchCnt = save }()

	data := writeInt32ParquetWithRowGroups(t, []int32{0, 1, 2, 3, 4, 5}, 2)
	param := &ExternalParam{
		ExParamConst: ExParamConst{
			Ctx:      context.Background(),
			Cols:     []*plan.ColDef{{Typ: plan.Type{Id: int32(types.T_int32), NotNullable: true}}},
			Extern:   &tree.ExternParam{ExParamConst: tree.ExParamConst{ScanType: tree.INLINE}},
			FileSize: []int64{int64(len(data))},
			ParquetRowGroupShards: []*pipeline.ParquetRowGroupShard{
				{FileIndex: 0, RowGroupStart: 1, RowGroupEnd: 3},
			},
		},
		ExParam: ExParam{Fileparam: &ExFileparam{FileIndex: 1, FileCnt: 1}},
	}
	param.Extern.Data = string(data)

	proc := testutil.NewProc(t)
	r := NewParquetReader(param, proc)
	fileEmpty, err := r.Open(param, proc)
	require.NoError(t, err)
	require.False(t, fileEmpty)
	defer r.Close()

	var total int
	for attempts := 0; attempts < 4; attempts++ {
		bat := batch.NewWithSize(0)
		finished, rerr := r.ReadBatch(context.Background(), bat, proc, nil)
		require.NoError(t, rerr)
		total += bat.RowCount()
		if finished {
			break
		}
	}
	require.Equal(t, 4, total)
}

func TestParquet_RowGroupSelection_InvalidShard(t *testing.T) {
	data := writeInt32ParquetWithRowGroups(t, []int32{0, 1, 2, 3}, 2)
	for _, shard := range []*pipeline.ParquetRowGroupShard{
		{FileIndex: 0, RowGroupStart: 1, RowGroupEnd: 4},
		{FileIndex: 0, RowGroupStart: 1, RowGroupEnd: 1},
	} {
		param := &ExternalParam{
			ExParamConst: ExParamConst{
				Ctx:      context.Background(),
				Attrs:    []plan.ExternAttr{{ColName: "c", ColIndex: 0}},
				Cols:     []*plan.ColDef{{Typ: plan.Type{Id: int32(types.T_int32), NotNullable: true}}},
				Extern:   &tree.ExternParam{ExParamConst: tree.ExParamConst{ScanType: tree.INLINE}},
				FileSize: []int64{int64(len(data))},
				ParquetRowGroupShards: []*pipeline.ParquetRowGroupShard{
					shard,
				},
			},
			ExParam: ExParam{Fileparam: &ExFileparam{FileIndex: 1, FileCnt: 1}},
		}
		param.Extern.Data = string(data)

		proc := testutil.NewProc(t)
		r := NewParquetReader(param, proc)
		fileEmpty, err := r.Open(param, proc)
		require.Error(t, err)
		require.False(t, fileEmpty)
		require.Contains(t, err.Error(), "invalid parquet row group shard")
	}
}

func TestParquet_RowGroupSelection_NestedRowMode(t *testing.T) {
	save := maxParquetBatchCnt
	maxParquetBatchCnt = 4
	defer func() { maxParquetBatchCnt = save }()

	data := writeNestedParquetWithRowGroups(t, []int32{0, 1, 2, 3}, 2)
	param := &ExternalParam{
		ExParamConst: ExParamConst{
			Ctx:      context.Background(),
			Attrs:    []plan.ExternAttr{{ColName: "c", ColIndex: 0}},
			Cols:     []*plan.ColDef{{Typ: plan.Type{Id: int32(types.T_text)}}},
			Extern:   &tree.ExternParam{ExParamConst: tree.ExParamConst{ScanType: tree.INLINE}},
			FileSize: []int64{int64(len(data))},
			ParquetRowGroupShards: []*pipeline.ParquetRowGroupShard{
				{FileIndex: 0, RowGroupStart: 1, RowGroupEnd: 2},
			},
		},
		ExParam: ExParam{Fileparam: &ExFileparam{FileIndex: 1, FileCnt: 1}},
	}
	param.Extern.Data = string(data)

	proc := testutil.NewProc(t)
	r := NewParquetReader(param, proc)
	fileEmpty, err := r.Open(param, proc)
	require.NoError(t, err)
	require.False(t, fileEmpty)
	defer r.Close()

	bat := vectorBatch([]types.Type{types.New(types.T_text, 0, 0)})
	finished, err := r.ReadBatch(context.Background(), bat, proc, nil)
	require.NoError(t, err)
	require.True(t, finished)
	require.Equal(t, 2, bat.RowCount())
	require.Equal(t, 2, bat.Vecs[0].Length())
}

func TestParquet_RowGroupSelection_SerialVsShards_Nulls(t *testing.T) {
	save := maxParquetBatchCnt
	maxParquetBatchCnt = 2
	defer func() { maxParquetBatchCnt = save }()

	data := writeNullableInt32ParquetWithRowGroups(t, []nullableInt32{
		{value: 1, valid: true},
		{valid: false},
		{value: 3, valid: true},
		{valid: false},
		{value: 5, valid: true},
		{value: 6, valid: true},
	}, 2)

	serial := scanNullableInt32Parquet(t, data, nil, false)
	var sharded []string
	for i := int32(0); i < 3; i++ {
		sharded = append(sharded, scanNullableInt32Parquet(t, data, []*pipeline.ParquetRowGroupShard{
			{FileIndex: 0, RowGroupStart: i, RowGroupEnd: i + 1},
		}, false)...)
	}
	require.Equal(t, []string{"1", "NULL", "3", "NULL", "5", "6"}, serial)
	require.Equal(t, serial, sharded)
}

func TestParquet_RowGroupSelection_NotNullViolation(t *testing.T) {
	data := writeNullableInt32ParquetWithRowGroups(t, []nullableInt32{
		{value: 1, valid: true},
		{value: 2, valid: true},
		{valid: false},
		{value: 4, valid: true},
	}, 2)

	param := nullableInt32ParquetParam(data, []*pipeline.ParquetRowGroupShard{
		{FileIndex: 0, RowGroupStart: 1, RowGroupEnd: 2},
	}, true)
	proc := testutil.NewProc(t)
	r := NewParquetReader(param, proc)
	fileEmpty, err := r.Open(param, proc)
	require.NoError(t, err)
	require.False(t, fileEmpty)
	defer r.Close()

	bat := vectorBatch([]types.Type{types.New(types.T_int32, 0, 0)})
	_, err = r.ReadBatch(context.Background(), bat, proc, nil)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrConstraintViolation), "unexpected error: %v", err)
	require.Contains(t, err.Error(), "cannot load NULL value into NOT NULL column")
}

func TestParquet_RowGroupSelection_SerialVsShards_NestedRowMode(t *testing.T) {
	save := maxParquetBatchCnt
	maxParquetBatchCnt = 2
	defer func() { maxParquetBatchCnt = save }()

	data := writeNestedParquetWithRowGroups(t, []int32{0, 1, 2, 3}, 2)
	serial := scanNestedTextParquet(t, data, nil)
	var sharded []string
	for i := int32(0); i < 2; i++ {
		sharded = append(sharded, scanNestedTextParquet(t, data, []*pipeline.ParquetRowGroupShard{
			{FileIndex: 0, RowGroupStart: i, RowGroupEnd: i + 1},
		})...)
	}
	require.Equal(t, serial, sharded)
	require.Len(t, serial, 4)
}

func TestParquet_ProfileStats_PagePath(t *testing.T) {
	save := maxParquetBatchCnt
	maxParquetBatchCnt = 2
	defer func() { maxParquetBatchCnt = save }()

	data := writeInt32ParquetWithRowGroups(t, []int32{0, 1, 2, 3, 4, 5}, 2)
	param := &ExternalParam{
		ExParamConst: ExParamConst{
			Ctx:      context.Background(),
			Attrs:    []plan.ExternAttr{{ColName: "c", ColIndex: 0}},
			Cols:     []*plan.ColDef{{Typ: plan.Type{Id: int32(types.T_int32), NotNullable: true}}},
			Extern:   &tree.ExternParam{ExParamConst: tree.ExParamConst{ScanType: tree.INLINE, Format: tree.PARQUET, Data: string(data)}},
			FileSize: []int64{int64(len(data))},
		},
		ExParam: ExParam{Fileparam: &ExFileparam{FileIndex: 1, FileCnt: 1}},
	}
	param.Extern.Data = string(data)

	proc := testutil.NewProc(t)
	r := NewParquetReader(param, proc)
	fileEmpty, err := r.Open(param, proc)
	require.NoError(t, err)
	require.False(t, fileEmpty)
	defer r.Close()

	var totalRows int
	for attempts := 0; attempts < 5; attempts++ {
		bat := vectorBatch([]types.Type{types.New(types.T_int32, 0, 0)})
		finished, rerr := r.ReadBatch(context.Background(), bat, proc, nil)
		require.NoError(t, rerr)
		totalRows += bat.RowCount()
		if finished {
			break
		}
	}
	require.Equal(t, 6, totalRows)

	stats := param.takeParquetProfile()
	require.Equal(t, int64(1), stats.Files)
	require.Equal(t, int64(3), stats.RowGroups)
	require.Equal(t, int64(6), stats.RowsRead)
	require.Equal(t, int64(len(data)), stats.BytesRead)
	require.Positive(t, stats.OpenTime)
	require.Positive(t, stats.ReadPageTime)
	require.Positive(t, stats.MapTime)
	require.Zero(t, stats.RowModeTime)
	require.True(t, param.takeParquetProfile().Empty())
}

func TestParquet_ProfileStats_RowMode(t *testing.T) {
	data := writeNestedParquetWithRowGroups(t, []int32{0, 1, 2, 3}, 2)
	param := &ExternalParam{
		ExParamConst: ExParamConst{
			Ctx:      context.Background(),
			Attrs:    []plan.ExternAttr{{ColName: "c", ColIndex: 0}},
			Cols:     []*plan.ColDef{{Typ: plan.Type{Id: int32(types.T_text)}}},
			Extern:   &tree.ExternParam{ExParamConst: tree.ExParamConst{ScanType: tree.INLINE, Format: tree.PARQUET}},
			FileSize: []int64{int64(len(data))},
			ParquetRowGroupShards: []*pipeline.ParquetRowGroupShard{
				{FileIndex: 0, RowGroupStart: 1, RowGroupEnd: 2},
			},
		},
		ExParam: ExParam{Fileparam: &ExFileparam{FileIndex: 1, FileCnt: 1}},
	}
	param.Extern.Data = string(data)

	proc := testutil.NewProc(t)
	r := NewParquetReader(param, proc)
	fileEmpty, err := r.Open(param, proc)
	require.NoError(t, err)
	require.False(t, fileEmpty)
	defer r.Close()

	bat := vectorBatch([]types.Type{types.New(types.T_text, 0, 0)})
	finished, err := r.ReadBatch(context.Background(), bat, proc, nil)
	require.NoError(t, err)
	require.True(t, finished)
	require.Equal(t, 2, bat.RowCount())

	stats := param.takeParquetProfile()
	require.Equal(t, int64(1), stats.Files)
	require.Equal(t, int64(1), stats.RowGroups)
	require.Equal(t, int64(2), stats.RowsRead)
	require.Equal(t, int64(len(data)), stats.BytesRead)
	require.Positive(t, stats.OpenTime)
	require.Positive(t, stats.RowModeTime)
}

// helper to build a batch with provided vector types
func vectorBatch(ts []types.Type) *batch.Batch {
	bat := batch.NewWithSize(len(ts))
	for i, t := range ts {
		bat.Vecs[i] = vector.NewVec(t)
	}
	return bat
}

func writeInt32ParquetWithRowGroups(t *testing.T, values []int32, rowsPerGroup int64) []byte {
	t.Helper()
	var buf bytes.Buffer
	schema := parquet.NewSchema("x", parquet.Group{"c": parquet.Leaf(parquet.Int32Type)})
	w := parquet.NewWriter(&buf, schema, parquet.MaxRowsPerRowGroup(rowsPerGroup))
	rows := make([]parquet.Row, len(values))
	for i, value := range values {
		rows[i] = parquet.Row{parquet.Int32Value(value).Level(0, 0, 0)}
	}
	_, err := w.WriteRows(rows)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	require.Len(t, f.RowGroups(), (len(values)+int(rowsPerGroup)-1)/int(rowsPerGroup))
	return buf.Bytes()
}

func writeNestedParquetWithRowGroups(t *testing.T, values []int32, rowsPerGroup int64) []byte {
	t.Helper()
	var buf bytes.Buffer
	schema := parquet.NewSchema("x", parquet.Group{
		"c": parquet.Group{
			"v": parquet.Leaf(parquet.Int32Type),
		},
	})
	w := parquet.NewWriter(&buf, schema, parquet.MaxRowsPerRowGroup(rowsPerGroup))
	rows := make([]parquet.Row, len(values))
	for i, value := range values {
		rows[i] = parquet.Row{parquet.Int32Value(value).Level(0, 0, 0)}
	}
	_, err := w.WriteRows(rows)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	require.Len(t, f.RowGroups(), (len(values)+int(rowsPerGroup)-1)/int(rowsPerGroup))
	return buf.Bytes()
}

type nullableInt32 struct {
	value int32
	valid bool
}

func writeNullableInt32ParquetWithRowGroups(t *testing.T, values []nullableInt32, rowsPerGroup int64) []byte {
	t.Helper()
	var buf bytes.Buffer
	schema := parquet.NewSchema("x", parquet.Group{"c": parquet.Optional(parquet.Leaf(parquet.Int32Type))})
	w := parquet.NewWriter(&buf, schema, parquet.MaxRowsPerRowGroup(rowsPerGroup))
	rows := make([]parquet.Row, len(values))
	for i, value := range values {
		if value.valid {
			rows[i] = parquet.Row{parquet.Int32Value(value.value).Level(0, 1, 0)}
		} else {
			rows[i] = parquet.Row{parquet.NullValue().Level(0, 0, 0)}
		}
	}
	_, err := w.WriteRows(rows)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	require.Len(t, f.RowGroups(), (len(values)+int(rowsPerGroup)-1)/int(rowsPerGroup))
	return buf.Bytes()
}

func nullableInt32ParquetParam(data []byte, shards []*pipeline.ParquetRowGroupShard, notNull bool) *ExternalParam {
	return &ExternalParam{
		ExParamConst: ExParamConst{
			Ctx:                   context.Background(),
			Attrs:                 []plan.ExternAttr{{ColName: "c", ColIndex: 0}},
			Cols:                  []*plan.ColDef{{Typ: plan.Type{Id: int32(types.T_int32), NotNullable: notNull}}},
			Extern:                &tree.ExternParam{ExParamConst: tree.ExParamConst{ScanType: tree.INLINE, Format: tree.PARQUET, Data: string(data)}},
			FileSize:              []int64{int64(len(data))},
			ParquetRowGroupShards: shards,
		},
		ExParam: ExParam{Fileparam: &ExFileparam{FileIndex: 1, FileCnt: 1}},
	}
}

func scanNullableInt32Parquet(t *testing.T, data []byte, shards []*pipeline.ParquetRowGroupShard, notNull bool) []string {
	t.Helper()
	param := nullableInt32ParquetParam(data, shards, notNull)
	param.Extern.Data = string(data)
	proc := testutil.NewProc(t)
	r := NewParquetReader(param, proc)
	fileEmpty, err := r.Open(param, proc)
	require.NoError(t, err)
	require.False(t, fileEmpty)
	defer r.Close()

	var rows []string
	for attempts := 0; attempts < 8; attempts++ {
		bat := vectorBatch([]types.Type{types.New(types.T_int32, 0, 0)})
		finished, rerr := r.ReadBatch(context.Background(), bat, proc, nil)
		require.NoError(t, rerr)
		values := vector.MustFixedColWithTypeCheck[int32](bat.Vecs[0])
		for i := 0; i < bat.RowCount(); i++ {
			if bat.Vecs[0].IsNull(uint64(i)) {
				rows = append(rows, "NULL")
			} else {
				rows = append(rows, fmt.Sprintf("%d", values[i]))
			}
		}
		if finished {
			return rows
		}
	}
	t.Fatalf("parquet scan did not finish")
	return nil
}

func scanNestedTextParquet(t *testing.T, data []byte, shards []*pipeline.ParquetRowGroupShard) []string {
	t.Helper()
	param := &ExternalParam{
		ExParamConst: ExParamConst{
			Ctx:                   context.Background(),
			Attrs:                 []plan.ExternAttr{{ColName: "c", ColIndex: 0}},
			Cols:                  []*plan.ColDef{{Typ: plan.Type{Id: int32(types.T_text)}}},
			Extern:                &tree.ExternParam{ExParamConst: tree.ExParamConst{ScanType: tree.INLINE, Format: tree.PARQUET}},
			FileSize:              []int64{int64(len(data))},
			ParquetRowGroupShards: shards,
		},
		ExParam: ExParam{Fileparam: &ExFileparam{FileIndex: 1, FileCnt: 1}},
	}
	param.Extern.Data = string(data)
	proc := testutil.NewProc(t)
	r := NewParquetReader(param, proc)
	fileEmpty, err := r.Open(param, proc)
	require.NoError(t, err)
	require.False(t, fileEmpty)
	defer r.Close()

	var rows []string
	for attempts := 0; attempts < 8; attempts++ {
		bat := vectorBatch([]types.Type{types.New(types.T_text, 0, 0)})
		finished, rerr := r.ReadBatch(context.Background(), bat, proc, nil)
		require.NoError(t, rerr)
		for i := 0; i < bat.RowCount(); i++ {
			if bat.Vecs[0].IsNull(uint64(i)) {
				rows = append(rows, "NULL")
			} else {
				rows = append(rows, bat.Vecs[0].GetStringAt(i))
			}
		}
		if finished {
			return rows
		}
	}
	t.Fatalf("parquet scan did not finish")
	return nil
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

	// Reinitializing a loader must clear the previous representation and cursor.
	var reused strLoader
	reused.init(encoding.FixedLenByteArrayValues([]byte("abcd"), 2))
	require.Equal(t, "ab", string(reused.loadNext()))
	reused.init(encoding.ByteArrayValues([]byte("xyz"), []uint32{0, 1, 3}))
	require.Equal(t, "x", string(reused.loadNext()))
	require.Equal(t, "yz", string(reused.loadNext()))

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

func Test_parquet_copyDictPageToVec_indexCountError(t *testing.T) {
	proc := testutil.NewProc(t)
	st := parquet.Int32Type
	page := st.NewPage(0, 3, encoding.Int32Values([]int32{0, 2, 1}))
	vec := vector.NewVec(types.New(types.T_int32, 0, 0))
	mp := &columnMapper{srcNull: false, dstNull: false, maxDefinitionLevel: 0}

	err := copyDictPageToVec[int32](mp, page, proc, vec, 3, []int32{0, 2}, func(idx int32) int32 { return idx })
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
	require.Contains(t, err.Error(), "dictionary indices")
	require.Zero(t, vec.Length())
}

func Test_parquet_copyDictPageToVec_definitionLevelMismatch(t *testing.T) {
	proc := testutil.NewProc(t)
	st := parquet.Int32Type
	page := st.NewPage(0, 3, encoding.Int32Values([]int32{0, 2, 1}))
	pageWithBadLevels := &parquetPageWithDefinitionLevels{
		Page:     page,
		levels:   []byte{0, 0, 0},
		numNulls: 1,
	}
	vec := vector.NewVec(types.New(types.T_int32, 0, 0))
	mp := &columnMapper{srcNull: true, dstNull: true, maxDefinitionLevel: 1}

	err := copyDictPageToVec[int32](mp, pageWithBadLevels, proc, vec, 3, []int32{0, 2}, func(idx int32) int32 { return idx })
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
	require.Contains(t, err.Error(), "NumNulls() indicates")
	require.Zero(t, vec.Length())
}

func Test_parquet_copyPageToVecMap_valueCountError(t *testing.T) {
	proc := testutil.NewProc(t)
	st := parquet.Int32Type
	page := st.NewPage(0, 3, encoding.Int32Values([]int32{1, 2, 3}))
	vec := vector.NewVec(types.New(types.T_int32, 0, 0))
	mp := &columnMapper{srcNull: false, dstNull: false, maxDefinitionLevel: 0}

	err := copyPageToVec(mp, &parquetPageWithData{
		Page: page,
		data: encoding.Int32Values([]int32{1, 2}),
	}, proc, vec, []int32{1, 2})
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
	require.Contains(t, err.Error(), "expected 3 non-null values")
	require.Zero(t, vec.Length())
}

func Test_parquet_copyPageToVecMap_definitionLevelCountError(t *testing.T) {
	proc := testutil.NewProc(t)
	st := parquet.Int32Type
	page := st.NewPage(0, 3, encoding.Int32Values([]int32{1, 2, 3}))
	pageWithBadLevels := &parquetPageWithDefinitionLevels{
		Page:     page,
		levels:   []byte{1},
		numNulls: 1,
	}
	vec := vector.NewVec(types.New(types.T_int32, 0, 0))
	mp := &columnMapper{srcNull: true, dstNull: true, maxDefinitionLevel: 1}

	err := copyPageToVec(mp, pageWithBadLevels, proc, vec, []int32{1, 2})
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
	require.Contains(t, err.Error(), "definition levels length 1 != numRows 3")
	require.Zero(t, vec.Length())
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
	var badErr error
	require.NotPanics(t, func() {
		_, badErr = decodeDecimal64Values(ctx, parquet.ByteArray, encoding.ByteArrayValues([]byte{1}, []uint32{0, 2}))
	})
	require.ErrorContains(t, badErr, "exceeds buffer length")
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
	badErr = nil
	require.NotPanics(t, func() {
		_, badErr = decodeDecimal128Values(ctx, parquet.ByteArray, encoding.ByteArrayValues([]byte{1}, []uint32{0, 2}))
	})
	require.ErrorContains(t, badErr, "exceeds buffer length")

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
	badErr = nil
	require.NotPanics(t, func() {
		_, badErr = decodeDecimal256Values(ctx, parquet.ByteArray, encoding.ByteArrayValues([]byte{1}, []uint32{0, 2}))
	})
	require.ErrorContains(t, badErr, "exceeds buffer length")

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
	optionalPage := &parquetPageWithDefinitionLevels{
		Page:     page,
		levels:   []byte{1, 1},
		numNulls: 0,
	}
	mp = &columnMapper{srcNull: true, dstNull: true, maxDefinitionLevel: 1}
	nc, err = prepareNullCheck(ctx, mp, optionalPage)
	require.NoError(t, err)
	require.True(t, nc.noNulls)
	require.Equal(t, byte(1), nc.maxDefinitionLevel)
	require.False(t, nc.isNull(0))
}

func Test_prepareNullCheck_rejectsInvalidCounts(t *testing.T) {
	ctx := context.Background()
	page := parquet.Int32Type.NewPage(0, 2, encoding.Int32Values([]int32{1, 2}))
	mp := &columnMapper{srcNull: false, dstNull: true, maxDefinitionLevel: 0}

	for _, tc := range []struct {
		name string
		page parquet.Page
	}{
		{
			name: "negative rows",
			page: &parquetPageWithNumRows{Page: page, numRows: -1},
		},
		{
			name: "negative nulls",
			page: &parquetPageWithDefinitionLevels{Page: page, numNulls: -1},
		},
		{
			name: "too many nulls",
			page: &parquetPageWithDefinitionLevels{Page: page, numNulls: 3},
		},
		{
			name: "nulls on required page",
			page: &parquetPageWithDefinitionLevels{Page: page, numNulls: 1},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := prepareNullCheck(ctx, mp, tc.page)
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
		})
	}
}

func Test_prepareNullCheck_rejectsInconsistentNoNullLevels(t *testing.T) {
	ctx := context.Background()
	page := parquet.Int32Type.NewPage(0, 2, encoding.Int32Values([]int32{1, 2}))
	mp := &columnMapper{srcNull: true, dstNull: true, maxDefinitionLevel: 1}

	for _, tc := range []struct {
		name   string
		levels []byte
		want   string
	}{
		{name: "missing levels", levels: nil, want: "definition levels are empty"},
		{name: "short levels", levels: []byte{1}, want: "definition levels length"},
		{name: "null level with zero null count", levels: []byte{1, 0}, want: "not non-null level"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wrapped := &parquetPageWithDefinitionLevels{Page: page, levels: tc.levels, numNulls: 0}
			_, err := prepareNullCheck(ctx, mp, wrapped)
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
			require.Contains(t, err.Error(), tc.want)
		})
	}
}

func Test_prepareNullCheck_rejectsDefinitionLevelAboveMaximum(t *testing.T) {
	ctx := context.Background()
	page := parquet.Int32Type.NewPage(0, 2, encoding.Int32Values([]int32{1, 2}))
	mp := &columnMapper{srcNull: true, dstNull: true, maxDefinitionLevel: 1}

	wrapped := &parquetPageWithDefinitionLevels{
		Page:     page,
		levels:   []byte{2, 0},
		numNulls: 1,
	}
	_, err := prepareNullCheck(ctx, mp, wrapped)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
	require.Contains(t, err.Error(), "exceeds maximum")
}

func Test_readParquetPageValues_rejectsNegativeCounts(t *testing.T) {
	ctx := context.Background()
	page := parquet.Int32Type.NewPage(0, 1, encoding.Int32Values([]int32{1}))

	_, err := readParquetPageValues(ctx, &parquetPageWithNumRows{Page: page, numRows: -1})
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
	require.Contains(t, err.Error(), "NumRows() -1 is negative")

	_, err = readParquetPageAllValues(ctx, &parquetPageWithNumValues{Page: page, numValues: -1})
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
	require.Contains(t, err.Error(), "NumValues() -1 is negative")
}

func TestParquet_EnsureCurrentPageRejectsNegativeRows(t *testing.T) {
	ctx := context.Background()
	page := parquet.Int32Type.NewPage(0, 1, encoding.Int32Values([]int32{1}))
	h := ParquetHandler{
		pages:       make([]parquet.Pages, 1),
		currentPage: []parquet.Page{&parquetPageWithNumRows{Page: page, numRows: -1}},
		pageOffset:  []int64{0},
	}
	param := &ExternalParam{ExParamConst: ExParamConst{Ctx: ctx}}

	more, err := h.ensureCurrentPage(0, param)
	require.False(t, more)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
	require.Contains(t, err.Error(), "NumRows() -1 is negative")
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

	// A zero-width fixed-length value is invalid even when the page has no non-NULL values.
	{
		var loader strLoader
		loader.init(encoding.FixedLenByteArrayValues(nil, 0))
		err := validateStringDataCount(ctx, &loader, 0)
		require.ErrorContains(t, err, "invalid fixed length 0")
	}

	for _, tc := range []struct {
		name    string
		offsets []uint32
		want    string
	}{
		{name: "offset exceeds buffer", offsets: []uint32{0, 7}, want: "exceeds buffer length"},
		{name: "offsets decrease", offsets: []uint32{0, 4, 3}, want: "precedes previous offset"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var loader strLoader
			loader.init(encoding.ByteArrayValues([]byte("abcdef"), tc.offsets))
			err := validateStringDataCount(ctx, &loader, int64(len(tc.offsets)-1))
			require.ErrorContains(t, err, tc.want)
		})
	}

	var sharedBufferLoader strLoader
	sharedBufferLoader.init(encoding.ByteArrayValues([]byte("abcdef"), []uint32{1, 3}))
	require.NoError(t, validateStringDataCount(ctx, &sharedBufferLoader, 1))
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
	fs := &fakeFS{b: data}
	param := &ExternalParam{
		ExParamConst: ExParamConst{
			Extern: &tree.ExternParam{ExParamConst: tree.ExParamConst{Format: tree.PARQUET}},
		},
	}
	r := &fsReaderAt{fs: fs, readPath: "fake:hello", ctx: context.Background(), param: param}
	buf := make([]byte, 5)
	n, err := r.ReadAt(buf, 6)
	require.NoError(t, err)
	require.Equal(t, 5, n)
	require.Equal(t, []byte("world"), buf)
	require.Equal(t, fileservice.Policy(fileservice.SkipFullFilePreloads), fs.lastPolicy)
	require.Equal(t, int64(6), fs.lastOffset)
	require.Equal(t, int64(5), fs.lastSize)
	require.Equal(t, int64(5), param.takeParquetProfile().BytesRead)
}

func Test_fsReaderAt_ReadAtReturnsEOFForShortRead(t *testing.T) {
	fs := &fakeFS{b: []byte("hello"), shortRead: true}
	r := &fsReaderAt{fs: fs, readPath: "fake:short", ctx: context.Background()}
	buf := make([]byte, 5)
	n, err := r.ReadAt(buf, 0)
	require.Equal(t, 4, n)
	require.ErrorIs(t, err, io.EOF)
}

func Test_fsReaderAt_ReadAtRejectsNegativeOffset(t *testing.T) {
	r := &fsReaderAt{fs: &fakeFS{b: []byte("hello")}, ctx: context.Background()}
	n, err := r.ReadAt(make([]byte, 1), -1)
	require.Zero(t, n)
	require.ErrorContains(t, err, "negative offset")
}

func Test_fsReaderAt_ReadAtRejectsOffsetOverflow(t *testing.T) {
	r := &fsReaderAt{fs: &fakeFS{b: []byte("hello")}, ctx: context.Background()}
	n, err := r.ReadAt(make([]byte, 2), math.MaxInt64)
	require.Zero(t, n)
	require.ErrorContains(t, err, "overflows int64")
}

func TestParquetRangeReadAheadCoalescesSequentialReads(t *testing.T) {
	data := make([]byte, 2*parquetRangeReadAheadMaxBytes)
	fs := &fakeFS{b: data}
	reader := &parquetRangeReadAheadReaderAt{
		reader:   &fsReaderAt{fs: fs, readPath: "fake:sequential.parquet", ctx: context.Background()},
		fileSize: int64(len(data)),
	}

	const requestSize = 64 * 1024
	for off := int64(0); off < 8*requestSize; off += requestSize {
		buf := make([]byte, requestSize)
		n, err := reader.ReadAt(buf, off)
		require.NoError(t, err)
		require.Equal(t, len(buf), n)
	}

	require.Equal(t, int64(2), fs.readCount)
	require.Equal(t, int64(8*requestSize), fs.simulatedRemoteRead)
	require.LessOrEqual(t, int64(cap(reader.window)), parquetRangeReadAheadMaxBytes)
}

func TestParquetRangeReadAheadBoundsSparseAmplification(t *testing.T) {
	data := make([]byte, 8*parquetRangeReadAheadMaxBytes)
	fs := &fakeFS{b: data}
	reader := &parquetRangeReadAheadReaderAt{
		reader:   &fsReaderAt{fs: fs, readPath: "fake:sparse.parquet", ctx: context.Background()},
		fileSize: int64(len(data)),
	}

	const requestSize = 64 * 1024
	const requestCount = 4
	for i := int64(0); i < requestCount; i++ {
		buf := make([]byte, requestSize)
		_, err := reader.ReadAt(buf, i*parquetRangeReadAheadMaxBytes)
		require.NoError(t, err)
	}

	requested := int64(requestCount * requestSize)
	require.Equal(t, int64(requestCount), fs.readCount)
	require.LessOrEqual(t, fs.simulatedRemoteRead, requested*parquetRangeReadAheadAmplification)
	require.LessOrEqual(t, int64(cap(reader.window)), parquetRangeReadAheadMaxBytes)
}

func TestParquetRangeReadAheadPropagatesErrors(t *testing.T) {
	data := make([]byte, parquetRangeReadAheadMaxBytes)
	fs := &fakeFS{b: data, readErr: context.Canceled}
	reader := &parquetRangeReadAheadReaderAt{
		reader:   &fsReaderAt{fs: fs, readPath: "fake:canceled.parquet", ctx: context.Background()},
		fileSize: int64(len(data)),
	}

	buf := make([]byte, 64*1024)
	n, err := reader.ReadAt(buf, 0)
	require.Zero(t, n)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, reader.window)
}

type malformedParquetReaderAt struct{}

func (malformedParquetReaderAt) ReadAt(p []byte, _ int64) (int, error) {
	return len(p) + 1, nil
}

func TestParquetRangeReadAheadRejectsInvalidReaderCount(t *testing.T) {
	reader := &parquetRangeReadAheadReaderAt{
		reader:   malformedParquetReaderAt{},
		fileSize: int64(parquetRangeReadAheadMaxBytes),
	}
	n, err := reader.ReadAt(make([]byte, 64*1024), 0)
	require.Zero(t, n)
	require.ErrorContains(t, err, "invalid byte count")
	require.Empty(t, reader.window)
}

type partialParquetReaderAt struct {
	data []byte
	n    int
	err  error
}

func (r partialParquetReaderAt) ReadAt(p []byte, _ int64) (int, error) {
	n := min(r.n, len(p))
	copy(p[:n], r.data[:n])
	return n, r.err
}

func TestParquetRangeReadAheadCopiesPartialBytesOnError(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "eof", err: io.EOF},
		{name: "underlying error", err: context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := &parquetRangeReadAheadReaderAt{
				reader:   partialParquetReaderAt{data: []byte("partial"), n: 2, err: tc.err},
				fileSize: 1024,
			}
			buf := bytes.Repeat([]byte{0xff}, 4)
			n, err := reader.ReadAt(buf, 0)
			require.Equal(t, 2, n)
			require.ErrorIs(t, err, tc.err)
			require.Equal(t, []byte("pa"), buf[:2])
			require.Equal(t, []byte{0xff, 0xff}, buf[2:])
		})
	}
}

func TestParquetRangeReadAheadConcurrentReaderAt(t *testing.T) {
	data := make([]byte, 2*parquetRangeReadAheadMaxBytes)
	for i := range data {
		data[i] = byte(i % 251)
	}
	fs := &fakeFS{b: data}
	reader := &parquetRangeReadAheadReaderAt{
		reader:   &fsReaderAt{fs: fs, readPath: "fake:concurrent.parquet", ctx: context.Background()},
		fileSize: int64(len(data)),
	}

	const readers = 16
	const requestSize = 64 * 1024
	errs := make(chan error, readers)
	var wg sync.WaitGroup
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			off := int64(index * requestSize)
			buf := make([]byte, requestSize)
			n, err := reader.ReadAt(buf, off)
			if err == nil && n != len(buf) {
				err = fmt.Errorf("read %d bytes, expected %d", n, len(buf))
			}
			if err == nil && !bytes.Equal(buf, data[off:off+requestSize]) {
				err = fmt.Errorf("data mismatch at offset %d", off)
			}
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.LessOrEqual(t, int64(cap(reader.window)), parquetRangeReadAheadMaxBytes)
}

func TestShouldReadAheadParquetRangesIsLoadOnly(t *testing.T) {
	newParam := func(scanType int, externType plan.ExternType, withShards, wholeFileFanout bool) *ExternalParam {
		param := &ExternalParam{ExParamConst: ExParamConst{
			ParquetWholeFileFanout: wholeFileFanout,
			Extern: &tree.ExternParam{
				ExParamConst: tree.ExParamConst{ScanType: scanType, Format: tree.PARQUET},
				ExParam:      tree.ExParam{ExternType: int32(externType)},
			},
		}}
		if withShards {
			param.ParquetRowGroupShards = []*pipeline.ParquetRowGroupShard{{RowGroupEnd: 1}}
		}
		return param
	}

	require.True(t, shouldReadAheadParquetRanges(newParam(tree.S3, plan.ExternType_LOAD, true, false)))
	require.True(t, shouldReadAheadParquetRanges(newParam(tree.S3, plan.ExternType_LOAD, false, true)))
	require.False(t, shouldReadAheadParquetRanges(newParam(tree.S3, plan.ExternType_EXTERNAL_TB, true, false)))
	require.False(t, shouldReadAheadParquetRanges(newParam(tree.INFILE, plan.ExternType_LOAD, true, false)))
	require.False(t, shouldReadAheadParquetRanges(newParam(tree.S3, plan.ExternType_LOAD, false, false)))
}

func TestParquetS3ReadAmplificationRepro(t *testing.T) {
	var buf bytes.Buffer
	schema := parquet.NewSchema("x", parquet.Group{
		"id":      parquet.Leaf(parquet.Int64Type),
		"payload": parquet.String(),
	})
	w := parquet.NewWriter(&buf, schema, parquet.MaxRowsPerRowGroup(16))
	rows := make([]parquet.Row, 4096)
	for i := range rows {
		payload := make([]byte, 256)
		state := uint32(i + 1)
		for j := range payload {
			state = state*1664525 + 1013904223
			payload[j] = byte(state >> 24)
		}
		rows[i] = parquet.Row{
			parquet.Int64Value(int64(i)).Level(0, 0, 0),
			parquet.ByteArrayValue(payload).Level(0, 0, 1),
		}
	}
	_, err := w.WriteRows(rows)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	readPayloadPages := func(reader io.ReaderAt) int {
		f, err := parquet.OpenFile(reader, int64(buf.Len()))
		require.NoError(t, err)
		require.Greater(t, len(f.RowGroups()), 100)
		pages := f.Root().Column("payload").Pages()
		defer func() {
			require.NoError(t, pages.Close())
		}()
		pageCount := 0
		for {
			_, err := pages.ReadPage()
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(t, err)
			pageCount++
		}
		return pageCount
	}

	rawFS := &fakeFS{b: buf.Bytes()}
	rawPages := readPayloadPages(&fsReaderAt{
		fs: rawFS, readPath: "fake:payload.parquet", ctx: context.Background(),
	})
	require.Positive(t, rawPages)
	require.GreaterOrEqual(t, rawFS.readCount, int64(3), "expect multiple small ReadAt calls")
	require.Equal(t, rawFS.logicalRead, rawFS.simulatedRemoteRead)

	coalescedFS := &fakeFS{b: buf.Bytes()}
	coalescedReader := &parquetRangeReadAheadReaderAt{
		reader: &fsReaderAt{
			fs: coalescedFS, readPath: "fake:payload.parquet", ctx: context.Background(),
		},
		fileSize: int64(buf.Len()),
	}
	coalescedPages := readPayloadPages(coalescedReader)
	require.Equal(t, rawPages, coalescedPages)
	require.Less(t, coalescedFS.readCount, rawFS.readCount)
	require.LessOrEqual(t, coalescedFS.simulatedRemoteRead,
		rawFS.logicalRead*parquetRangeReadAheadAmplification)
	require.LessOrEqual(t, int64(cap(coalescedReader.window)), parquetRangeReadAheadMaxBytes)
	require.Equal(t, fileservice.Policy(fileservice.SkipFullFilePreloads), coalescedFS.lastPolicy)
}

func BenchmarkParquetRangeReadAheadSequential(b *testing.B) {
	data := make([]byte, 8*parquetRangeReadAheadMaxBytes)
	const requestSize = 64 * 1024
	const simulatedRangeLatency = 100 * time.Microsecond
	for _, test := range []struct {
		name      string
		readAhead bool
	}{
		{name: "direct"},
		{name: "read_ahead", readAhead: true},
	} {
		b.Run(test.name, func(b *testing.B) {
			fs := &fakeFS{b: data, readLatency: simulatedRangeLatency}
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			var peakCacheBytes int
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				base := &fsReaderAt{fs: fs, readPath: "fake:benchmark.parquet", ctx: context.Background()}
				var reader io.ReaderAt = base
				var readAheadReader *parquetRangeReadAheadReaderAt
				if test.readAhead {
					readAheadReader = &parquetRangeReadAheadReaderAt{reader: base, fileSize: int64(len(data))}
					reader = readAheadReader
				}
				for off := int64(0); off < int64(len(data)); off += requestSize {
					buf := make([]byte, requestSize)
					if _, err := reader.ReadAt(buf, off); err != nil {
						b.Fatal(err)
					}
				}
				if readAheadReader != nil {
					peakCacheBytes = max(peakCacheBytes, cap(readAheadReader.window))
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(fs.readCount)/float64(b.N), "range_calls/op")
			b.ReportMetric(float64(fs.simulatedRemoteRead)/float64(b.N), "fetched_bytes/op")
			b.ReportMetric(float64(peakCacheBytes), "peak_cache_bytes")
			b.ReportMetric(float64(simulatedRangeLatency), "range_latency_ns")
		})
	}
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

	// First call -> one row, not finished yet
	bat := vectorBatch([]types.Type{types.New(types.T_int32, 0, 0)})
	require.NoError(t, h.getData(bat, param, proc))
	require.Equal(t, 1, bat.RowCount())
	require.True(t, h.offset < h.file.NumRows(), "should not be finished yet")
	// Second call -> last row and finish
	bat2 := vectorBatch([]types.Type{types.New(types.T_int32, 0, 0)})
	require.NoError(t, h.getData(bat2, param, proc))
	require.Equal(t, 1, bat2.RowCount())
	require.True(t, h.offset >= h.file.NumRows(), "should be finished")
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

func TestParquet_Timestamp_NotAdjustedToUTC_UsesValueTimezoneOffset(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	proc := testutil.NewProc(t)
	proc.Base.SessionInfo.TimeZone = loc

	// The parquet values encode these local wall-clock values as UTC epoch offsets.
	// New York has different offsets for the January and July values.
	values := []int64{
		time.Date(2024, time.January, 15, 10, 30, 0, 0, time.UTC).UnixMicro(),
		time.Date(2024, time.July, 15, 10, 30, 0, 0, time.UTC).UnixMicro(),
	}
	want := []string{"2024-01-15 10:30:00", "2024-07-15 10:30:00"}

	assertMappedValues := func(t *testing.T, f *parquet.File, page parquet.Page, expected []string) {
		t.Helper()
		vec := vector.NewVec(types.T_timestamp.ToType())
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_timestamp)})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))

		got := vector.MustFixedColWithTypeCheck[types.Timestamp](vec)
		require.Len(t, got, len(expected))
		for i := range expected {
			require.Equal(t, expected[i], got[i].String2(loc, 0))
		}
	}

	t.Run("plain page", func(t *testing.T) {
		rows := make([]parquet.Row, len(values))
		for i, value := range values {
			rows[i] = parquet.Row{parquet.Int64Value(value).Level(0, 0, 0)}
		}
		f, page := writeColumnAndGetPage(t, parquet.TimestampAdjusted(parquet.Microsecond, false), rows)
		assertMappedValues(t, f, page, want)
	})

	t.Run("dictionary page", func(t *testing.T) {
		f, page := writeDictAndGetPage(t,
			parquet.Encoded(parquet.TimestampAdjusted(parquet.Microsecond, false), &parquet.RLEDictionary),
			[]parquet.Value{parquet.Int64Value(values[0]), parquet.Int64Value(values[1]), parquet.Int64Value(values[0])},
		)
		assertMappedValues(t, f, page, append(want, want[0]))
	})
}

// TestParquet_EmptyFile_ColumnCountMatch tests that empty parquet files (0 rows)
// only check column count, not column names or types. This aligns with DuckDB behavior.
func TestParquet_EmptyFile_ColumnCountMatch(t *testing.T) {
	// Create an empty parquet file with 2 columns (id: int64, name: string)
	var buf bytes.Buffer
	schema := parquet.NewSchema("x", parquet.Group{
		"id":   parquet.Leaf(parquet.Int64Type),
		"name": parquet.Leaf(parquet.ByteArrayType),
	})
	w := parquet.NewWriter(&buf, schema)
	// Write no rows - just close to create empty file with schema
	require.NoError(t, w.Close())

	// Verify file has 0 rows
	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	require.Equal(t, int64(0), f.NumRows())

	// Create param with different column names but same count (2 columns)
	// This should succeed for empty files (only check column count)
	param := &ExternalParam{
		ExParamConst: ExParamConst{
			Ctx: context.Background(),
			Attrs: []plan.ExternAttr{
				{ColName: "col1", ColIndex: 0}, // different name from "id"
				{ColName: "col2", ColIndex: 1}, // different name from "name"
			},
			Cols: []*plan.ColDef{
				{Typ: plan.Type{Id: int32(types.T_int32)}},     // different type from int64
				{Typ: plan.Type{Id: int32(types.T_decimal64)}}, // different type from string
			},
			Extern:   &tree.ExternParam{ExParamConst: tree.ExParamConst{ScanType: tree.INLINE}},
			FileSize: []int64{int64(buf.Len())},
		},
		ExParam: ExParam{Fileparam: &ExFileparam{FileIndex: 1, FileCnt: 1}},
	}
	param.Extern.Data = string(buf.Bytes())

	// newParquetHandler should return (nil, nil) for empty file with matching column count
	h, err := newParquetHandler(param)
	require.NoError(t, err)
	require.Nil(t, h, "empty file should return nil handler")
}

// TestParquet_EmptyFile_ColumnCountMismatch tests that empty parquet files
// still fail when column count doesn't match.
func TestParquet_EmptyFile_ColumnCountMismatch(t *testing.T) {
	// Create an empty parquet file with 2 columns
	var buf bytes.Buffer
	schema := parquet.NewSchema("x", parquet.Group{
		"id":   parquet.Leaf(parquet.Int64Type),
		"name": parquet.Leaf(parquet.ByteArrayType),
	})
	w := parquet.NewWriter(&buf, schema)
	require.NoError(t, w.Close())

	// Create param expecting 3 columns (more than parquet has)
	param := &ExternalParam{
		ExParamConst: ExParamConst{
			Ctx: context.Background(),
			Attrs: []plan.ExternAttr{
				{ColName: "col1", ColIndex: 0},
				{ColName: "col2", ColIndex: 1},
				{ColName: "col3", ColIndex: 2}, // extra column
			},
			Cols: []*plan.ColDef{
				{Typ: plan.Type{Id: int32(types.T_int32)}},
				{Typ: plan.Type{Id: int32(types.T_varchar)}},
				{Typ: plan.Type{Id: int32(types.T_float64)}},
			},
			Extern:   &tree.ExternParam{ExParamConst: tree.ExParamConst{ScanType: tree.INLINE}},
			FileSize: []int64{int64(buf.Len())},
		},
		ExParam: ExParam{Fileparam: &ExFileparam{FileIndex: 1, FileCnt: 1}},
	}
	param.Extern.Data = string(buf.Bytes())

	// Should fail with column count mismatch error
	h, err := newParquetHandler(param)
	require.Error(t, err)
	require.Nil(t, h)
	require.Contains(t, err.Error(), "column count mismatch")
}

func TestParquet_IcebergEmptyFile_SkipsColumnCountMismatch(t *testing.T) {
	var buf bytes.Buffer
	schema := parquet.NewSchema("x", parquet.Group{
		"id": parquet.FieldID(parquet.Leaf(parquet.Int64Type), 1),
	})
	w := parquet.NewWriter(&buf, schema)
	require.NoError(t, w.Close())

	param := &ExternalParam{
		ExParamConst: ExParamConst{
			Ctx: context.Background(),
			Attrs: []plan.ExternAttr{
				{ColName: "id", ColIndex: 0},
				{ColName: "new_optional", ColIndex: 1},
			},
			Cols: []*plan.ColDef{
				{Typ: plan.Type{Id: int32(types.T_int64)}},
				{Typ: plan.Type{Id: int32(types.T_int32)}},
			},
			IcebergColumns: []*pipeline.IcebergColumnMapping{
				{MoColIndex: 0, IcebergFieldId: 1, CurrentFieldName: "id"},
				{MoColIndex: 1, IcebergFieldId: 2, CurrentFieldName: "new_optional", DefaultNullFill: true},
			},
			IcebergSnapshot: &pipeline.IcebergSnapshotRuntime{SnapshotId: 123},
			Extern: &tree.ExternParam{
				ExParamConst: tree.ExParamConst{ScanType: tree.INLINE, Format: tree.PARQUET},
				ExParam:      tree.ExParam{ExternType: int32(plan.ExternType_ICEBERG_TB)},
			},
			FileSize: []int64{int64(buf.Len())},
		},
		ExParam: ExParam{Fileparam: &ExFileparam{FileIndex: 1, FileCnt: 1}},
	}
	param.Extern.Data = string(buf.Bytes())

	h, err := newParquetHandler(param)
	require.NoError(t, err)
	require.Nil(t, h, "empty iceberg parquet file should be skipped")
}

// TestParquet_EmptyFile_ExtraParquetColumns tests that empty parquet files
// with more columns than table expects should fail (align with DuckDB behavior).
func TestParquet_EmptyFile_ExtraParquetColumns(t *testing.T) {
	// Create an empty parquet file with 3 columns
	var buf bytes.Buffer
	schema := parquet.NewSchema("x", parquet.Group{
		"id":    parquet.Leaf(parquet.Int64Type),
		"name":  parquet.Leaf(parquet.ByteArrayType),
		"extra": parquet.Leaf(parquet.Int32Type),
	})
	w := parquet.NewWriter(&buf, schema)
	require.NoError(t, w.Close())

	// Create param expecting only 2 columns (less than parquet has)
	param := &ExternalParam{
		ExParamConst: ExParamConst{
			Ctx: context.Background(),
			Attrs: []plan.ExternAttr{
				{ColName: "col1", ColIndex: 0},
				{ColName: "col2", ColIndex: 1},
			},
			Cols: []*plan.ColDef{
				{Typ: plan.Type{Id: int32(types.T_int32)}},
				{Typ: plan.Type{Id: int32(types.T_varchar)}},
			},
			Extern:   &tree.ExternParam{ExParamConst: tree.ExParamConst{ScanType: tree.INLINE}},
			FileSize: []int64{int64(buf.Len())},
		},
		ExParam: ExParam{Fileparam: &ExFileparam{FileIndex: 1, FileCnt: 1}},
	}
	param.Extern.Data = string(buf.Bytes())

	// Should fail - column count mismatch (parquet has 3, table expects 2)
	h, err := newParquetHandler(param)
	require.Error(t, err)
	require.Nil(t, h)
	require.Contains(t, err.Error(), "column count mismatch")
}

// TestParquet_ScanEmptyFile tests the full scanParquetFile flow with empty file.
func TestParquet_ScanEmptyFile(t *testing.T) {
	// Create an empty parquet file
	var buf bytes.Buffer
	schema := parquet.NewSchema("x", parquet.Group{
		"c": parquet.Leaf(parquet.Int32Type),
	})
	w := parquet.NewWriter(&buf, schema)
	require.NoError(t, w.Close())

	param := &ExternalParam{
		ExParamConst: ExParamConst{
			Ctx:      context.Background(),
			Attrs:    []plan.ExternAttr{{ColName: "different_name", ColIndex: 0}},
			Cols:     []*plan.ColDef{{Typ: plan.Type{Id: int32(types.T_int64)}}}, // different type
			Extern:   &tree.ExternParam{ExParamConst: tree.ExParamConst{ScanType: tree.INLINE}},
			FileSize: []int64{int64(buf.Len())},
		},
		ExParam: ExParam{Fileparam: &ExFileparam{FileIndex: 1, FileCnt: 1}},
	}
	param.Extern.Data = string(buf.Bytes())

	proc := testutil.NewProc(t)
	bat := vectorBatch([]types.Type{types.New(types.T_int64, 0, 0)})

	// ParquetReader.Open should return fileEmpty=true for empty files
	r := NewParquetReader(param, proc)
	fileEmpty, err := r.Open(param, proc)
	require.NoError(t, err)
	require.True(t, fileEmpty, "empty file should return fileEmpty=true")
	r.Close()
	require.Equal(t, 0, bat.RowCount(), "batch should have 0 rows")
}

// TestParquet_getParquetExpectedColCnt tests the helper function.
func TestParquet_getParquetExpectedColCnt(t *testing.T) {
	// Normal columns
	param := &ExternalParam{
		ExParamConst: ExParamConst{
			Attrs: []plan.ExternAttr{
				{ColName: "col1"},
				{ColName: "col2"},
				{ColName: "col3"},
			},
		},
	}
	require.Equal(t, 3, getParquetExpectedColCnt(param))

	// With hidden column __mo_filepath
	param2 := &ExternalParam{
		ExParamConst: ExParamConst{
			Attrs: []plan.ExternAttr{
				{ColName: "col1"},
				{ColName: "__mo_filepath"}, // hidden column
				{ColName: "col2"},
			},
		},
	}
	require.Equal(t, 2, getParquetExpectedColCnt(param2))
}

type parquetLoadIndexFixture struct {
	Value int64 `parquet:"value"`
}

type parquetReadRange struct {
	offset int64
	length int
}

type parquetTrackingReaderAt struct {
	reader *bytes.Reader
	reads  []parquetReadRange
}

func (r *parquetTrackingReaderAt) ReadAt(p []byte, offset int64) (int, error) {
	r.reads = append(r.reads, parquetReadRange{offset: offset, length: len(p)})
	return r.reader.ReadAt(p, offset)
}

func (r *parquetTrackingReaderAt) readsOffset(offset int64) bool {
	for _, read := range r.reads {
		if read.offset <= offset && offset < read.offset+int64(read.length) {
			return true
		}
	}
	return false
}

func TestParquetLoadFanoutSkipsUnusedIndexSections(t *testing.T) {
	var data bytes.Buffer
	writer := parquet.NewGenericWriter[parquetLoadIndexFixture](
		&data,
		parquet.MaxRowsPerRowGroup(1),
		parquet.DataPageStatistics(true),
		parquet.BloomFilters(parquet.SplitBlockFilter(10, "value")),
	)
	_, err := writer.Write([]parquetLoadIndexFixture{{Value: 1}, {Value: 2}, {Value: 3}})
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	metadataFile, err := parquet.OpenFile(bytes.NewReader(data.Bytes()), int64(data.Len()))
	require.NoError(t, err)
	chunk := metadataFile.Metadata().RowGroups[0].Columns[0]
	require.Positive(t, chunk.ColumnIndexOffset)
	require.Positive(t, chunk.OffsetIndexOffset)
	require.Positive(t, chunk.MetaData.BloomFilterOffset)

	defaultReader := &parquetTrackingReaderAt{reader: bytes.NewReader(data.Bytes())}
	_, err = parquet.OpenFile(defaultReader, int64(data.Len()))
	require.NoError(t, err)
	require.True(t, defaultReader.readsOffset(chunk.ColumnIndexOffset))
	require.True(t, defaultReader.readsOffset(chunk.OffsetIndexOffset))
	require.True(t, defaultReader.readsOffset(chunk.MetaData.BloomFilterOffset))

	param := &ExternalParam{ExParamConst: ExParamConst{
		ParquetRowGroupShards: []*pipeline.ParquetRowGroupShard{{FileIndex: 0, RowGroupStart: 0, RowGroupEnd: 1}},
	}}
	for range 3 { // One planning open plus two fanout scopes must not reread either section.
		reader := &parquetTrackingReaderAt{reader: bytes.NewReader(data.Bytes())}
		file, err := parquet.OpenFile(reader, int64(data.Len()), parquetLoadFileOptions(param)...)
		require.NoError(t, err)
		require.Len(t, file.RowGroups(), 3)
		require.False(t, reader.readsOffset(chunk.ColumnIndexOffset))
		require.False(t, reader.readsOffset(chunk.OffsetIndexOffset))
		require.False(t, reader.readsOffset(chunk.MetaData.BloomFilterOffset))
	}
}
