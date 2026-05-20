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

package external

import (
	"bytes"
	"context"
	"math/big"
	"os"
	"path/filepath"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/encoding"
	"github.com/stretchr/testify/require"
)

func TestParquetHasPhysicalAttrsCoverageHack(t *testing.T) {
	param := &ExternalParam{
		ExParamConst: ExParamConst{
			Extern: &tree.ExternParam{
				ExParamConst: tree.ExParamConst{
					HivePartitioning:  true,
					HivePartitionCols: []string{"year"},
				},
			},
			Cols: []*plan.ColDef{
				{Name: "hidden", Hidden: true},
				{Name: "year"},
				{Name: catalog.ExternalFilePath},
				{Name: "payload"},
			},
		},
	}

	param.Attrs = []plan.ExternAttr{{ColName: "bad", ColIndex: 99}}
	require.True(t, hasPhysicalParquetAttrs(param))

	param.Attrs = []plan.ExternAttr{
		{ColName: "hidden", ColIndex: 0},
		{ColName: "year", ColIndex: 1},
		{ColName: catalog.ExternalFilePath, ColIndex: 2},
	}
	require.False(t, hasPhysicalParquetAttrs(param))

	param.Attrs = append(param.Attrs, plan.ExternAttr{ColName: "payload", ColIndex: 3})
	require.True(t, hasPhysicalParquetAttrs(param))
}

func TestParquetOpenFileCoverageHack(t *testing.T) {
	ctx := context.Background()
	data := parquetCoverageBytes(t)

	localPath := filepath.Join(t.TempDir(), "data.parquet")
	require.NoError(t, os.WriteFile(localPath, data, 0644))
	param := &ExternalParam{
		ExParamConst: ExParamConst{
			Ctx:      ctx,
			Extern:   &tree.ExternParam{ExParamConst: tree.ExParamConst{ScanType: tree.INFILE}},
			FileSize: []int64{int64(len(data))},
		},
		ExParam: ExParam{Fileparam: &ExFileparam{FileIndex: 0, FileCnt: 1, Filepath: localPath}},
	}
	var h ParquetHandler
	require.ErrorContains(t, h.openFile(param, false), "invalid FileIndex")

	param.Fileparam.FileIndex = 1
	require.NoError(t, h.openFile(param, false))
	require.NotNil(t, h.file)

	bucket := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(bucket, "s3.parquet"), data, 0644))
	param = &ExternalParam{
		ExParamConst: ExParamConst{
			Ctx: ctx,
			Extern: &tree.ExternParam{
				ExParamConst: tree.ExParamConst{ScanType: tree.S3},
				ExParam: tree.ExParam{
					S3Param: &tree.S3Parameter{Endpoint: "disk", Bucket: bucket},
				},
			},
			FileSize: []int64{int64(len(data))},
		},
		ExParam: ExParam{Fileparam: &ExFileparam{FileIndex: 1, FileCnt: 1, Filepath: "s3.parquet"}},
	}
	h = ParquetHandler{}
	require.NoError(t, h.openFile(param, true))
	require.NotNil(t, h.file)
}

func TestParquetValueConvertersCoverageHack(t *testing.T) {
	ctx := context.Background()

	i, err := parquetValueToInt64(ctx, parquet.BooleanType, parquet.BooleanValue(false))
	require.NoError(t, err)
	require.Equal(t, int64(0), i)
	i, err = parquetValueToInt64(ctx, parquet.Uint(32).Type(), parquet.ValueOf(uint32(7)))
	require.NoError(t, err)
	require.Equal(t, int64(7), i)
	i, err = parquetValueToInt64(ctx, parquet.Int64Type, parquet.Int64Value(-9))
	require.NoError(t, err)
	require.Equal(t, int64(-9), i)
	_, err = parquetValueToInt64(ctx, parquet.ByteArrayType, parquet.ByteArrayValue([]byte("x")))
	require.Error(t, err)

	u, err := parquetValueToUint64(ctx, parquet.BooleanType, parquet.BooleanValue(true))
	require.NoError(t, err)
	require.Equal(t, uint64(1), u)
	u, err = parquetValueToUint64(ctx, parquet.Uint(32).Type(), parquet.ValueOf(uint32(8)))
	require.NoError(t, err)
	require.Equal(t, uint64(8), u)
	u, err = parquetValueToUint64(ctx, parquet.Uint(64).Type(), parquet.ValueOf(uint64(9)))
	require.NoError(t, err)
	require.Equal(t, uint64(9), u)
	_, err = parquetValueToUint64(ctx, parquet.Int32Type, parquet.Int32Value(-1))
	require.Error(t, err)
	_, err = parquetValueToUint64(ctx, parquet.Int64Type, parquet.Int64Value(-1))
	require.Error(t, err)
	_, err = parquetValueToUint64(ctx, parquet.ByteArrayType, parquet.ByteArrayValue([]byte("x")))
	require.Error(t, err)

	f, err := parquetValueToFloat64(ctx, parquet.Decimal(0, 9, parquet.Int32Type).Type(), parquet.Int32Value(12))
	require.NoError(t, err)
	require.Equal(t, float64(12), f)
	f, err = parquetValueToFloat64(ctx, parquet.Int64Type, parquet.Int64Value(13))
	require.NoError(t, err)
	require.Equal(t, float64(13), f)
	f, err = parquetValueToFloat64(ctx, parquet.FloatType, parquet.FloatValue(1.5))
	require.NoError(t, err)
	require.Equal(t, float64(1.5), f)
	f, err = parquetValueToFloat64(ctx, parquet.DoubleType, parquet.DoubleValue(2.5))
	require.NoError(t, err)
	require.Equal(t, float64(2.5), f)
	_, err = parquetValueToFloat64(ctx, parquet.ByteArrayType, parquet.ByteArrayValue([]byte("x")))
	require.Error(t, err)

	requireParquetString(t, ctx, parquet.Decimal(0, 9, parquet.Int32Type).Type(), parquet.Int32Value(14), "14")
	requireParquetString(t, ctx, parquet.Date().Type(), parquet.Int32Value(1), "1970-01-02")
	requireParquetStringNotEmpty(t, ctx, parquet.Time(parquet.Nanosecond).Type(), parquet.Int64Value(1_000))
	requireParquetStringNotEmpty(t, ctx, parquet.Time(parquet.Microsecond).Type(), parquet.Int64Value(2_000))
	requireParquetStringNotEmpty(t, ctx, parquet.Time(parquet.Millisecond).Type(), parquet.Int32Value(3))
	requireParquetStringNotEmpty(t, ctx, parquet.Timestamp(parquet.Nanosecond).Type(), parquet.Int64Value(1_000))
	requireParquetStringNotEmpty(t, ctx, parquet.TimestampAdjusted(parquet.Microsecond, false).Type(), parquet.Int64Value(2_000))
	requireParquetString(t, ctx, parquet.BooleanType, parquet.BooleanValue(true), "true")
	requireParquetString(t, ctx, parquet.Uint(32).Type(), parquet.ValueOf(uint32(15)), "15")
	requireParquetString(t, ctx, parquet.Int64Type, parquet.Int64Value(-16), "-16")
	requireParquetString(t, ctx, parquet.FloatType, parquet.FloatValue(1.25), "1.25")
	requireParquetString(t, ctx, parquet.DoubleType, parquet.DoubleValue(2.25), "2.25")
	_, err = parquetValueToString(ctx, parquet.ByteArrayType, parquet.ByteArrayValue([]byte("x")))
	require.Error(t, err)

	dec, err := parquetValueToDecimal256(ctx, parquet.Int32Type, parquet.Int32Value(17))
	require.NoError(t, err)
	require.Equal(t, "17", dec.Format(0))
	dec, err = parquetValueToDecimal256(ctx, parquet.Int64Type, parquet.Int64Value(18))
	require.NoError(t, err)
	require.Equal(t, "18", dec.Format(0))
	b, err := bigIntToTwosComplementBytes(ctx, big.NewInt(19), 8)
	require.NoError(t, err)
	dec, err = parquetValueToDecimal256(ctx, parquet.FixedLenByteArrayType(8), parquet.FixedLenByteArrayValue(b))
	require.NoError(t, err)
	require.Equal(t, "19", dec.Format(0))
	_, err = parquetValueToDecimal256(ctx, parquet.BooleanType, parquet.BooleanValue(true))
	require.Error(t, err)
}

func TestParquetProcessHelpersCoverageHack(t *testing.T) {
	ctx := context.Background()
	proc := testutil.NewProc(t)

	emptyJSONPage := parquet.ByteArrayType.NewPage(0, 0, encoding.ByteArrayValues(nil, []uint32{0}))
	require.NoError(t, processStringToJson(ctx, &columnMapper{}, emptyJSONPage, proc, vector.NewVec(types.T_json.ToType())))

	jsonPage := parquetOptionalPage(t, parquet.String(), []parquet.Row{
		{parquet.ByteArrayValue([]byte(`{"a":1}`)).Level(0, 1, 0)},
		{parquet.NullValue().Level(0, 0, 0)},
	})
	jsonVec := vector.NewVec(types.T_json.ToType())
	require.NoError(t, processStringToJson(ctx, &columnMapper{srcNull: true, dstNull: true, maxDefinitionLevel: 1}, jsonPage, proc, jsonVec))
	require.Equal(t, 2, jsonVec.Length())

	intPage := parquetOptionalPage(t, parquet.Leaf(parquet.Int32Type), []parquet.Row{
		{parquet.NullValue().Level(0, 0, 0)},
	})
	intVec := vector.NewVec(types.T_int32.ToType())
	require.NoError(t, processParquetValuesToFixed[int32](ctx,
		&columnMapper{srcNull: true, dstNull: true, maxDefinitionLevel: 1},
		intPage, proc, intVec, 0, func(v parquet.Value) (int32, error) { return v.Int32(), nil }))
	require.Equal(t, 1, intVec.Length())

	err := processParquetValuesToFixed[int32](ctx,
		&columnMapper{srcNull: true, dstNull: false, maxDefinitionLevel: 1},
		intPage, proc, vector.NewVec(types.T_int32.ToType()), 0,
		func(v parquet.Value) (int32, error) { return v.Int32(), nil })
	require.Error(t, err)

	requiredIntPage := parquet.Int32Type.NewPage(0, 1, encoding.Int32Values([]int32{1}))
	err = processParquetValuesToFixed[int32](ctx, &columnMapper{}, requiredIntPage, proc, vector.NewVec(types.T_int32.ToType()), 0,
		func(v parquet.Value) (int32, error) { return 0, moerr.NewInvalidInputNoCtx("coverage") })
	require.Error(t, err)

	bytesPage := parquetOptionalPage(t, parquet.Leaf(parquet.ByteArrayType), []parquet.Row{
		{parquet.NullValue().Level(0, 0, 0)},
	})
	bytesVec := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, processParquetValuesToBytes(ctx,
		&columnMapper{srcNull: true, dstNull: true, maxDefinitionLevel: 1},
		bytesPage, proc, bytesVec, func(v parquet.Value) ([]byte, error) { return v.ByteArray(), nil }))
	require.Equal(t, 1, bytesVec.Length())

	err = processParquetValuesToBytes(ctx,
		&columnMapper{srcNull: true, dstNull: false, maxDefinitionLevel: 1},
		bytesPage, proc, vector.NewVec(types.T_varchar.ToType()),
		func(v parquet.Value) ([]byte, error) { return v.ByteArray(), nil })
	require.Error(t, err)

	requiredBytesPage := parquet.ByteArrayType.NewPage(0, 1, encoding.ByteArrayValues([]byte("x"), []uint32{0, 1}))
	err = processParquetValuesToBytes(ctx, &columnMapper{}, requiredBytesPage, proc, vector.NewVec(types.T_varchar.ToType()),
		func(v parquet.Value) ([]byte, error) { return nil, moerr.NewInvalidInputNoCtx("coverage") })
	require.Error(t, err)

	emptyListPage := parquet.FloatType.NewPage(0, 0, encoding.FloatValues(nil))
	require.NoError(t, processParquetListToArray[float32](ctx, &columnMapper{}, emptyListPage, proc,
		vector.NewVec(types.New(types.T_array_float32, 1, 0)), 1, func(v parquet.Value) (float32, error) { return v.Float(), nil }))

	nullListPage := parquet.FloatType.NewPage(0, 1, encoding.FloatValues([]float32{1}))
	arrayVec := vector.NewVec(types.New(types.T_array_float32, 1, 0))
	require.NoError(t, processParquetListToArray[float32](ctx,
		&columnMapper{dstNull: true, maxDefinitionLevel: 1},
		nullListPage, proc, arrayVec, 1, func(v parquet.Value) (float32, error) { return v.Float(), nil }))
	require.Equal(t, 1, arrayVec.Length())

	err = processParquetListToArray[float32](ctx,
		&columnMapper{dstNull: false, maxDefinitionLevel: 1},
		nullListPage, proc, vector.NewVec(types.New(types.T_array_float32, 1, 0)), 1,
		func(v parquet.Value) (float32, error) { return v.Float(), nil })
	require.Error(t, err)

	err = processParquetListToArray[float32](ctx, &columnMapper{}, nullListPage, proc,
		vector.NewVec(types.New(types.T_array_float32, 1, 0)), 1,
		func(v parquet.Value) (float32, error) { return 0, moerr.NewInvalidInputNoCtx("coverage") })
	require.Error(t, err)
}

func TestParquetSourceKindCoverageHack(t *testing.T) {
	require.False(t, isParquetIntegerSource(parquet.Decimal(0, 9, parquet.Int32Type).Type(), true))
	require.False(t, isParquetIntegerSource(parquet.Date().Type(), true))
	require.False(t, isParquetIntegerSource(parquet.BooleanType, false))
	require.True(t, isParquetIntegerSource(parquet.BooleanType, true))
	require.True(t, isParquetIntegerSource(parquet.Int64Type, false))
	require.False(t, isParquetIntegerSource(parquet.ByteArrayType, true))

	require.True(t, isPlainOrSignedIntegerLogical(parquet.Int32Type))
	require.True(t, isPlainOrSignedIntegerLogical(parquet.Int(32).Type()))
	require.False(t, isPlainOrSignedIntegerLogical(parquet.Uint(32).Type()))

	require.True(t, isParquetFloatConvertibleSource(parquet.Int32Type))
	require.True(t, isParquetFloatConvertibleSource(parquet.Decimal(0, 9, parquet.Int64Type).Type()))
	require.True(t, isParquetFloatConvertibleSource(parquet.FloatType))
	require.True(t, isParquetFloatConvertibleSource(parquet.Decimal(0, 9, parquet.FixedLenByteArrayType(4)).Type()))
	require.False(t, isParquetFloatConvertibleSource(parquet.ByteArrayType))
	require.False(t, isParquetFloatConvertibleSource(parquet.BooleanType))

	require.True(t, isParquetValueStringConvertibleSource(parquet.Decimal(0, 9, parquet.Int32Type).Type()))
	require.True(t, isParquetValueStringConvertibleSource(parquet.Timestamp(parquet.Microsecond).Type()))
	require.True(t, isParquetValueStringConvertibleSource(parquet.DoubleType))
	require.False(t, isParquetValueStringConvertibleSource(parquet.ByteArrayType))
}

func parquetCoverageBytes(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	schema := parquet.NewSchema("x", parquet.Group{"c": parquet.Leaf(parquet.Int32Type)})
	w := parquet.NewWriter(&buf, schema)
	_, err := w.WriteRows([]parquet.Row{{parquet.Int32Value(7).Level(0, 0, 0)}})
	require.NoError(t, err)
	require.NoError(t, w.Close())
	return buf.Bytes()
}

func parquetOptionalPage(t *testing.T, node parquet.Node, rows []parquet.Row) parquet.Page {
	t.Helper()
	var buf bytes.Buffer
	schema := parquet.NewSchema("x", parquet.Group{"c": parquet.Optional(node)})
	w := parquet.NewWriter(&buf, schema)
	_, err := w.WriteRows(rows)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	page, err := f.Root().Column("c").Pages().ReadPage()
	require.NoError(t, err)
	return page
}

func requireParquetString(t *testing.T, ctx context.Context, st parquet.Type, v parquet.Value, expected string) {
	t.Helper()
	got, err := parquetValueToString(ctx, st, v)
	require.NoError(t, err)
	require.Equal(t, expected, got)
}

func requireParquetStringNotEmpty(t *testing.T, ctx context.Context, st parquet.Type, v parquet.Value) {
	t.Helper()
	got, err := parquetValueToString(ctx, st, v)
	require.NoError(t, err)
	require.NotEmpty(t, got)
}
