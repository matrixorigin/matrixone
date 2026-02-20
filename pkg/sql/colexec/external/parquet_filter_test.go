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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

// --- parquetValuesToZoneMap tests ---

func TestParquetValuesToZoneMap_Int32(t *testing.T) {
	minVal := parquet.Int32Value(10)
	maxVal := parquet.Int32Value(100)
	zm := parquetValuesToZoneMap(minVal, maxVal, types.T_int32)
	require.NotNil(t, zm)
	require.True(t, zm.IsInited())

	// Verify min/max bytes round-trip
	gotMin := types.DecodeInt32(zm.GetMinBuf())
	gotMax := types.DecodeInt32(zm.GetMaxBuf())
	require.Equal(t, int32(10), gotMin)
	require.Equal(t, int32(100), gotMax)
}

func TestParquetValuesToZoneMap_Int64(t *testing.T) {
	minVal := parquet.Int64Value(500)
	maxVal := parquet.Int64Value(9999)
	zm := parquetValuesToZoneMap(minVal, maxVal, types.T_int64)
	require.NotNil(t, zm)
	require.Equal(t, int64(500), types.DecodeInt64(zm.GetMinBuf()))
	require.Equal(t, int64(9999), types.DecodeInt64(zm.GetMaxBuf()))
}

func TestParquetValuesToZoneMap_Float64(t *testing.T) {
	minVal := parquet.DoubleValue(1.5)
	maxVal := parquet.DoubleValue(99.9)
	zm := parquetValuesToZoneMap(minVal, maxVal, types.T_float64)
	require.NotNil(t, zm)
	require.Equal(t, 1.5, types.DecodeFloat64(zm.GetMinBuf()))
	require.Equal(t, 99.9, types.DecodeFloat64(zm.GetMaxBuf()))
}

func TestParquetValuesToZoneMap_Float32(t *testing.T) {
	minVal := parquet.FloatValue(2.0)
	maxVal := parquet.FloatValue(8.0)
	zm := parquetValuesToZoneMap(minVal, maxVal, types.T_float32)
	require.NotNil(t, zm)
	require.Equal(t, float32(2.0), types.DecodeFloat32(zm.GetMinBuf()))
	require.Equal(t, float32(8.0), types.DecodeFloat32(zm.GetMaxBuf()))
}

func TestParquetValuesToZoneMap_Bool(t *testing.T) {
	minVal := parquet.BooleanValue(false)
	maxVal := parquet.BooleanValue(true)
	zm := parquetValuesToZoneMap(minVal, maxVal, types.T_bool)
	require.NotNil(t, zm)
	require.Equal(t, false, types.DecodeBool(zm.GetMinBuf()))
	require.Equal(t, true, types.DecodeBool(zm.GetMaxBuf()))
}

func TestParquetValuesToZoneMap_Varchar(t *testing.T) {
	minVal := parquet.ByteArrayValue([]byte("aaa"))
	maxVal := parquet.ByteArrayValue([]byte("zzz"))
	// String types are currently disabled for RowGroup filtering
	zm := parquetValuesToZoneMap(minVal, maxVal, types.T_varchar)
	require.Nil(t, zm)
}

func TestParquetValuesToZoneMap_SmallInts(t *testing.T) {
	// int8
	zm := parquetValuesToZoneMap(parquet.Int32Value(-10), parquet.Int32Value(20), types.T_int8)
	require.NotNil(t, zm)
	require.Equal(t, int8(-10), types.DecodeInt8(zm.GetMinBuf()))
	require.Equal(t, int8(20), types.DecodeInt8(zm.GetMaxBuf()))

	// int16
	zm = parquetValuesToZoneMap(parquet.Int32Value(-1000), parquet.Int32Value(2000), types.T_int16)
	require.NotNil(t, zm)
	require.Equal(t, int16(-1000), types.DecodeInt16(zm.GetMinBuf()))
	require.Equal(t, int16(2000), types.DecodeInt16(zm.GetMaxBuf()))

	// uint8
	zm = parquetValuesToZoneMap(parquet.Int32Value(0), parquet.Int32Value(255), types.T_uint8)
	require.NotNil(t, zm)
	require.Equal(t, uint8(0), types.DecodeUint8(zm.GetMinBuf()))
	require.Equal(t, uint8(255), types.DecodeUint8(zm.GetMaxBuf()))

	// uint16
	zm = parquetValuesToZoneMap(parquet.Int32Value(100), parquet.Int32Value(60000), types.T_uint16)
	require.NotNil(t, zm)
	require.Equal(t, uint16(100), types.DecodeUint16(zm.GetMinBuf()))
	require.Equal(t, uint16(60000), types.DecodeUint16(zm.GetMaxBuf()))

	// uint32
	zm = parquetValuesToZoneMap(parquet.Int32Value(0), parquet.Int32Value(100), types.T_uint32)
	require.NotNil(t, zm)
	require.Equal(t, uint32(0), types.DecodeUint32(zm.GetMinBuf()))
	require.Equal(t, uint32(100), types.DecodeUint32(zm.GetMaxBuf()))

	// uint64
	zm = parquetValuesToZoneMap(parquet.Int64Value(0), parquet.Int64Value(1000000), types.T_uint64)
	require.NotNil(t, zm)
	require.Equal(t, uint64(0), types.DecodeUint64(zm.GetMinBuf()))
	require.Equal(t, uint64(1000000), types.DecodeUint64(zm.GetMaxBuf()))
}

func TestParquetValuesToZoneMap_Date(t *testing.T) {
	// Parquet DATE is int32 days since epoch
	minVal := parquet.Int32Value(0)   // 1970-01-01
	maxVal := parquet.Int32Value(365) // 1971-01-01
	zm := parquetValuesToZoneMap(minVal, maxVal, types.T_date)
	require.NotNil(t, zm)
	require.True(t, zm.IsInited())
}

func TestParquetValuesToZoneMap_Timestamp(t *testing.T) {
	minVal := parquet.Int64Value(0)
	maxVal := parquet.Int64Value(1000000)
	zm := parquetValuesToZoneMap(minVal, maxVal, types.T_timestamp)
	require.NotNil(t, zm)
	require.True(t, zm.IsInited())
}

func TestParquetValuesToZoneMap_NullValues(t *testing.T) {
	zm := parquetValuesToZoneMap(parquet.NullValue(), parquet.Int32Value(10), types.T_int32)
	require.Nil(t, zm)

	zm = parquetValuesToZoneMap(parquet.Int32Value(10), parquet.NullValue(), types.T_int32)
	require.Nil(t, zm)
}

func TestParquetValuesToZoneMap_UnsupportedType(t *testing.T) {
	// T_decimal128 is not in our supported list
	zm := parquetValuesToZoneMap(parquet.Int64Value(1), parquet.Int64Value(2), types.T_decimal128)
	require.Nil(t, zm)
}

// --- buildColumnMetaFromParquet tests ---

func TestBuildColumnMetaFromParquet_WithBounds(t *testing.T) {
	// Write a parquet file with int32 column containing values [10, 20, 30]
	// so the RowGroup has statistics min=10, max=30
	buf := writeParquetInt32File(t, []int32{10, 20, 30}, 0)
	f, err := parquet.OpenFile(bytes.NewReader(buf), int64(len(buf)))
	require.NoError(t, err)

	rgs := f.RowGroups()
	require.Len(t, rgs, 1)

	chunks := rgs[0].ColumnChunks()
	require.True(t, len(chunks) > 0)

	meta := buildColumnMetaFromParquet(chunks[0], types.T_int32)
	require.NotNil(t, meta)

	zm := meta.ZoneMap()
	require.True(t, zm.IsInited())
	require.Equal(t, int32(10), types.DecodeInt32(zm.GetMinBuf()))
	require.Equal(t, int32(30), types.DecodeInt32(zm.GetMaxBuf()))
}

func TestBuildColumnMetaFromParquet_UnsupportedType(t *testing.T) {
	buf := writeParquetInt32File(t, []int32{1, 2}, 0)
	f, err := parquet.OpenFile(bytes.NewReader(buf), int64(len(buf)))
	require.NoError(t, err)

	chunks := f.RowGroups()[0].ColumnChunks()
	// Use an unsupported MO type
	meta := buildColumnMetaFromParquet(chunks[0], types.T_decimal128)
	require.Nil(t, meta)
}

// --- parquetColumnMetaFetcher tests ---

func TestParquetColumnMetaFetcher_MustGetColumn(t *testing.T) {
	zm := index.NewZM(types.T_int32, 0)
	v1 := int32(5)
	v2 := int32(50)
	index.UpdateZM(zm, types.EncodeInt32(&v1))
	index.UpdateZM(zm, types.EncodeInt32(&v2))

	meta := buildColumnMetaForTest(zm, 0)
	fetcher := &parquetColumnMetaFetcher{
		metas: map[uint16]objectio.ColumnMeta{0: meta},
	}

	// Existing key returns the meta
	got := fetcher.MustGetColumn(0)
	require.NotNil(t, got)
	require.True(t, got.ZoneMap().IsInited())

	// Missing key returns empty meta (not nil)
	got2 := fetcher.MustGetColumn(99)
	require.NotNil(t, got2)
}

// --- initRowGroupFilter tests ---

func TestInitRowGroupFilter_NoFilter(t *testing.T) {
	buf := writeParquetInt32File(t, []int32{1, 2, 3}, 0)
	f, err := parquet.OpenFile(bytes.NewReader(buf), int64(len(buf)))
	require.NoError(t, err)

	h := &ParquetHandler{file: f, batchCnt: maxParquetBatchCnt}
	param := makeParquetParam("c", types.T_int32)
	// No filter expression
	param.Filter = &FilterParam{}

	require.NoError(t, h.prepare(param))
	require.False(t, h.canFilter)
}

func TestInitRowGroupFilter_WithFilter(t *testing.T) {
	buf := writeParquetInt32File(t, []int32{1, 2, 3}, 0)
	f, err := parquet.OpenFile(bytes.NewReader(buf), int64(len(buf)))
	require.NoError(t, err)

	h := &ParquetHandler{file: f, batchCnt: maxParquetBatchCnt}
	param := makeParquetParam("c", types.T_int32)
	// Set up a filter expression (just needs to be non-nil for initRowGroupFilter)
	param.Filter = &FilterParam{
		FilterExpr: makeConstTrueExpr(), // dummy expr
		columnMap:  map[int]int{0: 0},
		AuxIdCnt:   1,
	}

	require.NoError(t, h.prepare(param))
	require.True(t, h.canFilter)
	require.Contains(t, h.filterColMap, 0)
}

// --- canSkipRowGroup tests ---

func TestCanSkipRowGroup_NoFilter(t *testing.T) {
	buf := writeParquetInt32File(t, []int32{1, 2, 3}, 0)
	f, err := parquet.OpenFile(bytes.NewReader(buf), int64(len(buf)))
	require.NoError(t, err)

	h := &ParquetHandler{file: f, batchCnt: maxParquetBatchCnt, canFilter: false}
	proc := testutil.NewProc(t)

	rgs := f.RowGroups()
	require.True(t, len(rgs) > 0)
	// Without filter, should never skip
	require.False(t, h.canSkipRowGroup(rgs[0], makeParquetParam("c", types.T_int32), proc))
}

// --- getDataByRowGroup end-to-end tests ---

func TestGetDataByRowGroup_ReadsAllWhenNoSkip(t *testing.T) {
	// Create a file with multiple RowGroups by using small MaxRowsPerRowGroup
	values := []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	buf := writeParquetInt32File(t, values, 2) // 2 rows per RowGroup → 5 RowGroups
	f, err := parquet.OpenFile(bytes.NewReader(buf), int64(len(buf)))
	require.NoError(t, err)

	rgs := f.RowGroups()
	require.True(t, len(rgs) > 1, "expected multiple RowGroups, got %d", len(rgs))

	h := &ParquetHandler{file: f, batchCnt: maxParquetBatchCnt}
	param := makeParquetParam("c", types.T_int32)
	// No filter → canFilter=false → should fall through to getDataByPage
	require.NoError(t, h.prepare(param))
	require.False(t, h.canFilter)

	proc := testutil.NewProc(t)
	bat := vectorBatch([]types.Type{types.New(types.T_int32, 0, 0)})
	require.NoError(t, h.getData(bat, param, proc))

	// Should read all 10 values
	require.Equal(t, 10, bat.RowCount())
	got := vector.MustFixedColWithTypeCheck[int32](bat.Vecs[0])
	require.Equal(t, values, got[:bat.RowCount()])
}

func TestGetDataByRowGroup_WithFilterEnabled(t *testing.T) {
	// Create a file with multiple RowGroups
	// RowGroup 1: [1, 2], RowGroup 2: [3, 4], RowGroup 3: [5, 6]
	values := []int32{1, 2, 3, 4, 5, 6}
	buf := writeParquetInt32File(t, values, 2)
	f, err := parquet.OpenFile(bytes.NewReader(buf), int64(len(buf)))
	require.NoError(t, err)

	rgs := f.RowGroups()
	require.True(t, len(rgs) >= 3, "expected >=3 RowGroups, got %d", len(rgs))

	h := &ParquetHandler{file: f, batchCnt: maxParquetBatchCnt}
	param := makeParquetParam("c", types.T_int32)
	// Set up filter that won't actually skip anything (dummy const expr)
	// This tests the getDataByRowGroup path is used when canFilter=true
	param.Filter = &FilterParam{
		FilterExpr: makeConstTrueExpr(),
		columnMap:  map[int]int{0: 0},
		AuxIdCnt:   1,
	}

	require.NoError(t, h.prepare(param))
	require.True(t, h.canFilter)

	proc := testutil.NewProc(t)
	bat := vectorBatch([]types.Type{types.New(types.T_int32, 0, 0)})
	require.NoError(t, h.getDataByRowGroup(bat, param, proc))

	// With a const expr that doesn't actually filter, all rows should be read
	require.Equal(t, 6, bat.RowCount())
	got := vector.MustFixedColWithTypeCheck[int32](bat.Vecs[0])
	require.Equal(t, values, got[:bat.RowCount()])
}

func TestGetDataByRowGroup_SteppedBatches(t *testing.T) {
	// Test that getDataByRowGroup respects batchCnt and returns partial results
	values := []int32{1, 2, 3, 4, 5, 6}
	buf := writeParquetInt32File(t, values, 2) // 3 RowGroups of 2 rows each
	f, err := parquet.OpenFile(bytes.NewReader(buf), int64(len(buf)))
	require.NoError(t, err)

	h := &ParquetHandler{file: f, batchCnt: 3} // read at most 3 rows per call
	param := makeParquetParam("c", types.T_int32)
	param.Filter = &FilterParam{
		FilterExpr: makeConstTrueExpr(),
		columnMap:  map[int]int{0: 0},
		AuxIdCnt:   1,
	}
	require.NoError(t, h.prepare(param))
	require.True(t, h.canFilter)

	proc := testutil.NewProc(t)

	// First batch: should get up to 3 rows
	bat1 := vectorBatch([]types.Type{types.New(types.T_int32, 0, 0)})
	require.NoError(t, h.getDataByRowGroup(bat1, param, proc))
	require.True(t, bat1.RowCount() > 0 && bat1.RowCount() <= 3)

	// Second batch: should get remaining rows
	bat2 := vectorBatch([]types.Type{types.New(types.T_int32, 0, 0)})
	require.NoError(t, h.getDataByRowGroup(bat2, param, proc))

	// Collect all values
	got1 := vector.MustFixedColWithTypeCheck[int32](bat1.Vecs[0])
	got2 := vector.MustFixedColWithTypeCheck[int32](bat2.Vecs[0])
	var all []int32
	all = append(all, got1[:bat1.RowCount()]...)
	all = append(all, got2[:bat2.RowCount()]...)

	// May need a third batch if not all consumed
	if h.curRGIdx < len(h.rowGroups) {
		bat3 := vectorBatch([]types.Type{types.New(types.T_int32, 0, 0)})
		require.NoError(t, h.getDataByRowGroup(bat3, param, proc))
		got3 := vector.MustFixedColWithTypeCheck[int32](bat3.Vecs[0])
		all = append(all, got3[:bat3.RowCount()]...)
	}

	require.Equal(t, values, all)
}

func TestGetDataByRowGroup_EmptyFile(t *testing.T) {
	// Write a parquet file with 0 rows
	var buf bytes.Buffer
	schema := parquet.NewSchema("x", parquet.Group{"c": parquet.Leaf(parquet.Int32Type)})
	w := parquet.NewWriter(&buf, schema)
	require.NoError(t, w.Close())

	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)

	h := &ParquetHandler{file: f, batchCnt: maxParquetBatchCnt}
	param := makeParquetParam("c", types.T_int32)
	param.Filter = &FilterParam{
		FilterExpr: makeConstTrueExpr(),
		columnMap:  map[int]int{0: 0},
		AuxIdCnt:   1,
	}
	require.NoError(t, h.prepare(param))

	proc := testutil.NewProc(t)
	bat := vectorBatch([]types.Type{types.New(types.T_int32, 0, 0)})
	require.NoError(t, h.getDataByRowGroup(bat, param, proc))
	require.Equal(t, 0, bat.RowCount())
}

// --- rgPagesOpen / closeRGPages tests ---

func TestRGPagesOpen_InitiallyFalse(t *testing.T) {
	buf := writeParquetInt32File(t, []int32{1, 2}, 0)
	f, err := parquet.OpenFile(bytes.NewReader(buf), int64(len(buf)))
	require.NoError(t, err)

	h := &ParquetHandler{file: f, batchCnt: maxParquetBatchCnt}
	param := makeParquetParam("c", types.T_int32)
	param.Filter = &FilterParam{
		FilterExpr: makeConstTrueExpr(),
		columnMap:  map[int]int{0: 0},
		AuxIdCnt:   1,
	}
	require.NoError(t, h.prepare(param))
	require.True(t, h.canFilter)

	// Pages should not be open initially when canFilter=true
	require.False(t, h.rgPagesOpen(param))
}

func TestCloseRGPages_ClearsState(t *testing.T) {
	buf := writeParquetInt32File(t, []int32{1, 2}, 0)
	f, err := parquet.OpenFile(bytes.NewReader(buf), int64(len(buf)))
	require.NoError(t, err)

	h := &ParquetHandler{file: f, batchCnt: maxParquetBatchCnt}
	param := makeParquetParam("c", types.T_int32)
	param.Filter = &FilterParam{
		FilterExpr: makeConstTrueExpr(),
		columnMap:  map[int]int{0: 0},
		AuxIdCnt:   1,
	}
	require.NoError(t, h.prepare(param))

	// Manually open pages for the first RowGroup
	rgs := f.RowGroups()
	if len(rgs) > 0 {
		chunks := rgs[0].ColumnChunks()
		h.pages[0] = chunks[0].Pages()
		require.True(t, h.rgPagesOpen(param))

		h.closeRGPages()
		require.False(t, h.rgPagesOpen(param))
		require.Nil(t, h.pages[0])
		require.Nil(t, h.currentPage[0])
		require.Equal(t, int64(0), h.pageOffset[0])
	}
}

// --- Test helpers ---

// writeParquetInt32File writes a parquet file with a single int32 column "c".
// If maxRowsPerRG > 0, it forces RowGroup splits at that row count.
func writeParquetInt32File(t *testing.T, values []int32, maxRowsPerRG int) []byte {
	t.Helper()
	var buf bytes.Buffer
	schema := parquet.NewSchema("x", parquet.Group{"c": parquet.Leaf(parquet.Int32Type)})

	opts := []parquet.WriterOption{schema}
	if maxRowsPerRG > 0 {
		opts = append(opts, parquet.MaxRowsPerRowGroup(int64(maxRowsPerRG)))
	}
	w := parquet.NewWriter(&buf, opts...)

	for _, v := range values {
		row := parquet.MakeRow([]parquet.Value{parquet.Int32Value(v).Level(0, 0, 0)})
		_, err := w.WriteRows([]parquet.Row{row})
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())
	return buf.Bytes()
}

// makeParquetParam creates a minimal ExternalParam for testing with a single column.
func makeParquetParam(colName string, colType types.T) *ExternalParam {
	return &ExternalParam{
		ExParamConst: ExParamConst{
			Ctx:      context.Background(),
			Attrs:    []plan.ExternAttr{{ColName: colName, ColIndex: 0}},
			Cols:     []*plan.ColDef{{Typ: plan.Type{Id: int32(colType), NotNullable: true}}},
			Extern:   &tree.ExternParam{ExParamConst: tree.ExParamConst{ScanType: tree.INLINE}},
			FileSize: []int64{0},
		},
		ExParam: ExParam{
			Fileparam: &ExFileparam{FileIndex: 1, FileCnt: 1},
			Filter:    &FilterParam{},
		},
	}
}

// makeConstTrueExpr creates a constant boolean TRUE expression for testing.
// EvaluateFilterByZoneMap with a const bool(true) returns needRead=true,
// so no RowGroups will be skipped.
func makeConstTrueExpr() *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{
				Value: &plan.Literal_Bval{Bval: true},
			},
		},
	}
}

// buildColumnMetaForTest creates a ColumnMeta with the given ZoneMap for testing.
func buildColumnMetaForTest(zm index.ZM, nullCnt uint32) objectio.ColumnMeta {
	meta := objectio.BuildColumnMeta()
	meta.SetZoneMap(zm)
	meta.SetNullCnt(nullCnt)
	return meta
}
