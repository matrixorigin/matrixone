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
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

func TestParquetNestedProjectionPrunesUnprojectedColumns(t *testing.T) {
	data := writeNestedProjectionParquet(t, 2, 64)
	param := nestedProjectionParam(data)
	proc := testutil.NewProc(t)
	r := NewParquetReader(param, proc)

	fileEmpty, err := r.Open(param, proc)
	require.NoError(t, err)
	require.False(t, fileEmpty)
	defer r.Close()

	require.Equal(t, [][]string{{"z_nested", "v"}}, r.h.rowReader.Schema().Columns())

	bat := vectorBatch([]types.Type{types.T_text.ToType()})
	defer bat.Clean(proc.Mp())
	finished, err := r.ReadBatch(context.Background(), bat, proc, nil)
	require.NoError(t, err)
	require.True(t, finished)
	require.Equal(t, 2, bat.RowCount())
	require.JSONEq(t, `{"v":0}`, bat.Vecs[0].GetStringAt(0))
	require.JSONEq(t, `{"v":1}`, bat.Vecs[0].GetStringAt(1))
}

func TestParquetNestedAndScalarProjectionPrunesOtherColumns(t *testing.T) {
	data := writeNestedProjectionParquet(t, 2, 64)
	param := nestedAndScalarProjectionParam(data)
	proc := testutil.NewProc(t)
	r := NewParquetReader(param, proc)

	fileEmpty, err := r.Open(param, proc)
	require.NoError(t, err)
	require.False(t, fileEmpty)
	defer r.Close()

	// Only nested columns are fed to the row reader. The projected scalar
	// sibling remains on the page-vectorized path.
	require.Equal(t, [][]string{{"z_nested", "v"}}, r.h.rowReader.Schema().Columns())
	require.Equal(t, []int{0}, r.h.dataColIndices)
	require.NotNil(t, r.h.pages[0])
	for _, pages := range r.h.pages[1:] {
		require.Nil(t, pages)
	}

	bat := vectorBatch([]types.Type{types.T_int32.ToType(), types.T_text.ToType()})
	defer bat.Clean(proc.Mp())
	finished, err := r.ReadBatch(context.Background(), bat, proc, nil)
	require.NoError(t, err)
	require.True(t, finished)
	require.Equal(t, 2, bat.RowCount())
	require.Equal(t, []int32{0, 0}, vector.MustFixedColWithTypeCheck[int32](bat.Vecs[0]))
	require.JSONEq(t, `{"v":0}`, bat.Vecs[1].GetStringAt(0))
	require.JSONEq(t, `{"v":1}`, bat.Vecs[1].GetStringAt(1))
}

func BenchmarkParquetProjectedNestedColumn(b *testing.B) {
	for _, unprojectedColumns := range []int{0, 8, 32, 64} {
		b.Run(fmt.Sprintf("unprojected=%d", unprojectedColumns), func(b *testing.B) {
			data := writeNestedProjectionParquet(b, 2048, unprojectedColumns)
			param := nestedProjectionParam(data)
			proc := testutil.NewProc(b)

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				r := NewParquetReader(param, proc)
				fileEmpty, err := r.Open(param, proc)
				if err != nil {
					b.Fatal(err)
				}
				if fileEmpty {
					b.Fatal("projected parquet benchmark file is empty")
				}

				bat := vectorBatch([]types.Type{types.T_text.ToType()})
				finished, err := r.ReadBatch(context.Background(), bat, proc, nil)
				if err != nil {
					b.Fatal(err)
				}
				if !finished || bat.RowCount() != 2048 {
					b.Fatalf("unexpected scan result: finished=%v rows=%d", finished, bat.RowCount())
				}
				if err := r.Close(); err != nil {
					b.Fatal(err)
				}
				bat.Clean(proc.Mp())
			}
		})
	}
}

func TestParquetWideMixedNestedProjectionUsesHybrid(t *testing.T) {
	data := writeWideMixedNestedParquet(t, 8)
	param := wideMixedNestedParam(data)
	counter := &parquetBenchmarkReaderAt{reader: bytes.NewReader(data)}
	h := newWideMixedNestedHandler(t, param, counter)
	defer func() {
		h.cleanup()
		_ = h.closePages(param.Ctx)
	}()

	// Both vector columns must be reconstructed by the projected row reader;
	// all 29 scalar siblings must remain on the page-vectorized path.
	require.True(t, h.hasNestedCols)
	require.Len(t, h.nestedColIndices, 2)
	require.Len(t, h.dataColIndices, 29)
	require.Len(t, h.rowReader.Schema().Columns(), 2)
}

// BenchmarkParquetWideMixedNestedProjection exercises the path involved in
// #29229: two real Parquet LIST-to-vector columns together with a wide set of
// scalar siblings. The row mode sub-benchmark reconstructs the pre-hybrid
// behavior, while hybrid keeps the scalar leaves on page mappers.
//
// The benchmark deliberately uses a ReaderAt wrapper instead of INLINE file
// reads so it also reports the number and total size of Parquet range reads.
// Run with -benchtime=1x when comparing a single workload sample, for example:
//
//	go test ./pkg/sql/colexec/external -run '^$' -bench WideMixedNested -benchmem -benchtime=1x
func BenchmarkParquetWideMixedNestedProjection(b *testing.B) {
	data := writeWideMixedNestedParquet(b, 4096)
	for _, mode := range []string{"hybrid", "row_mode"} {
		b.Run(mode, func(b *testing.B) {
			var (
				rowsTotal      int64
				rowGroupsTotal int64
				nestedCols     int64
				pageCols       int64
				rowModeNanos   int64
				pageMapNanos   int64
				readPageNanos  int64
				peakBatchBytes int64
				peakCacheBytes int64
				rangeCalls     int64
				fetchedBytes   int64
			)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				counter := &parquetBenchmarkReaderAt{reader: bytes.NewReader(data)}
				reader := &parquetRangeReadAheadReaderAt{
					reader:   counter,
					fileSize: int64(len(data)),
				}
				param := wideMixedNestedParam(data)
				proc := testutil.NewProc(b)
				h := newWideMixedNestedHandler(b, param, reader)
				if mode == "row_mode" {
					if h.rowReader != nil {
						_ = h.rowReader.Close()
					}
					h.rowReader = h.rowGroup.Rows()
					h.dataColIndices = nil
				}
				rowGroupsTotal += int64(len(h.rowGroups))
				nestedCols += int64(len(h.nestedColIndices))
				pageCols += int64(len(h.dataColIndices))

				for !h.isFinished() {
					bat := wideMixedNestedBatch()
					h.batchCnt = 1024
					if err := h.getData(bat, param, proc); err != nil {
						bat.Clean(proc.Mp())
						b.Fatal(err)
					}
					rowsTotal += int64(bat.RowCount())
					peakBatchBytes = max(peakBatchBytes, int64(bat.Size()))
					bat.Clean(proc.Mp())
				}
				stats := param.takeParquetProfile()
				rowModeNanos += stats.RowModeTime
				pageMapNanos += stats.MapTime
				readPageNanos += stats.ReadPageTime
				rangeCalls += counter.calls.Load()
				fetchedBytes += counter.bytes.Load()
				peakCacheBytes = max(peakCacheBytes, int64(cap(reader.window)))
				_ = h.closePages(param.Ctx)
				proc.Free()
			}
			b.StopTimer()
			b.ReportMetric(float64(rowsTotal)/float64(b.N), "rows/op")
			b.ReportMetric(float64(rowGroupsTotal)/float64(b.N), "row_groups/op")
			b.ReportMetric(float64(nestedCols)/float64(b.N), "nested_columns/op")
			b.ReportMetric(float64(pageCols)/float64(b.N), "page_columns/op")
			b.ReportMetric(float64(rowModeNanos)/float64(b.N), "row_mode_ns/op")
			b.ReportMetric(float64(pageMapNanos)/float64(b.N), "page_map_ns/op")
			b.ReportMetric(float64(readPageNanos)/float64(b.N), "read_page_ns/op")
			b.ReportMetric(float64(rangeCalls)/float64(b.N), "range_calls/op")
			b.ReportMetric(float64(fetchedBytes)/float64(b.N), "fetched_bytes/op")
			b.ReportMetric(float64(peakBatchBytes), "peak_batch_bytes")
			b.ReportMetric(float64(peakCacheBytes), "peak_cache_bytes")
		})
	}
}

type parquetBenchmarkReaderAt struct {
	reader *bytes.Reader
	calls  atomic.Int64
	bytes  atomic.Int64
}

func (r *parquetBenchmarkReaderAt) ReadAt(p []byte, off int64) (int, error) {
	r.calls.Add(1)
	r.bytes.Add(int64(len(p)))
	return r.reader.ReadAt(p, off)
}

func newWideMixedNestedHandler(b testing.TB, param *ExternalParam, reader io.ReaderAt) *ParquetHandler {
	b.Helper()
	fileSize := int64(len(param.Extern.Data))
	file, err := parquet.OpenFile(reader, fileSize)
	if err != nil {
		b.Fatal(err)
	}
	h := &ParquetHandler{
		file:                           file,
		rowGroups:                      file.RowGroups(),
		filepathColIndex:               -1,
		icebergDMLDataFilePathColIndex: -1,
		icebergDMLRowOrdinalColIndex:   -1,
	}
	h.rowGroup = parquet.MultiRowGroup(h.rowGroups...)
	h.rowGroupRows = h.rowGroup.NumRows()
	if err := h.prepare(param); err != nil {
		b.Fatal(err)
	}
	return h
}

func wideMixedNestedParam(data []byte) *ExternalParam {
	columns := wideMixedNestedColumns()
	attrs := make([]plan.ExternAttr, len(columns))
	defs := make([]*plan.ColDef, len(columns))
	for i, column := range columns {
		attrs[i] = plan.ExternAttr{ColName: column.name, ColIndex: int32(i)}
		defs[i] = &plan.ColDef{
			Name:    column.name,
			Typ:     plan.Type{Id: int32(column.target), Width: column.width, NotNullable: true},
			NotNull: true,
		}
	}
	return &ExternalParam{
		ExParamConst: ExParamConst{
			Ctx:      context.Background(),
			Attrs:    attrs,
			Cols:     defs,
			Extern:   &tree.ExternParam{ExParamConst: tree.ExParamConst{ScanType: tree.INLINE, Format: tree.PARQUET, Data: string(data)}},
			FileSize: []int64{int64(len(data))},
		},
		ExParam: ExParam{Fileparam: &ExFileparam{FileIndex: 1, FileCnt: 1}},
	}
}

type wideMixedNestedColumn struct {
	name   string
	node   parquet.Node
	target types.T
	width  int32
}

func wideMixedNestedColumns() []wideMixedNestedColumn {
	columns := []wideMixedNestedColumn{{
		name: "emb32", node: parquet.List(parquet.Leaf(parquet.DoubleType)),
		target: types.T_array_float32, width: 3,
	}, {
		name: "emb64", node: parquet.List(parquet.Leaf(parquet.DoubleType)),
		target: types.T_array_float64, width: 3,
	}}
	for i := 0; i < 20; i++ {
		columns = append(columns, wideMixedNestedColumn{
			name: fmt.Sprintf("i%02d", i), node: parquet.Leaf(parquet.Int64Type), target: types.T_int64,
		})
	}
	for i := 0; i < 5; i++ {
		columns = append(columns, wideMixedNestedColumn{
			name: fmt.Sprintf("s%02d", i), node: parquet.String(), target: types.T_int32,
		})
	}
	for i := 0; i < 4; i++ {
		columns = append(columns, wideMixedNestedColumn{
			name: fmt.Sprintf("b%02d", i), node: parquet.Leaf(parquet.BooleanType), target: types.T_float64,
		})
	}
	return columns
}

func writeWideMixedNestedParquet(tb testing.TB, rowCount int) []byte {
	tb.Helper()
	columns := wideMixedNestedColumns()
	group := make(parquet.Group, len(columns))
	byName := make(map[string]wideMixedNestedColumn, len(columns))
	for _, column := range columns {
		group[column.name] = column.node
		byName[column.name] = column
	}
	schema := parquet.NewSchema("wide-mixed", group)
	rows := make([]parquet.Row, rowCount)
	for rowIndex := range rows {
		row := make(parquet.Row, 0, len(schema.Columns())+2)
		for columnIndex, path := range schema.Columns() {
			column := byName[path[0]]
			switch {
			case strings.HasPrefix(column.name, "emb"):
				row = append(row,
					parquet.DoubleValue(float64(rowIndex)).Level(0, 1, columnIndex),
					parquet.DoubleValue(float64(rowIndex+1)).Level(1, 1, columnIndex),
					parquet.DoubleValue(float64(rowIndex+2)).Level(1, 1, columnIndex),
				)
			case column.name[0] == 'i':
				row = append(row, parquet.Int64Value(int64(rowIndex)).Level(0, 0, columnIndex))
			case column.name[0] == 's':
				row = append(row, parquet.ByteArrayValue([]byte(fmt.Sprint(rowIndex))).Level(0, 0, columnIndex))
			default:
				row = append(row, parquet.BooleanValue(rowIndex%2 == 0).Level(0, 0, columnIndex))
			}
		}
		rows[rowIndex] = row
	}
	var buf bytes.Buffer
	writer := parquet.NewWriter(&buf, schema, parquet.MaxRowsPerRowGroup(512))
	if _, err := writer.WriteRows(rows); err != nil {
		tb.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		tb.Fatal(err)
	}
	return buf.Bytes()
}

func wideMixedNestedBatch() *batch.Batch {
	columns := wideMixedNestedColumns()
	typesList := make([]types.Type, len(columns))
	for i, column := range columns {
		typesList[i] = types.New(column.target, column.width, 0)
	}
	return vectorBatch(typesList)
}

func writeNestedProjectionParquet(tb testing.TB, rowCount, unprojectedColumns int) []byte {
	tb.Helper()

	group := make(parquet.Group, unprojectedColumns+1)
	for i := 0; i < unprojectedColumns; i++ {
		group[fmt.Sprintf("a_unused_%03d", i)] = parquet.Leaf(parquet.Int32Type)
	}
	group["z_nested"] = parquet.Group{"v": parquet.Leaf(parquet.Int32Type)}
	schema := parquet.NewSchema("projection", group)

	rows := make([]parquet.Row, rowCount)
	for rowIndex := range rows {
		row := make(parquet.Row, 0, unprojectedColumns+1)
		for columnIndex := 0; columnIndex < unprojectedColumns; columnIndex++ {
			row = append(row, parquet.Int32Value(int32(columnIndex)).Level(0, 0, columnIndex))
		}
		row = append(row, parquet.Int32Value(int32(rowIndex)).Level(0, 0, unprojectedColumns))
		rows[rowIndex] = row
	}

	var buf bytes.Buffer
	w := parquet.NewWriter(&buf, schema)
	_, err := w.WriteRows(rows)
	require.NoError(tb, err)
	require.NoError(tb, w.Close())
	return buf.Bytes()
}

func nestedProjectionParam(data []byte) *ExternalParam {
	return &ExternalParam{
		ExParamConst: ExParamConst{
			Ctx:      context.Background(),
			Attrs:    []plan.ExternAttr{{ColName: "z_nested", ColIndex: 0}},
			Cols:     []*plan.ColDef{{Typ: plan.Type{Id: int32(types.T_text)}}},
			Extern:   &tree.ExternParam{ExParamConst: tree.ExParamConst{ScanType: tree.INLINE, Format: tree.PARQUET, Data: string(data)}},
			FileSize: []int64{int64(len(data))},
		},
		ExParam: ExParam{Fileparam: &ExFileparam{FileIndex: 1, FileCnt: 1}},
	}
}

func nestedAndScalarProjectionParam(data []byte) *ExternalParam {
	return &ExternalParam{
		ExParamConst: ExParamConst{
			Ctx: context.Background(),
			Attrs: []plan.ExternAttr{
				{ColName: "a_unused_000", ColIndex: 0},
				{ColName: "z_nested", ColIndex: 1},
			},
			Cols: []*plan.ColDef{
				{Typ: plan.Type{Id: int32(types.T_int32), NotNullable: true}},
				{Typ: plan.Type{Id: int32(types.T_text)}},
			},
			Extern:   &tree.ExternParam{ExParamConst: tree.ExParamConst{ScanType: tree.INLINE, Format: tree.PARQUET, Data: string(data)}},
			FileSize: []int64{int64(len(data))},
		},
		ExParam: ExParam{Fileparam: &ExFileparam{FileIndex: 1, FileCnt: 1}},
	}
}
