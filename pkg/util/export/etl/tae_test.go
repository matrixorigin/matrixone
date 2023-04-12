// Copyright 2022 Matrix Origin
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

package etl

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
	"github.com/stretchr/testify/require"
)

var dummyStrColumn = table.Column{Name: "str", ColType: table.TVarchar, Scale: 32, Default: "", Comment: "str column"}
var dummyInt64Column = table.Column{Name: "int64", ColType: table.TInt64, Default: "0", Comment: "int64 column"}
var dummyFloat64Column = table.Column{Name: "float64", ColType: table.TFloat64, Default: "0.0", Comment: "float64 column"}
var dummyUInt64Column = table.Column{Name: "uint64", ColType: table.TUint64, Default: "0", Comment: "uint64 column"}
var dummyDatetimeColumn = table.Column{Name: "datetime_6", ColType: table.TDatetime, Default: "", Comment: "datetime.6 column"}
var dummyJsonColumn = table.Column{Name: "json_col", ColType: table.TJson, Default: "{}", Comment: "json column"}

var dummyAllTypeTable = &table.Table{
	Account:          "test",
	Database:         "db_dummy",
	Table:            "tbl_all_type_dummy",
	Columns:          []table.Column{dummyStrColumn, dummyInt64Column, dummyFloat64Column, dummyUInt64Column, dummyDatetimeColumn, dummyJsonColumn},
	PrimaryKeyColumn: []table.Column{dummyStrColumn, dummyInt64Column},
	Engine:           table.ExternalTableEngine,
	Comment:          "dummy table",
	PathBuilder:      table.NewAccountDatePathBuilder(),
	TableOptions:     nil,
}

func TestTAEWriter_WriteElems(t *testing.T) {
	t.Logf("local timezone: %v", time.Local.String())
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.Nil(t, err)
	ctx := context.TODO()
	fs := testutil.NewFS()

	filepath := path.Join(t.TempDir(), "file.tae")
	writer := NewTAEWriter(ctx, dummyAllTypeTable, mp, filepath, fs)

	cnt := 10240
	lines := genLines(cnt)
	for _, row := range lines {
		err = writer.WriteRow(row)
		require.Nil(t, err)
	}
	_, err = writer.FlushAndClose()
	require.Nil(t, err)
	for _, row := range lines {
		row.Free()
	}
	// Done. write

	folder := path.Dir(filepath)
	files, err := fs.List(ctx, folder)
	require.Nil(t, err)
	require.Equal(t, 1, len(files))

	file := files[0]
	t.Logf("path: %s, size: %d", file.Name, file.Size)

	// ----- reader ------

	r, err := NewTaeReader(context.TODO(), dummyAllTypeTable, filepath, file.Size, fs, mp)
	require.Nil(t, err)

	// read data
	batchs, err := r.ReadAll(ctx)
	require.Nil(t, err)
	require.Equal(t, (cnt+BatchSize)/BatchSize, len(batchs))

	// read index
	for _, bbs := range r.bs {
		_, err = r.blockReader.LoadZoneMaps(context.Background(),
			r.idxs, bbs.GetExtent().Id(), mp)
		require.Nil(t, err)
	}

	readCnt := 0
	for batIDX, bat := range batchs {
		for _, vec := range bat.Vecs {
			rows, err := GetVectorArrayLen(context.TODO(), vec)
			require.Nil(t, err)
			t.Logf("calculate length: %d, vec.Length: %d, type: %s", rows, vec.Length(), vec.GetType().String())
		}
		rows := bat.Vecs[0].Length()
		ctn := strings.Builder{}
		for rowId := 0; rowId < rows; rowId++ {
			for _, vec := range bat.Vecs {
				val, err := ValToString(context.TODO(), vec, rowId)
				require.Nil(t, err)
				ctn.WriteString(val)
				ctn.WriteString(",")
			}
			ctn.WriteRune('\n')
		}
		//t.Logf("batch %d: \n%s", batIDX, ctn.String())
		t.Logf("read batch %d", batIDX)
		readCnt += rows
	}
	require.Equal(t, cnt, readCnt)
}

func genLines(cnt int) (lines []*table.Row) {
	lines = make([]*table.Row, 0, cnt)
	r := dummyAllTypeTable.GetRow(context.TODO())
	defer r.Free()
	for i := 0; i < cnt; i++ {
		row := r.Clone()
		row.SetColumnVal(dummyStrColumn, fmt.Sprintf("str_val_%d", i))
		row.SetColumnVal(dummyInt64Column, int64(i))
		row.SetColumnVal(dummyFloat64Column, float64(i))
		row.SetColumnVal(dummyUInt64Column, uint64(i))
		row.SetColumnVal(dummyDatetimeColumn, time.Now())
		row.SetColumnVal(dummyJsonColumn, fmt.Sprintf(`{"cnt":"%d"}`, i))
		lines = append(lines, row)
	}

	return
}

func TestTAEWriter_WriteRow(t *testing.T) {
	t.Logf("local timezone: %v", time.Local.String())
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.Nil(t, err)
	ctx := context.TODO()
	fs := testutil.NewFS()

	type fields struct {
		ctx context.Context
		fs  fileservice.FileService
	}
	type args struct {
		tbl   *table.Table
		items func() []table.RowField
	}

	var genSpanData = func() []table.RowField {
		arr := make([]table.RowField, 0, 128)
		arr = append(arr, &motrace.MOSpan{
			SpanConfig: trace.SpanConfig{SpanContext: trace.SpanContext{
				TraceID: trace.NilTraceID,
				SpanID:  trace.NilSpanID,
				Kind:    trace.SpanKindInternal,
			}},
			Name:      "span1",
			StartTime: time.Time{},
			EndTime:   time.Time{},
			Duration:  0,
		})
		arr = append(arr, &motrace.MOSpan{
			SpanConfig: trace.SpanConfig{SpanContext: trace.SpanContext{
				TraceID: trace.NilTraceID,
				SpanID:  trace.NilSpanID,
				Kind:    trace.SpanKindStatement,
			}},
			Name:      "span2",
			StartTime: time.Time{},
			EndTime:   time.Time{},
			Duration:  100,
		})

		return arr
	}

	var _1TxnID = [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x1}
	var _1SesID = [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x1}
	var genStmtData = func() []table.RowField {
		arr := make([]table.RowField, 0, 128)
		arr = append(arr,
			&motrace.StatementInfo{
				StatementID:          trace.NilTraceID,
				TransactionID:        _1TxnID,
				SessionID:            _1SesID,
				Account:              "MO",
				User:                 "moroot",
				Database:             "system",
				Statement:            "show tables",
				StatementFingerprint: "show tables",
				StatementTag:         "",
				ExecPlan:             nil,
			},
		)
		return arr
	}

	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "statement",
			fields: fields{
				ctx: ctx,
				fs:  fs,
			},
			args: args{
				tbl:   motrace.SingleStatementTable,
				items: genStmtData,
			},
		},
		{
			name: "span",
			fields: fields{
				ctx: ctx,
				fs:  fs,
			},
			args: args{
				tbl:   motrace.SingleRowLogTable,
				items: genSpanData,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			if tt.name == "span" {
				return
			}

			cfg := table.FilePathCfg{NodeUUID: "uuid", NodeType: "type", Extension: table.TaeExtension}
			filePath := cfg.LogsFilePathFactory("sys", tt.args.tbl, time.Now())
			writer := NewTAEWriter(tt.fields.ctx, tt.args.tbl, mp, filePath, tt.fields.fs)
			items := tt.args.items()
			for _, item := range items {
				row := item.GetTable().GetRow(tt.fields.ctx)
				item.FillRow(tt.fields.ctx, row)
				writer.WriteRow(row)
			}
			writer.FlushAndClose()

			folder := path.Dir(filePath)
			entrys, err := fs.List(ctx, folder)
			require.Nil(t, err)
			require.NotEqual(t, 0, len(entrys))
			for _, e := range entrys {
				t.Logf("file: %s, size: %d, is_dir: %v", e.Name, e.Size, e.IsDir)
				require.NotEqual(t, 44, e.Size)
			}
		})
	}
}

func TestTaeReadFile(t *testing.T) {
	filePath := "rawlog.tae"

	mp, err := mpool.NewMPool("TestTaeReadFile", 0, mpool.NoFixed)
	require.Nil(t, err)
	ctx := context.TODO()
	fs := testutil.NewFS()

	entrys, err := fs.List(context.TODO(), "etl:/")
	require.Nil(t, err)
	if len(entrys) == 0 {
		t.Skip()
	}
	require.Equal(t, 1, len(entrys))
	require.Equal(t, filePath, entrys[0].Name)

	fileSize := entrys[0].Size

	r, err := NewTaeReader(context.TODO(), motrace.SingleRowLogTable, filePath, fileSize, fs, mp)
	require.Nil(t, err)

	// read data
	batchs, err := r.ReadAll(ctx)
	require.Nil(t, err)

	// read index
	for _, bbs := range r.bs {
		_, err = r.blockReader.LoadZoneMaps(context.Background(),
			r.idxs, bbs.GetExtent().Id(), mp)
		require.Nil(t, err)
	}

	readCnt := 0
	for batIDX, bat := range batchs {
		for _, vec := range bat.Vecs {
			rows, err := GetVectorArrayLen(context.TODO(), vec)
			require.Nil(t, err)
			t.Logf("calculate length: %d, vec.Length: %d, type: %s", rows, vec.Length(), vec.GetType().String())
		}
		rows := bat.Vecs[0].Length()
		ctn := strings.Builder{}
		for rowId := 0; rowId < rows; rowId++ {
			for _, vec := range bat.Vecs {
				val, err := ValToString(context.TODO(), vec, rowId)
				require.Nil(t, err)
				ctn.WriteString(val)
				ctn.WriteString(",")
			}
			ctn.WriteRune('\n')
		}
		t.Logf("batch %d: \n%s", batIDX, ctn.String())
		//t.Logf("read batch %d", batIDX)
		readCnt += rows
	}
}

func TestTaeReadFile_ReadAll(t *testing.T) {

	mp, err := mpool.NewMPool("TestTaeReadFile", 0, mpool.NoFixed)
	require.Nil(t, err)
	ctx := context.TODO()
	fs := testutil.NewFS()

	folder := "/sys/logs/2023/01/11/rawlog"
	entrys, err := fs.List(context.TODO(), "etl:"+folder)
	require.Nil(t, err)
	if len(entrys) == 0 {
		t.Skip()
	}

	itemsCnt := make(map[string]int, 2)
	itemsCnt["span_info"] = 0
	itemsCnt["log_info"] = 0
	readCnt := 0
	for _, e := range entrys {
		t.Logf("file: %s, size: %d", e.Name, e.Size)

		r, err := NewTaeReader(context.TODO(), motrace.SingleRowLogTable, path.Join(folder, e.Name), e.Size, fs, mp)
		require.Nil(t, err)

		// read data
		batchs, err := r.ReadAll(ctx)
		require.Nil(t, err)

		// read index
		for _, bbs := range r.bs {
			_, err = r.blockReader.LoadZoneMaps(context.Background(),
				r.idxs, bbs.GetExtent().Id(), mp)
			require.Nil(t, err)
		}

		for batIDX, bat := range batchs {
			for _, vec := range bat.Vecs {
				rows, err := GetVectorArrayLen(context.TODO(), vec)
				require.Nil(t, err)
				t.Logf("calculate length: %d", rows)
				break
				//t.Logf("calculate length: %d, vec.Length: %d, type: %s", rows, vec.Length(), vec.GetType().String())
			}
			rows := bat.Vecs[0].Length()
			ctn := strings.Builder{}
			for rowId := 0; rowId < rows; rowId++ {
				for idx, vec := range bat.Vecs {
					val, err := ValToString(context.TODO(), vec, rowId)
					require.Nil(t, err)
					ctn.WriteString(val)
					ctn.WriteString(",")
					if idx == 0 {
						itemsCnt[val]++
					}
				}
				ctn.WriteRune('\n')
			}
			//t.Logf("batch %d: \n%s", batIDX, ctn.String())
			t.Logf("read batch %d", batIDX)
			readCnt += rows
		}
		t.Logf("cnt: %v", itemsCnt)
	}
	t.Logf("cnt: %v", itemsCnt)
}

func TestTAEWriter_writeEmpty(t *testing.T) {
	cfg := table.FilePathCfg{NodeUUID: "uuid", NodeType: "type", Extension: table.TaeExtension}
	ctx := context.TODO()
	tbl := motrace.SingleStatementTable
	fs := testutil.NewFS()
	filePath := cfg.LogsFilePathFactory("sys", tbl, time.Now())
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.Nil(t, err)
	writer := NewTAEWriter(ctx, tbl, mp, filePath, fs)
	_, err = writer.FlushAndClose()
	require.NotNil(t, err)
	var e *moerr.Error
	require.True(t, errors.As(err, &e))
	require.Equal(t, moerr.ErrEmptyRange, e.ErrorCode())
}
