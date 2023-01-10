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

package writer

import (
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
	"github.com/stretchr/testify/require"
	"path"
	"strings"
	"testing"
	"time"
)

var dummyStrColumn = table.Column{Name: "str", ColType: table.TVarchar, Precision: 32, Default: "", Comment: "str column"}
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
	configs := []fileservice.Config{{
		Name:    defines.ETLFileServiceName,
		Backend: "DISK",
		DataDir: t.TempDir(),
	},
	}
	var services = make([]fileservice.FileService, 0, 1)
	for _, config := range configs {
		service, err := fileservice.NewFileService(config)
		require.Nil(t, err)
		services = append(services, service)
	}
	// create FileServices
	fs, err := fileservice.NewFileServices(
		defines.LocalFileServiceName,
		services...,
	)
	require.Nil(t, err)

	filepath := path.Join(t.TempDir(), "file.tae")
	writer := NewTAEWriter(ctx, dummyAllTypeTable, mp, filepath, fs)

	cnt := 10240
	lines := genLines(cnt)
	for _, line := range lines {
		err = writer.WriteElems(line)
		require.Nil(t, err)
	}
	_, err = writer.FlushAndClose()
	require.Nil(t, err)
	// Done. write

	folder := path.Dir(filepath)
	files, err := fs.List(ctx, defines.ETLFileServiceName+fileservice.ServiceNameSeparator+folder)
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
		_, err = r.objectReader.ReadIndex(context.Background(), bbs.GetExtent(),
			r.idxs, objectio.ZoneMapType, mp)
		require.Nil(t, err)
	}

	readCnt := 0
	for batIDX, bat := range batchs {
		for _, vec := range bat.Vecs {
			rows, err := GetVectorArrayLen(context.TODO(), vec)
			require.Nil(t, err)
			t.Logf("calculate length: %d, vec.Length: %d, type: %s", rows, vec.Length(), vec.Typ.String())
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

func genLines(cnt int) (lines [][]any) {
	lines = make([][]any, 0, cnt)
	row := dummyAllTypeTable.GetRow(context.TODO())
	defer row.Free()
	for i := 0; i < cnt; i++ {
		row.Reset()
		row.SetColumnVal(dummyStrColumn, fmt.Sprintf("str_val_%d", i))
		row.SetColumnVal(dummyInt64Column, int64(i))
		row.SetColumnVal(dummyFloat64Column, float64(i))
		row.SetColumnVal(dummyUInt64Column, uint64(i))
		row.SetColumnVal(dummyDatetimeColumn, time.Now())
		row.SetColumnVal(dummyJsonColumn, fmt.Sprintf(`{"cnt":"%d"}`, i))
		lines = append(lines, row.GetRawColumn())
	}

	return
}

func TestTAEWriter(t *testing.T) {
	t.Logf("local timezone: %v", time.Local.String())
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.Nil(t, err)
	ctx := context.TODO()
	configs := []fileservice.Config{{
		Name:    defines.ETLFileServiceName,
		Backend: "DISK-ETL",
		DataDir: t.TempDir(),
	},
	}
	var services = make([]fileservice.FileService, 0, 1)
	for _, config := range configs {
		service, err := fileservice.NewFileService(config)
		require.Nil(t, err)
		services = append(services, service)
	}
	// create FileServices
	fs, err := fileservice.NewFileServices(
		defines.LocalFileServiceName,
		services...,
	)
	require.Nil(t, err)

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

	tests := []struct {
		name   string
		fields fields
		args   args
	}{
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

			filepath := "NOT_SET"
			_ = NewTAEWriter(tt.fields.ctx, tt.args.tbl, mp, filepath, tt.fields.fs)

		})
	}
}
