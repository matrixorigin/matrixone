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
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	"github.com/stretchr/testify/require"
	"path"
	"strings"
	"testing"
	"time"
)

var dummyStrColumn = table.Column{Name: "str", Type: "varchar(32)", ColType: table.TVarchar, Default: "", Comment: "str column"}
var dummyInt64Column = table.Column{Name: "int64", Type: "BIGINT", ColType: table.TInt64, Default: "0", Comment: "int64 column"}
var dummyFloat64Column = table.Column{Name: "float64", Type: "DOUBLE", ColType: table.TFloat64, Default: "0.0", Comment: "float64 column"}
var dummyUInt64Column = table.Column{Name: "int64", Type: "BIGINT UNSIGNED", ColType: table.TUint64, Default: "0", Comment: "uint64 column"}
var dummyDatetimeColumn = table.Column{Name: "datetime_6", Type: "datetime(6)", ColType: table.TDatetime, Default: "", Comment: "datetime.6 column"}
var dummyJsonColumn = table.Column{Name: "json_col", Type: "JSON", ColType: table.TJson, Default: "{}", Comment: "json column"}

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
	filepath := defines.ETLFileServiceName + ":" + path.Join(t.TempDir(), "file.tae")
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

	writer, err := NewTAEWriter(ctx, dummyAllTypeTable, mp, filepath, fs)
	require.Nil(t, err)

	cnt := 10240
	lines := genLines(cnt)
	for _, line := range lines {
		err = writer.WriteElems(line)
		require.Nil(t, err)
	}
	err = writer.Flush()
	require.Nil(t, err)
	// Done. write

	folder := path.Dir(filepath)
	files, err := fs.List(ctx, folder)
	require.Nil(t, err)
	require.Equal(t, 1, len(files))

	file := files[0]
	t.Logf("path: %s, size: %d", file.Name, file.Size)

	r, err := NewTaeReader(dummyAllTypeTable, filepath, file.Size, fs)
	require.Nil(t, err)
	batchs, err := r.ReadAll(ctx, mp)
	require.Nil(t, err)
	require.Equal(t, (cnt+BatchSize)/BatchSize, len(batchs))

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
	for i := 0; i < cnt; i++ {
		row := dummyAllTypeTable.GetRow(context.TODO())
		row.SetRawColumnVal(dummyStrColumn, fmt.Sprintf("str_val_%d", i))
		row.SetRawColumnVal(dummyInt64Column, int64(i))
		row.SetRawColumnVal(dummyFloat64Column, float64(i))
		row.SetRawColumnVal(dummyUInt64Column, uint64(i))
		row.SetRawColumnVal(dummyDatetimeColumn, time.Now())
		row.SetRawColumnVal(dummyJsonColumn, fmt.Sprintf(`{"cnt":"%d"}`, i))
		lines = append(lines, row.GetRawColumn())
	}

	return
}
