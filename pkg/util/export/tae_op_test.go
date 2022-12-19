package export

import (
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	"github.com/stretchr/testify/require"
	"path"
	"testing"
	"time"
)

// var dummyStrColumn = table.Column{Name: "str", Type: "varchar(32)", ColType: types.T_varchar.ToType(), Default: "", Comment: "str column"}
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
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.Nil(t, err)
	ctx := context.TODO()
	filepath := path.Join(t.TempDir(), "file.tae")
	configs := []fileservice.Config{{
		Name:    defines.ETLFileServiceName,
		Backend: "DISK",
		DataDir: t.TempDir(),
	},
	}
	var services []fileservice.FileService
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

	lines := genLines()
	for _, line := range lines {
		err = writer.WriteElems(line)
		require.Nil(t, err)
	}
	// Done. write
}

func genLines() (lines [][]any) {

	for i := 0; i < 1024; i++ {
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
