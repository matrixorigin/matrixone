// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"bufio"
	"errors"
	"os"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/prashantv/gostub"
	"github.com/smartystreets/goconvey/convey"
)

var colName1, colName2 = "DATABASE()", "VARIABLE_VALUE"

func Test_initExportFileParam(t *testing.T) {
	var oq = &outputQueue{
		mrs: &MysqlResultSet{},
		ep: &tree.ExportParam{
			Lines:  &tree.Lines{},
			Fields: &tree.Fields{},
		},
	}
	initExportFileParam(oq.ep, oq.mrs)

	col1 := new(MysqlColumn)
	col1.SetName(colName1)
	col2 := new(MysqlColumn)
	col2.SetName(colName2)
	oq.mrs.AddColumn(col1)
	oq.mrs.AddColumn(col2)

	oq.ep.ForceQuote = append(oq.ep.ForceQuote, colName1)
	oq.mrs.Name2Index[colName1] = 0
	initExportFileParam(oq.ep, oq.mrs)
}

func Test_openNewFile(t *testing.T) {
	convey.Convey("openNewFile failed", t, func() {
		var oq = &outputQueue{
			mrs: &MysqlResultSet{},
			ep: &tree.ExportParam{
				Lines:    &tree.Lines{},
				Fields:   &tree.Fields{},
				Header:   true,
				FilePath: "test/export.csv",
			},
		}
		stubs := gostub.StubFunc(&OpenFile, nil, errors.New("can not open file"))
		defer stubs.Reset()
		convey.So(openNewFile(oq.ep, oq.mrs), convey.ShouldNotBeNil)
	})

	convey.Convey("openNewFile succ", t, func() {
		var oq = &outputQueue{
			mrs: &MysqlResultSet{},
			ep: &tree.ExportParam{
				Lines:    &tree.Lines{},
				Fields:   &tree.Fields{},
				Header:   true,
				FilePath: "test/export.csv",
				LineSize: 1,
			},
		}
		col1 := new(MysqlColumn)
		col1.SetName(colName1)
		col2 := new(MysqlColumn)
		col2.SetName(colName2)
		oq.mrs.AddColumn(col1)
		oq.mrs.AddColumn(col2)

		var file = &os.File{}
		stubs := gostub.StubFunc(&OpenFile, file, nil)
		defer stubs.Reset()

		stubs = gostub.StubFunc(&writeDataToCSVFile, nil)
		defer stubs.Reset()

		convey.So(openNewFile(oq.ep, oq.mrs), convey.ShouldBeNil)
	})
}

func Test_formatOutputString(t *testing.T) {
	convey.Convey("openNewFile failed", t, func() {
		var oq = &outputQueue{
			mrs: &MysqlResultSet{},
			ep: &tree.ExportParam{
				Lines:    &tree.Lines{},
				Fields:   &tree.Fields{},
				Header:   true,
				FilePath: "test/export.csv",
				LineSize: 1,
			},
		}
		stubs := gostub.StubFunc(&writeDataToCSVFile, errors.New("write err"))
		defer stubs.Reset()
		convey.So(formatOutputString(oq, nil, nil, '\n', true), convey.ShouldNotBeNil)

		stubs = gostub.StubFunc(&writeDataToCSVFile, nil)
		defer stubs.Reset()
		convey.So(formatOutputString(oq, nil, nil, '\n', true), convey.ShouldBeNil)
	})
}

func Test_writeToCSVFile(t *testing.T) {
	convey.Convey("writeToCSVFile case", t, func() {
		var oq = &outputQueue{
			mrs: &MysqlResultSet{},
			ep: &tree.ExportParam{
				Lines:    &tree.Lines{},
				Fields:   &tree.Fields{},
				Header:   true,
				FilePath: "test/export.csv",
				LineSize: 1,
				Writer:   &bufio.Writer{},
			},
		}
		var output = []byte{'1', '2'}
		oq.ep.MaxFileSize = 1

		convey.So(writeToCSVFile(oq, output), convey.ShouldNotBeNil)

		oq.ep.Rows = 1
		stubs := gostub.StubFunc(&Flush, errors.New("Flush error"))
		defer stubs.Reset()

		convey.So(writeToCSVFile(oq, output), convey.ShouldNotBeNil)

		stubs = gostub.StubFunc(&Flush, nil)
		defer stubs.Reset()

		stubs = gostub.StubFunc(&Seek, int64(0), errors.New("Seek error"))
		defer stubs.Reset()
		convey.So(writeToCSVFile(oq, output), convey.ShouldNotBeNil)

		stubs = gostub.StubFunc(&Seek, int64(0), nil)
		defer stubs.Reset()
		stubs = gostub.StubFunc(&Read, 0, errors.New("Read error"))
		defer stubs.Reset()
		convey.So(writeToCSVFile(oq, output), convey.ShouldNotBeNil)

		stubs = gostub.StubFunc(&Read, 1, nil)
		defer stubs.Reset()

		stubs = gostub.StubFunc(&Truncate, errors.New("Truncate error"))
		defer stubs.Reset()
		convey.So(writeToCSVFile(oq, output), convey.ShouldNotBeNil)

		stubs = gostub.StubFunc(&Truncate, nil)
		defer stubs.Reset()
		stubs = gostub.StubFunc(&Close, errors.New("Close error"))
		defer stubs.Reset()
		convey.So(writeToCSVFile(oq, output), convey.ShouldNotBeNil)

		stubs = gostub.StubFunc(&Close, nil)
		defer stubs.Reset()
		stubs = gostub.StubFunc(&openNewFile, errors.New("openNewFile error"))
		defer stubs.Reset()
		convey.So(writeToCSVFile(oq, output), convey.ShouldNotBeNil)

		stubs = gostub.StubFunc(&openNewFile, nil)
		defer stubs.Reset()
		stubs = gostub.StubFunc(&writeDataToCSVFile, errors.New("writeDataToCSVFile error"))
		defer stubs.Reset()
		convey.So(writeToCSVFile(oq, output), convey.ShouldNotBeNil)

		stubs = gostub.StubFunc(&writeDataToCSVFile, nil)
		defer stubs.Reset()
		convey.So(writeToCSVFile(oq, output), convey.ShouldBeNil)
	})
}

func Test_writeDataToCSVFile(t *testing.T) {
	convey.Convey("writeDataToCSVFile case", t, func() {
		var oq = &outputQueue{
			mrs: &MysqlResultSet{},
			ep: &tree.ExportParam{
				Lines:    &tree.Lines{},
				Fields:   &tree.Fields{},
				Header:   true,
				FilePath: "test/export.csv",
				LineSize: 1,
				Writer:   &bufio.Writer{},
			},
		}
		var output = []byte{'1', '2'}
		stubs := gostub.StubFunc(&Write, 0, errors.New("writeDataToCSVFile error"))
		defer stubs.Reset()

		convey.So(writeDataToCSVFile(oq.ep, output), convey.ShouldNotBeNil)

		stubs = gostub.StubFunc(&Write, len(output), nil)
		defer stubs.Reset()
		convey.So(writeDataToCSVFile(oq.ep, output), convey.ShouldBeNil)

	})
}

func Test_exportDataToCSVFile(t *testing.T) {
	convey.Convey("exportDataToCSVFile succ", t, func() {
		var oq = &outputQueue{
			mrs: &MysqlResultSet{},
			ep: &tree.ExportParam{
				Lines:    &tree.Lines{},
				Fields:   &tree.Fields{},
				Header:   true,
				FilePath: "test/export.csv",
				LineSize: 1,
				Writer:   &bufio.Writer{},
			},
		}

		var col = make([]MysqlColumn, 12)
		col[5].flag = 0
		col[6].flag = 1 << 5
		var colType = []uint8{defines.MYSQL_TYPE_YEAR, defines.MYSQL_TYPE_YEAR, defines.MYSQL_TYPE_YEAR, defines.MYSQL_TYPE_SHORT, defines.MYSQL_TYPE_DOUBLE,
			defines.MYSQL_TYPE_LONGLONG, defines.MYSQL_TYPE_LONGLONG, defines.MYSQL_TYPE_VARCHAR, defines.MYSQL_TYPE_DATE, defines.MYSQL_TYPE_DATETIME,
			defines.MYSQL_TYPE_BOOL, defines.MYSQL_TYPE_DECIMAL}
		for i := 0; i < len(col); i++ {
			col[i].SetColumnType(colType[i])
			oq.mrs.AddColumn(&col[i])
		}
		var data = make([]interface{}, len(col))
		data[1] = 0
		data[2] = 1
		data[3] = 1
		data[4] = 1.0
		data[5] = 1
		data[6] = 1
		data[7] = []byte{1}
		data[8] = types.Date(1)
		data[9] = "2022-02-28 23:59:59.9999"
		data[10] = true
		data[11] = 1.2

		oq.mrs.AddRow(data)

		oq.ep.Symbol = make([][]byte, len(col))
		oq.ep.ColumnFlag = make([]bool, len(col))

		stubs := gostub.StubFunc(&formatOutputString, nil)
		defer stubs.Reset()

		convey.So(exportDataToCSVFile(oq), convey.ShouldBeNil)
	})

	convey.Convey("exportDataToCSVFile fail", t, func() {
		var oq = &outputQueue{
			mrs: &MysqlResultSet{},
			ep: &tree.ExportParam{
				Lines:    &tree.Lines{},
				Fields:   &tree.Fields{},
				Header:   true,
				FilePath: "test/export.csv",
				LineSize: 1,
				Writer:   &bufio.Writer{},
			},
		}
		var col = make([]MysqlColumn, 1)
		var colType = []uint8{defines.MYSQL_TYPE_TIMESTAMP}
		for i := 0; i < len(col); i++ {
			col[i].SetColumnType(colType[i])
			oq.mrs.AddColumn(&col[i])
		}

		var data = make([]interface{}, len(col))
		data[0] = 1
		oq.mrs.AddRow(data)
		oq.ep.Symbol = make([][]byte, len(col))
		oq.ep.ColumnFlag = make([]bool, len(col))

		stubs := gostub.StubFunc(&formatOutputString, nil)
		defer stubs.Reset()

		convey.So(exportDataToCSVFile(oq), convey.ShouldBeNil)
	})
}
