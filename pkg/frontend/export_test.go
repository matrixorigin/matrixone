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
	"bytes"
	"context"
	"testing"

	"github.com/prashantv/gostub"
	"github.com/smartystreets/goconvey/convey"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

var colName1, colName2 = "DATABASE()", "VARIABLE_VALUE"

func Test_initExportFileParam(t *testing.T) {
	ep := &ExportConfig{
		userConfig: &tree.ExportParam{
			Lines: &tree.Lines{
				TerminatedBy: &tree.Terminated{},
			},
			Fields: &tree.Fields{
				Terminated: &tree.Terminated{},
				EnclosedBy: &tree.EnclosedBy{},
				EscapedBy:  &tree.EscapedBy{},
			},
		},
		mrs: &MysqlResultSet{},
	}

	initExportFileParam(ep, ep.mrs)

	col1 := new(MysqlColumn)
	col1.SetName(colName1)
	col2 := new(MysqlColumn)
	col2.SetName(colName2)
	ep.mrs.AddColumn(col1)
	ep.mrs.AddColumn(col2)

	ep.userConfig.ForceQuote = append(ep.userConfig.ForceQuote, colName1)
	initExportFileParam(ep, ep.mrs)
}

func Test_openNewFile(t *testing.T) {
	convey.Convey("openNewFile succ", t, func() {
		ep := &ExportConfig{
			userConfig: &tree.ExportParam{
				Lines: &tree.Lines{
					TerminatedBy: &tree.Terminated{},
				},
				Fields: &tree.Fields{
					Terminated: &tree.Terminated{},
					EnclosedBy: &tree.EnclosedBy{},
					EscapedBy:  &tree.EscapedBy{},
				},
				Header:   true,
				FilePath: "test/export.csv",
			},
			mrs: &MysqlResultSet{},
		}

		col1 := new(MysqlColumn)
		col1.SetName(colName1)
		col2 := new(MysqlColumn)
		col2.SetName(colName2)
		ep.mrs.AddColumn(col1)
		ep.mrs.AddColumn(col2)

		stubs := gostub.StubFunc(&writeDataToCSVFile, nil)
		defer stubs.Reset()

		convey.So(openNewFile(context.TODO(), ep, ep.mrs), convey.ShouldBeNil)
	})
}

func Test_formatOutputString(t *testing.T) {
	convey.Convey("openNewFile failed", t, func() {
		ep := &ExportConfig{
			userConfig: &tree.ExportParam{
				Lines: &tree.Lines{
					TerminatedBy: &tree.Terminated{},
				},
				Fields: &tree.Fields{
					Terminated: &tree.Terminated{},
					EnclosedBy: &tree.EnclosedBy{},
					EscapedBy:  &tree.EscapedBy{},
				},
				Header:   true,
				FilePath: "test/export.csv",
			},
			mrs: &MysqlResultSet{},
		}
		buffer := &bytes.Buffer{}
		convey.So(formatOutputString(ep, nil, nil, '\n', true, buffer), convey.ShouldBeNil)
	})
}

func Test_writeToCSVFile(t *testing.T) {
	convey.Convey("writeToCSVFile case", t, func() {
		ep := &ExportConfig{
			userConfig: &tree.ExportParam{
				Lines: &tree.Lines{
					TerminatedBy: &tree.Terminated{},
				},
				Fields: &tree.Fields{
					Terminated: &tree.Terminated{},
					EnclosedBy: &tree.EnclosedBy{},
					EscapedBy:  &tree.EscapedBy{},
				},
				Header:   true,
				FilePath: "test/export.csv",
			},
			mrs: &MysqlResultSet{},
		}

		var output = []byte{'1', '2'}
		ep.userConfig.MaxFileSize = 1

		stubs := gostub.StubFunc(&Close, moerr.NewInternalError(context.TODO(), "Close error"))
		defer stubs.Reset()
		convey.So(writeToCSVFile(ep, output), convey.ShouldNotBeNil)

		// set Close to nil and openNewFile to err
		stubs = gostub.StubFunc(&Close, nil)
		defer stubs.Reset()
		stubs = gostub.StubFunc(&openNewFile, moerr.NewInternalError(context.TODO(), "openNewFile error"))
		defer stubs.Reset()
		convey.So(writeToCSVFile(ep, output), convey.ShouldNotBeNil)

		// set Close() to nil
		stubs = gostub.StubFunc(&Close, nil)
		defer stubs.Reset()
		// set openNewFile() to nil
		stubs = gostub.StubFunc(&openNewFile, nil)
		defer stubs.Reset()
		// set writeDataToCSVFile to nil
		stubs = gostub.StubFunc(&writeDataToCSVFile, nil)
		defer stubs.Reset()
		convey.So(writeToCSVFile(ep, output), convey.ShouldBeNil)

	})
}

func Test_writeDataToCSVFile(t *testing.T) {
	convey.Convey("writeDataToCSVFile case", t, func() {
		ep := &ExportConfig{
			userConfig: &tree.ExportParam{
				Lines: &tree.Lines{
					TerminatedBy: &tree.Terminated{},
				},
				Fields: &tree.Fields{
					Terminated: &tree.Terminated{},
					EnclosedBy: &tree.EnclosedBy{},
					EscapedBy:  &tree.EscapedBy{},
				},
				Header:   true,
				FilePath: "test/export.csv",
			},
			mrs: &MysqlResultSet{},
		}

		var output = []byte{'1', '2'}
		stubs := gostub.StubFunc(&Write, 0, moerr.NewInternalError(context.TODO(), "writeDataToCSVFile error"))
		defer stubs.Reset()

		convey.So(writeDataToCSVFile(ep, output), convey.ShouldNotBeNil)

		stubs = gostub.StubFunc(&Write, len(output), nil)
		defer stubs.Reset()
		convey.So(writeDataToCSVFile(ep, output), convey.ShouldBeNil)

	})
}

func Test_exportDataToCSVFile(t *testing.T) {
	convey.Convey("exportDataFromResultSetToCSVFile succ", t, func() {
		ep := &ExportConfig{
			userConfig: &tree.ExportParam{
				Lines: &tree.Lines{
					TerminatedBy: &tree.Terminated{},
				},
				Fields: &tree.Fields{
					Terminated: &tree.Terminated{},
					EnclosedBy: &tree.EnclosedBy{},
					EscapedBy:  &tree.EscapedBy{},
				},
				Header:   true,
				FilePath: "test/export.csv",
			},
			mrs: &MysqlResultSet{},
		}

		var col = make([]MysqlColumn, 15)
		col[5].flag = 0
		col[6].flag = 1 << 5
		var colType = []defines.MysqlType{
			defines.MYSQL_TYPE_YEAR,
			defines.MYSQL_TYPE_YEAR,
			defines.MYSQL_TYPE_YEAR,
			defines.MYSQL_TYPE_SHORT,
			defines.MYSQL_TYPE_DOUBLE,
			defines.MYSQL_TYPE_LONGLONG,
			defines.MYSQL_TYPE_LONGLONG,
			defines.MYSQL_TYPE_VARCHAR,
			defines.MYSQL_TYPE_DATE,
			defines.MYSQL_TYPE_DATETIME,
			defines.MYSQL_TYPE_BOOL,
			defines.MYSQL_TYPE_DECIMAL,
			defines.MYSQL_TYPE_JSON,
			defines.MYSQL_TYPE_VARCHAR,
			defines.MYSQL_TYPE_VARCHAR,
		}
		for i := 0; i < len(col); i++ {
			col[i].SetColumnType(colType[i])
			ep.mrs.AddColumn(&col[i])
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
		data[12], _ = types.ParseStringToByteJson(`{"a":1}`)
		data[13] = []float32{8.560689, 6.790359, 821.9778}
		data[13] = []float64{8.560689, 6.790359, 821.9778}

		ep.mrs.AddRow(data)

		ep.Symbol = make([][]byte, len(col))
		ep.ColumnFlag = make([]bool, len(col))

		stubs := gostub.StubFunc(&Close, nil)
		defer stubs.Reset()
		stubs = gostub.StubFunc(&openNewFile, nil)
		defer stubs.Reset()
		stubs = gostub.StubFunc(&writeDataToCSVFile, nil)
		defer stubs.Reset()

		convey.So(exportDataFromResultSetToCSVFile(ep), convey.ShouldBeNil)
	})

	convey.Convey("exportDataToCSVFile fail", t, func() {
		ep := &ExportConfig{
			userConfig: &tree.ExportParam{
				Lines: &tree.Lines{
					TerminatedBy: &tree.Terminated{},
				},
				Fields: &tree.Fields{
					Terminated: &tree.Terminated{},
					EnclosedBy: &tree.EnclosedBy{},
					EscapedBy:  &tree.EscapedBy{},
				},
				Header:   true,
				FilePath: "test/export.csv",
			},
			mrs: &MysqlResultSet{},
		}

		var col = make([]MysqlColumn, 1)
		var colType = []defines.MysqlType{defines.MYSQL_TYPE_TIMESTAMP}
		for i := 0; i < len(col); i++ {
			col[i].SetColumnType(colType[i])
			ep.mrs.AddColumn(&col[i])
		}

		var data = make([]interface{}, len(col))
		data[0] = 1
		ep.mrs.AddRow(data)
		ep.Symbol = make([][]byte, len(col))
		ep.ColumnFlag = make([]bool, len(col))

		stubs := gostub.StubFunc(&Close, nil)
		defer stubs.Reset()
		stubs = gostub.StubFunc(&openNewFile, nil)
		defer stubs.Reset()
		stubs = gostub.StubFunc(&writeDataToCSVFile, nil)
		defer stubs.Reset()
		convey.So(exportDataFromResultSetToCSVFile(ep), convey.ShouldBeNil)
	})
}
