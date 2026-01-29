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
	"strings"
	"testing"

	"github.com/prashantv/gostub"
	"github.com/smartystreets/goconvey/convey"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
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

func Test_getExportFormat(t *testing.T) {
	convey.Convey("getExportFormat returns correct format", t, func() {
		// Test default format (empty string returns "csv")
		ep := &ExportConfig{
			userConfig: &tree.ExportParam{
				ExportFormat: "",
			},
		}
		convey.So(ep.getExportFormat(), convey.ShouldEqual, "csv")

		// Test explicit csv format
		ep.userConfig.ExportFormat = "csv"
		convey.So(ep.getExportFormat(), convey.ShouldEqual, "csv")

		// Test jsonline format
		ep.userConfig.ExportFormat = "jsonline"
		convey.So(ep.getExportFormat(), convey.ShouldEqual, "jsonline")

		// Test parquet format
		ep.userConfig.ExportFormat = "parquet"
		convey.So(ep.getExportFormat(), convey.ShouldEqual, "parquet")
	})
}

func Test_vectorValueToJSON(t *testing.T) {
	mp := mpool.MustNewZero()
	convey.Convey("vectorValueToJSON converts types correctly", t, func() {
		// Test bool type
		convey.Convey("bool type", func() {
			vec := testutil.NewVector(1, types.T_bool.ToType(), mp, false, []bool{true})
			val, err := vectorValueToJSON(vec, 0, nil, nil)
			convey.So(err, convey.ShouldBeNil)
			convey.So(val, convey.ShouldEqual, true)
		})

		// Test int8 type
		convey.Convey("int8 type", func() {
			vec := testutil.NewVector(1, types.T_int8.ToType(), mp, false, []int8{42})
			val, err := vectorValueToJSON(vec, 0, nil, nil)
			convey.So(err, convey.ShouldBeNil)
			convey.So(val, convey.ShouldEqual, int8(42))
		})

		// Test int16 type
		convey.Convey("int16 type", func() {
			vec := testutil.NewVector(1, types.T_int16.ToType(), mp, false, []int16{1234})
			val, err := vectorValueToJSON(vec, 0, nil, nil)
			convey.So(err, convey.ShouldBeNil)
			convey.So(val, convey.ShouldEqual, int16(1234))
		})

		// Test int32 type
		convey.Convey("int32 type", func() {
			vec := testutil.NewVector(1, types.T_int32.ToType(), mp, false, []int32{123456})
			val, err := vectorValueToJSON(vec, 0, nil, nil)
			convey.So(err, convey.ShouldBeNil)
			convey.So(val, convey.ShouldEqual, int32(123456))
		})

		// Test int64 type
		convey.Convey("int64 type", func() {
			vec := testutil.NewVector(1, types.T_int64.ToType(), mp, false, []int64{1234567890})
			val, err := vectorValueToJSON(vec, 0, nil, nil)
			convey.So(err, convey.ShouldBeNil)
			convey.So(val, convey.ShouldEqual, int64(1234567890))
		})

		// Test uint8 type
		convey.Convey("uint8 type", func() {
			vec := testutil.NewVector(1, types.T_uint8.ToType(), mp, false, []uint8{255})
			val, err := vectorValueToJSON(vec, 0, nil, nil)
			convey.So(err, convey.ShouldBeNil)
			convey.So(val, convey.ShouldEqual, uint8(255))
		})

		// Test uint16 type
		convey.Convey("uint16 type", func() {
			vec := testutil.NewVector(1, types.T_uint16.ToType(), mp, false, []uint16{65535})
			val, err := vectorValueToJSON(vec, 0, nil, nil)
			convey.So(err, convey.ShouldBeNil)
			convey.So(val, convey.ShouldEqual, uint16(65535))
		})

		// Test uint32 type
		convey.Convey("uint32 type", func() {
			vec := testutil.NewVector(1, types.T_uint32.ToType(), mp, false, []uint32{4294967295})
			val, err := vectorValueToJSON(vec, 0, nil, nil)
			convey.So(err, convey.ShouldBeNil)
			convey.So(val, convey.ShouldEqual, uint32(4294967295))
		})

		// Test uint64 type
		convey.Convey("uint64 type", func() {
			vec := testutil.NewVector(1, types.T_uint64.ToType(), mp, false, []uint64{18446744073709551615})
			val, err := vectorValueToJSON(vec, 0, nil, nil)
			convey.So(err, convey.ShouldBeNil)
			convey.So(val, convey.ShouldEqual, uint64(18446744073709551615))
		})

		// Test float32 type
		convey.Convey("float32 type", func() {
			vec := testutil.NewVector(1, types.T_float32.ToType(), mp, false, []float32{3.14})
			val, err := vectorValueToJSON(vec, 0, nil, nil)
			convey.So(err, convey.ShouldBeNil)
			convey.So(val, convey.ShouldEqual, float32(3.14))
		})

		// Test float64 type
		convey.Convey("float64 type", func() {
			vec := testutil.NewVector(1, types.T_float64.ToType(), mp, false, []float64{3.14159265359})
			val, err := vectorValueToJSON(vec, 0, nil, nil)
			convey.So(err, convey.ShouldBeNil)
			convey.So(val, convey.ShouldEqual, 3.14159265359)
		})

		// Test varchar type
		convey.Convey("varchar type", func() {
			vec := testutil.NewVector(1, types.T_varchar.ToType(), mp, false, []string{"hello world"})
			val, err := vectorValueToJSON(vec, 0, nil, nil)
			convey.So(err, convey.ShouldBeNil)
			convey.So(val, convey.ShouldEqual, "hello world")
		})

		// Test date type
		convey.Convey("date type", func() {
			vec := testutil.NewVector(1, types.T_date.ToType(), mp, false, []string{"2023-12-25"})
			val, err := vectorValueToJSON(vec, 0, nil, nil)
			convey.So(err, convey.ShouldBeNil)
			convey.So(val, convey.ShouldEqual, "2023-12-25")
		})
	})
}

func TestEscapeJSONControl(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "normal characters",
			input:    `{"a":"hello world 123"}`,
			expected: `{"a":"hello world 123"}`,
		},
		{
			name:     "UTF-8 characters",
			input:    `{"a":"hello 世界 😊"}`,
			expected: `{"a":"hello 世界 😊"}`,
		},
		{
			name:     "single newline",
			input:    `{"a":"abc` + `\n` + `def"}`,
			expected: `{"a":"abc\\ndef"}`,
		},
		{
			name:     "single carriage return",
			input:    `{"a":"abc` + `\r` + `def"}`,
			expected: `{"a":"abc\\rdef"}`,
		},
		{
			name:     "single tab",
			input:    `{"a":"abc` + `\t` + `def"}`,
			expected: `{"a":"abc\\tdef"}`,
		},
		{
			name:     "single form feed",
			input:    `{"a":"abc` + `\f` + `def"}`,
			expected: `{"a":"abc\\fdef"}`,
		},
		{
			name:     "multiple newlines",
			input:    `{"a":"abc` + `\n\n` + `def"}`,
			expected: `{"a":"abc\\n\\ndef"}`,
		},
		{
			name:     "escaped sequences unchanged",
			input:    `{"a":"abc\\n\\r\\t\\b\\fdef"}`,
			expected: `{"a":"abc\\\\n\\\\r\\\\t\\\\b\\\\fdef"}`,
		},
		{
			name:     "mixed escaped and actual",
			input:    `{"a":"abc\\n` + `\t` + `def\\r"}`,
			expected: `{"a":"abc\\\\n\\tdef\\\\r"}`,
		},
		{
			name:     "control at start",
			input:    `\n` + `{"a":"def"}`,
			expected: `\\n{"a":"def"}`,
		},
		{
			name:     "control at end",
			input:    `{"a":"def"}` + `\n`,
			expected: `{"a":"def"}\\n`,
		},
		{
			name:     "long string with controls",
			input:    strings.Repeat("a", 10000) + `\n` + strings.Repeat("b", 10000) + `\t` + strings.Repeat("c", 10000),
			expected: strings.Repeat("a", 10000) + `\\n` + strings.Repeat("b", 10000) + `\\t` + strings.Repeat("c", 10000),
		},
		{
			name:     "backslash literal",
			input:    `{"a":"abc\\def"}`,
			expected: `{"a":"abc\\\\def"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := escapeJSONControlChars(tt.input)
			if res != tt.expected {
				t.Errorf("EscapeJSONControl(%v) =\n%v,\nwant\n%v", tt.input, res, tt.expected)
			}
		})
	}
}

func Test_getExportFilePath(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		fileCnt  uint
		expected string
	}{
		// Legacy behavior: no format specifier
		{
			name:     "no format specifier, fileCnt 0",
			filename: "output.csv",
			fileCnt:  0,
			expected: "output.csv",
		},
		{
			name:     "no format specifier, fileCnt 1",
			filename: "output.csv",
			fileCnt:  1,
			expected: "output.csv.1",
		},
		{
			name:     "no format specifier, fileCnt 10",
			filename: "output.csv",
			fileCnt:  10,
			expected: "output.csv.10",
		},

		// New behavior: %d format specifier
		{
			name:     "simple %d, fileCnt 0",
			filename: "data_%d.csv",
			fileCnt:  0,
			expected: "data_0.csv",
		},
		{
			name:     "simple %d, fileCnt 1",
			filename: "data_%d.csv",
			fileCnt:  1,
			expected: "data_1.csv",
		},
		{
			name:     "simple %d, fileCnt 9999",
			filename: "data_%d.parquet",
			fileCnt:  9999,
			expected: "data_9999.parquet",
		},

		// Padded format specifier: %05d
		{
			name:     "%05d padding, fileCnt 0",
			filename: "data_%05d.parquet",
			fileCnt:  0,
			expected: "data_00000.parquet",
		},
		{
			name:     "%05d padding, fileCnt 1",
			filename: "data_%05d.parquet",
			fileCnt:  1,
			expected: "data_00001.parquet",
		},
		{
			name:     "%05d padding, fileCnt 12345",
			filename: "data_%05d.parquet",
			fileCnt:  12345,
			expected: "data_12345.parquet",
		},
		{
			name:     "%05d padding, fileCnt exceeds width",
			filename: "data_%05d.parquet",
			fileCnt:  123456,
			expected: "data_123456.parquet",
		},

		// Other padding widths
		{
			name:     "%03d padding",
			filename: "file_%03d.jsonl",
			fileCnt:  7,
			expected: "file_007.jsonl",
		},
		{
			name:     "%010d zero padding",
			filename: "file_%010d.csv",
			fileCnt:  42,
			expected: "file_0000000042.csv",
		},

		// Stage URL with format specifier
		{
			name:     "stage URL with %05d",
			filename: "stage://bucket/data_%05d.parquet",
			fileCnt:  1,
			expected: "stage://bucket/data_00001.parquet",
		},

		// Multiple path components
		{
			name:     "path with directories",
			filename: "/path/to/output_%04d.csv",
			fileCnt:  99,
			expected: "/path/to/output_0099.csv",
		},

		// Edge case: % but not a valid format specifier (should use legacy behavior)
		{
			name:     "percent sign but not format specifier",
			filename: "file_100%.csv",
			fileCnt:  1,
			expected: "file_100%.csv.1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getExportFilePath(tt.filename, tt.fileCnt)
			if result != tt.expected {
				t.Errorf("getExportFilePath(%q, %d) = %q, want %q",
					tt.filename, tt.fileCnt, result, tt.expected)
			}
		})
	}
}

func Test_getEffectiveMaxFileSize(t *testing.T) {
	tests := []struct {
		name        string
		maxFileSize uint64
		splitSize   uint64
		expected    uint64
	}{
		{
			name:        "both zero",
			maxFileSize: 0,
			splitSize:   0,
			expected:    0,
		},
		{
			name:        "only MaxFileSize set",
			maxFileSize: 1024 * 1024, // 1MB
			splitSize:   0,
			expected:    1024 * 1024,
		},
		{
			name:        "only SplitSize set",
			maxFileSize: 0,
			splitSize:   10 * 1024 * 1024 * 1024, // 10GB
			expected:    10 * 1024 * 1024 * 1024,
		},
		{
			name:        "SplitSize takes precedence over MaxFileSize",
			maxFileSize: 1024 * 1024,             // 1MB
			splitSize:   10 * 1024 * 1024 * 1024, // 10GB
			expected:    10 * 1024 * 1024 * 1024,
		},
		{
			name:        "SplitSize smaller than MaxFileSize",
			maxFileSize: 10 * 1024 * 1024 * 1024, // 10GB
			splitSize:   1024 * 1024,             // 1MB
			expected:    1024 * 1024,             // SplitSize wins
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ep := &ExportConfig{
				userConfig: &tree.ExportParam{
					MaxFileSize: tt.maxFileSize,
					SplitSize:   tt.splitSize,
				},
			}
			result := getEffectiveMaxFileSize(ep)
			if result != tt.expected {
				t.Errorf("getEffectiveMaxFileSize() = %d, want %d", result, tt.expected)
			}
		})
	}
}

func Test_vectorValueToParquet(t *testing.T) {
	mp := mpool.MustNewZero()
	convey.Convey("vectorValueToParquet converts types correctly", t, func() {
		// Test bool type
		convey.Convey("bool type", func() {
			vec := testutil.NewVector(1, types.T_bool.ToType(), mp, false, []bool{true})
			val, err := vectorValueToParquet(vec, 0, nil)
			convey.So(err, convey.ShouldBeNil)
			convey.So(val, convey.ShouldEqual, true)
		})

		// Test int8 type (converted to int32 for parquet)
		convey.Convey("int8 type", func() {
			vec := testutil.NewVector(1, types.T_int8.ToType(), mp, false, []int8{42})
			val, err := vectorValueToParquet(vec, 0, nil)
			convey.So(err, convey.ShouldBeNil)
			convey.So(val, convey.ShouldEqual, int32(42))
		})

		// Test int16 type (converted to int32 for parquet)
		convey.Convey("int16 type", func() {
			vec := testutil.NewVector(1, types.T_int16.ToType(), mp, false, []int16{1234})
			val, err := vectorValueToParquet(vec, 0, nil)
			convey.So(err, convey.ShouldBeNil)
			convey.So(val, convey.ShouldEqual, int32(1234))
		})

		// Test int32 type
		convey.Convey("int32 type", func() {
			vec := testutil.NewVector(1, types.T_int32.ToType(), mp, false, []int32{123456})
			val, err := vectorValueToParquet(vec, 0, nil)
			convey.So(err, convey.ShouldBeNil)
			convey.So(val, convey.ShouldEqual, int32(123456))
		})

		// Test int64 type
		convey.Convey("int64 type", func() {
			vec := testutil.NewVector(1, types.T_int64.ToType(), mp, false, []int64{1234567890})
			val, err := vectorValueToParquet(vec, 0, nil)
			convey.So(err, convey.ShouldBeNil)
			convey.So(val, convey.ShouldEqual, int64(1234567890))
		})

		// Test uint8 type (converted to int32 for parquet)
		convey.Convey("uint8 type", func() {
			vec := testutil.NewVector(1, types.T_uint8.ToType(), mp, false, []uint8{255})
			val, err := vectorValueToParquet(vec, 0, nil)
			convey.So(err, convey.ShouldBeNil)
			convey.So(val, convey.ShouldEqual, int32(255))
		})

		// Test uint16 type (converted to int32 for parquet)
		convey.Convey("uint16 type", func() {
			vec := testutil.NewVector(1, types.T_uint16.ToType(), mp, false, []uint16{65535})
			val, err := vectorValueToParquet(vec, 0, nil)
			convey.So(err, convey.ShouldBeNil)
			convey.So(val, convey.ShouldEqual, int32(65535))
		})

		// Test uint32 type (converted to int32 for parquet)
		convey.Convey("uint32 type", func() {
			vec := testutil.NewVector(1, types.T_uint32.ToType(), mp, false, []uint32{100000})
			val, err := vectorValueToParquet(vec, 0, nil)
			convey.So(err, convey.ShouldBeNil)
			convey.So(val, convey.ShouldEqual, int32(100000))
		})

		// Test uint64 type (converted to int64 for parquet)
		convey.Convey("uint64 type", func() {
			vec := testutil.NewVector(1, types.T_uint64.ToType(), mp, false, []uint64{1234567890})
			val, err := vectorValueToParquet(vec, 0, nil)
			convey.So(err, convey.ShouldBeNil)
			convey.So(val, convey.ShouldEqual, int64(1234567890))
		})

		// Test float32 type
		convey.Convey("float32 type", func() {
			vec := testutil.NewVector(1, types.T_float32.ToType(), mp, false, []float32{3.14})
			val, err := vectorValueToParquet(vec, 0, nil)
			convey.So(err, convey.ShouldBeNil)
			convey.So(val, convey.ShouldEqual, float32(3.14))
		})

		// Test float64 type
		convey.Convey("float64 type", func() {
			vec := testutil.NewVector(1, types.T_float64.ToType(), mp, false, []float64{3.14159265359})
			val, err := vectorValueToParquet(vec, 0, nil)
			convey.So(err, convey.ShouldBeNil)
			convey.So(val, convey.ShouldEqual, 3.14159265359)
		})

		// Test varchar type
		convey.Convey("varchar type", func() {
			vec := testutil.NewVector(1, types.T_varchar.ToType(), mp, false, []string{"hello world"})
			val, err := vectorValueToParquet(vec, 0, nil)
			convey.So(err, convey.ShouldBeNil)
			convey.So(val, convey.ShouldEqual, "hello world")
		})

		// Test date type
		convey.Convey("date type", func() {
			vec := testutil.NewVector(1, types.T_date.ToType(), mp, false, []string{"2023-12-25"})
			val, err := vectorValueToParquet(vec, 0, nil)
			convey.So(err, convey.ShouldBeNil)
			convey.So(val, convey.ShouldEqual, "2023-12-25")
		})
	})
}

func Test_NewParquetWriter(t *testing.T) {
	convey.Convey("NewParquetWriter creates writer correctly", t, func() {
		// Test with nil MysqlResultSet
		convey.Convey("nil result set returns error", func() {
			_, err := NewParquetWriter(context.Background(), nil, nil)
			convey.So(err, convey.ShouldNotBeNil)
		})

		// Test with empty columns
		convey.Convey("empty columns returns error", func() {
			mrs := &MysqlResultSet{}
			_, err := NewParquetWriter(context.Background(), nil, mrs)
			convey.So(err, convey.ShouldNotBeNil)
		})

		// Test with valid columns
		convey.Convey("valid columns creates writer", func() {
			mrs := &MysqlResultSet{}
			col1 := new(MysqlColumn)
			col1.SetName("id")
			col1.SetColumnType(defines.MYSQL_TYPE_LONG)
			col2 := new(MysqlColumn)
			col2.SetName("name")
			col2.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
			mrs.AddColumn(col1)
			mrs.AddColumn(col2)

			pw, err := NewParquetWriter(context.Background(), nil, mrs)
			convey.So(err, convey.ShouldBeNil)
			convey.So(pw, convey.ShouldNotBeNil)
			convey.So(len(pw.columnNames), convey.ShouldEqual, 2)
			convey.So(pw.columnNames[0], convey.ShouldEqual, "id")
			convey.So(pw.columnNames[1], convey.ShouldEqual, "name")
		})
	})
}

func Test_ParquetWriter_Close(t *testing.T) {
	convey.Convey("ParquetWriter Close returns valid parquet data", t, func() {
		mrs := &MysqlResultSet{}
		col1 := new(MysqlColumn)
		col1.SetName("id")
		col1.SetColumnType(defines.MYSQL_TYPE_LONG)
		mrs.AddColumn(col1)

		pw, err := NewParquetWriter(context.Background(), nil, mrs)
		convey.So(err, convey.ShouldBeNil)

		// Close without writing any data
		data, err := pw.Close()
		convey.So(err, convey.ShouldBeNil)
		convey.So(len(data), convey.ShouldBeGreaterThan, 0)
		// Parquet files start with "PAR1" magic bytes
		convey.So(string(data[:4]), convey.ShouldEqual, "PAR1")
	})
}

func Test_inferFormatFromSuffix(t *testing.T) {
	convey.Convey("inferFormatFromSuffix returns correct format based on file suffix", t, func() {
		// CSV suffixes
		convey.So(inferFormatFromSuffix("data.csv"), convey.ShouldEqual, "csv")
		convey.So(inferFormatFromSuffix("data.CSV"), convey.ShouldEqual, "csv")
		convey.So(inferFormatFromSuffix("/path/to/data.csv"), convey.ShouldEqual, "csv")
		convey.So(inferFormatFromSuffix("stage://bucket/data.csv"), convey.ShouldEqual, "csv")

		// JSONLINE suffixes
		convey.So(inferFormatFromSuffix("data.jsonl"), convey.ShouldEqual, "jsonline")
		convey.So(inferFormatFromSuffix("data.jsonline"), convey.ShouldEqual, "jsonline")
		convey.So(inferFormatFromSuffix("data.ndjson"), convey.ShouldEqual, "jsonline")
		convey.So(inferFormatFromSuffix("data.JSONL"), convey.ShouldEqual, "jsonline")

		// Parquet suffixes
		convey.So(inferFormatFromSuffix("data.parquet"), convey.ShouldEqual, "parquet")
		convey.So(inferFormatFromSuffix("data.PARQUET"), convey.ShouldEqual, "parquet")
		convey.So(inferFormatFromSuffix("/path/to/data.parquet"), convey.ShouldEqual, "parquet")

		// With split pattern %d
		convey.So(inferFormatFromSuffix("data_%05d.csv"), convey.ShouldEqual, "csv")
		convey.So(inferFormatFromSuffix("data_%d.jsonl"), convey.ShouldEqual, "jsonline")
		convey.So(inferFormatFromSuffix("data_%05d.parquet"), convey.ShouldEqual, "parquet")

		// Unknown suffix returns empty string
		convey.So(inferFormatFromSuffix("data.txt"), convey.ShouldEqual, "")
		convey.So(inferFormatFromSuffix("data.json"), convey.ShouldEqual, "")
		convey.So(inferFormatFromSuffix("data"), convey.ShouldEqual, "")
	})
}

func Test_validateExportFormat(t *testing.T) {
	ctx := context.Background()

	convey.Convey("validateExportFormat validates and infers format correctly", t, func() {
		// Test 1: No FORMAT specified, infer from .csv suffix
		convey.Convey("infer csv from suffix", func() {
			ep := &tree.ExportParam{
				FilePath:     "data.csv",
				ExportFormat: "",
			}
			err := validateExportFormat(ctx, ep)
			convey.So(err, convey.ShouldBeNil)
			convey.So(ep.ExportFormat, convey.ShouldEqual, "csv")
		})

		// Test 2: No FORMAT specified, infer from .jsonl suffix
		convey.Convey("infer jsonline from suffix", func() {
			ep := &tree.ExportParam{
				FilePath:     "data.jsonl",
				ExportFormat: "",
			}
			err := validateExportFormat(ctx, ep)
			convey.So(err, convey.ShouldBeNil)
			convey.So(ep.ExportFormat, convey.ShouldEqual, "jsonline")
		})

		// Test 3: No FORMAT specified, infer from .parquet suffix
		convey.Convey("infer parquet from suffix", func() {
			ep := &tree.ExportParam{
				FilePath:     "data.parquet",
				ExportFormat: "",
			}
			err := validateExportFormat(ctx, ep)
			convey.So(err, convey.ShouldBeNil)
			convey.So(ep.ExportFormat, convey.ShouldEqual, "parquet")
		})

		// Test 4: No FORMAT specified, unknown suffix defaults to csv
		convey.Convey("unknown suffix defaults to csv", func() {
			ep := &tree.ExportParam{
				FilePath:     "data.txt",
				ExportFormat: "",
			}
			err := validateExportFormat(ctx, ep)
			convey.So(err, convey.ShouldBeNil)
			convey.So(ep.ExportFormat, convey.ShouldEqual, "csv")
		})

		// Test 5: FORMAT matches suffix - ok
		convey.Convey("format matches suffix", func() {
			ep := &tree.ExportParam{
				FilePath:     "data.csv",
				ExportFormat: "csv",
			}
			err := validateExportFormat(ctx, ep)
			convey.So(err, convey.ShouldBeNil)
		})

		// Test 6: FORMAT mismatches suffix - error
		convey.Convey("format mismatches suffix", func() {
			ep := &tree.ExportParam{
				FilePath:     "data.csv",
				ExportFormat: "jsonline",
			}
			err := validateExportFormat(ctx, ep)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldContainSubstring, "format 'jsonline' does not match file suffix '.csv'")
		})

		// Test 7: FORMAT with parquet suffix mismatch
		convey.Convey("format csv mismatches parquet suffix", func() {
			ep := &tree.ExportParam{
				FilePath:     "data.parquet",
				ExportFormat: "csv",
			}
			err := validateExportFormat(ctx, ep)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldContainSubstring, "format 'csv' does not match file suffix '.parquet'")
		})

		// Test 8: FORMAT with unknown suffix - ok (no suffix to validate against)
		convey.Convey("format with unknown suffix is allowed", func() {
			ep := &tree.ExportParam{
				FilePath:     "data.txt",
				ExportFormat: "jsonline",
			}
			err := validateExportFormat(ctx, ep)
			convey.So(err, convey.ShouldBeNil)
		})

		// Test 9: Split pattern with suffix validation
		convey.Convey("split pattern with suffix validation", func() {
			ep := &tree.ExportParam{
				FilePath:     "data_%05d.parquet",
				ExportFormat: "csv",
			}
			err := validateExportFormat(ctx, ep)
			convey.So(err, convey.ShouldNotBeNil)
		})

		// Test 10: Split pattern with matching format
		convey.Convey("split pattern with matching format", func() {
			ep := &tree.ExportParam{
				FilePath:     "data_%05d.parquet",
				ExportFormat: "parquet",
			}
			err := validateExportFormat(ctx, ep)
			convey.So(err, convey.ShouldBeNil)
		})
	})
}

func Test_ParquetWriter_Size(t *testing.T) {
	convey.Convey("ParquetWriter Size returns current buffer size", t, func() {
		ctx := context.Background()
		mrs := &MysqlResultSet{}
		col1 := new(MysqlColumn)
		col1.SetName("id")
		col1.SetColumnType(defines.MYSQL_TYPE_LONG)
		col2 := new(MysqlColumn)
		col2.SetName("name")
		col2.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
		mrs.AddColumn(col1)
		mrs.AddColumn(col2)

		pw, err := NewParquetWriter(ctx, nil, mrs)
		convey.So(err, convey.ShouldBeNil)
		convey.So(pw, convey.ShouldNotBeNil)

		// Initial size should be 4 (parquet magic bytes "PAR1")
		convey.So(pw.Size(), convey.ShouldEqual, 4)

		// After close, the buffer should have data
		data, err := pw.Close()
		convey.So(err, convey.ShouldBeNil)
		convey.So(len(data), convey.ShouldBeGreaterThan, 0)
	})
}

func Test_shouldSplitParquetFile(t *testing.T) {
	convey.Convey("shouldSplitParquetFile checks if file should be split", t, func() {
		convey.Convey("returns false when splitSize is 0", func() {
			result := shouldSplitParquetFile(1000, 0)
			convey.So(result, convey.ShouldBeFalse)
		})

		convey.Convey("returns false when current size is below threshold", func() {
			result := shouldSplitParquetFile(500, 1000)
			convey.So(result, convey.ShouldBeFalse)
		})

		convey.Convey("returns true when current size exceeds threshold", func() {
			result := shouldSplitParquetFile(1500, 1000)
			convey.So(result, convey.ShouldBeTrue)
		})

		convey.Convey("returns true when current size equals threshold", func() {
			result := shouldSplitParquetFile(1000, 1000)
			convey.So(result, convey.ShouldBeTrue)
		})
	})
}
