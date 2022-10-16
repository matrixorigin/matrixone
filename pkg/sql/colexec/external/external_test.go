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

package external

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/smartystreets/goconvey/convey"
)

const (
	Rows = 10 // default rows
)

// add unit tests for cases
type externalTestCase struct {
	arg      *Argument
	types    []types.Type
	proc     *process.Process
	cancel   context.CancelFunc
	format   string
	jsondata string
}

var (
	cases []externalTestCase
)

func newTestCase(all bool, format, jsondata string) externalTestCase {
	proc := testutil.NewProcess()
	proc.FileService = testutil.NewFS()
	ctx, cancel := context.WithCancel(context.Background())
	return externalTestCase{
		proc: proc,
		types: []types.Type{
			{Oid: types.T_int8},
		},
		arg: &Argument{
			Es: &ExternalParam{
				Ctx:       ctx,
				Fileparam: &ExternalFileparam{},
			},
		},
		cancel:   cancel,
		format:   format,
		jsondata: jsondata,
	}
}

func init() {
	cases = []externalTestCase{
		newTestCase(true, tree.CSV, ""),
		newTestCase(true, tree.JSONLINE, tree.OBJECT),
		newTestCase(true, tree.JSONLINE, tree.ARRAY),
	}
}

func Test_String(t *testing.T) {
	buf := new(bytes.Buffer)
	String(cases[0].arg, buf)
}

func Test_Prepare(t *testing.T) {
	convey.Convey("external Prepare", t, func() {
		for _, tcs := range cases {
			param := tcs.arg.Es
			err := Prepare(tcs.proc, tcs.arg)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(param.extern, convey.ShouldNotBeNil)
			convey.So(param.Fileparam.End, convey.ShouldBeTrue)
			extern := &tree.ExternParam{
				Filepath: "",
				Tail: &tree.TailParameter{
					IgnoredLines: 0,
				},
				FileService: tcs.proc.FileService,
				Format:      tcs.format,
				JsonData:    tcs.jsondata,
			}
			json_byte, err := json.Marshal(extern)
			if err != nil {
				panic(err)
			}
			param.CreateSql = string(json_byte)
			err = Prepare(tcs.proc, tcs.arg)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(param.FileList, convey.ShouldBeNil)
			convey.So(param.Fileparam.FileCnt, convey.ShouldEqual, 0)

			extern.Format = "test"
			json_byte, err = json.Marshal(extern)
			convey.So(err, convey.ShouldBeNil)
			param.CreateSql = string(json_byte)
			err = Prepare(tcs.proc, tcs.arg)
			convey.So(err, convey.ShouldNotBeNil)

			if tcs.format == tree.JSONLINE {
				extern = &tree.ExternParam{
					Filepath: "",
					Tail: &tree.TailParameter{
						IgnoredLines: 0,
					},
					Format: tcs.format,
				}
				extern.JsonData = tcs.jsondata
				json_byte, err = json.Marshal(extern)
				convey.So(err, convey.ShouldBeNil)
				param.CreateSql = string(json_byte)
				err = Prepare(tcs.proc, tcs.arg)
				convey.So(err, convey.ShouldBeNil)
				convey.So(param.FileList, convey.ShouldBeNil)
				convey.So(param.Fileparam.FileCnt, convey.ShouldEqual, 0)

				extern.JsonData = "test"
				json_byte, err = json.Marshal(extern)
				convey.So(err, convey.ShouldBeNil)
				param.CreateSql = string(json_byte)
				err = Prepare(tcs.proc, tcs.arg)
				convey.So(err, convey.ShouldNotBeNil)
			}
		}
	})
}

func Test_Call(t *testing.T) {
	convey.Convey("external Call", t, func() {
		for _, tcs := range cases {
			param := tcs.arg.Es
			extern := &tree.ExternParam{
				Filepath: "",
				Tail: &tree.TailParameter{
					IgnoredLines: 0,
				},
				FileService: tcs.proc.FileService,
				Format:      tcs.format,
				JsonData:    tcs.jsondata,
			}
			param.extern = extern
			param.Fileparam.End = false
			param.FileList = []string{"abc.txt"}
			end, err := Call(1, tcs.proc, tcs.arg)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(end, convey.ShouldBeFalse)

			param.Fileparam.End = false
			end, err = Call(1, tcs.proc, tcs.arg)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(end, convey.ShouldBeFalse)

			param.Fileparam.End = true
			end, err = Call(1, tcs.proc, tcs.arg)
			convey.So(err, convey.ShouldBeNil)
			convey.So(end, convey.ShouldBeTrue)
		}
	})
}

func Test_getCompressType(t *testing.T) {
	convey.Convey("getCompressType succ", t, func() {
		param := &tree.ExternParam{
			CompressType: tree.GZIP,
		}
		compress := getCompressType(param)
		convey.So(compress, convey.ShouldEqual, param.CompressType)

		param.CompressType = tree.AUTO
		param.Filepath = "a.gz"
		compress = getCompressType(param)
		convey.So(compress, convey.ShouldEqual, tree.GZIP)

		param.Filepath = "a.bz2"
		compress = getCompressType(param)
		convey.So(compress, convey.ShouldEqual, tree.BZIP2)

		param.Filepath = "a.lz4"
		compress = getCompressType(param)
		convey.So(compress, convey.ShouldEqual, tree.LZ4)

		param.Filepath = "a.csv"
		compress = getCompressType(param)
		convey.So(compress, convey.ShouldEqual, tree.NOCOMPRESS)

		param.Filepath = "a"
		compress = getCompressType(param)
		convey.So(compress, convey.ShouldEqual, tree.NOCOMPRESS)
	})
}

func Test_getUnCompressReader(t *testing.T) {
	convey.Convey("getUnCompressReader succ", t, func() {
		param := &tree.ExternParam{
			CompressType: tree.NOCOMPRESS,
		}
		read, err := getUnCompressReader(param, nil)
		convey.So(read, convey.ShouldBeNil)
		convey.So(err, convey.ShouldBeNil)

		param.CompressType = tree.BZIP2
		read, err = getUnCompressReader(param, &os.File{})
		convey.So(read, convey.ShouldNotBeNil)
		convey.So(err, convey.ShouldBeNil)

		param.CompressType = tree.FLATE
		read, err = getUnCompressReader(param, &os.File{})
		convey.So(read, convey.ShouldNotBeNil)
		convey.So(err, convey.ShouldBeNil)

		param.CompressType = tree.LZ4
		read, err = getUnCompressReader(param, &os.File{})
		convey.So(read, convey.ShouldNotBeNil)
		convey.So(err, convey.ShouldBeNil)

		param.CompressType = tree.LZW
		read, err = getUnCompressReader(param, &os.File{})
		convey.So(read, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)

		param.CompressType = "abc"
		read, err = getUnCompressReader(param, &os.File{})
		convey.So(read, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func Test_makeBatch(t *testing.T) {
	convey.Convey("makeBatch succ", t, func() {
		col := &plan.ColDef{
			Typ: &plan.Type{
				Id: int32(types.T_bool),
			},
		}
		param := &ExternalParam{
			Cols:  []*plan.ColDef{col},
			Attrs: []string{"a"},
		}
		plh := &ParseLineHandler{
			batchSize: 1,
		}
		_ = makeBatch(param, plh, testutil.TestUtilMp)
	})
}

func Test_GetBatchData(t *testing.T) {
	convey.Convey("GetBatchData succ", t, func() {
		line := []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "2020-09-07",
			"2020-09-07 00:00:00", "16", "17", "2020-09-07 00:00:00"}
		atrrs := []string{"col1", "col2", "col3", "col4", "col5", "col6", "col7", "col8", "col9", "col10",
			"col11", "col12", "col13", "col14", "col15", "col16", "col17", "col18"}
		buf := bytes.NewBuffer(nil)
		buf.WriteString("{")
		for idx, attr := range atrrs {
			buf.WriteString(fmt.Sprintf("\"%s\":\"%s\"", attr, line[idx]))
			if idx != len(atrrs)-1 {
				buf.WriteString(",")
			}
		}
		buf.WriteString("}")
		jsonline_object := []string{buf.String()}
		buf.Reset()
		for idx, attr := range atrrs {
			if idx == 0 {
				buf.WriteString(fmt.Sprintf("\"%s\":\"%s\"", line[idx], line[idx]))
			} else {
				buf.WriteString(fmt.Sprintf("\"%s\":\"%s\"", attr, line[idx]))
			}
			if idx != len(atrrs)-1 {
				buf.WriteString(",")
			}
		}
		jsonline_object_key_not_match := []string{"{" + buf.String() + "}"}
		buf.Reset()
		buf.WriteString("[")
		for idx := range line {
			buf.WriteString(fmt.Sprintf("\"%s\"", line[idx]))
			if idx != len(atrrs)-1 {
				buf.WriteString(",")
			}
		}
		buf.WriteString("]")
		jsonline_array := []string{buf.String()}
		buf.Reset()
		buf.WriteString("{")
		for i := 0; i < len(line)-1; i++ {
			buf.WriteString(fmt.Sprintf("\"%s\":\"%s\",", atrrs[i], line[i]))
			if i != len(line)-2 {
				buf.WriteString(",")
			}
		}
		buf.WriteString("}")
		jsonline_object_less := []string{buf.String()}
		buf.Reset()
		buf.WriteString("[")
		for i := 0; i < len(line)-1; i++ {
			buf.WriteString(fmt.Sprintf("\"%s\"", line[i]))
			if i != len(line)-2 {
				buf.WriteString(",")
			}
		}
		buf.WriteString("]")
		jsonline_array_less := []string{buf.String()}

		cols := []*plan.ColDef{
			{
				Typ: &plan.Type{
					Id: int32(types.T_bool),
				},
			},
			{
				Typ: &plan.Type{
					Id: int32(types.T_int8),
				},
			},
			{
				Typ: &plan.Type{
					Id: int32(types.T_int16),
				},
			},
			{
				Typ: &plan.Type{
					Id: int32(types.T_int32),
				},
			},
			{
				Typ: &plan.Type{
					Id: int32(types.T_int64),
				},
			},
			{
				Typ: &plan.Type{
					Id: int32(types.T_uint8),
				},
			},
			{
				Typ: &plan.Type{
					Id: int32(types.T_uint16),
				},
			},
			{
				Typ: &plan.Type{
					Id: int32(types.T_uint32),
				},
			},
			{
				Typ: &plan.Type{
					Id: int32(types.T_uint64),
				},
			},
			{
				Typ: &plan.Type{
					Id: int32(types.T_float32),
				},
			},
			{
				Typ: &plan.Type{
					Id: int32(types.T_float64),
				},
			},
			{
				Typ: &plan.Type{
					Id: int32(types.T_varchar),
				},
			},
			{
				Typ: &plan.Type{
					Id: int32(types.T_json),
				},
			},
			{
				Typ: &plan.Type{
					Id: int32(types.T_date),
				},
			},
			{
				Typ: &plan.Type{
					Id: int32(types.T_datetime),
				},
			},
			{
				Typ: &plan.Type{
					Id:    int32(types.T_decimal64),
					Width: 15,
					Scale: 0,
				},
			},
			{
				Typ: &plan.Type{
					Id:    int32(types.T_decimal128),
					Width: 17,
					Scale: 0,
				},
			},
			{
				Typ: &plan.Type{
					Id: int32(types.T_timestamp),
				},
			},
		}
		param := &ExternalParam{
			Attrs: atrrs,
			Cols:  cols,
			extern: &tree.ExternParam{
				Tail: &tree.TailParameter{
					Fields: &tree.Fields{},
				},
				Format: tree.CSV,
			},
		}
		param.Name2ColIndex = make(map[string]int32)
		for i := 0; i < len(atrrs); i++ {
			param.Name2ColIndex[atrrs[i]] = int32(i)
		}
		plh := &ParseLineHandler{
			batchSize:        1,
			simdCsvLineArray: [][]string{line},
		}

		proc := testutil.NewProc()
		_, err := GetBatchData(param, plh, proc)
		convey.So(err, convey.ShouldBeNil)

		plh.simdCsvLineArray = [][]string{line[:1]}
		_, err = GetBatchData(param, plh, proc)
		convey.So(err, convey.ShouldNotBeNil)

		for i := 0; i < len(atrrs); i++ {
			line[i] = "\\N"
		}
		plh.simdCsvLineArray = [][]string{line}
		_, err = GetBatchData(param, plh, proc)
		convey.So(err, convey.ShouldBeNil)

		line = []string{"0", "1.0", "2.0", "3.0", "4.0", "5.0", "6.0", "7.0", "8.0", "9.0", "10.0", "11.0", "13", "2020-09-07",
			"2020-09-07 00:00:00", "16", "17", "2020-09-07 00:00:00"}
		plh.simdCsvLineArray = [][]string{line}
		_, err = GetBatchData(param, plh, proc)
		convey.So(err, convey.ShouldBeNil)

		line = []string{"truefalse", "128", "32768", "2147483648", "9223372036854775808", "256", "65536", "4294967296", "18446744073709551616",
			"float32", "float64", "", "13", "date", "datetime", "decimal64", "decimal128", "timestamp"}
		for i := 0; i < len(atrrs); i++ {
			tmp := atrrs[i:]
			param.Attrs = tmp
			param.Cols = cols[i:]
			plh.simdCsvLineArray = [][]string{line}
			_, err = GetBatchData(param, plh, proc)
			convey.So(err, convey.ShouldNotBeNil)
		}

		param.extern.Tail.Fields.EnclosedBy = 't'
		_, err = GetBatchData(param, plh, proc)
		convey.So(err, convey.ShouldNotBeNil)

		line[1] = "128.9"
		line[2] = "32768.9"
		line[3] = "2147483648.9"
		line[4] = "a.9"
		line[5] = "256.9"
		line[6] = "65536.9"
		line[7] = "4294967296.9"
		line[8] = "a.9"
		for i := 1; i <= 8; i++ {
			tmp := atrrs[i:]
			param.Attrs = tmp
			param.Cols = cols[i:]
			plh.simdCsvLineArray = [][]string{line}
			_, err = GetBatchData(param, plh, proc)
			convey.So(err, convey.ShouldNotBeNil)
		}

		//test jsonline
		param.extern.Format = tree.JSONLINE
		param.extern.JsonData = tree.OBJECT
		param.Attrs = atrrs
		param.Cols = cols
		plh.simdCsvLineArray = [][]string{jsonline_object}
		_, err = GetBatchData(param, plh, proc)
		convey.So(err, convey.ShouldBeNil)
		plh.simdCsvLineArray = [][]string{jsonline_object_less}
		_, err = GetBatchData(param, plh, proc)
		convey.So(err, convey.ShouldNotBeNil)
		plh.simdCsvLineArray = [][]string{jsonline_object_key_not_match}
		_, err = GetBatchData(param, plh, proc)
		convey.So(err, convey.ShouldNotBeNil)

		param.extern.Format = tree.CSV
		_, err = GetBatchData(param, plh, proc)
		convey.So(err, convey.ShouldNotBeNil)

		param.extern.Format = tree.JSONLINE
		param.extern.JsonData = tree.ARRAY
		plh.simdCsvLineArray = [][]string{jsonline_array}
		_, err = GetBatchData(param, plh, proc)
		convey.So(err, convey.ShouldBeNil)

		param.extern.JsonData = "test"
		_, err = GetBatchData(param, plh, proc)
		convey.So(err, convey.ShouldNotBeNil)

		plh.simdCsvLineArray = [][]string{jsonline_array_less}
		_, err = GetBatchData(param, plh, proc)
		convey.So(err, convey.ShouldNotBeNil)

		jsonline_array_less[0] = jsonline_object_less[0][1:]
		plh.simdCsvLineArray = [][]string{jsonline_array_less}
		_, err = GetBatchData(param, plh, proc)
		convey.So(err, convey.ShouldNotBeNil)
		jsonline_array = append(jsonline_array, jsonline_array_less...)
		plh.simdCsvLineArray = [][]string{jsonline_array}
		_, err = GetBatchData(param, plh, proc)
		convey.So(err, convey.ShouldNotBeNil)
	})
}
