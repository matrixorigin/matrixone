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
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	Rows = 10 // default rows
)

// add unit tests for cases
type externalTestCase struct {
	arg      *External
	types    []types.Type
	proc     *process.Process
	cancel   context.CancelFunc
	format   string
	jsondata string
}

var (
	cases         []externalTestCase
	defaultOption = []string{"filepath", "abc", "format", "jsonline", "jsondata", "array"}
)

func newTestCase(format, jsondata string) externalTestCase {
	proc := testutil.NewProcess()
	proc.Base.FileService = testutil.NewFS()
	ctx, cancel := context.WithCancel(context.Background())
	return externalTestCase{
		proc:  proc,
		types: []types.Type{types.T_int8.ToType()},
		arg: &External{
			ctr: container{},
			Es: &ExternalParam{
				ExParamConst: ExParamConst{
					Ctx: ctx,
				},
				ExParam: ExParam{
					Fileparam: &ExFileparam{},
					Filter:    &FilterParam{},
				},
			},
			OperatorBase: vm.OperatorBase{
				OperatorInfo: vm.OperatorInfo{
					Idx:     1,
					IsFirst: false,
					IsLast:  false,
				},
			},
		},
		cancel:   cancel,
		format:   format,
		jsondata: jsondata,
	}
}

func init() {
	cases = []externalTestCase{
		newTestCase(tree.CSV, ""),
		newTestCase(tree.JSONLINE, tree.OBJECT),
		newTestCase(tree.JSONLINE, tree.ARRAY),
	}
}

func Test_String(t *testing.T) {
	buf := new(bytes.Buffer)
	cases[0].arg.String(buf)
}

func Test_Prepare(t *testing.T) {
	convey.Convey("external Prepare", t, func() {
		for _, tcs := range cases {
			param := tcs.arg.Es
			extern := &tree.ExternParam{
				ExParamConst: tree.ExParamConst{
					Filepath: "",
					Tail: &tree.TailParameter{
						IgnoredLines: 0,
					},
					Format: tcs.format,
					Option: defaultOption,
				},
				ExParam: tree.ExParam{
					FileService: tcs.proc.Base.FileService,
					JsonData:    tcs.jsondata,
					Ctx:         context.Background(),
				},
			}
			json_byte, err := json.Marshal(extern)
			if err != nil {
				panic(err)
			}
			param.CreateSql = string(json_byte)
			tcs.arg.Es.Extern = extern
			err = tcs.arg.Prepare(tcs.proc)
			convey.So(err, convey.ShouldBeNil)
			convey.So(param.FileList, convey.ShouldBeNil)
			convey.So(param.Fileparam.FileCnt, convey.ShouldEqual, 0)

			extern.Format = tcs.format
			json_byte, err = json.Marshal(extern)
			convey.So(err, convey.ShouldBeNil)
			param.CreateSql = string(json_byte)
			err = tcs.arg.Prepare(tcs.proc)
			convey.So(err, convey.ShouldBeNil)

			if tcs.format == tree.JSONLINE {
				extern = &tree.ExternParam{
					ExParamConst: tree.ExParamConst{
						Filepath: "",
						Tail: &tree.TailParameter{
							IgnoredLines: 0,
						},
						Format: tcs.format,
						Option: defaultOption,
					},
				}
				extern.JsonData = tcs.jsondata
				json_byte, err = json.Marshal(extern)
				convey.So(err, convey.ShouldBeNil)
				param.CreateSql = string(json_byte)
				err = tcs.arg.Prepare(tcs.proc)
				convey.So(err, convey.ShouldBeNil)
				convey.So(param.FileList, convey.ShouldResemble, []string(nil))
				convey.So(param.Fileparam.FileCnt, convey.ShouldEqual, 0)

				extern.Option = []string{"filepath", "abc", "format", "jsonline", "jsondata", "array"}
				json_byte, err = json.Marshal(extern)
				convey.So(err, convey.ShouldBeNil)
				param.CreateSql = string(json_byte)

				err = tcs.arg.Prepare(tcs.proc)
				convey.So(err, convey.ShouldBeNil)
			}
		}
	})
}

func Test_Call(t *testing.T) {
	convey.Convey("external Call", t, func() {
		for _, tcs := range cases {
			param := tcs.arg.Es
			extern := &tree.ExternParam{
				ExParamConst: tree.ExParamConst{
					Filepath: "",
					Tail: &tree.TailParameter{
						IgnoredLines: 0,
					},
					Format: tcs.format,
				},
				ExParam: tree.ExParam{
					FileService: tcs.proc.Base.FileService,
					JsonData:    tcs.jsondata,
					Ctx:         context.Background(),
				},
			}
			param.Extern = extern
			param.Fileparam.End = false
			param.FileList = []string{"abc.txt"}
			param.FileOffsetTotal = []*pipeline.FileOffset{
				{
					Offset: []int64{0, -1},
				},
			}
			param.FileSize = []int64{1}
			end, err := vm.Exec(tcs.arg, tcs.proc)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(end.Status == vm.ExecStop, convey.ShouldBeFalse)

			param.Fileparam.End = false
			end, err = vm.Exec(tcs.arg, tcs.proc)
			convey.So(err, convey.ShouldBeNil)
			convey.So(end.Status == vm.ExecStop, convey.ShouldBeTrue)

			param.Fileparam.End = true
			end, err = vm.Exec(tcs.arg, tcs.proc)
			convey.So(err, convey.ShouldBeNil)
			convey.So(end.Status == vm.ExecStop, convey.ShouldBeTrue)
		}
	})
}

func Test_Call2(t *testing.T) {
	cases2 := []externalTestCase{
		newTestCase(tree.CSV, ""),
		newTestCase(tree.JSONLINE, tree.OBJECT),
		newTestCase(tree.JSONLINE, tree.ARRAY),
	}
	convey.Convey("external Call", t, func() {
		for _, tcs := range cases2 {
			param := tcs.arg.Es
			extern := &tree.ExternParam{
				ExParamConst: tree.ExParamConst{
					Filepath: "",
					Tail: &tree.TailParameter{
						IgnoredLines: 0,
					},
					Format:   tcs.format,
					ScanType: tree.INLINE,
				},
				ExParam: tree.ExParam{
					FileService: tcs.proc.Base.FileService,
					JsonData:    tcs.jsondata,
					Ctx:         context.Background(),
				},
			}
			param.Extern = extern
			attrs := []string{"col1", "col2", "col3"}
			param.Attrs = attrs

			cols := []*plan.ColDef{
				{
					Typ: plan.Type{
						Id: int32(types.T_bool),
					},
				},
				{
					Typ: plan.Type{
						Id: int32(types.T_int8),
					},
				},
				{
					Typ: plan.Type{
						Id: int32(types.T_int16),
					},
				},
			}
			param.Cols = cols
			param.Fileparam.End = false
			param.FileList = []string{"abc.txt"}
			param.FileOffsetTotal = []*pipeline.FileOffset{
				{
					Offset: []int64{0, -1},
				},
			}
			param.FileSize = []int64{1}

			var err error
			err = tcs.arg.Prepare(tcs.proc)
			convey.So(err, convey.ShouldBeNil)
			end, err := vm.Exec(tcs.arg, tcs.proc)
			convey.So(err, convey.ShouldBeNil)
			convey.So(end.Status == vm.ExecStop, convey.ShouldBeFalse)
			tcs.arg.Reset(tcs.proc, false, nil)

			err = tcs.arg.Prepare(tcs.proc)
			convey.So(err, convey.ShouldBeNil)
			end, err = vm.Exec(tcs.arg, tcs.proc)
			convey.So(err, convey.ShouldBeNil)
			convey.So(end.Status == vm.ExecStop, convey.ShouldBeTrue)
			tcs.arg.Free(tcs.proc, false, nil)
			require.Equal(t, int64(0), tcs.proc.Mp().CurrNB())
		}
	})
}

func Test_CALL3(t *testing.T) {
	case3 := newTestCase(tree.CSV, "")

	convey.Convey("external Call", t, func() {
		tcs := case3
		param := tcs.arg.Es
		extern := &tree.ExternParam{
			ExParamConst: tree.ExParamConst{
				Filepath: "",
				Tail: &tree.TailParameter{
					IgnoredLines: 0,
					Fields: &tree.Fields{
						Terminated: &tree.Terminated{
							Value: ",",
						},
					},
				},
				Format:   tcs.format,
				ScanType: tree.INLINE,
			},
			ExParam: tree.ExParam{
				FileService: tcs.proc.Base.FileService,
				JsonData:    tcs.jsondata,
				Ctx:         context.Background(),
			},
		}
		param.Extern = extern
		attrs := []string{"col1", "col2", "col3"}
		param.Attrs = attrs

		cols := []*plan.ColDef{
			{
				Typ: plan.Type{
					Id: int32(types.T_int32),
				},
			},
			{
				Typ: plan.Type{
					Id: int32(types.T_int8),
				},
			},
			{
				Typ: plan.Type{
					Id: int32(types.T_int16),
				},
			},
		}
		param.Cols = cols
		param.Fileparam.End = false
		param.FileList = []string{"abc.txt"}
		param.FileOffsetTotal = []*pipeline.FileOffset{
			{
				Offset: []int64{0, -1},
			},
		}
		param.FileSize = []int64{1}

		// line := []string{"1", "2", "3"}
		param.Name2ColIndex = make(map[string]int32)
		for i := 0; i < len(attrs); i++ {
			param.Name2ColIndex[attrs[i]] = int32(i)
		}
		param.TbColToDataCol = make(map[string]int32)
		for i := 0; i < len(attrs); i++ {
			param.TbColToDataCol[attrs[i]] = int32(i)
		}

		param.Extern.Data = "1,2,3"
		var err error

		err = tcs.arg.Prepare(tcs.proc)
		convey.So(err, convey.ShouldBeNil)
		param.plh, err = getMOCSVReader(param, tcs.proc)
		require.NoError(t, err)
		end, err := vm.Exec(tcs.arg, tcs.proc)
		convey.So(err, convey.ShouldBeNil)
		convey.So(end.Status == vm.ExecStop, convey.ShouldBeFalse)
		tcs.arg.Reset(tcs.proc, false, nil)

		param.Fileparam.End = false
		err = tcs.arg.Prepare(tcs.proc)
		convey.So(err, convey.ShouldBeNil)
		param.plh, err = getMOCSVReader(param, tcs.proc)
		require.NoError(t, err)
		end, err = vm.Exec(tcs.arg, tcs.proc)
		convey.So(err, convey.ShouldBeNil)
		convey.So(end.Status == vm.ExecStop, convey.ShouldBeFalse)
		tcs.arg.Free(tcs.proc, false, nil)
		require.Equal(t, int64(0), tcs.proc.Mp().CurrNB())
	})
}

func Test_getCompressType(t *testing.T) {
	convey.Convey("getCompressType succ", t, func() {
		param := &tree.ExternParam{
			ExParamConst: tree.ExParamConst{
				CompressType: tree.GZIP,
			},
			ExParam: tree.ExParam{
				Ctx: context.Background(),
			},
		}
		compress := GetCompressType(param, param.Filepath)
		convey.So(compress, convey.ShouldEqual, param.CompressType)

		param.CompressType = tree.AUTO
		param.Filepath = "a.gz"
		compress = GetCompressType(param, param.Filepath)
		convey.So(compress, convey.ShouldEqual, tree.GZIP)

		param.Filepath = "a.bz2"
		compress = GetCompressType(param, param.Filepath)
		convey.So(compress, convey.ShouldEqual, tree.BZIP2)

		param.Filepath = "a.lz4"
		compress = GetCompressType(param, param.Filepath)
		convey.So(compress, convey.ShouldEqual, tree.LZ4)

		param.Filepath = "a.csv"
		compress = GetCompressType(param, param.Filepath)
		convey.So(compress, convey.ShouldEqual, tree.NOCOMPRESS)

		param.Filepath = "a"
		compress = GetCompressType(param, param.Filepath)
		convey.So(compress, convey.ShouldEqual, tree.NOCOMPRESS)
	})
}

func Test_getUnCompressReader(t *testing.T) {
	convey.Convey("getUnCompressReader succ", t, func() {
		param := &tree.ExternParam{
			ExParamConst: tree.ExParamConst{
				CompressType: tree.NOCOMPRESS,
			},
			ExParam: tree.ExParam{
				Ctx: context.Background(),
			},
		}
		read, err := getUnCompressReader(param, param.Filepath, nil)
		convey.So(read, convey.ShouldBeNil)
		convey.So(err, convey.ShouldBeNil)

		param.CompressType = tree.BZIP2
		read, err = getUnCompressReader(param, param.Filepath, &os.File{})
		convey.So(read, convey.ShouldNotBeNil)
		convey.So(err, convey.ShouldBeNil)

		param.CompressType = tree.FLATE
		read, err = getUnCompressReader(param, param.Filepath, &os.File{})
		convey.So(read, convey.ShouldNotBeNil)
		convey.So(err, convey.ShouldBeNil)

		param.CompressType = tree.LZ4
		read, err = getUnCompressReader(param, param.Filepath, &os.File{})
		convey.So(read, convey.ShouldNotBeNil)
		convey.So(err, convey.ShouldBeNil)

		param.CompressType = tree.LZW
		read, err = getUnCompressReader(param, param.Filepath, &os.File{})
		convey.So(read, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)

		param.CompressType = "abc"
		read, err = getUnCompressReader(param, param.Filepath, &os.File{})
		convey.So(read, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func TestReadDirSymlink(t *testing.T) {
	root := t.TempDir()
	ctx := context.Background()

	// create a/b/c
	err := os.MkdirAll(filepath.Join(root, "a", "b", "c"), 0755)
	assert.Nil(t, err)

	// write a/b/c/foo
	err = os.WriteFile(filepath.Join(root, "a", "b", "c", "foo"), []byte("abc"), 0644)
	assert.Nil(t, err)

	// symlink a/b/d to a/b/c
	err = os.Symlink(
		filepath.Join(root, "a", "b", "c"),
		filepath.Join(root, "a", "b", "d"),
	)
	assert.Nil(t, err)

	// read a/b/d/foo
	fooPathInB := filepath.Join(root, "a", "b", "d", "foo")
	files, _, err := plan2.ReadDir(&tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			Filepath: fooPathInB,
		},
		ExParam: tree.ExParam{
			Ctx: ctx,
		},
	})
	assert.Nil(t, err)
	assert.Equal(t, 1, len(files))
	assert.Equal(t, fooPathInB, files[0])

	path1 := root + "/a//b/./../b/c/foo"
	files1, _, err := plan2.ReadDir(&tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			Filepath: path1,
		},
		ExParam: tree.ExParam{
			Ctx: ctx,
		},
	})
	assert.Nil(t, err)
	pathWant1 := root + "/a/b/c/foo"
	assert.Equal(t, 1, len(files1))
	assert.Equal(t, pathWant1, files1[0])
}

func Test_fliterByAccountAndFilename(t *testing.T) {
	type args struct {
		node     *plan.Node
		proc     *process.Process
		fileList []string
		fileSize []int64
	}

	files := []struct {
		date types.Date
		path string
		size int64
	}{
		{738551, "etl:/sys/logs/2023/02/01/filepath", 1},
		{738552, "etl:/sys/logs/2023/02/02/filepath", 2},
		{738553, "etl:/sys/logs/2023/02/03/filepath", 3},
		{738554, "etl:/sys/logs/2023/02/04/filepath", 4},
		{738555, "etl:/sys/logs/2023/02/05/filepath", 5},
		{738556, "etl:/sys/logs/2023/02/06/filepath", 6},
	}

	toPathArr := func(files []struct {
		date types.Date
		path string
		size int64
	}) []string {
		fileList := make([]string, len(files))
		for idx, f := range files {
			fileList[idx] = f.path
		}
		return fileList
	}
	toSizeArr := func(files []struct {
		date types.Date
		path string
		size int64
	}) []int64 {
		fileSize := make([]int64, len(files))
		for idx, f := range files {
			fileSize[idx] = f.size
		}
		return fileSize
	}

	fileList := toPathArr(files)
	fileSize := toSizeArr(files)

	e, err := function.GetFunctionByName(context.Background(), "=", []types.Type{types.T_date.ToType(), types.T_date.ToType()})
	if err != nil {
		panic(err)
	}
	equalDate2DateFid := e.GetEncodedOverloadID()

	e, err = function.GetFunctionByName(context.Background(), "<", []types.Type{types.T_date.ToType(), types.T_date.ToType()})
	if err != nil {
		panic(err)
	}
	lessDate2DateFid := e.GetEncodedOverloadID()

	e, err = function.GetFunctionByName(context.Background(), "mo_log_date", []types.Type{types.T_varchar.ToType()})
	if err != nil {
		panic(err)
	}
	mologdateFid := e.GetEncodedOverloadID()
	tableName := "dummy_table"

	mologdateConst := func(idx int) *plan.Expr {
		return &plan.Expr{
			Typ: plan.Type{
				Id: int32(types.T_date),
			},
			Expr: &plan.Expr_Lit{
				Lit: &plan.Literal{
					Isnull: false,
					Value: &plan.Literal_Dateval{
						Dateval: int32(files[idx].date),
					},
				},
			},
		}
	}
	mologdateFunc := func() *plan.Expr {
		return &plan.Expr{
			Typ: plan.Type{
				Id: int32(types.T_date),
			},
			Expr: &plan.Expr_F{
				F: &plan.Function{
					Func: &plan.ObjectRef{Obj: mologdateFid, ObjName: "mo_log_date"},
					Args: []*plan.Expr{
						{
							Typ: plan.Type{
								Id: int32(types.T_varchar),
							},
							Expr: &plan.Expr_Col{
								Col: &plan.ColRef{
									RelPos: 0,
									ColPos: 0,
									Name:   tableName + "." + catalog.ExternalFilePath,
								},
							},
						},
					},
				},
			},
		}
	}

	nodeWithFunction := func(expr *plan.Expr_F) *plan.Node {
		return &plan.Node{
			NodeType: plan.Node_EXTERNAL_SCAN,
			Stats:    &plan.Stats{},
			TableDef: &plan.TableDef{
				TableType: "func_table",
				TblFunc: &plan.TableFunction{
					Name: tableName,
				},
				Cols: []*plan.ColDef{
					{
						Name: catalog.ExternalFilePath,
						Typ: plan.Type{
							Id:    int32(types.T_varchar),
							Width: types.MaxVarcharLen,
							Table: tableName,
						},
					},
				},
			},
			FilterList: []*plan.Expr{
				{
					Typ: plan.Type{
						Id: int32(types.T_bool),
					},
					Expr: expr,
				},
			},
		}
	}

	tests := []struct {
		name  string
		args  args
		want  []string
		want1 []int64
	}{
		{
			name: "mo_log_date_20230205",
			args: args{
				node: nodeWithFunction(&plan.Expr_F{
					F: &plan.Function{
						Func: &plan.ObjectRef{Obj: equalDate2DateFid, ObjName: "="},
						Args: []*plan.Expr{
							mologdateConst(5),
							mologdateFunc(),
						},
					},
				}),
				proc:     testutil.NewProc(),
				fileList: fileList,
				fileSize: fileSize,
			},
			want:  []string{files[5].path},
			want1: []int64{files[5].size},
		},
		{
			name: "mo_log_date_gt_20230202",
			args: args{
				node: nodeWithFunction(&plan.Expr_F{
					F: &plan.Function{
						Func: &plan.ObjectRef{Obj: lessDate2DateFid, ObjName: "<"},
						Args: []*plan.Expr{
							mologdateConst(2),
							mologdateFunc(),
						},
					},
				}),
				proc:     testutil.NewProc(),
				fileList: fileList,
				fileSize: fileSize,
			},
			want:  toPathArr(files[3:]),
			want1: toSizeArr(files[3:]),
		},
		{
			name: "mo_log_date_lt_20230202",
			args: args{
				node: nodeWithFunction(&plan.Expr_F{
					F: &plan.Function{
						Func: &plan.ObjectRef{Obj: lessDate2DateFid, ObjName: "<"},
						Args: []*plan.Expr{
							mologdateFunc(),
							mologdateConst(2),
						},
					},
				}),
				proc:     testutil.NewProc(),
				fileList: fileList,
				fileSize: fileSize,
			},
			want:  toPathArr(files[:2]),
			want1: toSizeArr(files[:2]),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := filterByAccountAndFilename(context.TODO(), tt.args.node, tt.args.proc, tt.args.fileList, tt.args.fileSize)
			require.Nil(t, err)
			require.Equal(t, tt.want, got)
			require.Equal(t, tt.want1, got1)
		})
	}
}

// test load data local infile with a compress file which not exists
// getUnCompressReader will return EOF err in that case, and getMOCSVReader should handle EOF, and return nil err
func Test_getMOCSVReader(t *testing.T) {
	case1 := newTestCase(tree.CSV, "")
	param := case1.arg.Es
	extern := &tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			Filepath: "",
			Tail: &tree.TailParameter{
				IgnoredLines: 0,
			},
			Format:   case1.format,
			ScanType: tree.INFILE,
		},
		ExParam: tree.ExParam{
			FileService: case1.proc.Base.FileService,
			JsonData:    case1.jsondata,
			Ctx:         context.Background(),
			Local:       true,
		},
	}
	var writer *io.PipeWriter
	case1.proc.Base.LoadLocalReader, writer = io.Pipe()
	_ = writer.Close()

	param.Extern = extern
	param.Fileparam.Filepath = "/noexistsfile.gz"
	_, err := getMOCSVReader(param, case1.proc)
	require.Equal(t, nil, err)
}
