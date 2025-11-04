// Copyright 2022 - 2025 Matrix Origin
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
	"time"

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
	"github.com/syncthing/notify"
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
	defaultOption = []string{"filepath", "abc", "format", "jsonline", "jsondata", "array"}
)

func newTestCase(t *testing.T, format, jsondata string) externalTestCase {
	proc := testutil.NewProcess(t)
	proc.Base.FileService = testutil.NewFS(t)
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

func makeCases(t *testing.T) []externalTestCase {
	return []externalTestCase{
		newTestCase(t, tree.CSV, ""),
		newTestCase(t, tree.JSONLINE, tree.OBJECT),
		newTestCase(t, tree.JSONLINE, tree.ARRAY),
	}
}

func Test_String(t *testing.T) {
	buf := new(bytes.Buffer)
	cases := makeCases(t)
	cases[0].arg.String(buf)
}

func Test_Prepare(t *testing.T) {
	cases := makeCases(t)
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
	cases := makeCases(t)
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
			err := tcs.arg.Prepare(tcs.proc)
			convey.So(err, convey.ShouldBeNil)
			end, err := vm.Exec(tcs.arg, tcs.proc)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(end.Status == vm.ExecStop, convey.ShouldBeFalse)

			err = tcs.arg.Prepare(tcs.proc)
			convey.So(err, convey.ShouldBeNil)
			param.Fileparam.End = false

			end, err = vm.Exec(tcs.arg, tcs.proc)
			convey.So(err, convey.ShouldBeNil)
			convey.So(end.Status == vm.ExecStop, convey.ShouldBeTrue)

			err = tcs.arg.Prepare(tcs.proc)
			convey.So(err, convey.ShouldBeNil)
			param.Fileparam.End = true
			end, err = vm.Exec(tcs.arg, tcs.proc)
			convey.So(err, convey.ShouldBeNil)
			convey.So(end.Status == vm.ExecStop, convey.ShouldBeTrue)
		}
	})
}

func Test_Call2(t *testing.T) {
	cases2 := []externalTestCase{
		newTestCase(t, tree.CSV, ""),
		newTestCase(t, tree.JSONLINE, tree.OBJECT),
		newTestCase(t, tree.JSONLINE, tree.ARRAY),
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
			attrs := []plan.ExternAttr{{ColName: "col1", ColIndex: 0, ColFieldIndex: 0}, {ColName: "col2", ColIndex: 1, ColFieldIndex: 1}, {ColName: "col3", ColIndex: 2, ColFieldIndex: 2}}
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
	case3 := newTestCase(t, tree.CSV, "")

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
		attrs := []plan.ExternAttr{{ColName: "col1", ColIndex: 0, ColFieldIndex: 0}, {ColName: "col2", ColIndex: 1, ColFieldIndex: 1}, {ColName: "col3", ColIndex: 2, ColFieldIndex: 2}}
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

func TestReadDirSymlink(t *testing.T) {
	ctx := context.Background()

	root, err := os.MkdirTemp("", "*")
	assert.Nil(t, err)
	t.Logf("Created temp directory: %s", root)

	// Resolve root to its canonical path to handle symlinks (e.g., /var -> /private/var on macOS)
	originalRoot := root
	root, err = filepath.EvalSymlinks(root)
	assert.Nil(t, err)
	if originalRoot != root {
		t.Logf("Root path resolved from %s to %s", originalRoot, root)
	} else {
		t.Logf("Root path: %s (no symlink resolution needed)", root)
	}

	t.Cleanup(func() {
		t.Logf("Cleaning up temp directory: %s", root)
		_ = os.RemoveAll(root)
	})

	evChan := make(chan notify.EventInfo, 1024)
	err = notify.Watch(filepath.Join(root, "..."), evChan, notify.All)
	// File system watching may fail on some platforms or in CI environments, so we don't fail the test
	if err != nil {
		t.Logf("Warning: notify.Watch failed (non-fatal): %v", err)
		evChan = nil
	}
	if evChan != nil {
		defer notify.Stop(evChan)
	}

	testDone := make(chan struct{})
	fsLogDone := make(chan struct{})
	go func() {
		defer func() {
			close(fsLogDone)
		}()
		if evChan == nil {
			return
		}
		for {
			select {
			case ev := <-evChan:
				t.Logf("notify: %+v", ev)
			case <-testDone:
				time.Sleep(time.Second * 3) // wait event
				// drain
				for {
					select {
					case ev := <-evChan:
						t.Logf("notify: %+v", ev)
					default:
						return
					}
				}
			}
		}
	}()

	// create a/b/c
	err = os.MkdirAll(filepath.Join(root, "a", "b", "c"), 0755)
	assert.Nil(t, err)
	t.Logf("Created directory structure: %s", filepath.Join(root, "a", "b", "c"))

	// write a/b/c/foo
	fooPath := filepath.Join(root, "a", "b", "c", "foo")
	err = os.WriteFile(fooPath, []byte("abc"), 0644)
	assert.Nil(t, err)
	t.Logf("Created test file: %s", fooPath)

	// symlink a/b/d to a/b/c
	symlinkSrc := filepath.Join(root, "a", "b", "c")
	symlinkDst := filepath.Join(root, "a", "b", "d")
	err = os.Symlink(symlinkSrc, symlinkDst)
	assert.Nil(t, err)
	t.Logf("Created symlink: %s -> %s", symlinkDst, symlinkSrc)

	// sync root dir
	f, err := os.Open(root)
	assert.Nil(t, err)
	err = f.Sync()
	assert.Nil(t, err)
	err = f.Close()
	assert.Nil(t, err)

	// ensure symlink is valid - compare using EvalSymlinks on both sides
	symlinkPath := filepath.Join(root, "a", "b", "d")
	actual, err := filepath.EvalSymlinks(symlinkPath)
	assert.Nil(t, err)
	targetPath := filepath.Join(root, "a", "b", "c")
	expected, err := filepath.EvalSymlinks(targetPath)
	assert.Nil(t, err)
	assert.Equal(t, expected, actual)
	t.Logf("Symlink verification: %s resolves to %s (expected: %s)", symlinkPath, actual, expected)

	// read a/b/d/foo
	fooPathInB := filepath.Join(root, "a", "b", "d", "foo")
	t.Logf("Testing ReadDir with path through symlink: %s", fooPathInB)

	// Retry mechanism to handle race conditions in CI environments
	// where file system state may change during symlink resolution
	var files []string
	var maxRetries = 3
	for i := 0; i < maxRetries; i++ {
		if i > 0 {
			t.Logf("ReadDir retry attempt %d/%d for path: %s", i+1, maxRetries, fooPathInB)
		}

		// Check if path still exists before calling ReadDir
		if _, statErr := os.Stat(fooPathInB); statErr != nil {
			t.Logf("WARNING: os.Stat failed for %s: %v", fooPathInB, statErr)
		}

		files, _, err = plan2.ReadDir(&tree.ExternParam{
			ExParamConst: tree.ExParamConst{
				Filepath: fooPathInB,
			},
			ExParam: tree.ExParam{
				Ctx: ctx,
			},
		})
		if err == nil {
			t.Logf("ReadDir succeeded on attempt %d, found %d file(s)", i+1, len(files))
			break
		}
		if i < maxRetries-1 {
			t.Logf("ReadDir attempt %d failed with error: %v (type: %T), retrying in 100ms...", i+1, err, err)
			time.Sleep(100 * time.Millisecond)
		} else {
			t.Logf("ReadDir failed after %d attempts, last error: %v", maxRetries, err)
		}
	}
	assert.Nil(t, err)
	assert.Equal(t, 1, len(files))
	if len(files) > 0 {
		t.Logf("ReadDir returned file: %s", files[0])
	}
	assert.Equal(t, fooPathInB, files[0])

	path1 := filepath.Join(root, "a", "b", "..", "b", "c", "foo")
	t.Logf("Testing ReadDir with path containing '..': %s", path1)
	files1, _, err := plan2.ReadDir(&tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			Filepath: path1,
		},
		ExParam: tree.ExParam{
			Ctx: ctx,
		},
	})
	assert.Nil(t, err)
	pathWant1 := filepath.Join(root, "a", "b", "c", "foo")
	assert.Equal(t, 1, len(files1))
	if len(files1) > 0 {
		t.Logf("ReadDir with '..' returned: %s (expected: %s)", files1[0], pathWant1)
	}
	assert.Equal(t, pathWant1, files1[0])

	err = os.Remove(filepath.Join(root, "a", "b", "c", "foo"))
	assert.Nil(t, err)
	t.Logf("Test file removed, waiting for file system events to settle")

	close(testDone)
	<-fsLogDone
	t.Logf("TestReadDirSymlink completed successfully")
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
				proc:     testutil.NewProc(t),
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
				proc:     testutil.NewProc(t),
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
				proc:     testutil.NewProc(t),
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
	case1 := newTestCase(t, tree.CSV, "")
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
