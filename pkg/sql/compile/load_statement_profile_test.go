// Copyright 2026 Matrix Origin
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

package compile

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/external"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestInternalLoadStatementProfile(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, tc := range []struct {
		stmt *tree.Load
		want bool
	}{
		{&tree.Load{Local: true}, false},
		{&tree.Load{Local: true, DuplicateHandling: &tree.DuplicateKeyReplace{}}, false},
		{&tree.Load{DuplicateHandling: &tree.DuplicateKeyIgnore{}}, true},
		{&tree.Load{Local: true}, false},
		{&tree.Load{}, false},
	} {
		initInternalStatementProfile(proc, tc.stmt)
		stmtType, queryType, ignore := proc.GetStmtProfile().GetStatementRuntimeProfile()
		require.Equal(t, tc.stmt.GetStatementType(), stmtType)
		require.Equal(t, tree.QueryTypeDML, queryType)
		require.Equal(t, tc.want, ignore)
		param := &tree.ExternParam{ExParam: tree.ExParam{ExternType: int32(plan.ExternType_LOAD), Local: tc.stmt.Local}}
		require.Equal(t, !tc.want, effectiveExternalStrictMode(proc, param, true), "implicit LOCAL assignment policy must not relax the decoder")
		require.False(t, effectiveExternalStrictMode(proc, param, false))
	}
}

func TestLoadTimeReaderPlacement(t *testing.T) {
	newScan := func(t *testing.T, format string) (*Compile, *plan.Node, *tree.ExternParam) {
		c := NewMockCompile(t)
		c.addr, c.ncpu = "coordinator:6001", 8
		c.execType = plan2.ExecTypeAP_MULTICN
		c.cnList = engine.Nodes{{Addr: "old-worker:6001", Mcpu: 8}, {Addr: "worker:6001", Mcpu: 8}}
		c.anal = &AnalyzeModule{qry: &plan.Query{}}
		c.pn = &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{MaxDop: 2}}}
		n := &plan.Node{
			TableDef:   &plan.TableDef{Cols: []*plan.ColDef{{Name: "v", Typ: plan.Type{Id: int32(types.T_time), Scale: 6}}}},
			ExternScan: &plan.ExternScan{Type: int32(plan.ExternType_LOAD), TbColToDataCol: map[string]int32{"v": 0}},
		}
		param := csvLoadFanoutParam("")
		param.Ctx = fileservice.WithSQLFileAccess(c.proc.Ctx, "sys", "moadmin")
		param.ScanType, param.Format, param.Parallel = tree.S3, format, true
		return c, n, param
	}
	checkScopes := func(t *testing.T, c *Compile, ss []*Scope) {
		t.Helper()
		for _, scope := range ss {
			t.Cleanup(scope.release)
			require.Equal(t, c.addr, scope.NodeInfo.Addr, "the scan and assignment must not reach an old worker")
			require.LessOrEqual(t, scope.NodeInfo.Mcpu, 2)
		}
	}
	for _, format := range []string{tree.CSV, tree.JSONLINE, tree.PARQUET} {
		t.Run(format+" whole files", func(t *testing.T) {
			c, n, param := newScan(t, format)
			files := []string{"part-0", "part-1", "part-2"}
			ss, err := c.compileExternScanWholeFileFanout(n, param, files, []int64{10, 20, 30}, true, format == tree.PARQUET)
			require.NoError(t, err)
			checkScopes(t, c, ss)
			require.Len(t, ss, 2)
			seen := make(map[string]int)
			for _, scope := range ss {
				ext := scope.RootOp.(*external.External)
				for _, file := range ext.Es.FileList {
					seen[file]++
				}
			}
			require.Equal(t, map[string]int{"part-0": 1, "part-1": 1, "part-2": 1}, seen)
			if format == tree.CSV {
				input := &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
				target := &plan.Expr{Typ: n.TableDef.Cols[0].Typ, Expr: &plan.Expr_T{T: &plan.TargetType{}}}
				assignment, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "cast_assign", []*plan.Expr{input, target})
				require.NoError(t, err)
				ss = c.compileProjection(&plan.Node{ProjectList: []*plan.Expr{assignment}}, ss)
				for _, scope := range ss {
					ext := scope.RootOp.(*external.External)
					require.Equal(t, c.addr, scope.NodeInfo.Addr)
					require.Equal(t, "cast_assign", ext.ProjectList[0].GetF().Func.ObjName)
					require.Equal(t, int32(types.T_time), ext.ProjectList[0].Typ.Id)
					require.Equal(t, int32(types.T_varchar), ext.ProjectList[0].GetF().Args[0].Typ.Id)
				}
			}
		})
	}
	t.Run("Parquet row groups", func(t *testing.T) {
		c, n, param := newScan(t, tree.PARQUET)
		groups := []parquetRowGroupMeta{{fileIndex: 0, rowGroupIndex: 0, numRows: 1, bytes: 10}, {fileIndex: 0, rowGroupIndex: 1, numRows: 2, bytes: 20}, {fileIndex: 0, rowGroupIndex: 2, numRows: 3, bytes: 30}}
		ss, err := c.compileExternScanParquetRowGroupFanout(n, param, []string{"file.parquet"}, []int64{60}, groups, true)
		require.NoError(t, err)
		checkScopes(t, c, ss)
		require.Len(t, ss, 2)
		require.Equal(t, 2, c.parquetLoadFileFanoutDOP(n, param))
		seen := make(map[int32]int)
		for _, scope := range ss {
			for _, shard := range scope.RootOp.(*external.External).Es.ParquetRowGroupShards {
				for i := shard.RowGroupStart; i < shard.RowGroupEnd; i++ {
					seen[i]++
				}
			}
		}
		require.Equal(t, map[int32]int{0: 1, 1: 1, 2: 1}, seen)
	})
	t.Run("byte offsets", func(t *testing.T) {
		c, n, param := newScan(t, tree.CSV)
		param.ScanType, param.Tail.IgnoredLines = tree.INFILE, 0
		file := filepath.Clean(filepath.Join(GetFilePath(), "../../../test/distributed/resources/load_data/parallel_1.txt"))
		ss, err := c.compileExternScanParallelReadWrite(n, param, []string{file}, []int64{int64(colexec.WriteS3Threshold) * 3}, true)
		require.NoError(t, err)
		checkScopes(t, c, ss)
		require.Len(t, ss, 1)
		require.Equal(t, 2, ss[0].NodeInfo.Mcpu)
		// The fixture ends before the nominal split; worker placement stays
		// parallel while only the final whole-file range is assigned.
		require.Equal(t, []int64{0, -1}, ss[0].RootOp.(*external.External).Es.FileOffsetTotal[0].Offset)
	})
	for _, control := range []string{"non TIME", "foreign TIME", "Arrow", "omitted TIME", "user variable"} {
		t.Run(control+" control", func(t *testing.T) {
			c, n, param := newScan(t, tree.CSV)
			switch control {
			case "non TIME":
				n.TableDef.Cols[0].Typ.Id = int32(types.T_int32)
			case "foreign TIME":
				param.ExternType = int32(plan.ExternType_EXTERNAL_TB)
			case "Arrow":
				param.Format = tree.ARROW
			case "omitted TIME":
				delete(n.ExternScan.TbColToDataCol, "v")
			case "user variable":
				n.ExternScan.TbColToDataCol["v"] = -1
			}
			ss, err := c.compileExternScanWholeFileFanout(n, param, []string{"a", "b", "c", "d"}, []int64{1, 1, 1, 1}, true, false)
			require.NoError(t, err)
			require.Len(t, ss, 4)
			for _, scope := range ss {
				t.Cleanup(scope.release)
				require.NotEqual(t, c.addr, scope.NodeInfo.Addr)
			}
		})
	}
}

func TestNonStrictMultilineRangesFitWorkers(t *testing.T) {
	// A sparse prefix admits three workers without allocating/scanning a large
	// fixture. Only the 20-byte CSV suffix belongs to this scan.
	const data = "1,\"aaaaaaaaa\nb\"\n2,x\n"
	const size = int64(colexec.WriteS3Threshold) * 3
	start := size - int64(len(data))
	file := filepath.Join(t.TempDir(), "multiline.csv")
	func() {
		f, err := os.Create(file)
		require.NoError(t, err)
		defer f.Close()
		_, err = f.WriteAt([]byte(data), start)
		require.NoError(t, err)
	}()
	for _, tc := range []struct {
		name  string
		nodes engine.Nodes
	}{
		{"one stage", engine.Nodes{{Addr: "cn1:6001", Mcpu: 3}}},
		{"empty last stage", engine.Nodes{{Addr: "cn1:6001", Mcpu: 2}, {Addr: "cn2:6001", Mcpu: 1}}},
		{"partial last stage", engine.Nodes{{Addr: "cn1:6001", Mcpu: 1}, {Addr: "cn2:6001", Mcpu: 2}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := NewMockCompile(t)
			c.addr, c.cnList = "cn1:6001", tc.nodes
			c.execType = plan2.ExecTypeAP_MULTICN
			c.anal = &AnalyzeModule{qry: &plan.Query{}}
			n := &plan.Node{TableDef: &plan.TableDef{}, ExternScan: &plan.ExternScan{Type: int32(plan.ExternType_LOAD)}}
			param := &tree.ExternParam{
				ExParamConst: tree.ExParamConst{Filepath: file, FileStartOff: start, Format: tree.CSV, ScanType: tree.INFILE, CompressType: tree.NOCOMPRESS, Tail: &tree.TailParameter{}},
				ExParam:      tree.ExParam{Ctx: fileservice.WithSQLFileAccess(c.proc.Ctx, "sys", "moadmin"), Parallel: true, Strict: false, ExternType: int32(plan.ExternType_LOAD)},
			}
			ss, err := c.compileExternScanParallelReadWrite(n, param, []string{file}, []int64{size}, false)
			require.NoError(t, err)
			for _, scope := range ss {
				t.Cleanup(scope.release)
			}
			require.Len(t, ss, len(tc.nodes))
			var got []int64
			workers := 0
			for _, scope := range ss {
				workers += scope.NodeInfo.Mcpu
				got = append(got, scope.RootOp.(*external.External).Es.FileOffsetTotal[0].Offset...)
			}
			require.Equal(t, 3, workers)
			require.Equal(t, []int64{start, start + int64(len(data)-len("2,x\n")), start + int64(len(data)-len("2,x\n")), -1}, got,
				"two intact ranges must be assigned exactly once across three workers")
		})
	}
}
