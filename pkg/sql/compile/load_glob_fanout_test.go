// Copyright 2021 - 2026 Matrix Origin
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

package compile

import (
	"encoding/json"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/external"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/lockop"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func csvLoadFanoutParam(compress string) *tree.ExternParam {
	return &tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			ScanType:     tree.INFILE,
			Filepath:     "/load/part-*.csv",
			Format:       tree.CSV,
			CompressType: compress,
			Tail:         &tree.TailParameter{IgnoredLines: 1},
		},
		ExParam: tree.ExParam{
			ExternType:            int32(plan.ExternType_LOAD),
			ParallelLoadRequested: true,
		},
	}
}

func TestMultiFileFanoutEligible(t *testing.T) {
	param := csvLoadFanoutParam("")
	require.True(t, multiFileFanoutEligible(param, 2))

	// One file is not a fanout; the single-file paths stay reachable.
	require.False(t, multiFileFanoutEligible(param, 1))
	require.False(t, multiFileFanoutEligible(param, 0))

	// Compression is irrelevant to eligibility: a whole-file shard is exactly
	// what a compressed source needs, since it cannot be split by offset.
	require.True(t, multiFileFanoutEligible(csvLoadFanoutParam(tree.LZ4), 3))
	require.True(t, multiFileFanoutEligible(csvLoadFanoutParam(tree.GZIP), 3))

	jsonline := csvLoadFanoutParam("")
	jsonline.Format = tree.JSONLINE
	require.True(t, multiFileFanoutEligible(jsonline, 2))

	// Parquet is excluded in both roles: it redistributes row groups on its
	// own path, and this rule must not take that over.
	for _, format := range []string{tree.PARQUET} {
		columnarLoad := csvLoadFanoutParam("")
		columnarLoad.Format = format
		require.False(t, multiFileFanoutEligible(columnarLoad, 2), format)

		columnarExtTable := csvLoadFanoutParam("")
		columnarExtTable.Format = format
		columnarExtTable.ExternType = int32(plan.ExternType_EXTERNAL_TB)
		require.False(t, multiFileFanoutEligible(columnarExtTable, 2), format)
	}

	// Bind time clears Parallel under LoadParallelMinSize, which is why the
	// gate also reads ParallelLoadRequested -- many small files must still fan
	// out, since file count is what sets the degree of parallelism here.
	sizeCleared := csvLoadFanoutParam("")
	sizeCleared.Parallel = false
	require.True(t, multiFileFanoutEligible(sizeCleared, 8))

	// No `parallel 'true'` at all stays on the serial path.
	serial := csvLoadFanoutParam("")
	serial.ParallelLoadRequested = false
	require.False(t, multiFileFanoutEligible(serial, 8))

	// Parallel without the recorded request (a hand-built param, before bind
	// mirrors one into the other) still counts as asking for parallel.
	rawParallel := csvLoadFanoutParam("")
	rawParallel.ParallelLoadRequested = false
	rawParallel.Parallel = true
	require.True(t, multiFileFanoutEligible(rawParallel, 8))

	// An external table scan has no `parallel` option, so file count alone
	// decides -- with or without the LOAD-only flag set.
	extTable := csvLoadFanoutParam("")
	extTable.ExternType = int32(plan.ExternType_EXTERNAL_TB)
	extTable.ParallelLoadRequested = false
	require.True(t, multiFileFanoutEligible(extTable, 4))
	require.False(t, multiFileFanoutEligible(extTable, 1))

	// A query-result scan keeps its own zonemap reader and layout.
	resultScan := csvLoadFanoutParam("")
	resultScan.ExternType = int32(plan.ExternType_RESULT_SCAN)
	require.False(t, multiFileFanoutEligible(resultScan, 4))

	require.False(t, multiFileFanoutEligible(nil, 4))
}

func TestCompileExternScanMultiFileFanoutCsvLoad(t *testing.T) {
	testCompile := NewMockCompile(t)
	testCompile.cnList = engine.Nodes{{Addr: "cn1:6001", Mcpu: 2}, {Addr: "cn2:6001", Mcpu: 2}}
	testCompile.addr = "cn1:6001"
	testCompile.execType = plan2.ExecTypeAP_MULTICN
	testCompile.anal = &AnalyzeModule{qry: &plan.Query{}}

	param := csvLoadFanoutParam(tree.LZ4)
	param.Filepath = "/load/part-*.csv.lz4"
	n := &plan.Node{
		TableDef: &plan.TableDef{},
		ExternScan: &plan.ExternScan{
			Type:           int32(plan.ExternType_LOAD),
			TbColToDataCol: map[string]int32{},
		},
	}
	fileList := []string{
		"/load/part-000.csv.lz4",
		"/load/part-001.csv.lz4",
		"/load/part-002.csv.lz4",
	}
	fileSize := []int64{30, 10, 20}

	ss, err := testCompile.compileExternScanMultiFileFanout(n, param, fileList, fileSize, true)
	require.NoError(t, err)
	// One scope per file: compressed files are read one thread each, so the
	// parallelism is the file count.
	require.Len(t, ss, 3)
	require.Equal(t, "/load/part-*.csv.lz4", param.Filepath)

	seen := make(map[string]int64)
	for _, scope := range ss {
		require.NoError(t, checkScopeWithExpectedList(scope, []vm.OpType{vm.External}))
		require.Equal(t, 1, scope.NodeInfo.Mcpu)
		ext, ok := scope.RootOp.(*external.External)
		require.True(t, ok)

		// Whole files, never a byte range, and never a shared start offset.
		require.False(t, ext.Es.Extern.Parallel)
		require.Zero(t, ext.Es.Extern.FileStartOff)
		require.Len(t, ext.Es.FileOffsetTotal, len(ext.Es.FileList))
		for _, off := range ext.Es.FileOffsetTotal {
			require.Equal(t, []int64{0, -1}, off.Offset)
		}

		// IGNORE n LINES survives to the reader, which re-applies it per file.
		require.Equal(t, uint64(1), ext.Es.Extern.Tail.IgnoredLines)
		// LoadEmptyNumericAsZero still keys off the requested-parallel flag.
		require.True(t, ext.Es.LoadEmptyNumericAsZero)
		// ParallelLoad (varchar staging vectors) belongs to LOCAL loads only.
		require.False(t, ext.Es.ParallelLoad)

		require.NotEmpty(t, ext.Es.FileList)
		for i, f := range ext.Es.FileList {
			require.NotContains(t, seen, f)
			seen[f] = ext.Es.FileSize[i]
		}
	}
	require.Equal(t, map[string]int64{
		"/load/part-000.csv.lz4": 30,
		"/load/part-001.csv.lz4": 10,
		"/load/part-002.csv.lz4": 20,
	}, seen)
}

func TestCompileExternScanMultiFileFanoutSingleShardFallsBackToSerial(t *testing.T) {
	testCompile := NewMockCompile(t)
	testCompile.cnList = engine.Nodes{{Addr: "cn1:6001", Mcpu: 1}}
	testCompile.addr = "cn1:6001"
	testCompile.ncpu = 1
	testCompile.execType = plan2.ExecTypeTP
	testCompile.anal = &AnalyzeModule{qry: &plan.Query{}}

	param := csvLoadFanoutParam("")
	n := &plan.Node{
		TableDef: &plan.TableDef{},
		ExternScan: &plan.ExternScan{
			Type:           int32(plan.ExternType_LOAD),
			TbColToDataCol: map[string]int32{},
		},
	}
	fileList := []string{"/load/part-000.csv", "/load/part-001.csv"}
	fileSize := []int64{10, 20}

	ss, err := testCompile.compileExternScanMultiFileFanout(n, param, fileList, fileSize, true)
	require.NoError(t, err)
	require.Len(t, ss, 1)

	ext, ok := ss[0].RootOp.(*external.External)
	require.True(t, ok)
	// Both files land on the one scope and are still read whole, in order.
	require.Equal(t, fileList, ext.Es.FileList)
	require.False(t, ext.Es.Extern.Parallel)
	require.Equal(t, uint64(1), ext.Es.Extern.Tail.IgnoredLines)
}

// A multi-file LOAD fanout hands a TP-classified plan one scope per file shard.
// Every IsSingleScope caller acts on a true answer by keeping ss[0] and dropping
// the rest, so it must not claim "single" for a longer list.
func TestIsSingleScopeRejectsMultiScopeTpPlan(t *testing.T) {
	testCompile := NewMockCompile(t)

	single := []*Scope{{NodeInfo: engine.Node{Addr: "cn1:6001", Mcpu: 1}}}
	multi := []*Scope{
		{NodeInfo: engine.Node{Addr: "cn1:6001", Mcpu: 1}},
		{NodeInfo: engine.Node{Addr: "cn1:6001", Mcpu: 1}},
	}
	wide := []*Scope{{NodeInfo: engine.Node{Addr: "cn1:6001", Mcpu: 4}}}

	testCompile.execType = plan2.ExecTypeTP
	require.True(t, testCompile.IsTpQuery())
	require.True(t, testCompile.IsSingleScope(single))
	// TP still answers true for one wide scope; only the list length is new.
	require.True(t, testCompile.IsSingleScope(wide))
	require.False(t, testCompile.IsSingleScope(multi))

	testCompile.execType = plan2.ExecTypeAP_ONECN
	require.True(t, testCompile.IsSingleScope(single))
	require.False(t, testCompile.IsSingleScope(wide))
	require.False(t, testCompile.IsSingleScope(multi))
}

// An external table scan over a pattern distributes whole files the same way a
// LOAD does -- there is no `parallel` option to ask for it, so file count alone
// decides, and nothing is read by byte range.
func TestCompileExternScanMultiFileFanoutExternalTable(t *testing.T) {
	testCompile := NewMockCompile(t)
	testCompile.cnList = engine.Nodes{{Addr: "cn1:6001", Mcpu: 2}, {Addr: "cn2:6001", Mcpu: 2}}
	testCompile.addr = "cn1:6001"
	testCompile.execType = plan2.ExecTypeAP_MULTICN
	testCompile.anal = &AnalyzeModule{qry: &plan.Query{}}

	param := &tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			ScanType:     tree.INFILE,
			Filepath:     "/ext/part-*.csv.lz4",
			Format:       tree.CSV,
			CompressType: tree.LZ4,
			Tail:         &tree.TailParameter{},
		},
		ExParam: tree.ExParam{
			ExternType: int32(plan.ExternType_EXTERNAL_TB),
		},
	}
	// No parallel flag of any kind, unlike a LOAD.
	require.False(t, param.Parallel)
	require.False(t, param.ParallelLoadRequested)
	require.True(t, multiFileFanoutEligible(param, 3))

	n := &plan.Node{
		TableDef: &plan.TableDef{},
		ExternScan: &plan.ExternScan{
			Type:           int32(plan.ExternType_EXTERNAL_TB),
			TbColToDataCol: map[string]int32{},
		},
	}
	fileList := []string{"/ext/part-000.csv.lz4", "/ext/part-001.csv.lz4", "/ext/part-002.csv.lz4"}
	fileSize := []int64{30, 10, 20}

	ss, err := testCompile.compileExternScanMultiFileFanout(n, param, fileList, fileSize, true)
	require.NoError(t, err)
	require.Len(t, ss, 3)

	seen := make(map[string]bool)
	for _, scope := range ss {
		require.NoError(t, checkScopeWithExpectedList(scope, []vm.OpType{vm.External}))
		require.Equal(t, 1, scope.NodeInfo.Mcpu)
		ext, ok := scope.RootOp.(*external.External)
		require.True(t, ok)
		require.False(t, ext.Es.Extern.Parallel)
		for _, off := range ext.Es.FileOffsetTotal {
			require.Equal(t, []int64{0, -1}, off.Offset)
		}
		for _, f := range ext.Es.FileList {
			require.NotContains(t, seen, f)
			seen[f] = true
		}
	}
	require.Len(t, seen, len(fileList))
}

// The byte-offset split is reserved for one file; reaching it with more is a
// routing bug, not a slow plan, so it fails loudly instead of silently
// splitting every file at the first file's boundaries.
func TestCompileExternScanParallelReadWriteRejectsMultipleFiles(t *testing.T) {
	testCompile := NewMockCompile(t)
	testCompile.addr = "cn1:6001"
	testCompile.anal = &AnalyzeModule{qry: &plan.Query{}}

	param := csvLoadFanoutParam("")
	param.Parallel = true
	n := &plan.Node{
		TableDef:   &plan.TableDef{},
		ExternScan: &plan.ExternScan{Type: int32(plan.ExternType_LOAD)},
	}

	_, err := testCompile.compileExternScanParallelReadWrite(
		n, param,
		[]string{"/load/a.csv", "/load/b.csv"},
		[]int64{100, 100},
		true,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "reserved for a single file")
}

// A multi-file LOAD fanout hands a TP-classified plan one scope per file shard.
// The lock operator is attached to ss[0] alone, so compileLock must merge first
// or rows from every other shard reach the writer without their row locks --
// for a table whose only uniqueness is a UNIQUE key, those are the locks that
// serialize concurrent writers.
func TestCompileLockMergesMultiScopeTpPlan(t *testing.T) {
	newCompile := func() *Compile {
		c := NewMockCompile(t)
		c.execType = plan2.ExecTypeTP
		c.pn = &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{
			StmtType: plan.Query_INSERT,
			LoadTag:  true,
			Steps:    []int32{0},
		}}}
		c.lockTables = make(map[uint64]*plan.LockTarget)
		c.anal = &AnalyzeModule{qry: &plan.Query{}}
		txnOp := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
		txnOp.EXPECT().Txn().Return(txn.TxnMeta{Mode: txn.TxnMode_Pessimistic}).AnyTimes()
		c.proc.Base.TxnOperator = txnOp
		return c
	}
	newNode := func() *plan.Node {
		return &plan.Node{LockTargets: []*plan.LockTarget{{
			TableId:       42,
			PrimaryColTyp: plan.Type{Id: int32(types.T_int64)},
		}}}
	}
	c := newCompile()
	shard := func() *Scope {
		return &Scope{
			Magic:    Normal,
			NodeInfo: engine.Node{Addr: "cn1:6001", Mcpu: 1},
			Proc:     c.proc.NewNoContextChildProc(0),
		}
	}

	require.True(t, c.IsTpQuery())
	shards := []*Scope{shard(), shard(), shard()}
	got, err := c.compileLock(newNode(), shards)
	require.NoError(t, err)
	require.Len(t, got, 1)
	_, ok := got[0].RootOp.(*lockop.LockOp)
	require.True(t, ok, "the lock must sit above the merge of every shard")
	require.Len(t, got[0].PreScopes, len(shards))
	for _, s := range shards {
		_, isLock := s.RootOp.(*lockop.LockOp)
		require.False(t, isLock, "no shard may carry a private lock")
	}

	// One scope under TP keeps the unmerged shape.
	single := []*Scope{shard()}
	got, err = newCompile().compileLock(newNode(), single)
	require.NoError(t, err)
	require.Equal(t, single, got)
	_, ok = got[0].RootOp.(*lockop.LockOp)
	require.True(t, ok)
}

// A remote whole-file shard is rebuilt on another CN from CreateSql alone.  For
// an external table that string is the stored DDL, which does not carry the
// ExternType taken from the plan: decoded as is, the reader would apply LOAD's
// exact column-count rule and reject rows an external table accepts.  The
// shard must ship the compile-time normalized param instead, including S3
// settings resolved from a stage, which InitS3Param cannot rebuild from Option.
func TestCompileExternScanMultiFileFanoutRemoteKeepsNormalizedParam(t *testing.T) {
	testCompile := NewMockCompile(t)
	testCompile.cnList = engine.Nodes{{Addr: "cn1:6001", Mcpu: 2}, {Addr: "cn2:6001", Mcpu: 2}}
	testCompile.addr = "cn1:6001"
	testCompile.execType = plan2.ExecTypeAP_MULTICN
	testCompile.anal = &AnalyzeModule{qry: &plan.Query{}}

	// The stored DDL: no ExternType, and a stage-backed source has no S3
	// options for InitS3Param to rebuild credentials from.
	ddl, err := json.Marshal(&tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			ScanType: tree.INFILE,
			Filepath: "stage://s1/part-*.csv",
			Format:   tree.CSV,
			Tail:     &tree.TailParameter{},
		},
	})
	require.NoError(t, err)

	// What getExternParam leaves after InitInfileOrStageParam resolved the stage.
	param := &tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			ScanType: tree.S3,
			Filepath: "prefix/part-*.csv",
			Format:   tree.CSV,
			// A stale credential and filepath must be replaced by the resolved
			// values; unrelated keys survive.
			Option: []string{"bucket", "stale", "filepath", "stale/*.csv", "format", "csv"},
			Tail: &tree.TailParameter{
				Assignments: tree.UpdateExprs{&tree.UpdateExpr{}},
			},
		},
		ExParam: tree.ExParam{
			ExternType: int32(plan.ExternType_EXTERNAL_TB),
			S3Param:    &tree.S3Parameter{Bucket: "bkt", Region: "r1", APIKey: "k", APISecret: "s"},
			Ctx:        testCompile.proc.Ctx,
		},
	}
	n := &plan.Node{
		TableDef: &plan.TableDef{
			Createsql: string(ddl),
			Cols: []*plan.ColDef{{
				Name: "a",
				Typ:  plan.Type{Id: int32(types.T_int64)},
			}},
		},
		ExternScan: &plan.ExternScan{
			Type:           int32(plan.ExternType_EXTERNAL_TB),
			TbColToDataCol: map[string]int32{"a": 0},
		},
	}
	fileList := []string{"prefix/part-0.csv", "prefix/part-1.csv", "prefix/part-2.csv"}
	fileSize := []int64{10, 10, 10}

	ss, err := testCompile.compileExternScanMultiFileFanout(n, param, fileList, fileSize, true)
	require.NoError(t, err)
	require.Greater(t, len(ss), 1)
	// The caller's param is not mutated by serializing a shard.
	require.NotNil(t, param.Ctx)
	require.Len(t, param.Tail.Assignments, 1)

	for _, scope := range ss {
		ext, ok := scope.RootOp.(*external.External)
		require.True(t, ok)
		require.NotEqual(t, string(ddl), ext.Es.CreateSql)

		// A CN from before this change runs exactly this on the payload:
		// unmarshal into the ExternParam it knows, then InitS3Param from
		// Option.  Unknown JSON fields would be dropped silently, so the
		// resolved settings must reach it through fields and options it reads.
		legacy := &tree.ExternParam{}
		require.NoError(t, json.Unmarshal([]byte(ext.Es.CreateSql), legacy))
		require.NoError(t, plan2.InitS3Param(legacy))
		require.Equal(t, int32(plan.ExternType_EXTERNAL_TB), legacy.ExternType)
		require.Equal(t, tree.S3, legacy.ScanType)
		require.Equal(t, "prefix/part-*.csv", legacy.Filepath)
		require.Equal(t, *param.S3Param, *legacy.S3Param)

		_, in, err := convertToPipelineInstruction(ext, testCompile.proc, &scopeContext{}, 1)
		require.NoError(t, err)
		op, err := convertToVmOperator(in, &scopeContext{}, nil)
		require.NoError(t, err)
		decoded := op.(*external.External)
		require.Nil(t, decoded.Es.Extern, "the codec carries CreateSql, not Extern")

		require.NoError(t, decoded.Prepare(testCompile.proc))
		got := decoded.Es.Extern
		require.Equal(t, int32(plan.ExternType_EXTERNAL_TB), got.ExternType)
		require.Equal(t, tree.S3, got.ScanType)
		require.Equal(t, "prefix/part-*.csv", got.Filepath)
		require.Equal(t, *param.S3Param, *got.S3Param)
		require.False(t, got.Parallel)
		require.NotNil(t, got.FileService)
		decoded.Free(testCompile.proc, false, nil)
	}
}

func TestS3ParamOptionsRoundTripsThroughInitS3Param(t *testing.T) {
	s3 := &tree.S3Parameter{
		Endpoint: "e", Region: "r", APIKey: "k", APISecret: "s", Bucket: "b",
		Provider: "minio", RoleArn: "arn", ExternalId: "x",
	}
	option := s3ParamOptions([]string{"Bucket", "old", "compression", "lz4"}, s3)
	param := &tree.ExternParam{ExParamConst: tree.ExParamConst{Option: option, Filepath: "p"}}
	require.NoError(t, plan2.InitS3Param(param))
	require.Equal(t, *s3, *param.S3Param)
	require.Equal(t, "lz4", param.CompressType)
	require.Equal(t, "p", param.Filepath)
}

// max_dop caps how many readers a file fanout places on one CN, the same
// per-CN cap CalcQueryDOP receives.  With max_dop=1 a multi-file scan has one
// reader per CN, which on a single CN is the serial path.
func TestFileFanoutHonorsMaxDop(t *testing.T) {
	newCompile := func(maxDop int64) *Compile {
		c := NewMockCompile(t)
		c.addr = "cn1:6001"
		c.ncpu = 8
		c.execType = plan2.ExecTypeAP_ONECN
		c.anal = &AnalyzeModule{qry: &plan.Query{}}
		c.pn = &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{MaxDop: maxDop}}}
		return c
	}
	extTable := &tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			ScanType: tree.INFILE,
			Filepath: "/ext/part-*.csv",
			Format:   tree.CSV,
			Tail:     &tree.TailParameter{},
		},
		ExParam: tree.ExParam{ExternType: int32(plan.ExternType_EXTERNAL_TB)},
	}

	require.Len(t, newCompile(0).getHiveFileFanoutNodes(extTable, 6), 6)
	require.Len(t, newCompile(3).getHiveFileFanoutNodes(extTable, 6), 3)
	require.Len(t, newCompile(1).getHiveFileFanoutNodes(extTable, 6), 1)

	n := &plan.Node{
		TableDef: &plan.TableDef{},
		ExternScan: &plan.ExternScan{
			Type:           int32(plan.ExternType_EXTERNAL_TB),
			TbColToDataCol: map[string]int32{},
		},
	}
	fileList := []string{"/ext/part-0.csv", "/ext/part-1.csv", "/ext/part-2.csv", "/ext/part-3.csv"}
	fileSize := []int64{10, 10, 10, 10}
	ss, err := newCompile(1).compileExternScanMultiFileFanout(n, extTable, fileList, fileSize, true)
	require.NoError(t, err)
	require.Len(t, ss, 1, "max_dop=1 must not open concurrent readers")
	ext, ok := ss[0].RootOp.(*external.External)
	require.True(t, ok)
	require.Equal(t, fileList, ext.Es.FileList)

	// S3 workers: each CN is capped as well.
	s3 := *extTable
	s3.ScanType = tree.S3
	c := newCompile(2)
	c.execType = plan2.ExecTypeAP_MULTICN
	c.cnList = engine.Nodes{{Addr: "cn1:6001", Mcpu: 8}, {Addr: "cn2:6001", Mcpu: 8}}
	nodes := c.getHiveFileFanoutNodes(&s3, 16)
	perCN := make(map[string]int)
	for _, node := range nodes {
		perCN[node.Addr]++
	}
	require.Equal(t, map[string]int{"cn1:6001": 2, "cn2:6001": 2}, perCN)
}
