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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/external"
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

	// The columnar formats are excluded in both roles: parquet redistributes
	// row groups and arrow record batches, each on its own path, and this rule
	// must not take either over.
	for _, format := range []string{tree.PARQUET, tree.ARROW} {
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
