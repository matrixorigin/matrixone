// Copyright 2026 Matrix Origin
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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/external"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRunFilePathFilters_LeftoverContainsUnconsumed locks the compile-layer
// contract that motivates the fpFilters → rowFilters append in
// getHivePartitionFileList. If runFilePathFilters is modified (or deleted) to
// no longer surface FilterFileList's unconsumed predicates as its leftover
// return value, queries like
//
//	WHERE __mo_filepath LIKE '%x%' OR false
//
// will silently skip their predicate and return wrong rows. This test fires
// immediately in that scenario.
func TestRunFilePathFilters_LeftoverContainsUnconsumed(t *testing.T) {
	proc := testutil.NewProc(t)

	td := &plan.TableDef{
		Cols: []*plan.ColDef{
			{Name: "year"},
			{Name: catalog.ExternalFilePath},
		},
	}
	orExpr := &plan.Expr{
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: int64(function.OR) << 32, ObjName: "or"},
			Args: []*plan.Expr{
				{
					Expr: &plan.Expr_F{F: &plan.Function{
						Func: &plan.ObjectRef{Obj: function.EqualFunctionEncodedID},
						Args: []*plan.Expr{
							{Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 1, Name: catalog.ExternalFilePath}}},
							{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "x"}}}},
						},
					}},
				},
				{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Bval{Bval: false}}}},
			},
		}},
	}

	fileList := []string{"/warehouse/data/year=2024/f.parquet"}
	fileSize := []int64{100}

	outFileList, outFileSize, leftover, err := runFilePathFilters(
		proc.Ctx, proc, td, []*plan.Expr{orExpr}, fileList, fileSize)
	require.NoError(t, err)

	assert.Equal(t, fileList, outFileList,
		"FilterFileList short-circuits when judgeContainColname rejects all fpFilters")
	assert.Equal(t, fileSize, outFileSize)
	require.Equal(t, 1, len(leftover),
		"unconsumed OR(filepath, literal) must be returned in leftover so the caller can "+
			"append it to rowFilters; losing it silently drops the predicate at runtime")
	assert.Same(t, orExpr, leftover[0])
}

// TestToLowerSet covers the tiny helper feeding partColSet.
func TestToLowerSet(t *testing.T) {
	got := toLowerSet([]string{"Year", "MONTH", "day"})
	assert.True(t, got["year"])
	assert.True(t, got["month"])
	assert.True(t, got["day"])
	assert.False(t, got["Year"])
	assert.Equal(t, 3, len(got))

	// Empty input.
	got = toLowerSet(nil)
	assert.Equal(t, 0, len(got))
}

// TestRunFilePathFilters_AllConsumed exercises the consumed-path of
// runFilePathFilters where FilterFileList keeps the filter and leftover is
// empty — distinct from the unconsumed regression above.
func TestRunFilePathFilters_NoFilters(t *testing.T) {
	proc := testutil.NewProc(t)
	td := &plan.TableDef{Cols: []*plan.ColDef{{Name: catalog.ExternalFilePath}}}
	// Empty fpFilters → FilterFileList short-circuits, returns fileList unchanged.
	fileList := []string{"/a.parquet"}
	fileSize := []int64{10}
	out, outSz, leftover, err := runFilePathFilters(proc.Ctx, proc, td, nil, fileList, fileSize)
	require.NoError(t, err)
	assert.Equal(t, fileList, out)
	assert.Equal(t, fileSize, outSz)
	assert.Empty(t, leftover)
}

func TestUpdateHivePartitionScanStats(t *testing.T) {
	node := &plan.Node{Stats: &plan.Stats{Outcnt: 99, TableCnt: 99}}
	result := &external.PartitionDiscoveryResult{
		PartitionCount:    3,
		PrunedCount:       2,
		ListCalls:         4,
		DiscoveryDuration: 1,
	}

	updateHivePartitionScanStats(node, "/data", result, []int64{100, 200})

	require.NotNil(t, node.Stats)
	assert.Equal(t, int32(2), node.Stats.BlockNum)
	assert.Equal(t, float64(300), node.Stats.Cost)
	assert.Equal(t, float64(99), node.Stats.Outcnt, "compile-time physical stats must not rewrite logical row estimate")
	assert.Equal(t, float64(99), node.Stats.TableCnt)
}

func TestUpdateHivePartitionScanStatsInitializesDefaults(t *testing.T) {
	node := &plan.Node{}
	result := &external.PartitionDiscoveryResult{
		PartitionCount:     4,
		PrunedCount:        1,
		ListCalls:          3,
		CacheHits:          2,
		CacheMisses:        1,
		PrunedFiles:        7,
		PrunedBytes:        11,
		DirectPrefixHits:   5,
		DirectPrefixMisses: 6,
		DiscoveryDuration:  time.Millisecond,
	}

	updateHivePartitionScanStats(node, "/warehouse", result, []int64{4, 5, 6})

	require.NotNil(t, node.Stats)
	assert.Equal(t, int32(3), node.Stats.BlockNum)
	assert.Equal(t, float64(15), node.Stats.Cost)
	assert.Equal(t, float64(3), node.Stats.Outcnt)
	assert.Equal(t, float64(3), node.Stats.TableCnt)
}

func TestGetHivePartitionDiscoverOptionsFromSessionVars(t *testing.T) {
	proc := testutil.NewProc(t)
	proc.Ctx = defines.AttachAccountId(proc.Ctx, 42)
	proc.SetResolveVariableFunc(func(varName string, isSystemVar, isGlobalVar bool) (interface{}, error) {
		switch varName {
		case hivePartitionCacheTTLVar:
			return int64(5), nil
		case hivePartitionCacheMaxEntriesVar:
			return int64(77), nil
		case hivePartitionCacheMaxBytesVar:
			return int64(4096), nil
		case hivePartitionListConcurrencyVar:
			return int64(3), nil
		default:
			return int64(0), nil
		}
	})
	c := &Compile{proc: proc}
	param := &tree.ExternParam{
		ExParamConst: tree.ExParamConst{Filepath: "/data", ScanType: tree.INFILE},
	}

	opts, err := c.getHivePartitionDiscoverOptions(param)
	require.NoError(t, err)
	require.NotNil(t, opts)
	assert.Equal(t, 5*time.Second, opts.CacheTTL)
	assert.Equal(t, 77, opts.CacheMaxEntries)
	assert.Equal(t, int64(4096), opts.CacheMaxBytes)
	assert.Equal(t, 3, opts.ListConcurrency)
	assert.Contains(t, opts.CacheKeyPrefix, "account=42")
	assert.Contains(t, opts.CacheKeyPrefix, "base=/data")
}

func TestGetHivePartitionDiscoverOptionsNoopAndError(t *testing.T) {
	proc := testutil.NewProc(t)
	c := &Compile{proc: proc}
	opts, err := c.getHivePartitionDiscoverOptions(&tree.ExternParam{})
	require.NoError(t, err)
	assert.Nil(t, opts)

	proc.SetResolveVariableFunc(func(varName string, isSystemVar, isGlobalVar bool) (interface{}, error) {
		return int64(0), nil
	})
	opts, err = c.getHivePartitionDiscoverOptions(&tree.ExternParam{})
	require.NoError(t, err)
	assert.Nil(t, opts)

	proc.SetResolveVariableFunc(func(varName string, isSystemVar, isGlobalVar bool) (interface{}, error) {
		if varName == hivePartitionCacheMaxEntriesVar {
			return nil, moerr.NewInternalErrorNoCtx("boom")
		}
		return int64(0), nil
	})
	_, err = c.getHivePartitionDiscoverOptions(&tree.ExternParam{})
	require.Error(t, err)
}

func TestResolveHivePartitionIntVarCoverage(t *testing.T) {
	cases := []struct {
		name string
		val  interface{}
		want int64
	}{
		{name: "nil", val: nil, want: 0},
		{name: "int", val: int(1), want: 1},
		{name: "int8", val: int8(2), want: 2},
		{name: "int16", val: int16(3), want: 3},
		{name: "int32", val: int32(4), want: 4},
		{name: "int64", val: int64(5), want: 5},
		{name: "uint", val: uint(6), want: 6},
		{name: "uint8", val: uint8(7), want: 7},
		{name: "uint16", val: uint16(8), want: 8},
		{name: "uint32", val: uint32(9), want: 9},
		{name: "uint64", val: uint64(10), want: 10},
		{name: "string", val: " 11 ", want: 11},
		{name: "empty string", val: " \t ", want: 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveHivePartitionIntVar(
				func(varName string, isSystemVar, isGlobalVar bool) (interface{}, error) {
					assert.Equal(t, "x", varName)
					assert.True(t, isSystemVar)
					assert.False(t, isGlobalVar)
					return tc.val, nil
				},
				"x",
			)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}

	_, err := resolveHivePartitionIntVar(func(string, bool, bool) (interface{}, error) {
		return uint64(^uint64(0)), nil
	}, "x")
	require.Error(t, err)

	_, err = resolveHivePartitionIntVar(func(string, bool, bool) (interface{}, error) {
		return "not-an-int", nil
	}, "x")
	require.Error(t, err)

	_, err = resolveHivePartitionIntVar(func(string, bool, bool) (interface{}, error) {
		return struct{}{}, nil
	}, "x")
	require.Error(t, err)
}

func TestGetHivePartitionFileListCoverageHack(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "year=2024"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "year=2024", "a.parquet"), []byte("x"), 0644))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "year=2025"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "year=2025", "b.parquet"), []byte("xy"), 0644))
	selectedFile := filepath.Join(dir, "year=2024", "a.parquet")

	proc := testutil.NewProc(t)
	proc.SetResolveVariableFunc(func(varName string, isSystemVar, isGlobalVar bool) (interface{}, error) {
		if varName == hivePartitionListConcurrencyVar {
			return int64(2), nil
		}
		return int64(0), nil
	})
	c := &Compile{proc: proc}
	node := &plan.Node{
		TableDef: &plan.TableDef{Cols: []*plan.ColDef{
			{Name: "id", Typ: plan.Type{Id: int32(types.T_int32)}},
			{Name: "year", Typ: plan.Type{Id: int32(types.T_int32)}},
			{Name: catalog.ExternalFilePath, Typ: plan.Type{Id: int32(types.T_varchar)}},
		}},
		FilterList: []*plan.Expr{{
			Typ: plan.Type{Id: int32(types.T_bool)},
			Expr: &plan.Expr_F{F: &plan.Function{
				Func: &plan.ObjectRef{Obj: function.EqualFunctionEncodedID},
				Args: []*plan.Expr{
					{
						Typ:  plan.Type{Id: int32(types.T_varchar)},
						Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 2, Name: catalog.ExternalFilePath}},
					},
					{
						Typ:  plan.Type{Id: int32(types.T_varchar)},
						Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: selectedFile}}},
					},
				},
			}},
		}},
		Stats: &plan.Stats{},
	}
	param := &tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			Filepath:              dir,
			ScanType:              tree.INFILE,
			HivePartitioning:      true,
			HivePartitionCols:     []string{"year"},
			HivePartitionColTypes: []tree.HivePartColType{{Id: int32(types.T_int32), NullAbility: true}},
		},
	}

	files, sizes, err := c.getHivePartitionFileList(node, param)
	require.NoError(t, err)
	assert.Equal(t, []string{selectedFile}, files)
	assert.Equal(t, []int64{1}, sizes)
	assert.Empty(t, node.FilterList)
	require.NotNil(t, node.Stats)
	assert.Equal(t, int32(1), node.Stats.BlockNum)
	assert.Equal(t, float64(1), node.Stats.Cost)
}
