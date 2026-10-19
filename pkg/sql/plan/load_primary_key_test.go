// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/config"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestLoadPrimaryKeyValidationPlan(t *testing.T) {
	for _, tc := range []struct {
		name, table, data, mode string
		want                    int
	}{
		{"primary", "nation", "1,n,1,c", "", 1},
		{"primary and unique", "dept", "1,d,l", "", 1},
		{"ignore unchanged", "nation", "1,n,1,c", "ignore", 0},
		{"replace unchanged", "nation", "1,n,1,c", "replace", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(true, newPlanTestProcess(t))
			pn, err := runOneStmt(mock, t, fmt.Sprintf("load data inline format='csv', data='%s' %s into table %s fields terminated by ','", tc.data, tc.mode, tc.table))
			require.NoError(t, err)
			require.True(t, pn.GetQuery().LoadTag)
			dedups := 0
			for _, n := range pn.GetQuery().Nodes {
				if n.NodeType == planpb.Node_JOIN && n.JoinType == planpb.Node_DEDUP {
					dedups++
					require.Equal(t, planpb.Node_FAIL, n.OnDuplicateAction)
					require.False(t, n.DedupInputKeysUnique, "external input cannot prove primary-key uniqueness")
				}
			}
			require.Equal(t, tc.want, dedups)
			if tc.want > 0 {
				requireLoadValidationIndependentOfWrites(t, pn.GetQuery())
			}
			if tc.table == "dept" {
				maintained := 0
				for _, n := range pn.GetQuery().Nodes {
					if n.NodeType == planpb.Node_MULTI_UPDATE {
						maintained += len(n.UpdateCtxList)
					}
				}
				require.Equal(t, 1+len(mock.ctxt.tables[tc.table].Indexes), maintained, "LOAD must retain base and index writes")
			}
		})
	}
}

func TestLoadPrimaryKeyValidationJoinOrientation(t *testing.T) {
	for _, tc := range []struct {
		name     string
		spillMem int64
		right    bool
	}{
		{"right map fits", 512 << 20, true},
		{"left spill fallback", 1 << 20, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := newPlanTestProcess(t)
			proc.Base.Lim.Size = 2 << 30 // Budget the estimated keys, without allocating rows.
			mock := NewMockOptimizer(true, proc)
			pn, err := runOneStmt(mock, t, "load data inline format='csv', data='1,d,l' into table dept fields terminated by ','")
			require.NoError(t, err)
			qry := pn.GetQuery()
			var check *planpb.Node
			for _, n := range qry.Nodes {
				if n.NodeType == planpb.Node_JOIN && n.JoinType == planpb.Node_DEDUP {
					require.Nil(t, check)
					check = n
				}
			}
			require.NotNil(t, check)
			// Restore logical roles before rerunning the real orientation passes.
			if check.IsRightJoin {
				check.Children[0], check.Children[1] = check.Children[1], check.Children[0]
				check.IsRightJoin = false
			}
			// Runtime filters are generated after these passes. Remove that later
			// physical dependency before changing the costing inputs and replaying.
			check.RuntimeFilterBuildList = nil
			check.RuntimeFilterProbeList = nil
			target, input := check.Children[0], check.Children[1]
			require.Equal(t, planpb.Node_TABLE_SCAN, qry.Nodes[target].NodeType)
			require.Equal(t, planpb.Node_SINK_SCAN, qry.Nodes[input].NodeType)
			qry.Nodes[target].Stats = &planpb.Stats{Outcnt: 100, Rowsize: 8}
			qry.Nodes[input].Stats = &planpb.Stats{Outcnt: 1 << 20, Rowsize: 1 << 10}
			builder := NewQueryBuilder(planpb.Query_INSERT, &mock.ctxt, false, true)
			builder.qry = qry
			builder.joinSpillMem = tc.spillMem
			builder.determineBuildAndProbeSide(check.NodeId, false)
			require.True(t, check.IsRightJoin, "costing chooses the smaller existing target")
			builder.disableMemoryUnsafeRightDedup(check.NodeId)
			require.Equal(t, tc.right, check.IsRightJoin)
			builder.swapJoinChildren(check.NodeId)
			want := []int32{target, input}
			if tc.right {
				want = []int32{input, target}
			}
			require.Equal(t, want, check.Children)
			requireLoadValidationIndependentOfWrites(t, qry)
		})
	}
}

func TestLoadPrimaryKeyValidationFallback(t *testing.T) {
	old := config.CNPrimaryCheck.Load()
	t.Cleanup(func() { config.CNPrimaryCheck.Store(old) })
	for _, check := range []bool{false, true} {
		t.Run(fmt.Sprintf("CNPrimaryCheck=%t", check), func(t *testing.T) {
			config.CNPrimaryCheck.Store(check)
			mock := NewMockOptimizer(true, newPlanTestProcess(t))
			stmt, err := mysql.ParseOne(t.Context(), "load data inline format='csv', data='1,n,1,c' into table nation fields terminated by ','", 1)
			require.NoError(t, err)
			defer stmt.Free()
			pn, err := buildLoad(stmt.(*tree.Load), &mock.ctxt, false)
			require.NoError(t, err)
			fuzzy := 0
			for _, n := range pn.GetQuery().Nodes {
				if n.NodeType == planpb.Node_FUZZY_FILTER {
					fuzzy++
				}
			}
			require.Equal(t, 1, fuzzy, "legacy LOAD must check input and target keys in either CN mode")
		})
	}
}

func TestLoadPrimaryKeyValidationExemptions(t *testing.T) {
	table := &planpb.TableDef{Name: "ordinary", Pkey: &planpb.PrimaryKeyDef{PkeyColName: "id"}}
	plain := &tree.Load{}
	require.True(t, loadChecksPrimaryKey(plain, table))
	require.True(t, loadChecksPrimaryKey(&tree.Load{Local: true}, table))
	table.Pkey.PkeyColName = catalog.FakePrimaryKeyColName
	require.False(t, loadChecksPrimaryKey(plain, table))
	table.Pkey = nil
	require.False(t, loadChecksPrimaryKey(plain, table))
	for _, tc := range []struct {
		name  string
		setup func(*QueryBuilder, *planpb.TableDef)
	}{
		{"restore", func(b *QueryBuilder, _ *planpb.TableDef) { b.isRestore = true }},
		{"explicit hint", func(b *QueryBuilder, _ *planpb.TableDef) { b.optimizerHints = &OptimizerHints{skipDedup: 1} }},
		{"internal index", func(_ *QueryBuilder, tbl *planpb.TableDef) { tbl.Name = catalog.SecondaryIndexTableNamePrefix + "test" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := NewQueryBuilder(planpb.Query_INSERT, NewMockCompilerContext(true, newPlanTestProcess(t)), false, true)
			b.qry.LoadTag = true
			b.checkLoadPrimaryKey = true
			target := &planpb.TableDef{Name: "ordinary"}
			tc.setup(b, target)
			require.True(t, b.canSkipDedup(target))
		})
	}
}

func TestLoadCompositePrimaryKeyValidationPlan(t *testing.T) {
	ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
	create, err := mysql.ParseOne(t.Context(), "create table load_tuple(a int,b int,primary key(a,b))", 1)
	require.NoError(t, err)
	defer create.Free()
	ddl, err := BuildPlan(ctx, create, false)
	require.NoError(t, err)
	table := ddl.GetDdl().GetCreateTable().TableDef
	table.TblId = 990030411
	table.TableType = catalog.SystemOrdinaryRel
	table.Cols = append(table.Cols, MakeRowIdColDef())
	ctx.tables["load_tuple"] = table
	ctx.objects["load_tuple"] = &planpb.ObjectRef{Obj: int64(table.TblId), SchemaName: "tpch", ObjName: "load_tuple"}
	stmt, err := mysql.ParseOne(t.Context(), "load data inline format='csv', data='1,1' into table load_tuple fields terminated by ','", 1)
	require.NoError(t, err)
	defer stmt.Free()
	pn, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
	found := false
	for _, n := range pn.GetQuery().Nodes {
		if n.NodeType == planpb.Node_JOIN && n.JoinType == planpb.Node_DEDUP {
			found = true
			require.Equal(t, planpb.Node_FAIL, n.OnDuplicateAction)
			require.Len(t, n.DedupColTypes, 2, "both tuple components belong to the dedup key")
			require.False(t, n.DedupInputKeysUnique)
		}
	}
	require.True(t, found)
	requireLoadValidationIndependentOfWrites(t, pn.GetQuery())
}

// A successful FAIL check preserves the input row set, but its hash partitions
// must not become the bulk writer's partitions. Check both sides of the shared
// source so retaining an unreachable DEDUP node cannot satisfy this contract.
func requireLoadValidationIndependentOfWrites(t *testing.T, qry *planpb.Query) {
	t.Helper()
	var validationSources, writeSources []int32
	var visit func(int32, bool)
	visit = func(id int32, writing bool) {
		n := qry.Nodes[id]
		if writing {
			require.False(t, n.NodeType == planpb.Node_JOIN && n.JoinType == planpb.Node_DEDUP,
				"bulk writers must read the prepared input, independently of validation partitioning")
		}
		if n.NodeType == planpb.Node_SINK_SCAN {
			if writing {
				writeSources = append(writeSources, n.SourceStep...)
			} else {
				validationSources = append(validationSources, n.SourceStep...)
			}
		}
		for _, child := range n.Children {
			visit(child, writing)
		}
	}
	for _, n := range qry.Nodes {
		if n.NodeType == planpb.Node_MULTI_UPDATE {
			visit(n.NodeId, true)
		}
	}
	for _, root := range qry.Steps {
		n := qry.Nodes[root]
		if n.NodeType == planpb.Node_PROJECT && len(n.Children) == 1 {
			child := qry.Nodes[n.Children[0]]
			if child.NodeType == planpb.Node_JOIN && child.JoinType == planpb.Node_DEDUP {
				require.Equal(t, planpb.Node_FAIL, child.OnDuplicateAction)
				require.Len(t, n.ProjectList, 1, "validation retains only the primary-key identity")
				visit(root, false)
			}
		}
	}
	require.NotEmpty(t, validationSources, "the independent validation must be an executable step")
	require.NotEmpty(t, writeSources)
	require.Equal(t, validationSources, writeSources, "both branches consume one prepared image")
}
