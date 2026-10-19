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
}
