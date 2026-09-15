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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestOnlyFullGroupByNotNullUniqueKey(t *testing.T) {
	const basic = "select deptno, ename, sum(sal) from constraint_test.emp group by deptno"
	for _, tc := range []struct {
		name    string
		sql     string
		parts   []string
		change  func(*pbplan.TableDef, *pbplan.ColDef, *pbplan.IndexDef)
		wantErr bool
	}{
		{name: "single key"},
		{name: "composite key", parts: []string{"deptno", "mgr"}, sql: "select ename, sum(sal) from constraint_test.emp group by mgr, deptno"},
		{name: "extra group", sql: "select ename, sum(sal) from constraint_test.emp group by deptno, job"},
		{name: "expression having order", sql: "select concat(ename,'!'), sum(sal) from constraint_test.emp group by deptno having ename <> '' order by ename"},
		{name: "alias and duplicate join rows", sql: "select e.deptno, e.ename, count(*) from constraint_test.emp e join constraint_test.emp x on e.mgr=x.mgr group by e.deptno"},
		{name: "other join binding", sql: "select e.deptno, x.ename, count(*) from constraint_test.emp e join constraint_test.emp x on e.deptno=x.deptno group by e.deptno"},
		{name: "derived relation", sql: "select deptno, ename, sum(sal) from (select deptno,ename,sal from constraint_test.emp) d group by deptno"},
		{name: "partial composite", parts: []string{"deptno", "mgr"}, wantErr: true},
		{name: "transformed key", sql: "select ename, sum(sal) from constraint_test.emp group by deptno+1", wantErr: true},
		{name: "inactive rollup key", sql: "select deptno, ename, sum(sal) from constraint_test.emp group by deptno with rollup", wantErr: true},
		{name: "nonunique", change: func(_ *pbplan.TableDef, _ *pbplan.ColDef, idx *pbplan.IndexDef) { idx.Unique = false }, wantErr: true},
		{name: "missing index table", change: func(_ *pbplan.TableDef, _ *pbplan.ColDef, idx *pbplan.IndexDef) { idx.TableExist = false }, wantErr: true},
		{name: "invisible constraint", change: func(_ *pbplan.TableDef, _ *pbplan.ColDef, idx *pbplan.IndexDef) { idx.Visible = false }},
		{name: "missing parts", parts: []string{}, wantErr: true},
		{name: "unknown part", parts: []string{"missing_column"}, wantErr: true},
		{name: "duplicate part", parts: []string{"deptno", "DEPTNO"}, wantErr: true},
		{name: "nullable", change: func(_ *pbplan.TableDef, col *pbplan.ColDef, _ *pbplan.IndexDef) { col.Default.NullAbility = true }, wantErr: true},
		{name: "nullable filtered", sql: "select deptno, ename, sum(sal) from constraint_test.emp where deptno is not null group by deptno", change: func(_ *pbplan.TableDef, col *pbplan.ColDef, _ *pbplan.IndexDef) { col.Default.NullAbility = true }},
		{name: "missing storage identity", change: func(_ *pbplan.TableDef, _ *pbplan.ColDef, idx *pbplan.IndexDef) { idx.IndexTableName = "" }, wantErr: true},
		{name: "missing nullability", change: func(_ *pbplan.TableDef, col *pbplan.ColDef, _ *pbplan.IndexDef) { col.Default = nil }, wantErr: true},
		{name: "generated component", change: func(_ *pbplan.TableDef, col *pbplan.ColDef, _ *pbplan.IndexDef) {
			col.GeneratedCol = &pbplan.GeneratedCol{}
		}, wantErr: true},
		{name: "hidden component", change: func(_ *pbplan.TableDef, col *pbplan.ColDef, _ *pbplan.IndexDef) { col.Hidden = true }, wantErr: true},
		{name: "prefix key", change: func(_ *pbplan.TableDef, _ *pbplan.ColDef, idx *pbplan.IndexDef) {
			idx.IndexAlgoParams = `{"prefix_lengths":"deptno:2"}`
		}, wantErr: true},
		{name: "malformed params", change: func(_ *pbplan.TableDef, _ *pbplan.ColDef, idx *pbplan.IndexDef) { idx.IndexAlgoParams = "{" }, wantErr: true},
		{name: "nonordinary index", change: func(_ *pbplan.TableDef, _ *pbplan.ColDef, idx *pbplan.IndexDef) { idx.IndexAlgo = "hnsw" }, wantErr: true},
		{name: "float equality", change: func(_ *pbplan.TableDef, col *pbplan.ColDef, _ *pbplan.IndexDef) {
			col.Typ = pbplan.Type{Id: int32(types.T_float64)}
		}, wantErr: true},
		{name: "char equality", change: func(_ *pbplan.TableDef, col *pbplan.ColDef, _ *pbplan.IndexDef) {
			col.Typ = pbplan.Type{Id: int32(types.T_char), Width: 8}
		}, wantErr: true},
		{name: "collated varchar", change: func(_ *pbplan.TableDef, col *pbplan.ColDef, _ *pbplan.IndexDef) {
			col.Typ = pbplan.Type{Id: int32(types.T_varchar), Width: 8, Charset: uint32(types.CharsetUTF8)}
		}, wantErr: true},
		{name: "binary string", change: func(_ *pbplan.TableDef, col *pbplan.ColDef, _ *pbplan.IndexDef) {
			col.Typ = pbplan.Type{Id: int32(types.T_varbinary), Width: 8}
		}},
		{name: "column flag is not a key", change: func(tbl *pbplan.TableDef, col *pbplan.ColDef, _ *pbplan.IndexDef) {
			tbl.Indexes = nil
			col.Unique = true
		}, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			optimizer := NewMockOptimizer(false)
			optimizer.ctxt.SetSqlModeOverride("ONLY_FULL_GROUP_BY")
			table := optimizer.ctxt.tablesByQualifiedName[mockQualifiedTableName("constraint_test", "emp")]
			require.NotNil(t, table)
			for _, name := range []string{"deptno", "mgr"} {
				pos, ok := tableColumnPosition(table, name)
				require.True(t, ok)
				table.Cols[pos].Default = &pbplan.Default{NullAbility: false}
			}
			parts := tc.parts
			if parts == nil {
				parts = []string{"deptno"}
			}
			idx := &pbplan.IndexDef{Unique: true, TableExist: true, IndexTableName: "unique_dept", Visible: true, Parts: parts}
			table.Indexes = []*pbplan.IndexDef{nil, idx}
			pos, ok := tableColumnPosition(table, "deptno")
			require.True(t, ok)
			if tc.change != nil {
				tc.change(table, table.Cols[pos], idx)
			}
			query := tc.sql
			if query == "" {
				query = basic
			}
			logical, err := runOneStmt(optimizer, t, query)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.True(t, reachableNodeType(logical.GetQuery(), pbplan.Node_AGG), "unique-key validation must not eliminate aggregation")
		})
	}
}
