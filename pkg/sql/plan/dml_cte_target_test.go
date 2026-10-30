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

package plan

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestInsertCTEPhysicalTarget(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		route := "modern"
		if legacy {
			route = "legacy"
		}
		for _, tc := range []struct{ sql, legacyError string }{
			{"with nation as (select 9 as id) insert into nation values(1,'x',0,'c')", ""},
			{"with q as (select 9 as id) insert into nation values(1,'x',0,'c')", ""},
			{"with nation as (select 9 as id) insert into tpch.nation values(1,'x',0,'c')", ""},
			{"with nation as (select 1 as id) insert into nation select id,'x',0,'c' from nation", ""},
			{"insert into nation with nation as (select 1 as id) select id,'x',0,'c' from nation", ""},
			{"with unused as (select 9 as id) insert into nation with q as (select 1 as id) select id,'x',0,'c' from q", ""},
			{"with outer_cte as (select 1 as id) insert into nation with inner_cte as (select id from outer_cte) select id,'x',0,'c' from inner_cte", ""},
			{"with q as (select 9 as id) insert into nation with q as (select 1 as id) select id,'x',0,'c' from q", ""},
			{"with outer_cte as (select cast(1 as decimal(10,0)) as id) insert into nation with q as (select id+1 as id from outer_cte) select id,'x',0,'c' from q", ""},
			{"with recursive q(id) as (select 1 union all select id+1 from q where id<2) insert into nation select id,'x',0,'c' from q", ""},
			{"with q as (select 1 as id) insert into nation values((select id from q),'x',0,'c')", "subquery in JOIN condition"},
		} {
			sql := tc.sql

			t.Run(sql+"/"+route, func(t *testing.T) {
				statement, err := mysql.ParseOne(t.Context(), sql, 1)
				require.NoError(t, err)
				defer statement.Free()
				insert := statement.(*tree.Insert)
				originalRows, originalWith, outerWith := insert.Rows, insert.Rows.With, insert.With
				for range 2 {
					mock := NewMockOptimizer(true, newPlanTestProcess(t))
					var p *Plan
					if legacy {
						p, err = buildInsert(insert, mock.CurrentContext(), false, false)
					} else {
						p, err = BuildPlan(mock.CurrentContext(), statement, false)
					}
					if legacy && tc.legacyError != "" {
						require.ErrorContains(t, err, tc.legacyError)
						require.Same(t, originalRows, insert.Rows)
						require.True(t, originalWith == insert.Rows.With)
						require.Same(t, outerWith, insert.With)
						continue
					}
					require.NoError(t, err)
					require.NotNil(t, p)
					require.Same(t, originalRows, insert.Rows)
					require.True(t, originalWith == insert.Rows.With)
					require.Same(t, outerWith, insert.With)
					var targets []string
					for _, node := range p.GetQuery().Nodes {
						if node.NodeType == planpb.Node_INSERT {
							targets = append(targets, node.InsertCtx.TableDef.Name)
						}
						for _, writer := range node.UpdateCtxList {
							targets = append(targets, writer.TableDef.Name)
						}
					}
					require.Contains(t, targets, "nation")
				}
			})
		}
	}
}

func TestUpdateCTETargetNamespace(t *testing.T) {
	for _, tc := range []struct{ name, sql, wantError string }{
		{"qualified physical target", "with nation as (select 1 as id) update tpch.nation set n_name='x' where n_nationkey=1", ""},
		{"alias equals unused CTE", "with x as (select 1 as id) update nation as x set n_name='x' where n_nationkey=1", ""},
		{"CTE target", "with nation as (select 1 as n_nationkey, 'cte' as n_name) update nation set n_name='x'", "not updatable"},
		{"ambiguous read CTE", "with q as (select 1 as n_nationkey,'cte' as n_name) update nation join q on nation.n_nationkey=q.n_nationkey set n_name='x'", "ambiguous"},
		{"qualified read CTE", "with q as (select 1 as n_nationkey,'cte' as n_name) update nation join q on nation.n_nationkey=q.n_nationkey set nation.n_name='x'", ""},
		{"unique unqualified target", "with q as (select 1 as n_nationkey) update nation join q on nation.n_nationkey=q.n_nationkey set n_name='x'", ""},
		{"wrong schema", "update tpch.nation set wrong.nation.n_name='x'", "unknown UPDATE target"},
		{"FROM preserves target namespace", "update nation set n_name='x' from nation2 where nation.n_nationkey=nation2.n_nationkey", ""},
		{"target JOIN plus FROM ambiguity", "with q as (select 1 as n_nationkey,'cte' as n_name) update nation join q on nation.n_nationkey=q.n_nationkey set n_name='x' from region where nation.n_regionkey=region.r_regionkey", "ambiguous"},
		{"target JOIN plus FROM unique column", "with q as (select 1 as n_nationkey) update nation join q on nation.n_nationkey=q.n_nationkey set n_name='x' from region where nation.n_regionkey=region.r_regionkey", ""},
		{"target USING plus FROM read only", "with q as (select 1 as n_nationkey) update nation right join q using(n_nationkey) set n_nationkey=2 from region where nation.n_regionkey=region.r_regionkey", "not updatable"},
		{"qualified CTE assignment", "with q as (select 1 as n_nationkey,'cte' as n_name) update nation join q on nation.n_nationkey=q.n_nationkey set q.n_name='x'", "table"},
		{"aliased CTE target", "with q as (select 1 as n_nationkey,'cte' as n_name) update q as x set x.n_name='x'", "table"},
		{"FULL USING merged target", "with q as (select 1 as n_nationkey) update nation full join q using(n_nationkey) set n_nationkey=2", "not uniquely writable"},
		{"FULL USING explicit physical target", "with q as (select 1 as n_nationkey) update nation full join q using(n_nationkey) set nation.n_nationkey=2 where nation.n_nationkey=1", ""},
		{"qualified SET", "update tpch.nation set tpch.nation.n_name='x'", ""},
		{"ON ambiguity", "with q as (select 1 as n_nationkey) update nation join q on nation.n_nationkey=q.n_nationkey set n_nationkey=2", "ambiguous"},
		{"USING physical left", "with q as (select 1 as n_nationkey) update nation join q using(n_nationkey) set n_nationkey=2", ""},
		{"USING read-only right", "with q as (select 1 as n_nationkey) update nation right join q using(n_nationkey) set n_nationkey=2", "not updatable"},
		{"USING physical right", "with q as (select 1 as n_nationkey) update q right join nation using(n_nationkey) set n_nationkey=2", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(true, newPlanTestProcess(t))
			stmt, err := mysql.ParseOne(t.Context(), tc.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			p, err := BuildPlan(mock.CurrentContext(), stmt, false)
			if tc.wantError != "" {
				require.ErrorContains(t, err, tc.wantError)
				require.False(t, moerr.IsMoErrCode(err, moerr.ErrUnsupportedDML))
				return
			}
			require.NoError(t, err)
			var targets []string
			for _, n := range p.GetQuery().Nodes {
				for _, w := range n.UpdateCtxList {
					targets = append(targets, w.TableDef.Name)
				}
			}
			require.Contains(t, targets, "nation")
		})
	}
}

func TestInsertCTEFailedBindingPreservesSourceScope(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		mock := NewMockOptimizer(true, newPlanTestProcess(t))
		stmt, err := mysql.ParseOne(t.Context(), "with nation as (select 1 as id) insert into nation select missing_col,'x',0,'c' from nation", 1)
		require.NoError(t, err)
		defer stmt.Free()
		insert := stmt.(*tree.Insert)
		rows, with := insert.Rows, insert.Rows.With
		for range 2 {
			if legacy {
				_, err = buildInsert(insert, mock.CurrentContext(), false, false)
			} else {
				_, err = BuildPlan(mock.CurrentContext(), stmt, false)
			}
			require.ErrorContains(t, err, "missing_col")
			require.Same(t, rows, insert.Rows)
			require.True(t, with == insert.Rows.With)
		}
	}
}

func TestUpdateQualifiedCTETargetMetadata(t *testing.T) {
	mock := NewMockOptimizer(true, newPlanTestProcess(t))
	stmt, err := mysql.ParseOne(t.Context(), "with nation as (select 1 as id) update tpch.nation set n_name='x'", 1)
	require.NoError(t, err)
	defer stmt.Free()
	info := NewDMLContext()
	err = info.ResolveUpdateTables(mock.CurrentContext(), stmt.(*tree.Update))
	require.NoError(t, err)
	require.Len(t, info.tableDefs, 1)
	require.Equal(t, "nation", info.tableDefs[0].Name)
}

func TestUpdateGeneratedDefaultTargetScope(t *testing.T) {
	for _, tc := range []struct {
		sql    string
		reject bool
	}{
		{"update emp set comm=10,sal=default where empno=1", false},
		{"update emp set sal=99 where empno=1", true},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			mock := NewMockOptimizer(true, newPlanTestProcess(t))
			setMockGeneratedColumn(t, mock, "emp", "sal", "comm")
			p, err := runOneStmt(mock, t, tc.sql)
			if tc.reject {
				require.ErrorContains(t, err, "generated column")
			} else {
				require.NoError(t, err)
				require.Contains(t, modernBaseUpdateContextNames(p.GetQuery()), "emp")
			}
		})
	}
}

func TestUpdateTargetScopeCaptureRestored(t *testing.T) {
	for _, tc := range []struct{ sql, wantError string }{
		{"update nation set n_name='x' from nation2 where nation.n_nationkey=nation2.n_nationkey", ""},
		{"update nation set wrong.nation.n_name='x' from nation2 where nation.n_nationkey=nation2.n_nationkey", "unknown UPDATE target"},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			mock := NewMockOptimizer(true, newPlanTestProcess(t))
			stmt, err := mysql.ParseOne(t.Context(), tc.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			builder := NewQueryBuilder(planpb.Query_UPDATE, mock.CurrentContext(), false, false)
			previous := &updateTargetScope{join: &tree.JoinTableExpr{}}
			builder.updateTargetScope = previous
			_, err = builder.bindUpdate(stmt.(*tree.Update), NewBindContext(builder, nil), NewDMLContext())
			if tc.wantError != "" {
				require.ErrorContains(t, err, tc.wantError)
			} else {
				require.NoError(t, err)
			}
			require.Same(t, previous, builder.updateTargetScope)
			require.Nil(t, previous.context)
		})
	}
}

func TestInsertCTEDeclarationScopeErrors(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		for _, tc := range []struct{ sql, wantError string }{
			{"with a as (select id from b), b as (select 1 as id) insert into nation with c as (select id from a) select id,'x',0,'c' from c", `table "b" does not exist`},
			{"with a as (select 1 as id), a as (select 2 as id) insert into nation select id,'x',0,'c' from a", "specified more than once"},
			{"with a as (select 1 as id) insert into nation with b as (select missing_col from a) select * from b", "missing_col"},
		} {
			sql := tc.sql

			t.Run(sql+map[bool]string{true: "/legacy", false: "/modern"}[legacy], func(t *testing.T) {
				stmt, err := mysql.ParseOne(t.Context(), sql, 1)
				require.NoError(t, err)
				defer stmt.Free()
				insert := stmt.(*tree.Insert)
				rows, sourceWith, outerWith := insert.Rows, insert.Rows.With, insert.With
				for range 2 {
					mock := NewMockOptimizer(true, newPlanTestProcess(t))
					if legacy {
						_, err = buildInsert(insert, mock.CurrentContext(), false, false)
					} else {
						_, err = BuildPlan(mock.CurrentContext(), stmt, false)
					}
					require.ErrorContains(t, err, tc.wantError)
					require.Same(t, rows, insert.Rows)
					require.True(t, sourceWith == insert.Rows.With)
					require.Same(t, outerWith, insert.With)
				}
			})
		}
	}
}
