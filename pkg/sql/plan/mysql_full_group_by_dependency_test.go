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

func TestGroupByProjectionPreservesCollatedPayload(t *testing.T) {
	optimizer := NewMockOptimizer(false)
	optimizer.ctxt.SetSqlModeOverride("ONLY_FULL_GROUP_BY")
	table := optimizer.ctxt.tablesByQualifiedName[mockQualifiedTableName("constraint_test", "emp")]
	pos, ok := tableColumnPosition(table, "ename")
	require.True(t, ok)
	table.Cols[pos].Typ.Charset = uint32(types.CharsetUTF8)
	_, err := runOneStmt(optimizer, t, "select empno,ename,sum(sal) from (select empno,ename,sal from constraint_test.emp) d group by empno")
	require.NoError(t, err, "projection copies the payload; its collation must not be treated as a storage-key proof")
}

func TestGroupByDependencyGroupingMaskIsolation(t *testing.T) {
	typ := pbplan.Type{Id: int32(types.T_int64)}
	table := &pbplan.TableDef{
		Cols: []*pbplan.ColDef{{Name: "id", Typ: typ}, {Name: "value", Typ: typ}},
		Pkey: &pbplan.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
	}
	builder := &QueryBuilder{qry: &pbplan.Query{Nodes: []*pbplan.Node{{NodeType: pbplan.Node_TABLE_SCAN, BindingTags: []int32{1}, TableDef: table}}}}
	ctx := &BindContext{fullGroupByInputReady: true, groupingFlag: []bool{true},
		groups: []*Expr{{Typ: typ, Expr: &pbplan.Expr_Col{Col: &pbplan.ColRef{RelPos: 1, ColPos: 0}}}}}
	require.True(t, builder.fullGroupByDependencyAllows(ctx, 1, 1))
	relation := ctx.fullGroupByProof.relation
	ctx.groupingFlag[0] = false
	require.False(t, builder.fullGroupByDependencyAllows(ctx, 1, 1))
	ctx.groupingFlag[0] = true
	require.True(t, builder.fullGroupByDependencyAllows(ctx, 1, 1))
	require.Same(t, relation, ctx.fullGroupByProof.relation, "reuse immutable facts, not a stale grouping closure")
}

func TestGroupByEqualityRejectsChangedBoundDomain(t *testing.T) {
	typ := pbplan.Type{Id: int32(types.T_uint64)}
	r := newFullGroupByRelation()
	r.columns[fullGroupByColumn{1, 0}] = typ
	r.columns[fullGroupByColumn{2, 0}] = typ
	left := &Expr{Typ: typ, Expr: &pbplan.Expr_Col{Col: &pbplan.ColRef{RelPos: 1, ColPos: 0}}}
	right := &Expr{Typ: typ, Expr: &pbplan.Expr_Col{Col: &pbplan.ColRef{RelPos: 2, ColPos: 0}}}
	expr := &Expr{Expr: &pbplan.Expr_F{F: &pbplan.Function{Func: &pbplan.ObjectRef{ObjName: "="}, Args: []*Expr{left, right}}}}
	_, _, ok := r.equality(expr)
	require.True(t, ok)
	left.Typ.Id = int32(types.T_float64)
	right.Typ.Id = int32(types.T_float64)
	_, _, ok = r.equality(expr)
	require.False(t, ok, "storage uniqueness cannot justify equality in a lossy resolved domain")
}

func TestOnlyFullGroupByDependencyClosure(t *testing.T) {
	for _, tc := range []struct {
		name    string
		sql     string
		unique  []string
		wantErr bool
	}{
		{"nullable filtered", "select deptno,ename,sum(sal) from constraint_test.emp where deptno is not null group by deptno", []string{"deptno"}, false},
		{"nullable unfiltered", "select deptno,ename,sum(sal) from constraint_test.emp group by deptno", []string{"deptno"}, true},
		{"nullable OR", "select deptno,ename,sum(sal) from constraint_test.emp where deptno is not null or mgr=1 group by deptno", []string{"deptno"}, true},
		{"nullable HAVING", "select deptno,ename,sum(sal) from constraint_test.emp group by deptno having deptno is not null", []string{"deptno"}, true},
		{"nullable outer ON", "select e.deptno,e.ename,count(*) from constraint_test.emp e left join constraint_test.dept d on e.deptno=d.deptno and e.deptno is not null group by e.deptno", []string{"deptno"}, true},
		{"composite filtered", "select ename,sum(sal) from constraint_test.emp where deptno is not null and mgr is not null group by deptno,mgr", []string{"deptno", "mgr"}, false},
		{"composite partly filtered", "select ename,sum(sal) from constraint_test.emp where deptno is not null group by deptno,mgr", []string{"deptno", "mgr"}, true},
		{"derived primary key", "select empno,ename,sum(sal) from (select empno,ename,sal from constraint_test.emp) d group by empno", nil, false},
		{"nested projection aliases", "select id,label,sum(amount) from (select empno id,ename label,sal amount from (select empno,ename,sal from constraint_test.emp) e) d group by id", nil, false},
		{"cte key", "with d as (select empno,ename,sal from constraint_test.emp) select empno,ename,sum(sal) from d group by empno", nil, false},
		{"derived nullable filter", "select k,ename,sum(sal) from (select deptno k,ename,sal from constraint_test.emp) d where k is not null group by k", []string{"deptno"}, false},
		{"projection loses key", "select ename,sum(sal) from (select ename,sal from constraint_test.emp) d group by sal", nil, true},
		{"projection transforms key", "select ename,sum(sal) from (select empno+1 k,ename,sal from constraint_test.emp) d group by k", nil, true},
		{"limit boundary", "select empno,ename,sum(sal) from (select empno,ename,sal from constraint_test.emp limit 2) d group by empno", nil, true},
		{"union boundary", "select empno,ename,sum(sal) from (select empno,ename,sal from constraint_test.emp union all select empno,ename,sal from constraint_test.emp) d group by empno", nil, true},
		{"distinct boundary", "select empno,ename,sum(sal) from (select distinct empno,ename,sal from constraint_test.emp) d group by empno", nil, true},
		{"inner ON", "select e.deptno,d.dname,sum(e.sal) from constraint_test.emp e join constraint_test.dept d on e.deptno=d.deptno group by e.deptno", nil, false},
		{"nullsafe equality", "select e.deptno,d.dname,sum(e.sal) from constraint_test.emp e join constraint_test.dept d on e.deptno<=>d.deptno group by e.deptno", nil, true},
		{"cast equality", "select e.deptno,d.dname,sum(e.sal) from constraint_test.emp e join constraint_test.dept d on cast(e.deptno as char)=d.deptno group by e.deptno", nil, true},
		{"comma WHERE", "select e.deptno,d.dname,sum(e.sal) from constraint_test.emp e,constraint_test.dept d where e.deptno=d.deptno group by e.deptno", nil, false},
		{"USING", "select deptno,d.dname,sum(e.sal) from constraint_test.emp e join constraint_test.dept d using(deptno) group by deptno", nil, false},
		{"left child key", "select e.empno,d.dname,sum(e.sal) from constraint_test.emp e left join constraint_test.dept d on e.deptno=d.deptno group by e.empno", nil, false},
		{"right child key", "select e.empno,d.dname,sum(e.sal) from constraint_test.dept d right join constraint_test.emp e on e.deptno=d.deptno group by e.empno", nil, false},
		{"left foreign key", "select e.deptno,d.dname,sum(e.sal) from constraint_test.emp e left join constraint_test.dept d on e.deptno=d.deptno group by e.deptno", nil, false},
		{"left varying residual", "select e.deptno,d.dname,sum(e.sal) from constraint_test.emp e left join constraint_test.dept d on e.deptno=d.deptno and e.mgr>0 group by e.deptno", nil, true},
		{"left determined residual", "select e.empno,d.dname,sum(e.sal) from constraint_test.emp e left join constraint_test.dept d on e.deptno=d.deptno and e.mgr>0 group by e.empno", nil, false},
		{"left volatile residual", "select e.empno,d.dname,sum(e.sal) from constraint_test.emp e left join constraint_test.dept d on e.deptno=d.deptno and rand()>0 group by e.empno", nil, true},
		{"weak constants stay scoped", "select d.deptno,e.ename,count(*) from constraint_test.emp e left join (select deptno,dname from constraint_test.dept where deptno=1) d on e.deptno=d.deptno group by d.deptno", nil, true},
		{"left reverse direction", "select d.deptno,e.ename,sum(e.sal) from constraint_test.emp e left join constraint_test.dept d on e.deptno=d.deptno group by d.deptno", nil, true},
		{"full outer boundary", "select e.empno,d.dname,count(*) from constraint_test.emp e full outer join constraint_test.dept d on e.deptno=d.deptno group by e.empno", nil, true},
		{"repeated CTE identity", "with d as (select empno,ename from constraint_test.emp) select a.empno,b.ename,count(*) from d a cross join d b group by a.empno", nil, true},
		{"repeated CTE equality", "with d as (select empno,ename from constraint_test.emp) select a.empno,b.ename,count(*) from d a join d b on a.empno=b.empno group by a.empno", nil, false},
		{"nested outer", "select e.empno,d2.dname,count(*) from constraint_test.emp e left join constraint_test.dept d on e.deptno=d.deptno left join constraint_test.dept d2 on d.deptno=d2.deptno group by e.empno", nil, false},
		{"inactive key", "select e.empno,d.dname,count(*) from constraint_test.emp e left join constraint_test.dept d on e.deptno=d.deptno group by e.empno with rollup", nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			optimizer := NewMockOptimizer(false)
			optimizer.ctxt.SetSqlModeOverride("ONLY_FULL_GROUP_BY")
			if tc.unique != nil {
				table := optimizer.ctxt.tablesByQualifiedName[mockQualifiedTableName("constraint_test", "emp")]
				table.Indexes = []*pbplan.IndexDef{{Unique: true, TableExist: true, IndexTableName: "fd_unique", Parts: tc.unique}}
				for _, name := range tc.unique {
					pos, ok := tableColumnPosition(table, name)
					require.True(t, ok)
					table.Cols[pos].Default = &pbplan.Default{NullAbility: true}
					table.Cols[pos].NotNull = false
					table.Cols[pos].Typ.NotNullable = false
				}
			}
			logical, err := runOneStmt(optimizer, t, tc.sql)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, logical.GetQuery())
			if tc.name == "inner ON" || tc.name == "left foreign key" {
				require.True(t, reachableNodeType(logical.GetQuery(), pbplan.Node_AGG))
				require.True(t, reachableNodeType(logical.GetQuery(), pbplan.Node_JOIN))
			}
		})
	}
}
