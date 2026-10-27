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
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func TestCountEliminatesUnobservedSort(t *testing.T) {
	for _, tt := range []struct {
		name         string
		sql          string
		distinctKeys int
	}{
		{"star", "select count(*) from (select ename from constraint_test.emp order by ename) s", 0},
		{"constant", "select count(1) from (select ename from constraint_test.emp order by ename) s", 0},
		{"nullable column", "select count(comm) from (select comm from constraint_test.emp order by comm) s", 0},
		{"distinct tuples", "select count(*) from (select distinct ename, deptno from constraint_test.emp order by ename, deptno) s", 2},
		{"aliases", "select count(name) from (select ename as name from constraint_test.emp order by name) s", 0},
		{"multiple counts", "select count(*), count(comm) from (select comm from constraint_test.emp order by comm) s", 0},
		{"join", `select count(1) from (
			select distinct e.ename, d.dname from constraint_test.emp e
			join constraint_test.dept d on e.deptno = d.deptno order by e.ename, d.dname
		) s`, 2},
		// An effect-free LEFT JOIN branch released by the Sort may still be
		// pruned: its unique-key right side cannot raise or filter rows.
		{"effect-free left join", `select count(*) from (
			select e.ename from constraint_test.emp e
			left join constraint_test.dept d on e.deptno = d.deptno order by d.deptno
		) s`, 0},
		// INNER JOIN branches are never pruned for missing references, so a
		// released failing expression on the inner side stays observable in
		// the plan. Explicit text casts are lenient per MySQL (0 + warning),
		// so the runtime-raising witness uses an invalid regexp pattern,
		// which still errors at execution.
		{"diagnostic inner join", `select count(*) from (
			select e.ename from constraint_test.emp e
			join constraint_test.dept d on e.deptno = d.deptno and length(regexp_substr(d.dname, '[')) > 0
			order by d.deptno
		) s`, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			logical, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, tt.sql)
			require.NoError(t, err)
			query := logical.GetQuery()
			require.NotNil(t, query)
			require.False(t, reachableNodeType(query, planpb.Node_SORT), "COUNT must not retain an unobserved Sort")
			count := firstReachableNode(query, planpb.Node_AGG)
			require.NotNil(t, count)
			require.Empty(t, count.GroupBy)
			if tt.distinctKeys != 0 {
				require.Equal(t, 2, reachableNodeTypeCount(query, planpb.Node_AGG))
				distinct := query.Nodes[count.Children[0]]
				require.Equal(t, planpb.Node_AGG, distinct.NodeType)
				require.Len(t, distinct.GroupBy, tt.distinctKeys, "removing Sort must not prune DISTINCT tuple keys")
			}
		})
	}
}

func TestCountKeepsObservableOrUnsafeSort(t *testing.T) {
	for _, tt := range []struct {
		name string
		sql  string
	}{
		{"limit", "select count(comm) from (select comm from constraint_test.emp order by comm limit 2) s"},
		{"offset", "select count(*) from (select ename from constraint_test.emp order by ename limit 2 offset 1) s"},
		{"mixed aggregate", "select count(*), sum(sal) from (select sal from constraint_test.emp order by sal) s"},
		{"count distinct", "select count(distinct deptno) from (select deptno from constraint_test.emp order by deptno) s"},
		{"volatile order", "select count(*) from (select ename from constraint_test.emp order by rand()) s"},
		// Explicit text casts are lenient per MySQL (0 + warning), so the
		// runtime-raising witness is an invalid regexp pattern, which errors
		// at execution instead of returning a value.
		{"failing order", "select count(*) from (select ename from constraint_test.emp order by regexp_substr(ename, '[')) s"},
		{"failing alias", "select count(*) from (select regexp_substr(ename, '[') as n from constraint_test.emp order by n) s"},
		{"volatile count", "select count(rand()) from (select ename from constraint_test.emp order by ename) s"},
		{"failing count", "select count(regexp_substr(ename, '[')) from (select ename from constraint_test.emp order by ename) s"},
		{"window output", "select count(*) from (select row_number() over (order by empno) as rn from constraint_test.emp order by rn) s"},
		{"non count consumer", "select ename from (select ename from constraint_test.emp order by ename) s"},
		// Removing the Sort would release the last outside reference to the
		// LEFT JOIN right branch, letting removeEffectlessLeftJoins prune the
		// failing ON expression.
		{"diagnostic left join", `select count(*) from (
			select e.ename from constraint_test.emp e
			left join constraint_test.dept d on e.deptno = d.deptno and length(regexp_substr(d.dname, '[')) > 0
			order by d.deptno
		) s`},
		// A released LEFT JOIN branch keeps its failing scan filter too:
		// pruning the branch would discard that evaluation.
		{"left join filtered branch", `select count(*) from (
			select e.ename from constraint_test.emp e
			left join (select deptno from constraint_test.dept where length(regexp_substr(dname, '[')) > 0) d
			on e.deptno = d.deptno order by d.deptno
		) s`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			logical, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, tt.sql)
			require.NoError(t, err)
			require.True(t, reachableNodeType(logical.GetQuery(), planpb.Node_SORT))
		})
	}
}

// The Sort may hold the last outside reference to a LEFT JOIN right branch.
// Eliminating it would let removeEffectlessLeftJoins prune that branch and hide
// the failing ON cast, so both nodes must stay reachable.
func TestCountSortKeepsReleasedDiagnosticLeftJoin(t *testing.T) {
	logical, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, `select count(*) from (
		select e.ename from constraint_test.emp e
		left join constraint_test.dept d on e.deptno = d.deptno and cast(d.dname as decimal(10,0)) > 0
		order by d.deptno
	) s`)
	require.NoError(t, err)
	query := logical.GetQuery()
	require.True(t, reachableNodeType(query, planpb.Node_SORT), "diagnostic Sort subtree must not be eliminated")
	join := firstReachableNode(query, planpb.Node_JOIN)
	require.NotNil(t, join, "LEFT JOIN owning the released diagnostic branch must survive")
	require.Equal(t, planpb.Node_LEFT, join.JoinType)
	require.NotEmpty(t, join.OnList, "the failing ON predicate must stay evaluable")
}

func TestCountSortEliminationPreservesOuterOrder(t *testing.T) {
	logical, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t,
		"select count(*) from (select ename from constraint_test.emp order by ename) s order by count(*)")
	require.NoError(t, err)
	query := logical.GetQuery()
	require.Equal(t, 1, reachableNodeTypeCount(query, planpb.Node_SORT))
	count := firstReachableNode(query, planpb.Node_AGG)
	require.NotNil(t, count)
	require.NotEqual(t, planpb.Node_SORT, query.Nodes[count.Children[0]].NodeType)
}

func newCountSortTestBuilder(t *testing.T) *QueryBuilder {
	t.Helper()
	typ := planpb.Type{Id: int32(types.T_int64)}
	count, err := BindFuncExprImplByPlanExpr(newPlanTestProcess(t).Ctx, "count",
		[]*planpb.Expr{GetColExpr(typ, 20, 0)})
	require.NoError(t, err)
	return &QueryBuilder{qry: &planpb.Query{
		Steps: []int32{3},
		Nodes: []*planpb.Node{
			{NodeId: 0, NodeType: planpb.Node_TABLE_SCAN, BindingTags: []int32{10},
				TableDef: &planpb.TableDef{Cols: []*planpb.ColDef{{Name: "a", Typ: typ}}}},
			{NodeId: 1, NodeType: planpb.Node_PROJECT, BindingTags: []int32{20},
				Children: []int32{0}, ProjectList: []*planpb.Expr{GetColExpr(typ, 10, 0)}},
			{NodeId: 2, NodeType: planpb.Node_SORT, Children: []int32{1},
				OrderBy: []*planpb.OrderBySpec{{Expr: GetColExpr(typ, 20, 0)}}},
			{NodeId: 3, NodeType: planpb.Node_AGG, BindingTags: []int32{30, 31},
				Children: []int32{2}, AggList: []*planpb.Expr{count}},
		},
	}}
}

func TestCountSortEliminationRewiresOnlyItsConsumer(t *testing.T) {
	builder := newCountSortTestBuilder(t)
	builder.qry.Steps = append(builder.qry.Steps, 2) // another consumer observes the Sort
	builder.removeUnobservedCountSorts(3)
	require.Equal(t, []int32{1}, builder.qry.Nodes[3].Children)
	require.Equal(t, []int32{1}, builder.qry.Nodes[2].Children)
	require.Len(t, builder.qry.Nodes[2].OrderBy, 1)
	require.Equal(t, 1, reachableNodeTypeCount(builder.qry, planpb.Node_SORT))
	builder.removeUnobservedCountSorts(3) // idempotent on the rewritten edge
	require.Equal(t, []int32{1}, builder.qry.Nodes[3].Children)
}

func TestCountSortEliminationRejectsUnprovenBoundaries(t *testing.T) {
	for _, tt := range []struct {
		name   string
		mutate func(*QueryBuilder)
	}{
		{"found rows", func(b *QueryBuilder) { b.sqlCalcFoundRows = true }},
		{"sort limit", func(b *QueryBuilder) { b.qry.Nodes[2].Limit = makePlan2Uint64ConstExprWithType(1) }},
		{"sort offset", func(b *QueryBuilder) { b.qry.Nodes[2].Offset = makePlan2Uint64ConstExprWithType(1) }},
		{"sort projection", func(b *QueryBuilder) { b.qry.Nodes[2].ProjectList = b.qry.Nodes[1].ProjectList }},
		{"sort filter", func(b *QueryBuilder) {
			b.qry.Nodes[2].FilterList = []*planpb.Expr{makePlan2BoolConstExprWithType(true)}
		}},
		{"count limit", func(b *QueryBuilder) { b.qry.Nodes[3].Limit = makePlan2Uint64ConstExprWithType(1) }},
		{"grouped count", func(b *QueryBuilder) { b.qry.Nodes[3].GroupBy = b.qry.Nodes[1].ProjectList }},
		{"distinct count", func(b *QueryBuilder) {
			fn := b.qry.Nodes[3].AggList[0].GetF()
			fn.Func.Obj = int64(uint64(fn.Func.Obj) | function.Distinct)
		}},
		{"runtime literal", func(b *QueryBuilder) {
			expr := makePlan2Int64ConstExprWithType(1)
			expr.GetLit().Src = makePlan2Int64ConstExprWithType(1)
			b.qry.Nodes[2].OrderBy[0].Expr = expr
		}},
		{"unsupported key type", func(b *QueryBuilder) { b.qry.Nodes[2].OrderBy[0].Expr.Typ.Id = int32(types.T_json) }},
		{"unknown producer", func(b *QueryBuilder) { b.qry.Nodes[1].ProjectList[0].GetCol().RelPos = 99 }},
		{"ambiguous producer", func(b *QueryBuilder) { b.qry.Nodes[0].BindingTags = []int32{20} }},
		{"alias cycle", func(b *QueryBuilder) { b.qry.Nodes[1].ProjectList[0].GetCol().RelPos = 20 }},
		{"materialized producer", func(b *QueryBuilder) { b.qry.Nodes[1].NodeType = planpb.Node_SINK_SCAN }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			builder := newCountSortTestBuilder(t)
			tt.mutate(builder)
			builder.removeUnobservedCountSorts(3)
			require.Equal(t, []int32{2}, builder.qry.Nodes[3].Children)
		})
	}
}
