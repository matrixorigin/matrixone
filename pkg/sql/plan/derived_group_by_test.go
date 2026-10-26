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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func derivedGroupTestContext(t *testing.T, builder *QueryBuilder) *BindContext {
	t.Helper()
	x := GetColExpr(planpb.Type{Id: int32(types.T_int32)}, 7, 0)
	wide, err := appendCastBeforeExpr(builder.GetContext(), DeepCopyExpr(x), planpb.Type{Id: int32(types.T_int64)})
	require.NoError(t, err)
	derived, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "-", []*planpb.Expr{wide, MakePlan2Int64ConstExprWithType(1)})
	require.NoError(t, err)
	// The derived key comes first so retaining x changes its output slot.
	return &BindContext{groupTag: 11, groups: []*planpb.Expr{derived, x}, groupingFlag: []bool{true, true},
		projects:            []*planpb.Expr{GetColExpr(derived.Typ, 11, 0), GetColExpr(x.Typ, 11, 1)},
		groupByAst:          map[string]int32{"derived": 0, "x": 1},
		groupByCanonicalAst: map[string]int32{"canonical-x": 1},
		groupByParamAst:     map[string]int32{"x-ordinal": 1}}
}

func TestPruneDerivedGroupBy(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(false, newPlanTestProcess(t)), false, true)
	ctx := derivedGroupTestContext(t, builder)
	original := DeepCopyExpr(ctx.groups[0])
	originalGroups := fmt.Sprint(ctx.groups)
	having := []*planpb.Expr{DeepCopyExpr(ctx.projects[0])}
	order := &planpb.OrderBySpec{Expr: DeepCopyExpr(ctx.projects[1])}
	builder.pruneDerivedGroupBy(ctx, [][]*planpb.Expr{ctx.projects, having}, []*planpb.OrderBySpec{nil, order})
	require.Len(t, ctx.groups, 1)
	require.Equal(t, []bool{true}, ctx.groupingFlag)
	require.Equal(t, map[string]int32{"x": 0}, ctx.groupByAst)
	require.Equal(t, map[string]int32{"canonical-x": 0}, ctx.groupByCanonicalAst)
	require.Equal(t, map[string]int32{"x-ordinal": 0}, ctx.groupByParamAst)
	for _, expr := range []*planpb.Expr{ctx.projects[0], having[0]} {
		require.True(t, sameAffineResultType(original, expr))
		require.Equal(t, original.GetF().Func.Obj, expr.GetF().Func.Obj)
		cast := expr.GetF().Args[0]
		require.Equal(t, original.GetF().Args[0].GetF().Func.Obj, cast.GetF().Func.Obj)
		require.Equal(t, int32(11), cast.GetF().Args[0].GetCol().RelPos)
		require.Equal(t, int32(0), cast.GetF().Args[0].GetCol().ColPos)
	}
	require.Equal(t, int32(0), ctx.projects[1].GetCol().ColPos)
	require.Equal(t, int32(0), order.Expr.GetCol().ColPos)
	require.Equal(t, originalGroups, fmt.Sprint([]*planpb.Expr{original, ctx.groups[0]}), "source expressions remain immutable")
}

func TestPruneDerivedGroupByAtomicFallback(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(false, newPlanTestProcess(t)), false, true)
	tests := []struct {
		name   string
		mutate func(*BindContext)
	}{
		{"grouping extension", func(c *BindContext) { c.groupingFuncAllowed = true }},
		{"expanded grouping set", func(c *BindContext) { c.isGroupingSet = true }},
		{"inactive key", func(c *BindContext) { c.groupingFlag[0] = false }},
		{"inconsistent flags", func(c *BindContext) { c.groupingFlag = c.groupingFlag[:1] }},
		{"sample", func(c *BindContext) { c.sampleFunc.hasSampleFunc = true }},
		{"time window", func(c *BindContext) { c.times = []*planpb.Expr{MakePlan2Int64ConstExprWithType(1)} }},
		{"nil group", func(c *BindContext) { c.groups[1] = nil }},
		{"invalid registry", func(c *BindContext) { c.groupByCanonicalAst["bad"] = 99 }},
		{"missing source anchor", func(c *BindContext) { c.groups[0].GetF().Args[0].GetF().Args[0].GetCol().ColPos = 2 }},
		{"conflicting source type", func(c *BindContext) { c.groups[0].GetF().Args[0].GetF().Args[0].Typ.NotNullable = true }},
		{"invalid overload", func(c *BindContext) { c.groups[0].GetF().Func.Obj++ }},
		{"dynamic group literal", func(c *BindContext) { c.groups[0].GetF().Args[1].GetLit().Src = GetColExpr(c.groups[1].Typ, 7, 0) }},
		{"dynamic consumer provenance", func(c *BindContext) { c.projects[0].PreparedNumeric = &planpb.PreparedNumericMetadata{Fallback: true} }},
		{"sourced consumer literal", func(c *BindContext) {
			c.projects = append(c.projects, MakePlan2Int64ConstExprWithType(1))
			c.projects[2].GetLit().Src = DeepCopyExpr(c.projects[0])
		}},
		{"consumer subquery", func(c *BindContext) {
			c.projects = append(c.projects, &planpb.Expr{Expr: &planpb.Expr_Sub{Sub: &planpb.SubqueryRef{}}})
		}},
		{"consumer correlated ref", func(c *BindContext) {
			c.projects = append(c.projects, &planpb.Expr{Expr: &planpb.Expr_Corr{Corr: &planpb.CorrColRef{RelPos: 7, ColPos: 0}}})
		}},
		{"stale consumer slot", func(c *BindContext) { c.projects[0].GetCol().ColPos = 99 }},
		{"stale consumer type", func(c *BindContext) { c.projects[0].Typ.Width++ }},
		{"post-aggregate input ref", func(c *BindContext) { c.aggregates = []*planpb.Expr{DeepCopyExpr(c.projects[0])} }},
		{"unknown consumer variant", func(c *BindContext) { c.projects = append(c.projects, &planpb.Expr{}) }},
		{"wide consumers exceed work bound", func(c *BindContext) {
			for range derivedGroupRewriteNodes {
				c.projects = append(c.projects, DeepCopyExpr(c.projects[0]))
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := derivedGroupTestContext(t, builder)
			tt.mutate(c)
			beforeGroups, beforeProjects := fmt.Sprint(c.groups), fmt.Sprint(c.projects)
			beforeRegistry := fmt.Sprint(c.groupByAst)
			builder.pruneDerivedGroupBy(c, [][]*planpb.Expr{c.projects}, nil)
			require.Equal(t, beforeGroups, fmt.Sprint(c.groups))
			require.Equal(t, beforeProjects, fmt.Sprint(c.projects))
			require.Equal(t, beforeRegistry, fmt.Sprint(c.groupByAst))
		})
	}
	t.Run("deep expressions exceed work bound", func(t *testing.T) {
		c := derivedGroupTestContext(t, builder)
		for range derivedGroupRewriteDepth {
			c.groups[0] = &planpb.Expr{Typ: c.groups[0].Typ, Expr: &planpb.Expr_F{F: &planpb.Function{Func: c.groups[0].GetF().Func, Args: []*planpb.Expr{c.groups[0], MakePlan2Int64ConstExprWithType(0)}}}}
		}
		original := c.groups[0]
		builder.pruneDerivedGroupBy(c, [][]*planpb.Expr{c.projects}, nil)
		require.Len(t, c.groups, 2)
		require.Same(t, original, c.groups[0])
	})
}

func TestBuildPlanPrunesDerivedGroupKeys(t *testing.T) {
	tests := []struct {
		name, sql string
		keys      int
	}{
		{"sum retains original distribution keys", "select deptno,deptno-1,deptno-2,deptno-3,sum(deptno) from constraint_test.emp group by deptno,deptno-1,deptno-2,deptno-3", 4},
		{"mixed count sum", "select deptno,deptno-1,count(*),sum(deptno) from constraint_test.emp group by deptno,deptno-1", 2},
		{"avg", "select deptno,deptno-1,avg(deptno) from constraint_test.emp group by deptno,deptno-1", 2},
		{"min max", "select deptno,deptno-1,min(deptno),max(deptno) from constraint_test.emp group by deptno,deptno-1", 2},
		{"distinct count", "select deptno,deptno-1,count(distinct mgr) from constraint_test.emp group by deptno,deptno-1", 2},
		{"wide count", "select deptno,deptno-1,count(ename) from constraint_test.emp group by deptno,deptno-1", 2},
		{"computed count", "select deptno,deptno-1,count(length(ename)) from constraint_test.emp group by deptno,deptno-1", 2},
		{"unsupported retained key", "select deptno,ename,deptno-1,count(*) from constraint_test.emp group by deptno,ename,deptno-1", 3},
		{"q36", "select deptno,deptno-1,deptno-2,deptno-3,count(*) c from constraint_test.emp group by deptno,deptno-1,deptno-2,deptno-3 order by c desc limit 10", 1},
		{"alias ordinal reordered", "select e.deptno-1 d,e.deptno,count(*) c from constraint_test.emp e group by 1,2 having d>0 order by 1", 1},
		{"multiple source columns", "select deptno,mgr,cast(deptno as signed)+cast(mgr as signed),count(*) from constraint_test.emp group by cast(deptno as signed)+cast(mgr as signed),deptno,mgr", 2},
		{"nested total arithmetic", "select deptno,-(cast(deptno as signed)*2-3),count(*) from constraint_test.emp group by -(cast(deptno as signed)*2-3),deptno", 1},
		{"unselected derived key", "select count(*) from constraint_test.emp group by deptno,deptno-1", 1},
		{"distinct projection", "select distinct deptno-1,count(*) from constraint_test.emp group by deptno-1,deptno", 1},
		{"window consumer", "select deptno,deptno-1,row_number() over(partition by deptno-1 order by deptno),count(*) from constraint_test.emp group by deptno-1,deptno", 1},
		{"hidden order expression", "select deptno,count(*) from constraint_test.emp group by deptno,deptno-1 order by deptno-1", 1},
		{"no direct anchor", "select deptno-1,deptno-2,count(*) from constraint_test.emp group by deptno-1,deptno-2", 2},
		{"overflow retains key", "select deptno,mgr,deptno+mgr,count(*) from constraint_test.emp group by deptno,mgr,deptno+mgr", 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			optimizer := NewMockOptimizer(false, newPlanTestProcess(t))
			p, err := runOneStmt(optimizer, t, tt.sql)
			require.NoError(t, err)
			found := false
			for _, n := range p.GetQuery().Nodes {
				if n.NodeType == planpb.Node_AGG && len(n.AggList) > 0 {
					found = true
					require.Len(t, n.GroupBy, tt.keys)
				}
			}
			require.True(t, found)
		})
	}
}

func TestPruneDerivedGroupByUnknownStateIsAtomic(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(false, newPlanTestProcess(t)), false, true)
	for _, tc := range []struct {
		name, function string
		typ            types.T
		mixed          bool
	}{
		{"sum-int", "sum", types.T_int32, false},
		{"sum-bigint", "sum", types.T_int64, false},
		{"sum-float", "sum", types.T_float64, false},
		{"sum-decimal", "sum", types.T_decimal128, false},
		{"avg", "avg", types.T_int32, false},
		{"min", "min", types.T_int32, false},
		{"max", "max", types.T_int32, false},
		{"mixed-count-sum", "sum", types.T_int32, true},
		{"wide-count", "count", types.T_varchar, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := derivedGroupTestContext(t, builder)
			arg := GetColExpr(planpb.Type{Id: int32(tc.typ), Width: 20, Scale: 2}, 7, 2)
			agg, err := BindFuncExprImplByPlanExpr(builder.GetContext(), tc.function, []*planpb.Expr{arg})
			require.NoError(t, err)
			ctx.aggregates = []*planpb.Expr{agg}
			if tc.mixed {
				count, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "count", []*planpb.Expr{DeepCopyExpr(ctx.groups[1])})
				require.NoError(t, err)
				ctx.aggregates = append([]*planpb.Expr{count}, ctx.aggregates...)
			}
			order := &planpb.OrderBySpec{Expr: DeepCopyExpr(ctx.projects[0])}
			before := fmt.Sprint(ctx.groups, ctx.aggregates, ctx.projects, ctx.groupingFlag, ctx.groupByAst, ctx.groupByCanonicalAst, ctx.groupByParamAst, order)
			builder.pruneDerivedGroupBy(ctx, [][]*planpb.Expr{ctx.projects}, []*planpb.OrderBySpec{order})
			require.Equal(t, before, fmt.Sprint(ctx.groups, ctx.aggregates, ctx.projects, ctx.groupingFlag, ctx.groupByAst, ctx.groupByCanonicalAst, ctx.groupByParamAst, order))
		})
	}
}
