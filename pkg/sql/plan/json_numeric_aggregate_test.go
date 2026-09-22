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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func jsonNumericAggregatePlanColumn() *planpb.Expr {
	return &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_json)},
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
			RelPos: 1,
			ColPos: 1,
		}},
	}
}

func jsonNumericAggregateNotNullPlanColumn() *planpb.Expr {
	column := jsonNumericAggregatePlanColumn()
	column.Typ.NotNullable = true
	return column
}

func requireJSONNumericAggregateBindingForColumn(
	t *testing.T, name string, column *planpb.Expr,
) *planpb.Expr {
	t.Helper()
	expr, err := BindFuncExprImplByPlanExpr(
		context.Background(), name, []*planpb.Expr{column})
	require.NoError(t, err)
	require.Equal(t, int32(types.T_float64), expr.Typ.Id)
	require.NotNil(t, expr.GetF())
	require.Len(t, expr.GetF().Args, 1)
	arg := expr.GetF().Args[0]
	require.Equal(t, int32(types.T_float64), arg.Typ.Id)
	require.True(t, isCastOverload(arg, 0), "JSON aggregate argument must use implicit CAST")
	require.NotNil(t, arg.GetF())
	require.Len(t, arg.GetF().Args, 2)
	require.Equal(t, int32(types.T_json), arg.GetF().Args[0].Typ.Id)
	require.Equal(t, int32(types.T_float64), arg.GetF().Args[1].Typ.Id)
	return expr
}

func requireJSONNumericAggregateBinding(t *testing.T, name string) *planpb.Expr {
	return requireJSONNumericAggregateBindingForColumn(
		t, name, jsonNumericAggregatePlanColumn())
}

func TestJSONNumericAggregateBindingUsesDoubleDomain(t *testing.T) {
	for _, name := range []string{
		"sum", "avg", "var_pop", "var_samp", "stddev_pop", "stddev_samp",
		"variance", "std", "stddev",
	} {
		t.Run(name, func(t *testing.T) {
			requireJSONNumericAggregateBinding(t, name)
		})
	}
}

func TestJSONNumericAggregateCastDoesNotInheritJSONNotNull(t *testing.T) {
	for _, name := range []string{"sum", "avg"} {
		t.Run(name, func(t *testing.T) {
			expr, err := BindFuncExprImplByPlanExpr(
				context.Background(), name, []*planpb.Expr{jsonNumericAggregateNotNullPlanColumn()})
			require.NoError(t, err)
			require.Len(t, expr.GetF().Args, 1)
			cast := expr.GetF().Args[0]
			require.True(t, isCastOverload(cast, 0))
			require.False(t, cast.Typ.NotNullable,
				"JSON literal null must remain SQL NULL after JSON-to-DOUBLE cast")
		})
	}
}

func TestNumericCastKeepsNotNullSourceContract(t *testing.T) {
	source := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_int64), NotNullable: true},
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
			RelPos: 1,
			ColPos: 1,
		}},
	}
	cast, err := appendCastBeforeExpr(
		context.Background(), source, planpb.Type{Id: int32(types.T_float64)})
	require.NoError(t, err)
	require.True(t, cast.Typ.NotNullable,
		"non-JSON numeric casts must preserve a NOT NULL source")
}

func TestJSONNumericAggregateBindingRejectsWrongShapes(t *testing.T) {
	ctx := context.Background()
	for _, name := range []string{
		"sum", "avg", "var_pop", "var_samp", "stddev_pop", "stddev_samp",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := BindFuncExprImplByPlanExpr(ctx, name, nil)
			require.Error(t, err)
			_, err = BindFuncExprImplByPlanExpr(ctx, name, []*planpb.Expr{
				jsonNumericAggregatePlanColumn(), jsonNumericAggregatePlanColumn(),
			})
			require.Error(t, err)
			_, err = BindFuncExprImplByPlanExpr(ctx, name, []*planpb.Expr{{
				Typ:  planpb.Type{Id: int32(types.T_bool)},
				Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 1, ColPos: 1}},
			}})
			require.Error(t, err)
		})
	}
}

func TestNumericAggregateBindingPreservesExistingDomains(t *testing.T) {
	ctx := context.Background()
	int64Column := func() *planpb.Expr {
		return &planpb.Expr{
			Typ:  planpb.Type{Id: int32(types.T_int64)},
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 1, ColPos: 1}},
		}
	}

	for _, name := range []string{
		"sum", "avg", "var_pop", "var_samp", "stddev_pop", "stddev_samp",
	} {
		t.Run(name, func(t *testing.T) {
			expr, err := BindFuncExprImplByPlanExpr(ctx, name, []*planpb.Expr{int64Column()})
			require.NoError(t, err)
			require.Len(t, expr.GetF().Args, 1)
			require.Equal(t, int32(types.T_int64), expr.GetF().Args[0].Typ.Id)
			require.False(t, isCastOverload(expr.GetF().Args[0], 0))
			if name == "sum" {
				require.Equal(t, int32(types.T_decimal128), expr.Typ.Id)
				require.Equal(t, int32(38), expr.Typ.Width)
				require.Zero(t, expr.Typ.Scale)
			} else if name == "avg" {
				require.Equal(t, int32(types.T_decimal128), expr.Typ.Id)
				require.Equal(t, int32(23), expr.Typ.Width)
				require.Equal(t, int32(4), expr.Typ.Scale)
			} else {
				require.Equal(t, int32(types.T_float64), expr.Typ.Id)
			}
		})
	}

	for _, name := range []string{"sum", "avg"} {
		t.Run(name+" distinct rewrite", func(t *testing.T) {
			bound := requireJSONNumericAggregateBinding(t, name)
			bound.GetF().Func.Obj = int64(uint64(bound.GetF().Func.Obj) | function.Distinct)
			builder, outer := newDistinctAggTestBuilder(
				1_000, 10, 100, []*planpb.Expr{bound})
			require.NoError(t, builder.optimizeDistinctAgg(1))
			require.Len(t, builder.qry.Nodes, 3)
			inner := builder.qry.Nodes[2]
			require.Len(t, inner.GroupBy, 2)
			require.Equal(t, int32(types.T_float64), inner.GroupBy[1].Typ.Id)
			require.True(t, isCastOverload(inner.GroupBy[1], 0))
			require.False(t, inner.GroupBy[1].Typ.NotNullable,
				"the DISTINCT group key must retain JSON literal NULL")
			require.Zero(t, uint64(outer.AggList[0].GetF().Func.Obj)&function.Distinct)
			require.Equal(t, int32(types.T_float64), outer.AggList[0].GetF().Args[0].Typ.Id)
			require.False(t, outer.AggList[0].GetF().Args[0].Typ.NotNullable)
		})

		t.Run(name+" NOT NULL source distinct rewrite", func(t *testing.T) {
			bound := requireJSONNumericAggregateBindingForColumn(
				t, name, jsonNumericAggregateNotNullPlanColumn())
			bound.GetF().Func.Obj = int64(uint64(bound.GetF().Func.Obj) | function.Distinct)
			builder, outer := newDistinctAggTestBuilder(
				1_000, 10, 100, []*planpb.Expr{bound})
			require.NoError(t, builder.optimizeDistinctAgg(1))
			require.Len(t, builder.qry.Nodes, 3)
			inner := builder.qry.Nodes[2]
			require.Len(t, inner.GroupBy, 2)
			require.False(t, inner.GroupBy[1].Typ.NotNullable,
				"NOT NULL JSON source must not make JSON-to-DOUBLE key NOT NULL")
			require.False(t, outer.AggList[0].GetF().Args[0].Typ.NotNullable)
		})

		t.Run(name+" sibling aggregate keeps bound argument", func(t *testing.T) {
			bound := requireJSONNumericAggregateBinding(t, name)
			bound.GetF().Func.Obj = int64(uint64(bound.GetF().Func.Obj) | function.Distinct)
			sibling := distinctAggTestExpr(
				function.SUM, false, planpb.Type{Id: int32(types.T_float64)},
				distinctAggTestCol(types.T_float64, 1, 2, 100))
			builder, outer := newDistinctAggTestBuilder(
				1_000, 10, 100, []*planpb.Expr{bound, sibling})
			require.NoError(t, builder.optimizeDistinctAgg(1))
			require.Len(t, builder.qry.Nodes, 2)
			require.True(t, uint64(outer.AggList[0].GetF().Func.Obj)&function.Distinct != 0)
			require.Equal(t, int32(types.T_float64), outer.AggList[0].GetF().Args[0].Typ.Id)
			require.True(t, isCastOverload(outer.AggList[0].GetF().Args[0], 0))
		})

		t.Run(name+" sibling aggregate keeps NULLable bound argument", func(t *testing.T) {
			bound := requireJSONNumericAggregateBindingForColumn(
				t, name, jsonNumericAggregateNotNullPlanColumn())
			bound.GetF().Func.Obj = int64(uint64(bound.GetF().Func.Obj) | function.Distinct)
			sibling := distinctAggTestExpr(
				function.SUM, false, planpb.Type{Id: int32(types.T_float64)},
				distinctAggTestCol(types.T_float64, 1, 2, 100))
			builder, outer := newDistinctAggTestBuilder(
				1_000, 10, 100, []*planpb.Expr{bound, sibling})
			require.NoError(t, builder.optimizeDistinctAgg(1))
			require.Len(t, builder.qry.Nodes, 2)
			require.True(t, uint64(outer.AggList[0].GetF().Func.Obj)&function.Distinct != 0)
			require.False(t, outer.AggList[0].GetF().Args[0].Typ.NotNullable)
		})
	}
}

func TestJSONNumericAggregateWindowUsesDoubleDomain(t *testing.T) {
	for _, name := range []string{
		"sum", "avg", "var_pop", "var_samp", "stddev_pop", "stddev_samp",
	} {
		t.Run(name, func(t *testing.T) {
			optimizer := NewMockOptimizer(true)
			table := DeepCopyTableDef(optimizer.ctxt.tables["nation"], true)
			table.Cols[1].Typ = planpb.Type{Id: int32(types.T_json)}
			optimizer.ctxt.tables["nation"] = table

			logicPlan, err := runOneStmt(optimizer, t,
				"select "+name+"(n_name) over (partition by n_regionkey order by n_nationkey rows between unbounded preceding and current row) from nation")
			require.NoError(t, err)
			var found bool
			for _, node := range logicPlan.GetQuery().Nodes {
				if node.NodeType != planpb.Node_WINDOW {
					continue
				}
				for _, item := range node.WinSpecList {
					window := item.GetW()
					if window == nil || window.WindowFunc == nil {
						continue
					}
					fn := window.WindowFunc.GetF()
					if fn == nil || fn.Func == nil || fn.Func.ObjName != name {
						continue
					}
					found = true
					require.Equal(t, int32(types.T_float64), window.WindowFunc.Typ.Id)
					require.Len(t, fn.Args, 1)
					require.Equal(t, int32(types.T_float64), fn.Args[0].Typ.Id)
					require.True(t, isCastOverload(fn.Args[0], 0))
				}
			}
			require.True(t, found, "window function %s was not found", name)
		})
	}
}
