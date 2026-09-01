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
	"math"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func makeAffineSumForTest(
	t *testing.T,
	builder *QueryBuilder,
	sourceType types.T,
	sourcePos int32,
	shift int64,
) *planpb.Expr {
	t.Helper()
	source := GetColExpr(planpb.Type{Id: int32(sourceType)}, 7, sourcePos)
	base, err := appendCastBeforeExpr(
		builder.GetContext(), source, planpb.Type{Id: int32(types.T_int64)})
	require.NoError(t, err)
	return makeAffineSumFromBaseForTest(t, builder, base, shift)
}

func makeAffineSumFromBaseForTest(
	t *testing.T,
	builder *QueryBuilder,
	base *planpb.Expr,
	shift int64,
) *planpb.Expr {
	t.Helper()
	add, err := BindFuncExprImplByPlanExpr(
		builder.GetContext(), "+", []*planpb.Expr{DeepCopyExpr(base), makePlan2Int64ConstExprWithType(shift)})
	require.NoError(t, err)
	aggregate, err := BindFuncExprImplByPlanExpr(
		builder.GetContext(), "sum", []*planpb.Expr{add})
	require.NoError(t, err)
	return aggregate
}

func makeSumFromBaseForTest(
	t *testing.T,
	builder *QueryBuilder,
	base *planpb.Expr,
) *planpb.Expr {
	t.Helper()
	aggregate, err := BindFuncExprImplByPlanExpr(
		builder.GetContext(), "sum", []*planpb.Expr{DeepCopyExpr(base)})
	require.NoError(t, err)
	return aggregate
}

func affineTestContext(aggregates []*planpb.Expr) *BindContext {
	const aggregateTag int32 = 11
	projects := make([]*planpb.Expr, len(aggregates))
	for i, aggregate := range aggregates {
		projects[i] = GetColExpr(aggregate.Typ, aggregateTag, int32(i))
	}
	return &BindContext{
		aggregateTag:   aggregateTag,
		aggregates:     aggregates,
		projects:       projects,
		aggregateByAst: map[string]int32{"old": int32(len(aggregates) - 1)},
	}
}

func TestRewriteAffineSumFamilies(t *testing.T) {
	builder := NewQueryBuilder(
		planpb.Query_SELECT, NewMockCompilerContext(false), false, true)

	t.Run("compact and rewrite every direct consumer", func(t *testing.T) {
		aggregates := []*planpb.Expr{
			makeAffineSumForTest(t, builder, types.T_uint16, 0, 1),
			makeAffineSumForTest(t, builder, types.T_uint16, 0, 2),
			makeAffineSumForTest(t, builder, types.T_uint16, 0, 3),
			makeAffineSumForTest(t, builder, types.T_uint16, 0, 89),
		}
		ctx := affineTestContext(aggregates)
		ctx.aggregateByAst = map[string]int32{
			"anchor":  0,
			"derived": 3,
		}
		having := []*planpb.Expr{DeepCopyExpr(ctx.projects[3])}
		orderBy := &planpb.OrderBySpec{Expr: DeepCopyExpr(ctx.projects[2])}

		builder.rewriteAffineSumFamilies(
			ctx, [][]*planpb.Expr{ctx.projects, having}, []*planpb.OrderBySpec{orderBy})

		require.Len(t, ctx.aggregates, 2)
		require.Equal(t, map[string]int32{"anchor": 0}, ctx.aggregateByAst)
		require.Equal(t, int32(0), ctx.projects[0].GetCol().ColPos)
		require.Equal(t, int32(1), ctx.projects[1].GetCol().ColPos)
		require.Equal(t, "+", ctx.projects[2].GetF().Func.ObjName)
		require.Equal(t, "+", ctx.projects[3].GetF().Func.ObjName)
		require.Equal(t, "+", having[0].GetF().Func.ObjName)
		require.Equal(t, "+", orderBy.Expr.GetF().Func.ObjName)
		for i := range ctx.projects {
			require.True(t, sameAffineResultType(ctx.projects[i], aggregates[i]))
		}
	})

	for _, sourceType := range []types.T{types.T_uint8, types.T_uint16, types.T_uint32} {
		t.Run(sourceType.String()+" is exact", func(t *testing.T) {
			ctx := affineTestContext([]*planpb.Expr{
				makeAffineSumForTest(t, builder, sourceType, 0, 0),
				makeAffineSumForTest(t, builder, sourceType, 0, 1),
				makeAffineSumForTest(t, builder, sourceType, 0, 2),
			})
			builder.rewriteAffineSumFamilies(ctx, [][]*planpb.Expr{ctx.projects}, nil)
			require.Len(t, ctx.aggregates, 2)
		})
	}
	for _, sourceType := range []types.T{types.T_int8, types.T_int16, types.T_int32} {
		t.Run(sourceType.String()+" signed domain is exact", func(t *testing.T) {
			ctx := affineTestContext([]*planpb.Expr{
				makeAffineSumForTest(t, builder, sourceType, 0, -1),
				makeAffineSumForTest(t, builder, sourceType, 0, 0),
				makeAffineSumForTest(t, builder, sourceType, 0, 1),
			})
			builder.rewriteAffineSumFamilies(ctx, [][]*planpb.Expr{ctx.projects}, nil)
			require.Len(t, ctx.aggregates, 2)
		})
	}

	t.Run("compound exact integer base", func(t *testing.T) {
		source := GetColExpr(planpb.Type{Id: int32(types.T_uint16)}, 7, 0)
		cast, err := appendCastBeforeExpr(
			builder.GetContext(), source, planpb.Type{Id: int32(types.T_int64)})
		require.NoError(t, err)
		base, err := BindFuncExprImplByPlanExpr(
			builder.GetContext(), "*", []*planpb.Expr{cast, makePlan2Int64ConstExprWithType(2)})
		require.NoError(t, err)
		ctx := affineTestContext([]*planpb.Expr{
			makeAffineSumFromBaseForTest(t, builder, base, 1),
			makeAffineSumFromBaseForTest(t, builder, base, 2),
			makeAffineSumFromBaseForTest(t, builder, base, 3),
		})
		builder.rewriteAffineSumFamilies(ctx, [][]*planpb.Expr{ctx.projects}, nil)
		require.Len(t, ctx.aggregates, 2)
	})

	t.Run("explicit exact widening cast", func(t *testing.T) {
		source := GetColExpr(planpb.Type{Id: int32(types.T_int16)}, 7, 0)
		base, err := appendExplicitCastBeforeExpr(
			builder.GetContext(), source, planpb.Type{Id: int32(types.T_int64)})
		require.NoError(t, err)
		ctx := affineTestContext([]*planpb.Expr{
			makeAffineSumFromBaseForTest(t, builder, base, -1),
			makeAffineSumFromBaseForTest(t, builder, base, 0),
			makeAffineSumFromBaseForTest(t, builder, base, 1),
		})

		builder.rewriteAffineSumFamilies(ctx, [][]*planpb.Expr{ctx.projects}, nil)

		require.Len(t, ctx.aggregates, 2)
	})

	t.Run("any adjacent pair can anchor the family", func(t *testing.T) {
		ctx := affineTestContext([]*planpb.Expr{
			makeAffineSumForTest(t, builder, types.T_uint16, 0, 1),
			makeAffineSumForTest(t, builder, types.T_uint16, 0, 3),
			makeAffineSumForTest(t, builder, types.T_uint16, 0, 4),
		})
		builder.rewriteAffineSumFamilies(ctx, [][]*planpb.Expr{ctx.projects}, nil)
		require.Len(t, ctx.aggregates, 2)
		require.Equal(t, "+", ctx.projects[0].GetF().Func.ObjName)
	})

	t.Run("subtraction and unshifted forms share one base", func(t *testing.T) {
		source := GetColExpr(planpb.Type{Id: int32(types.T_int16)}, 7, 0)
		base, err := appendCastBeforeExpr(
			builder.GetContext(), source, planpb.Type{Id: int32(types.T_int64)})
		require.NoError(t, err)
		minusOne, err := BindFuncExprImplByPlanExpr(
			builder.GetContext(), "-", []*planpb.Expr{
				DeepCopyExpr(base), makePlan2Int64ConstExprWithType(1),
			})
		require.NoError(t, err)
		minusOneSum := makeSumFromBaseForTest(t, builder, minusOne)
		ctx := affineTestContext([]*planpb.Expr{
			minusOneSum,
			makeSumFromBaseForTest(t, builder, base),
			makeAffineSumFromBaseForTest(t, builder, base, 1),
		})

		builder.rewriteAffineSumFamilies(ctx, [][]*planpb.Expr{ctx.projects}, nil)

		require.Len(t, ctx.aggregates, 2)
		require.Equal(t, "+", ctx.projects[2].GetF().Func.ObjName)
	})

	tests := []struct {
		name       string
		aggregates func() []*planpb.Expr
		mutate     func([]*planpb.Expr)
	}{
		{
			name: "uint64 source",
			aggregates: func() []*planpb.Expr {
				return []*planpb.Expr{
					makeAffineSumForTest(t, builder, types.T_uint64, 0, 1),
					makeAffineSumForTest(t, builder, types.T_uint64, 0, 2),
					makeAffineSumForTest(t, builder, types.T_uint64, 0, 3),
				}
			},
		},
		{
			name: "no adjacent shifts",
			aggregates: func() []*planpb.Expr {
				return []*planpb.Expr{
					makeAffineSumForTest(t, builder, types.T_uint16, 0, 1),
					makeAffineSumForTest(t, builder, types.T_uint16, 0, 3),
					makeAffineSumForTest(t, builder, types.T_uint16, 0, 5),
				}
			},
		},
		{
			name: "only two members",
			aggregates: func() []*planpb.Expr {
				return []*planpb.Expr{
					makeAffineSumForTest(t, builder, types.T_uint16, 0, 1),
					makeAffineSumForTest(t, builder, types.T_uint16, 0, 2),
				}
			},
		},
		{
			name: "different bases",
			aggregates: func() []*planpb.Expr {
				return []*planpb.Expr{
					makeAffineSumForTest(t, builder, types.T_uint16, 0, 1),
					makeAffineSumForTest(t, builder, types.T_uint16, 0, 2),
					makeAffineSumForTest(t, builder, types.T_uint16, 1, 3),
				}
			},
		},
		{
			name: "aggregate domain overflow",
			aggregates: func() []*planpb.Expr {
				return []*planpb.Expr{
					makeAffineSumForTest(t, builder, types.T_uint32, 0, math.MaxInt64-1),
					makeAffineSumForTest(t, builder, types.T_uint32, 0, math.MaxInt64),
					makeAffineSumForTest(t, builder, types.T_uint32, 0, math.MaxInt64-2),
				}
			},
		},
		{
			name: "distinct aggregate",
			aggregates: func() []*planpb.Expr {
				return []*planpb.Expr{
					makeAffineSumForTest(t, builder, types.T_uint16, 0, 1),
					makeAffineSumForTest(t, builder, types.T_uint16, 0, 2),
					makeAffineSumForTest(t, builder, types.T_uint16, 0, 3),
				}
			},
			mutate: func(aggregates []*planpb.Expr) {
				for _, aggregate := range aggregates {
					aggregate.GetF().Func.Obj = int64(
						uint64(aggregate.GetF().Func.Obj) | function.Distinct)
				}
			},
		},
		{
			name: "aggregate configuration",
			aggregates: func() []*planpb.Expr {
				return []*planpb.Expr{
					makeAffineSumForTest(t, builder, types.T_uint16, 0, 1),
					makeAffineSumForTest(t, builder, types.T_uint16, 0, 2),
					makeAffineSumForTest(t, builder, types.T_uint16, 0, 3),
				}
			},
			mutate: func(aggregates []*planpb.Expr) {
				for _, aggregate := range aggregates {
					aggregate.GetF().AggConfigType =
						planpb.AggregateConfigType_AGG_CONFIG_GROUP_CONCAT_ORDER
				}
			},
		},
		{
			name: "malformed arithmetic function id",
			aggregates: func() []*planpb.Expr {
				return []*planpb.Expr{
					makeAffineSumForTest(t, builder, types.T_uint16, 0, 1),
					makeAffineSumForTest(t, builder, types.T_uint16, 0, 2),
					makeAffineSumForTest(t, builder, types.T_uint16, 0, 3),
				}
			},
			mutate: func(aggregates []*planpb.Expr) {
				for _, aggregate := range aggregates {
					aggregate.GetF().Args[0].GetF().Func.Obj++
				}
			},
		},
		{
			name: "malformed cast target",
			aggregates: func() []*planpb.Expr {
				source := GetColExpr(planpb.Type{Id: int32(types.T_int16)}, 7, 0)
				base, err := appendExplicitCastBeforeExpr(
					builder.GetContext(), source, planpb.Type{Id: int32(types.T_int64)})
				require.NoError(t, err)
				return []*planpb.Expr{
					makeAffineSumFromBaseForTest(t, builder, base, 1),
					makeAffineSumFromBaseForTest(t, builder, base, 2),
					makeAffineSumFromBaseForTest(t, builder, base, 3),
				}
			},
			mutate: func(aggregates []*planpb.Expr) {
				for _, aggregate := range aggregates {
					cast := aggregate.GetF().Args[0].GetF().Args[0]
					cast.GetF().Args[1].Typ.Id = int32(types.T_decimal128)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name+" falls back", func(t *testing.T) {
			aggregates := test.aggregates()
			if test.mutate != nil {
				test.mutate(aggregates)
			}
			ctx := affineTestContext(aggregates)
			originalProjects := append([]*planpb.Expr(nil), ctx.projects...)
			builder.rewriteAffineSumFamilies(ctx, [][]*planpb.Expr{ctx.projects}, nil)
			require.Len(t, ctx.aggregates, len(aggregates))
			for i := range ctx.projects {
				require.Same(t, originalProjects[i], ctx.projects[i])
			}
		})
	}
}

func TestCheckedAffineInt64Arithmetic(t *testing.T) {
	tests := []struct {
		name      string
		operation func(int64, int64) (int64, bool)
		left      int64
		right     int64
		want      int64
		wantFits  bool
	}{
		{name: "add maximum", operation: checkedAffineAdd, left: math.MaxInt64 - 1, right: 1, want: math.MaxInt64, wantFits: true},
		{name: "add positive overflow", operation: checkedAffineAdd, left: math.MaxInt64, right: 1},
		{name: "add negative overflow", operation: checkedAffineAdd, left: math.MinInt64, right: -1},
		{name: "subtract minimum", operation: checkedAffineSub, left: math.MinInt64 + 1, right: 1, want: math.MinInt64, wantFits: true},
		{name: "subtract positive overflow", operation: checkedAffineSub, left: math.MinInt64, right: 1},
		{name: "subtract negative overflow", operation: checkedAffineSub, left: math.MaxInt64, right: -1},
		{name: "subtract negative", operation: checkedAffineSub, left: math.MinInt64, right: -1, want: math.MinInt64 + 1, wantFits: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, fits := test.operation(test.left, test.right)
			require.Equal(t, test.wantFits, fits)
			if fits {
				require.Equal(t, test.want, got)
			}
		})
	}
}

func TestRewriteAffineSumFamiliesMaximumSafeDomain(t *testing.T) {
	builder := NewQueryBuilder(
		planpb.Query_SELECT, NewMockCompilerContext(false), false, true)
	maxShift := maxExactAffineSumInput - int64(math.MaxUint32)
	ctx := affineTestContext([]*planpb.Expr{
		makeAffineSumForTest(t, builder, types.T_uint32, 0, maxShift-2),
		makeAffineSumForTest(t, builder, types.T_uint32, 0, maxShift-1),
		makeAffineSumForTest(t, builder, types.T_uint32, 0, maxShift),
	})

	builder.rewriteAffineSumFamilies(ctx, [][]*planpb.Expr{ctx.projects}, nil)

	require.Len(t, ctx.aggregates, 2)
}

func TestRewriteAffineSumFamiliesIsAtomic(t *testing.T) {
	builder := NewQueryBuilder(
		planpb.Query_SELECT, NewMockCompilerContext(false), false, true)
	aggregates := []*planpb.Expr{
		makeAffineSumForTest(t, builder, types.T_uint16, 0, 1),
		makeAffineSumForTest(t, builder, types.T_uint16, 0, 2),
		makeAffineSumForTest(t, builder, types.T_uint16, 0, 3),
	}
	ctx := affineTestContext(aggregates)
	originalProjects := append([]*planpb.Expr(nil), ctx.projects...)
	correlated := &planpb.Expr{
		Typ: aggregates[2].Typ,
		Expr: &planpb.Expr_Corr{Corr: &planpb.CorrColRef{
			RelPos: ctx.aggregateTag,
			ColPos: 2,
			Depth:  1,
		}},
	}

	builder.rewriteAffineSumFamilies(
		ctx, [][]*planpb.Expr{ctx.projects, {correlated}}, nil)

	require.Len(t, ctx.aggregates, 3)
	require.NotEmpty(t, ctx.aggregateByAst)
	for i := range ctx.projects {
		require.Same(t, originalProjects[i], ctx.projects[i])
	}
}

func TestAffineSumFamilyPlanShape(t *testing.T) {
	physicalAggregateCount := func(t *testing.T, sql string) int {
		t.Helper()
		logicPlan, err := runOneStmt(NewMockOptimizer(false), t, sql)
		require.NoError(t, err)
		query := logicPlan.GetQuery()
		require.NotNil(t, query)
		for _, node := range query.Nodes {
			if node.NodeType == planpb.Node_AGG {
				return len(node.AggList)
			}
		}
		require.FailNow(t, "aggregate node not found")
		return 0
	}

	t.Run("Q30 scale shape retains one unshifted sum and two anchors", func(t *testing.T) {
		var sql strings.Builder
		sql.WriteString("select sum(attr_seqnum)")
		for shift := 1; shift <= 89; shift++ {
			_, _ = fmt.Fprintf(&sql, ", sum(attr_seqnum + %d)", shift)
		}
		sql.WriteString(" from mo_catalog.mo_columns")
		require.Equal(t, 3, physicalAggregateCount(t, sql.String()))
	})

	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "every aggregate consumer",
			sql: "select sum(attr_seqnum + 1), sum(attr_seqnum + 2), " +
				"sum(attr_seqnum + 3), sum(attr_seqnum + 89) " +
				"from mo_catalog.mo_columns " +
				"having sum(attr_seqnum + 3) >= 0 " +
				"order by sum(attr_seqnum + 89)",
		},
		{
			name: "signed and unshifted",
			sql: "select sum(attnum - 1), sum(attnum + 0), sum(attnum + 1) " +
				"from mo_catalog.mo_columns",
		},
		{
			name: "compound exact base",
			sql: "select sum(attr_seqnum * 2 + 1), sum(attr_seqnum * 2 + 2), " +
				"sum(attr_seqnum * 2 + 3) from mo_catalog.mo_columns",
		},
		{
			name: "anchor above minimum shift",
			sql: "select sum(attr_seqnum + 1), sum(attr_seqnum + 3), " +
				"sum(attr_seqnum + 4) from mo_catalog.mo_columns",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, 2, physicalAggregateCount(t, test.sql))
		})
	}
}
