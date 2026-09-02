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
		emptyOrderBy := &planpb.OrderBySpec{}

		builder.rewriteAffineSumFamilies(
			ctx,
			[][]*planpb.Expr{ctx.projects, having},
			[]*planpb.OrderBySpec{nil, emptyOrderBy, orderBy},
		)

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

	t.Run("central adjacent pair covers the complete safe coefficient radius", func(t *testing.T) {
		zero := makePlan2Int64ConstExprWithType(0)
		ctx := affineTestContext([]*planpb.Expr{
			makeAffineSumFromBaseForTest(t, builder, zero, -maxExactAffineSumInput),
			makeAffineSumFromBaseForTest(t, builder, zero, -maxExactAffineSumInput+1),
			makeAffineSumFromBaseForTest(t, builder, zero, 0),
			makeAffineSumFromBaseForTest(t, builder, zero, 1),
			makeAffineSumFromBaseForTest(t, builder, zero, maxExactAffineSumInput),
		})

		builder.rewriteAffineSumFamilies(ctx, [][]*planpb.Expr{ctx.projects}, nil)

		require.Len(t, ctx.aggregates, 2)
		for i := range ctx.projects {
			require.True(t, sameAffineResultType(ctx.projects[i], ctx.aggregates[0]))
		}
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
		{
			name: "runtime-sourced shift literal",
			aggregates: func() []*planpb.Expr {
				return []*planpb.Expr{
					makeAffineSumForTest(t, builder, types.T_uint16, 0, 1),
					makeAffineSumForTest(t, builder, types.T_uint16, 0, 2),
					makeAffineSumForTest(t, builder, types.T_uint16, 0, 3),
				}
			},
			mutate: func(aggregates []*planpb.Expr) {
				for pos, aggregate := range aggregates {
					shift := aggregate.GetF().Args[0].GetF().Args[1].GetLit()
					require.NotNil(t, shift)
					shift.Src = &planpb.Expr{
						Typ:  planpb.Type{Id: int32(types.T_text)},
						Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: int32(pos)}},
					}
				}
			},
		},
		{
			name: "prepared-metadata shift literal",
			aggregates: func() []*planpb.Expr {
				return []*planpb.Expr{
					makeAffineSumForTest(t, builder, types.T_uint16, 0, 1),
					makeAffineSumForTest(t, builder, types.T_uint16, 0, 2),
					makeAffineSumForTest(t, builder, types.T_uint16, 0, 3),
				}
			},
			mutate: func(aggregates []*planpb.Expr) {
				for pos, aggregate := range aggregates {
					shift := aggregate.GetF().Args[0].GetF().Args[1]
					shift.PreparedNumeric = &planpb.PreparedNumericMetadata{
						Fallback: true,
						ParamPos: int32(pos),
					}
				}
			},
		},
		{
			name: "prepared-metadata aggregate",
			aggregates: func() []*planpb.Expr {
				return []*planpb.Expr{
					makeAffineSumForTest(t, builder, types.T_uint16, 0, 1),
					makeAffineSumForTest(t, builder, types.T_uint16, 0, 2),
					makeAffineSumForTest(t, builder, types.T_uint16, 0, 3),
				}
			},
			mutate: func(aggregates []*planpb.Expr) {
				for pos, aggregate := range aggregates {
					aggregate.PreparedNumeric = &planpb.PreparedNumericMetadata{
						Fallback: true,
						ParamPos: int32(pos),
					}
				}
			},
		},
		{
			name: "prepared-metadata nested base",
			aggregates: func() []*planpb.Expr {
				return []*planpb.Expr{
					makeAffineSumForTest(t, builder, types.T_uint16, 0, 1),
					makeAffineSumForTest(t, builder, types.T_uint16, 0, 2),
					makeAffineSumForTest(t, builder, types.T_uint16, 0, 3),
				}
			},
			mutate: func(aggregates []*planpb.Expr) {
				for pos, aggregate := range aggregates {
					base := aggregate.GetF().Args[0].GetF().Args[0]
					base.PreparedNumeric = &planpb.PreparedNumericMetadata{
						Fallback: true,
						ParamPos: int32(pos),
					}
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

func TestExactAffineIntegerRangeProof(t *testing.T) {
	builder := NewQueryBuilder(
		planpb.Query_SELECT, NewMockCompilerContext(false), false, true)

	literalTests := []struct {
		name string
		expr *planpb.Expr
		want affineIntegerRange
		ok   bool
	}{
		{name: "int8", expr: makePlan2Int8ConstExprWithType(-8), want: affineIntegerRange{min: -8, max: -8}, ok: true},
		{name: "int16", expr: makePlan2Int16ConstExprWithType(-16), want: affineIntegerRange{min: -16, max: -16}, ok: true},
		{name: "int32", expr: makePlan2Int32ConstExprWithType(-32), want: affineIntegerRange{min: -32, max: -32}, ok: true},
		{name: "int64", expr: makePlan2Int64ConstExprWithType(-64), want: affineIntegerRange{min: -64, max: -64}, ok: true},
		{name: "uint8", expr: makePlan2Uint8ConstExprWithType(8), want: affineIntegerRange{min: 8, max: 8}, ok: true},
		{name: "uint16", expr: makePlan2Uint16ConstExprWithType(16), want: affineIntegerRange{min: 16, max: 16}, ok: true},
		{name: "uint32", expr: makePlan2Uint32ConstExprWithType(32), want: affineIntegerRange{min: 32, max: 32}, ok: true},
		{name: "uint64 within signed domain", expr: makePlan2Uint64ConstExprWithType(math.MaxInt64), want: affineIntegerRange{min: math.MaxInt64, max: math.MaxInt64}, ok: true},
		{
			name: "int8 payload outside declared type",
			expr: &planpb.Expr{
				Typ:  planpb.Type{Id: int32(types.T_int8)},
				Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I8Val{I8Val: math.MaxInt8 + 1}}},
			},
		},
		{
			name: "uint16 payload outside declared type",
			expr: &planpb.Expr{
				Typ:  planpb.Type{Id: int32(types.T_uint16)},
				Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_U16Val{U16Val: math.MaxUint16 + 1}}},
			},
		},
		{name: "uint64 outside signed domain", expr: makePlan2Uint64ConstExprWithType(math.MaxUint64)},
		{
			name: "literal payload does not match declared type",
			expr: &planpb.Expr{
				Typ:  planpb.Type{Id: int32(types.T_int32)},
				Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: 1}}},
			},
		},
	}
	for _, test := range literalTests {
		t.Run(test.name, func(t *testing.T) {
			got, ok := exactAffineIntegerRange(builder.GetContext(), test.expr)
			require.Equal(t, test.ok, ok)
			if ok {
				require.Equal(t, test.want, got)
			}
		})
	}

	t.Run("unary operators preserve a proven widened domain", func(t *testing.T) {
		source := GetColExpr(planpb.Type{Id: int32(types.T_int16)}, 7, 0)
		widened, err := appendCastBeforeExpr(
			builder.GetContext(), source, planpb.Type{Id: int32(types.T_int64)})
		require.NoError(t, err)

		for _, test := range []struct {
			name string
			want affineIntegerRange
		}{
			{name: "unary_plus", want: affineIntegerRange{min: math.MinInt16, max: math.MaxInt16}},
			{name: "unary_minus", want: affineIntegerRange{min: -math.MaxInt16, max: -math.MinInt16}},
		} {
			t.Run(test.name, func(t *testing.T) {
				expr, err := BindFuncExprImplByPlanExpr(
					builder.GetContext(), test.name, []*planpb.Expr{DeepCopyExpr(widened)})
				require.NoError(t, err)
				got, ok := exactAffineIntegerRange(builder.GetContext(), expr)
				require.True(t, ok)
				require.Equal(t, test.want, got)
			})
		}
	})

	t.Run("unary minus rejects the full int64 domain", func(t *testing.T) {
		expr, err := BindFuncExprImplByPlanExpr(
			builder.GetContext(), "unary_minus", []*planpb.Expr{
				GetColExpr(planpb.Type{Id: int32(types.T_int64)}, 7, 0),
			})
		require.NoError(t, err)
		_, ok := exactAffineIntegerRange(builder.GetContext(), expr)
		require.False(t, ok)
	})
}

func TestCombineAffineIntegerRanges(t *testing.T) {
	tests := []struct {
		name        string
		op          string
		left, right affineIntegerRange
		want        affineIntegerRange
		ok          bool
	}{
		{
			name: "addition", op: "+",
			left: affineIntegerRange{min: -2, max: 3}, right: affineIntegerRange{min: -4, max: 5},
			want: affineIntegerRange{min: -6, max: 8}, ok: true,
		},
		{
			name: "subtraction", op: "-",
			left: affineIntegerRange{min: -2, max: 3}, right: affineIntegerRange{min: -4, max: 5},
			want: affineIntegerRange{min: -7, max: 7}, ok: true,
		},
		{
			name: "multiplication crosses zero", op: "*",
			left: affineIntegerRange{min: -2, max: 3}, right: affineIntegerRange{min: -4, max: 5},
			want: affineIntegerRange{min: -12, max: 15}, ok: true,
		},
		{
			name: "addition overflows", op: "+",
			left: affineIntegerRange{min: math.MaxInt64, max: math.MaxInt64}, right: affineIntegerRange{min: 1, max: 1},
		},
		{
			name: "subtraction overflows", op: "-",
			left: affineIntegerRange{min: math.MinInt64, max: math.MinInt64}, right: affineIntegerRange{min: 1, max: 1},
		},
		{
			name: "multiplication overflows", op: "*",
			left: affineIntegerRange{min: math.MaxInt64, max: math.MaxInt64}, right: affineIntegerRange{min: 2, max: 2},
		},
		{name: "unsupported operator", op: "/"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, ok := combineAffineIntegerRanges(test.op, test.left, test.right)
			require.Equal(t, test.ok, ok)
			if ok {
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

	minShift := -maxExactAffineSumInput - int64(math.MinInt32)
	ctx = affineTestContext([]*planpb.Expr{
		makeAffineSumForTest(t, builder, types.T_int32, 0, minShift),
		makeAffineSumForTest(t, builder, types.T_int32, 0, minShift+1),
		makeAffineSumForTest(t, builder, types.T_int32, 0, minShift+2),
	})
	builder.rewriteAffineSumFamilies(ctx, [][]*planpb.Expr{ctx.projects}, nil)
	require.Len(t, ctx.aggregates, 2, "negative exact boundary should be symmetric")

	ctx = affineTestContext([]*planpb.Expr{
		makeAffineSumForTest(t, builder, types.T_int32, 0, minShift-1),
		makeAffineSumForTest(t, builder, types.T_int32, 0, minShift),
		makeAffineSumForTest(t, builder, types.T_int32, 0, minShift+1),
	})
	builder.rewriteAffineSumFamilies(ctx, [][]*planpb.Expr{ctx.projects}, nil)
	require.Len(t, ctx.aggregates, 3, "one endpoint below the negative bound must fall back")

	zero := makePlan2Int64ConstExprWithType(0)
	ctx = affineTestContext([]*planpb.Expr{
		makeAffineSumFromBaseForTest(t, builder, zero, -maxExactAffineSumInput),
		makeAffineSumFromBaseForTest(t, builder, zero, -maxExactAffineSumInput+1),
		makeAffineSumFromBaseForTest(t, builder, zero, maxExactAffineSumInput),
	})
	builder.rewriteAffineSumFamilies(ctx, [][]*planpb.Expr{ctx.projects}, nil)
	require.Len(t, ctx.aggregates, 3, "an unrepresentable signed anchor delta must fall back atomically")
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

	for _, invalid := range []*planpb.Expr{
		GetColExpr(aggregates[0].Typ, ctx.aggregateTag, int32(len(aggregates))),
		{Typ: aggregates[0].Typ},
		{Typ: aggregates[0].Typ, Expr: &planpb.Expr_Col{}},
		{Typ: aggregates[0].Typ, Expr: &planpb.Expr_F{}},
		{Typ: aggregates[0].Typ, Expr: &planpb.Expr_F{F: &planpb.Function{
			Args: []*planpb.Expr{GetColExpr(aggregates[0].Typ, ctx.aggregateTag, 0)},
		}}},
		{Typ: aggregates[0].Typ, Expr: &planpb.Expr_List{}},
		{Typ: aggregates[0].Typ, Expr: &planpb.Expr_List{List: &planpb.ExprList{
			List: []*planpb.Expr{{Typ: aggregates[0].Typ}},
		}}},
		{Typ: aggregates[0].Typ, Expr: &planpb.Expr_Sub{}},
		{Typ: aggregates[0].Typ, Expr: &planpb.Expr_W{}},
		{Typ: aggregates[0].Typ, Expr: &planpb.Expr_W{W: &planpb.WindowSpec{
			WindowFunc: GetColExpr(aggregates[0].Typ, ctx.aggregateTag, 0),
		}}},
		{Typ: aggregates[0].Typ, Expr: &planpb.Expr_W{W: &planpb.WindowSpec{
			WindowFunc: GetColExpr(aggregates[0].Typ, ctx.aggregateTag, 0),
			Frame:      &planpb.FrameClause{End: &planpb.FrameBound{}},
		}}},
	} {
		ctx = affineTestContext(aggregates)
		originalProjects = append([]*planpb.Expr(nil), ctx.projects...)
		consumer := []*planpb.Expr{invalid}
		builder.rewriteAffineSumFamilies(
			ctx, [][]*planpb.Expr{ctx.projects, consumer}, nil)
		require.Len(t, ctx.aggregates, 3)
		require.Same(t, invalid, consumer[0])
		for i := range ctx.projects {
			require.Same(t, originalProjects[i], ctx.projects[i])
		}
	}
}

func TestRewriteAffineSumFamiliesNestedConsumers(t *testing.T) {
	builder := NewQueryBuilder(
		planpb.Query_SELECT, NewMockCompilerContext(false), false, true)
	aggregates := []*planpb.Expr{
		makeAffineSumForTest(t, builder, types.T_uint16, 0, 1),
		makeAffineSumForTest(t, builder, types.T_uint16, 0, 2),
		makeAffineSumForTest(t, builder, types.T_uint16, 0, 3),
	}

	tests := []struct {
		name string
		wrap func(*planpb.Expr) *planpb.Expr
	}{
		{
			name: "function",
			wrap: func(ref *planpb.Expr) *planpb.Expr {
				wrapped, err := BindFuncExprImplByPlanExpr(
					builder.GetContext(), "+", []*planpb.Expr{ref, makePlan2Int64ConstExprWithType(0)})
				require.NoError(t, err)
				return wrapped
			},
		},
		{
			name: "list",
			wrap: func(ref *planpb.Expr) *planpb.Expr {
				return &planpb.Expr{Typ: ref.Typ, Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{ref}}}}
			},
		},
		{
			name: "subquery child",
			wrap: func(ref *planpb.Expr) *planpb.Expr {
				return &planpb.Expr{Typ: ref.Typ, Expr: &planpb.Expr_Sub{Sub: &planpb.SubqueryRef{Child: ref}}}
			},
		},
		{
			name: "complete window spec",
			wrap: func(ref *planpb.Expr) *planpb.Expr {
				return &planpb.Expr{Typ: ref.Typ, Expr: &planpb.Expr_W{W: &planpb.WindowSpec{
					WindowFunc:  DeepCopyExpr(ref),
					PartitionBy: []*planpb.Expr{DeepCopyExpr(ref)},
					OrderBy:     []*planpb.OrderBySpec{{Expr: DeepCopyExpr(ref)}},
					Frame: &planpb.FrameClause{
						Start: &planpb.FrameBound{Val: DeepCopyExpr(ref)},
						End:   &planpb.FrameBound{Val: DeepCopyExpr(ref)},
					},
				}}}
			},
		},
		{
			name: "sparse window spec",
			wrap: func(ref *planpb.Expr) *planpb.Expr {
				return &planpb.Expr{Typ: ref.Typ, Expr: &planpb.Expr_W{W: &planpb.WindowSpec{
					WindowFunc: DeepCopyExpr(ref),
					OrderBy: []*planpb.OrderBySpec{
						nil,
						{Expr: DeepCopyExpr(ref)},
					},
					Frame: &planpb.FrameClause{
						Start: &planpb.FrameBound{},
						End:   &planpb.FrameBound{Val: DeepCopyExpr(ref)},
					},
				}}}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := affineTestContext(aggregates)
			consumer := []*planpb.Expr{test.wrap(DeepCopyExpr(ctx.projects[2]))}

			builder.rewriteAffineSumFamilies(
				ctx, [][]*planpb.Expr{ctx.projects, consumer}, nil)

			require.Len(t, ctx.aggregates, 2)
			derivedCount := 0
			require.NoError(t, planpb.VisitExprTree(consumer[0], func(expr *planpb.Expr) error {
				if col := expr.GetCol(); col != nil && col.RelPos == ctx.aggregateTag {
					require.Less(t, col.ColPos, int32(len(ctx.aggregates)))
				}
				if fn := expr.GetF(); fn != nil && fn.Func != nil && fn.Func.ObjName == "+" {
					derivedCount++
				}
				return nil
			}))
			require.Positive(t, derivedCount)
		})
	}

	ctx := affineTestContext(aggregates)
	nonMatchingCorrelation := []*planpb.Expr{{
		Typ: aggregates[2].Typ,
		Expr: &planpb.Expr_Corr{Corr: &planpb.CorrColRef{
			RelPos: ctx.aggregateTag + 1,
			ColPos: 2,
			Depth:  1,
		}},
	}}
	builder.rewriteAffineSumFamilies(
		ctx, [][]*planpb.Expr{ctx.projects, nonMatchingCorrelation}, nil)
	require.Len(t, ctx.aggregates, 2)
	require.NotNil(t, nonMatchingCorrelation[0].GetCorr())
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

	t.Run("aggregate results consumed by windows", func(t *testing.T) {
		require.Equal(t, 2, physicalAggregateCount(t,
			"select sum(sum(attr_seqnum + 1)) over (), "+
				"sum(sum(attr_seqnum + 2)) over (), "+
				"sum(sum(attr_seqnum + 3)) over () "+
				"from mo_catalog.mo_columns"))
	})

	t.Run("prepared shifts remain physical", func(t *testing.T) {
		prepare := buildPreparedAggregatePlan(t,
			"select sum(attr_seqnum + ?), sum(attr_seqnum + ?), "+
				"sum(attr_seqnum + ?) from mo_catalog.mo_columns")
		physical := -1
		for _, node := range prepare.Plan.GetQuery().Nodes {
			if node.NodeType == planpb.Node_AGG {
				physical = len(node.AggList)
				break
			}
		}
		require.Equal(t, 3, physical)
	})

	t.Run("time-window and fill consumers", func(t *testing.T) {
		mock := NewMockOptimizer(false)
		mockTimeWindowScaleTable(t, mock, types.T_datetime.ToType())
		logicPlan, err := runOneStmt(mock, t,
			"select _wstart, sum(v + 1), sum(v + 2), sum(v + 3) "+
				"from tw_scale interval(ts, 10, minute) sliding(5, minute) fill(linear)")
		require.NoError(t, err)

		var aggregateNode, timeWindowNode, fillNode *planpb.Node
		for _, node := range logicPlan.GetQuery().Nodes {
			switch node.NodeType {
			case planpb.Node_AGG:
				aggregateNode = node
			case planpb.Node_TIME_WINDOW:
				timeWindowNode = node
			case planpb.Node_FILL:
				fillNode = node
			}
		}
		require.NotNil(t, aggregateNode)
		require.Len(t, aggregateNode.AggList, 2)
		require.NotNil(t, timeWindowNode)
		require.Len(t, timeWindowNode.AggList, 4, "boundary carrier plus three result aggregates")
		require.NotNil(t, fillNode)
		require.Len(t, fillNode.AggList, 3)
	})
}
