// Copyright 2022 Matrix Origin
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
	"strconv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

type stubWindowBinder struct {
	bindExprFunc       func(tree.Expr, int32, bool) (*planpb.Expr, error)
	bindFuncExprFunc   func(string, []tree.Expr, int32) (*planpb.Expr, error)
	makeFrameValueFunc func(tree.Expr, *planpb.Type) (*planpb.Expr, error)
}

func (b *stubWindowBinder) BindExpr(expr tree.Expr, depth int32, isRoot bool) (*planpb.Expr, error) {
	return b.bindExprFunc(expr, depth, isRoot)
}

func (b *stubWindowBinder) bindFuncExprImplByAstExpr(name string, args []tree.Expr, depth int32) (*planpb.Expr, error) {
	return b.bindFuncExprFunc(name, args, depth)
}

func (b *stubWindowBinder) makeFrameConstValue(expr tree.Expr, typ *planpb.Type) (*planpb.Expr, error) {
	return b.makeFrameValueFunc(expr, typ)
}

func (b *stubWindowBinder) GetContext() context.Context {
	return context.Background()
}

func testNumVal(v int64) tree.Expr {
	return tree.NewNumVal(v, strconv.FormatInt(v, 10), false, tree.P_int64)
}

func testWindowFuncExpr(name string, funcType tree.FuncType, ws *tree.WindowSpec, args ...tree.Expr) *tree.FuncExpr {
	return &tree.FuncExpr{
		Func:       tree.FuncName2ResolvableFunctionReference(tree.NewUnresolvedColName(name)),
		Type:       funcType,
		Exprs:      args,
		WindowSpec: ws,
	}
}

func testLagWindowExpr() *tree.FuncExpr {
	return testWindowFuncExpr(
		"lag",
		tree.FUNC_TYPE_DEFAULT,
		&tree.WindowSpec{
			PartitionBy: tree.Exprs{testNumVal(1)},
			OrderBy: tree.OrderBy{
				tree.NewOrder(testNumVal(1), tree.Descending, tree.NullsLast, false),
			},
		},
		testNumVal(1),
	)
}

func testRangeWindowExpr() *tree.FuncExpr {
	return testWindowFuncExpr(
		"sum",
		tree.FUNC_TYPE_DEFAULT,
		&tree.WindowSpec{
			OrderBy: tree.OrderBy{
				tree.NewOrder(testNumVal(1), tree.Ascending, tree.DefaultNullsPosition, false),
			},
			HasFrame: true,
			Frame: &tree.FrameClause{
				Type:   tree.Range,
				HasEnd: true,
				Start: &tree.FrameBound{
					Type: tree.Preceding,
					Expr: testNumVal(1),
				},
				End: &tree.FrameBound{
					Type: tree.Following,
					Expr: testNumVal(2),
				},
			},
		},
		testNumVal(1),
	)
}

func TestProjectionAndHavingBinderBindExprOnWindowAlias(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	bindCtx := NewBindContext(builder, nil)
	bindCtx.windowTag = builder.genNewBindTag()

	windowExpr := testLagWindowExpr()
	astStr := tree.String(windowExpr, dialect.MYSQL)
	bindCtx.windowByAst[astStr] = 0
	bindCtx.windows = []*planpb.Expr{{Typ: planpb.Type{Id: int32(types.T_int64)}}}

	havingBinder := NewHavingBinder(builder, bindCtx)
	havingExpr, err := havingBinder.BindExpr(windowExpr, 0, true)
	require.NoError(t, err)
	require.Equal(t, bindCtx.windowTag, havingExpr.GetCol().RelPos)
	require.Equal(t, int32(0), havingExpr.GetCol().ColPos)

	projectionBinder := NewProjectionBinder(builder, bindCtx, havingBinder)
	projExpr, err := projectionBinder.BindExpr(windowExpr, 0, true)
	require.NoError(t, err)
	require.Equal(t, bindCtx.windowTag, projExpr.GetCol().RelPos)
	require.Equal(t, int32(0), projExpr.GetCol().ColPos)

	// insideAgg=true should NOT resolve window alias via windowByAst
	havingBinder.insideAgg = true
	_, err = havingBinder.BindExpr(windowExpr, 0, true)
	require.Error(t, err)
}

func TestProjectionBinderBindWinFuncCachesWindowExpr(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	bindCtx := NewBindContext(builder, nil)
	bindCtx.windowTag = builder.genNewBindTag()

	havingBinder := NewHavingBinder(builder, bindCtx)
	projectionBinder := NewProjectionBinder(builder, bindCtx, havingBinder)

	firstExpr, err := projectionBinder.BindWinFunc("lag", testLagWindowExpr(), 0, true)
	require.NoError(t, err)
	require.Len(t, bindCtx.windows, 1)
	require.Equal(t, bindCtx.windowTag, firstExpr.GetCol().RelPos)
	require.Equal(t, int32(0), firstExpr.GetCol().ColPos)

	windowSpec := bindCtx.windows[0].GetW()
	require.Equal(t, "lag", windowSpec.Name)
	require.Len(t, windowSpec.PartitionBy, 1)
	require.Len(t, windowSpec.OrderBy, 1)
	require.Equal(t, planpb.FrameClause_ROWS, windowSpec.Frame.Type)
	require.True(t, windowSpec.Frame.Start.UnBounded)
	require.True(t, windowSpec.Frame.End.UnBounded)
	require.Equal(t, planpb.OrderBySpec_DESC|planpb.OrderBySpec_NULLS_LAST|planpb.OrderBySpec_INTERNAL, windowSpec.OrderBy[0].Flag)

	secondExpr, err := projectionBinder.BindWinFunc("lag", testLagWindowExpr(), 0, true)
	require.NoError(t, err)
	require.Len(t, bindCtx.windows, 1)
	require.Equal(t, firstExpr.GetCol().RelPos, secondExpr.GetCol().RelPos)
	require.Equal(t, firstExpr.GetCol().ColPos, secondExpr.GetCol().ColPos)
}

func TestHavingBinderBindWinFuncCoversFrameAndGuard(t *testing.T) {
	t.Run("inside aggregate rejects window func", func(t *testing.T) {
		builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
		bindCtx := NewBindContext(builder, nil)
		bindCtx.windowTag = builder.genNewBindTag()

		havingBinder := NewHavingBinder(builder, bindCtx)
		havingBinder.insideAgg = true

		_, err := havingBinder.BindWinFunc("lag", testLagWindowExpr(), 0, true)
		require.Error(t, err)
	})

	t.Run("range frame binds frame constants", func(t *testing.T) {
		builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
		bindCtx := NewBindContext(builder, nil)
		bindCtx.windowTag = builder.genNewBindTag()

		havingBinder := NewHavingBinder(builder, bindCtx)
		expr, err := havingBinder.BindWinFunc("sum", testRangeWindowExpr(), 0, true)
		require.NoError(t, err)
		require.Equal(t, bindCtx.windowTag, expr.GetCol().RelPos)
		require.Len(t, bindCtx.windows, 1)

		windowSpec := bindCtx.windows[0].GetW()
		require.Equal(t, planpb.FrameClause_RANGE, windowSpec.Frame.Type)
		require.NotNil(t, windowSpec.Frame.Start.Val)
		require.NotNil(t, windowSpec.Frame.End.Val)
	})
}

func TestBindWindowFuncExprValidationAndHelpers(t *testing.T) {
	t.Run("distinct window func is rejected", func(t *testing.T) {
		binder := &stubWindowBinder{
			bindExprFunc: func(tree.Expr, int32, bool) (*planpb.Expr, error) {
				return makePlan2Int64ConstExprWithType(1), nil
			},
			bindFuncExprFunc: func(string, []tree.Expr, int32) (*planpb.Expr, error) {
				return makePlan2Int64ConstExprWithType(1), nil
			},
			makeFrameValueFunc: func(tree.Expr, *planpb.Type) (*planpb.Expr, error) {
				return makePlan2Int64ConstExprWithType(1), nil
			},
		}
		ctx := &BindContext{windowTag: 9, windowByAst: make(map[string]int32)}

		_, err := bindWindowFuncExpr(
			binder,
			ctx,
			"sum",
			testWindowFuncExpr(
				"sum",
				tree.FUNC_TYPE_DISTINCT,
				&tree.WindowSpec{
					OrderBy:  tree.OrderBy{tree.NewOrder(testNumVal(1), tree.Ascending, tree.DefaultNullsPosition, false)},
					HasFrame: true,
					Frame: &tree.FrameClause{
						Type: tree.Rows,
						Start: &tree.FrameBound{
							Type: tree.Preceding,
						},
						End: &tree.FrameBound{
							Type: tree.CurrentRow,
						},
					},
				},
				testNumVal(1),
			),
			0,
			true,
		)
		require.Error(t, err)
	})

	t.Run("groups frame is rejected", func(t *testing.T) {
		binder := &stubWindowBinder{
			bindExprFunc: func(tree.Expr, int32, bool) (*planpb.Expr, error) {
				return makePlan2Int64ConstExprWithType(1), nil
			},
			bindFuncExprFunc: func(string, []tree.Expr, int32) (*planpb.Expr, error) {
				return makePlan2Int64ConstExprWithType(1), nil
			},
			makeFrameValueFunc: func(tree.Expr, *planpb.Type) (*planpb.Expr, error) {
				return makePlan2Int64ConstExprWithType(1), nil
			},
		}
		ctx := &BindContext{windowTag: 9, windowByAst: make(map[string]int32)}

		_, err := bindWindowFuncExpr(
			binder,
			ctx,
			"sum",
			testWindowFuncExpr(
				"sum",
				tree.FUNC_TYPE_DEFAULT,
				&tree.WindowSpec{
					OrderBy:  tree.OrderBy{tree.NewOrder(testNumVal(1), tree.Ascending, tree.DefaultNullsPosition, false)},
					HasFrame: true,
					Frame: &tree.FrameClause{
						Type: tree.Groups,
						Start: &tree.FrameBound{
							Type: tree.Preceding,
						},
						End: &tree.FrameBound{
							Type: tree.CurrentRow,
						},
					},
				},
				testNumVal(1),
			),
			0,
			true,
		)
		require.Error(t, err)
	})

	t.Run("range frame rejects non-numeric order by for non-WIN_ORDER func", func(t *testing.T) {
		binder := &stubWindowBinder{
			bindExprFunc: func(tree.Expr, int32, bool) (*planpb.Expr, error) {
				return makePlan2StringConstExprWithType("x"), nil
			},
			bindFuncExprFunc: func(string, []tree.Expr, int32) (*planpb.Expr, error) {
				return makePlan2Int64ConstExprWithType(1), nil
			},
			makeFrameValueFunc: func(tree.Expr, *planpb.Type) (*planpb.Expr, error) {
				return makePlan2Int64ConstExprWithType(1), nil
			},
		}
		ctx := &BindContext{windowTag: 9, windowByAst: make(map[string]int32)}

		_, err := bindWindowFuncExpr(binder, ctx, "sum", testRangeWindowExpr(), 0, true)
		require.Error(t, err)
	})

	t.Run("range frame allows non-numeric order by for WIN_ORDER func", func(t *testing.T) {
		binder := &stubWindowBinder{
			bindExprFunc: func(tree.Expr, int32, bool) (*planpb.Expr, error) {
				return makePlan2StringConstExprWithType("x"), nil
			},
			bindFuncExprFunc: func(string, []tree.Expr, int32) (*planpb.Expr, error) {
				return makePlan2Int64ConstExprWithType(1), nil
			},
			makeFrameValueFunc: func(tree.Expr, *planpb.Type) (*planpb.Expr, error) {
				return makePlan2Int64ConstExprWithType(1), nil
			},
		}
		ctx := &BindContext{windowTag: 9, windowByAst: make(map[string]int32)}

		// row_number is WIN_ORDER, should succeed with varchar ORDER BY
		_, err := bindWindowFuncExpr(
			binder,
			ctx,
			"row_number",
			testWindowFuncExpr(
				"row_number",
				tree.FUNC_TYPE_DEFAULT,
				&tree.WindowSpec{
					OrderBy: tree.OrderBy{tree.NewOrder(testNumVal(1), tree.Ascending, tree.DefaultNullsPosition, false)},
					Frame: &tree.FrameClause{
						Type:  tree.Range,
						Start: &tree.FrameBound{Type: tree.Preceding, UnBounded: true},
						End:   &tree.FrameBound{Type: tree.Following, UnBounded: true},
					},
				},
			),
			0,
			true,
		)
		require.NoError(t, err)
	})

	t.Run("range unbounded frame rejects non-numeric order by for agg func", func(t *testing.T) {
		binder := &stubWindowBinder{
			bindExprFunc: func(tree.Expr, int32, bool) (*planpb.Expr, error) {
				return makePlan2StringConstExprWithType("x"), nil
			},
			bindFuncExprFunc: func(string, []tree.Expr, int32) (*planpb.Expr, error) {
				return makePlan2Int64ConstExprWithType(1), nil
			},
			makeFrameValueFunc: func(tree.Expr, *planpb.Type) (*planpb.Expr, error) {
				return makePlan2Int64ConstExprWithType(1), nil
			},
		}
		ctx := &BindContext{windowTag: 9, windowByAst: make(map[string]int32)}

		// sum with RANGE UNBOUNDED + varchar ORDER BY should fail
		_, err := bindWindowFuncExpr(
			binder,
			ctx,
			"sum",
			testWindowFuncExpr(
				"sum",
				tree.FUNC_TYPE_DEFAULT,
				&tree.WindowSpec{
					OrderBy: tree.OrderBy{tree.NewOrder(testNumVal(1), tree.Ascending, tree.DefaultNullsPosition, false)},
					Frame: &tree.FrameClause{
						Type:  tree.Range,
						Start: &tree.FrameBound{Type: tree.Preceding, UnBounded: true},
						End:   &tree.FrameBound{Type: tree.CurrentRow},
					},
				},
				testNumVal(1),
			),
			0,
			true,
		)
		require.Error(t, err)
	})

	t.Run("buildWindowColRefExpr keeps tag and column", func(t *testing.T) {
		expr := buildWindowColRefExpr(&BindContext{windowTag: 17}, planpb.Type{Id: int32(types.T_int64)}, 3)
		require.Equal(t, int32(17), expr.GetCol().RelPos)
		require.Equal(t, int32(3), expr.GetCol().ColPos)
	})

	t.Run("bindFuncExpr error propagates", func(t *testing.T) {
		binder := &stubWindowBinder{
			bindExprFunc:     func(tree.Expr, int32, bool) (*planpb.Expr, error) { return nil, nil },
			bindFuncExprFunc: func(string, []tree.Expr, int32) (*planpb.Expr, error) { return nil, moerr.NewInternalErrorNoCtx("fail") },
			makeFrameValueFunc: func(tree.Expr, *planpb.Type) (*planpb.Expr, error) { return nil, nil },
		}
		ctx := &BindContext{windowTag: 9, windowByAst: make(map[string]int32)}
		_, err := bindWindowFuncExpr(binder, ctx, "sum", testWindowFuncExpr("sum", tree.FUNC_TYPE_DEFAULT,
			&tree.WindowSpec{Frame: &tree.FrameClause{Type: tree.Rows, Start: &tree.FrameBound{Type: tree.Preceding, UnBounded: true}, End: &tree.FrameBound{Type: tree.CurrentRow}}},
			testNumVal(1)), 0, true)
		require.Error(t, err)
	})

	t.Run("partition by bind error propagates", func(t *testing.T) {
		binder := &stubWindowBinder{
			bindExprFunc:     func(tree.Expr, int32, bool) (*planpb.Expr, error) { return nil, moerr.NewInternalErrorNoCtx("fail") },
			bindFuncExprFunc: func(string, []tree.Expr, int32) (*planpb.Expr, error) { return makePlan2Int64ConstExprWithType(1), nil },
			makeFrameValueFunc: func(tree.Expr, *planpb.Type) (*planpb.Expr, error) { return nil, nil },
		}
		ctx := &BindContext{windowTag: 9, windowByAst: make(map[string]int32)}
		_, err := bindWindowFuncExpr(binder, ctx, "sum", testWindowFuncExpr("sum", tree.FUNC_TYPE_DEFAULT,
			&tree.WindowSpec{PartitionBy: tree.Exprs{testNumVal(1)}, Frame: &tree.FrameClause{Type: tree.Rows, Start: &tree.FrameBound{Type: tree.Preceding, UnBounded: true}, End: &tree.FrameBound{Type: tree.CurrentRow}}},
			testNumVal(1)), 0, true)
		require.Error(t, err)
	})

	t.Run("order by bind error propagates", func(t *testing.T) {
		binder := &stubWindowBinder{
			bindExprFunc:     func(tree.Expr, int32, bool) (*planpb.Expr, error) { return nil, moerr.NewInternalErrorNoCtx("fail") },
			bindFuncExprFunc: func(string, []tree.Expr, int32) (*planpb.Expr, error) { return makePlan2Int64ConstExprWithType(1), nil },
			makeFrameValueFunc: func(tree.Expr, *planpb.Type) (*planpb.Expr, error) { return nil, nil },
		}
		ctx := &BindContext{windowTag: 9, windowByAst: make(map[string]int32)}
		_, err := bindWindowFuncExpr(binder, ctx, "sum", testWindowFuncExpr("sum", tree.FUNC_TYPE_DEFAULT,
			&tree.WindowSpec{OrderBy: tree.OrderBy{tree.NewOrder(testNumVal(1), tree.Ascending, tree.DefaultNullsPosition, false)}, Frame: &tree.FrameClause{Type: tree.Rows, Start: &tree.FrameBound{Type: tree.Preceding, UnBounded: true}, End: &tree.FrameBound{Type: tree.CurrentRow}}},
			testNumVal(1)), 0, true)
		require.Error(t, err)
	})

	t.Run("nulls first flag is set", func(t *testing.T) {
		binder := &stubWindowBinder{
			bindExprFunc:     func(tree.Expr, int32, bool) (*planpb.Expr, error) { return makePlan2Int64ConstExprWithType(1), nil },
			bindFuncExprFunc: func(string, []tree.Expr, int32) (*planpb.Expr, error) { return makePlan2Int64ConstExprWithType(1), nil },
			makeFrameValueFunc: func(tree.Expr, *planpb.Type) (*planpb.Expr, error) { return makePlan2Int64ConstExprWithType(1), nil },
		}
		ctx := &BindContext{windowTag: 9, windowByAst: make(map[string]int32)}
		_, err := bindWindowFuncExpr(binder, ctx, "sum", testWindowFuncExpr("sum", tree.FUNC_TYPE_DEFAULT,
			&tree.WindowSpec{OrderBy: tree.OrderBy{tree.NewOrder(testNumVal(1), tree.Ascending, tree.NullsFirst, false)}, HasFrame: true, Frame: &tree.FrameClause{Type: tree.Rows, Start: &tree.FrameBound{Type: tree.Preceding, UnBounded: true}, End: &tree.FrameBound{Type: tree.CurrentRow}}},
			testNumVal(1)), 0, true)
		require.NoError(t, err)
		require.True(t, ctx.windows[0].GetW().OrderBy[0].Flag&planpb.OrderBySpec_NULLS_FIRST != 0)
	})

	t.Run("frame start unbounded following is rejected", func(t *testing.T) {
		binder := &stubWindowBinder{
			bindExprFunc:     func(tree.Expr, int32, bool) (*planpb.Expr, error) { return makePlan2Int64ConstExprWithType(1), nil },
			bindFuncExprFunc: func(string, []tree.Expr, int32) (*planpb.Expr, error) { return makePlan2Int64ConstExprWithType(1), nil },
			makeFrameValueFunc: func(tree.Expr, *planpb.Type) (*planpb.Expr, error) { return makePlan2Int64ConstExprWithType(1), nil },
		}
		ctx := &BindContext{windowTag: 9, windowByAst: make(map[string]int32)}
		_, err := bindWindowFuncExpr(binder, ctx, "sum", testWindowFuncExpr("sum", tree.FUNC_TYPE_DEFAULT,
			&tree.WindowSpec{Frame: &tree.FrameClause{Type: tree.Rows, Start: &tree.FrameBound{Type: tree.Following, UnBounded: true}, End: &tree.FrameBound{Type: tree.Following, UnBounded: true}}},
			testNumVal(1)), 0, true)
		require.Error(t, err)
	})

	t.Run("frame start following with end preceding is rejected", func(t *testing.T) {
		binder := &stubWindowBinder{
			bindExprFunc:     func(tree.Expr, int32, bool) (*planpb.Expr, error) { return makePlan2Int64ConstExprWithType(1), nil },
			bindFuncExprFunc: func(string, []tree.Expr, int32) (*planpb.Expr, error) { return makePlan2Int64ConstExprWithType(1), nil },
			makeFrameValueFunc: func(tree.Expr, *planpb.Type) (*planpb.Expr, error) { return makePlan2Int64ConstExprWithType(1), nil },
		}
		ctx := &BindContext{windowTag: 9, windowByAst: make(map[string]int32)}
		_, err := bindWindowFuncExpr(binder, ctx, "sum", testWindowFuncExpr("sum", tree.FUNC_TYPE_DEFAULT,
			&tree.WindowSpec{Frame: &tree.FrameClause{Type: tree.Rows, Start: &tree.FrameBound{Type: tree.Following}, End: &tree.FrameBound{Type: tree.Preceding}}},
			testNumVal(1)), 0, true)
		require.Error(t, err)
	})

	t.Run("frame start current row with end preceding is rejected", func(t *testing.T) {
		binder := &stubWindowBinder{
			bindExprFunc:     func(tree.Expr, int32, bool) (*planpb.Expr, error) { return makePlan2Int64ConstExprWithType(1), nil },
			bindFuncExprFunc: func(string, []tree.Expr, int32) (*planpb.Expr, error) { return makePlan2Int64ConstExprWithType(1), nil },
			makeFrameValueFunc: func(tree.Expr, *planpb.Type) (*planpb.Expr, error) { return makePlan2Int64ConstExprWithType(1), nil },
		}
		ctx := &BindContext{windowTag: 9, windowByAst: make(map[string]int32)}
		_, err := bindWindowFuncExpr(binder, ctx, "sum", testWindowFuncExpr("sum", tree.FUNC_TYPE_DEFAULT,
			&tree.WindowSpec{Frame: &tree.FrameClause{Type: tree.Rows, Start: &tree.FrameBound{Type: tree.CurrentRow}, End: &tree.FrameBound{Type: tree.Preceding}}},
			testNumVal(1)), 0, true)
		require.Error(t, err)
	})

	t.Run("frame end unbounded preceding is rejected", func(t *testing.T) {
		binder := &stubWindowBinder{
			bindExprFunc:     func(tree.Expr, int32, bool) (*planpb.Expr, error) { return makePlan2Int64ConstExprWithType(1), nil },
			bindFuncExprFunc: func(string, []tree.Expr, int32) (*planpb.Expr, error) { return makePlan2Int64ConstExprWithType(1), nil },
			makeFrameValueFunc: func(tree.Expr, *planpb.Type) (*planpb.Expr, error) { return makePlan2Int64ConstExprWithType(1), nil },
		}
		ctx := &BindContext{windowTag: 9, windowByAst: make(map[string]int32)}
		_, err := bindWindowFuncExpr(binder, ctx, "sum", testWindowFuncExpr("sum", tree.FUNC_TYPE_DEFAULT,
			&tree.WindowSpec{Frame: &tree.FrameClause{Type: tree.Rows, Start: &tree.FrameBound{Type: tree.Preceding, UnBounded: true}, End: &tree.FrameBound{Type: tree.Preceding, UnBounded: true}}},
			testNumVal(1)), 0, true)
		require.Error(t, err)
	})

	t.Run("range N preceding with multiple order by is rejected", func(t *testing.T) {
		binder := &stubWindowBinder{
			bindExprFunc:     func(tree.Expr, int32, bool) (*planpb.Expr, error) { return makePlan2Int64ConstExprWithType(1), nil },
			bindFuncExprFunc: func(string, []tree.Expr, int32) (*planpb.Expr, error) { return makePlan2Int64ConstExprWithType(1), nil },
			makeFrameValueFunc: func(tree.Expr, *planpb.Type) (*planpb.Expr, error) { return makePlan2Int64ConstExprWithType(1), nil },
		}
		ctx := &BindContext{windowTag: 9, windowByAst: make(map[string]int32)}
		_, err := bindWindowFuncExpr(binder, ctx, "sum", testWindowFuncExpr("sum", tree.FUNC_TYPE_DEFAULT,
			&tree.WindowSpec{
				OrderBy: tree.OrderBy{
					tree.NewOrder(testNumVal(1), tree.Ascending, tree.DefaultNullsPosition, false),
					tree.NewOrder(testNumVal(2), tree.Ascending, tree.DefaultNullsPosition, false),
				},
				HasFrame: true,
				Frame: &tree.FrameClause{Type: tree.Range, Start: &tree.FrameBound{Type: tree.Preceding, Expr: testNumVal(1)}, End: &tree.FrameBound{Type: tree.CurrentRow}},
			},
			testNumVal(1)), 0, true)
		require.Error(t, err)
	})

	t.Run("frame start val bind error propagates", func(t *testing.T) {
		binder := &stubWindowBinder{
			bindExprFunc:     func(tree.Expr, int32, bool) (*planpb.Expr, error) { return makePlan2Int64ConstExprWithType(1), nil },
			bindFuncExprFunc: func(string, []tree.Expr, int32) (*planpb.Expr, error) { return makePlan2Int64ConstExprWithType(1), nil },
			makeFrameValueFunc: func(tree.Expr, *planpb.Type) (*planpb.Expr, error) { return nil, moerr.NewInternalErrorNoCtx("fail") },
		}
		ctx := &BindContext{windowTag: 9, windowByAst: make(map[string]int32)}
		_, err := bindWindowFuncExpr(binder, ctx, "sum", testWindowFuncExpr("sum", tree.FUNC_TYPE_DEFAULT,
			&tree.WindowSpec{HasFrame: true, Frame: &tree.FrameClause{Type: tree.Rows, Start: &tree.FrameBound{Type: tree.Preceding, Expr: testNumVal(1)}, End: &tree.FrameBound{Type: tree.CurrentRow}}},
			testNumVal(1)), 0, true)
		require.Error(t, err)
	})

	t.Run("frame end val bind error propagates", func(t *testing.T) {
		binder := &stubWindowBinder{
			bindExprFunc:     func(tree.Expr, int32, bool) (*planpb.Expr, error) { return makePlan2Int64ConstExprWithType(1), nil },
			bindFuncExprFunc: func(string, []tree.Expr, int32) (*planpb.Expr, error) { return makePlan2Int64ConstExprWithType(1), nil },
			makeFrameValueFunc: func(tree.Expr, *planpb.Type) (*planpb.Expr, error) {
				return nil, moerr.NewInternalErrorNoCtx("fail")
			},
		}
		ctx := &BindContext{windowTag: 9, windowByAst: make(map[string]int32)}
		_, err := bindWindowFuncExpr(binder, ctx, "sum", testWindowFuncExpr("sum", tree.FUNC_TYPE_DEFAULT,
			&tree.WindowSpec{HasFrame: true, Frame: &tree.FrameClause{Type: tree.Rows, Start: &tree.FrameBound{Type: tree.Preceding, UnBounded: true}, End: &tree.FrameBound{Type: tree.Following, Expr: testNumVal(1)}}},
			testNumVal(1)), 0, true)
		require.Error(t, err)
	})
}

func TestWindowFrameConstValueHelpers(t *testing.T) {
	proc := testutil.NewProc(t)

	t.Run("typ nil returns bound expr directly", func(t *testing.T) {
		expected := makePlan2Int64ConstExprWithType(7)
		got, err := makeWindowFrameConstValue(
			func(tree.Expr, int32, bool) (*Expr, error) {
				return expected, nil
			},
			proc,
			context.Background(),
			testNumVal(7),
			nil,
		)
		require.NoError(t, err)
		require.Same(t, expected, got)
	})

	t.Run("typed expr is constant folded", func(t *testing.T) {
		got, err := makeWindowFrameConstValue(
			func(tree.Expr, int32, bool) (*Expr, error) {
				return makePlan2Int64ConstExprWithType(11), nil
			},
			proc,
			context.Background(),
			testNumVal(11),
			&planpb.Type{Id: int32(types.T_int64)},
		)
		require.NoError(t, err)
		require.Equal(t, int64(11), got.GetLit().Value.(*planpb.Literal_I64Val).I64Val)
	})

	t.Run("interval expr is normalized through helper", func(t *testing.T) {
		got, err := makeWindowFrameConstValue(
			func(tree.Expr, int32, bool) (*Expr, error) {
				return &Expr{
					Typ: planpb.Type{Id: int32(types.T_interval)},
					Expr: &planpb.Expr_List{
						List: &planpb.ExprList{
							List: []*planpb.Expr{
								makePlan2StringConstExprWithType("2"),
								makePlan2StringConstExprWithType("day"),
							},
						},
					},
				}, nil
			},
			proc,
			context.Background(),
			testNumVal(1),
			nil,
		)
		require.NoError(t, err)
		require.Equal(t, int64(2), got.GetList().List[0].GetLit().Value.(*planpb.Literal_I64Val).I64Val)
	})

	t.Run("reset interval handles numeric value", func(t *testing.T) {
		expr := &Expr{
			Typ: planpb.Type{Id: int32(types.T_interval)},
			Expr: &planpb.Expr_List{
				List: &planpb.ExprList{
					List: []*planpb.Expr{
						makePlan2Int64ConstExprWithType(3),
						makePlan2StringConstExprWithType("day"),
					},
				},
			},
		}
		got, err := resetWindowIntervalExpr(context.Background(), proc, expr)
		require.NoError(t, err)
		require.Equal(t, int64(3), got.GetList().List[0].GetLit().Value.(*planpb.Literal_I64Val).I64Val)
	})
}

func TestBinderMakeFrameConstValueWrappers(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	bindCtx := NewBindContext(builder, nil)

	havingBinder := NewHavingBinder(builder, bindCtx)
	projectionBinder := NewProjectionBinder(builder, bindCtx, havingBinder)

	projExpr, err := projectionBinder.makeFrameConstValue(testNumVal(5), &planpb.Type{Id: int32(types.T_int64)})
	require.NoError(t, err)
	require.Equal(t, int64(5), projExpr.GetLit().Value.(*planpb.Literal_I64Val).I64Val)

	havingExpr, err := havingBinder.makeFrameConstValue(testNumVal(6), &planpb.Type{Id: int32(types.T_int64)})
	require.NoError(t, err)
	require.Equal(t, int64(6), havingExpr.GetLit().Value.(*planpb.Literal_I64Val).I64Val)
}

func TestContainsTagCoversWindowSubAndCorrBranches(t *testing.T) {
	windowExpr := &planpb.Expr{
		Expr: &planpb.Expr_W{
			W: &planpb.WindowSpec{
				WindowFunc: &planpb.Expr{
					Expr: &planpb.Expr_Col{
						Col: &planpb.ColRef{RelPos: 1},
					},
				},
				PartitionBy: []*planpb.Expr{
					{
						Expr: &planpb.Expr_List{
							List: &planpb.ExprList{
								List: []*planpb.Expr{
									{
										Expr: &planpb.Expr_Col{
											Col: &planpb.ColRef{RelPos: 2},
										},
									},
								},
							},
						},
					},
				},
				OrderBy: []*planpb.OrderBySpec{
					{
						Expr: &planpb.Expr{
							Expr: &planpb.Expr_Corr{
								Corr: &planpb.CorrColRef{RelPos: 3},
							},
						},
					},
				},
			},
		},
	}

	require.False(t, containsTag(nil, 1))
	require.True(t, containsTag(windowExpr, 1))
	require.True(t, containsTag(windowExpr, 2))
	require.True(t, containsTag(windowExpr, 3))
	require.False(t, containsTag(&planpb.Expr{Expr: &planpb.Expr_Sub{}}, 1))
	require.True(t, containsTag(&planpb.Expr{
		Expr: &planpb.Expr_Sub{
			Sub: &planpb.SubqueryRef{
				Child: &planpb.Expr{
					Expr: &planpb.Expr_Col{
						Col: &planpb.ColRef{RelPos: 4},
					},
				},
			},
		},
	}, 4))
	require.False(t, containsTag(makePlan2Int64ConstExprWithType(1), 5))
}
