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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type windowFuncExprBinder interface {
	BindExpr(tree.Expr, int32, bool) (*plan.Expr, error)
	bindFuncExprImplByAstExpr(string, []tree.Expr, int32) (*plan.Expr, error)
	makeFrameConstValue(tree.Expr, *plan.Type) (*plan.Expr, error)
	GetContext() context.Context
}

func bindWindowFuncExpr(b windowFuncExprBinder, ctx *BindContext, funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	if astExpr.Type == tree.FUNC_TYPE_DISTINCT {
		return nil, moerr.NewNYI(b.GetContext(), "DISTINCT in window function")
	}

	astStr := tree.String(astExpr, dialect.MYSQL)
	if colPos, ok := ctx.windowByAst[astStr]; ok {
		return buildWindowColRefExpr(ctx, ctx.windows[colPos].Typ, colPos), nil
	}

	w := &plan.WindowSpec{}
	ws := astExpr.WindowSpec

	// window function
	windowFunc, err := b.bindFuncExprImplByAstExpr(funcName, astExpr.Exprs, depth)
	if err != nil {
		return nil, err
	}
	w.WindowFunc = windowFunc
	w.Name = funcName

	isWinValueFunc := function.GetFunctionIsWinValueFunByName(funcName)
	if isWinValueFunc && !ws.HasFrame {
		ws.Frame = &tree.FrameClause{Type: tree.Rows}
		ws.Frame.Start = &tree.FrameBound{Type: tree.Preceding, UnBounded: true}
		ws.Frame.End = &tree.FrameBound{Type: tree.Following, UnBounded: true}
	}

	for _, group := range ws.PartitionBy {
		expr, err := b.BindExpr(group, depth, isRoot)
		if err != nil {
			return nil, err
		}
		w.PartitionBy = append(w.PartitionBy, expr)
	}

	if ws.OrderBy != nil {
		w.OrderBy = make([]*plan.OrderBySpec, 0, len(ws.OrderBy))
		for _, order := range ws.OrderBy {
			expr, err := b.BindExpr(order.Expr, depth, isRoot)
			if err != nil {
				return nil, err
			}

			orderBy := &plan.OrderBySpec{
				Expr: expr,
				Flag: plan.OrderBySpec_INTERNAL,
			}

			switch order.Direction {
			case tree.Ascending:
				orderBy.Flag |= plan.OrderBySpec_ASC
			case tree.Descending:
				orderBy.Flag |= plan.OrderBySpec_DESC
			}

			switch order.NullsPosition {
			case tree.NullsFirst:
				orderBy.Flag |= plan.OrderBySpec_NULLS_FIRST
			case tree.NullsLast:
				orderBy.Flag |= plan.OrderBySpec_NULLS_LAST
			}

			w.OrderBy = append(w.OrderBy, orderBy)
		}
	}

	switch ws.Frame.Start.Type {
	case tree.Following:
		if ws.Frame.Start.UnBounded {
			return nil, moerr.NewParseError(b.GetContext(), "Window '<unnamed window>': frame start cannot be UNBOUNDED FOLLOWING.")
		}
		if ws.Frame.End.Type == tree.Preceding || ws.Frame.End.Type == tree.CurrentRow {
			return nil, moerr.NewParseError(b.GetContext(), "Window '<unnamed window>': frame start or end is negative, NULL or of non-integral type")
		}
	case tree.CurrentRow:
		if ws.Frame.End.Type == tree.Preceding {
			return nil, moerr.NewParseError(b.GetContext(), "Window '<unnamed window>': frame start or end is negative, NULL or of non-integral type")
		}
	}

	if ws.Frame.End.Type == tree.Preceding && ws.Frame.End.UnBounded {
		return nil, moerr.NewParseError(b.GetContext(), "Window '<unnamed window>': frame end cannot be UNBOUNDED PRECEDING.")
	}

	w.Frame = &plan.FrameClause{
		Type: plan.FrameClause_FrameType(ws.Frame.Type),
		Start: &plan.FrameBound{
			Type:      plan.FrameBound_BoundType(ws.Frame.Start.Type),
			UnBounded: ws.Frame.Start.UnBounded,
		},
		End: &plan.FrameBound{
			Type:      plan.FrameBound_BoundType(ws.Frame.End.Type),
			UnBounded: ws.Frame.End.UnBounded,
		},
	}
	var typ *plan.Type
	switch ws.Frame.Type {
	case tree.Rows:
		typ = &plan.Type{Id: int32(types.T_uint64)}
	case tree.Range:
		if len(w.OrderBy) != 1 && isNRange(ws.Frame) {
			return nil, moerr.NewParseError(b.GetContext(), "Window '<unnamed window>' with RANGE N PRECEDING/FOLLOWING frame requires exactly one ORDER BY expression, of numeric or temporal type")
		}
		if len(w.OrderBy) != 0 {
			typ = &w.OrderBy[0].Expr.Typ
			t := types.Type{Oid: types.T(typ.Id)}
			if !t.IsNumericOrTemporal() {
				return nil, moerr.NewParseError(b.GetContext(), "Window '<unnamed window>' with RANGE N PRECEDING/FOLLOWING frame requires exactly one ORDER BY expression, of numeric or temporal type")
			}
		}
	case tree.Groups:
		return nil, moerr.NewNYI(b.GetContext(), "GROUPS in WINDOW FUNCTION condition")
	}
	if ws.Frame.Start.Expr != nil {
		w.Frame.Start.Val, err = b.makeFrameConstValue(ws.Frame.Start.Expr, typ)
		if err != nil {
			return nil, err
		}
	}
	if ws.Frame.End.Expr != nil {
		w.Frame.End.Val, err = b.makeFrameConstValue(ws.Frame.End.Expr, typ)
		if err != nil {
			return nil, err
		}
	}

	colPos := int32(len(ctx.windows))
	ctx.windows = append(ctx.windows, &plan.Expr{
		Typ:  w.WindowFunc.Typ,
		Expr: &plan.Expr_W{W: w},
	})
	ctx.windowByAst[astStr] = colPos

	return buildWindowColRefExpr(ctx, w.WindowFunc.Typ, colPos), nil
}

func buildWindowColRefExpr(ctx *BindContext, typ plan.Type, colPos int32) *plan.Expr {
	return &plan.Expr{
		Typ: typ,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: ctx.windowTag,
				ColPos: colPos,
			},
		},
	}
}

func makeWindowFrameConstValue(
	baseBindExpr func(tree.Expr, int32, bool) (*Expr, error),
	proc *process.Process,
	bindCtx context.Context,
	expr tree.Expr,
	typ *plan.Type,
) (*plan.Expr, error) {
	e, err := baseBindExpr(expr, 0, true)
	if err != nil {
		return nil, err
	}
	if e.Typ.Id == int32(types.T_interval) {
		return resetWindowIntervalExpr(bindCtx, proc, e)
	}
	if typ == nil {
		return e, nil
	}
	e, err = appendCastBeforeExpr(bindCtx, e, *typ)
	if err != nil {
		return nil, err
	}

	executor, err := colexec.NewExpressionExecutor(proc, e)
	if err != nil {
		return nil, err
	}
	defer executor.Free()
	vec, err := executor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
	if err != nil {
		return nil, err
	}
	c := rule.GetConstantValue(vec, false, 0)

	return &plan.Expr{
		Typ:  *typ,
		Expr: &plan.Expr_Lit{Lit: c},
	}, nil
}

func resetWindowIntervalExpr(bindCtx context.Context, proc *process.Process, e *Expr) (*Expr, error) {
	e1 := e.Expr.(*plan.Expr_List).List.List[0]
	e2 := e.Expr.(*plan.Expr_List).List.List[1]

	intervalTypeStr := e2.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_Sval).Sval
	intervalType, err := types.IntervalTypeOf(intervalTypeStr)
	if err != nil {
		return nil, err
	}

	if e1.Typ.Id == int32(types.T_varchar) || e1.Typ.Id == int32(types.T_char) {
		s := e1.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_Sval).Sval
		returnNum, returnType, err := types.NormalizeInterval(s, intervalType)
		if err != nil {
			return nil, err
		}

		e.Expr.(*plan.Expr_List).List.List[0] = makePlan2Int64ConstExprWithType(returnNum)
		e.Expr.(*plan.Expr_List).List.List[1] = makePlan2Int64ConstExprWithType(int64(returnType))
		return e, nil
	}

	typ := &plan.Type{Id: int32(types.T_int64)}
	numberExpr, err := appendCastBeforeExpr(bindCtx, e1, *typ)
	if err != nil {
		return nil, err
	}

	executor, err := colexec.NewExpressionExecutor(proc, numberExpr)
	if err != nil {
		return nil, err
	}
	defer executor.Free()
	vec, err := executor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
	if err != nil {
		return nil, err
	}
	c := rule.GetConstantValue(vec, false, 0)

	e.Expr.(*plan.Expr_List).List.List[0] = &plan.Expr{Typ: *typ, Expr: &plan.Expr_Lit{Lit: c}}
	e.Expr.(*plan.Expr_List).List.List[1] = makePlan2Int64ConstExprWithType(int64(intervalType))

	return e, nil
}
