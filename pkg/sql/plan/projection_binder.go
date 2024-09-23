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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
)

func NewProjectionBinder(builder *QueryBuilder, ctx *BindContext, havingBinder *HavingBinder) *ProjectionBinder {
	b := &ProjectionBinder{
		havingBinder: havingBinder,
	}
	b.sysCtx = builder.GetContext()
	b.builder = builder
	b.ctx = ctx
	b.impl = b

	return b
}

func (b *ProjectionBinder) BindExpr(astExpr tree.Expr, depth int32, isRoot bool) (*plan.Expr, error) {
	astStr := tree.String(astExpr, dialect.MYSQL)

	if colPos, ok := b.ctx.timeByAst[astStr]; ok {
		if astStr != TimeWindowEnd && astStr != TimeWindowStart {
			b.ctx.timeAsts = append(b.ctx.timeAsts, astExpr)
		}
		return &plan.Expr{
			Typ: b.ctx.times[colPos].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: b.ctx.timeTag,
					ColPos: colPos,
				},
			},
		}, nil
	}

	if colPos, ok := b.ctx.groupByAst[astStr]; ok {
		return &plan.Expr{
			Typ: b.ctx.groups[colPos].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: b.ctx.groupTag,
					ColPos: colPos,
				},
			},
		}, nil
	}

	if colPos, ok := b.ctx.aggregateByAst[astStr]; ok {
		return &plan.Expr{
			Typ: b.ctx.aggregates[colPos].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: b.ctx.aggregateTag,
					ColPos: colPos,
				},
			},
		}, nil
	}

	if colPos, ok := b.ctx.windowByAst[astStr]; ok {
		return &plan.Expr{
			Typ: b.ctx.windows[colPos].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: b.ctx.windowTag,
					ColPos: colPos,
				},
			},
		}, nil
	}

	if colPos, ok := b.ctx.sampleByAst[astStr]; ok {
		return &plan.Expr{
			Typ: b.ctx.sampleFunc.columns[colPos].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: b.ctx.sampleTag,
					ColPos: colPos,
				},
			},
		}, nil
	}

	return b.baseBindExpr(astExpr, depth, isRoot)
}

func (b *ProjectionBinder) BindColRef(astExpr *tree.UnresolvedName, depth int32, isRoot bool) (*plan.Expr, error) {
	return b.baseBindColRef(astExpr, depth, isRoot)
}

func (b *ProjectionBinder) BindAggFunc(funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	return b.havingBinder.BindAggFunc(funcName, astExpr, depth, isRoot)
}

func (b *ProjectionBinder) BindWinFunc(funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	if astExpr.Type == tree.FUNC_TYPE_DISTINCT {
		return nil, moerr.NewNYI(b.GetContext(), "DISTINCT in window function")
	}

	colPos := int32(len(b.ctx.windows))
	astStr := tree.String(astExpr, dialect.MYSQL)
	b.ctx.windowByAst[astStr] = colPos
	w := &plan.WindowSpec{}
	ws := astExpr.WindowSpec
	var err error

	// window function
	w.WindowFunc, err = b.bindFuncExprImplByAstExpr(funcName, astExpr.Exprs, depth)
	if err != nil {
		return nil, err
	}
	w.Name = funcName
	// partition by
	for _, group := range ws.PartitionBy {
		expr, err := b.BindExpr(group, depth, isRoot)
		if err != nil {
			return nil, err
		}
		w.PartitionBy = append(w.PartitionBy, expr)
	}
	// order by
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
	// preceding and following
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
		if len(w.OrderBy) == 0 {
			// not N range
			break
		}
		typ = &w.OrderBy[0].Expr.Typ
		t := types.Type{Oid: types.T(typ.Id)}
		if !t.IsNumericOrTemporal() {
			return nil, moerr.NewParseError(b.GetContext(), "Window '<unnamed window>' with RANGE N PRECEDING/FOLLOWING frame requires exactly one ORDER BY expression, of numeric or temporal type")
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

	// append
	b.ctx.windows = append(b.ctx.windows, &plan.Expr{
		Typ:  w.WindowFunc.Typ,
		Expr: &plan.Expr_W{W: w},
	})

	return &plan.Expr{
		Typ: w.WindowFunc.Typ,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: b.ctx.windowTag,
				ColPos: colPos,
			},
		},
	}, nil
}

func isNRange(f *tree.FrameClause) bool {
	if f.Start.Expr == nil && f.End.Expr == nil {
		return false
	}
	return true
}

func (b *ProjectionBinder) makeFrameConstValue(expr tree.Expr, typ *plan.Type) (*plan.Expr, error) {
	e, err := b.baseBindExpr(expr, 0, true)
	if err != nil {
		return nil, err
	}
	if e.Typ.Id == int32(types.T_interval) {
		return b.resetInterval(e)
	}
	if typ == nil {
		return e, nil
	}
	e, err = appendCastBeforeExpr(b.GetContext(), e, *typ)
	if err != nil {
		return nil, err
	}

	executor, err := colexec.NewExpressionExecutor(b.builder.compCtx.GetProcess(), e)
	if err != nil {
		return nil, err
	}
	defer executor.Free()
	vec, err := executor.Eval(b.builder.compCtx.GetProcess(), []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
	if err != nil {
		return nil, err
	}
	c := rule.GetConstantValue(vec, false, 0)

	return &plan.Expr{
		Typ:  *typ,
		Expr: &plan.Expr_Lit{Lit: c},
	}, nil
}

func (b *ProjectionBinder) resetInterval(e *Expr) (*Expr, error) {
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
	numberExpr, err := appendCastBeforeExpr(b.GetContext(), e1, *typ)
	if err != nil {
		return nil, err
	}

	executor, err := colexec.NewExpressionExecutor(b.builder.compCtx.GetProcess(), numberExpr)
	if err != nil {
		return nil, err
	}
	defer executor.Free()
	vec, err := executor.Eval(b.builder.compCtx.GetProcess(), []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
	if err != nil {
		return nil, err
	}
	c := rule.GetConstantValue(vec, false, 0)

	e.Expr.(*plan.Expr_List).List.List[0] = &plan.Expr{Typ: *typ, Expr: &plan.Expr_Lit{Lit: c}}
	e.Expr.(*plan.Expr_List).List.List[1] = makePlan2Int64ConstExprWithType(int64(intervalType))

	return e, nil
}

func (b *ProjectionBinder) BindSubquery(astExpr *tree.Subquery, isRoot bool) (*plan.Expr, error) {
	return b.baseBindSubquery(astExpr, isRoot)
}

func (b *ProjectionBinder) BindTimeWindowFunc(funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	b.ctx.timeAsts = append(b.ctx.timeAsts, astExpr)
	return b.havingBinder.BindTimeWindowFunc(funcName, astExpr, depth, isRoot)
}
