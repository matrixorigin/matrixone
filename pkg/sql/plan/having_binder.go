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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

func NewHavingBinder(builder *QueryBuilder, ctx *BindContext) *HavingBinder {
	b := &HavingBinder{
		insideAgg: false,
	}
	b.sysCtx = builder.GetContext()
	b.builder = builder
	b.ctx = ctx
	b.impl = b

	return b
}

func (b *HavingBinder) BindExpr(astExpr tree.Expr, depth int32, isRoot bool) (*plan.Expr, error) {
	astStr := tree.String(astExpr, dialect.MYSQL)

	if !b.insideAgg {
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
	}

	if colPos, ok := b.ctx.aggregateByAst[astStr]; ok {
		if !b.insideAgg {
			return &plan.Expr{
				Typ: b.ctx.aggregates[colPos].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: b.ctx.aggregateTag,
						ColPos: colPos,
					},
				},
			}, nil
		} else {
			return nil, moerr.NewInvalidInput(b.GetContext(), "nestted aggregate function")
		}
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

func (b *HavingBinder) BindColRef(astExpr *tree.UnresolvedName, depth int32, isRoot bool) (*plan.Expr, error) {
	if b.insideAgg {
		expr, err := b.baseBindColRef(astExpr, depth, isRoot)
		if err != nil {
			return nil, err
		}

		if _, ok := expr.Expr.(*plan.Expr_Corr); ok {
			return nil, moerr.NewNYI(b.GetContext(), "correlated columns in aggregate function")
		}

		return expr, nil
	} else if b.builder.mysqlCompatible {
		expr, err := b.baseBindColRef(astExpr, depth, isRoot)
		if err != nil {
			return nil, err
		}

		if _, ok := expr.Expr.(*plan.Expr_Corr); ok {
			return nil, moerr.NewNYI(b.GetContext(), "correlated columns in aggregate function")
		}

		newExpr, _ := BindFuncExprImplByPlanExpr(b.builder.compCtx.GetContext(), "any_value", []*plan.Expr{expr})
		colPos := len(b.ctx.aggregates)
		b.ctx.aggregates = append(b.ctx.aggregates, newExpr)
		return &plan.Expr{
			Typ: b.ctx.aggregates[colPos].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: b.ctx.aggregateTag,
					ColPos: int32(colPos),
				},
			},
		}, nil
	} else {
		return nil, moerr.NewSyntaxErrorf(b.GetContext(), "column %q must appear in the GROUP BY clause or be used in an aggregate function", tree.String(astExpr, dialect.MYSQL))
	}
}

func (b *HavingBinder) BindAggFunc(funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	if b.insideAgg {
		return nil, moerr.NewSyntaxErrorf(b.GetContext(), "aggregate function %s calls cannot be nested", funcName)
	}

	if funcName == NameGroupConcat {
		err := b.processForceWindows(funcName, astExpr, depth, isRoot)
		if err != nil {
			return nil, err
		}
	}

	b.insideAgg = true
	expr, err := b.bindFuncExprImplByAstExpr(funcName, astExpr.Exprs, depth)
	if err != nil {
		return nil, err
	}
	if astExpr.Type == tree.FUNC_TYPE_DISTINCT {
		if funcName != "max" && funcName != "min" && funcName != "any_value" {
			expr.GetF().Func.Obj = int64(uint64(expr.GetF().Func.Obj) | function.Distinct)
		}
	}
	b.insideAgg = false

	if b.ctx.timeTag > 0 && b.ctx.sliding {
		expr, err = b.remapAggToTimeWindowCacheAgg(expr)
		if err != nil {
			return nil, err
		}
	}

	colPos := int32(len(b.ctx.aggregates))
	astStr := tree.String(astExpr, dialect.MYSQL)
	b.ctx.aggregateByAst[astStr] = colPos
	b.ctx.aggregates = append(b.ctx.aggregates, expr)

	return &plan.Expr{
		Typ: expr.Typ,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: b.ctx.aggregateTag,
				ColPos: colPos,
			},
		},
	}, nil
}

func (b *HavingBinder) remapAggToTimeWindowCacheAgg(expr *Expr) (*Expr, error) {
	f := expr.Expr.(*plan.Expr_F).F

	funcId, _ := function.DecodeOverloadID(f.Func.Obj)
	switch funcId {
	case function.AVG:
		typ := types.New(types.T(f.Args[0].Typ.Id), f.Args[0].Typ.Width, f.Args[0].Typ.Scale)
		fGet, err := function.GetFunctionByName(b.GetContext(), "avg_tw_cache", []types.Type{typ})
		if err != nil {
			return nil, err
		}
		f.Func.Obj = fGet.GetEncodedOverloadID()
		f.Func.ObjName = "avg_tw_cache"
		expr.Typ.Id = int32(fGet.GetReturnType().Oid)
		expr.Typ.Width = fGet.GetReturnType().Width
		expr.Typ.Scale = fGet.GetReturnType().Scale
	}
	return expr, nil
}

func (b *HavingBinder) remapAggToTimeWindowResultAgg(expr *Expr) (*Expr, error) {
	obj := expr.Expr.(*plan.Expr_F).F.Func

	funcId, _ := function.DecodeOverloadID(obj.Obj)
	switch funcId {
	case function.COUNT:
		fGet, err := function.GetFunctionByName(b.GetContext(), "sum", []types.Type{types.T_int64.ToType()})
		if err != nil {
			return nil, err
		}
		obj.Obj = fGet.GetEncodedOverloadID()
		obj.ObjName = "sum"
	case function.AVG_TW_CACHE:
		typ := types.New(types.T(expr.Typ.Id), expr.Typ.Width, expr.Typ.Scale)
		fGet, err := function.GetFunctionByName(b.GetContext(), "avg_tw_result", []types.Type{typ})
		if err != nil {
			return nil, err
		}
		obj.Obj = fGet.GetEncodedOverloadID()
		obj.ObjName = "avg_tw_result"
		expr.Typ.Id = int32(fGet.GetReturnType().Oid)
		expr.Typ.Width = fGet.GetReturnType().Width
		expr.Typ.Scale = fGet.GetReturnType().Scale
	}
	return expr, nil
}

func (b *HavingBinder) processForceWindows(funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) error {

	if len(astExpr.OrderBy) < 1 {
		return nil
	}
	b.ctx.forceWindows = true
	b.ctx.isDistinct = true

	w := &plan.WindowSpec{}
	ws := &tree.WindowSpec{}

	// window function
	w.Name = funcName

	// partition by
	w.PartitionBy = DeepCopyExprList(b.ctx.groups)

	//order by
	w.OrderBy = make([]*plan.OrderBySpec, 0, len(astExpr.OrderBy))

	for _, order := range astExpr.OrderBy {
		orderExpr := order.Expr
		if numVal, ok := order.Expr.(*tree.NumVal); ok {
			switch numVal.Kind() {
			case tree.Int:
				colPos, _ := numVal.Int64()
				if numVal.Negative() {
					moerr.NewSyntaxErrorf(b.GetContext(), "ORDER BY position %v is negative", colPos)
				}
				if colPos < 1 || int(colPos) > len(astExpr.Exprs)-1 {
					return moerr.NewSyntaxErrorf(b.GetContext(), "ORDER BY position %v is not in group_concat arguments", colPos)
				}
				orderExpr = astExpr.Exprs[colPos-1]
			default:
				return moerr.NewSyntaxError(b.GetContext(), "non-integer constant in ORDER BY")
			}

		}

		if _, ok := order.Expr.(*tree.Subquery); ok {
			return moerr.NewNotSupported(b.GetContext(), "subquery in group_concat ORDER BY")
		}

		b.insideAgg = true
		expr, err := b.BindExpr(orderExpr, depth, isRoot)
		b.insideAgg = false

		if err != nil {
			return err
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

	w.Frame = getFrame(ws)

	// append
	b.ctx.windows = append(b.ctx.windows, &plan.Expr{
		Expr: &plan.Expr_W{W: w},
	})

	return nil
}

func getFrame(ws *tree.WindowSpec) *plan.FrameClause {

	f := &tree.FrameClause{Type: tree.Range}

	if ws.OrderBy == nil {
		f.Start = &tree.FrameBound{Type: tree.Preceding, UnBounded: true}
		f.End = &tree.FrameBound{Type: tree.Following, UnBounded: true}
	} else {
		f.Start = &tree.FrameBound{Type: tree.Preceding, UnBounded: true}
		f.End = &tree.FrameBound{Type: tree.CurrentRow}
	}

	ws.HasFrame = false
	ws.Frame = f

	return &plan.FrameClause{
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
}

func (b *HavingBinder) BindWinFunc(funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	if b.insideAgg {
		return nil, moerr.NewSyntaxError(b.GetContext(), "aggregate function calls cannot contain window function calls")
	} else {
		return nil, moerr.NewSyntaxErrorf(b.GetContext(), "window %s functions not allowed in having clause", funcName)
	}
}

func (b *HavingBinder) BindSubquery(astExpr *tree.Subquery, isRoot bool) (*plan.Expr, error) {
	return b.baseBindSubquery(astExpr, isRoot)
}

func (b *HavingBinder) BindTimeWindowFunc(funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	if astExpr.Type == tree.FUNC_TYPE_DISTINCT {
		return nil, moerr.NewNotSupported(b.GetContext(), "DISTINCT in time window")
	}
	var err error

	forgeColCnt := int32(0)
	for _, expr := range b.ctx.times {
		if e, ok := expr.Expr.(*plan.Expr_Col); ok {
			if e.Col.Name == TimeWindowStart {
				forgeColCnt++
			}
			if e.Col.Name == TimeWindowEnd {
				forgeColCnt++
			}
		}
	}

	colPos := int32(len(b.ctx.times))
	aggColPos := colPos - forgeColCnt

	expr := DeepCopyExpr(b.ctx.aggregates[aggColPos])
	expr.Expr.(*plan.Expr_F).F.Args = []*plan.Expr{
		{
			Typ: b.ctx.aggregates[aggColPos].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: b.ctx.aggregateTag,
					ColPos: aggColPos,
				},
			},
		},
	}
	if b.ctx.sliding {
		expr, err = b.remapAggToTimeWindowResultAgg(expr)
		if err != nil {
			return nil, err
		}
	}
	b.ctx.times = append(b.ctx.times, expr)

	astStr := tree.String(astExpr, dialect.MYSQL)
	b.ctx.timeByAst[astStr] = colPos

	return &plan.Expr{
		Typ: expr.Typ,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: b.ctx.timeTag,
				ColPos: colPos,
			},
		},
	}, nil
}
