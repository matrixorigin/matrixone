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
	"encoding/binary"

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
	astStr := windowExprAstKey(astExpr)

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
	if funcName == NameGroupConcat {
		if err := b.bindGroupConcatOrderBy(astExpr, expr, depth, isRoot); err != nil {
			b.insideAgg = false
			return nil, err
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

func (b *HavingBinder) bindGroupConcatOrderBy(astExpr *tree.FuncExpr, expr *plan.Expr, depth int32, isRoot bool) error {
	if len(astExpr.OrderBy) < 1 {
		return nil
	}

	fn := expr.GetF()
	if fn == nil {
		return moerr.NewInternalError(b.GetContext(), "invalid group_concat expression")
	}
	concatArgCnt := len(fn.Args) - 1 // last arg is separator
	if concatArgCnt < 1 {
		return moerr.NewSyntaxError(b.GetContext(), "group_concat requires arguments")
	}

	separatorLit := fn.Args[concatArgCnt].GetLit()
	if separatorLit == nil {
		return moerr.NewInternalError(b.GetContext(), "invalid group_concat separator")
	}
	separator := separatorLit.GetSval()

	orderExprs := make([]*plan.Expr, 0, len(astExpr.OrderBy))
	orderFlags := make([]byte, 0, len(astExpr.OrderBy))
	for _, order := range astExpr.OrderBy {
		orderExpr := order.Expr
		if numVal, ok := order.Expr.(*tree.NumVal); ok {
			switch numVal.Kind() {
			case tree.Int:
				if numVal.Negative() {
					return moerr.NewSyntaxErrorf(b.GetContext(), "ORDER BY position %s is negative", numVal.String())
				}
				colPos, ok := numVal.Uint64()
				if !ok {
					return moerr.NewSyntaxError(b.GetContext(), "non-integer constant in ORDER BY")
				}
				if colPos < 1 || colPos > uint64(concatArgCnt) {
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

		oldInsideAgg := b.insideAgg
		b.insideAgg = true
		expr, err := b.BindExpr(orderExpr, depth, isRoot)
		b.insideAgg = oldInsideAgg

		if err != nil {
			return err
		}

		switch order.Direction {
		case tree.Descending:
			orderFlags = append(orderFlags, 1)
		case tree.Ascending:
			orderFlags = append(orderFlags, 0)
		default:
			orderFlags = append(orderFlags, 0)
		}

		orderExprs = append(orderExprs, expr)
	}
	config := encodeGroupConcatOrderConfig(concatArgCnt, orderFlags, separator)
	args := make([]*plan.Expr, 0, concatArgCnt+len(orderExprs)+1)
	args = append(args, fn.Args[:concatArgCnt]...)
	args = append(args, orderExprs...)
	args = append(args, &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_varchar), Width: int32(len(config)), NotNullable: true},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{
			Value: &plan.Literal_Sval{Sval: string(config)},
		}},
	})
	fn.Args = args

	return nil
}

func encodeGroupConcatOrderConfig(concatArgCnt int, orderFlags []byte, separator string) []byte {
	separatorBytes := []byte(separator)
	config := make([]byte, 0, 16+len(orderFlags)+len(separatorBytes))
	config = append(config, []byte("\x00GCORDER1")...)
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(concatArgCnt))
	config = append(config, buf[:]...)
	binary.BigEndian.PutUint32(buf[:], uint32(len(orderFlags)))
	config = append(config, buf[:]...)
	config = append(config, orderFlags...)
	binary.BigEndian.PutUint32(buf[:], uint32(len(separatorBytes)))
	config = append(config, buf[:]...)
	config = append(config, separatorBytes...)
	return config
}

func (b *HavingBinder) BindWinFunc(funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	if b.insideAgg {
		return nil, moerr.NewSyntaxError(b.GetContext(), "aggregate function calls cannot contain window function calls")
	}
	return bindWindowFuncExpr(b, b.ctx, funcName, astExpr, depth, isRoot)
}

func (b *HavingBinder) BindSubquery(astExpr *tree.Subquery, isRoot bool) (*plan.Expr, error) {
	return b.baseBindSubquery(astExpr, isRoot)
}

func (b *HavingBinder) makeFrameConstValue(expr tree.Expr, typ *plan.Type) (*plan.Expr, error) {
	return makeWindowFrameConstValue(b.baseBindExpr, b.builder.compCtx.GetProcess(), b.GetContext(), expr, typ)
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
