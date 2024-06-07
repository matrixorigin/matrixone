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
	"encoding/hex"
	"fmt"
	"go/constant"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (b *baseBinder) baseBindExpr(astExpr tree.Expr, depth int32, isRoot bool) (expr *Expr, err error) {
	switch exprImpl := astExpr.(type) {
	case *tree.NumVal:
		if d, ok := b.impl.(*DefaultBinder); ok {
			expr, err = b.bindNumVal(exprImpl, d.typ)
		} else {
			expr, err = b.bindNumVal(exprImpl, plan.Type{})
		}
	case *tree.ParenExpr:
		expr, err = b.impl.BindExpr(exprImpl.Expr, depth, isRoot)

	case *tree.OrExpr:
		expr, err = b.bindFuncExprImplByAstExpr("or", []tree.Expr{exprImpl.Left, exprImpl.Right}, depth)

	case *tree.NotExpr:
		if subqueryAst, ok := exprImpl.Expr.(*tree.Subquery); ok {
			expr, err = b.impl.BindSubquery(subqueryAst, isRoot)
			if err != nil {
				return
			}

			subquery := expr.GetSub()
			if subquery.Typ == plan.SubqueryRef_EXISTS {
				subquery.Typ = plan.SubqueryRef_NOT_EXISTS
			}
		} else {
			expr, err = b.impl.BindExpr(exprImpl.Expr, depth, false)
			if err != nil {
				return
			}

			expr, err = BindFuncExprImplByPlanExpr(b.GetContext(), "not", []*plan.Expr{expr})
		}

	case *tree.AndExpr:
		expr, err = b.bindFuncExprImplByAstExpr("and", []tree.Expr{exprImpl.Left, exprImpl.Right}, depth)

	case *tree.UnaryExpr:
		expr, err = b.bindUnaryExpr(exprImpl, depth, isRoot)

	case *tree.BinaryExpr:
		expr, err = b.bindBinaryExpr(exprImpl, depth, isRoot)

	case *tree.ComparisonExpr:
		expr, err = b.bindComparisonExpr(exprImpl, depth, isRoot)

	case *tree.FuncExpr:
		expr, err = b.bindFuncExpr(exprImpl, depth, isRoot)

	case *tree.RangeCond:
		expr, err = b.bindRangeCond(exprImpl, depth, isRoot)

	case *tree.UnresolvedName:
		// check existence
		if b.GetContext() != nil && b.GetContext().Value(defines.InSp{}) != nil && b.GetContext().Value(defines.InSp{}).(bool) {
			tmpScope := b.GetContext().Value(defines.VarScopeKey{}).(*[]map[string]interface{})
			for i := len(*tmpScope) - 1; i >= 0; i-- {
				curScope := (*tmpScope)[i]
				if _, ok := curScope[strings.ToLower(exprImpl.Parts[0])]; ok {
					typ := types.T_text.ToType()
					expr = &Expr{
						Typ: makePlan2Type(&typ),
						Expr: &plan.Expr_V{
							V: &plan.VarRef{
								Name:   exprImpl.Parts[0],
								System: false,
								Global: false,
							},
						},
					}
					err = nil
					return
				}
			}
		}
		expr, err = b.impl.BindColRef(exprImpl, depth, isRoot)

	case *tree.SerialExtractExpr:
		expr, err = b.bindFuncExprImplByAstExpr("serial_extract", []tree.Expr{astExpr}, depth)

	case *tree.CastExpr:
		expr, err = b.impl.BindExpr(exprImpl.Expr, depth, false)
		if err != nil {
			return
		}
		var typ Type
		typ, err = getTypeFromAst(b.GetContext(), exprImpl.Type)
		if err != nil {
			return
		}
		expr, err = appendCastBeforeExpr(b.GetContext(), expr, typ)

	case *tree.BitCastExpr:
		expr, err = b.bindFuncExprImplByAstExpr("bit_cast", []tree.Expr{astExpr}, depth)

	case *tree.IsNullExpr:
		expr, err = b.bindFuncExprImplByAstExpr("isnull", []tree.Expr{exprImpl.Expr}, depth)

	case *tree.IsNotNullExpr:
		expr, err = b.bindFuncExprImplByAstExpr("isnotnull", []tree.Expr{exprImpl.Expr}, depth)

	case *tree.IsUnknownExpr:
		expr, err = b.bindFuncExprImplByAstExpr("isnull", []tree.Expr{exprImpl.Expr}, depth)

	case *tree.IsNotUnknownExpr:
		expr, err = b.bindFuncExprImplByAstExpr("isnotnull", []tree.Expr{exprImpl.Expr}, depth)

	case *tree.IsTrueExpr:
		expr, err = b.bindFuncExprImplByAstExpr("istrue", []tree.Expr{exprImpl.Expr}, depth)

	case *tree.IsNotTrueExpr:
		expr, err = b.bindFuncExprImplByAstExpr("isnottrue", []tree.Expr{exprImpl.Expr}, depth)

	case *tree.IsFalseExpr:
		expr, err = b.bindFuncExprImplByAstExpr("isfalse", []tree.Expr{exprImpl.Expr}, depth)

	case *tree.IsNotFalseExpr:
		expr, err = b.bindFuncExprImplByAstExpr("isnotfalse", []tree.Expr{exprImpl.Expr}, depth)

	case *tree.Tuple:
		exprs := make([]*Expr, 0, len(exprImpl.Exprs))
		var planItem *Expr
		for _, astItem := range exprImpl.Exprs {
			planItem, err = b.impl.BindExpr(astItem, depth, false)
			if err != nil {
				return
			}
			exprs = append(exprs, planItem)
		}
		expr = &Expr{
			Expr: &plan.Expr_List{
				List: &plan.ExprList{
					List: exprs,
				},
			},
			Typ: plan.Type{
				Id: int32(types.T_tuple),
			},
		}

	case *tree.CaseExpr:
		expr, err = b.bindCaseExpr(exprImpl, depth, isRoot)

	case *tree.IntervalExpr:
		err = moerr.NewNYI(b.GetContext(), "expr interval'%v'", exprImpl)

	case *tree.XorExpr:
		expr, err = b.bindFuncExprImplByAstExpr("xor", []tree.Expr{exprImpl.Left, exprImpl.Right}, depth)

	case *tree.Subquery:
		expr, err = b.impl.BindSubquery(exprImpl, isRoot)

	case *tree.DefaultVal:
		return &Expr{
			Typ: plan.Type{
				Id:          int32(types.T_bool),
				NotNullable: true,
			},
			Expr: &plan.Expr_Lit{
				Lit: &Const{
					Isnull: false,
					Value: &plan.Literal_Defaultval{
						Defaultval: true,
					},
				},
			},
		}, nil
	case *tree.UpdateVal:
		return &Expr{
			Expr: &plan.Expr_Lit{
				Lit: &Const{
					Isnull: false,
					Value: &plan.Literal_UpdateVal{
						UpdateVal: true,
					},
				},
			},
		}, nil
	case *tree.MaxValue:
		return &Expr{
			Expr: &plan.Expr_Max{
				Max: &MaxValue{
					Value: "maxvalue",
				},
			},
		}, nil
	case *tree.VarExpr:
		expr, err = b.baseBindVar(exprImpl, depth, isRoot)

	case *tree.ParamExpr:
		if !b.builder.isPrepareStatement {
			err = moerr.NewInvalidInput(b.GetContext(), "only prepare statement can use ? expr")
		} else {
			expr, err = b.baseBindParam(exprImpl, depth, isRoot)
		}

	case *tree.StrVal:
		err = moerr.NewNYI(b.GetContext(), "expr str'%v'", exprImpl)

	case *tree.ExprList:
		err = moerr.NewNYI(b.GetContext(), "expr plan.ExprList'%v'", exprImpl)

	case tree.UnqualifiedStar:
		// select * from table
		// * should only appear in SELECT clause
		err = moerr.NewInvalidInput(b.GetContext(), "SELECT clause contains unqualified star")

	default:
		err = moerr.NewNYI(b.GetContext(), "expr '%+v'", exprImpl)
	}

	return
}

func (b *baseBinder) baseBindParam(astExpr *tree.ParamExpr, depth int32, isRoot bool) (expr *plan.Expr, err error) {
	typ := types.T_text.ToType()
	return &Expr{
		Typ: makePlan2Type(&typ),
		Expr: &plan.Expr_P{
			P: &plan.ParamRef{
				Pos: int32(astExpr.Offset),
			},
		},
	}, nil
}

func (b *baseBinder) baseBindVar(astExpr *tree.VarExpr, depth int32, isRoot bool) (expr *plan.Expr, err error) {
	typ := types.T_text.ToType()
	return &Expr{
		Typ: makePlan2Type(&typ),
		Expr: &plan.Expr_V{
			V: &plan.VarRef{
				Name:   astExpr.Name,
				System: astExpr.System,
				Global: astExpr.Global,
			},
		},
	}, nil
}

const (
	TimeWindowStart = "_wstart"
	TimeWindowEnd   = "_wend"
)

func (b *baseBinder) baseBindColRef(astExpr *tree.UnresolvedName, depth int32, isRoot bool) (expr *plan.Expr, err error) {
	if b.ctx == nil {
		return nil, moerr.NewInvalidInput(b.GetContext(), "ambiguous column reference '%v'", astExpr.Parts[0])
	}
	astStr := tree.String(astExpr, dialect.MYSQL)

	col := astExpr.Parts[0]
	table := astExpr.Parts[1]
	name := tree.String(astExpr, dialect.MYSQL)

	if b.ctx.timeTag > 0 && (col == TimeWindowStart || col == TimeWindowEnd) {
		colPos := int32(len(b.ctx.times))
		expr = &plan.Expr{
			Typ: plan.Type{Id: int32(types.T_timestamp)},
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: b.ctx.timeTag,
					ColPos: colPos,
					Name:   col,
				},
			},
		}
		b.ctx.timeByAst[astStr] = colPos
		b.ctx.times = append(b.ctx.times, expr)
		return
	}

	relPos := NotFound
	colPos := NotFound
	var typ *plan.Type
	localErrCtx := errutil.ContextWithNoReport(b.GetContext(), true)

	if len(table) == 0 {
		if binding, ok := b.ctx.bindingByCol[col]; ok {
			if binding != nil {
				relPos = binding.tag
				colPos = binding.colIdByName[col]
				typ = DeepCopyType(binding.types[colPos])
				table = binding.table
			} else {
				return nil, moerr.NewInvalidInput(b.GetContext(), "ambiguous column reference '%v'", name)
			}
		} else {
			err = moerr.NewInvalidInput(localErrCtx, "column %s does not exist", name)
		}
	} else {
		if binding, ok := b.ctx.bindingByTable[table]; ok {
			colPos = binding.FindColumn(col)
			if colPos == AmbiguousName {
				return nil, moerr.NewInvalidInput(b.GetContext(), "ambiguous column reference '%v'", name)
			}
			if colPos != NotFound {
				typ = DeepCopyType(binding.types[colPos])
				relPos = binding.tag
			} else {
				err = moerr.NewInvalidInput(localErrCtx, "column '%s' does not exist", name)
			}
		} else {
			err = moerr.NewInvalidInput(localErrCtx, "missing FROM-clause entry for table '%v'", table)
		}
	}

	if typ != nil && typ.Id == int32(types.T_enum) && len(typ.GetEnumvalues()) != 0 {
		if err != nil {
			errutil.ReportError(b.GetContext(), err)
			return
		}
		astArgs := []tree.Expr{
			tree.NewNumValWithType(constant.MakeString(typ.Enumvalues), typ.Enumvalues, false, tree.P_char),
		}

		// bind ast function's args
		args := make([]*Expr, len(astArgs)+1)
		for idx, arg := range astArgs {
			if idx == len(args)-1 {
				continue
			}
			expr, err := b.impl.BindExpr(arg, depth, false)
			if err != nil {
				return nil, err
			}
			args[idx] = expr
		}
		args[len(args)-1] = &Expr{
			Typ: *typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: relPos,
					ColPos: colPos,
					Name:   col,
				},
			},
		}

		return BindFuncExprImplByPlanExpr(b.GetContext(), moEnumCastIndexToValueFun, args)
	}

	if colPos != NotFound {
		b.boundCols = append(b.boundCols, table+"."+col)

		expr = &plan.Expr{
			Typ: *typ,
		}

		if depth == 0 {
			expr.Expr = &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: relPos,
					ColPos: colPos,
					Name:   col,
				},
			}
		} else {
			expr.Expr = &plan.Expr_Corr{
				Corr: &plan.CorrColRef{
					RelPos: relPos,
					ColPos: colPos,
					Depth:  depth,
				},
			}
		}
		if err != nil {
			errutil.ReportError(b.GetContext(), err)
		}
		return
	}

	parent := b.ctx.parent
	for parent != nil && parent.binder == nil {
		parent = parent.parent
	}

	if parent == nil {
		if err != nil {
			errutil.ReportError(b.GetContext(), err)
		}
		return
	}

	expr, err = parent.binder.BindColRef(astExpr, depth+1, isRoot)

	if err == nil {
		b.ctx.isCorrelated = true
	}

	return
}

func (b *baseBinder) baseBindSubquery(astExpr *tree.Subquery, isRoot bool) (*Expr, error) {
	if b.ctx == nil {
		return nil, moerr.NewInvalidInput(b.GetContext(), "field reference doesn't support SUBQUERY")
	}
	subCtx := NewBindContext(b.builder, b.ctx)

	var nodeID int32
	var err error
	switch subquery := astExpr.Select.(type) {
	case *tree.ParenSelect:
		nodeID, err = b.builder.buildSelect(subquery.Select, subCtx, false)
		if err != nil {
			return nil, err
		}
	case *tree.Select:
		nodeID, err = b.builder.buildSelect(subquery, subCtx, false)
		if err != nil {
			return nil, err
		}

	default:
		return nil, moerr.NewNYI(b.GetContext(), "unsupported select statement: %s", tree.String(astExpr, dialect.MYSQL))
	}

	rowSize := int32(len(subCtx.results))

	returnExpr := &plan.Expr{
		Typ: plan.Type{
			Id: int32(types.T_tuple),
		},
		Expr: &plan.Expr_Sub{
			Sub: &plan.SubqueryRef{
				NodeId:  nodeID,
				RowSize: rowSize,
			},
		},
	}

	if astExpr.Exists {
		returnExpr.Typ = plan.Type{
			Id:          int32(types.T_bool),
			NotNullable: true,
		}
		returnExpr.GetSub().Typ = plan.SubqueryRef_EXISTS
	} else if rowSize == 1 {
		returnExpr.Typ = subCtx.results[0].Typ
	}

	return returnExpr, nil
}

func (b *baseBinder) bindCaseExpr(astExpr *tree.CaseExpr, depth int32, isRoot bool) (*Expr, error) {
	args := make([]tree.Expr, 0, len(astExpr.Whens)+1)
	caseExist := astExpr.Expr != nil

	for _, whenExpr := range astExpr.Whens {
		if caseExist {
			newCandExpr := tree.NewComparisonExpr(tree.EQUAL, astExpr.Expr, whenExpr.Cond)
			args = append(args, newCandExpr)
		} else {
			args = append(args, whenExpr.Cond)
		}
		args = append(args, whenExpr.Val)
	}

	if astExpr.Else != nil {
		args = append(args, astExpr.Else)
	} else {
		args = append(args, tree.NewNumValWithType(constant.MakeUnknown(), "", false, tree.P_null))
	}

	return b.bindFuncExprImplByAstExpr("case", args, depth)
}

func (b *baseBinder) bindRangeCond(astExpr *tree.RangeCond, depth int32, isRoot bool) (*Expr, error) {
	if astExpr.Not {
		// rewrite 'col not between 1, 20' to 'col < 1 or col > 20'
		newLeftExpr := tree.NewComparisonExpr(tree.LESS_THAN, astExpr.Left, astExpr.From)
		newRightExpr := tree.NewComparisonExpr(tree.GREAT_THAN, astExpr.Left, astExpr.To)
		return b.bindFuncExprImplByAstExpr("or", []tree.Expr{newLeftExpr, newRightExpr}, depth)
	} else {
		if _, ok := astExpr.Left.(*tree.Tuple); ok {
			newLeftExpr := tree.NewComparisonExpr(tree.GREAT_THAN_EQUAL, astExpr.Left, astExpr.From)
			newRightExpr := tree.NewComparisonExpr(tree.LESS_THAN_EQUAL, astExpr.Left, astExpr.To)
			return b.bindFuncExprImplByAstExpr("and", []tree.Expr{newLeftExpr, newRightExpr}, depth)
		}

		return b.bindFuncExprImplByAstExpr("between", []tree.Expr{astExpr.Left, astExpr.From, astExpr.To}, depth)
	}
}

func (b *baseBinder) bindUnaryExpr(astExpr *tree.UnaryExpr, depth int32, isRoot bool) (*Expr, error) {
	switch astExpr.Op {
	case tree.UNARY_MINUS:
		return b.bindFuncExprImplByAstExpr("unary_minus", []tree.Expr{astExpr.Expr}, depth)
	case tree.UNARY_PLUS:
		return b.bindFuncExprImplByAstExpr("unary_plus", []tree.Expr{astExpr.Expr}, depth)
	case tree.UNARY_TILDE:
		return b.bindFuncExprImplByAstExpr("unary_tilde", []tree.Expr{astExpr.Expr}, depth)
	case tree.UNARY_MARK:
		return b.bindFuncExprImplByAstExpr("unary_mark", []tree.Expr{astExpr.Expr}, depth)
	}
	return nil, moerr.NewNYI(b.GetContext(), "'%v'", astExpr)
}

func (b *baseBinder) bindBinaryExpr(astExpr *tree.BinaryExpr, depth int32, isRoot bool) (*Expr, error) {
	switch astExpr.Op {
	case tree.PLUS:
		return b.bindFuncExprImplByAstExpr("+", []tree.Expr{astExpr.Left, astExpr.Right}, depth)
	case tree.MINUS:
		return b.bindFuncExprImplByAstExpr("-", []tree.Expr{astExpr.Left, astExpr.Right}, depth)
	case tree.MULTI:
		return b.bindFuncExprImplByAstExpr("*", []tree.Expr{astExpr.Left, astExpr.Right}, depth)
	case tree.MOD:
		return b.bindFuncExprImplByAstExpr("%", []tree.Expr{astExpr.Left, astExpr.Right}, depth)
	case tree.DIV:
		return b.bindFuncExprImplByAstExpr("/", []tree.Expr{astExpr.Left, astExpr.Right}, depth)
	case tree.INTEGER_DIV:
		return b.bindFuncExprImplByAstExpr("div", []tree.Expr{astExpr.Left, astExpr.Right}, depth)
	case tree.BIT_XOR:
		return b.bindFuncExprImplByAstExpr("^", []tree.Expr{astExpr.Left, astExpr.Right}, depth)
	case tree.BIT_OR:
		return b.bindFuncExprImplByAstExpr("|", []tree.Expr{astExpr.Left, astExpr.Right}, depth)
	case tree.BIT_AND:
		return b.bindFuncExprImplByAstExpr("&", []tree.Expr{astExpr.Left, astExpr.Right}, depth)
	case tree.LEFT_SHIFT:
		return b.bindFuncExprImplByAstExpr("<<", []tree.Expr{astExpr.Left, astExpr.Right}, depth)
	case tree.RIGHT_SHIFT:
		return b.bindFuncExprImplByAstExpr(">>", []tree.Expr{astExpr.Left, astExpr.Right}, depth)
	}
	return nil, moerr.NewNYI(b.GetContext(), "'%v' operator", astExpr.Op.ToString())
}

func (b *baseBinder) bindComparisonExpr(astExpr *tree.ComparisonExpr, depth int32, isRoot bool) (*Expr, error) {
	var op string

	switch astExpr.Op {
	case tree.EQUAL:
		op = "="
		switch leftexpr := astExpr.Left.(type) {
		case *tree.Tuple:
			switch rightexpr := astExpr.Right.(type) {
			case *tree.Tuple:
				if len(leftexpr.Exprs) == len(rightexpr.Exprs) {
					var expr1, expr2 *plan.Expr
					var err error
					for i := 1; i < len(leftexpr.Exprs); i++ {
						if i == 1 {
							expr1, err = b.bindFuncExprImplByAstExpr("=", []tree.Expr{leftexpr.Exprs[0], rightexpr.Exprs[0]}, depth)
							if err != nil {
								return nil, err
							}
						}
						expr2, err = b.bindFuncExprImplByAstExpr("=", []tree.Expr{leftexpr.Exprs[i], rightexpr.Exprs[i]}, depth)
						if err != nil {
							return nil, err
						}
						expr1, err = BindFuncExprImplByPlanExpr(b.GetContext(), "and", []*plan.Expr{expr1, expr2})
						if err != nil {
							return nil, err
						}
					}
					return expr1, nil
				} else {
					return nil, moerr.NewInvalidInput(b.GetContext(), "two tuples have different length(%v,%v)", len(leftexpr.Exprs), len(rightexpr.Exprs))
				}
			}
		}

	case tree.LESS_THAN:
		op = "<"
		switch leftexpr := astExpr.Left.(type) {
		case *tree.Tuple:
			switch rightexpr := astExpr.Right.(type) {
			case *tree.Tuple:
				if len(leftexpr.Exprs) == len(rightexpr.Exprs) {
					var expr1, expr2 *plan.Expr
					var err error
					for i := len(leftexpr.Exprs) - 2; i >= 0; i-- {
						if i == len(leftexpr.Exprs)-2 {
							expr1, err = b.bindFuncExprImplByAstExpr("<", []tree.Expr{leftexpr.Exprs[i+1], rightexpr.Exprs[i+1]}, depth)
							if err != nil {
								return nil, err
							}
						}
						expr2, err = b.bindFuncExprImplByAstExpr("=", []tree.Expr{leftexpr.Exprs[i], rightexpr.Exprs[i]}, depth)
						if err != nil {
							return nil, err
						}
						expr1, err = BindFuncExprImplByPlanExpr(b.GetContext(), "and", []*plan.Expr{expr1, expr2})
						if err != nil {
							return nil, err
						}
						expr2, err = b.bindFuncExprImplByAstExpr("<", []tree.Expr{leftexpr.Exprs[i], rightexpr.Exprs[i]}, depth)
						if err != nil {
							return nil, err
						}
						expr1, err = BindFuncExprImplByPlanExpr(b.GetContext(), "or", []*plan.Expr{expr1, expr2})
						if err != nil {
							return nil, err
						}
					}
					return expr1, nil
				} else {
					return nil, moerr.NewInvalidInput(b.GetContext(), "two tuples have different length(%v,%v)", len(leftexpr.Exprs), len(rightexpr.Exprs))
				}
			}
		}

	case tree.LESS_THAN_EQUAL:
		op = "<="
		switch leftexpr := astExpr.Left.(type) {
		case *tree.Tuple:
			switch rightexpr := astExpr.Right.(type) {
			case *tree.Tuple:
				if len(leftexpr.Exprs) == len(rightexpr.Exprs) {
					var expr1, expr2 *plan.Expr
					var err error
					for i := len(leftexpr.Exprs) - 2; i >= 0; i-- {
						if i == len(leftexpr.Exprs)-2 {
							expr1, err = b.bindFuncExprImplByAstExpr("<=", []tree.Expr{leftexpr.Exprs[i+1], rightexpr.Exprs[i+1]}, depth)
							if err != nil {
								return nil, err
							}
						}
						expr2, err = b.bindFuncExprImplByAstExpr("=", []tree.Expr{leftexpr.Exprs[i], rightexpr.Exprs[i]}, depth)
						if err != nil {
							return nil, err
						}
						expr1, err = BindFuncExprImplByPlanExpr(b.GetContext(), "and", []*plan.Expr{expr1, expr2})
						if err != nil {
							return nil, err
						}
						expr2, err = b.bindFuncExprImplByAstExpr("<", []tree.Expr{leftexpr.Exprs[i], rightexpr.Exprs[i]}, depth)
						if err != nil {
							return nil, err
						}
						expr1, err = BindFuncExprImplByPlanExpr(b.GetContext(), "or", []*plan.Expr{expr1, expr2})
						if err != nil {
							return nil, err
						}
					}
					return expr1, nil
				} else {
					return nil, moerr.NewInvalidInput(b.GetContext(), "two tuples have different length(%v,%v)", len(leftexpr.Exprs), len(rightexpr.Exprs))
				}
			}
		}

	case tree.GREAT_THAN:
		op = ">"
		switch leftexpr := astExpr.Left.(type) {
		case *tree.Tuple:
			switch rightexpr := astExpr.Right.(type) {
			case *tree.Tuple:
				if len(leftexpr.Exprs) == len(rightexpr.Exprs) {
					var expr1, expr2 *plan.Expr
					var err error
					for i := len(leftexpr.Exprs) - 2; i >= 0; i-- {
						if i == len(leftexpr.Exprs)-2 {
							expr1, err = b.bindFuncExprImplByAstExpr(">", []tree.Expr{leftexpr.Exprs[i+1], rightexpr.Exprs[i+1]}, depth)
							if err != nil {
								return nil, err
							}
						}
						expr2, err = b.bindFuncExprImplByAstExpr("=", []tree.Expr{leftexpr.Exprs[i], rightexpr.Exprs[i]}, depth)
						if err != nil {
							return nil, err
						}
						expr1, err = BindFuncExprImplByPlanExpr(b.GetContext(), "and", []*plan.Expr{expr1, expr2})
						if err != nil {
							return nil, err
						}
						expr2, err = b.bindFuncExprImplByAstExpr(">", []tree.Expr{leftexpr.Exprs[i], rightexpr.Exprs[i]}, depth)
						if err != nil {
							return nil, err
						}
						expr1, err = BindFuncExprImplByPlanExpr(b.GetContext(), "or", []*plan.Expr{expr1, expr2})
						if err != nil {
							return nil, err
						}
					}
					return expr1, nil
				} else {
					return nil, moerr.NewInvalidInput(b.GetContext(), "two tuples have different length(%v,%v)", len(leftexpr.Exprs), len(rightexpr.Exprs))
				}
			}
		}

	case tree.GREAT_THAN_EQUAL:
		op = ">="
		switch leftexpr := astExpr.Left.(type) {
		case *tree.Tuple:
			switch rightexpr := astExpr.Right.(type) {
			case *tree.Tuple:
				if len(leftexpr.Exprs) == len(rightexpr.Exprs) {
					var expr1, expr2 *plan.Expr
					var err error
					for i := len(leftexpr.Exprs) - 2; i >= 0; i-- {
						if i == len(leftexpr.Exprs)-2 {
							expr1, err = b.bindFuncExprImplByAstExpr(">=", []tree.Expr{leftexpr.Exprs[i+1], rightexpr.Exprs[i+1]}, depth)
							if err != nil {
								return nil, err
							}
						}
						expr2, err = b.bindFuncExprImplByAstExpr("=", []tree.Expr{leftexpr.Exprs[i], rightexpr.Exprs[i]}, depth)
						if err != nil {
							return nil, err
						}
						expr1, err = BindFuncExprImplByPlanExpr(b.GetContext(), "and", []*plan.Expr{expr1, expr2})
						if err != nil {
							return nil, err
						}
						expr2, err = b.bindFuncExprImplByAstExpr(">", []tree.Expr{leftexpr.Exprs[i], rightexpr.Exprs[i]}, depth)
						if err != nil {
							return nil, err
						}
						expr1, err = BindFuncExprImplByPlanExpr(b.GetContext(), "or", []*plan.Expr{expr1, expr2})
						if err != nil {
							return nil, err
						}
					}
					return expr1, nil
				} else {
					return nil, moerr.NewInvalidInput(b.GetContext(), "two tuples have different length(%v,%v)", len(leftexpr.Exprs), len(rightexpr.Exprs))
				}
			}
		}

	case tree.NOT_EQUAL:
		op = "<>"
		switch leftexpr := astExpr.Left.(type) {
		case *tree.Tuple:
			switch rightexpr := astExpr.Right.(type) {
			case *tree.Tuple:
				if len(leftexpr.Exprs) == len(rightexpr.Exprs) {
					var expr1, expr2 *plan.Expr
					var err error
					for i := 1; i < len(leftexpr.Exprs); i++ {
						if i == 1 {
							expr1, err = b.bindFuncExprImplByAstExpr("<>", []tree.Expr{leftexpr.Exprs[0], rightexpr.Exprs[0]}, depth)
							if err != nil {
								return nil, err
							}
						}
						expr2, err = b.bindFuncExprImplByAstExpr("<>", []tree.Expr{leftexpr.Exprs[i], rightexpr.Exprs[i]}, depth)
						if err != nil {
							return nil, err
						}
						expr1, err = BindFuncExprImplByPlanExpr(b.GetContext(), "or", []*plan.Expr{expr1, expr2})
						if err != nil {
							return nil, err
						}
					}
					return expr1, nil
				} else {
					return nil, moerr.NewInvalidInput(b.GetContext(), "two tuples have different length(%v,%v)", len(leftexpr.Exprs), len(rightexpr.Exprs))
				}
			}
		}

	case tree.LIKE:
		op = "like"

	case tree.NOT_LIKE:
		newExpr := tree.NewComparisonExpr(tree.LIKE, astExpr.Left, astExpr.Right)
		return b.bindFuncExprImplByAstExpr("not", []tree.Expr{newExpr}, depth)

	case tree.ILIKE:
		op = "ilike"

	case tree.NOT_ILIKE:
		newExpr := tree.NewComparisonExpr(tree.ILIKE, astExpr.Left, astExpr.Right)
		return b.bindFuncExprImplByAstExpr("not", []tree.Expr{newExpr}, depth)

	case tree.IN:
		switch astExpr.Right.(type) {
		case *tree.Tuple:
			op = "in"

		default:
			leftArg, err := b.impl.BindExpr(astExpr.Left, depth, false)
			if err != nil {
				return nil, err
			}

			rightArg, err := b.impl.BindExpr(astExpr.Right, depth, false)
			if err != nil {
				return nil, err
			}

			if subquery := rightArg.GetSub(); subquery != nil {
				if list := leftArg.GetList(); list != nil {
					if len(list.List) != int(subquery.RowSize) {
						return nil, moerr.NewNYI(b.GetContext(), "subquery should return %d columns", len(list.List))
					}
				} else {
					if subquery.RowSize > 1 {
						return nil, moerr.NewInvalidInput(b.GetContext(), "subquery returns more than 1 column")
					}
				}

				subquery.Typ = plan.SubqueryRef_IN
				subquery.Child = leftArg

				rightArg.Typ = plan.Type{
					Id:          int32(types.T_bool),
					NotNullable: leftArg.Typ.NotNullable && rightArg.Typ.NotNullable,
				}

				return rightArg, nil
			} else {
				return BindFuncExprImplByPlanExpr(b.GetContext(), "in", []*plan.Expr{leftArg, rightArg})
			}
		}

	case tree.NOT_IN:
		switch astExpr.Right.(type) {
		case *tree.Tuple:
			op = "not_in"

		default:
			leftArg, err := b.impl.BindExpr(astExpr.Left, depth, false)
			if err != nil {
				return nil, err
			}

			rightArg, err := b.impl.BindExpr(astExpr.Right, depth, false)
			if err != nil {
				return nil, err
			}

			if subquery := rightArg.GetSub(); subquery != nil {
				if list := leftArg.GetList(); list != nil {
					if len(list.List) != int(subquery.RowSize) {
						return nil, moerr.NewInvalidInput(b.GetContext(), "subquery should return %d columns", len(list.List))
					}
				} else {
					if subquery.RowSize > 1 {
						return nil, moerr.NewInvalidInput(b.GetContext(), "subquery should return 1 column")
					}
				}

				subquery.Typ = plan.SubqueryRef_NOT_IN
				subquery.Child = leftArg

				rightArg.Typ = plan.Type{
					Id:          int32(types.T_bool),
					NotNullable: leftArg.Typ.NotNullable && rightArg.Typ.NotNullable,
				}

				return rightArg, nil
			} else {
				expr, err := BindFuncExprImplByPlanExpr(b.GetContext(), "in", []*plan.Expr{leftArg, rightArg})
				if err != nil {
					return nil, err
				}

				return BindFuncExprImplByPlanExpr(b.GetContext(), "not", []*plan.Expr{expr})
			}
		}
	case tree.REG_MATCH:
		op = "reg_match"
	case tree.NOT_REG_MATCH:
		op = "not_reg_match"
	default:
		return nil, moerr.NewNYI(b.GetContext(), "'%v'", astExpr)
	}

	if astExpr.SubOp >= tree.ANY {
		expr, err := b.impl.BindExpr(astExpr.Right, depth, false)
		if err != nil {
			return nil, err
		}

		child, err := b.impl.BindExpr(astExpr.Left, depth, false)
		if err != nil {
			return nil, err
		}

		if subquery := expr.GetSub(); subquery != nil {
			if list := child.GetList(); list != nil {
				if len(list.List) != int(subquery.RowSize) {
					return nil, moerr.NewInvalidInput(b.GetContext(), "subquery should return %d columns", len(list.List))
				}
			} else {
				if subquery.RowSize > 1 {
					return nil, moerr.NewInvalidInput(b.GetContext(), "subquery should return 1 column")
				}
			}

			subquery.Op = op
			subquery.Child = child

			switch astExpr.SubOp {
			case tree.ANY, tree.SOME:
				subquery.Typ = plan.SubqueryRef_ANY
			case tree.ALL:
				subquery.Typ = plan.SubqueryRef_ALL
			}

			expr.Typ = plan.Type{
				Id:          int32(types.T_bool),
				NotNullable: expr.Typ.NotNullable && child.Typ.NotNullable,
			}

			return expr, nil
		} else {
			return nil, moerr.NewInvalidInput(b.GetContext(), "subquery '%s' is not a quantifying subquery", astExpr.SubOp.ToString())
		}
	}

	return b.bindFuncExprImplByAstExpr(op, []tree.Expr{astExpr.Left, astExpr.Right}, depth)
}

func (b *baseBinder) bindFuncExpr(astExpr *tree.FuncExpr, depth int32, isRoot bool) (*Expr, error) {
	funcRef, ok := astExpr.Func.FunctionReference.(*tree.UnresolvedName)
	if !ok {
		return nil, moerr.NewNYI(b.GetContext(), "function expr '%v'", astExpr)
	}
	funcName := funcRef.Parts[0]

	if function.GetFunctionIsAggregateByName(funcName) && astExpr.WindowSpec == nil {
		if b.ctx.timeTag > 0 {
			return b.impl.BindTimeWindowFunc(funcName, astExpr, depth, isRoot)
		}
		return b.impl.BindAggFunc(funcName, astExpr, depth, isRoot)
	} else if function.GetFunctionIsWinFunByName(funcName) {
		return b.impl.BindWinFunc(funcName, astExpr, depth, isRoot)
	}

	return b.bindFuncExprImplByAstExpr(funcName, astExpr.Exprs, depth)
}

func (b *baseBinder) bindFuncExprImplByAstExpr(name string, astArgs []tree.Expr, depth int32) (*plan.Expr, error) {
	// rewrite some ast Exprs before binding
	switch name {
	case "nullif":
		// rewrite 'nullif(expr1, expr2)' to 'case when expr1=expr2 then null else expr1'
		if len(astArgs) != 2 {
			return nil, moerr.NewInvalidArg(b.GetContext(), "nullif need two args", len(astArgs))
		}
		elseExpr := astArgs[0]
		thenExpr := tree.NewNumValWithType(constant.MakeUnknown(), "", false, tree.P_char)
		whenExpr := tree.NewComparisonExpr(tree.EQUAL, astArgs[0], astArgs[1])
		astArgs = []tree.Expr{whenExpr, thenExpr, elseExpr}
		name = "case"

	case "ifnull":
		// rewrite 'ifnull(expr1, expr2)' to 'case when isnull(expr1) then expr2 else null'
		if len(astArgs) != 2 {
			return nil, moerr.NewInvalidArg(b.GetContext(), "ifnull function need two args", len(astArgs))
		}
		elseExpr := astArgs[0]
		thenExpr := astArgs[1]
		whenExpr := tree.NewIsNullExpr(astArgs[0])
		astArgs = []tree.Expr{whenExpr, thenExpr, elseExpr}
		name = "case"

		//case "extract":
		//	// "extract(year from col_name)"  parser return year as UnresolvedName.
		//	// we must rewrite it to string。 because binder bind UnresolvedName as column name
		//	unit := astArgs[0].(*tree.UnresolvedName).Parts[0]
		//	astArgs[0] = tree.NewNumVal(constant.MakeString(unit), unit, false)

	case "count":
		if b.ctx == nil {
			return nil, moerr.NewInvalidInput(b.GetContext(), "invalid field reference to COUNT")
		}
		// we will rewrite "count(*)" to "starcount(col)"
		// count(*) : astExprs[0].(type) is *tree.NumVal
		// count(col_name) : astExprs[0].(type) is *tree.UnresolvedName
		switch nval := astArgs[0].(type) {
		case *tree.NumVal:
			if nval.String() == "*" {
				if len(b.ctx.bindings) == 0 || len(b.ctx.bindings[0].cols) == 0 {
					// sql: 'select count(*)' without from clause. we do nothing
				} else {
					// sql: 'select count(*) from t1',
					// rewrite count(*) to starcount(col_name)
					name = "starcount"

					astArgs = []tree.Expr{tree.NewNumValWithType(constant.MakeInt64(1), "1", false, tree.P_int64)}
				}
			}
		}

	case "approx_count":
		if b.ctx == nil {
			return nil, moerr.NewInvalidInput(b.GetContext(), "invalid field reference to COUNT")
		}
		name = "count"

	case "trim":
		astArgs = astArgs[1:]
	}

	// bind ast function's args
	var args []*Expr
	if name == "bit_cast" {
		bitCastExpr := astArgs[0].(*tree.BitCastExpr)
		binExpr, err := b.impl.BindExpr(bitCastExpr.Expr, depth, false)
		if err != nil {
			return nil, err
		}

		typ, err := getTypeFromAst(b.GetContext(), bitCastExpr.Type)
		if err != nil {
			return nil, err
		}
		typeExpr := &Expr{
			Typ: typ,
			Expr: &plan.Expr_T{
				T: &plan.TargetType{},
			},
		}

		args = []*Expr{binExpr, typeExpr}
	} else if name == "serial_extract" {
		serialExtractExpr := astArgs[0].(*tree.SerialExtractExpr)

		// 1. bind serial expr
		serialExpr, err := b.impl.BindExpr(serialExtractExpr.SerialExpr, depth, false)
		if err != nil {
			return nil, err
		}

		// 2. bind index expr
		idxExpr, err := b.impl.BindExpr(serialExtractExpr.IndexExpr, depth, false)
		if err != nil {
			return nil, err
		}

		// 3. bind type
		typ, err := getTypeFromAst(b.GetContext(), serialExtractExpr.ResultType)
		if err != nil {
			return nil, err
		}
		typeExpr := &Expr{
			Typ: typ,
			Expr: &plan.Expr_T{
				T: &plan.TargetType{},
			},
		}

		// 4. return [serialExpr, idxExpr, typeExpr]. Used in list_builtIn.go
		args = []*Expr{serialExpr, idxExpr, typeExpr}
	} else {
		args = make([]*Expr, len(astArgs))
		for idx, arg := range astArgs {
			expr, err := b.impl.BindExpr(arg, depth, false)
			if err != nil {
				return nil, err
			}

			args[idx] = expr
		}
	}

	if b.builder != nil {
		e, err := bindFuncExprAndConstFold(b.GetContext(), b.builder.compCtx.GetProcess(), name, args)
		if err == nil {
			return e, nil
		}
		if !strings.Contains(err.Error(), "not supported") {
			return nil, err
		}
	} else {
		// return bindFuncExprImplByPlanExpr(b.GetContext(), name, args)
		// first look for builtin func
		builtinExpr, err := BindFuncExprImplByPlanExpr(b.GetContext(), name, args)
		if err == nil {
			return builtinExpr, nil
		}
		if !strings.Contains(err.Error(), "not supported") {
			return nil, err
		}
	}

	// not a builtin func, look to resolve udf
	cmpCtx := b.builder.compCtx
	udf, err := cmpCtx.ResolveUdf(name, args)
	if err != nil {
		return nil, err
	}

	return bindFuncExprImplUdf(b, name, udf, astArgs, depth)
}

func bindFuncExprImplUdf(b *baseBinder, name string, udf *function.Udf, args []tree.Expr, depth int32) (*plan.Expr, error) {
	if udf == nil {
		return nil, moerr.NewNotSupported(b.GetContext(), "function '%s'", name)
	}

	switch udf.Language {
	case string(tree.SQL):
		sql := udf.Body
		// replace sql with actual arg value
		fmtctx := tree.NewFmtCtx(dialect.MYSQL, tree.WithQuoteString(true))
		for i := 0; i < len(args); i++ {
			args[i].Format(fmtctx)
			sql = strings.Replace(sql, "$"+strconv.Itoa(i+1), fmtctx.String(), 1)
			fmtctx.Reset()
		}

		// if does not contain SELECT, an expression. In order to pass the parser,
		// make it start with a 'SELECT'.

		var expr *plan.Expr

		if !strings.Contains(sql, "select") {
			sql = "select " + sql
			substmts, err := parsers.Parse(b.GetContext(), dialect.MYSQL, sql, 1, 0)
			if err != nil {
				return nil, err
			}
			expr, err = b.impl.BindExpr(substmts[0].(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr, depth, false)
			if err != nil {
				return nil, err
			}
		} else {
			substmts, err := parsers.Parse(b.GetContext(), dialect.MYSQL, sql, 1, 0)
			if err != nil {
				return nil, err
			}
			subquery := tree.NewSubquery(substmts[0], false)
			expr, err = b.impl.BindSubquery(subquery, false)
			if err != nil {
				return nil, err
			}
		}
		return expr, nil
	case string(tree.PYTHON):
		expr, err := b.bindPythonUdf(udf, args, depth)
		if err != nil {
			return nil, err
		}
		return expr, nil
	default:
		return nil, moerr.NewInvalidArg(b.GetContext(), "function language", udf.Language)
	}
}

func (b *baseBinder) bindPythonUdf(udf *function.Udf, astArgs []tree.Expr, depth int32) (*plan.Expr, error) {
	args := make([]*Expr, 2*len(astArgs)+2)

	// python udf self info and query context
	args[0] = udf.GetPlanExpr()

	// bind ast function's args
	for idx, arg := range astArgs {
		expr, err := b.impl.BindExpr(arg, depth, false)
		if err != nil {
			return nil, err
		}
		args[idx+1] = expr
	}

	// function args
	fArgTypes := udf.GetArgsPlanType()
	for i, t := range fArgTypes {
		args[len(astArgs)+i+1] = &Expr{Typ: *t}
	}

	// function ret
	fRetType := udf.GetRetPlanType()
	args[2*len(astArgs)+1] = &Expr{Typ: *fRetType}

	return BindFuncExprImplByPlanExpr(b.GetContext(), "python_user_defined_function", args)
}

func bindFuncExprAndConstFold(ctx context.Context, proc *process.Process, name string, args []*Expr) (*plan.Expr, error) {
	retExpr, err := BindFuncExprImplByPlanExpr(ctx, name, args)
	if err != nil {
		return nil, err
	}

	switch retExpr.GetF().GetFunc().GetObjName() {
	case "+", "-", "*", "/", "unary_minus", "unary_plus", "unary_tilde", "cast", "serial", "serial_full":
		if proc != nil {
			tmpexpr, _ := ConstantFold(batch.EmptyForConstFoldBatch, DeepCopyExpr(retExpr), proc, false)
			if tmpexpr != nil {
				retExpr = tmpexpr
			}
		}

	case "between":
		if proc == nil {
			goto between_fallback
		}

		fnArgs := retExpr.GetF().Args
		arg1, err := ConstantFold(batch.EmptyForConstFoldBatch, fnArgs[1], proc, false)
		if err != nil {
			goto between_fallback
		}
		fnArgs[1] = arg1

		lit0 := arg1.GetLit()
		if arg1.Typ.Id == int32(types.T_any) || lit0 == nil {
			goto between_fallback
		}

		arg2, err := ConstantFold(batch.EmptyForConstFoldBatch, fnArgs[2], proc, false)
		if err != nil {
			goto between_fallback
		}
		fnArgs[2] = arg2

		lit1 := arg1.GetLit()
		if arg1.Typ.Id == int32(types.T_any) || lit1 == nil {
			goto between_fallback
		}

		rangeCheckFn, _ := BindFuncExprImplByPlanExpr(ctx, "<=", []*plan.Expr{arg1, arg2})
		rangeCheckRes, _ := ConstantFold(batch.EmptyForConstFoldBatch, rangeCheckFn, proc, false)
		rangeCheckVal := rangeCheckRes.GetLit()
		if rangeCheckVal == nil || !rangeCheckVal.GetBval() {
			goto between_fallback
		}

		retExpr, _ = ConstantFold(batch.EmptyForConstFoldBatch, retExpr, proc, false)
	}

	return retExpr, nil

between_fallback:
	fnArgs := retExpr.GetF().Args
	leftFn, err := BindFuncExprImplByPlanExpr(ctx, ">=", []*plan.Expr{DeepCopyExpr(fnArgs[0]), fnArgs[1]})
	if err != nil {
		return nil, err
	}
	rightFn, err := BindFuncExprImplByPlanExpr(ctx, "<=", []*plan.Expr{fnArgs[0], fnArgs[2]})
	if err != nil {
		return nil, err
	}

	retExpr, err = BindFuncExprImplByPlanExpr(ctx, "and", []*plan.Expr{leftFn, rightFn})
	if err != nil {
		return nil, err
	}
	retExpr, err = ConstantFold(batch.EmptyForConstFoldBatch, retExpr, proc, false)
	if err != nil {
		return nil, err
	}

	return retExpr, nil
}

func BindFuncExprImplByPlanExpr(ctx context.Context, name string, args []*Expr) (*plan.Expr, error) {
	var err error

	// deal with some special function
	switch name {
	case "interval":
		// rewrite interval function to ListExpr, and return directly
		return &plan.Expr{
			Typ: plan.Type{
				Id: int32(types.T_interval),
			},
			Expr: &plan.Expr_List{
				List: &plan.ExprList{
					List: args,
				},
			},
		}, nil
	case "and", "or", "not", "xor":
		// why not append cast function?
		// for i := 0; i < len(args); i++ {
		// 	if args[i].Typ.Id != types.T_bool {
		// 		arg, err := appendCastBeforeExpr(args[i], &plan.Type{
		// 			Id: types.T_bool,
		// 		})
		// 		if err != nil {
		// 			return nil, err
		// 		}
		// 		args[i] = arg
		// 	}
		// }
		if err := convertValueIntoBool(name, args, true); err != nil {
			return nil, err
		}
	case "=", "<", "<=", ">", ">=", "<>":
		// why not append cast function?
		if err := convertValueIntoBool(name, args, false); err != nil {
			return nil, err
		}
	case "date_add", "date_sub":
		// rewrite date_add/date_sub function
		// date_add(col_name, "1 day"), will rewrite to date_add(col_name, number, unit)
		if len(args) != 2 {
			return nil, moerr.NewInvalidArg(ctx, "date_add/date_sub function need two args", len(args))
		}
		args, err = resetDateFunction(ctx, args[0], args[1])
		if err != nil {
			return nil, err
		}
	case "adddate", "subdate":
		if len(args) != 2 {
			return nil, moerr.NewInvalidArg(ctx, "adddate/subdate function need two args", len(args))
		}
		args, err = resetDateFunction(ctx, args[0], args[1])
		if err != nil {
			return nil, err
		}
		if name == "adddate" {
			name = "date_add"
		} else {
			name = "date_sub"
		}
	case "+":
		if len(args) != 2 {
			return nil, moerr.NewInvalidArg(ctx, "operator + need two args", len(args))
		}
		if isNullExpr(args[0]) {
			return args[0], nil
		}
		if isNullExpr(args[1]) {
			return args[1], nil
		}
		if args[0].Typ.Id == int32(types.T_date) && args[1].Typ.Id == int32(types.T_interval) {
			name = "date_add"
			args, err = resetDateFunctionArgs(ctx, args[0], args[1])
		} else if args[0].Typ.Id == int32(types.T_interval) && args[1].Typ.Id == int32(types.T_date) {
			name = "date_add"
			args, err = resetDateFunctionArgs(ctx, args[1], args[0])
		} else if args[0].Typ.Id == int32(types.T_datetime) && args[1].Typ.Id == int32(types.T_interval) {
			name = "date_add"
			args, err = resetDateFunctionArgs(ctx, args[0], args[1])
		} else if args[0].Typ.Id == int32(types.T_interval) && args[1].Typ.Id == int32(types.T_datetime) {
			name = "date_add"
			args, err = resetDateFunctionArgs(ctx, args[1], args[0])
		} else if args[0].Typ.Id == int32(types.T_varchar) && args[1].Typ.Id == int32(types.T_interval) {
			name = "date_add"
			args, err = resetDateFunctionArgs(ctx, args[0], args[1])
		} else if args[0].Typ.Id == int32(types.T_interval) && args[1].Typ.Id == int32(types.T_varchar) {
			name = "date_add"
			args, err = resetDateFunctionArgs(ctx, args[1], args[0])
		} else if args[0].Typ.Id == int32(types.T_varchar) && args[1].Typ.Id == int32(types.T_varchar) {
			name = "concat"
		}
		if err != nil {
			return nil, err
		}
	case "-":
		if len(args) != 2 {
			return nil, moerr.NewInvalidArg(ctx, "operator - need two args", len(args))
		}
		if isNullExpr(args[0]) {
			return args[0], nil
		}
		if isNullExpr(args[1]) {
			return args[1], nil
		}
		// rewrite "date '2001' - interval '1 day'" to date_sub(date '2001', 1, day(unit))
		if args[0].Typ.Id == int32(types.T_date) && args[1].Typ.Id == int32(types.T_interval) {
			name = "date_sub"
			args, err = resetDateFunctionArgs(ctx, args[0], args[1])
		} else if args[0].Typ.Id == int32(types.T_datetime) && args[1].Typ.Id == int32(types.T_interval) {
			name = "date_sub"
			args, err = resetDateFunctionArgs(ctx, args[0], args[1])
		} else if args[0].Typ.Id == int32(types.T_varchar) && args[1].Typ.Id == int32(types.T_interval) {
			name = "date_sub"
			args, err = resetDateFunctionArgs(ctx, args[0], args[1])
		}
		if err != nil {
			return nil, err
		}
	case "*", "/", "%":
		if len(args) != 2 {
			return nil, moerr.NewInvalidArg(ctx, fmt.Sprintf("operator %s need two args", name), len(args))
		}
		if isNullExpr(args[0]) {
			return args[0], nil
		}
		if isNullExpr(args[1]) {
			return args[1], nil
		}
	case "unary_minus":
		if len(args) == 0 {
			return nil, moerr.NewInvalidArg(ctx, name+" function have invalid input args length", len(args))
		}
		if args[0].Typ.Id == int32(types.T_uint64) {
			args[0], err = appendCastBeforeExpr(ctx, args[0], plan.Type{
				Id:          int32(types.T_decimal128),
				NotNullable: args[0].Typ.NotNullable,
			})
			if err != nil {
				return nil, err
			}
		}
	case "oct", "bit_and", "bit_or", "bit_xor":
		if len(args) == 0 {
			return nil, moerr.NewInvalidArg(ctx, name+" function have invalid input args length", len(args))
		}
		if args[0].Typ.Id == int32(types.T_decimal128) || args[0].Typ.Id == int32(types.T_decimal64) {
			args[0], err = appendCastBeforeExpr(ctx, args[0], plan.Type{
				Id:          int32(types.T_float64),
				NotNullable: args[0].Typ.NotNullable,
			})
			if err != nil {
				return nil, err
			}
		}
	case "like":
		// sql 'select * from t where col like ?'  the ? Expr's type will be T_any
		if len(args) != 2 {
			return nil, moerr.NewInvalidArg(ctx, name+" function have invalid input args length", len(args))
		}
		if args[0].Typ.Id == int32(types.T_any) {
			args[0].Typ.Id = int32(types.T_varchar)
		}
		if args[1].Typ.Id == int32(types.T_any) {
			args[1].Typ.Id = int32(types.T_varchar)
		}
		if args[0].Typ.Id == int32(types.T_json) {
			targetTp := types.T_varchar.ToType()
			args[0], err = appendCastBeforeExpr(ctx, args[0], makePlan2Type(&targetTp), false)
			if err != nil {
				return nil, err
			}
		}
		if args[1].Typ.Id == int32(types.T_json) {
			targetTp := types.T_varchar.ToType()
			args[1], err = appendCastBeforeExpr(ctx, args[1], makePlan2Type(&targetTp), false)
			if err != nil {
				return nil, err
			}
		}
	case "timediff":
		if len(args) != 2 {
			return nil, moerr.NewInvalidArg(ctx, name+" function have invalid input args length", len(args))
		}

	case "str_to_date", "to_date":
		if len(args) != 2 {
			return nil, moerr.NewInvalidArg(ctx, name+" function have invalid input args length", len(args))
		}

		if args[1].Typ.Id == int32(types.T_varchar) || args[1].Typ.Id == int32(types.T_char) {
			var tp = types.T_date
			if exprC := args[1].GetLit(); exprC != nil {
				sval := exprC.Value.(*plan.Literal_Sval)
				tp, _ = ExtractToDateReturnType(sval.Sval)
			}
			args = append(args, makePlan2DateConstNullExpr(tp))

		} else if args[1].Typ.Id == int32(types.T_any) {
			args = append(args, makePlan2DateConstNullExpr(types.T_datetime))
		} else {
			return nil, moerr.NewInvalidArg(ctx, name+" function have invalid input args length", len(args))
		}
	case "unix_timestamp":
		if len(args) == 1 {
			if types.T(args[0].Typ.Id).IsMySQLString() {
				if exprC := args[0].GetLit(); exprC != nil {
					sval := exprC.Value.(*plan.Literal_Sval)
					tp := judgeUnixTimestampReturnType(sval.Sval)
					if tp == types.T_int64 {
						args = append(args, makePlan2Int64ConstExprWithType(0))
					} else {
						args = append(args, makePlan2Decimal128ConstNullExpr())
					}
				} else {
					args = append(args, makePlan2Decimal128ConstNullExpr())
				}
			}
		} else if len(args) > 1 {
			return nil, moerr.NewInvalidArg(ctx, name+" function have invalid input args size", len(args))
		}
	case "ascii":
		if len(args) != 1 {
			return nil, moerr.NewInvalidArg(ctx, name+" function have invalid input args length", len(args))
		}
		tp := types.T(args[0].Typ.Id)
		switch {
		case tp.IsMySQLString(), tp.IsInteger():
		default:
			targetTp := types.T_varchar.ToType()
			args[0], err = appendCastBeforeExpr(ctx, args[0], makePlan2Type(&targetTp), false)
			if err != nil {
				return nil, err
			}
		}

	case "in", "not_in":
		//if all the expr in the in list can safely cast to left type, we call it safe
		if rightList := args[1].GetList(); rightList != nil {
			typLeft := makeTypeByPlan2Expr(args[0])
			var inExprList, orExprList []*plan.Expr

			for _, rightVal := range rightList.List {
				if checkNoNeedCast(makeTypeByPlan2Expr(rightVal), typLeft, rightVal) {
					inExpr, err := appendCastBeforeExpr(ctx, rightVal, args[0].Typ)
					if err != nil {
						return nil, err
					}
					inExprList = append(inExprList, inExpr)
				} else {
					orExprList = append(orExprList, rightVal)
				}
			}

			var newExpr *plan.Expr

			if len(inExprList) > 1 {
				leftType := makeTypeByPlan2Expr(args[0])
				argsType := []types.Type{leftType, leftType}
				fGet, err := function.GetFunctionByName(ctx, name, argsType)
				if err != nil {
					return nil, err
				}

				funcID := fGet.GetEncodedOverloadID()
				returnType := fGet.GetReturnType()
				rightList.List = inExprList
				exprType := makePlan2Type(&returnType)
				exprType.NotNullable = function.DeduceNotNullable(funcID, args)
				newExpr = &Expr{
					Typ: exprType,
					Expr: &plan.Expr_F{
						F: &plan.Function{
							Func: getFunctionObjRef(funcID, name),
							Args: args,
						},
					},
				}
			} else if len(inExprList) > 0 {
				orExprList = append(inExprList, orExprList...)
			}

			//expand the in list to col=a or col=b or ......
			if name == "in" {
				for _, expr := range orExprList {
					tmpExpr, _ := BindFuncExprImplByPlanExpr(ctx, "=", []*Expr{DeepCopyExpr(args[0]), expr})
					if newExpr == nil {
						newExpr = tmpExpr
					} else {
						newExpr, _ = BindFuncExprImplByPlanExpr(ctx, "or", []*Expr{newExpr, tmpExpr})
					}
				}
			} else {
				for _, expr := range orExprList {
					tmpExpr, _ := BindFuncExprImplByPlanExpr(ctx, "!=", []*Expr{DeepCopyExpr(args[0]), expr})
					if newExpr == nil {
						newExpr = tmpExpr
					} else {
						newExpr, _ = BindFuncExprImplByPlanExpr(ctx, "and", []*Expr{newExpr, tmpExpr})
					}
				}
			}

			return newExpr, nil
		}
	}

	// get args(exprs) & types
	argsLength := len(args)
	argsType := make([]types.Type, argsLength)
	for idx, expr := range args {
		argsType[idx] = makeTypeByPlan2Expr(expr)
	}

	var funcID int64
	var returnType types.Type
	var argsCastType []types.Type

	// get function definition
	fGet, err := function.GetFunctionByName(ctx, name, argsType)
	if err != nil {
		if name == "between" {
			leftFn, err := BindFuncExprImplByPlanExpr(ctx, ">=", []*plan.Expr{DeepCopyExpr(args[0]), args[1]})
			if err != nil {
				return nil, err
			}

			rightFn, err := BindFuncExprImplByPlanExpr(ctx, "<=", []*plan.Expr{args[0], args[2]})
			if err != nil {
				return nil, err
			}

			return BindFuncExprImplByPlanExpr(ctx, "and", []*plan.Expr{leftFn, rightFn})
		}

		return nil, err
	}

	funcID = fGet.GetEncodedOverloadID()
	returnType = fGet.GetReturnType()
	argsCastType, _ = fGet.ShouldDoImplicitTypeCast()

	if function.GetFunctionIsAggregateByName(name) {
		if constExpr := args[0].GetLit(); constExpr != nil && constExpr.Isnull {
			args[0].Typ = makePlan2Type(&returnType)
		}
	}

	// rewrite some cast rule:  expr:  int32Col > 10,
	// old rule: cast(int32Col as int64) >10 ,   new rule: int32Col > (cast 10 as int32)
	switch name {
	case "=", "<", "<=", ">", ">=", "<>":
		// if constant's type higher than column's type
		// and constant's value in range of column's type, then no cast was needed
		switch args[0].Expr.(type) {
		case *plan.Expr_Lit:
			if args[1].GetCol() != nil {
				if checkNoNeedCast(argsType[0], argsType[1], args[0]) {
					argsCastType = []types.Type{argsType[1], argsType[1]}
					// need to update function id
					fGet, err = function.GetFunctionByName(ctx, name, argsCastType)
					if err != nil {
						return nil, err
					}
					funcID = fGet.GetEncodedOverloadID()
				}
			}
		case *plan.Expr_Col:
			if checkNoNeedCast(argsType[1], argsType[0], args[1]) {
				argsCastType = []types.Type{argsType[0], argsType[0]}
				fGet, err = function.GetFunctionByName(ctx, name, argsCastType)
				if err != nil {
					return nil, err
				}
				funcID = fGet.GetEncodedOverloadID()
			}
		}

	case "like":
		// if constant's type higher than column's type
		// and constant's value in range of column's type, then no cast was needed
		switch args[0].Expr.(type) {
		case *plan.Expr_Col:
			if checkNoNeedCast(argsType[1], argsType[0], args[1]) {
				argsCastType = []types.Type{argsType[0], argsType[0]}
				fGet, err = function.GetFunctionByName(ctx, name, argsCastType)
				if err != nil {
					return nil, err
				}
				funcID = fGet.GetEncodedOverloadID()
			}
		}

	case "between":
		if checkNoNeedCast(argsType[1], argsType[0], args[1]) && checkNoNeedCast(argsType[2], argsType[0], args[2]) {
			argsCastType = []types.Type{argsType[0], argsType[0], argsType[0]}
			fGet, err = function.GetFunctionByName(ctx, name, argsCastType)
			if err != nil {
				return nil, err
			}
			funcID = fGet.GetEncodedOverloadID()
		}

	case "timediff":
		if len(argsType) == len(argsCastType) {
			for i := range argsType {
				if int(argsType[i].Oid) == int(types.T_time) && int(argsCastType[i].Oid) == int(types.T_datetime) {
					return nil, moerr.NewInvalidInput(ctx, name+" function have invalid input args type")
				}
			}
		}

	case "python_user_defined_function":
		size := (argsLength - 2) / 2
		args = args[:size+1]
		argsLength = len(args)
		argsType = argsType[:size+1]
		if len(argsCastType) > 0 {
			argsCastType = argsCastType[:size+1]
		}
	}

	if len(argsCastType) != 0 {
		if len(argsCastType) != argsLength {
			return nil, moerr.NewInvalidArg(ctx, "cast types length not match args length", "")
		}
		for idx, castType := range argsCastType {
			if !argsType[idx].Eq(castType) && castType.Oid != types.T_any {
				if argsType[idx].Oid == castType.Oid && castType.Oid.IsDecimal() && argsType[idx].Scale == castType.Scale {
					continue
				}
				typ := makePlan2Type(&castType)
				args[idx], err = appendCastBeforeExpr(ctx, args[idx], typ)
				if err != nil {
					return nil, err
				}
			}
		}
	}

	// return new expr
	Typ := makePlan2Type(&returnType)
	Typ.NotNullable = function.DeduceNotNullable(funcID, args)
	return &Expr{
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: getFunctionObjRef(funcID, name),
				Args: args,
			},
		},
		Typ: Typ,
	}, nil
}

func (b *baseBinder) bindNumVal(astExpr *tree.NumVal, typ Type) (*Expr, error) {
	// over_int64_err := moerr.NewInternalError(b.GetContext(), "", "Constants over int64 will support in future version.")
	// rewrite the hexnum process logic
	// for float64, if the number is over 1<<53-1,it will lost, so if typ is float64,
	// don't cast 0xXXXX as float64, use the uint64
	returnDecimalExpr := func(val string) (*Expr, error) {
		if !typ.IsEmpty() {
			return appendCastBeforeExpr(b.GetContext(), makePlan2StringConstExprWithType(val), typ)
		}
		return makePlan2DecimalExprWithType(b.GetContext(), val)
	}

	returnHexNumExpr := func(val string, isBin ...bool) (*Expr, error) {
		if !typ.IsEmpty() {
			isFloat := typ.Id == int32(types.T_float32) || typ.Id == int32(types.T_float64)
			return appendCastBeforeExpr(b.GetContext(), makePlan2StringConstExprWithType(val, isBin[0]), typ, isBin[0], isFloat)
		}
		return makePlan2StringConstExprWithType(val, isBin...), nil
	}

	switch astExpr.ValType {
	case tree.P_null:
		return makePlan2NullConstExprWithType(), nil
	case tree.P_bool:
		val := constant.BoolVal(astExpr.Value)
		return makePlan2BoolConstExprWithType(val), nil
	case tree.P_int64:
		val, ok := constant.Int64Val(astExpr.Value)
		if !ok {
			return nil, moerr.NewInvalidInput(b.GetContext(), "invalid int value '%s'", astExpr.Value.String())
		}
		expr := makePlan2Int64ConstExprWithType(val)
		if !typ.IsEmpty() && typ.Id == int32(types.T_varchar) {
			return appendCastBeforeExpr(b.GetContext(), expr, typ)
		}
		return expr, nil
	case tree.P_uint64:
		val, ok := constant.Uint64Val(astExpr.Value)
		if !ok {
			return nil, moerr.NewInvalidInput(b.GetContext(), "invalid int value '%s'", astExpr.Value.String())
		}
		return makePlan2Uint64ConstExprWithType(val), nil
	case tree.P_decimal:
		if !typ.IsEmpty() {
			if typ.Id == int32(types.T_decimal64) {
				d64, err := types.ParseDecimal64(astExpr.String(), typ.Width, typ.Scale)
				if err != nil {
					return nil, err
				}
				return &Expr{
					Expr: &plan.Expr_Lit{
						Lit: &Const{
							Isnull: false,
							Value: &plan.Literal_Decimal64Val{
								Decimal64Val: &plan.Decimal64{A: int64(d64)},
							},
						},
					},
					Typ: typ,
				}, nil
			}
			if typ.Id == int32(types.T_decimal128) {
				d128, err := types.ParseDecimal128(astExpr.String(), typ.Width, typ.Scale)
				if err != nil {
					return nil, err
				}
				a := int64(d128.B0_63)
				b := int64(d128.B64_127)
				return &Expr{
					Expr: &plan.Expr_Lit{
						Lit: &Const{
							Isnull: false,
							Value: &plan.Literal_Decimal128Val{
								Decimal128Val: &plan.Decimal128{A: a, B: b},
							},
						},
					},
					Typ: typ,
				}, nil
			}
			return appendCastBeforeExpr(b.GetContext(), makePlan2StringConstExprWithType(astExpr.String()), typ)
		}
		d128, scale, err := types.Parse128(astExpr.String())
		if err != nil {
			return nil, err
		}
		a := int64(d128.B0_63)
		b := int64(d128.B64_127)
		return &Expr{
			Expr: &plan.Expr_Lit{
				Lit: &Const{
					Isnull: false,
					Value: &plan.Literal_Decimal128Val{
						Decimal128Val: &plan.Decimal128{A: a, B: b},
					},
				},
			},
			Typ: plan.Type{
				Id:          int32(types.T_decimal128),
				Width:       38,
				Scale:       scale,
				NotNullable: true,
			},
		}, nil
	case tree.P_float64:
		originString := astExpr.String()
		if !typ.IsEmpty() && (typ.Id == int32(types.T_decimal64) || typ.Id == int32(types.T_decimal128)) {
			return returnDecimalExpr(originString)
		}
		if !strings.Contains(originString, "e") {
			expr, err := returnDecimalExpr(originString)
			if err == nil {
				return expr, nil
			}
		}
		floatValue, ok := constant.Float64Val(astExpr.Value)
		if !ok {
			return returnDecimalExpr(originString)
		}
		return makePlan2Float64ConstExprWithType(floatValue), nil
	case tree.P_hexnum:
		s := astExpr.String()[2:]
		if len(s)%2 != 0 {
			s = string('0') + s
		}
		bytes, _ := hex.DecodeString(s)
		return returnHexNumExpr(string(bytes), true)
	case tree.P_ScoreBinary:
		return returnHexNumExpr(astExpr.String(), true)
	case tree.P_bit:
		s := astExpr.String()[2:]
		bytes, _ := util.DecodeBinaryString(s)
		return returnHexNumExpr(string(bytes), true)
	case tree.P_char:
		expr := makePlan2StringConstExprWithType(astExpr.String())
		return expr, nil
	case tree.P_nulltext:
		expr := MakePlan2NullTextConstExprWithType(astExpr.String())
		return expr, nil
	default:
		return nil, moerr.NewInvalidInput(b.GetContext(), "unsupport value '%s'", astExpr.String())
	}
}

func (b *baseBinder) GetContext() context.Context { return b.sysCtx }

// --- util functions ----

func appendCastBeforeExpr(ctx context.Context, expr *Expr, toType Type, isBin ...bool) (*Expr, error) {
	toType.NotNullable = expr.Typ.NotNullable
	argsType := []types.Type{
		makeTypeByPlan2Expr(expr),
		makeTypeByPlan2Type(toType),
	}
	fGet, err := function.GetFunctionByName(ctx, "cast", argsType)
	if err != nil {
		return nil, err
	}
	// for 0xXXXX, if the value is over 1<<53-1, when covert it into float64,it will lost, so just change it into uint64
	typ := toType
	if len(isBin) == 2 && isBin[0] && isBin[1] {
		typ.Id = int32(types.T_uint64)
	}
	return &Expr{
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: getFunctionObjRef(fGet.GetEncodedOverloadID(), "cast"),
				Args: []*Expr{
					expr,
					{
						Typ: typ,
						Expr: &plan.Expr_T{
							T: &plan.TargetType{},
						},
					},
				},
			},
		},
		Typ: typ,
	}, nil
}

func resetDateFunctionArgs(ctx context.Context, dateExpr *Expr, intervalExpr *Expr) ([]*Expr, error) {
	firstExpr := intervalExpr.GetList().List[0]
	secondExpr := intervalExpr.GetList().List[1]

	intervalTypeStr := secondExpr.GetLit().GetSval()
	intervalType, err := types.IntervalTypeOf(intervalTypeStr)
	if err != nil {
		return nil, err
	}

	intervalTypeInFunction := &plan.Type{
		Id: int32(types.T_int64),
	}

	if firstExpr.Typ.Id == int32(types.T_varchar) || firstExpr.Typ.Id == int32(types.T_char) {
		s := firstExpr.GetLit().GetSval()
		returnNum, returnType, err := types.NormalizeInterval(s, intervalType)

		if err != nil {
			return nil, err
		}
		// "date '2020-10-10' - interval 1 Hour"  will return datetime
		// so we rewrite "date '2020-10-10' - interval 1 Hour"  to  "date_add(datetime, 1, hour)"
		if dateExpr.Typ.Id == int32(types.T_date) {
			switch returnType {
			case types.Day, types.Week, types.Month, types.Quarter, types.Year:
			default:
				dateExpr, err = appendCastBeforeExpr(ctx, dateExpr, plan.Type{
					Id: int32(types.T_datetime),
				})

				if err != nil {
					return nil, err
				}
			}
		}
		return []*Expr{
			dateExpr,
			makePlan2Int64ConstExprWithType(returnNum),
			makePlan2Int64ConstExprWithType(int64(returnType)),
		}, nil
	}

	// "date '2020-10-10' - interval 1 Hour"  will return datetime
	// so we rewrite "date '2020-10-10' - interval 1 Hour"  to  "date_add(datetime, 1, hour)"
	if dateExpr.Typ.Id == int32(types.T_date) {
		switch intervalType {
		case types.Day, types.Week, types.Month, types.Quarter, types.Year:
		default:
			dateExpr, err = appendCastBeforeExpr(ctx, dateExpr, plan.Type{
				Id: int32(types.T_datetime),
			})

			if err != nil {
				return nil, err
			}
		}
	}

	numberExpr, err := appendCastBeforeExpr(ctx, firstExpr, *intervalTypeInFunction)
	if err != nil {
		return nil, err
	}

	return []*Expr{
		dateExpr,
		numberExpr,
		makePlan2Int64ConstExprWithType(int64(intervalType)),
	}, nil
}

func resetDateFunction(ctx context.Context, dateExpr *Expr, intervalExpr *Expr) ([]*Expr, error) {
	switch intervalExpr.Expr.(type) {
	case *plan.Expr_List:
		return resetDateFunctionArgs(ctx, dateExpr, intervalExpr)
	}
	list := &plan.ExprList{
		List: make([]*Expr, 2),
	}
	list.List[0] = intervalExpr
	strType := &plan.Type{
		Id: int32(types.T_char),
	}
	strExpr := &Expr{
		Expr: &plan.Expr_Lit{
			Lit: &Const{
				Value: &plan.Literal_Sval{
					Sval: "day",
				},
			},
		},
		Typ: *strType,
	}
	list.List[1] = strExpr
	expr := &plan.Expr_List{
		List: list,
	}
	listExpr := &Expr{
		Expr: expr,
	}
	return resetDateFunctionArgs(ctx, dateExpr, listExpr)
}
