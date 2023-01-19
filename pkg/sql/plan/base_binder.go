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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/builtin/binary"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

func (b *baseBinder) baseBindExpr(astExpr tree.Expr, depth int32, isRoot bool) (expr *Expr, err error) {
	switch exprImpl := astExpr.(type) {
	case *tree.NumVal:
		if d, ok := b.impl.(*DefaultBinder); ok {
			expr, err = b.bindNumVal(exprImpl, d.typ)
		} else {
			expr, err = b.bindNumVal(exprImpl, nil)
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

			subquery := expr.Expr.(*plan.Expr_Sub)
			if subquery.Sub.Typ == plan.SubqueryRef_EXISTS {
				subquery.Sub.Typ = plan.SubqueryRef_NOT_EXISTS
			}
		} else {
			expr, err = b.impl.BindExpr(exprImpl.Expr, depth, false)
			if err != nil {
				return
			}

			expr, err = bindFuncExprImplByPlanExpr(b.GetContext(), "not", []*plan.Expr{expr})
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
		expr, err = b.impl.BindColRef(exprImpl, depth, isRoot)

	case *tree.CastExpr:
		expr, err = b.impl.BindExpr(exprImpl.Expr, depth, false)
		if err != nil {
			return
		}
		var typ *Type
		typ, err = getTypeFromAst(b.GetContext(), exprImpl.Type)
		if err != nil {
			return
		}
		expr, err = appendCastBeforeExpr(b.GetContext(), expr, typ)

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
			Typ: &plan.Type{
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
			Expr: &plan.Expr_C{
				C: &Const{
					Isnull: false,
					Value: &plan.Const_Defaultval{
						Defaultval: true,
					},
				},
			},
		}, nil
	case *tree.UpdateVal:
		return &Expr{
			Expr: &plan.Expr_C{
				C: &Const{
					Isnull: false,
					Value: &plan.Const_UpdateVal{
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
		expr, err = b.baseBindParam(exprImpl, depth, isRoot)

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
	return &Expr{
		Typ: &plan.Type{
			Id: int32(types.T_any),
		},
		Expr: &plan.Expr_P{
			P: &plan.ParamRef{
				Pos: int32(astExpr.Offset),
			},
		},
	}, nil
}

func (b *baseBinder) baseBindVar(astExpr *tree.VarExpr, depth int32, isRoot bool) (expr *plan.Expr, err error) {
	return &Expr{
		Typ: &plan.Type{
			Id: int32(types.T_any),
		},
		Expr: &plan.Expr_V{
			V: &plan.VarRef{
				Name:   astExpr.Name,
				System: astExpr.System,
				Global: astExpr.Global,
			},
		},
	}, nil
}

func (b *baseBinder) baseBindColRef(astExpr *tree.UnresolvedName, depth int32, isRoot bool) (expr *plan.Expr, err error) {
	if b.ctx == nil {
		return nil, moerr.NewInvalidInput(b.GetContext(), "ambigous column reference '%v'", astExpr.Parts[0])
	}

	col := astExpr.Parts[0]
	table := astExpr.Parts[1]
	name := tree.String(astExpr, dialect.MYSQL)

	relPos := NotFound
	colPos := NotFound
	var typ *plan.Type
	localErrCtx := errutil.ContextWithNoReport(b.GetContext(), true)

	if len(table) == 0 {
		if binding, ok := b.ctx.bindingByCol[col]; ok {
			if binding != nil {
				relPos = binding.tag
				colPos = binding.colIdByName[col]
				typ = binding.types[colPos]
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
				typ = binding.types[colPos]
				relPos = binding.tag
			} else {
				err = moerr.NewInvalidInput(localErrCtx, "column '%s' does not exist", name)
			}
		} else {
			err = moerr.NewInvalidInput(localErrCtx, "missing FROM-clause entry for table '%v'", table)
		}
	}

	if colPos != NotFound {
		b.boundCols = append(b.boundCols, table+"."+col)

		expr = &plan.Expr{
			Typ: typ,
		}

		if depth == 0 {
			expr.Expr = &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: relPos,
					ColPos: colPos,
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

	default:
		return nil, moerr.NewNYI(b.GetContext(), "unsupported select statement: %s", tree.String(astExpr, dialect.MYSQL))
	}

	rowSize := int32(len(subCtx.results))

	returnExpr := &plan.Expr{
		Typ: &plan.Type{
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
		returnExpr.Typ = &plan.Type{
			Id:          int32(types.T_bool),
			NotNullable: true,
			Size:        1,
		}
		returnExpr.Expr.(*plan.Expr_Sub).Sub.Typ = plan.SubqueryRef_EXISTS
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
		newLefExpr := tree.NewComparisonExpr(tree.LESS_THAN, astExpr.Left, astExpr.From)
		newRightExpr := tree.NewComparisonExpr(tree.GREAT_THAN, astExpr.Left, astExpr.To)
		return b.bindFuncExprImplByAstExpr("or", []tree.Expr{newLefExpr, newRightExpr}, depth)
	} else {
		// rewrite 'col between 1, 20 ' to ' col >= 1 and col <= 2'
		newLefExpr := tree.NewComparisonExpr(tree.GREAT_THAN_EQUAL, astExpr.Left, astExpr.From)
		newRightExpr := tree.NewComparisonExpr(tree.LESS_THAN_EQUAL, astExpr.Left, astExpr.To)
		return b.bindFuncExprImplByAstExpr("and", []tree.Expr{newLefExpr, newRightExpr}, depth)
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
		return nil, moerr.NewNYI(b.GetContext(), "'%v'", astExpr)
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

	case tree.LESS_THAN:
		op = "<"

	case tree.LESS_THAN_EQUAL:
		op = "<="

	case tree.GREAT_THAN:
		op = ">"

	case tree.GREAT_THAN_EQUAL:
		op = ">="

	case tree.NOT_EQUAL:
		op = "<>"

	case tree.LIKE:
		op = "like"

	case tree.NOT_LIKE:
		newExpr := tree.NewComparisonExpr(tree.LIKE, astExpr.Left, astExpr.Right)
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

			if subquery, ok := rightArg.Expr.(*plan.Expr_Sub); ok {
				if list, ok := leftArg.Expr.(*plan.Expr_List); ok {
					if len(list.List.List) != int(subquery.Sub.RowSize) {
						return nil, moerr.NewNYI(b.GetContext(), "subquery should return %d columns", len(list.List.List))
					}
				} else {
					if subquery.Sub.RowSize > 1 {
						return nil, moerr.NewInvalidInput(b.GetContext(), "subquery returns more than 1 column")
					}
				}

				subquery.Sub.Typ = plan.SubqueryRef_IN
				subquery.Sub.Child = leftArg

				rightArg.Typ = &plan.Type{
					Id:          int32(types.T_bool),
					NotNullable: leftArg.Typ.NotNullable && rightArg.Typ.NotNullable,
					Size:        1,
				}

				return rightArg, nil
			} else {
				return bindFuncExprImplByPlanExpr(b.GetContext(), "in", []*plan.Expr{leftArg, rightArg})
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

			if subquery, ok := rightArg.Expr.(*plan.Expr_Sub); ok {
				if list, ok := leftArg.Expr.(*plan.Expr_List); ok {
					if len(list.List.List) != int(subquery.Sub.RowSize) {
						return nil, moerr.NewInvalidInput(b.GetContext(), "subquery should return %d columns", len(list.List.List))
					}
				} else {
					if subquery.Sub.RowSize > 1 {
						return nil, moerr.NewInvalidInput(b.GetContext(), "subquery should return 1 column")
					}
				}

				subquery.Sub.Typ = plan.SubqueryRef_NOT_IN
				subquery.Sub.Child = leftArg

				rightArg.Typ = &plan.Type{
					Id:          int32(types.T_bool),
					NotNullable: leftArg.Typ.NotNullable && rightArg.Typ.NotNullable,
					Size:        1,
				}

				return rightArg, nil
			} else {
				expr, err := bindFuncExprImplByPlanExpr(b.GetContext(), "in", []*plan.Expr{leftArg, rightArg})
				if err != nil {
					return nil, err
				}

				return bindFuncExprImplByPlanExpr(b.GetContext(), "not", []*plan.Expr{expr})
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

		if subquery, ok := expr.Expr.(*plan.Expr_Sub); ok {
			if list, ok := child.Expr.(*plan.Expr_List); ok {
				if len(list.List.List) != int(subquery.Sub.RowSize) {
					return nil, moerr.NewInvalidInput(b.GetContext(), "subquery should return %d columns", len(list.List.List))
				}
			} else {
				if subquery.Sub.RowSize > 1 {
					return nil, moerr.NewInvalidInput(b.GetContext(), "subquery should return 1 column")
				}
			}

			subquery.Sub.Op = op
			subquery.Sub.Child = child

			switch astExpr.SubOp {
			case tree.ANY, tree.SOME:
				subquery.Sub.Typ = plan.SubqueryRef_ANY
			case tree.ALL:
				subquery.Sub.Typ = plan.SubqueryRef_ALL
			}

			expr.Typ = &plan.Type{
				Id:          int32(types.T_bool),
				NotNullable: expr.Typ.NotNullable && child.Typ.NotNullable,
				Size:        1,
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

	if function.GetFunctionIsAggregateByName(funcName) {
		return b.impl.BindAggFunc(funcName, astExpr, depth, isRoot)
	} else if function.GetFunctionIsWinfunByName(funcName) {
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
		elseExpr := tree.NewNumValWithType(constant.MakeUnknown(), "", false, tree.P_null)
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

					astArgs[0] = tree.NewNumValWithType(constant.MakeInt64(1), "1", false, tree.P_int64)
				}
			}
		}
	}
	// bind ast function's args
	args := make([]*Expr, len(astArgs))
	for idx, arg := range astArgs {
		expr, err := b.impl.BindExpr(arg, depth, false)
		if err != nil {
			return nil, err
		}
		args[idx] = expr
	}

	return bindFuncExprImplByPlanExpr(b.GetContext(), name, args)
}

func bindFuncExprImplByPlanExpr(ctx context.Context, name string, args []*Expr) (*plan.Expr, error) {
	var err error

	// deal with some special function
	switch name {
	case "date":
		// rewrite date function to cast function, and retrun directly
		if len(args) == 0 {
			return nil, moerr.NewInvalidArg(ctx, name+" function have invalid input args length", len(args))
		}
		if args[0].Typ.Id != int32(types.T_varchar) && args[0].Typ.Id != int32(types.T_char) {
			return appendCastBeforeExpr(ctx, args[0], &Type{
				Id: int32(types.T_date),
			})
		}
	case "interval":
		// rewrite interval function to ListExpr, and retrun directly
		return &plan.Expr{
			Typ: &plan.Type{
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
			args[0], err = appendCastBeforeExpr(ctx, args[0], &plan.Type{
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
			args[0], err = appendCastBeforeExpr(ctx, args[0], &plan.Type{
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
			if exprC, ok := args[1].Expr.(*plan.Expr_C); ok {
				sval := exprC.C.Value.(*plan.Const_Sval)
				tp, _ := binary.JudgmentToDateReturnType(sval.Sval)
				args = append(args, makePlan2DateConstNullExpr(tp))
			} else {
				return nil, moerr.NewInvalidArg(ctx, "to_date format", "not constant")
			}
		} else if args[1].Typ.Id == int32(types.T_any) {
			args = append(args, makePlan2DateConstNullExpr(types.T_datetime))
		} else {
			return nil, moerr.NewInvalidArg(ctx, name+" function have invalid input args length", len(args))
		}
	case "unix_timestamp":
		if len(args) == 1 {
			if types.IsString(types.T(args[0].Typ.Id)) {
				if exprC, ok := args[0].Expr.(*plan.Expr_C); ok {
					sval := exprC.C.Value.(*plan.Const_Sval)
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
		case types.IsString(tp), types.IsInteger(tp):
		default:
			targetTp := types.T_varchar.ToType()
			args[0], err = appendCastBeforeExpr(ctx, args[0], makePlan2Type(&targetTp), false)
			if err != nil {
				return nil, err
			}
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
	funcID, returnType, argsCastType, err = function.GetFunctionByName(ctx, name, argsType)
	if err != nil {
		return nil, err
	}
	if function.GetFunctionIsAggregateByName(name) {
		if constExpr, ok := args[0].Expr.(*plan.Expr_C); ok && constExpr.C.Isnull {
			args[0].Typ = makePlan2Type(&returnType)
		}
	}

	// rewrite some cast rule:  expr:  int32Col > 10,
	// old rule: cast(int32Col as int64) >10 ,   new rule: int32Col > (cast 10 as int32)
	switch name {
	case "=", "<", "<=", ">", ">=", "<>", "like":
		// if constant's type higher than column's type
		// and constant's value in range of column's type, then no cast was needed
		switch leftExpr := args[0].Expr.(type) {
		case *plan.Expr_C:
			if _, ok := args[1].Expr.(*plan.Expr_Col); ok {
				if checkNoNeedCast(argsType[0], argsType[1], leftExpr) {
					tmpType := argsType[1] // cast const_expr as column_expr's type
					argsCastType = []types.Type{tmpType, tmpType}
					// need to update function id
					funcID, _, _, err = function.GetFunctionByName(ctx, name, argsCastType)
					if err != nil {
						return nil, err
					}
				}
			}
		case *plan.Expr_Col:
			if rightExpr, ok := args[1].Expr.(*plan.Expr_C); ok {
				if checkNoNeedCast(argsType[1], argsType[0], rightExpr) {
					tmpType := argsType[0] // cast const_expr as column_expr's type
					argsCastType = []types.Type{tmpType, tmpType}
					funcID, _, _, err = function.GetFunctionByName(ctx, name, argsCastType)
					if err != nil {
						return nil, err
					}
				}
			}
		}

	case "in", "not_in":
		//if all the expr in the in list can safely cast to left type, we call it safe
		var safe bool
		if rightList, ok := args[1].Expr.(*plan.Expr_List); ok {
			typLeft := makeTypeByPlan2Expr(args[0])
			lenList := len(rightList.List.List)

			for i := 0; i < lenList; i++ {
				if constExpr, ok := rightList.List.List[i].Expr.(*plan.Expr_C); ok {
					safe = checkNoNeedCast(makeTypeByPlan2Expr(rightList.List.List[i]), typLeft, constExpr)
					if !safe {
						break
					}
				} else {
					safe = false
					break
				}
			}

			if safe {
				//if safe, try to cast the in list to left type
				for i := 0; i < lenList; i++ {
					rightList.List.List[i], err = appendCastBeforeExpr(ctx, rightList.List.List[i], args[0].Typ)
					if err != nil {
						return nil, err
					}
				}
			} else {
				//expand the in list to col=a or col=b or ......
				if name == "in" {
					newExpr, _ := bindFuncExprImplByPlanExpr(ctx, "=", []*Expr{DeepCopyExpr(args[0]), DeepCopyExpr(rightList.List.List[0])})
					for i := 1; i < lenList; i++ {
						tmpExpr, _ := bindFuncExprImplByPlanExpr(ctx, "=", []*Expr{DeepCopyExpr(args[0]), DeepCopyExpr(rightList.List.List[i])})
						newExpr, _ = bindFuncExprImplByPlanExpr(ctx, "or", []*Expr{newExpr, tmpExpr})
					}
					return newExpr, nil
				} else {
					//expand the not in list to col!=a and col!=b and ......
					newExpr, _ := bindFuncExprImplByPlanExpr(ctx, "!=", []*Expr{DeepCopyExpr(args[0]), DeepCopyExpr(rightList.List.List[0])})
					for i := 1; i < lenList; i++ {
						tmpExpr, _ := bindFuncExprImplByPlanExpr(ctx, "!=", []*Expr{DeepCopyExpr(args[0]), DeepCopyExpr(rightList.List.List[i])})
						newExpr, _ = bindFuncExprImplByPlanExpr(ctx, "and", []*Expr{newExpr, tmpExpr})
					}
					return newExpr, nil
				}
			}
		}

	case "timediff":
		if len(argsType) == len(argsCastType) {
			for i := range argsType {
				if int(argsType[i].Oid) == int(types.T_time) && int(argsCastType[i].Oid) == int(types.T_datetime) {
					return nil, moerr.NewInvalidInput(ctx, name+" function have invalid input args type")
				}
			}
		}
	}

	if len(argsCastType) != 0 {
		if len(argsCastType) != argsLength {
			return nil, moerr.NewInvalidArg(ctx, "cast types length not match args length", "")
		}
		for idx, castType := range argsCastType {
			if !argsType[idx].Eq(castType) && castType.Oid != types.T_any {
				typ := makePlan2Type(&castType)
				args[idx], err = appendCastBeforeExpr(ctx, args[idx], typ)
				if err != nil {
					return nil, err
				}
			}
		}
	}

	if function.GetFunctionAppendHideArgByID(funcID) {
		// Append a hidden parameter to the function. The default value is constant null
		args = append(args, makePlan2NullConstExprWithType())
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

func (b *baseBinder) bindNumVal(astExpr *tree.NumVal, typ *Type) (*Expr, error) {
	// over_int64_err := moerr.NewInternalError(b.GetContext(), "", "Constants over int64 will support in future version.")
	// rewrite the hexnum process logic
	// for float64, if the number is over 1<<53-1,it will lost, so if typ is float64,
	// don't cast 0xXXXX as float64, use the uint64
	returnDecimalExpr := func(val string) (*Expr, error) {
		if typ != nil {
			return appendCastBeforeExpr(b.GetContext(), makePlan2StringConstExprWithType(val), typ)
		}
		return makePlan2DecimalExprWithType(b.GetContext(), val)
	}

	returnHexNumExpr := func(val string, isBin ...bool) (*Expr, error) {
		if typ != nil {
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
		if typ != nil && typ.Id == int32(types.T_varchar) {
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
		if typ != nil {
			if typ.Id == int32(types.T_decimal64) {
				d64, err := types.Decimal64_FromStringWithScale(astExpr.String(), typ.Width, typ.Scale)
				if err != nil {
					return nil, err
				}
				return &Expr{
					Expr: &plan.Expr_C{
						C: &Const{
							Isnull: false,
							Value: &plan.Const_Decimal64Val{
								Decimal64Val: &plan.Decimal64{A: types.Decimal64ToInt64Raw(d64)},
							},
						},
					},
					Typ: typ,
				}, nil
			}
			if typ.Id == int32(types.T_decimal128) {
				d128, err := types.Decimal128_FromStringWithScale(astExpr.String(), typ.Width, typ.Scale)
				if err != nil {
					return nil, err
				}
				a, b := types.Decimal128ToInt64Raw(d128)
				return &Expr{
					Expr: &plan.Expr_C{
						C: &Const{
							Isnull: false,
							Value: &plan.Const_Decimal128Val{
								Decimal128Val: &plan.Decimal128{A: a, B: b},
							},
						},
					},
					Typ: typ,
				}, nil
			}
			return appendCastBeforeExpr(b.GetContext(), makePlan2StringConstExprWithType(astExpr.String()), typ)
		}
		d128, scale, err := types.ParseStringToDecimal128WithoutTable(astExpr.String())
		if err != nil {
			return nil, err
		}
		a, b := types.Decimal128ToInt64Raw(d128)
		return &Expr{
			Expr: &plan.Expr_C{
				C: &Const{
					Isnull: false,
					Value: &plan.Const_Decimal128Val{
						Decimal128Val: &plan.Decimal128{A: a, B: b},
					},
				},
			},
			Typ: &plan.Type{
				Id:          int32(types.T_decimal128),
				Width:       34,
				Scale:       scale,
				Precision:   34,
				NotNullable: true,
			},
		}, nil
	case tree.P_float64:
		originString := astExpr.String()
		if typ != nil && (typ.Id == int32(types.T_decimal64) || typ.Id == int32(types.T_decimal128)) {
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
		return returnDecimalExpr(astExpr.String())
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

func appendCastBeforeExpr(ctx context.Context, expr *Expr, toType *Type, isBin ...bool) (*Expr, error) {
	if expr.Typ.Id == int32(types.T_any) {
		return expr, nil
	}
	toType.NotNullable = expr.Typ.NotNullable
	argsType := []types.Type{
		makeTypeByPlan2Expr(expr),
		makeTypeByPlan2Type(toType),
	}
	funcID, _, _, err := function.GetFunctionByName(ctx, "cast", argsType)
	if err != nil {
		return nil, err
	}
	// for 0xXXXX, if the value is over 1<<53-1, when covert it into float64,it will lost, so just change it into uint64
	typ := *toType
	if len(isBin) == 2 && isBin[0] && isBin[1] {
		typ.Id = int32(types.T_uint64)
	}
	return &Expr{
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: getFunctionObjRef(funcID, "cast"),
				Args: []*Expr{expr,
					{
						Typ: &typ,
						Expr: &plan.Expr_T{
							T: &plan.TargetType{
								Typ: &typ,
							},
						},
					}},
			},
		},
		Typ: &typ,
	}, nil
}

func resetDateFunctionArgs(ctx context.Context, dateExpr *Expr, intervalExpr *Expr) ([]*Expr, error) {
	firstExpr := intervalExpr.Expr.(*plan.Expr_List).List.List[0]
	secondExpr := intervalExpr.Expr.(*plan.Expr_List).List.List[1]

	intervalTypeStr := secondExpr.Expr.(*plan.Expr_C).C.Value.(*plan.Const_Sval).Sval
	intervalType, err := types.IntervalTypeOf(intervalTypeStr)
	if err != nil {
		return nil, err
	}

	intervalTypeInFunction := &plan.Type{
		Id:   int32(types.T_int64),
		Size: 8,
	}

	if firstExpr.Typ.Id == int32(types.T_varchar) || firstExpr.Typ.Id == int32(types.T_char) {
		s := firstExpr.Expr.(*plan.Expr_C).C.Value.(*plan.Const_Sval).Sval
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
				dateExpr, err = appendCastBeforeExpr(ctx, dateExpr, &plan.Type{
					Id:   int32(types.T_datetime),
					Size: 8,
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
			dateExpr, err = appendCastBeforeExpr(ctx, dateExpr, &plan.Type{
				Id:   int32(types.T_datetime),
				Size: 8,
			})

			if err != nil {
				return nil, err
			}
		}
	}

	numberExpr, err := appendCastBeforeExpr(ctx, firstExpr, intervalTypeInFunction)
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
		Id:   int32(types.T_char),
		Size: 4,
	}
	strExpr := &Expr{
		Expr: &plan.Expr_C{
			C: &Const{
				Value: &plan.Const_Sval{
					Sval: "day",
				},
			},
		},
		Typ: strType,
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
