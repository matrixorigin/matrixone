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

package plan2

import (
	"fmt"
	"go/constant"
	"math"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan2/function"
)

func splitAndBindCondition(astExpr tree.Expr, ctx *BindContext) ([]*plan.Expr, error) {
	conds := splitConjunctiveCondition(astExpr)
	exprs := make([]*plan.Expr, len(conds))

	for i, cond := range conds {
		expr, err := ctx.binder.BindExpr(cond, 0, true)
		if err != nil {
			return nil, err
		}
		exprs[i] = expr
	}

	return exprs, nil
}

func (b *baseBinder) baseBindExpr(astExpr tree.Expr, depth int32, isRoot bool) (expr *Expr, err error) {
	switch exprImpl := astExpr.(type) {
	case *tree.NumVal:
		expr, err = b.bindNumVal(exprImpl)

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

			expr, err = bindFuncExprImplByPlanExpr("not", []*plan.Expr{expr})
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
		typ, err = getTypeFromAst(exprImpl.Type)
		if err != nil {
			return
		}
		expr, err = appendCastBeforeExpr(expr, typ)

	case *tree.IsNullExpr:
		expr, err = b.bindFuncExprImplByAstExpr("isnull", []tree.Expr{exprImpl.Expr}, depth)

	case *tree.IsNotNullExpr:
		expr, err = b.bindFuncExprImplByAstExpr("not", []tree.Expr{tree.NewIsNullExpr(exprImpl.Expr)}, depth)

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
				Id: plan.Type_TUPLE,
			},
		}

	case *tree.CaseExpr:
		expr, err = b.bindCaseExpr(exprImpl, depth, isRoot)

	case *tree.IntervalExpr:
		err = errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("expr interval'%v' is not support now", exprImpl))

	case *tree.XorExpr:
		expr, err = b.bindFuncExprImplByAstExpr("xor", []tree.Expr{exprImpl.Left, exprImpl.Right}, depth)

	case *tree.Subquery:
		if !isRoot && exprImpl.Exists {
			// TODO: implement MARK join to better support non-scalar subqueries
			return nil, errors.New(errno.InternalError, "EXISTS subquery as non-root expression not yet supported")
		}

		expr, err = b.impl.BindSubquery(exprImpl, isRoot)

	case *tree.DefaultVal:
		err = errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("expr default'%v' is not support now", exprImpl))

	case *tree.MaxValue:
		err = errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("expr max'%v' is not support now", exprImpl))

	case *tree.VarExpr:
		expr, err = b.baseBindVar(exprImpl, depth, isRoot)

	case *tree.StrVal:
		err = errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("expr str'%v' is not support now", exprImpl))

	case *tree.ExprList:
		err = errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("expr plan.ExprList'%v' is not support now", exprImpl))

	case tree.UnqualifiedStar:
		// select * from table
		// * should only appear in SELECT clause
		err = errors.New(errno.SyntaxErrororAccessRuleViolation, "unqualified star should only appear in SELECT clause")

	default:
		err = errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("expr '%+v' is not support now", exprImpl))
	}

	return
}

func (b *baseBinder) baseBindVar(astExpr *tree.VarExpr, depth int32, isRoot bool) (expr *plan.Expr, err error) {
	var getVal interface{}
	getVal, err = b.builder.compCtx.ResolveVariable(astExpr.Name, astExpr.System, astExpr.Global)
	if err != nil {
		return nil, err
	}
	getIntExpr := func(data int64) *plan.Expr {
		return &Expr{
			Expr: &plan.Expr_C{
				C: &Const{
					Isnull: false,
					Value: &plan.Const_Ival{
						Ival: data,
					},
				},
			},
			Typ: &plan.Type{
				Id:       plan.Type_INT64,
				Nullable: false,
				Size:     8,
			},
		}
	}
	getFloatExpr := func(data float64) *plan.Expr {
		return &Expr{
			Expr: &plan.Expr_C{
				C: &Const{
					Isnull: false,
					Value: &plan.Const_Dval{
						Dval: data,
					},
				},
			},
			Typ: &plan.Type{
				Id:       plan.Type_FLOAT64,
				Nullable: false,
				Size:     8,
			},
		}
	}

	switch val := getVal.(type) {
	case string:
		expr = &Expr{
			Expr: &plan.Expr_C{
				C: &Const{
					Isnull: false,
					Value: &plan.Const_Sval{
						Sval: val,
					},
				},
			},
			Typ: &plan.Type{
				Id:       plan.Type_VARCHAR,
				Nullable: false,
				Size:     4,
				Width:    math.MaxInt32,
			},
		}
	case int:
		expr = getIntExpr(int64(val))
	case uint8:
		expr = getIntExpr(int64(val))
	case uint16:
		expr = getIntExpr(int64(val))
	case uint32:
		expr = getIntExpr(int64(val))
	case int8:
		expr = getIntExpr(int64(val))
	case int16:
		expr = getIntExpr(int64(val))
	case int32:
		expr = getIntExpr(int64(val))
	case int64:
		expr = getIntExpr(val)
	case uint64:
		err = errors.New(errno.SyntaxErrororAccessRuleViolation, "decimal var not support now")
	case float32:
		expr = getFloatExpr(float64(val))
	case float64:
		expr = getFloatExpr(val)
	case bool:
		return &Expr{
			Expr: &plan.Expr_C{
				C: &Const{
					Isnull: false,
					Value: &plan.Const_Bval{
						Bval: val,
					},
				},
			},
			Typ: &plan.Type{
				Id:       plan.Type_BOOL,
				Nullable: false,
				Size:     1,
			},
		}, nil
	case nil:
		expr = &Expr{
			Expr: &plan.Expr_C{
				C: &Const{
					Isnull: true,
				},
			},
			Typ: &plan.Type{
				Id:       plan.Type_ANY,
				Nullable: true,
			},
		}
	case types.Decimal64, types.Decimal128:
		err = errors.New(errno.SyntaxErrororAccessRuleViolation, "decimal var not support now")
	default:
		err = errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("type of var %q is not support now", astExpr.Name))
	}
	return
}

func (b *baseBinder) baseBindColRef(astExpr *tree.UnresolvedName, depth int32, isRoot bool) (expr *plan.Expr, err error) {
	col := astExpr.Parts[0]
	table := astExpr.Parts[1]
	name := tree.String(astExpr, dialect.MYSQL)

	relPos := NotFound
	colPos := NotFound
	var typ *plan.Type

	if len(table) == 0 {
		if binding, ok := b.ctx.bindingByCol[col]; ok {
			if binding != nil {
				relPos = binding.tag
				colPos = binding.colIdByName[col]
				typ = binding.types[colPos]
			} else {
				return nil, errors.New(errno.AmbiguousColumn, fmt.Sprintf("column reference %q is ambiguous", name))
			}
		} else {
			err = errors.New(errno.InvalidColumnReference, fmt.Sprintf("column %q does not exist", name))
		}
	} else {
		if binding, ok := b.ctx.bindingByTable[table]; ok {
			colPos = binding.FindColumn(col)
			if colPos == AmbiguousName {
				return nil, errors.New(errno.AmbiguousColumn, fmt.Sprintf("column reference %q is ambiguous", name))
			}
			if colPos != NotFound {
				typ = binding.types[colPos]
				relPos = binding.tag
			} else {
				err = errors.New(errno.InvalidColumnReference, fmt.Sprintf("column %q does not exist", name))
			}
		} else {
			err = errors.New(errno.UndefinedTable, fmt.Sprintf("missing FROM-clause entry for table %q", table))
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

		return
	}

	parent := b.ctx.parent
	for parent != nil && parent.binder == nil {
		parent = parent.parent
	}

	if parent == nil {
		return
	}

	return parent.binder.BindColRef(astExpr, depth+1, isRoot)
}

func (b *baseBinder) baseBindSubquery(astExpr *tree.Subquery, isRoot bool) (*Expr, error) {
	subCtx := NewBindContext(b.builder, b.ctx)

	var nodeId int32
	var err error
	switch subquery := astExpr.Select.(type) {
	case *tree.ParenSelect:
		nodeId, err = b.builder.buildSelect(subquery.Select, subCtx, false)
		if err != nil {
			return nil, err
		}

	default:
		return nil, errors.New(errno.SyntaxError, fmt.Sprintf("unsupported select statement: %s", tree.String(astExpr, dialect.MYSQL)))
	}

	returnExpr := &plan.Expr{
		Typ: &plan.Type{
			Id: plan.Type_TUPLE,
		},
		Expr: &plan.Expr_Sub{
			Sub: &plan.SubqueryRef{
				NodeId: nodeId,
			},
		},
	}
	if astExpr.Exists {
		returnExpr.Typ = &plan.Type{
			Id:       plan.Type_BOOL,
			Nullable: false,
			Size:     1,
		}
		returnExpr.Expr.(*plan.Expr_Sub).Sub.Typ = plan.SubqueryRef_EXISTS
	} else {
		if len(subCtx.projects) > 1 {
			// TODO: may support row constructor in the future?
			return nil, errors.New(errno.SyntaxError, "subquery must return only one column")
		}
		returnExpr.Typ = subCtx.projects[0].Typ
	}

	return returnExpr, nil
}

func (b *baseBinder) bindCaseExpr(astExpr *tree.CaseExpr, depth int32, isRoot bool) (*Expr, error) {
	var args []tree.Expr
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
		args = append(args, tree.NewNumVal(constant.MakeUnknown(), "", false))
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
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", astExpr))
	case tree.UNARY_MARK:
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", astExpr))
	}
	return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", astExpr))
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
	}
	return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", astExpr))
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
		new_expr := tree.NewComparisonExpr(tree.LIKE, astExpr.Left, astExpr.Right)
		return b.bindFuncExprImplByAstExpr("not", []tree.Expr{new_expr}, depth)

	case tree.IN:
		switch list := astExpr.Right.(type) {
		case *tree.Tuple:
			var new_expr tree.Expr
			for _, expr := range list.Exprs {
				if new_expr == nil {
					new_expr = tree.NewComparisonExpr(tree.EQUAL, astExpr.Left, expr)
				} else {
					equal_expr := tree.NewComparisonExpr(tree.EQUAL, astExpr.Left, expr)
					new_expr = tree.NewOrExpr(new_expr, equal_expr)
				}
			}
			return b.impl.BindExpr(new_expr, depth, false)

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
				if !isRoot {
					// TODO: implement MARK join to better support non-scalar subqueries
					return nil, errors.New(errno.InternalError, "IN subquery as non-root expression not yet supported")
				}

				if _, ok := leftArg.Expr.(*plan.Expr_List); ok {
					return nil, errors.New(errno.InternalError, "row constructor in IN subquery not yet supported")
				}

				subquery.Sub.Typ = plan.SubqueryRef_IN
				subquery.Sub.Child = leftArg
				return rightArg, nil
			} else {
				return bindFuncExprImplByPlanExpr("in", []*plan.Expr{leftArg, rightArg})
			}
		}

	case tree.NOT_IN:
		switch list := astExpr.Right.(type) {
		case *tree.Tuple:
			var new_expr tree.Expr
			for _, expr := range list.Exprs {
				if new_expr == nil {
					new_expr = tree.NewComparisonExpr(tree.NOT_EQUAL, astExpr.Left, expr)
				} else {
					equal_expr := tree.NewComparisonExpr(tree.NOT_EQUAL, astExpr.Left, expr)
					new_expr = tree.NewAndExpr(new_expr, equal_expr)
				}
			}
			return b.impl.BindExpr(new_expr, depth, false)

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
				if !isRoot {
					// TODO: implement MARK join to better support non-scalar subqueries
					return nil, errors.New(errno.InternalError, "IN subquery as non-root expression not yet supported")
				}

				if _, ok := leftArg.Expr.(*plan.Expr_List); ok {
					return nil, errors.New(errno.InternalError, "row constructor in IN subquery not yet supported")
				}

				subquery.Sub.Typ = plan.SubqueryRef_NOT_IN
				subquery.Sub.Child = leftArg
				return rightArg, nil
			} else {
				expr, err := bindFuncExprImplByPlanExpr("in", []*plan.Expr{leftArg, rightArg})
				if err != nil {
					return nil, err
				}

				return bindFuncExprImplByPlanExpr("not", []*plan.Expr{expr})
			}
		}

	default:
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", astExpr))
	}

	if astExpr.SubOp >= tree.ANY {
		if (astExpr.SubOp == tree.ANY || astExpr.SubOp == tree.SOME) && op != "=" {
			return nil, errors.New(errno.InternalError, "non-equi ANY subquery not yet supported")
		}

		if astExpr.SubOp == tree.ALL && op != "<>" {
			return nil, errors.New(errno.InternalError, "non-equi ANY subquery not yet supported")
		}

		expr, err := b.impl.BindExpr(astExpr.Right, depth, false)
		if err != nil {
			return nil, err
		}

		child, err := b.impl.BindExpr(astExpr.Left, depth, false)
		if err != nil {
			return nil, err
		}

		if _, ok := child.Expr.(*plan.Expr_List); ok {
			return nil, errors.New(errno.InternalError, "row constructor in ANY subquery not yet supported")
		}

		if subquery, ok := expr.Expr.(*plan.Expr_Sub); ok {
			if !isRoot {
				// TODO: implement MARK join to better support non-scalar subqueries
				return nil, errors.New(errno.InternalError, fmt.Sprintf("%q subquery as non-root expression not yet supported", strings.ToUpper(astExpr.SubOp.ToString())))
			}

			subquery.Sub.Op = op
			subquery.Sub.Child = child

			switch astExpr.SubOp {
			case tree.ANY, tree.SOME:
				subquery.Sub.Typ = plan.SubqueryRef_ANY
			case tree.ALL:
				subquery.Sub.Typ = plan.SubqueryRef_ALL
			}

			return expr, nil
		} else {
			return nil, errors.New(errno.InternalError, fmt.Sprintf("%q can only quantify subquery", astExpr.SubOp.ToString()))
		}
	}

	return b.bindFuncExprImplByAstExpr(op, []tree.Expr{astExpr.Left, astExpr.Right}, depth)
}

func (b *baseBinder) bindFuncExpr(astExpr *tree.FuncExpr, depth int32, isRoot bool) (*Expr, error) {
	funcRef, ok := astExpr.Func.FunctionReference.(*tree.UnresolvedName)
	if !ok {
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("function expr '%v' is not support now", astExpr))
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
	case "extract":
		// ”extract(year from col_name)"  parser return year as UnresolvedName.
		// we must rewrite it to string。 because binder bind UnresolvedName as column name
		unit := astArgs[0].(*tree.UnresolvedName).Parts[0]
		astArgs[0] = tree.NewNumVal(constant.MakeString(unit), unit, false)
	case "count":
		// we will rewrite "count(*)" to "starcount(col)"
		// count(*) : astExprs[0].(type) is *tree.NumVal
		// count(col_name) : astExprs[0].(type) is *tree.UnresolvedName
		switch nval := astArgs[0].(type) {
		case *tree.NumVal:
			// rewrite count(*) to starcount(col_name)
			if nval.String() == "*" {
				name = "starcount"
				if len(b.ctx.bindings) == 0 || len(b.ctx.bindings[0].cols) == 0 {
					return nil, errors.New(errno.InvalidColumnReference, "can not find any column when rewrite count(*) to starcount(col)")
				}
				var newCountCol *tree.UnresolvedName
				newCountCol, err := tree.NewUnresolvedName(b.ctx.bindings[0].cols[0])
				if err != nil {
					return nil, err
				}
				astArgs[0] = newCountCol
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

	return bindFuncExprImplByPlanExpr(name, args)
}

func bindFuncExprImplByPlanExpr(name string, args []*Expr) (*plan.Expr, error) {
	var err error

	// deal with some special function
	switch name {
	case "date":
		// rewrite date function to cast function, and retrun directly
		return appendCastBeforeExpr(args[0], &Type{
			Id: plan.Type_DATE,
		})
	case "interval":
		// rewrite interval function to cast function, and retrun directly
		return appendCastBeforeExpr(args[0], &plan.Type{
			Id: plan.Type_INTERVAL,
		})
	case "and", "or", "not", "xor":
		// why not append cast function?
		// for i := 0; i < len(args); i++ {
		// 	if args[i].Typ.Id != plan.Type_BOOL {
		// 		arg, err := appendCastBeforeExpr(args[i], &plan.Type{
		// 			Id: plan.Type_BOOL,
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
			return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, "date_add/date_sub function need two args")
		}
		args, err = resetDateFunctionArgs(args[0], args[1])
		if err != nil {
			return nil, err
		}
	case "+":
		// rewrite "date '2001' + interval '1 day'" to date_add(date '2001', 1, day(unit))
		if len(args) != 2 {
			return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, "operator function need two args")
		}
		if args[0].Typ.Id == plan.Type_DATE && args[1].Typ.Id == plan.Type_INTERVAL {
			name = "date_add"
			args, err = resetDateFunctionArgs(args[0], args[1])
		} else if args[0].Typ.Id == plan.Type_INTERVAL && args[1].Typ.Id == plan.Type_DATE {
			name = "date_add"
			args, err = resetDateFunctionArgs(args[1], args[0])
		} else if args[0].Typ.Id == plan.Type_DATETIME && args[1].Typ.Id == plan.Type_INTERVAL {
			name = "date_add"
			args, err = resetDateFunctionArgs(args[0], args[1])
		} else if args[0].Typ.Id == plan.Type_INTERVAL && args[1].Typ.Id == plan.Type_DATETIME {
			name = "date_add"
			args, err = resetDateFunctionArgs(args[1], args[0])
		} else if args[0].Typ.Id == plan.Type_VARCHAR && args[1].Typ.Id == plan.Type_INTERVAL {
			name = "date_add"
			args, err = resetDateFunctionArgs(args[0], args[1])
		} else if args[0].Typ.Id == plan.Type_INTERVAL && args[1].Typ.Id == plan.Type_VARCHAR {
			name = "date_add"
			args, err = resetDateFunctionArgs(args[1], args[0])
		}
		if err != nil {
			return nil, err
		}
	case "-":
		// rewrite "date '2001' - interval '1 day'" to date_sub(date '2001', 1, day(unit))
		if args[0].Typ.Id == plan.Type_DATE && args[1].Typ.Id == plan.Type_INTERVAL {
			name = "date_sub"
			args, err = resetDateFunctionArgs(args[0], args[1])
		} else if args[0].Typ.Id == plan.Type_DATETIME && args[1].Typ.Id == plan.Type_INTERVAL {
			name = "date_sub"
			args, err = resetDateFunctionArgs(args[0], args[1])
		} else if args[0].Typ.Id == plan.Type_VARCHAR && args[1].Typ.Id == plan.Type_INTERVAL {
			name = "date_sub"
			args, err = resetDateFunctionArgs(args[0], args[1])
		}
		if err != nil {
			return nil, err
		}
	}

	// get args(exprs) & types
	argsLength := len(args)
	argsType := make([]types.T, argsLength)
	for idx, expr := range args {
		argsType[idx] = types.T(expr.Typ.Id)
	}

	// get function definition
	funcDef, funcId, argsCastType, err := function.GetFunctionByName(name, argsType)
	if err != nil {
		return nil, err
	}
	if argsCastType != nil {
		if len(argsCastType) != argsLength {
			return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, "cast types length not match args length")
		}
		for idx, castType := range argsCastType {
			if argsType[idx] != castType {
				args[idx], err = appendCastBeforeExpr(args[idx], &plan.Type{
					Id: plan.Type_TypeId(castType),
				})
				if err != nil {
					return nil, err
				}
			}
		}
	}

	// return new expr
	return &Expr{
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: getFunctionObjRef(funcId, name),
				Args: args,
			},
		},
		Typ: &Type{
			Id: plan.Type_TypeId(funcDef.ReturnTyp),
		},
	}, nil
}

func (b *baseBinder) bindNumVal(astExpr *tree.NumVal) (*Expr, error) {
	switch astExpr.Value.Kind() {
	case constant.Unknown:
		return &Expr{
			Expr: &plan.Expr_C{
				C: &Const{
					Isnull: true,
				},
			},
			Typ: &plan.Type{
				Id:       plan.Type_ANY,
				Nullable: true,
			},
		}, nil
	case constant.Bool:
		boolValue := constant.BoolVal(astExpr.Value)
		return &Expr{
			Expr: &plan.Expr_C{
				C: &Const{
					Isnull: false,
					Value: &plan.Const_Bval{
						Bval: boolValue,
					},
				},
			},
			Typ: &plan.Type{
				Id:       plan.Type_BOOL,
				Nullable: false,
				Size:     1,
			},
		}, nil
	case constant.Int:
		intValue, _ := constant.Int64Val(astExpr.Value)
		if astExpr.Negative() {
			intValue = -intValue
		}
		return &Expr{
			Expr: &plan.Expr_C{
				C: &Const{
					Isnull: false,
					Value: &plan.Const_Ival{
						Ival: intValue,
					},
				},
			},
			Typ: &plan.Type{
				Id:       plan.Type_INT64,
				Nullable: false,
				Size:     8,
			},
		}, nil
	case constant.Float:
		floatValue, _ := constant.Float64Val(astExpr.Value)
		if astExpr.Negative() {
			floatValue = -floatValue
		}
		return &Expr{
			Expr: &plan.Expr_C{
				C: &Const{
					Isnull: false,
					Value: &plan.Const_Dval{
						Dval: floatValue,
					},
				},
			},
			Typ: &plan.Type{
				Id:       plan.Type_FLOAT64,
				Nullable: false,
				Size:     8,
			},
		}, nil
	case constant.String:
		stringValue := constant.StringVal(astExpr.Value)
		return &Expr{
			Expr: &plan.Expr_C{
				C: &Const{
					Isnull: false,
					Value: &plan.Const_Sval{
						Sval: stringValue,
					},
				},
			},
			Typ: &plan.Type{
				Id:       plan.Type_VARCHAR,
				Nullable: false,
				Size:     4,
				Width:    math.MaxInt32,
			},
		}, nil
	default:
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("unsupport value: %v", astExpr.Value))
	}
}

// --- util functions ----

func appendCastBeforeExpr(expr *Expr, toType *Type) (*Expr, error) {
	argsType := []types.T{
		types.T(expr.Typ.Id),
		types.T(toType.Id),
	}
	_, funcId, _, err := function.GetFunctionByName("cast", argsType)
	if err != nil {
		return nil, err
	}
	return &Expr{
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: getFunctionObjRef(funcId, "cast"),
				Args: []*Expr{expr, {
					Expr: &plan.Expr_T{
						T: &plan.TargetType{
							Typ: toType,
						},
					},
				}},
			},
		},
		Typ: toType,
	}, nil
}

func resetDateFunctionArgs(dateExpr *Expr, intervalExpr *Expr) ([]*Expr, error) {
	strExpr := intervalExpr.Expr.(*plan.Expr_F).F.Args[0].Expr
	intervalStr := strExpr.(*plan.Expr_C).C.Value.(*plan.Const_Sval).Sval
	intervalArray := strings.Split(intervalStr, " ")

	intervalType, err := types.IntervalTypeOf(intervalArray[1])
	if err != nil {
		return nil, err
	}
	returnNum, returnType, err := types.NormalizeInterval(intervalArray[0], intervalType)
	if err != nil {
		return nil, err
	}

	// "date '2020-10-10' - interval 1 Hour"  will return datetime
	// so we rewrite "date '2020-10-10' - interval 1 Hour"  to  "date_add(datetime, 1, hour)"
	if dateExpr.Typ.Id == plan.Type_DATE {
		switch returnType {
		case types.Day, types.Week, types.Month, types.Quarter, types.Year:
		default:
			dateExpr, err = appendCastBeforeExpr(dateExpr, &plan.Type{
				Id:   plan.Type_DATETIME,
				Size: 8,
			})

			if err != nil {
				return nil, err
			}
		}
	}

	return []*Expr{
		dateExpr,
		{
			Expr: &plan.Expr_C{
				C: &Const{
					Value: &plan.Const_Ival{
						Ival: returnNum,
					},
				},
			},
			Typ: &plan.Type{
				Id:   plan.Type_INT64,
				Size: 8,
			},
		},
		{
			Expr: &plan.Expr_C{
				C: &Const{
					Value: &plan.Const_Ival{
						Ival: int64(returnType),
					},
				},
			},
			Typ: &plan.Type{
				Id:   plan.Type_INT64,
				Size: 8,
			},
		},
	}, nil
}

//splitConjunctiveCondition split a expression to a list of AND conditions.
func splitConjunctiveCondition(astExpr tree.Expr) []tree.Expr {
	var astExprs []tree.Expr
	switch typ := astExpr.(type) {
	case nil:
	case *tree.AndExpr:
		astExprs = append(astExprs, splitConjunctiveCondition(typ.Left)...)
		astExprs = append(astExprs, splitConjunctiveCondition(typ.Right)...)
	case *tree.ParenExpr:
		astExprs = append(astExprs, splitConjunctiveCondition(typ.Expr)...)
	default:
		astExprs = append(astExprs, astExpr)
	}
	return astExprs
}
