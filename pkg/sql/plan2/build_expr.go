// Copyright 2021 - 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
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
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

//splitAndBuildExpr split expr to AND conditions first，and then build []*conditions to []*Expr
func splitAndBuildExpr(stmt tree.Expr, ctx CompilerContext, query *Query, node *Node, binderCtx *BinderContext, needAgg bool) ([]*Expr, error) {
	conds := splitExprToAND(stmt)
	exprs := make([]*Expr, len(conds))
	for i, cond := range conds {
		expr, isAgg, err := buildExpr(*cond, ctx, query, node, binderCtx, needAgg)
		if err != nil {
			return nil, err
		}
		if needAgg != isAgg {
			return nil, errors.New(errno.GroupingError, fmt.Sprintf("'%v' must appear in the GROUP BY clause or be used in an aggregate function", tree.String(*cond, dialect.MYSQL)))
		}
		exprs[i] = expr
	}

	return exprs, nil
}

//buildExpr
func buildExpr(stmt tree.Expr, ctx CompilerContext, query *Query, node *Node, binderCtx *BinderContext, needAgg bool) (resultExpr *Expr, isAgg bool, err error) {
	colName := tree.String(stmt, dialect.MYSQL)

	if needAgg {
		colPos := int32(-1)
		for i, col := range node.GroupBy {
			if colName == col.ColName {
				colPos = int32(i)
				break
			}
		}
		if colPos != -1 {
			return &Expr{
				Typ:     node.GroupBy[colPos].Typ,
				ColName: colName,
				Expr: &plan.Expr_Col{
					Col: &ColRef{
						RelPos: -1,
						ColPos: colPos,
					},
				},
			}, true, nil
		}

		colPos = -1
		for i, col := range node.AggList {
			if colName == col.ColName {
				colPos = int32(i)
				break
			}
		}
		if colPos != -1 {
			return &Expr{
				Typ:     node.AggList[colPos].Typ,
				ColName: colName,
				Expr: &plan.Expr_Col{
					Col: &ColRef{
						RelPos: -2,
						ColPos: colPos,
					},
				},
			}, true, nil
		}
	} else {
		for i, child := range node.Children {
			for j, proj := range query.Nodes[child].ProjectList {
				if len(proj.TableName) == 0 && colName == proj.ColName {
					resultExpr = &plan.Expr{
						Typ:     proj.Typ,
						ColName: proj.ColName,
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								RelPos: int32(i),
								ColPos: int32(j),
							},
						},
					}

					return
				}
			}
		}
	}

	switch astExpr := stmt.(type) {
	case *tree.NumVal:
		resultExpr, err = buildNumVal(astExpr)
		isAgg = needAgg
	case *tree.ParenExpr:
		resultExpr, isAgg, err = buildExpr(astExpr.Expr, ctx, query, node, binderCtx, needAgg)
	case *tree.OrExpr:
		resultExpr, isAgg, err = getFunctionExprByNameAndAstExprs("or", false, []tree.Expr{astExpr.Left, astExpr.Right}, ctx, query, node, binderCtx, needAgg)
	case *tree.XorExpr:
		resultExpr, isAgg, err = getFunctionExprByNameAndAstExprs("xor", false, []tree.Expr{astExpr.Left, astExpr.Right}, ctx, query, node, binderCtx, needAgg)
	case *tree.NotExpr:
		resultExpr, isAgg, err = getFunctionExprByNameAndAstExprs("not", false, []tree.Expr{astExpr.Expr}, ctx, query, node, binderCtx, needAgg)
	case *tree.AndExpr:
		resultExpr, isAgg, err = getFunctionExprByNameAndAstExprs("and", false, []tree.Expr{astExpr.Left, astExpr.Right}, ctx, query, node, binderCtx, needAgg)
	case *tree.UnaryExpr:
		resultExpr, isAgg, err = buildUnaryExpr(astExpr, ctx, query, node, binderCtx, needAgg)
	case *tree.BinaryExpr:
		resultExpr, isAgg, err = buildBinaryExpr(astExpr, ctx, query, node, binderCtx, needAgg)
	case *tree.ComparisonExpr:
		resultExpr, isAgg, err = buildComparisonExpr(astExpr, ctx, query, node, binderCtx, needAgg)
	case *tree.FuncExpr:
		resultExpr, isAgg, err = buildFunctionExpr(astExpr, ctx, query, node, binderCtx, needAgg)
	case *tree.RangeCond:
		resultExpr, isAgg, err = buildRangeCond(astExpr, ctx, query, node, binderCtx, needAgg)
	case *tree.UnresolvedName:
		resultExpr, err = buildColRefExpr(astExpr, ctx, query, node, binderCtx, needAgg)
	case *tree.CastExpr:
		resultExpr, isAgg, err = buildCastExpr(astExpr, ctx, query, node, binderCtx, needAgg)
	case *tree.IsNullExpr:
		resultExpr, isAgg, err = getFunctionExprByNameAndAstExprs("ifnull", false, []tree.Expr{astExpr.Expr}, ctx, query, node, binderCtx, needAgg)
	case *tree.IsNotNullExpr:
		resultExpr, isAgg, err = getFunctionExprByNameAndAstExprs("ifnull", false, []tree.Expr{astExpr.Expr}, ctx, query, node, binderCtx, needAgg)
		if err != nil {
			return
		}
		resultExpr, _, err = getFunctionExprByNameAndPlanExprs("not", false, []*Expr{resultExpr})
	case *tree.Tuple:
		exprs := make([]*Expr, 0, len(astExpr.Exprs))
		for _, ast := range astExpr.Exprs {
			resultExpr, isAgg, err = buildExpr(ast, ctx, query, node, binderCtx, needAgg)
			if err != nil {
				return
			}
			exprs = append(exprs, resultExpr)
		}
		return &Expr{
			Expr: &plan.Expr_List{
				List: &plan.ExprList{
					List: exprs,
				},
			},
			Typ: &plan.Type{
				Id: plan.Type_TUPLE,
			},
		}, needAgg, nil
	case *tree.CaseExpr:
		resultExpr, isAgg, err = buildCaseExpr(astExpr, ctx, query, node, binderCtx, needAgg)
	case *tree.IntervalExpr:
		return nil, false, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("expr interval'%v' is not support now", stmt))
	case *tree.Subquery:
		resultExpr, err = buildSubQuery(astExpr, ctx, query, node, binderCtx)
		isAgg = needAgg
	case *tree.DefaultVal:
		return nil, false, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("expr default'%v' is not support now", stmt))
	case *tree.MaxValue:
		return nil, false, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("expr max'%v' is not support now", stmt))
	case *tree.VarExpr:
		return nil, false, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("expr var'%v' is not support now", stmt))
	case *tree.StrVal:
		return nil, false, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("expr str'%v' is not support now", stmt))
	case *tree.ExprList:
		return nil, false, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("expr plan.ExprList'%v' is not support now", stmt))
	case tree.UnqualifiedStar:
		// select * from table
		list := &plan.ExprList{}
		err = unfoldStar(query, node, list, "")
		if err != nil {
			return
		}
		return &Expr{
			Expr: &plan.Expr_List{
				List: list,
			},
		}, false, nil
	default:
		return nil, false, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("expr '%+v' is not support now", stmt))
	}

	if err != nil {
		return
	}

	if len(resultExpr.ColName) == 0 {
		resultExpr.ColName = colName
	}

	if col, ok := resultExpr.Expr.(*plan.Expr_Col); ok {
		if col.Col.RelPos == -2 && node.NodeType == plan.Node_AGG {
			node.AggList[col.Col.ColPos].ColName = colName
		}
	}

	return
}

func buildCastExpr(astExpr *tree.CastExpr, ctx CompilerContext, query *Query, node *Node, binderCtx *BinderContext, needAgg bool) (expr *Expr, isAgg bool, err error) {
	expr, isAgg, err = buildExpr(astExpr.Expr, ctx, query, node, binderCtx, needAgg)
	if err != nil {
		return
	}
	typ, err := getTypeFromAst(astExpr.Type)
	if err != nil {
		return
	}
	expr, err = appendCastExpr(expr, typ)
	return
}

func buildCaseExpr(astExpr *tree.CaseExpr, ctx CompilerContext, query *Query, node *Node, binderCtx *BinderContext, needAgg bool) (expr *Expr, isAgg bool, err error) {
	var args []*Expr
	var caseExpr *Expr

	if astExpr.Expr != nil {
		caseExpr, _, err = buildExpr(astExpr.Expr, ctx, query, node, binderCtx, needAgg)
		if err != nil {
			return nil, false, err
		}
	}

	isAgg = true
	for _, whenExpr := range astExpr.Whens {
		condExpr, paramIsAgg, err := buildExpr(whenExpr.Cond, ctx, query, node, binderCtx, needAgg)
		if err != nil {
			return nil, false, err
		}
		isAgg = isAgg && paramIsAgg
		if caseExpr != nil {
			// rewrite "case col when 1 then '1' else '2'" to "case when col=1 then '1' else '2'"
			condExpr, _, err = getFunctionExprByNameAndPlanExprs("=", false, []*Expr{caseExpr, condExpr})
			if err != nil {
				return nil, false, err
			}
		}
		args = append(args, condExpr)

		valExpr, paramIsAgg, err := buildExpr(whenExpr.Val, ctx, query, node, binderCtx, needAgg)
		if err != nil {
			return nil, false, err
		}
		isAgg = isAgg && paramIsAgg
		args = append(args, valExpr)
	}

	if astExpr.Else != nil {
		elseExpr, _, err := buildExpr(astExpr.Else, ctx, query, node, binderCtx, needAgg)
		if err != nil {
			return nil, false, err
		}
		args = append(args, elseExpr)
	} else {
		args = append(args, &Expr{
			Expr: &plan.Expr_C{
				C: &Const{
					Isnull: true,
				},
			},
			Typ: &plan.Type{
				Id:       plan.Type_ANY,
				Nullable: true,
			},
		})
	}
	return getFunctionExprByNameAndPlanExprs("case", false, args)
}

func buildColRefExpr(astExpr *tree.UnresolvedName, ctx CompilerContext, query *Query, node *Node, binderCtx *BinderContext, needAgg bool) (expr *Expr, err error) {
	switch astExpr.NumParts {
	case 1:
		// FIXME: expand star
		if astExpr.Star {
			table := astExpr.Parts[0]
			list := &plan.ExprList{}
			err = unfoldStar(query, node, list, table)
			if err != nil {
				return
			}
			return &Expr{
				Expr: &plan.Expr_List{
					List: list,
				},
			}, nil
		}
		name := astExpr.Parts[0]
		if binderCtx != nil {
			if val, ok := binderCtx.columnAlias[name]; ok {
				return DeepCopyExpr(val), nil
			}
		}

		return buildUnresolvedName(query, node, name, "", binderCtx)
	case 2:
		table := astExpr.Parts[1]
		return buildUnresolvedName(query, node, astExpr.Parts[0], table, binderCtx)
	case 3:
		// todo
	case 4:
		// todo
	}
	return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", astExpr))
}

func buildRangeCond(astExpr *tree.RangeCond, ctx CompilerContext, query *Query, node *Node, binderCtx *BinderContext, needAgg bool) (resultExpr *Expr, isAgg bool, err error) {
	if astExpr.Not {
		left, paramIsAgg, err := getFunctionExprByNameAndAstExprs("<", false, []tree.Expr{astExpr.Left, astExpr.From}, ctx, query, node, binderCtx, needAgg)
		if err != nil {
			return nil, false, err
		}
		isAgg = paramIsAgg
		right, paramIsAgg, err := getFunctionExprByNameAndAstExprs(">", false, []tree.Expr{astExpr.Left, astExpr.To}, ctx, query, node, binderCtx, needAgg)
		if err != nil {
			return nil, false, err
		}
		isAgg = isAgg && paramIsAgg
		resultExpr, _, err = getFunctionExprByNameAndPlanExprs("or", false, []*Expr{left, right})
		return resultExpr, isAgg, err
	} else {
		left, paramIsAgg, err := getFunctionExprByNameAndAstExprs(">=", false, []tree.Expr{astExpr.Left, astExpr.From}, ctx, query, node, binderCtx, needAgg)
		if err != nil {
			return nil, false, err
		}
		isAgg = paramIsAgg
		right, paramIsAgg, err := getFunctionExprByNameAndAstExprs("<=", false, []tree.Expr{astExpr.Left, astExpr.To}, ctx, query, node, binderCtx, needAgg)
		if err != nil {
			return nil, false, err
		}
		isAgg = isAgg && paramIsAgg
		resultExpr, _, err = getFunctionExprByNameAndPlanExprs("and", false, []*Expr{left, right})
		return resultExpr, isAgg, err
	}
}

func buildFunctionExpr(astExpr *tree.FuncExpr, ctx CompilerContext, query *Query, node *Node, binderCtx *BinderContext, needAgg bool) (expr *Expr, isAgg bool, err error) {
	funcReference, ok := astExpr.Func.FunctionReference.(*tree.UnresolvedName)
	if !ok {
		return nil, false, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("function expr '%v' is not support now", astExpr))
	}
	funcName := funcReference.Parts[0]
	return getFunctionExprByNameAndAstExprs(funcName, astExpr.Type == tree.FUNC_TYPE_DISTINCT, astExpr.Exprs, ctx, query, node, binderCtx, needAgg)
}

func buildComparisonExpr(astExpr *tree.ComparisonExpr, ctx CompilerContext, query *Query, node *Node, binderCtx *BinderContext, needAgg bool) (resultExpr *Expr, isAgg bool, err error) {
	switch astExpr.Op {
	case tree.EQUAL:
		return getFunctionExprByNameAndAstExprs("=", false, []tree.Expr{astExpr.Left, astExpr.Right}, ctx, query, node, binderCtx, needAgg)
	case tree.LESS_THAN:
		return getFunctionExprByNameAndAstExprs("<", false, []tree.Expr{astExpr.Left, astExpr.Right}, ctx, query, node, binderCtx, needAgg)
	case tree.LESS_THAN_EQUAL:
		return getFunctionExprByNameAndAstExprs("<=", false, []tree.Expr{astExpr.Left, astExpr.Right}, ctx, query, node, binderCtx, needAgg)
	case tree.GREAT_THAN:
		return getFunctionExprByNameAndAstExprs(">", false, []tree.Expr{astExpr.Left, astExpr.Right}, ctx, query, node, binderCtx, needAgg)
	case tree.GREAT_THAN_EQUAL:
		return getFunctionExprByNameAndAstExprs(">=", false, []tree.Expr{astExpr.Left, astExpr.Right}, ctx, query, node, binderCtx, needAgg)
	case tree.NOT_EQUAL:
		return getFunctionExprByNameAndAstExprs("<>", false, []tree.Expr{astExpr.Left, astExpr.Right}, ctx, query, node, binderCtx, needAgg)
	case tree.LIKE:
		return getFunctionExprByNameAndAstExprs("like", false, []tree.Expr{astExpr.Left, astExpr.Right}, ctx, query, node, binderCtx, needAgg)
	case tree.NOT_LIKE:
		resultExpr, isAgg, err = getFunctionExprByNameAndAstExprs("like", false, []tree.Expr{astExpr.Left, astExpr.Right}, ctx, query, node, binderCtx, needAgg)
		if err != nil {
			return
		}
		resultExpr, _, err = getFunctionExprByNameAndPlanExprs("not", false, []*Expr{resultExpr})
		return
	case tree.IN:
		return buildInExpr(astExpr, ctx, query, node, binderCtx, needAgg)
	case tree.NOT_IN:
		return buildNotInExpr(astExpr, ctx, query, node, binderCtx, needAgg)
	}
	return nil, false, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", astExpr))
}

func buildInExpr(astExpr *tree.ComparisonExpr, ctx CompilerContext, query *Query, node *Node, binderCtx *BinderContext, needAgg bool) (resultExpr *Expr, isAgg bool, err error) {
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
		return buildExpr(new_expr, ctx, query, node, binderCtx, needAgg)
	default:
		return getFunctionExprByNameAndAstExprs("in", false, []tree.Expr{astExpr.Left, astExpr.Right}, ctx, query, node, binderCtx, needAgg)
	}
}

func buildNotInExpr(astExpr *tree.ComparisonExpr, ctx CompilerContext, query *Query, node *Node, binderCtx *BinderContext, needAgg bool) (resultExpr *Expr, isAgg bool, err error) {
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
		return buildExpr(new_expr, ctx, query, node, binderCtx, needAgg)
	default:
		resultExpr, _, err := getFunctionExprByNameAndAstExprs("in", false, []tree.Expr{astExpr.Left, astExpr.Right}, ctx, query, node, binderCtx, needAgg)
		if err != nil {
			return nil, false, err
		}
		return getFunctionExprByNameAndPlanExprs("not", false, []*Expr{resultExpr})
	}
}

func buildUnaryExpr(astExpr *tree.UnaryExpr, ctx CompilerContext, query *Query, node *Node, binderCtx *BinderContext, needAgg bool) (expr *Expr, isAgg bool, err error) {
	switch astExpr.Op {
	case tree.UNARY_MINUS:
		return getFunctionExprByNameAndAstExprs("unary_minus", false, []tree.Expr{astExpr.Expr}, ctx, query, node, binderCtx, needAgg)
	case tree.UNARY_PLUS:
		return getFunctionExprByNameAndAstExprs("unary_plus", false, []tree.Expr{astExpr.Expr}, ctx, query, node, binderCtx, needAgg)
	case tree.UNARY_TILDE:
		return nil, false, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", astExpr))
	case tree.UNARY_MARK:
		return nil, false, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", astExpr))
	}
	return nil, false, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", astExpr))
}

func buildBinaryExpr(astExpr *tree.BinaryExpr, ctx CompilerContext, query *Query, node *Node, binderCtx *BinderContext, needAgg bool) (expr *Expr, isAgg bool, err error) {
	switch astExpr.Op {
	case tree.PLUS:
		return getFunctionExprByNameAndAstExprs("+", false, []tree.Expr{astExpr.Left, astExpr.Right}, ctx, query, node, binderCtx, needAgg)
	case tree.MINUS:
		return getFunctionExprByNameAndAstExprs("-", false, []tree.Expr{astExpr.Left, astExpr.Right}, ctx, query, node, binderCtx, needAgg)
	case tree.MULTI:
		return getFunctionExprByNameAndAstExprs("*", false, []tree.Expr{astExpr.Left, astExpr.Right}, ctx, query, node, binderCtx, needAgg)
	case tree.MOD:
		return getFunctionExprByNameAndAstExprs("%", false, []tree.Expr{astExpr.Left, astExpr.Right}, ctx, query, node, binderCtx, needAgg)
	case tree.DIV:
		return getFunctionExprByNameAndAstExprs("/", false, []tree.Expr{astExpr.Left, astExpr.Right}, ctx, query, node, binderCtx, needAgg)
	case tree.INTEGER_DIV:
		return getFunctionExprByNameAndAstExprs("div", false, []tree.Expr{astExpr.Left, astExpr.Right}, ctx, query, node, binderCtx, needAgg)
	}

	return nil, false, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", astExpr))
}

func buildNumVal(astExpr *tree.NumVal) (*Expr, error) {
	getStringExpr := func(val string) *Expr {
		return &Expr{
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
				Width:    int32(len(val)),
			},
		}
	}

	returnDecimalExpr := func(val string) (*Expr, error) {
		typ := &plan.Type{
			Id:       plan.Type_DECIMAL128,
			Width:    int32(len(val)),
			Scale:    0,
			Nullable: false,
		}
		return appendCastBeforeExpr(getStringExpr(val), typ)
	}

	switch astExpr.ValType {
	case tree.P_null:
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
	case tree.P_bool:
		val := constant.BoolVal(astExpr.Value)
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
	case tree.P_int64:
		val, ok := constant.Int64Val(astExpr.Value)
		if !ok {
			return nil, errors.New("", "Parser error")
		}
		return &Expr{
			Expr: &plan.Expr_C{
				C: &Const{
					Isnull: false,
					Value: &plan.Const_Ival{
						Ival: val,
					},
				},
			},
			Typ: &plan.Type{
				Id:       plan.Type_INT64,
				Nullable: false,
				Size:     8,
			},
		}, nil
	case tree.P_uint64:
		val, ok := constant.Uint64Val(astExpr.Value)
		if !ok {
			return nil, errors.New("", "Parser error")
		}
		return &Expr{
			Expr: &plan.Expr_C{
				C: &Const{
					Isnull: false,
					Value: &plan.Const_Uval{
						Uval: val,
					},
				},
			},
			Typ: &plan.Type{
				Id:       plan.Type_UINT64,
				Nullable: false,
				Size:     8,
			},
		}, nil
	case tree.P_decimal:
		return returnDecimalExpr(astExpr.String())
	case tree.P_float64:
		floatValue, ok := constant.Float64Val(astExpr.Value)
		if !ok {
			return returnDecimalExpr(astExpr.String())
		}
		//if astExpr.Negative() {
		//	floatValue = -floatValue
		//}
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
	case tree.P_hexnum:
		val, err := strconv.ParseInt(astExpr.String()[2:], 16, 64)
		if err != nil {
			return returnDecimalExpr(astExpr.String())
		}
		return &Expr{
			Expr: &plan.Expr_C{
				C: &Const{
					Isnull: false,
					Value: &plan.Const_Ival{
						Ival: val,
					},
				},
			},
			Typ: &plan.Type{
				Id:       plan.Type_INT64,
				Nullable: false,
				Size:     8,
			},
		}, nil
	case tree.P_char:
		stringValue := constant.StringVal(astExpr.Value)
		return getStringExpr(stringValue), nil
	default:
		return nil, errors.New("", fmt.Sprintf("unsupport value: %v", astExpr.Value))
	}
}
