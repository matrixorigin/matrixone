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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

//splitAndBuildExpr split expr to AND conditions first，and then build []*conditions to []*Expr
func splitAndBuildExpr(stmt tree.Expr, ctx CompilerContext, query *Query, aliasCtx *AliasContext) ([]*plan.Expr, error) {
	var exprs []*plan.Expr

	conds := splitExprToAND(stmt)
	for _, cond := range conds {
		result_expr, err := buildExpr(*cond, ctx, query, aliasCtx)
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, result_expr)
	}

	return exprs, nil
}

//buildExpr
func buildExpr(stmt tree.Expr, ctx CompilerContext, query *Query, aliasCtx *AliasContext) (*plan.Expr, error) {
	switch astExpr := stmt.(type) {
	case *tree.NumVal:
		return buildNumVal(astExpr.Value)
	case *tree.ParenExpr:
		return buildExpr(astExpr.Expr, ctx, query, aliasCtx)
	case *tree.OrExpr:
		return getFunctionExprByNameAndExprs("OR", []tree.Expr{astExpr.Left, astExpr.Right}, ctx, query, aliasCtx)
	case *tree.NotExpr:
		return getFunctionExprByNameAndExprs("NOT", []tree.Expr{astExpr.Expr}, ctx, query, aliasCtx)
	case *tree.AndExpr:
		return getFunctionExprByNameAndExprs("AND", []tree.Expr{astExpr.Expr}, ctx, query, aliasCtx)
	case *tree.UnaryExpr:
		return buildUnaryExpr(astExpr, ctx, query, aliasCtx)
	case *tree.BinaryExpr:
		return buildBinaryExpr(astExpr, ctx, query, aliasCtx)
	case *tree.ComparisonExpr:
		return buildComparisonExpr(astExpr, ctx, query, aliasCtx)
	case *tree.FuncExpr:
		return buildFunctionExpr(astExpr, ctx, query, aliasCtx)
	case *tree.RangeCond:
		return buildRangeCond(astExpr, ctx, query, aliasCtx)
	case *tree.UnresolvedName:
		return buildUnresolvedName(astExpr, ctx, query, aliasCtx)
	case *tree.CastExpr:
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", stmt))
	case *tree.IsNullExpr:
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", stmt))
	case *tree.IsNotNullExpr:
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", stmt))
	case *tree.Tuple:
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", stmt))
	case *tree.CaseExpr:
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", stmt))
	case *tree.IntervalExpr:
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", stmt))
	case *tree.DefaultVal:
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", stmt))
	case *tree.MaxValue:
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", stmt))
	case *tree.VarExpr:
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", stmt))
	case *tree.StrVal:
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", stmt))
	case *tree.ExprList:
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", stmt))
	}
	return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", stmt))
}

func buildUnresolvedName(expr *tree.UnresolvedName, ctx CompilerContext, query *Query, aliasCtx *AliasContext) (*plan.Expr, error) {
	switch expr.NumParts {
	case 1:
		if aliasCtx != nil {
			if expr, ok := aliasCtx.columnAlias[expr.Parts[0]]; ok {
				return expr, nil
			}
		}
		return getColExprByFieldName(expr.Parts[0], getDefaultTableDef(query))
	case 2:
		var tableDef *plan.TableDef
		if aliasCtx != nil {
			if val, ok := aliasCtx.tableAlias[expr.Parts[1]]; ok {
				tableDef = val
			}
		}
		if tableDef == nil {
			_, tableDef = ctx.Resolve(expr.Parts[1])
		}
		return getColExprByFieldName(expr.Parts[0], tableDef)
	case 3:
		//todo
	case 4:
		//todo
	}
	return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", expr))
}

func buildRangeCond(expr *tree.RangeCond, ctx CompilerContext, query *Query, aliasCtx *AliasContext) (*plan.Expr, error) {
	if expr.Not {
		left, err := getFunctionExprByNameAndExprs("<", []tree.Expr{expr.Left, expr.From}, ctx, query, aliasCtx)
		if err != nil {
			return nil, err
		}
		right, err := getFunctionExprByNameAndExprs(">", []tree.Expr{expr.Left, expr.To}, ctx, query, aliasCtx)
		if err != nil {
			return nil, err
		}
		funObjRef, _ := getFunctionObjRef("OR")
		return &plan.Expr{
			Expr: &plan.Expr_F{
				F: &plan.Function{
					Func: funObjRef,
					Args: []*plan.Expr{left, right},
				},
			},
		}, nil
	} else {
		left, err := getFunctionExprByNameAndExprs(">=", []tree.Expr{expr.Left, expr.From}, ctx, query, aliasCtx)
		if err != nil {
			return nil, err
		}
		right, err := getFunctionExprByNameAndExprs("<=", []tree.Expr{expr.Left, expr.To}, ctx, query, aliasCtx)
		if err != nil {
			return nil, err
		}
		funObjRef, _ := getFunctionObjRef("AND")
		return &plan.Expr{
			Expr: &plan.Expr_F{
				F: &plan.Function{
					Func: funObjRef,
					Args: []*plan.Expr{left, right},
				},
			},
		}, nil
	}
}

func buildFunctionExpr(expr *tree.FuncExpr, ctx CompilerContext, query *Query, aliasCtx *AliasContext) (*plan.Expr, error) {
	name, ok := expr.Func.FunctionReference.(*tree.UnresolvedName)
	if !ok {
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", expr))
	}
	funcName := strings.ToUpper(name.Parts[0])
	//todo  check if need map funcName to plan2's function name
	return getFunctionExprByNameAndExprs(funcName, expr.Exprs, ctx, query, aliasCtx)
}

func buildComparisonExpr(expr *tree.ComparisonExpr, ctx CompilerContext, query *Query, aliasCtx *AliasContext) (*plan.Expr, error) {
	switch expr.Op {
	case tree.EQUAL:
		return getFunctionExprByNameAndExprs("=", []tree.Expr{expr.Left, expr.Right}, ctx, query, aliasCtx)
	case tree.LESS_THAN:
		return getFunctionExprByNameAndExprs("<", []tree.Expr{expr.Left, expr.Right}, ctx, query, aliasCtx)
	case tree.LESS_THAN_EQUAL:
		return getFunctionExprByNameAndExprs("<=", []tree.Expr{expr.Left, expr.Right}, ctx, query, aliasCtx)
	case tree.GREAT_THAN:
		return getFunctionExprByNameAndExprs(">", []tree.Expr{expr.Left, expr.Right}, ctx, query, aliasCtx)
	case tree.GREAT_THAN_EQUAL:
		return getFunctionExprByNameAndExprs(">=", []tree.Expr{expr.Left, expr.Right}, ctx, query, aliasCtx)
	case tree.NOT_EQUAL:
		return getFunctionExprByNameAndExprs("<>", []tree.Expr{expr.Left, expr.Right}, ctx, query, aliasCtx)
	case tree.LIKE:
		return getFunctionExprByNameAndExprs("LIKE", []tree.Expr{expr.Left, expr.Right}, ctx, query, aliasCtx)
	}
	return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", expr))
}

func buildUnaryExpr(expr *tree.UnaryExpr, ctx CompilerContext, query *Query, aliasCtx *AliasContext) (*plan.Expr, error) {
	switch expr.Op {
	case tree.UNARY_MINUS:
		return getFunctionExprByNameAndExprs("UNARY_MINUS", []tree.Expr{expr.Expr}, ctx, query, aliasCtx)
	case tree.UNARY_PLUS:
		return getFunctionExprByNameAndExprs("UNARY_PLUS", []tree.Expr{expr.Expr}, ctx, query, aliasCtx)
	case tree.UNARY_TILDE:
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", expr))
	case tree.UNARY_MARK:
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", expr))
	}
	return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", expr))
}

func buildBinaryExpr(expr *tree.BinaryExpr, ctx CompilerContext, query *Query, aliasCtx *AliasContext) (*plan.Expr, error) {
	switch expr.Op {
	case tree.PLUS:
		return getFunctionExprByNameAndExprs("+", []tree.Expr{expr.Left, expr.Right}, ctx, query, aliasCtx)
	case tree.MINUS:
		return getFunctionExprByNameAndExprs("-", []tree.Expr{expr.Left, expr.Right}, ctx, query, aliasCtx)
	case tree.MULTI:
		return getFunctionExprByNameAndExprs("*", []tree.Expr{expr.Left, expr.Right}, ctx, query, aliasCtx)
	case tree.MOD:
		return getFunctionExprByNameAndExprs("%", []tree.Expr{expr.Left, expr.Right}, ctx, query, aliasCtx)
	case tree.DIV:
		return getFunctionExprByNameAndExprs("/", []tree.Expr{expr.Left, expr.Right}, ctx, query, aliasCtx)
	case tree.INTEGER_DIV:
		//todo confirm what is the difference from tree.DIV
		return getFunctionExprByNameAndExprs("/", []tree.Expr{expr.Left, expr.Right}, ctx, query, aliasCtx)
	}

	return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", expr))
}

func buildNumVal(val constant.Value) (*plan.Expr, error) {
	switch val.Kind() {
	case constant.Int:
		intValue, _ := constant.Int64Val(val)
		return &plan.Expr{
			Expr: &plan.Expr_C{
				C: &plan.Const{
					Isnull: false,
					Value: &plan.Const_Ival{
						Ival: intValue,
					},
				},
			},
		}, nil
	case constant.Float:
		floatValue, _ := constant.Float64Val(val)
		return &plan.Expr{
			Expr: &plan.Expr_C{
				C: &plan.Const{
					Isnull: false,
					Value: &plan.Const_Dval{
						Dval: floatValue,
					},
				},
			},
		}, nil
	case constant.String:
		stringValue := constant.StringVal(val)
		return &plan.Expr{
			Expr: &plan.Expr_C{
				C: &plan.Const{
					Isnull: false,
					Value: &plan.Const_Sval{
						Sval: stringValue,
					},
				},
			},
		}, nil
	default:
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("unsupport value: %v", val))
	}
}
