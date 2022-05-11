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
	"math"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

//splitAndBuildExpr split expr to AND conditions first，and then build []*conditions to []*Expr
func splitAndBuildExpr(stmt tree.Expr, ctx CompilerContext, query *Query, selectCtx *SelectContext) ([]*plan.Expr, error) {
	conds := splitExprToAND(stmt)
	exprs := make([]*plan.Expr, 0, len(conds))
	for _, cond := range conds {
		expr, err := buildExpr(*cond, ctx, query, selectCtx)
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, expr)
	}

	return exprs, nil
}

//buildExpr
func buildExpr(stmt tree.Expr, ctx CompilerContext, query *Query, selectCtx *SelectContext) (*plan.Expr, error) {
	switch astExpr := stmt.(type) {
	case *tree.NumVal:
		return buildNumVal(astExpr.Value)
	case *tree.ParenExpr:
		return buildExpr(astExpr.Expr, ctx, query, selectCtx)
	case *tree.OrExpr:
		return getFunctionExprByNameAndExprs("OR", []tree.Expr{astExpr.Left, astExpr.Right}, ctx, query, selectCtx)
	case *tree.NotExpr:
		return getFunctionExprByNameAndExprs("NOT", []tree.Expr{astExpr.Expr}, ctx, query, selectCtx)
	case *tree.AndExpr:
		return getFunctionExprByNameAndExprs("AND", []tree.Expr{astExpr.Left, astExpr.Right}, ctx, query, selectCtx)
	case *tree.UnaryExpr:
		return buildUnaryExpr(astExpr, ctx, query, selectCtx)
	case *tree.BinaryExpr:
		return buildBinaryExpr(astExpr, ctx, query, selectCtx)
	case *tree.ComparisonExpr:
		return buildComparisonExpr(astExpr, ctx, query, selectCtx)
	case *tree.FuncExpr:
		return buildFunctionExpr(astExpr, ctx, query, selectCtx)
	case *tree.RangeCond:
		return buildRangeCond(astExpr, ctx, query, selectCtx)
	case *tree.UnresolvedName:
		return buildUnresolvedName(astExpr, ctx, query, selectCtx)
	case *tree.CastExpr:
		return buildCast(astExpr, ctx, query, selectCtx)
	case *tree.IsNullExpr:
		return getFunctionExprByNameAndExprs("IFNULL", []tree.Expr{astExpr.Expr}, ctx, query, selectCtx)
	case *tree.IsNotNullExpr:
		expr, err := getFunctionExprByNameAndExprs("IFNULL", []tree.Expr{astExpr.Expr}, ctx, query, selectCtx)
		if err != nil {
			return nil, err
		}
		funObjRef := getFunctionObjRef("NOT")
		return &plan.Expr{
			Expr: &plan.Expr_F{
				F: &plan.Function{
					Func: funObjRef,
					Args: []*plan.Expr{expr},
				},
			},
			Typ: &plan.Type{
				Id: plan.Type_BOOL,
			},
		}, nil
	case *tree.Tuple:
		exprs := make([]*plan.Expr, 0, len(astExpr.Exprs))
		for _, ast := range astExpr.Exprs {
			expr, err := buildExpr(ast, ctx, query, selectCtx)
			if err != nil {
				return nil, err
			}
			exprs = append(exprs, expr)
		}
		return &plan.Expr{
			Expr: &plan.Expr_List{
				List: &plan.ExprList{
					List: exprs,
				},
			},
			Typ: &plan.Type{
				Id: plan.Type_TUPLE,
			},
		}, nil
	case *tree.CaseExpr:
		return buildCase(astExpr, ctx, query, selectCtx)
	case *tree.IntervalExpr:
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("expr interval'%v' is not support now", stmt))
	case *tree.XorExpr:
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("expr xor'%v' is not support now", stmt))
	case *tree.Subquery:
		return buildSubQuery(astExpr, ctx, query, selectCtx)
	case *tree.DefaultVal:
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("expr default'%v' is not support now", stmt))
	case *tree.MaxValue:
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("expr max'%v' is not support now", stmt))
	case *tree.VarExpr:
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("expr var'%v' is not support now", stmt))
	case *tree.StrVal:
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("expr str'%v' is not support now", stmt))
	case *tree.ExprList:
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("expr exprlist'%v' is not support now", stmt))
	case tree.UnqualifiedStar:
		//select * from table
		list := &plan.ExprList{}
		err := unfoldStar(query, list, "")
		if err != nil {
			return nil, err
		}
		return &plan.Expr{
			Expr: &plan.Expr_List{
				List: list,
			},
		}, nil
	default:
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("expr '%+v' is not support now", stmt))
	}
}

func buildCast(astExpr *tree.CastExpr, ctx CompilerContext, query *Query, selectCtx *SelectContext) (*plan.Expr, error) {
	expr, err := buildExpr(astExpr.Expr, ctx, query, selectCtx)
	if err != nil {
		return nil, err
	}
	oid := uint8(astExpr.Type.(*tree.T).InternalType.Oid)
	typeId, ok := AstTypeToPlanTypeMap[oid]
	if !ok {
		return nil, errors.New(errno.IndeterminateDatatype, fmt.Sprintf("'%v' is not support now", astExpr))
	}
	return &plan.Expr{
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: getFunctionObjRef("CAST"),
				Args: []*plan.Expr{expr},
			},
		},
		Typ: &plan.Type{
			Id: typeId,
		},
	}, nil
}

func buildCase(astExpr *tree.CaseExpr, ctx CompilerContext, query *Query, selectCtx *SelectContext) (*plan.Expr, error) {
	var caseExpr *plan.Expr
	var elseExpr *plan.Expr
	var whenExpr *plan.Expr
	var err error

	if astExpr.Expr != nil {
		caseExpr, err = buildExpr(astExpr.Expr, ctx, query, selectCtx)
		if err != nil {
			return nil, err
		}
	}

	if astExpr.Else != nil {
		elseExpr, err = buildExpr(astExpr.Else, ctx, query, selectCtx)
		if err != nil {
			return nil, err
		}
	}

	whenList := make([]*plan.Expr, 0, len(astExpr.Whens))
	for _, whenExpr := range astExpr.Whens {
		exprs := make([]*plan.Expr, 0, 2)
		expr, err := buildExpr(whenExpr.Cond, ctx, query, selectCtx)
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, expr)
		expr, err = buildExpr(whenExpr.Val, ctx, query, selectCtx)
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, expr)

		whenList = append(whenList, &plan.Expr{
			Expr: &plan.Expr_List{
				List: &plan.ExprList{
					List: exprs,
				},
			},
			Typ: &plan.Type{
				Id: plan.Type_TUPLE,
			},
		})
	}
	whenExpr = &plan.Expr{
		Expr: &plan.Expr_List{
			List: &plan.ExprList{
				List: whenList,
			},
		},
		Typ: &plan.Type{
			Id: plan.Type_TUPLE,
		},
	}

	return &plan.Expr{
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: getFunctionObjRef("CASE"),
				Args: []*plan.Expr{caseExpr, whenExpr, elseExpr},
			},
		},
		Typ: &plan.Type{
			Id: plan.Type_ANY,
		},
	}, nil
}

func buildUnresolvedName(expr *tree.UnresolvedName, ctx CompilerContext, query *Query, selectCtx *SelectContext) (*plan.Expr, error) {
	switch expr.NumParts {
	case 1:
		// a.*
		if expr.Star {
			table := expr.Parts[0]
			list := &plan.ExprList{}
			err := unfoldStar(query, list, table)
			if err != nil {
				return nil, err
			}
			return &plan.Expr{
				Expr: &plan.Expr_List{
					List: list,
				},
			}, nil
		}
		name := expr.Parts[0]
		if selectCtx != nil {
			if val, ok := selectCtx.columnAlias[name]; ok {
				return val, nil
			}
		}

		return getExprFromUnresolvedName(query, name, "", selectCtx)
	case 2:
		table := expr.Parts[1]
		return getExprFromUnresolvedName(query, expr.Parts[0], table, selectCtx)
	case 3:
		//todo
	case 4:
		//todo
	}
	return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", expr))
}

func buildRangeCond(expr *tree.RangeCond, ctx CompilerContext, query *Query, selectCtx *SelectContext) (*plan.Expr, error) {
	if expr.Not {
		left, err := getFunctionExprByNameAndExprs("<", []tree.Expr{expr.Left, expr.From}, ctx, query, selectCtx)
		if err != nil {
			return nil, err
		}
		right, err := getFunctionExprByNameAndExprs(">", []tree.Expr{expr.Left, expr.To}, ctx, query, selectCtx)
		if err != nil {
			return nil, err
		}
		funObjRef := getFunctionObjRef("OR")
		return &plan.Expr{
			Expr: &plan.Expr_F{
				F: &plan.Function{
					Func: funObjRef,
					Args: []*plan.Expr{left, right},
				},
			},
			Typ: &plan.Type{
				Id: plan.Type_BOOL,
			},
		}, nil
	} else {
		left, err := getFunctionExprByNameAndExprs(">=", []tree.Expr{expr.Left, expr.From}, ctx, query, selectCtx)
		if err != nil {
			return nil, err
		}
		right, err := getFunctionExprByNameAndExprs("<=", []tree.Expr{expr.Left, expr.To}, ctx, query, selectCtx)
		if err != nil {
			return nil, err
		}
		funObjRef := getFunctionObjRef("AND")
		return &plan.Expr{
			Expr: &plan.Expr_F{
				F: &plan.Function{
					Func: funObjRef,
					Args: []*plan.Expr{left, right},
				},
			},
			Typ: &plan.Type{
				Id: plan.Type_BOOL,
			},
		}, nil
	}
}

func buildFunctionExpr(expr *tree.FuncExpr, ctx CompilerContext, query *Query, selectCtx *SelectContext) (*plan.Expr, error) {
	funcReference, ok := expr.Func.FunctionReference.(*tree.UnresolvedName)
	if !ok {
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("function '%v' is not support now", expr))
	}
	funcName := strings.ToUpper(funcReference.Parts[0])

	//todo confirm: change count(*) to count(1)  but count(*) funcReference.Star is false why?
	if funcName == "COUNT" && funcReference.Star {
		funObjRef := getFunctionObjRef(funcName)
		return &plan.Expr{
			Expr: &plan.Expr_F{
				F: &plan.Function{
					Func: funObjRef,
					Args: []*plan.Expr{
						{
							Expr: &plan.Expr_C{
								C: &plan.Const{
									Isnull: false,
									Value: &plan.Const_Ival{
										Ival: 1,
									},
								},
							},
							Typ: &plan.Type{
								Id: plan.Type_INT64,
							},
						}},
				},
			},
			Typ: &plan.Type{
				Id: plan.Type_INT64,
			},
		}, nil
	}
	return getFunctionExprByNameAndExprs(funcName, expr.Exprs, ctx, query, selectCtx)
}

func buildComparisonExpr(expr *tree.ComparisonExpr, ctx CompilerContext, query *Query, selectCtx *SelectContext) (*plan.Expr, error) {
	switch expr.Op {
	case tree.EQUAL:
		return getFunctionExprByNameAndExprs("=", []tree.Expr{expr.Left, expr.Right}, ctx, query, selectCtx)
	case tree.LESS_THAN:
		return getFunctionExprByNameAndExprs("<", []tree.Expr{expr.Left, expr.Right}, ctx, query, selectCtx)
	case tree.LESS_THAN_EQUAL:
		return getFunctionExprByNameAndExprs("<=", []tree.Expr{expr.Left, expr.Right}, ctx, query, selectCtx)
	case tree.GREAT_THAN:
		return getFunctionExprByNameAndExprs(">", []tree.Expr{expr.Left, expr.Right}, ctx, query, selectCtx)
	case tree.GREAT_THAN_EQUAL:
		return getFunctionExprByNameAndExprs(">=", []tree.Expr{expr.Left, expr.Right}, ctx, query, selectCtx)
	case tree.NOT_EQUAL:
		return getFunctionExprByNameAndExprs("<>", []tree.Expr{expr.Left, expr.Right}, ctx, query, selectCtx)
	case tree.LIKE:
		return getFunctionExprByNameAndExprs("LIKE", []tree.Expr{expr.Left, expr.Right}, ctx, query, selectCtx)
	case tree.NOT_LIKE:
		expr, err := getFunctionExprByNameAndExprs("LIKE", []tree.Expr{expr.Left, expr.Right}, ctx, query, selectCtx)
		if err != nil {
			return nil, err
		}
		funObjRef := getFunctionObjRef("NOT")
		return &plan.Expr{
			Expr: &plan.Expr_F{
				F: &plan.Function{
					Func: funObjRef,
					Args: []*plan.Expr{expr},
				},
			},
			Typ: &plan.Type{
				Id: plan.Type_BOOL,
			},
		}, nil
	case tree.IN:
		return getFunctionExprByNameAndExprs("IN", []tree.Expr{expr.Left, expr.Right}, ctx, query, selectCtx)
	case tree.NOT_IN:
		expr, err := getFunctionExprByNameAndExprs("IN", []tree.Expr{expr.Left, expr.Right}, ctx, query, selectCtx)
		if err != nil {
			return nil, err
		}
		funObjRef := getFunctionObjRef("NOT")
		return &plan.Expr{
			Expr: &plan.Expr_F{
				F: &plan.Function{
					Func: funObjRef,
					Args: []*plan.Expr{expr},
				},
			},
			Typ: &plan.Type{
				Id: plan.Type_BOOL,
			},
		}, nil
	}
	return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", expr))
}

func buildUnaryExpr(expr *tree.UnaryExpr, ctx CompilerContext, query *Query, selectCtx *SelectContext) (*plan.Expr, error) {
	switch expr.Op {
	case tree.UNARY_MINUS:
		return getFunctionExprByNameAndExprs("UNARY_MINUS", []tree.Expr{expr.Expr}, ctx, query, selectCtx)
	case tree.UNARY_PLUS:
		return getFunctionExprByNameAndExprs("UNARY_PLUS", []tree.Expr{expr.Expr}, ctx, query, selectCtx)
	case tree.UNARY_TILDE:
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", expr))
	case tree.UNARY_MARK:
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", expr))
	}
	return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", expr))
}

func buildBinaryExpr(expr *tree.BinaryExpr, ctx CompilerContext, query *Query, selectCtx *SelectContext) (*plan.Expr, error) {
	switch expr.Op {
	case tree.PLUS:
		return getFunctionExprByNameAndExprs("+", []tree.Expr{expr.Left, expr.Right}, ctx, query, selectCtx)
	case tree.MINUS:
		return getFunctionExprByNameAndExprs("-", []tree.Expr{expr.Left, expr.Right}, ctx, query, selectCtx)
	case tree.MULTI:
		return getFunctionExprByNameAndExprs("*", []tree.Expr{expr.Left, expr.Right}, ctx, query, selectCtx)
	case tree.MOD:
		return getFunctionExprByNameAndExprs("%", []tree.Expr{expr.Left, expr.Right}, ctx, query, selectCtx)
	case tree.DIV:
		return getFunctionExprByNameAndExprs("/", []tree.Expr{expr.Left, expr.Right}, ctx, query, selectCtx)
	case tree.INTEGER_DIV:
		//todo confirm what is the difference from tree.DIV
		return getFunctionExprByNameAndExprs("/", []tree.Expr{expr.Left, expr.Right}, ctx, query, selectCtx)
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
			Typ: &plan.Type{
				Id:        plan.Type_INT64,
				Nullable:  false,
				Width:     0,
				Precision: 0,
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
			Typ: &plan.Type{
				Id:        plan.Type_FLOAT64,
				Nullable:  false,
				Width:     0,
				Precision: 0,
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
			Typ: &plan.Type{
				Id:        plan.Type_VARCHAR,
				Nullable:  false,
				Width:     math.MaxInt32,
				Precision: 0,
			},
		}, nil
	default:
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("unsupport value: %v", val))
	}
}
