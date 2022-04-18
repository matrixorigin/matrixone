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

//splitExprToAND split a expression to a list of AND conditions.
func splitExprToAND(expr tree.Expr) []*tree.Expr {
	var exprs []*tree.Expr

	switch typ := expr.(type) {
	case nil:
	case *tree.AndExpr:
		exprs = append(exprs, splitExprToAND(typ.Left)...)
		exprs = append(exprs, splitExprToAND(typ.Right)...)
	case *tree.ParenExpr:
		exprs = append(exprs, splitExprToAND(typ.Expr)...)
	default:
		exprs = append(exprs, &expr)
	}

	return exprs
}

//splitAndBuildExpr split expr to AND conditions first，and then build []*conditions to []*Expr
func splitAndBuildExpr(stmt tree.Expr, ctx CompilerContext, query *Query) ([]*plan.Expr, error) {
	var exprs []*plan.Expr

	conds := splitExprToAND(stmt)
	for _, cond := range conds {
		result_expr, err := buildExpr(*cond, ctx, query)
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, result_expr)
	}

	return exprs, nil
}

func getFunctionExprByNameAndExprs(name string, exprs []tree.Expr, ctx CompilerContext, query *Query) (*plan.Expr, error) {
	funObjRef, err := getFunctionObjRef(name)
	if err != nil {
		return nil, err
	}
	var args []*plan.Expr
	for _, astExpr := range exprs {
		expr, err := buildExpr(astExpr, ctx, query)
		if err != nil {
			return nil, err
		}
		args = append(args, expr)
	}
	return &plan.Expr{
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: funObjRef,
				Args: args,
			},
		},
	}, nil
}

func getFunctionObjRef(name string) (*plan.ObjectRef, error) {
	//todo need to check if function name exist
	return &plan.ObjectRef{
		ObjName: name,
	}, nil
}

//buildExpr
func buildExpr(stmt tree.Expr, ctx CompilerContext, query *Query) (*plan.Expr, error) {
	switch astExpr := stmt.(type) {
	case *tree.NumVal:
		return buildNumVal(astExpr.Value)
	case *tree.ParenExpr:
		return buildExpr(astExpr.Expr, ctx, query)
	case *tree.OrExpr:
		return getFunctionExprByNameAndExprs("OR", []tree.Expr{astExpr.Left, astExpr.Right}, ctx, query)
	case *tree.NotExpr:
		return getFunctionExprByNameAndExprs("NOT", []tree.Expr{astExpr.Expr}, ctx, query)
	case *tree.AndExpr:
		return getFunctionExprByNameAndExprs("AND", []tree.Expr{astExpr.Expr}, ctx, query)
	case *tree.UnaryExpr:
		return buildUnaryExpr(astExpr, ctx, query)
	case *tree.BinaryExpr:
		return buildBinaryExpr(astExpr, ctx, query)
	case *tree.ComparisonExpr:
		return buildComparisonExpr(astExpr, ctx, query)
	case *tree.FuncExpr:
		return buildFunctionExpr(astExpr, ctx, query)
	case *tree.RangeCond:
		return buildRangeCond(astExpr, ctx, query)
	case *tree.UnresolvedName:
		return buildUnresolvedName(astExpr, ctx, query)
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

func getColExprByFieldName(name string, tableDef *plan.TableDef) (*plan.Expr, error) {
	for idx, col := range tableDef.Cols {
		if col.Name == name {
			return &plan.Expr{
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						Name:   name,
						ColPos: int32(idx),
					},
				},
			}, nil
		}
	}

	return nil, errors.New(errno.InvalidColumnReference, fmt.Sprintf("column '%v' is not exist", name))
}

func buildUnresolvedName(expr *tree.UnresolvedName, ctx CompilerContext, query *Query) (*plan.Expr, error) {
	switch expr.NumParts {
	case 1:
		return getColExprByFieldName(expr.Parts[0], query.Nodes[len(query.Nodes)-1].TableDef)
	case 2:
		name := expr.Parts[1] + "." + expr.Parts[0]
		_, tableDef := ctx.Resolve(name)
		return getColExprByFieldName(expr.Parts[0], tableDef)
	case 3:
		//todo
	case 4:
		//todo
	}
	return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", expr))
}

func buildRangeCond(expr *tree.RangeCond, ctx CompilerContext, query *Query) (*plan.Expr, error) {
	if expr.Not {
		left, err := getFunctionExprByNameAndExprs("<", []tree.Expr{expr.Left, expr.From}, ctx, query)
		if err != nil {
			return nil, err
		}
		right, err := getFunctionExprByNameAndExprs(">", []tree.Expr{expr.Left, expr.To}, ctx, query)
		if err != nil {
			return nil, err
		}
		funObjRef, err := getFunctionObjRef("OR")
		return &plan.Expr{
			Expr: &plan.Expr_F{
				F: &plan.Function{
					Func: funObjRef,
					Args: []*plan.Expr{left, right},
				},
			},
		}, nil
	} else {
		left, err := getFunctionExprByNameAndExprs(">=", []tree.Expr{expr.Left, expr.From}, ctx, query)
		if err != nil {
			return nil, err
		}
		right, err := getFunctionExprByNameAndExprs("<=", []tree.Expr{expr.Left, expr.To}, ctx, query)
		if err != nil {
			return nil, err
		}
		funObjRef, err := getFunctionObjRef("AND")
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

func buildFunctionExpr(expr *tree.FuncExpr, ctx CompilerContext, query *Query) (*plan.Expr, error) {
	name, ok := expr.Func.FunctionReference.(*tree.UnresolvedName)
	if !ok {
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", expr))
	}
	funcName := strings.ToUpper(name.Parts[0])
	//todo  check if need map funcName to plan2's function name
	return getFunctionExprByNameAndExprs(funcName, expr.Exprs, ctx, query)
}

func buildComparisonExpr(expr *tree.ComparisonExpr, ctx CompilerContext, query *Query) (*plan.Expr, error) {
	switch expr.Op {
	case tree.EQUAL:
		return getFunctionExprByNameAndExprs("=", []tree.Expr{expr.Left, expr.Right}, ctx, query)
	case tree.LESS_THAN:
		return getFunctionExprByNameAndExprs("<", []tree.Expr{expr.Left, expr.Right}, ctx, query)
	case tree.LESS_THAN_EQUAL:
		return getFunctionExprByNameAndExprs("<=", []tree.Expr{expr.Left, expr.Right}, ctx, query)
	case tree.GREAT_THAN:
		return getFunctionExprByNameAndExprs(">", []tree.Expr{expr.Left, expr.Right}, ctx, query)
	case tree.GREAT_THAN_EQUAL:
		return getFunctionExprByNameAndExprs(">=", []tree.Expr{expr.Left, expr.Right}, ctx, query)
	case tree.NOT_EQUAL:
		return getFunctionExprByNameAndExprs("<>", []tree.Expr{expr.Left, expr.Right}, ctx, query)
	case tree.LIKE:
		return getFunctionExprByNameAndExprs("LIKE", []tree.Expr{expr.Left, expr.Right}, ctx, query)
	}
	return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", expr))
}

func buildUnaryExpr(expr *tree.UnaryExpr, ctx CompilerContext, query *Query) (*plan.Expr, error) {
	switch expr.Op {
	case tree.UNARY_MINUS:
		return getFunctionExprByNameAndExprs("UNARY_MINUS", []tree.Expr{expr.Expr}, ctx, query)
	case tree.UNARY_PLUS:
		return getFunctionExprByNameAndExprs("UNARY_PLUS", []tree.Expr{expr.Expr}, ctx, query)
	case tree.UNARY_TILDE:
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", expr))
	case tree.UNARY_MARK:
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", expr))
	}
	return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", expr))
}

func buildBinaryExpr(expr *tree.BinaryExpr, ctx CompilerContext, query *Query) (*plan.Expr, error) {
	switch expr.Op {
	case tree.PLUS:
		return getFunctionExprByNameAndExprs("+", []tree.Expr{expr.Left, expr.Right}, ctx, query)
	case tree.MINUS:
		return getFunctionExprByNameAndExprs("-", []tree.Expr{expr.Left, expr.Right}, ctx, query)
	case tree.MULTI:
		return getFunctionExprByNameAndExprs("*", []tree.Expr{expr.Left, expr.Right}, ctx, query)
	case tree.MOD:
		return getFunctionExprByNameAndExprs("%", []tree.Expr{expr.Left, expr.Right}, ctx, query)
	case tree.DIV:
		return getFunctionExprByNameAndExprs("/", []tree.Expr{expr.Left, expr.Right}, ctx, query)
	case tree.INTEGER_DIV:
		//todo confirm what is the difference from tree.DIV
		return getFunctionExprByNameAndExprs("/", []tree.Expr{expr.Left, expr.Right}, ctx, query)
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
