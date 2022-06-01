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

func (b *baseBinder) splitAndBindCondition(astExpr tree.Expr, ctx *BindContext) ([]*plan.Expr, error) {
	conds := splitConjunctiveCondition(astExpr)
	exprs := make([]*plan.Expr, len(conds))

	for i, cond := range conds {
		alias := tree.String(cond, dialect.MYSQL)
		expr, err := b.bindExprImpl(alias, cond, ctx)
		if err != nil {
			return nil, err
		}
		exprs[i] = expr
	}

	return exprs, nil
}

func (b *baseBinder) bindExprImpl(alias string, astExpr tree.Expr, ctx *BindContext) (expr *Expr, err error) {
	switch astExpr := astExpr.(type) {
	case *tree.NumVal:
		expr, err = b.bindNumVal(astExpr)
	case *tree.ParenExpr:
		expr, err = b.BindExpr(alias, astExpr.Expr, ctx)
	case *tree.OrExpr:
		expr, err = b.bindFuncExprImplByAstExpr("or", []tree.Expr{astExpr.Left, astExpr.Right}, ctx)
	case *tree.NotExpr:
		expr, err = b.bindFuncExprImplByAstExpr("not", []tree.Expr{astExpr.Expr}, ctx)
	case *tree.AndExpr:
		expr, err = b.bindFuncExprImplByAstExpr("and", []tree.Expr{astExpr.Left, astExpr.Right}, ctx)
	case *tree.UnaryExpr:
		expr, err = b.bindUnaryExpr(astExpr, ctx)
	case *tree.BinaryExpr:
		expr, err = b.bindBinaryExpr(astExpr, ctx)
	case *tree.ComparisonExpr:
		expr, err = b.bindComparisonExpr(astExpr, ctx)
	case *tree.FuncExpr:
		expr, err = b.bindFuncExpr(alias, astExpr, ctx)
	case *tree.RangeCond:
		expr, err = b.bindRangeCond(astExpr, ctx)
	case *tree.UnresolvedName:
		expr, err = b.BindColRef(alias, astExpr, ctx)
	case *tree.CastExpr:
		expr, err = b.BindExpr(tree.String(astExpr.Expr, dialect.MYSQL), astExpr.Expr, ctx)
		if err != nil {
			return
		}
		var typ *Type
		typ, err = getTypeFromAst(astExpr.Type)
		if err != nil {
			return
		}
		expr, err = appendCastExpr(expr, typ)
	case *tree.IsNullExpr:
		expr, err = b.bindFuncExprImplByAstExpr("ifnull", []tree.Expr{astExpr.Expr}, ctx)
	case *tree.IsNotNullExpr:
		expr, err = b.bindFuncExprImplByAstExpr("ifnull", []tree.Expr{astExpr.Expr}, ctx)
		if err != nil {
			return
		}
		expr, err = b.bindFuncExprImplByPlanExpr("not", []*Expr{expr}, ctx)
	case *tree.Tuple:
		exprs := make([]*Expr, 0, len(astExpr.Exprs))
		var planItem *Expr
		for _, astItem := range astExpr.Exprs {
			planItem, err = b.BindExpr(tree.String(astItem, dialect.MYSQL), astItem, ctx)
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
		expr, err = b.bindCaseExpr(astExpr, ctx)
	case *tree.IntervalExpr:
		// parser will not return this type
		// return directly?
		err = errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("expr interval'%v' is not support now", astExpr))
	case *tree.XorExpr:
		// return directly?
		err = errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("expr xor'%v' is not support now", astExpr))
	case *tree.Subquery:
		// TODO
		//
	case *tree.DefaultVal:
		// return directly?
		err = errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("expr default'%v' is not support now", astExpr))
	case *tree.MaxValue:
		// return directly?
		err = errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("expr max'%v' is not support now", astExpr))
	case *tree.VarExpr:
		// return directly?
		err = errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("expr var'%v' is not support now", astExpr))
	case *tree.StrVal:
		// return directly?
		err = errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("expr str'%v' is not support now", astExpr))
	case *tree.ExprList:
		// return directly?
		err = errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("expr plan.ExprList'%v' is not support now", astExpr))
	case tree.UnqualifiedStar:
		// select * from table
		// TODO
	default:
		// return directly?
		err = errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("expr '%+v' is not support now", astExpr))
	}

	parent := ctx.parent
	depth := 0
	dummyErr := err
	for dummyErr != nil && parent != nil && parent.binder != nil {
		expr, dummyErr = parent.binder.BindExpr(alias, astExpr, parent)
		depth++
		parent = parent.parent
	}

	if dummyErr != nil {
		return nil, err
	}
	if depth > 0 {
		ctx.addCorrCol(expr)
		return expr, nil
	}

	return expr, nil
}

func (b *baseBinder) bindCaseExpr(astExpr *tree.CaseExpr, ctx *BindContext) (*Expr, error) {
	var caseExpr *Expr
	var elseExpr *Expr
	var err error

	if astExpr.Expr != nil {
		caseExpr, err = b.BindExpr(tree.String(astExpr.Expr, dialect.MYSQL), astExpr.Expr, ctx)
		if err != nil {
			return nil, err
		}
	}
	if astExpr.Else != nil {
		elseExpr, err = b.BindExpr(tree.String(astExpr.Else, dialect.MYSQL), astExpr.Else, ctx)
		if err != nil {
			return nil, err
		}
	}

	whenList := make([]*Expr, len(astExpr.Whens))
	for idx, whenExpr := range astExpr.Whens {
		exprs := make([]*Expr, 2)
		condExpr, err := b.BindExpr(tree.String(whenExpr.Cond, dialect.MYSQL), whenExpr.Cond, ctx)
		if err != nil {
			return nil, err
		}
		if caseExpr == nil {
			// rewrite "case col when 1 then '1' else '2'" to "case when col=1 then '1' else '2'"
			exprs[0], err = b.bindFuncExprImplByPlanExpr("=", []*Expr{caseExpr, condExpr}, ctx)
			if err != nil {
				return nil, err
			}
		} else {
			exprs[0] = condExpr
		}
		valExpr, err := b.BindExpr(tree.String(whenExpr.Val, dialect.MYSQL), whenExpr.Val, ctx)
		if err != nil {
			return nil, err
		}
		exprs[1] = valExpr

		whenList[idx] = &Expr{
			Expr: &plan.Expr_List{
				List: &plan.ExprList{
					List: exprs,
				},
			},
			Typ: &plan.Type{
				Id: plan.Type_TUPLE,
			},
		}
	}

	whenExpr := &Expr{
		Expr: &plan.Expr_List{
			List: &plan.ExprList{
				List: whenList,
			},
		},
		Typ: &plan.Type{
			Id: plan.Type_TUPLE,
		},
	}

	//we will use case(whenlist, else)
	if elseExpr == nil {
		// TODO how to deal with nil arg?
		return b.bindFuncExprImplByPlanExpr("case", []*Expr{whenExpr}, ctx)
	} else {
		return b.bindFuncExprImplByPlanExpr("case", []*Expr{whenExpr, elseExpr}, ctx)
	}
}

func (b *baseBinder) bindRangeCond(astExpr *tree.RangeCond, ctx *BindContext) (*Expr, error) {
	leftExpr, err := b.BindExpr(tree.String(astExpr.Left, dialect.MYSQL), astExpr.Left, ctx)
	if err != nil {
		return nil, err
	}
	fromExpr, err := b.BindExpr(tree.String(astExpr.From, dialect.MYSQL), astExpr.From, ctx)
	if err != nil {
		return nil, err
	}
	toExpr, err := b.BindExpr(tree.String(astExpr.To, dialect.MYSQL), astExpr.To, ctx)
	if err != nil {
		return nil, err
	}

	if astExpr.Not {
		left, err := b.bindFuncExprImplByPlanExpr("<", []*Expr{leftExpr, fromExpr}, ctx)
		if err != nil {
			return nil, err
		}
		right, err := b.bindFuncExprImplByPlanExpr(">", []*Expr{leftExpr, toExpr}, ctx)
		if err != nil {
			return nil, err
		}
		return b.bindFuncExprImplByPlanExpr("or", []*Expr{left, right}, ctx)
	} else {
		left, err := b.bindFuncExprImplByPlanExpr(">=", []*Expr{leftExpr, fromExpr}, ctx)
		if err != nil {
			return nil, err
		}
		right, err := b.bindFuncExprImplByPlanExpr("<=", []*Expr{leftExpr, toExpr}, ctx)
		if err != nil {
			return nil, err
		}
		return b.bindFuncExprImplByPlanExpr("and", []*Expr{left, right}, ctx)
	}
}

func (b *baseBinder) bindUnaryExpr(astExpr *tree.UnaryExpr, ctx *BindContext) (*Expr, error) {
	switch astExpr.Op {
	case tree.UNARY_MINUS:
		return b.bindFuncExprImplByAstExpr("unary_minus", []tree.Expr{astExpr.Expr}, ctx)
	case tree.UNARY_PLUS:
		return b.bindFuncExprImplByAstExpr("unary_plus", []tree.Expr{astExpr.Expr}, ctx)
	case tree.UNARY_TILDE:
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", astExpr))
	case tree.UNARY_MARK:
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", astExpr))
	}
	return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", astExpr))
}

func (b *baseBinder) bindBinaryExpr(astExpr *tree.BinaryExpr, ctx *BindContext) (*Expr, error) {
	switch astExpr.Op {
	case tree.PLUS:
		return b.bindFuncExprImplByAstExpr("+", []tree.Expr{astExpr.Left, astExpr.Right}, ctx)
	case tree.MINUS:
		return b.bindFuncExprImplByAstExpr("-", []tree.Expr{astExpr.Left, astExpr.Right}, ctx)
	case tree.MULTI:
		return b.bindFuncExprImplByAstExpr("*", []tree.Expr{astExpr.Left, astExpr.Right}, ctx)
	case tree.MOD:
		return b.bindFuncExprImplByAstExpr("%", []tree.Expr{astExpr.Left, astExpr.Right}, ctx)
	case tree.DIV:
		return b.bindFuncExprImplByAstExpr("/", []tree.Expr{astExpr.Left, astExpr.Right}, ctx)
	case tree.INTEGER_DIV:
		return b.bindFuncExprImplByAstExpr("integer_div", []tree.Expr{astExpr.Left, astExpr.Right}, ctx)
	}
	return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", astExpr))
}

func (b *baseBinder) bindComparisonExpr(astExpr *tree.ComparisonExpr, ctx *BindContext) (*Expr, error) {
	switch astExpr.Op {
	case tree.EQUAL:
		return b.bindFuncExprImplByAstExpr("=", []tree.Expr{astExpr.Left, astExpr.Right}, ctx)
	case tree.LESS_THAN:
		return b.bindFuncExprImplByAstExpr("<", []tree.Expr{astExpr.Left, astExpr.Right}, ctx)
	case tree.LESS_THAN_EQUAL:
		return b.bindFuncExprImplByAstExpr("<=", []tree.Expr{astExpr.Left, astExpr.Right}, ctx)
	case tree.GREAT_THAN:
		return b.bindFuncExprImplByAstExpr(">", []tree.Expr{astExpr.Left, astExpr.Right}, ctx)
	case tree.GREAT_THAN_EQUAL:
		return b.bindFuncExprImplByAstExpr(">=", []tree.Expr{astExpr.Left, astExpr.Right}, ctx)
	case tree.NOT_EQUAL:
		return b.bindFuncExprImplByAstExpr("<>", []tree.Expr{astExpr.Left, astExpr.Right}, ctx)
	case tree.LIKE:
		return b.bindFuncExprImplByAstExpr("like", []tree.Expr{astExpr.Left, astExpr.Right}, ctx)
	case tree.NOT_LIKE:
		expr, err := b.bindFuncExprImplByAstExpr("like", []tree.Expr{astExpr.Left, astExpr.Right}, ctx)
		if err != nil {
			return nil, err
		}
		return b.bindFuncExprImplByPlanExpr("not", []*Expr{expr}, ctx)
	case tree.IN:
		return b.bindFuncExprImplByAstExpr("in", []tree.Expr{astExpr.Left, astExpr.Right}, ctx)
	case tree.NOT_IN:
		expr, err := b.bindFuncExprImplByAstExpr("in", []tree.Expr{astExpr.Left, astExpr.Right}, ctx)
		if err != nil {
			return nil, err
		}
		return b.bindFuncExprImplByPlanExpr("not", []*Expr{expr}, ctx)
	}
	return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", astExpr))
}

func (b *baseBinder) bindFuncExpr(alias string, astExpr *tree.FuncExpr, ctx *BindContext) (*Expr, error) {
	funcRef, ok := astExpr.Func.FunctionReference.(*tree.UnresolvedName)
	if !ok {
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("function expr '%v' is not support now", astExpr))
	}
	funcName := funcRef.Parts[0]

	if isAggFunc(funcName) {
		return b.BindAggFunc(alias, funcName, astExpr, ctx)
	} else if isWinFunc(funcName) {
		return b.BindWinFunc(alias, funcName, astExpr, ctx)
	}

	return b.bindFuncExprImplByAstExpr(funcName, astExpr.Exprs, ctx)
}

func (b *baseBinder) bindFuncExprImplByAstExpr(name string, args []tree.Expr, ctx *BindContext) (*plan.Expr, error) {
	// rewrite some ast Exprs before binding
	switch name {
	case "extract":
		// ”extract(year from col_name)"  parser return year as UnresolvedName.
		// we must rewrite it to string。 because binder bind UnresolvedName as column name
		unit := args[0].(*tree.UnresolvedName).Parts[0]
		args[0] = tree.NewNumVal(constant.MakeString(unit), unit, false)
	case "count":
		// we will rewrite "count(*)" to "starcount(col)"
		// count(*) : astExprs[0].(type) is *tree.NumVal
		// count(col_name) : astExprs[0].(type) is *tree.UnresolvedName
		switch args[0].(type) {
		case *tree.NumVal:
			// rewrite count(*) to starcount(col_name)
			name = "starcount"
			if len(ctx.bindings) == 0 || len(ctx.bindings[0].cols) == 0 {
				return nil, errors.New(errno.InvalidColumnReference, "can not find any column when rewrite count(*) to starcount(col)")
			}
			var newCountCol *tree.UnresolvedName
			newCountCol, err := tree.NewUnresolvedName(ctx.bindings[0].cols[0])
			if err != nil {
				return nil, err
			}
			args[0] = newCountCol
		}
	}

	// bind ast function's args
	newArgs := make([]*Expr, len(args))
	for idx, arg := range args {
		alias := tree.String(arg, dialect.MYSQL)
		expr, err := b.BindExpr(alias, arg, ctx)
		if err != nil {
			return nil, err
		}
		newArgs[idx] = expr
	}

	return b.bindFuncExprImplByPlanExpr(name, newArgs, ctx)

}

func (b *baseBinder) bindFuncExprImplByPlanExpr(name string, args []*Expr, ctx *BindContext) (expr *Expr, err error) {
	// deal with some special function
	switch name {
	case "date":
		// rewrite date function to cast function, and retrun directly
		return appendCastExpr(args[0], &Type{
			Id: plan.Type_DATE,
		})
	case "interval":
		// rewrite interval function to cast function, and retrun directly
		return appendCastExpr(args[0], &plan.Type{
			Id: plan.Type_INTERVAL,
		})
	case "and", "or":
		// rewrite and/or funciton's args to bool
		// TODO , wangjian's code will conver expr to bool directly, but i don't think it is necessary
		for i := 0; i < 2; i++ {
			if args[i].Typ.Id != plan.Type_BOOL {
				arg, err := appendCastExpr(args[i], &plan.Type{
					Id: plan.Type_BOOL,
				})
				if err != nil {
					return nil, err
				}
				args[i] = arg
			}
		}
	case "=", "<", "<=", ">", ">=", "<>":
		// TODO , wangjian's code will conver arg to bool, but i don't think it is necessary
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
	case "+", "-":
		// rewrite "date '2001' + interval '1 day'" to date_add(date '2001', 1, day(unit))
		if len(args) != 2 {
			err = errors.New(errno.SyntaxErrororAccessRuleViolation, "operator function need two args")
			return
		}
		namesMap := map[string]string{
			"+": "date_add",
			"-": "date_sub",
		}
		if args[0].Typ.Id == plan.Type_DATE && args[1].Typ.Id == plan.Type_INTERVAL {
			args, err = resetDateFunctionArgs(args[0], args[1])
			if err != nil {
				return
			}
		}
		if args[0].Typ.Id == plan.Type_INTERVAL && args[1].Typ.Id == plan.Type_DATE {
			if name == "-" {
				err = errors.New(errno.SyntaxErrororAccessRuleViolation, "(interval - date) is no supported")
				return
			}
			args, err = resetDateFunctionArgs(args[1], args[2])
			if err != nil {
				return
			}
		}
		name = namesMap[name]
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
		return
	}
	if argsCastType != nil {
		if len(argsCastType) != argsLength {
			err = errors.New(errno.SyntaxErrororAccessRuleViolation, "cast types length not match args length")
			return
		}
		for idx, castType := range argsCastType {
			if argsType[idx] != castType {
				args[idx], err = appendCastExpr(args[idx], &plan.Type{
					Id: plan.Type_TypeId(castType),
				})
				if err != nil {
					return
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

func appendCastExpr(expr *Expr, toType *Type) (*Expr, error) {
	argsType := []types.T{
		types.T(expr.Typ.Id),
		types.T(toType.Id),
	}
	_, funcId, _, err := function.GetFunctionByName("cast", argsType)
	if err != nil {
		return nil, err
	}
	// FIX ME,  pipeline need two args for cast function
	// which kind of Expr will be used in second arg?
	return &Expr{
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: getFunctionObjRef(funcId, "cast"),
				Args: []*Expr{expr},
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

func isAggFunc(name string) bool {
	if funcSig, ok := BuiltinFunctionsMap[name]; ok {
		return funcSig.Flag == plan.Function_AGG
	}
	return false
}

func isWinFunc(name string) bool {
	if funcSig, ok := BuiltinFunctionsMap[name]; ok {
		return funcSig.Flag == plan.Function_WIN
	}
	return false
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
