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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan2/function"
)

func getFunctionExprByNameAndPlanExprs(name string, distinct bool, exprs []*Expr) (resultExpr *Expr, isAgg bool, err error) {
	// deal with special function
	switch name {
	case "+":
		if len(exprs) != 2 {
			return nil, false, errors.New(errno.SyntaxErrororAccessRuleViolation, "operator function need two args")
		}
		if exprs[0].Typ.Id == plan.Type_DATE && exprs[1].Typ.Id == plan.Type_INTERVAL {
			resultExpr, err = getIntervalFunction(name, exprs[0], exprs[1])
			return
		}
		if exprs[0].Typ.Id == plan.Type_INTERVAL && exprs[1].Typ.Id == plan.Type_DATE {
			resultExpr, err = getIntervalFunction(name, exprs[1], exprs[0])
			return
		}
		if exprs[0].Typ.Id == plan.Type_DATETIME && exprs[1].Typ.Id == plan.Type_INTERVAL {
			resultExpr, err = getIntervalFunction(name, exprs[0], exprs[1])
			return
		}
		if exprs[0].Typ.Id == plan.Type_INTERVAL && exprs[1].Typ.Id == plan.Type_DATETIME {
			resultExpr, err = getIntervalFunction(name, exprs[1], exprs[0])
			return
		}
		if exprs[0].Typ.Id == plan.Type_VARCHAR && exprs[1].Typ.Id == plan.Type_INTERVAL {
			resultExpr, err = getIntervalFunction(name, exprs[0], exprs[1])
			return
		}
		if exprs[0].Typ.Id == plan.Type_INTERVAL && exprs[1].Typ.Id == plan.Type_VARCHAR {
			resultExpr, err = getIntervalFunction(name, exprs[1], exprs[0])
			return
		}
	case "-":
		if exprs[0].Typ.Id == plan.Type_DATE && exprs[1].Typ.Id == plan.Type_INTERVAL {
			resultExpr, err = getIntervalFunction(name, exprs[0], exprs[1])
			return
		}
		if exprs[0].Typ.Id == plan.Type_DATETIME && exprs[1].Typ.Id == plan.Type_INTERVAL {
			resultExpr, err = getIntervalFunction(name, exprs[0], exprs[1])
			return
		}
		if exprs[0].Typ.Id == plan.Type_VARCHAR && exprs[1].Typ.Id == plan.Type_INTERVAL {
			resultExpr, err = getIntervalFunction(name, exprs[0], exprs[1])
			return
		}

	case "and", "or", "not", "xor":
		if err := convertValueIntoBool(name, exprs, true); err != nil {
			return nil, false, err
		}
	case "=", "<", "<=", ">", ">=", "<>":
		if err := convertValueIntoBool(name, exprs, false); err != nil {
			return nil, false, err
		}
	}

	// get args(exprs) & types
	argsLength := len(exprs)
	argsType := make([]types.T, argsLength)
	for idx, expr := range exprs {
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
				exprs[idx], err = appendCastExpr(exprs[idx], &plan.Type{
					Id: plan.Type_TypeId(castType),
				})
				if err != nil {
					return
				}
			}
		}
	}

	// return new expr
	returnType := &Type{
		Id: plan.Type_TypeId(funcDef.ReturnTyp),
	}
	resultExpr = &Expr{
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: getFunctionObjRef(funcId, name),
				Args: exprs,
			},
		},
		Typ: returnType,
	}
	isAgg = funcDef.IsAggregate()
	if isAgg && distinct {
		fe := resultExpr.Expr.(*plan.Expr_F)
		fe.F.Func.Obj = int64(uint64(fe.F.Func.Obj) | function.Distinct)
	}
	return
}

func rewriteStarToCol(query *Query, node *Node) (string, error) {
	if node.NodeType == plan.Node_TABLE_SCAN {
		return node.TableDef.Cols[0].Name, nil
	} else {
		for _, child := range node.Children {
			for _, col := range query.Nodes[child].ProjectList {
				return col.ColName, nil
			}
		}
	}
	return "", errors.New(errno.InvalidColumnReference, "can not find any column when rewrite count(*) to starcount(col)")
}

func getFunctionExprByNameAndAstExprs(name string, distinct bool, astExprs []tree.Expr, ctx CompilerContext, query *Query, node *Node, binderCtx *BinderContext, needAgg bool) (resultExpr *Expr, isAgg bool, err error) {
	// name = strings.ToLower(name)
	args := make([]*Expr, len(astExprs))
	// deal with special function [rewrite some ast function expr]
	switch name {
	//case "extract":
	//	// rewrite args[0]
	//	kindExpr := astExprs[0].(*tree.UnresolvedName)
	//	astExprs[0] = tree.NewNumValWithType(constant.MakeString(kindExpr.Parts[0]), kindExpr.Parts[0], false, tree.P_char)
	case "count":
		// count(*) : astExprs[0].(type) is *tree.NumVal
		// count(col_name) : astExprs[0].(type) is *tree.UnresolvedName
		switch astExprs[0].(type) {
		case *tree.NumVal:
			// rewrite count(*) to starcount(col_name)
			name = "starcount"
			var countColName string
			countColName, err = rewriteStarToCol(query, node)
			if err != nil {
				return
			}
			var newCountCol *tree.UnresolvedName
			newCountCol, err = tree.NewUnresolvedName(countColName)
			if err != nil {
				return
			}
			astExprs[0] = newCountCol
		}
	}

	isAgg = true

	// get args
	var expr *Expr
	var paramIsAgg bool
	for idx, astExpr := range astExprs {
		expr, paramIsAgg, err = buildExpr(astExpr, ctx, query, node, binderCtx, needAgg)
		if err != nil {
			return
		}
		isAgg = isAgg && paramIsAgg
		args[idx] = expr
	}

	// deal with special function
	switch name {
	case "date":
		resultExpr, err = appendCastExpr(args[0], &plan.Type{
			Id: plan.Type_DATE,
		})
		return
	case "interval":
		resultExpr, err = appendCastExpr(args[0], &plan.Type{
			Id: plan.Type_INTERVAL,
		})
		return
	case "date_add", "date_sub":
		if len(args) != 2 {
			return nil, false, errors.New(errno.SyntaxErrororAccessRuleViolation, "date_add/date_sub function need two args")
		}
		args, err = resetIntervalFunctionExprs(args[0], args[1])
		if err != nil {
			return
		}
	}

	resultExpr, paramIsAgg, err = getFunctionExprByNameAndPlanExprs(name, distinct, args)
	if paramIsAgg {
		node.AggList = append(node.AggList, resultExpr)
		resultExpr = &Expr{
			Typ: resultExpr.Typ,
			Expr: &plan.Expr_Col{
				Col: &ColRef{
					RelPos: -2,
					ColPos: int32(len(node.AggList) - 1),
				},
			},
		}
		isAgg = true
	}
	return
}

func appendCastExpr(expr *Expr, toType *Type) (*Expr, error) {
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

func getFunctionObjRef(funcId int64, name string) *ObjectRef {
	return &ObjectRef{
		Obj:     funcId,
		ObjName: name,
	}
}

func resetIntervalFunctionExprs(dateExpr *Expr, intervalExpr *Expr) ([]*Expr, error) {
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

	// rewrite "date '2020-10-10' - interval 1 Hour" to date_add(datetime, 1, hour)
	if dateExpr.Typ.Id == plan.Type_DATE {
		switch returnType {
		case types.Day, types.Week, types.Month, types.Quarter, types.Year:
		default:
			dateExpr, err = appendCastExpr(dateExpr, &plan.Type{
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

func getIntervalFunction(name string, dateExpr *Expr, intervalExpr *Expr) (*Expr, error) {
	exprs, err := resetIntervalFunctionExprs(dateExpr, intervalExpr)
	if err != nil {
		return nil, err
	}

	// only support date operator now
	namesMap := map[string]string{
		"+": "date_add",
		"-": "date_sub",
	}
	resultExpr, _, err := getFunctionExprByNameAndPlanExprs(namesMap[name], false, exprs)

	return resultExpr, err
}

func convertValueIntoBool(name string, args []*Expr, isLogic bool) error {
	if !isLogic && (len(args) != 2 || (args[0].Typ.Id != plan.Type_BOOL && args[1].Typ.Id != plan.Type_BOOL)) {
		return nil
	}
	for _, arg := range args {
		if arg.Typ.Id == plan.Type_BOOL {
			continue
		}
		switch ex := arg.Expr.(type) {
		case *plan.Expr_C:
			arg.Typ.Id = plan.Type_BOOL
			switch value := ex.C.Value.(type) {
			case *plan.Const_Ival:
				if value.Ival == 0 {
					ex.C.Value = &plan.Const_Bval{Bval: false}
				} else if value.Ival == 1 {
					ex.C.Value = &plan.Const_Bval{Bval: true}
				} else {
					return errors.New("", fmt.Sprintf("Can't cast '%v' as boolean type.", value.Ival))
				}
			}
		}
	}
	return nil
}
