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
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
	"sort"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

var (
	_ VisitPlanRule = &GetParamRule{}
	_ VisitPlanRule = &ResetParamOrderRule{}
	_ VisitPlanRule = &ResetParamRefRule{}
	_ VisitPlanRule = &ResetVarRefRule{}
	_ VisitPlanRule = &ConstantFoldRule{}
)

var (
	constantFoldRule = rule.NewConstantFold(false)
)

type GetParamRule struct {
	params     map[int]int
	mapTypes   map[int]int32
	paramTypes []int32
	schemas    []*plan.ObjectRef
}

func NewGetParamRule() *GetParamRule {
	return &GetParamRule{
		params:   make(map[int]int),
		mapTypes: make(map[int]int32),
	}
}

func (rule *GetParamRule) MatchNode(node *Node) bool {
	if node.NodeType == plan.Node_TABLE_SCAN {
		rule.schemas = append(rule.schemas, &plan.ObjectRef{
			Server:     node.ObjRef.Server,
			Db:         node.ObjRef.Db,
			Schema:     node.ObjRef.Schema,
			Obj:        node.ObjRef.Obj,
			ServerName: node.ObjRef.ServerName,
			DbName:     node.ObjRef.DbName,
			SchemaName: node.ObjRef.SchemaName,
			ObjName:    node.ObjRef.ObjName,
		})
	}
	return false
}

func (rule *GetParamRule) IsApplyExpr() bool {
	return true
}

func (rule *GetParamRule) ApplyNode(node *Node) error {
	return nil
}

func (rule *GetParamRule) ApplyExpr(e *plan.Expr) (*plan.Expr, error) {
	switch exprImpl := e.Expr.(type) {
	case *plan.Expr_F:
		for i := range exprImpl.F.Args {
			exprImpl.F.Args[i], _ = rule.ApplyExpr(exprImpl.F.Args[i])
		}
		return e, nil
	case *plan.Expr_P:
		pos := int(exprImpl.P.Pos)
		rule.params[pos] = 0
		if e.Typ.Id == int32(types.T_any) && e.Typ.NotNullable {
			// is not null, use string
			rule.mapTypes[pos] = int32(types.T_varchar)
		} else {
			rule.mapTypes[pos] = e.Typ.Id
		}
		return e, nil
	default:
		return e, nil
	}
}

func (rule *GetParamRule) SetParamOrder() {
	argPos := []int{}
	for pos := range rule.params {
		argPos = append(argPos, pos)
	}
	sort.Ints(argPos)
	rule.paramTypes = make([]int32, len(argPos))

	for idx, pos := range argPos {
		rule.params[pos] = idx
		rule.paramTypes[idx] = rule.mapTypes[pos]
	}
}

// ---------------------------

type ResetParamOrderRule struct {
	params map[int]int
}

func NewResetParamOrderRule(params map[int]int) *ResetParamOrderRule {
	return &ResetParamOrderRule{
		params: params,
	}
}

func (rule *ResetParamOrderRule) MatchNode(_ *Node) bool {
	return false
}

func (rule *ResetParamOrderRule) IsApplyExpr() bool {
	return true
}

func (rule *ResetParamOrderRule) ApplyNode(node *Node) error {
	return nil
}

func (rule *ResetParamOrderRule) ApplyExpr(e *plan.Expr) (*plan.Expr, error) {
	switch exprImpl := e.Expr.(type) {
	case *plan.Expr_F:
		for i := range exprImpl.F.Args {
			exprImpl.F.Args[i], _ = rule.ApplyExpr(exprImpl.F.Args[i])
		}
		return e, nil
	case *plan.Expr_P:
		exprImpl.P.Pos = int32(rule.params[int(exprImpl.P.Pos)])
		return e, nil
	default:
		return e, nil
	}
}

// ---------------------------

type ResetParamRefRule struct {
	params []*Expr
}

func NewResetParamRefRule(params []*Expr) *ResetParamRefRule {
	return &ResetParamRefRule{
		params: params,
	}
}

func (rule *ResetParamRefRule) MatchNode(_ *Node) bool {
	return false
}

func (rule *ResetParamRefRule) IsApplyExpr() bool {
	return true
}

func (rule *ResetParamRefRule) ApplyNode(node *Node) error {
	return nil
}

func (rule *ResetParamRefRule) ApplyExpr(e *plan.Expr) (*plan.Expr, error) {
	var err error
	switch exprImpl := e.Expr.(type) {
	case *plan.Expr_F:
		needResetFunction := false
		for i, arg := range exprImpl.F.Args {
			if _, ok := arg.Expr.(*plan.Expr_P); ok {
				needResetFunction = true
			}
			exprImpl.F.Args[i], err = rule.ApplyExpr(arg)
			if err != nil {
				return nil, err
			}
		}

		// reset function
		if needResetFunction {
			return bindFuncExprImplByPlanExpr(exprImpl.F.Func.GetObjName(), exprImpl.F.Args)
		}
		return e, nil
	case *plan.Expr_P:
		return &plan.Expr{
			Typ:  e.Typ,
			Expr: rule.params[int(exprImpl.P.Pos)].Expr,
		}, nil
	default:
		return e, nil
	}
}

// ---------------------------

type ResetVarRefRule struct {
	compCtx CompilerContext
}

func NewResetVarRefRule(compCtx CompilerContext) *ResetVarRefRule {
	return &ResetVarRefRule{
		compCtx: compCtx,
	}
}

func (rule *ResetVarRefRule) MatchNode(_ *Node) bool {
	return false
}

func (rule *ResetVarRefRule) IsApplyExpr() bool {
	return true
}

func (rule *ResetVarRefRule) ApplyNode(node *Node) error {
	return nil
}

func (rule *ResetVarRefRule) ApplyExpr(e *plan.Expr) (*plan.Expr, error) {
	var err error
	switch exprImpl := e.Expr.(type) {
	case *plan.Expr_F:
		needResetFunction := false
		for i, arg := range exprImpl.F.Args {
			if _, ok := arg.Expr.(*plan.Expr_V); ok {
				needResetFunction = true
			}
			exprImpl.F.Args[i], err = rule.ApplyExpr(arg)
			if err != nil {
				return nil, err
			}
		}

		// reset function
		if needResetFunction {
			return bindFuncExprImplByPlanExpr(exprImpl.F.Func.GetObjName(), exprImpl.F.Args)
		}
		return e, nil
	case *plan.Expr_V:
		var getVal interface{}
		var expr *plan.Expr
		getVal, err = rule.compCtx.ResolveVariable(exprImpl.V.Name, exprImpl.V.System, exprImpl.V.Global)
		if err != nil {
			return nil, err
		}

		switch val := getVal.(type) {
		case string:
			expr = makePlan2StringConstExprWithType(val)
		case int:
			expr = makePlan2Int64ConstExprWithType(int64(val))
		case uint8:
			expr = makePlan2Int64ConstExprWithType(int64(val))
		case uint16:
			expr = makePlan2Int64ConstExprWithType(int64(val))
		case uint32:
			expr = makePlan2Int64ConstExprWithType(int64(val))
		case int8:
			expr = makePlan2Int64ConstExprWithType(int64(val))
		case int16:
			expr = makePlan2Int64ConstExprWithType(int64(val))
		case int32:
			expr = makePlan2Int64ConstExprWithType(int64(val))
		case int64:
			expr = makePlan2Int64ConstExprWithType(val)
		case uint64:
			expr = makePlan2Uint64ConstExprWithType(val)
		case float32:
			// when we build plan with constant in float, we cast them to decimal.
			// so we cast @float_var to decimal too.
			strVal := strconv.FormatFloat(float64(val), 'f', -1, 64)
			expr, err = makePlan2DecimalExprWithType(strVal)
		case float64:
			// when we build plan with constant in float, we cast them to decimal.
			// so we cast @float_var to decimal too.
			strVal := strconv.FormatFloat(val, 'f', -1, 64)
			expr, err = makePlan2DecimalExprWithType(strVal)
		case bool:
			expr = makePlan2BoolConstExprWithType(val)
		case nil:
			expr = makePlan2NullConstExprWithType()
		case types.Decimal64, types.Decimal128:
			err = moerr.NewNYINoCtx("decimal var")
		default:
			err = moerr.NewParseErrorNoCtx("type of var %q is not supported now", exprImpl.V.Name)
		}
		if e.Typ.Id != int32(types.T_any) && expr.Typ.Id != e.Typ.Id {
			return appendCastBeforeExpr(expr, e.Typ)
		}
		return expr, err
	default:
		return e, nil
	}
}

type ConstantFoldRule struct {
	compCtx CompilerContext
	rule    *rule.ConstantFold
}

func NewConstantFoldRule(compCtx CompilerContext) *ConstantFoldRule {
	return &ConstantFoldRule{
		compCtx: compCtx,
		rule:    constantFoldRule,
	}
}

func (r *ConstantFoldRule) MatchNode(node *Node) bool {
	return r.rule.Match(node)
}

func (r *ConstantFoldRule) IsApplyExpr() bool {
	return false
}

func (r *ConstantFoldRule) ApplyNode(node *Node) error {
	r.rule.Apply(node, nil, r.compCtx.GetProcess())
	return nil
}

func (r *ConstantFoldRule) ApplyExpr(e *plan.Expr) (*plan.Expr, error) {
	return e, nil
}
