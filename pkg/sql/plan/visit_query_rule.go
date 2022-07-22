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
	"fmt"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
)

var (
	_ VisitRule = &GetParamRule{}
	_ VisitRule = &ResetParamOrderRule{}
	_ VisitRule = &ResetParamRefRule{}
	_ VisitRule = &ResetParamRefRule{}
)

type GetParamRule struct {
	params  map[int]int
	schemas []*plan.ObjectRef
}

func NewGetParamRule() *GetParamRule {
	return &GetParamRule{
		params: make(map[int]int),
	}
}

func (rule *GetParamRule) Match(node *Node) bool {
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
	return true
}

func (rule *GetParamRule) Apply(node *Node, _ *Query) error {
	if node.Limit != nil {
		node.Limit = rule.getParam(node.Limit)
	}

	if node.Offset != nil {
		node.Offset = rule.getParam(node.Offset)
	}

	for i := range node.OnList {
		node.OnList[i] = rule.getParam(node.OnList[i])
	}

	for i := range node.FilterList {
		node.FilterList[i] = rule.getParam(node.FilterList[i])
	}

	for i := range node.ProjectList {
		node.ProjectList[i] = rule.getParam(node.ProjectList[i])
	}

	return nil
}

func (rule *GetParamRule) getParam(e *plan.Expr) *plan.Expr {
	switch exprImpl := e.Expr.(type) {
	case *plan.Expr_F:
		for i := range exprImpl.F.Args {
			exprImpl.F.Args[i] = rule.getParam(exprImpl.F.Args[i])
		}
		return e
	case *plan.Expr_P:
		rule.params[int(exprImpl.P.Pos)] = 0
		return e
	default:
		return e
	}
}

func (rule *GetParamRule) SetParamOrder() {
	argPos := []int{}
	for pos := range rule.params {
		argPos = append(argPos, pos)
	}
	sort.Ints(argPos)
	for idx, pos := range argPos {
		rule.params[pos] = idx
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

func (rule *ResetParamOrderRule) Match(_ *Node) bool {
	return true
}

func (rule *ResetParamOrderRule) Apply(node *Node, qry *Query) error {

	if node.Limit != nil {
		node.Limit = rule.setOrder(node.Limit)
	}

	if node.Offset != nil {
		node.Offset = rule.setOrder(node.Offset)
	}

	for i := range node.OnList {
		node.OnList[i] = rule.setOrder(node.OnList[i])
	}

	for i := range node.FilterList {
		node.FilterList[i] = rule.setOrder(node.FilterList[i])
	}

	for i := range node.ProjectList {
		node.ProjectList[i] = rule.setOrder(node.ProjectList[i])
	}

	return nil
}

func (rule *ResetParamOrderRule) setOrder(e *plan.Expr) *plan.Expr {
	switch exprImpl := e.Expr.(type) {
	case *plan.Expr_F:
		for i := range exprImpl.F.Args {
			exprImpl.F.Args[i] = rule.setOrder(exprImpl.F.Args[i])
		}
		return e
	case *plan.Expr_P:
		exprImpl.P.Pos = int32(rule.params[int(exprImpl.P.Pos)])
		return e
	default:
		return e
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

func (rule *ResetParamRefRule) Match(_ *Node) bool {
	return true
}

func (rule *ResetParamRefRule) Apply(node *Node, qry *Query) error {
	if node.Limit != nil {
		node.Limit = rule.resetParamRef(node.Limit)
	}

	if node.Offset != nil {
		node.Offset = rule.resetParamRef(node.Offset)
	}

	for i := range node.OnList {
		node.OnList[i] = rule.resetParamRef(node.OnList[i])
	}

	for i := range node.FilterList {
		node.FilterList[i] = rule.resetParamRef(node.FilterList[i])
	}

	for i := range node.ProjectList {
		node.ProjectList[i] = rule.resetParamRef(node.ProjectList[i])
	}

	return nil
}

func (rule *ResetParamRefRule) resetParamRef(e *plan.Expr) *plan.Expr {
	switch exprImpl := e.Expr.(type) {
	case *plan.Expr_F:
		needResetFunction := false
		for i, arg := range exprImpl.F.Args {
			if _, ok := arg.Expr.(*plan.Expr_P); ok {
				needResetFunction = true
			}
			exprImpl.F.Args[i] = rule.resetParamRef(arg)
		}

		// reset function
		if needResetFunction {
			newExpr, _ := bindFuncExprImplByPlanExpr(exprImpl.F.Func.GetObjName(), exprImpl.F.Args)
			return newExpr
		}
		return e
	case *plan.Expr_P:
		return rule.params[int(exprImpl.P.Pos)]
	default:
		return e
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

func (rule *ResetVarRefRule) Match(_ *Node) bool {
	return true
}

func (rule *ResetVarRefRule) Apply(node *Node, qry *Query) error {
	var err error
	if node.Limit != nil {
		node.Limit, err = rule.resetVarRef(node.Limit)
	}
	if err != nil {
		return err
	}

	if node.Offset != nil {
		node.Offset, err = rule.resetVarRef(node.Offset)
	}
	if err != nil {
		return err
	}

	for i := range node.OnList {
		node.OnList[i], err = rule.resetVarRef(node.OnList[i])
	}
	if err != nil {
		return err
	}

	for i := range node.FilterList {
		node.FilterList[i], err = rule.resetVarRef(node.FilterList[i])
	}
	if err != nil {
		return err
	}

	for i := range node.ProjectList {
		node.ProjectList[i], err = rule.resetVarRef(node.ProjectList[i])
	}
	if err != nil {
		return err
	}
	return nil
}

func (rule *ResetVarRefRule) resetVarRef(e *plan.Expr) (*plan.Expr, error) {
	var err error
	switch exprImpl := e.Expr.(type) {
	case *plan.Expr_F:
		needResetFunction := false
		for i, arg := range exprImpl.F.Args {
			if _, ok := arg.Expr.(*plan.Expr_V); ok {
				needResetFunction = true
			}
			exprImpl.F.Args[i], err = rule.resetVarRef(arg)
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
			expr = makePlan2Float64ConstExprWithType(float64(val))
		case float64:
			expr = makePlan2Float64ConstExprWithType(val)
		case bool:
			expr = makePlan2BoolConstExprWithType(val)
		case nil:
			expr = makePlan2NullConstExprWithType()
		case types.Decimal64, types.Decimal128:
			err = errors.New("", "decimal var not support now")
		default:
			err = errors.New("", fmt.Sprintf("type of var %q is not supported now", exprImpl.V.Name))
		}
		return expr, err
	default:
		return e, nil
	}
}
