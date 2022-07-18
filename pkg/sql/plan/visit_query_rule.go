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
	"sort"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

var (
	_ Rule = &GetParamRule{}
	_ Rule = &ResetParamOrderRule{}
	_ Rule = &ResetParamOrderRule{}
)

type GetParamRule struct {
	params  map[int]int
	schemas []*plan.ObjectRef
}

func NewGetParamRule() GetParamRule {
	return GetParamRule{
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

func (rule *GetParamRule) Apply(node *Node, _ *Query) {
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

func NewResetParamOrderRule(params map[int]int) ResetParamOrderRule {
	return ResetParamOrderRule{
		params: params,
	}
}

func (rule *ResetParamOrderRule) Match(_ *Node) bool {
	return true
}

func (rule *ResetParamOrderRule) Apply(node *Node, qry *Query) {

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

func NewResetParamRefRule(params []*Expr) ResetParamRefRule {
	return ResetParamRefRule{
		params: params,
	}
}

func (rule *ResetParamRefRule) Match(_ *Node) bool {
	return true
}

func (rule *ResetParamRefRule) Apply(node *Node, qry *Query) {
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
