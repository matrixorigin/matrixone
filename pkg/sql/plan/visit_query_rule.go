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
	_ Rule = &getParamRule{}
	_ Rule = &resetParamRule{}
)

type getParamRule struct {
	args map[int]int
}

func NewGetParamRule() getParamRule {
	return getParamRule{
		args: map[int]int{},
	}
}

func (rule *getParamRule) Match(_ *Node) bool {
	return true
}

func (rule *getParamRule) Apply(node *Node, _ *Query) {
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

func (rule *getParamRule) getParam(e *plan.Expr) *plan.Expr {
	switch exprImpl := e.Expr.(type) {
	case *plan.Expr_F:
		for i := range exprImpl.F.Args {
			exprImpl.F.Args[i] = rule.getParam(exprImpl.F.Args[i])
		}
		return e
	case *plan.Expr_P:
		rule.args[int(exprImpl.P.Pos)] = 0
		return e
	default:
		return e
	}
}

func (rule *getParamRule) SetParamOrder() {
	argPos := []int{}
	for pos := range rule.args {
		argPos = append(argPos, pos)
	}
	sort.Ints(argPos)
	for idx, pos := range argPos {
		rule.args[pos] = idx
	}
}

// ---------------------------

type resetParamRule struct {
	args map[int]int
}

func NewResetParamRule(args map[int]int) resetParamRule {
	return resetParamRule{
		args: args,
	}
}

func (rule *resetParamRule) Match(_ *Node) bool {
	return true
}

func (rule *resetParamRule) Apply(node *Node, qry *Query) {

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

func (rule *resetParamRule) setOrder(e *plan.Expr) *plan.Expr {
	switch exprImpl := e.Expr.(type) {
	case *plan.Expr_F:
		for i := range exprImpl.F.Args {
			exprImpl.F.Args[i] = rule.setOrder(exprImpl.F.Args[i])
		}
		return e
	case *plan.Expr_P:
		exprImpl.P.Pos = int32(rule.args[int(exprImpl.P.Pos)])
		return e
	default:
		return e
	}
}

// ---------------------------

type resetParamRefRule struct {
	args map[int]*Expr
}

func NewResetParamRefRule(args map[int]*Expr) resetParamRefRule {
	return resetParamRefRule{
		args: args,
	}
}

func (rule *resetParamRefRule) Match(_ *Node) bool {
	return true
}

func (rule *resetParamRefRule) Apply(node *Node, qry *Query) {
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

func (rule *resetParamRefRule) resetParamRef(e *plan.Expr) *plan.Expr {
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
		return rule.args[int(exprImpl.P.Pos)]
	default:
		return e
	}
}
