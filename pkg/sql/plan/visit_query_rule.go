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
	_ Rule = &getArgRule{}
	_ Rule = &resetArgRule{}
)

type getArgRule struct {
	args map[int]int
}

func NewGetArgRule() getArgRule {
	return getArgRule{
		args: map[int]int{},
	}
}

func (rule *getArgRule) Match(_ *Node) bool {
	return true
}

func (rule *getArgRule) Apply(node *Node, _ *Query) {
	if node.Limit != nil {
		node.Limit = rule.getArg(node.Limit)
	}

	if node.Offset != nil {
		node.Offset = rule.getArg(node.Offset)
	}

	for i := range node.OnList {
		node.OnList[i] = rule.getArg(node.OnList[i])
	}

	for i := range node.FilterList {
		node.FilterList[i] = rule.getArg(node.FilterList[i])
	}

	for i := range node.ProjectList {
		node.ProjectList[i] = rule.getArg(node.ProjectList[i])
	}
}

func (rule *getArgRule) getArg(e *plan.Expr) *plan.Expr {
	switch exprImpl := e.Expr.(type) {
	case *plan.Expr_F:
		for i := range exprImpl.F.Args {
			exprImpl.F.Args[i] = rule.getArg(exprImpl.F.Args[i])
		}
		return e
	case *plan.Expr_P:
		rule.args[int(exprImpl.P.Pos)] = 0
		return e
	default:
		return e
	}
}

func (rule *getArgRule) SetArgOrder() {
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

type resetArgRule struct {
	args map[int]int
}

func NewResetArgRule(args map[int]int) getArgRule {
	return getArgRule{
		args: args,
	}
}

func (rule *resetArgRule) Match(_ *Node) bool {
	return true
}

func (rule *resetArgRule) Apply(node *Node, qry *Query) {

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

func (rule *resetArgRule) setOrder(e *plan.Expr) *plan.Expr {
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
