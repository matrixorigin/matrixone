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

	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan2/rule"
)

var defaultRules = map[plan.Node_NodeType][]Rule{}

func init() {
	defaultRules[plan.Node_TABLE_SCAN] = []Rule{
		rule.NewConstantFlod(),
	}
	defaultRules[plan.Node_PROJECT] = []Rule{
		rule.NewConstantFlod(),
	}
	defaultRules[plan.Node_AGG] = []Rule{
		rule.NewConstantFlod(),
	}
	defaultRules[plan.Node_JOIN] = []Rule{
		rule.NewConstantFlod(),
	}
	defaultRules[plan.Node_SORT] = []Rule{
		rule.NewConstantFlod(),
	}
}

func NewBaseOptimizr(ctx CompilerContext) *BaseOptimizer {
	return &BaseOptimizer{
		ctx:   ctx,
		rules: defaultRules,
	}
}

func (opt *BaseOptimizer) CurrentContext() CompilerContext {
	return opt.ctx
}

func (opt *BaseOptimizer) Optimize(stmt tree.Statement) (*Query, error) {
	pn, err := BuildPlan(opt.ctx, stmt)
	if err != nil {
		return nil, err
	}
	qry, ok := pn.Plan.(*plan.Plan_Query)
	if !ok {
		panic(errors.New(errno.SyntaxErrororAccessRuleViolation, pn.String()))
	}
	return opt.optimize(qry.Query)
}

func (opt *BaseOptimizer) optimize(qry *Query) (*Query, error) {
	if len(qry.Steps) != 1 {
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("query '%s' not support now", qry))
	}
	n := qry.Nodes[qry.Steps[0]]
	opt.exploreNode(n)
	return qry, nil
}

func (opt *BaseOptimizer) exploreNode(n *Node) {
	rules, ok := opt.rules[n.NodeType]
	if !ok {
		return
	}
	for _, rule := range rules {
		if rule.Match(n) {
			rule.Apply(n, opt.qry)
		}
	}
	for i := range n.Children {
		opt.exploreNode(opt.qry.Nodes[n.Children[i]])
	}
}
