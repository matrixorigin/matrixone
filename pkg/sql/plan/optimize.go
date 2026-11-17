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

package plan

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
)

var defaultRules = []Rule{}
var prepareRules = []Rule{}

func init() {
	defaultRules = []Rule{
		rule.NewConstantFold(false),
		// rule.NewPredicatePushdown(),
	}
	prepareRules = []Rule{
		rule.NewConstantFold(true),
	}
}

func NewBaseOptimizer(ctx CompilerContext) *BaseOptimizer {
	return &BaseOptimizer{
		ctx:   ctx,
		rules: defaultRules,
	}
}

func NewPrepareOptimizer(ctx CompilerContext) *BaseOptimizer {
	return &BaseOptimizer{
		ctx:   ctx,
		rules: prepareRules,
	}
}

func (opt *BaseOptimizer) CurrentContext() CompilerContext {
	return opt.ctx
}

func (opt *BaseOptimizer) Optimize(stmt tree.Statement, isPrepareStmt bool) (*Query, error) {
	pn, err := BuildPlan(opt.ctx, stmt, isPrepareStmt)
	if err != nil {
		return nil, err
	}
	qry, ok := pn.Plan.(*plan.Plan_Query)
	if !ok {
		panic(moerr.NewInternalError(opt.ctx.GetContext(), pn.String()))
	}
	opt.qry = qry.Query
	return opt.optimize()
}

func (opt *BaseOptimizer) optimize() (*Query, error) {
	if len(opt.qry.Steps) == 0 {
		return opt.qry, nil
	}
	for _, step := range opt.qry.Steps {
		opt.exploreNode(opt.qry.Nodes[step])
	}

	opt.pruneUsedNodes(opt.qry)

	return opt.qry, nil
}

func (opt *BaseOptimizer) exploreNode(n *Node) {
	for i := range n.Children {
		opt.exploreNode(opt.qry.Nodes[n.Children[i]])
	}
	for _, rule := range opt.rules {
		if rule.Match(n) {
			rule.Apply(n, opt.qry, opt.ctx.GetProcess())
		}
	}
	// 域改写和 List/Vec 折叠完成后再去重，避免 reader 重复执行相同谓词。
	n.BlockFilterList = DeduplicateFinalBlockFilters(n.BlockFilterList)
}

// DeduplicateFinalBlockFilters 仅压缩本次新增准入的小 IN 重复项。
// 调用方若需保留缓存列表及 Fold 槽位，必须传入独立切片。
func DeduplicateFinalBlockFilters(filters []*plan.Expr) []*plan.Expr {
	if len(filters) < 2 {
		return filters
	}
	buckets := make(map[uint64][]*plan.Expr, len(filters))
	unique := filters[:0]
	for _, filter := range filters {
		// 不把此次修复变成通用表达式去重，也不散列原有大静态向量。
		if filter.GetF().GetFunc().GetObjName() != "in" || !isSimpleZoneMapFilter(filter) || !zoneMapExprIsStable(filter) {
			unique = append(unique, filter)
			continue
		}
		identity := finalBlockFilterIdentity(filter)
		hash := exprStructuralHash(identity)
		duplicate := false
		for _, previous := range buckets[hash] {
			if exprStructuralEqual(previous, identity) {
				duplicate = true
				break
			}
		}
		if !duplicate {
			buckets[hash] = append(buckets[hash], identity)
			unique = append(unique, filter)
		}
	}
	clear(filters[len(unique):])
	return unique
}

// Vec 的可执行类型、值和 NULL bitmap 都在 Data 中；外层 Typ 可能仍是
// 折叠前的 tuple，也可能是元素类型。只规范比较视图，不改 reader 的表达式。
func finalBlockFilterIdentity(expr *plan.Expr) *plan.Expr {
	switch e := expr.Expr.(type) {
	case *plan.Expr_Vec:
		return &plan.Expr{Expr: e}
	case *plan.Expr_F:
		copyExpr, fn := *expr, *e.F
		fn.Args = make([]*plan.Expr, len(e.F.Args))
		for i, arg := range e.F.Args {
			fn.Args[i] = finalBlockFilterIdentity(arg)
		}
		copyExpr.Expr = &plan.Expr_F{F: &fn}
		return &copyExpr
	case *plan.Expr_List:
		copyExpr := *expr
		items := make([]*plan.Expr, len(e.List.List))
		for i, item := range e.List.List {
			items[i] = finalBlockFilterIdentity(item)
		}
		copyExpr.Expr = &plan.Expr_List{List: &plan.ExprList{List: items}}
		return &copyExpr
	default:
		return expr
	}
}

func (opt *BaseOptimizer) pruneUsedNodes(qry *plan.Query) {
	newSteps := make([]int32, 0, len(qry.Steps))
	newNodes := make([]*plan.Node, 0, len(qry.Nodes))

	for _, step := range qry.Steps {
		newSteps = append(newSteps, opt.compactPlanTree(qry, step, &newNodes))
	}

	qry.Steps = newSteps
	qry.Nodes = newNodes
}

func (opt *BaseOptimizer) compactPlanTree(qry *plan.Query, nodeID int32, nodes *[]*plan.Node) int32 {
	node := qry.Nodes[nodeID]

	for i, childID := range node.Children {
		node.Children[i] = opt.compactPlanTree(qry, childID, nodes)
	}

	node.NodeId = int32(len(*nodes))
	*nodes = append(*nodes, node)

	return node.NodeId
}
