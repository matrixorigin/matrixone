// Copyright 2026 Matrix Origin
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

import "github.com/matrixorigin/matrixone/pkg/pb/plan"

// ivfMembershipRowFetchFilters selects whole deterministic MATCH conjuncts
// whose truth is already proved by the exact membership SEMI join. Scalar-only
// filters stay on row fetch. Neither the source nor its backing slice is changed.
func (builder *QueryBuilder) ivfMembershipRowFetchFilters(scan *plan.Node) (retained, removed []*plan.Expr) {
	for _, expr := range scan.FilterList {
		if exprCallsFunc(expr, "fulltext_match") {
			// A retained MATCH wrapper may rely on a bare MATCH to drive its score
			// stream. Preserve their complete access path when any conjunct is unsafe.
			if ContainsVolatileFunction(expr) || ContainsSequenceFunction(expr) {
				return scan.FilterList, nil
			}
			removed = append(removed, expr)
		} else {
			retained = append(retained, expr)
		}
	}
	if len(removed) == 0 {
		return scan.FilterList, nil
	}

	// The copied producer exports PK only; its SEMI join does not export scores.
	// An ancestor projection/order/window can still need the original binding's
	// score stream. Inspect before copying, conservatively retaining all MATCH
	// filters if any other query expression consumes MATCH, even on another table.
	guard := &ivfMatchConsumerGuard{}
	visitor := &VisitPlan{}
	visited := make(map[int32]bool)
	var inspect func(int32) bool
	inspect = func(id int32) bool {
		if id < 0 || int(id) >= len(builder.qry.Nodes) {
			return false
		}
		if visited[id] {
			return true
		}
		visited[id] = true
		node := builder.qry.Nodes[id]
		if node == nil {
			return false
		}
		nodeCopy := *node
		if node == scan {
			nodeCopy.FilterList = nil
		}
		if err := visitor.exploreNode(builder.GetContext(), guard, &nodeCopy, id); err != nil || guard.found {
			return false
		}
		// These executable fields are outside the generic visitor's coverage.
		for _, expr := range node.FillVal {
			guard.inspect(expr)
		}
		for _, expr := range []*plan.Expr{node.Interval, node.Sliding, node.Timestamp, node.WEnd} {
			guard.inspect(expr)
		}
		if guard.found {
			return false
		}
		for _, child := range node.Children {
			if !inspect(child) {
				return false
			}
		}
		return true
	}
	for _, root := range builder.qry.Steps {
		if !inspect(root) {
			return scan.FilterList, nil
		}
	}
	// A detached/internal caller cannot prove the absence of ancestor consumers.
	if !visited[scan.NodeId] {
		return scan.FilterList, nil
	}
	return retained, removed
}

type ivfMatchConsumerGuard struct{ found bool }

func (*ivfMatchConsumerGuard) MatchNode(*plan.Node) bool  { return false }
func (*ivfMatchConsumerGuard) IsApplyExpr() bool          { return true }
func (*ivfMatchConsumerGuard) ApplyNode(*plan.Node) error { return nil }
func (guard *ivfMatchConsumerGuard) ApplyExpr(expr *plan.Expr) (*plan.Expr, error) {
	guard.inspect(expr)
	return expr, nil
}
func (guard *ivfMatchConsumerGuard) inspect(expr *plan.Expr) {
	guard.found = guard.found || exprCallsFunc(expr, "fulltext_match") || exprCallsFunc(expr, "fulltext_match_score")
}
