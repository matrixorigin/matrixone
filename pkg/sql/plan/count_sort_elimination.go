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

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// removeUnobservedCountSorts removes a direct, unbounded Sort input to a scalar
// COUNT. Projection cleanup runs first, so ordinary derived-table aliases need
// no separate order-demand propagation. Do not cross LIMIT, windows, joins or
// shared producers to search for more Sorts: their consumers have independent
// contracts. DISTINCT below the Sort remains intact.
func (builder *QueryBuilder) removeUnobservedCountSorts(rootID int32) {
	if builder.sqlCalcFoundRows {
		return
	}
	seen := make(map[int32]bool)
	producers := make(map[int32]*plan.Node)
	var candidates []*plan.Node
	var visit func(int32)
	visit = func(id int32) {
		if seen[id] {
			return
		}
		seen[id] = true
		node := builder.qry.Nodes[id]
		// DISTINCT lowering transfers a Project's binding tag to an Aggregate;
		// collect current reachable owners rather than using stale tag2NodeID.
		for _, tag := range node.BindingTags {
			if _, exists := producers[tag]; exists {
				producers[tag] = nil // an ambiguous owner is not a safety proof
			} else {
				producers[tag] = node
			}
		}
		for _, child := range node.Children {
			visit(child)
		}
		if isScalarCountForSortElimination(node) {
			sort := builder.qry.Nodes[node.Children[0]]
			if sort.NodeType == plan.Node_SORT && len(sort.Children) == 1 &&
				sort.Limit == nil && sort.Offset == nil && sort.RankOption == nil &&
				len(sort.BindingTags) == 0 && len(sort.ProjectList) == 0 &&
				len(sort.FilterList) == 0 && sort.SampleFunc == nil {
				candidates = append(candidates, node)
			}
		}
	}
	visit(rootID)

	proof := countSortValueProof{producers: producers, columns: make(map[[2]int32]bool)}
	for _, agg := range candidates {
		sort := builder.qry.Nodes[agg.Children[0]]
		safe := true
		for _, expr := range agg.AggList {
			if !proof.safe(expr.GetF().Args[0]) {
				safe = false
				break
			}
		}
		for _, order := range sort.OrderBy {
			if order == nil || order.Expr == nil || !countSortKeyTypeIsTotal(order.Expr.Typ) || !proof.safe(order.Expr) {
				safe = false
				break
			}
		}
		if safe {
			// Rewire only this consumer. A shared Sort is still available to
			// another consumer that observes its order.
			agg.Children[0] = sort.Children[0]
		}
	}
}

func isScalarCountForSortElimination(node *plan.Node) bool {
	if node.NodeType != plan.Node_AGG || len(node.Children) != 1 ||
		len(node.GroupBy) != 0 || len(node.GroupingFlag) != 0 || len(node.AggList) == 0 ||
		len(node.FilterList) != 0 || node.Limit != nil || node.Offset != nil || node.RankOption != nil ||
		node.SampleFunc != nil {
		return false
	}
	for _, expr := range node.AggList {
		fn := expr.GetF()
		if fn == nil || fn.Func == nil || len(fn.Args) != 1 ||
			uint64(fn.Func.Obj)&function.Distinct != 0 ||
			fn.AggConfigType != plan.AggregateConfigType_AGG_CONFIG_NONE || len(fn.AggConfig) != 0 {
			return false
		}
		id, _ := function.DecodeOverloadID(fn.Func.Obj)
		if id != function.COUNT && id != function.STARCOUNT {
			return false
		}
	}
	return true
}

// In addition to total key evaluation, require a scalar domain whose comparison
// cannot itself fail. Do not elide an unsupported vector/JSON/geometry ordering.
func countSortKeyTypeIsTotal(typ plan.Type) bool {
	t := types.T(typ.Id)
	return t.IsOrdered() || t.IsDecimal() || t.IsMySQLString() || t == types.T_bool || t == types.T_uuid
}

type countSortValueProof struct {
	producers map[int32]*plan.Node
	columns   map[[2]int32]bool
}

// safe follows aliases and DISTINCT group keys as well as the visible expression.
// Determinism alone is insufficient: pruning a sort-only cast can suppress its
// error, and moving COUNT evaluation before the Sort can change volatile calls.
func (proof *countSortValueProof) safe(expr *plan.Expr) bool {
	if !isTruncationSafeRowExpr(expr) || expr.AuxId < 0 {
		return false
	}
	if lit := expr.GetLit(); lit != nil {
		return lit.Src == nil
	}
	if fn := expr.GetF(); fn != nil {
		// isTruncationSafeRowExpr admits only total casts here.
		return proof.safe(fn.Args[0])
	}
	col := expr.GetCol()
	key := [2]int32{col.RelPos, col.ColPos}
	if safe, known := proof.columns[key]; known {
		return safe
	}
	// Also terminates a malformed alias cycle without granting a proof.
	proof.columns[key] = false
	node := proof.producers[col.RelPos]
	if node == nil || col.ColPos < 0 || len(node.BindingTags) == 0 || node.BindingTags[0] != col.RelPos {
		return false
	}
	var safe bool
	switch node.NodeType {
	case plan.Node_TABLE_SCAN:
		safe = node.TableDef != nil && int(col.ColPos) < len(node.TableDef.Cols)
	case plan.Node_PROJECT:
		safe = int(col.ColPos) < len(node.ProjectList) && proof.safe(node.ProjectList[col.ColPos])
	case plan.Node_AGG:
		// Only group-key outputs are row expressions. Aggregate/window outputs
		// and materialized/recursive producer boundaries remain conservative.
		safe = int(col.ColPos) < len(node.GroupBy) && proof.safe(node.GroupBy[col.ColPos])
	}
	proof.columns[key] = safe
	return safe
}
