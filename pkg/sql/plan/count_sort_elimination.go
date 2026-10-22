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
//
// The Sort may also hold the last outside reference to a LEFT JOIN right
// branch: dropping it would let removeEffectlessLeftJoins prune that branch
// and discard any runtime error its evaluation would raise. The elimination
// therefore keeps every binding its OrderBy releases either protected by a
// surviving reference or owned by a subtree whose pruning is provably free of
// runtime effects.
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
	var released *countSortReleasedBindings
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
			// The liveness snapshot must describe the untouched plan, so build
			// it before the first rewiring rather than at function entry.
			if released == nil {
				released = newCountSortReleasedBindings(builder, rootID)
			}
			safe = builder.eliminationKeepsPrunableBranchesEffectClosed(released, producers, sort)
		}
		if safe {
			// Rewire only this consumer. A shared Sort is still available to
			// another consumer that observes its order.
			agg.Children[0] = sort.Children[0]
			released.recordRemoval(sort)
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

// countSortReleasedBindings decides whether removing one candidate Sort could
// newly expose a plan branch to removeEffectlessLeftJoins. ancestorProt holds,
// for every binding tag, the number of references made by its strict ancestor
// nodes, excluding OnList: the join-pruning rule subtracts the owning join's
// OnList before testing protection, and references inside a producer's own
// subtree never delay that pruning either. removed accumulates the OrderBy
// references of Sorts this rule has already detached, so sequentially
// processed candidates see a consistent count.
type countSortReleasedBindings struct {
	ancestorProt map[int32]int
	removed      map[int32]int
}

func newCountSortReleasedBindings(builder *QueryBuilder, rootID int32) *countSortReleasedBindings {
	released := &countSortReleasedBindings{
		ancestorProt: make(map[int32]int),
		removed:      make(map[int32]int),
	}
	running := make(map[int32]int)
	seen := make(map[int32]bool)
	var walk func(int32)
	walk = func(id int32) {
		if seen[id] {
			// A shared subgraph is counted once; its bindings are already
			// rejected as ambiguous producers before any release is granted.
			return
		}
		seen[id] = true
		node := builder.qry.Nodes[id]
		// A node's own expressions reference its outputs, but they cannot
		// protect the node from pruning, so snapshot before counting them.
		for _, tag := range node.BindingTags {
			released.ancestorProt[tag] = running[tag]
		}
		countSortKeepRefs(node, 1, running)
		for _, child := range node.Children {
			walk(child)
		}
		countSortKeepRefs(node, -1, running)
	}
	walk(rootID)
	return released
}

// countSortKeepRefs mirrors the reference counting of
// removeEffectlessLeftJoins with OnList excluded, because an ON reference
// alone never keeps a binding alive for that rewrite.
func countSortKeepRefs(node *plan.Node, inc int, tagCnt map[int32]int) {
	increaseTagCntForExprList(node.ProjectList, inc, tagCnt)
	increaseTagCntForExprList(node.DedupKeyExprs, inc, tagCnt)
	if node.PreInsertUkCtx != nil {
		for _, exprs := range node.PreInsertUkCtx.KeyDiagnosticExprs {
			increaseTagCntForExprList(exprs.GetList(), inc, tagCnt)
		}
	}
	increaseTagCntForExprList(node.FilterList, inc, tagCnt)
	increaseTagCntForExprList(node.GroupBy, inc, tagCnt)
	increaseTagCntForExprList(node.AggList, inc, tagCnt)
	increaseTagCntForExprList(node.WinSpecList, inc, tagCnt)
	for i := range node.OrderBy {
		increaseTagCnt(node.OrderBy[i].Expr, inc, tagCnt)
	}
	if node.DedupJoinCtx != nil {
		increaseTagCntForColRefList(node.DedupJoinCtx.OldColList, 2*inc, tagCnt)
		increaseTagCntForExprList(node.DedupJoinCtx.UpdateColExprList, 2*inc, tagCnt)
		for _, col := range dedupJoinMetadataCols(node.DedupJoinCtx) {
			if col != nil {
				tagCnt[col.RelPos] += 2 * inc
			}
		}
	}
	for _, updateCtx := range node.UpdateCtxList {
		increaseTagCntForColRefList(updateCtx.InsertCols, 2*inc, tagCnt)
		increaseTagCntForColRefList(updateCtx.DeleteCols, 2*inc, tagCnt)
		increaseTagCntForColRefList(updateCtx.PartitionCols, 2*inc, tagCnt)
		if updateCtx.ChangedRowsCol != nil {
			tagCnt[updateCtx.ChangedRowsCol.RelPos] += 2 * inc
		}
		for _, col := range []*plan.ColRef{updateCtx.AffectedRowsWeightCol, updateCtx.PhysicalChangedRowsCol} {
			if col != nil {
				tagCnt[col.RelPos] += 2 * inc
			}
		}
		increaseTagCntForColRefList(updateCtx.AffectedRowsCols, 2*inc, tagCnt)
	}
}

func (released *countSortReleasedBindings) recordRemoval(sort *plan.Node) {
	for _, order := range sort.OrderBy {
		increaseTagCnt(order.Expr, 1, released.removed)
	}
}

// eliminationKeepsPrunableBranchesEffectClosed allows the Sort removal only
// when every binding its OrderBy releases either keeps a surviving
// ancestor-level reference or is produced inside a branch whose later pruning
// cannot hide a runtime error or a volatile evaluation.
func (builder *QueryBuilder) eliminationKeepsPrunableBranchesEffectClosed(
	released *countSortReleasedBindings, producers map[int32]*plan.Node, sort *plan.Node) bool {
	sortRefs := make(map[int32]int)
	for _, order := range sort.OrderBy {
		increaseTagCnt(order.Expr, 1, sortRefs)
	}
	var releasedTags []int32
	for tag, refs := range sortRefs {
		protection := released.ancestorProt[tag] - released.removed[tag]
		if protection < refs {
			// The Sort cannot hold more ancestor references than the snapshot
			// recorded; never grant a proof from inconsistent counting.
			return false
		}
		if protection == refs {
			releasedTags = append(releasedTags, tag)
		}
	}
	if len(releasedTags) == 0 {
		return true
	}
	// Walk the current plan: earlier candidates may already have been rewired.
	parents := countSortSubtreeParents(builder, sort.Children[0])
	checked := make(map[int32]bool)
	for _, tag := range releasedTags {
		producer := producers[tag]
		if producer == nil {
			return false
		}
		id := producer.NodeId
		if _, ok := parents[id]; !ok {
			// A producer outside the Sort subtree has an unbounded blast
			// radius once its reference is gone.
			return false
		}
		for {
			parent, ok := parents[id]
			if !ok || parent < 0 {
				break
			}
			if anc := builder.qry.Nodes[parent]; anc.NodeType == plan.Node_JOIN &&
				anc.JoinType == plan.Node_LEFT && id == anc.Children[1] && !checked[parent] {
				checked[parent] = true
				if !builder.leftJoinPruneIsEffectClosed(anc) {
					return false
				}
			}
			id = parent
		}
	}
	return true
}

// countSortSubtreeParents maps each node below rootID to its parent, using -1
// as the root sentinel. A shared child keeps its first parent; an ambiguous
// producer is rejected by the producers map before this lookup is trusted.
func countSortSubtreeParents(builder *QueryBuilder, rootID int32) map[int32]int32 {
	parents := map[int32]int32{rootID: -1}
	var walk func(int32)
	walk = func(id int32) {
		for _, child := range builder.qry.Nodes[id].Children {
			if _, seen := parents[child]; seen {
				continue
			}
			parents[child] = id
			walk(child)
		}
	}
	walk(rootID)
	return parents
}

// leftJoinPruneIsEffectClosed proves that removeEffectlessLeftJoins replacing
// this LEFT JOIN by its left child cannot hide a runtime effect: the join's
// own predicates and every expression in its right subtree must be total and
// free of volatile or side-effecting evaluation. Reusing the truncation-safe
// proof keeps this rule at the same standard as the singleton-aggregate and
// LIMIT-pushdown rewrites.
func (builder *QueryBuilder) leftJoinPruneIsEffectClosed(join *plan.Node) bool {
	if !countSortPrunableNodeIsEffectClosed(join) ||
		!countSortPrunableExprListsAreTotal(join) {
		return false
	}
	stack := []int32{join.Children[1]}
	for len(stack) > 0 {
		node := builder.qry.Nodes[stack[len(stack)-1]]
		stack = stack[:len(stack)-1]
		if !countSortPrunableNodeIsEffectClosed(node) ||
			!countSortPrunableExprListsAreTotal(node) {
			return false
		}
		stack = append(stack, node.Children...)
	}
	return true
}

// countSortPrunableNodeIsEffectClosed restricts a prunable branch to plain
// row-producing operators without diagnostics, limits, windows, aggregates or
// DML/locking/adaptive payloads whose removal could lose an effect.
func countSortPrunableNodeIsEffectClosed(node *plan.Node) bool {
	switch node.NodeType {
	case plan.Node_TABLE_SCAN, plan.Node_PROJECT, plan.Node_JOIN:
	default:
		return false
	}
	if len(node.FilterList) != 0 && node.NodeType != plan.Node_TABLE_SCAN {
		// Pruning drops a non-scan filter's row selection as well as its
		// evaluation. A total scan filter, by contrast, only loses a
		// provably error-free evaluation with the pruned branch.
		return false
	}
	return node.Limit == nil && node.Offset == nil && node.RankOption == nil && node.SampleFunc == nil &&
		len(node.WinSpecList) == 0 && len(node.OrderBy) == 0 &&
		len(node.GroupBy) == 0 && len(node.GroupingFlag) == 0 && len(node.AggList) == 0 &&
		len(node.DedupKeyExprs) == 0 && node.PreInsertCtx == nil && node.PreInsertUkCtx == nil &&
		node.PreInsertSkCtx == nil && node.InsertCtx == nil && node.DeleteCtx == nil &&
		node.PostDmlCtx == nil && len(node.UpdateCtxList) == 0 && len(node.OnUpdateExprs) == 0 &&
		len(node.LockTargets) == 0 && len(node.TblFuncExprList) == 0 && node.RowsetData == nil &&
		node.ClusterTable == nil && node.Fuzzymessage == nil && node.DedupJoinCtx == nil &&
		len(node.SendMsgList) == 0 && len(node.RecvMsgList) == 0 && len(node.SourceStep) == 0
}

func countSortPrunableExprListsAreTotal(node *plan.Node) bool {
	for _, expr := range node.ProjectList {
		if !isTruncationSafeRowExpr(expr) {
			return false
		}
	}
	for _, expr := range node.FilterList {
		if !isTruncationSafePredicateExpr(expr) {
			return false
		}
	}
	for _, expr := range node.OnList {
		if !isTruncationSafePredicateExpr(expr) {
			return false
		}
	}
	return true
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
