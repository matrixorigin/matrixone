// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"sort"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
)

// deepRuntimeFilterTree proves an ancestor build-before-probe partial order.
// It does not prove physical delivery: Compile separately admits only complete,
// naturally local, concurrently started publishers on the scan's message board.
type deepRuntimeFilterTree struct {
	query        *planpb.Query
	order        []int32
	enter, leave map[int32]int
}

func newDeepRuntimeFilterTree(query *planpb.Query, root int32) *deepRuntimeFilterTree {
	if query == nil || query.StmtType != planpb.Query_SELECT || len(query.Steps) != 1 ||
		query.Correlation != nil || len(query.Params) != 0 || len(query.BackgroundQueries) != 0 {
		return nil
	}
	tree := &deepRuntimeFilterTree{query: query, enter: make(map[int32]int), leave: make(map[int32]int)}
	var walk func(int32, int) bool
	walk = func(id int32, depth int) bool {
		if id < 0 || int(id) >= len(query.Nodes) || query.Nodes[id] == nil || depth >= 128 || len(tree.order) >= 4096 {
			return false
		}
		if _, shared := tree.enter[id]; shared {
			return false
		}
		node := query.Nodes[id]
		if !deepRuntimeFilterOrdinaryNode(node) {
			return false
		}
		tree.enter[id] = len(tree.order)
		tree.order = append(tree.order, id)
		for _, child := range node.Children {
			if !walk(child, depth+1) {
				return false
			}
		}
		tree.leave[id] = len(tree.order)
		return true
	}
	if !walk(root, 0) || !tree.runtimeFilterEdgesAreAcyclic() {
		return nil
	}
	return tree
}

func deepRuntimeFilterOrdinaryNode(node *planpb.Node) bool {
	switch node.NodeType {
	case planpb.Node_TABLE_SCAN:
		if len(node.Children) != 0 || node.TableDef == nil {
			return false
		}
	case planpb.Node_JOIN:
		if node.JoinType != planpb.Node_INNER || node.IsRightJoin || len(node.Children) != 2 {
			return false
		}
	case planpb.Node_PROJECT, planpb.Node_FILTER, planpb.Node_AGG, planpb.Node_SORT:
		if len(node.Children) != 1 {
			return false
		}
	default:
		return false
	}
	if len(node.SourceStep) != 0 || node.RecursiveSink || node.RecursiveCte ||
		node.ExtraOptions != "" || node.FilterIsBarrier || node.RankOption != nil ||
		node.SampleFunc != nil || node.IndexReaderParam != nil || node.IndexSearchScan != nil ||
		len(node.LockTargets) != 0 || len(node.RecvMsgList) != 0 ||
		node.DedupJoinCtx != nil || node.ExternScan != nil || node.RowsetData != nil ||
		node.InsertCtx != nil || node.DeleteCtx != nil || node.PreInsertCtx != nil ||
		node.PreInsertUkCtx != nil || node.PreInsertSkCtx != nil || node.PostDmlCtx != nil ||
		len(node.UpdateCtxList) != 0 || node.Fuzzymessage != nil {
		return false
	}
	for _, header := range node.SendMsgList {
		if node.NodeType != planpb.Node_JOIN || header.MsgType != int32(message.MsgJoinMap) {
			return false
		}
	}
	valid, remaining := true, 4096
	stop := moerr.GetOkStopCurrRecur()
	owner := *node
	owner.TableDef = nil // catalog defaults are not executed by a scan
	err := planpb.VisitExpressionsInOwner(&owner, func(expr *planpb.Expr) error {
		return planpb.VisitExprTree(expr, func(expr *planpb.Expr) error {
			remaining--
			if remaining < 0 {
				return stop
			}
			valid = valid && expr.GetCorr() == nil && expr.GetSub() == nil
			return nil
		})
	})
	return valid && err == nil
}

func (tree *deepRuntimeFilterTree) contains(root, id int32) bool {
	start, exists := tree.enter[id]
	first, valid := tree.enter[root]
	return exists && valid && start >= first && start < tree.leave[root]
}

func ordinaryRuntimeFilterSpec(spec *planpb.RuntimeFilterSpec) bool {
	return spec != nil && spec.Tag > 0 && !spec.ScalarPredicate && !spec.MustApply &&
		!spec.UseMembershipFilter && (spec.Expr != nil || spec.BuildExpr != nil)
}

func (tree *deepRuntimeFilterTree) runtimeFilterEdgesAreAcyclic() bool {
	owners := make(map[int32]int32)
	ordinary := make(map[int32]bool)
	for _, id := range tree.order {
		node := tree.query.Nodes[id]
		for _, spec := range node.RuntimeFilterBuildList {
			if spec == nil || spec.Tag <= 0 || node.NodeType != planpb.Node_JOIN {
				return false
			}
			if _, duplicate := owners[spec.Tag]; duplicate {
				return false
			}
			owners[spec.Tag] = id
			ordinary[spec.Tag] = ordinaryRuntimeFilterSpec(spec)
			if !ordinaryRuntimeFilterSpec(spec) && !(node.GetStats().GetHashmapStats().GetShuffle() && spec.Expr == nil && spec.BuildExpr == nil) {
				return false
			}
		}
	}
	for _, id := range tree.order {
		node := tree.query.Nodes[id]
		for _, spec := range node.RuntimeFilterProbeList {
			if spec == nil {
				return false
			}
			owner, exists := owners[spec.Tag]
			if !exists {
				return false
			}
			if node.NodeType == planpb.Node_JOIN && owner == id && node.GetStats().GetHashmapStats().GetShuffle() && spec.Expr == nil && spec.BuildExpr == nil {
				continue // Shuffle readiness dependency, not a scan wait.
			}
			producer := tree.query.Nodes[owner]
			if node.NodeType != planpb.Node_TABLE_SCAN || !ordinary[spec.Tag] || !ordinaryRuntimeFilterSpec(spec) || !tree.contains(producer.Children[0], id) {
				return false
			}
		}
	}
	return true
}

func (builder *QueryBuilder) deepRuntimeFilterScan(tree *deepRuntimeFilterTree, root int32, col *planpb.ColRef) *planpb.Node {
	if tree == nil || col == nil {
		return nil
	}
	target := int32(-1)
	for _, id := range tree.order {
		node := tree.query.Nodes[id]
		if node.NodeType == planpb.Node_TABLE_SCAN && len(node.BindingTags) == 1 && node.BindingTags[0] == col.RelPos {
			if target >= 0 {
				return nil
			}
			target = id
		}
	}
	if target < 0 || !tree.contains(root, target) {
		return nil
	}
	for root != target {
		node := tree.query.Nodes[root]
		if node.Limit != nil || node.Offset != nil || !areTruncationSafePredicates(node.FilterList) ||
			!areTruncationSafePredicates(node.BlockFilterList) || !areTruncationSafePredicates(node.OnList) {
			return nil
		}
		switch node.NodeType {
		case planpb.Node_FILTER:
			root = node.Children[0]
		case planpb.Node_JOIN:
			child := 0
			if tree.contains(node.Children[1], target) {
				child = 1
				if !builder.scalarRuntimeFilterSiblingIsSafeToSkip(node.Children[0]) {
					return nil
				}
			}
			root = node.Children[child]
		default:
			return nil
		}
	}
	scan := tree.query.Nodes[target]
	if scan.Limit != nil || scan.Offset != nil || col.ColPos < 0 || int(col.ColPos) >= len(scan.TableDef.Cols) ||
		!areTruncationSafePredicates(scan.FilterList) || !areTruncationSafePredicates(scan.BlockFilterList) {
		return nil
	}
	return scan
}

// DeepOrdinaryRuntimeFilterTags identifies optional deep pairs in a finalized
// plan, including unsafe ones which compilation must disable. The bool proves
// only the logical tree/ancestor order, never message delivery or completeness.
func DeepOrdinaryRuntimeFilterTags(query *planpb.Query) ([]int32, bool) {
	if query == nil {
		return nil, false
	}
	seen := make(map[int32]bool)
	var tags []int32
	for _, node := range query.Nodes {
		if node == nil || node.NodeType != planpb.Node_JOIN || node.JoinType != planpb.Node_INNER ||
			node.IsRightJoin || len(node.Children) != 2 || node.GetStats().GetHashmapStats().GetShuffle() ||
			node.Children[0] < 0 || int(node.Children[0]) >= len(query.Nodes) || query.Nodes[node.Children[0]] == nil ||
			query.Nodes[node.Children[0]].NodeType == planpb.Node_TABLE_SCAN {
			continue
		}
		for _, spec := range node.RuntimeFilterBuildList {
			if ordinaryRuntimeFilterSpec(spec) && !seen[spec.Tag] {
				seen[spec.Tag] = true
				tags = append(tags, spec.Tag)
			}
		}
	}
	sort.Slice(tags, func(i, j int) bool { return tags[i] < tags[j] })
	if len(tags) == 0 {
		return nil, true
	}
	return tags, len(query.Steps) == 1 && newDeepRuntimeFilterTree(query, query.Steps[0]) != nil
}
