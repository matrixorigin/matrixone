// Copyright 2024 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	indexplugin "github.com/matrixorigin/matrixone/pkg/indexplugin"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

type vectorSortContext struct {
	projNode      *plan.Node
	sortNode      *plan.Node
	scanNode      *plan.Node
	childNode     *plan.Node
	orderExpr     *plan.Expr
	distFnExpr    *plan.Function
	sortDirection plan.OrderBySpec_OrderByFlag
	limit         *plan.Expr // internal candidate budget (LIMIT + OFFSET)
	resultLimit   *plan.Expr
	resultOffset  *plan.Expr
	rankOption    *plan.RankOption

	providerNodeID int32
	vecArgExpr     *plan.Expr
}

func (builder *QueryBuilder) resolveScanNodeWithIndex(node *plan.Node, depth int32) *plan.Node {
	if node.NodeType == plan.Node_PROJECT && len(node.Children) == 1 {
		return builder.resolveScanNodeWithIndex(builder.qry.Nodes[node.Children[0]], depth)
	}

	if depth == 0 {
		if node.NodeType == plan.Node_TABLE_SCAN && node.TableDef.Indexes != nil {
			return node
		}
		return nil
	}

	if (node.NodeType == plan.Node_SORT || node.NodeType == plan.Node_AGG) && len(node.Children) == 1 {
		return builder.resolveScanNodeWithIndex(builder.qry.Nodes[node.Children[0]], depth-1)
	}

	return nil
}

// buildVectorSortContextFromSort builds the same context as buildVectorSortContext, but
// anchored at the Top-K SORT itself rather than at a PROJECT above it.
//
// The project-anchored form only sees a Top-K whose parent is a PROJECT, so any consumer
// between the two hides it: an outer ORDER BY leaves Project->Sort->Sort->Scan, where the
// resolver's single hop is spent on the outer sort (#25967), and a join leaves the Top-K
// as a join input with no project above it at all (#25974). Both then fall back to a full
// scan with an exact sort — correct, but the index is silently unused. Anchoring here
// makes the Top-K rewritable wherever it sits; applyIndices repoints whichever parent it
// has from the returned node id.
//
// projNode is deliberately left nil: there is no project to mutate or read column
// requirements from, which the rewrites detect (see spliceVectorRewrite, and the
// index-only gate in applyIndicesForSortUsingIvfflat).
func (builder *QueryBuilder) buildVectorSortContextFromSort(sortNode *plan.Node) *vectorSortContext {
	if sortNode == nil || sortNode.NodeType != plan.Node_SORT || len(sortNode.OrderBy) != 1 {
		return nil
	}
	// Only a Top-K carries its own limit. A bare ordering node (the outer ORDER BY of
	// #25967) has none, and rewriting it would discard the ordering its consumer wants.
	if sortNode.Limit == nil {
		return nil
	}
	// NOTE: only the plain shape is handled here. The vector-provider join
	// (buildVectorSortContextThroughJoin) also hides behind a consumer in principle, but
	// no SQL shape reachable in testing produces that context — it needs a single-row,
	// NOT NULL, limit-1 provider join all at once — so a sort-anchored fallback for it
	// would be unverifiable code. Left deliberately out; see the decision log.
	return builder.buildVectorSortContextFrom(nil, sortNode)
}

func (builder *QueryBuilder) buildVectorSortContext(projNode *plan.Node) *vectorSortContext {
	sortNode := builder.resolveSortNode(projNode, 1)
	if sortNode == nil || len(sortNode.OrderBy) != 1 {
		return nil
	}
	return builder.buildVectorSortContextFrom(projNode, sortNode)
}

// buildVectorSortContextFrom is the shared body of the project- and sort-anchored
// builders: everything below the Top-K sort is resolved identically, only the anchor
// differs. projNode may be nil.
func (builder *QueryBuilder) buildVectorSortContextFrom(projNode, sortNode *plan.Node) *vectorSortContext {
	scanNode := builder.resolveScanNodeWithIndex(sortNode, 1)
	if scanNode == nil {
		return nil
	}

	orderExpr := sortNode.OrderBy[0].Expr
	distFnExpr := orderExpr.GetF()
	var childNode *plan.Node
	if distFnExpr == nil {
		if len(sortNode.Children) == 0 {
			return nil
		}
		childNode = builder.qry.Nodes[sortNode.Children[0]]
		if childNode.NodeType == plan.Node_PROJECT {
			// Bounds-check: the ORDER BY column indexes the child project's list, but the
			// two can disagree. `select count(*) from (<top-k>) t` prunes the derived
			// table's projection to nothing while the sort still carries ColPos from
			// before pruning, so indexing blind panics with index out of range.
			col := orderExpr.GetCol()
			if col == nil || col.ColPos < 0 || int(col.ColPos) >= len(childNode.ProjectList) {
				return nil
			}
			distFnExpr = childNode.ProjectList[col.ColPos].GetF()
		}
		if distFnExpr == nil {
			return nil
		}
	}

	// SORT anchor only (projNode == nil): the child PROJECT is detached and each of its
	// non-distance expressions is published through idxColMap, which applyIndices then
	// substitutes into EVERY ancestor referencing that column -- a deep copy per reference.
	// Fine for a column reference, wrong for anything evaluated: two ancestors would each
	// get their own rand()/uuid()/now(), so the value a JOIN predicate admitted a row on can
	// differ from the value projected out of it, and moving evaluation above a JOIN changes
	// how many times it runs.
	//
	// The PROJECT anchor is unaffected: spliceVectorRewrite applies its remap to that one
	// node, so the expression is still evaluated exactly once.
	//
	// exprCanRemoveProject is the planner's existing answer to this same question --
	// removeSimpleProjections refuses to inline a PROJECT holding a CannotFold or
	// IsRealTimeRelated function. Reuse it rather than restate it, so the two cannot drift.
	if projNode == nil && childNode != nil && !vectorChildProjectIsDetachable(childNode, orderExpr) {
		return nil
	}

	limit, offset, rankOption := pickVectorPagination(sortNode, scanNode, projNode)
	if limit == nil {
		return nil
	}
	candidateLimit, ok := buildCandidateLimit(limit, offset)
	if !ok {
		return nil
	}

	return &vectorSortContext{
		projNode:      projNode,
		sortNode:      sortNode,
		scanNode:      scanNode,
		childNode:     childNode,
		orderExpr:     orderExpr,
		distFnExpr:    distFnExpr,
		sortDirection: sortNode.OrderBy[0].Flag,
		limit:         candidateLimit,
		resultLimit:   DeepCopyExpr(limit),
		resultOffset:  DeepCopyExpr(offset),
		rankOption:    rankOption,
	}
}

func (builder *QueryBuilder) buildVectorSortContextThroughJoin(projNode *plan.Node) *vectorSortContext {
	sortNode := builder.resolveSortNode(projNode, 1)
	if sortNode == nil || len(sortNode.OrderBy) != 1 {
		return nil
	}
	joinNode, childNode := builder.resolveJoinNodeForVectorSort(sortNode)
	if joinNode == nil || len(joinNode.Children) != 2 || !isVectorProviderJoin(joinNode) {
		return nil
	}

	orderExpr := sortNode.OrderBy[0].Expr
	distFnExpr := orderExpr.GetF()
	if distFnExpr == nil && childNode != nil {
		orderCol := orderExpr.GetCol()
		if orderCol == nil || orderCol.ColPos < 0 || int(orderCol.ColPos) >= len(childNode.ProjectList) {
			return nil
		}
		distFnExpr = childNode.ProjectList[orderCol.ColPos].GetF()
	}
	if distFnExpr == nil || len(distFnExpr.Args) != 2 {
		return nil
	}

	leftNodeID, rightNodeID := joinNode.Children[0], joinNode.Children[1]
	leftNode, rightNode := builder.qry.Nodes[leftNodeID], builder.qry.Nodes[rightNodeID]
	leftTags := builder.collectBindingTags(leftNode)
	rightTags := builder.collectBindingTags(rightNode)

	scanNode, providerNodeID, providerTags, vecArgExpr := builder.pickJoinThroughVectorSides(
		leftNodeID,
		leftNode,
		leftTags,
		rightNodeID,
		rightNode,
		rightTags,
		distFnExpr,
	)
	if scanNode == nil || vecArgExpr == nil {
		return nil
	}
	if !builder.isJoinThroughProjectionSafe(projNode, childNode, orderExpr, providerTags) {
		return nil
	}

	limit, offset, rankOption := pickVectorPagination(sortNode, scanNode, projNode)
	if limit == nil {
		return nil
	}
	candidateLimit, ok := buildCandidateLimit(limit, offset)
	if !ok {
		return nil
	}

	return &vectorSortContext{
		projNode:       projNode,
		sortNode:       sortNode,
		scanNode:       scanNode,
		childNode:      childNode,
		orderExpr:      orderExpr,
		distFnExpr:     distFnExpr,
		sortDirection:  sortNode.OrderBy[0].Flag,
		limit:          candidateLimit,
		resultLimit:    DeepCopyExpr(limit),
		resultOffset:   DeepCopyExpr(offset),
		rankOption:     rankOption,
		providerNodeID: providerNodeID,
		vecArgExpr:     vecArgExpr,
	}
}

func (builder *QueryBuilder) resolveJoinNodeForVectorSort(sortNode *plan.Node) (*plan.Node, *plan.Node) {
	if sortNode == nil || len(sortNode.Children) != 1 {
		return nil, nil
	}

	childNode := builder.qry.Nodes[sortNode.Children[0]]
	if childNode.NodeType == plan.Node_JOIN {
		return childNode, nil
	}
	if childNode.NodeType == plan.Node_PROJECT && len(childNode.Children) == 1 {
		joinNode := builder.qry.Nodes[childNode.Children[0]]
		if joinNode.NodeType == plan.Node_JOIN {
			return joinNode, childNode
		}
	}
	return nil, nil
}

func isVectorProviderJoin(joinNode *plan.Node) bool {
	return joinNode.JoinType == plan.Node_INNER && isTrivialJoinOnList(joinNode.OnList)
}

func isTrivialJoinOnList(onList []*plan.Expr) bool {
	for _, expr := range onList {
		lit := expr.GetLit()
		if lit == nil || !lit.GetBval() {
			return false
		}
	}
	return true
}

func (builder *QueryBuilder) pickJoinThroughVectorSides(
	leftNodeID int32,
	leftNode *plan.Node,
	leftTags map[int32]bool,
	rightNodeID int32,
	rightNode *plan.Node,
	rightTags map[int32]bool,
	distFnExpr *plan.Function,
) (*plan.Node, int32, map[int32]bool, *plan.Expr) {
	if scanNode, vecArgExpr := builder.tryJoinThroughVectorSide(leftNode, leftTags, rightNode, rightTags, distFnExpr); scanNode != nil {
		return scanNode, rightNodeID, rightTags, vecArgExpr
	}
	if scanNode, vecArgExpr := builder.tryJoinThroughVectorSide(rightNode, rightTags, leftNode, leftTags, distFnExpr); scanNode != nil {
		return scanNode, leftNodeID, leftTags, vecArgExpr
	}
	return nil, -1, nil, nil
}

func (builder *QueryBuilder) tryJoinThroughVectorSide(
	mainNode *plan.Node,
	mainTags map[int32]bool,
	providerNode *plan.Node,
	providerTags map[int32]bool,
	distFnExpr *plan.Function,
) (*plan.Node, *plan.Expr) {
	scanNode := builder.directScanWithVectorIndex(mainNode)
	if scanNode == nil || len(scanNode.BindingTags) == 0 || !builder.isSingleRowVectorProvider(providerNode) {
		return nil, nil
	}

	vecArgExpr := extractJoinThroughProviderVectorArg(distFnExpr, scanNode.BindingTags[0], mainTags, providerTags)
	if vecArgExpr == nil {
		return nil, nil
	}
	if !builder.isNonNullVectorProviderArg(providerNode, vecArgExpr) {
		return nil, nil
	}
	return scanNode, vecArgExpr
}

func (builder *QueryBuilder) directScanWithVectorIndex(node *plan.Node) *plan.Node {
	if node == nil || node.NodeType != plan.Node_TABLE_SCAN || node.TableDef == nil || len(node.BindingTags) == 0 {
		return nil
	}
	for _, idx := range node.TableDef.Indexes {
		// Recognize every plugin-registered vector index (HNSW, CAGRA,
		// IVF-PQ, IVF-FLAT). The join-through and direct-scan rewrites
		// must agree on the algo set — using the central
		// indexplugin.IsVectorIndexAlgo capability check keeps them
		// from drifting back into hardcoded algo lists like the previous
		// IsIvfIndexAlgo || IsHnswIndexAlgo gate, which silently
		// excluded CAGRA / IVF-PQ from the join-through path.
		if idx != nil && indexplugin.IsVectorIndexAlgo(idx.IndexAlgo) {
			return node
		}
	}
	return nil
}

func extractJoinThroughProviderVectorArg(
	distFnExpr *plan.Function,
	scanTag int32,
	mainTags map[int32]bool,
	providerTags map[int32]bool,
) *plan.Expr {
	if distFnExpr == nil || len(distFnExpr.Args) != 2 {
		return nil
	}

	arg0Col := distFnExpr.Args[0].GetCol()
	arg1Col := distFnExpr.Args[1].GetCol()
	if arg0Col == nil || arg1Col == nil {
		return nil
	}
	if arg0Col.RelPos == scanTag && mainTags[arg0Col.RelPos] && providerTags[arg1Col.RelPos] {
		return distFnExpr.Args[1]
	}
	if arg1Col.RelPos == scanTag && mainTags[arg1Col.RelPos] && providerTags[arg0Col.RelPos] {
		return distFnExpr.Args[0]
	}
	return nil
}

func (builder *QueryBuilder) isJoinThroughProjectionSafe(
	projNode *plan.Node,
	childNode *plan.Node,
	orderExpr *plan.Expr,
	providerTags map[int32]bool,
) bool {
	if exprListRefsAnyTag(projNode.ProjectList, providerTags) {
		return false
	}
	if childNode == nil {
		return true
	}

	sortIdx := int32(-1)
	if orderCol := orderExpr.GetCol(); orderCol != nil {
		sortIdx = orderCol.ColPos
	}
	for i, expr := range childNode.ProjectList {
		if int32(i) == sortIdx {
			continue
		}
		if exprRefsAnyTag(expr, providerTags) {
			return false
		}
	}
	return true
}

func exprListRefsAnyTag(exprs []*plan.Expr, tags map[int32]bool) bool {
	for _, expr := range exprs {
		if exprRefsAnyTag(expr, tags) {
			return true
		}
	}
	return false
}

func exprRefsAnyTag(expr *plan.Expr, tags map[int32]bool) bool {
	if expr == nil {
		return false
	}
	switch impl := expr.Expr.(type) {
	case *plan.Expr_Col:
		return tags[impl.Col.RelPos]
	case *plan.Expr_F:
		return exprListRefsAnyTag(impl.F.Args, tags)
	case *plan.Expr_List:
		return exprListRefsAnyTag(impl.List.List, tags)
	case *plan.Expr_Sub:
		return false
	default:
		return false
	}
}

func (builder *QueryBuilder) collectBindingTags(node *plan.Node) map[int32]bool {
	tags := make(map[int32]bool)
	builder.collectBindingTagsRecursive(node, tags, make(map[int32]struct{}))
	return tags
}

func (builder *QueryBuilder) collectBindingTagsRecursive(node *plan.Node, tags map[int32]bool, visited map[int32]struct{}) {
	if node == nil {
		return
	}
	if _, ok := visited[node.NodeId]; ok {
		return
	}
	visited[node.NodeId] = struct{}{}
	for _, tag := range node.BindingTags {
		tags[tag] = true
	}
	for _, childID := range node.Children {
		builder.collectBindingTagsRecursive(builder.qry.Nodes[childID], tags, visited)
	}
}

func (builder *QueryBuilder) isSingleRowVectorProvider(node *plan.Node) bool {
	if node == nil {
		return false
	}
	switch node.NodeType {
	case plan.Node_TABLE_SCAN:
		return tableScanHasSingleRowFilter(node)
	case plan.Node_PROJECT, plan.Node_SORT:
		if len(node.Children) != 1 {
			return false
		}
		return builder.isSingleRowVectorProvider(builder.qry.Nodes[node.Children[0]])
	default:
		return false
	}
}

func (builder *QueryBuilder) isNonNullVectorProviderArg(providerNode *plan.Node, vecArgExpr *plan.Expr) bool {
	if vecArgExpr == nil {
		return false
	}
	if vecArgExpr.Typ.NotNullable {
		return true
	}
	col := vecArgExpr.GetCol()
	if col == nil {
		return false
	}
	return builder.providerColIsNonNull(providerNode, col.RelPos, col.ColPos)
}

func (builder *QueryBuilder) providerColIsNonNull(node *plan.Node, tag int32, colPos int32) bool {
	if node == nil {
		return false
	}
	if filterListHasIsNotNullOnCol(node.FilterList, tag, colPos) {
		return true
	}
	switch node.NodeType {
	case plan.Node_TABLE_SCAN:
		if len(node.BindingTags) == 0 || node.BindingTags[0] != tag || node.TableDef == nil {
			return false
		}
		return colPos >= 0 && int(colPos) < len(node.TableDef.Cols) && node.TableDef.Cols[colPos].Typ.NotNullable
	case plan.Node_PROJECT:
		if len(node.BindingTags) > 0 && node.BindingTags[0] == tag {
			if colPos < 0 || int(colPos) >= len(node.ProjectList) {
				return false
			}
			projectExpr := node.ProjectList[colPos]
			if projectExpr.Typ.NotNullable {
				return true
			}
			if projectCol := projectExpr.GetCol(); projectCol != nil && len(node.Children) == 1 {
				return builder.providerColIsNonNull(
					builder.qry.Nodes[node.Children[0]],
					projectCol.RelPos,
					projectCol.ColPos,
				)
			}
			return false
		}
	case plan.Node_SORT:
		if filterListHasIsNotNullOnCol(node.FilterList, tag, colPos) {
			return true
		}
	}
	for _, childID := range node.Children {
		if builder.providerColIsNonNull(builder.qry.Nodes[childID], tag, colPos) {
			return true
		}
	}
	return false
}

func tableScanHasSingleRowFilter(node *plan.Node) bool {
	if node == nil || node.TableDef == nil || len(node.BindingTags) == 0 {
		return false
	}
	tag := node.BindingTags[0]
	if node.TableDef.Pkey != nil {
		pkCols := node.TableDef.Pkey.Names
		if len(pkCols) == 0 && node.TableDef.Pkey.PkeyColName != "" {
			pkCols = []string{node.TableDef.Pkey.PkeyColName}
		}
		if filterListHasConstEqualityOnCols(node.FilterList, node.TableDef, tag, pkCols) {
			return true
		}
	}
	for _, idx := range node.TableDef.Indexes {
		if idx != nil && idx.Unique && filterListHasConstEqualityOnCols(node.FilterList, node.TableDef, tag, idx.Parts) {
			return true
		}
	}
	return false
}

func filterListHasConstEqualityOnCols(filters []*plan.Expr, tableDef *plan.TableDef, tag int32, colNames []string) bool {
	if len(colNames) == 0 {
		return false
	}
	for _, colName := range colNames {
		colPos, ok := tableDef.Name2ColIndex[catalog.ResolveAlias(colName)]
		if !ok {
			return false
		}
		if !filterListHasConstEqualityOnCol(filters, tag, colPos) {
			return false
		}
	}
	return true
}

func filterListHasConstEqualityOnCol(filters []*plan.Expr, tag int32, colPos int32) bool {
	for _, filter := range filters {
		fn := filter.GetF()
		if fn == nil || fn.Func.ObjName != "=" || len(fn.Args) != 2 {
			continue
		}
		if exprIsCol(fn.Args[0], tag, colPos) && isRuntimeConstExpr(fn.Args[1]) {
			return true
		}
		if exprIsCol(fn.Args[1], tag, colPos) && isRuntimeConstExpr(fn.Args[0]) {
			return true
		}
	}
	return false
}

func filterListHasIsNotNullOnCol(filters []*plan.Expr, tag int32, colPos int32) bool {
	for _, filter := range filters {
		fn := filter.GetF()
		if fn == nil || len(fn.Args) != 1 {
			continue
		}
		if fn.Func.ObjName != "isnotnull" && fn.Func.ObjName != "is_not_null" {
			continue
		}
		if exprIsCol(fn.Args[0], tag, colPos) {
			return true
		}
	}
	return false
}

func exprIsCol(expr *plan.Expr, tag int32, colPos int32) bool {
	col := expr.GetCol()
	return col != nil && col.RelPos == tag && col.ColPos == colPos
}

func vectorSearchProviderChildren(vecCtx *vectorSortContext) []int32 {
	if vecCtx == nil || vecCtx.vecArgExpr == nil || vecCtx.providerNodeID < 0 {
		return nil
	}
	return []int32{vecCtx.providerNodeID}
}

func vectorResultPagination(vecCtx *vectorSortContext) (*plan.Expr, *plan.Expr) {
	if vecCtx == nil || vecCtx.resultLimit == nil {
		return nil, nil
	}
	return DeepCopyExpr(vecCtx.resultLimit), DeepCopyExpr(vecCtx.resultOffset)
}

func hasCompleteVectorPagination(vecCtx *vectorSortContext) bool {
	return vecCtx != nil && vecCtx.limit != nil && vecCtx.resultLimit != nil
}

func pickVectorLimit(sortNode, scanNode, projNode *plan.Node) (*plan.Expr, *plan.RankOption) {
	limit, _, rankOption := pickVectorPagination(sortNode, scanNode, projNode)
	return limit, rankOption
}

func pickVectorPagination(sortNode, scanNode, projNode *plan.Node) (*plan.Expr, *plan.Expr, *plan.RankOption) {
	if sortNode.Limit != nil {
		return sortNode.Limit, sortNode.Offset, sortNode.RankOption
	}
	if scanNode.Limit != nil {
		return scanNode.Limit, scanNode.Offset, scanNode.RankOption
	}
	if projNode.Limit != nil {
		return projNode.Limit, projNode.Offset, projNode.RankOption
	}
	return nil, nil, nil
}

func (builder *QueryBuilder) resolveSortNode(node *plan.Node, depth int32) *plan.Node {
	if depth == 0 {
		if node.NodeType == plan.Node_SORT {
			return node
		}
		return nil
	}

	if node.NodeType == plan.Node_PROJECT && len(node.Children) == 1 {
		return builder.resolveSortNode(builder.qry.Nodes[node.Children[0]], depth-1)
	}

	return nil
}

func (builder *QueryBuilder) resolveScanNodeFromProject(node *plan.Node, depth int32) *plan.Node {
	if depth == 0 {
		if node.NodeType == plan.Node_TABLE_SCAN && node.TableDef.Indexes != nil {
			return node
		}
		return nil
	}

	if node.NodeType == plan.Node_PROJECT && len(node.Children) == 1 {
		return builder.resolveScanNodeFromProject(builder.qry.Nodes[node.Children[0]], depth-1)
	}

	return nil
}

func isDescendingVectorSort(flag plan.OrderBySpec_OrderByFlag) bool {
	return flag&plan.OrderBySpec_DESC != 0
}

func (builder *QueryBuilder) validateVectorIndexSortRewrite(vecCtx *vectorSortContext) (bool, error) {
	if vecCtx == nil {
		return true, nil
	}
	if !isDescendingVectorSort(vecCtx.sortDirection) {
		return true, nil
	}

	// IVF/HNSW candidate generation is nearest-neighbor oriented: using it for
	// DESC would pick near candidates first and then reverse-sort the reduced set,
	// which is not equivalent to a true farthest-neighbor query. Keep the original
	// execution path so the query naturally falls back to the exact/force behavior.
	return false, nil
}

// spliceVectorRewrite attaches the rewritten Top-K subtree (newRootID) where the original
// one was, and returns the node id the caller must hand back to applyIndices.
//
// Two anchors exist. When the rewrite was entered from the PROJECT directly above the
// Top-K sort, that project keeps its identity: its child pointer is repointed and any
// column remap is applied to it directly, so the returned id is unchanged.
//
// When the rewrite was entered from the SORT itself — the Top-K sits under an outer
// ORDER BY (#25967) or is a join input (#25974), so there is no project to mutate — the
// new subtree root is returned instead, and applyIndices' caller repoints the parent via
// `node.Children[i] = applyIndices(childID, ...)`. The remap goes into idxColMap rather
// than into one node, because the columns it renames (the CTE's distance output becoming
// the index score) are read by ancestors this function cannot see; applyIndices walks
// children first and then calls replaceColumnsForNode on each ancestor, so every consumer
// picks the mapping up on the way back out.
func (builder *QueryBuilder) spliceVectorRewrite(
	vecCtx *vectorSortContext,
	nodeID int32,
	newRootID int32,
	remap map[[2]int32]*plan.Expr,
	idxColMap map[[2]int32]*plan.Expr,
) int32 {
	if vecCtx.projNode != nil {
		vecCtx.projNode.Children[0] = newRootID
		if len(remap) > 0 {
			replaceColumnsForNode(vecCtx.projNode, remap)
		}
		return nodeID
	}
	for k, v := range remap {
		idxColMap[k] = v
	}
	return newRootID
}

// vectorChildProjectIsDetachable reports whether the PROJECT below the Top-K may be removed
// with its expressions republished to arbitrarily many ancestors.
//
// The distance entry is exempt: it is replaced by a single ColRef to the index score, so it
// is not duplicated however many ancestors read it. Every other entry is deep-copied per
// reference, which is only sound for something whose value does not depend on being
// evaluated once -- precisely what exprCanRemoveProject already decides.
func vectorChildProjectIsDetachable(childNode *plan.Node, orderExpr *plan.Expr) bool {
	sortIdx := -1
	if col := orderExpr.GetCol(); col != nil {
		sortIdx = int(col.ColPos)
	}
	for i, proj := range childNode.ProjectList {
		if i == sortIdx {
			continue
		}
		if !exprCanRemoveProject(proj) {
			return false
		}
	}
	return true
}

// vectorRemapForChildProject builds the column remap for the PROJECT that sits between the
// Top-K sort and the scan: the sorted-on column becomes the index score, and the rest pass
// through (optionally rewritten to the table function's columns for an index-only scan).
// Returns nil when there is no such project, in which case scanRemap applies as-is.
func vectorRemapForChildProject(
	childNode *plan.Node,
	orderExpr *plan.Expr,
	scoreExpr *plan.Expr,
	scanRemap map[[2]int32]*plan.Expr,
) map[[2]int32]*plan.Expr {
	if childNode == nil {
		return scanRemap
	}
	// sortIdx = -1 when the ORDER BY carries the distance call itself rather than a
	// reference to the child's projected column (buildVectorSortContextThroughJoin sets
	// childNode regardless of that form). There is then no column to replace with the
	// score, but the child is still being detached, so every other column it projects
	// must be remapped or ancestors keep dangling references to a node that has left the
	// plan.
	sortIdx := -1
	if col := orderExpr.GetCol(); col != nil {
		sortIdx = int(col.ColPos)
	}
	remap := make(map[[2]int32]*plan.Expr, len(childNode.ProjectList))
	for i, proj := range childNode.ProjectList {
		key := [2]int32{childNode.BindingTags[0], int32(i)}
		if i == sortIdx {
			remap[key] = DeepCopyExpr(scoreExpr)
			continue
		}
		if scanRemap != nil {
			remap[key] = replaceColumnsForExpr(DeepCopyExpr(proj), scanRemap)
			continue
		}
		remap[key] = proj
	}
	return remap
}

func (builder *QueryBuilder) stabilizeExactVectorSort(vecCtx *vectorSortContext) {
	if builder == nil || vecCtx == nil || vecCtx.sortNode == nil || vecCtx.scanNode == nil {
		return
	}
	sortNode := vecCtx.sortNode
	if len(sortNode.OrderBy) != 1 || len(sortNode.Children) != 1 {
		return
	}
	tableDef := vecCtx.scanNode.TableDef
	if tableDef == nil || tableDef.Pkey == nil {
		return
	}
	pkPos, ok := tableDef.Name2ColIndex[tableDef.Pkey.PkeyColName]
	if !ok || int(pkPos) >= len(tableDef.Cols) {
		return
	}
	var pkExpr *plan.Expr
	if vecCtx.childNode != nil && vecCtx.childNode.NodeType == plan.Node_PROJECT {
		pkExpr = builder.resolveProjectedVectorSortTiebreak(vecCtx.childNode, tableDef.Cols[pkPos].Typ, tableDef.Pkey.PkeyColName)
	} else {
		pkExpr = builder.buildPkExprFromNode(sortNode.Children[0], tableDef.Cols[pkPos].Typ, tableDef.Pkey.PkeyColName)
	}
	if pkExpr == nil {
		return
	}

	// Exact vector search keeps the original sort path. Add the primary key as a
	// deterministic tiebreaker so equal-distance top-k queries stay stable after
	// reload/compaction changes the physical scan order.
	sortNode.OrderBy = append(sortNode.OrderBy, &plan.OrderBySpec{Expr: pkExpr})
}

func (builder *QueryBuilder) resolveProjectedVectorSortTiebreak(projectNode *plan.Node, pkType plan.Type, pkName string) *plan.Expr {
	if builder == nil || projectNode == nil || projectNode.NodeType != plan.Node_PROJECT || len(projectNode.Children) != 1 || len(projectNode.BindingTags) == 0 {
		return nil
	}

	for idx, expr := range projectNode.ProjectList {
		col := expr.GetCol()
		if col == nil || builder.getColName(col) != pkName {
			continue
		}
		return &plan.Expr{
			Typ: pkType,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{
				RelPos: projectNode.BindingTags[0],
				ColPos: int32(idx),
				Name:   pkName,
			}},
		}
	}

	pkExpr := builder.buildPkExprFromNode(projectNode.Children[0], pkType, pkName)
	if pkExpr == nil {
		return nil
	}

	colPos := int32(len(projectNode.ProjectList))
	projectNode.ProjectList = append(projectNode.ProjectList, pkExpr)
	return &plan.Expr{
		Typ: pkType,
		Expr: &plan.Expr_Col{Col: &plan.ColRef{
			RelPos: projectNode.BindingTags[0],
			ColPos: colPos,
			Name:   pkName,
		}},
	}
}

// getDistRangeFromFilters peels filters of the shape `distfn(col, lit) <op> K`
// off the filter list and collects the bounds into a *plan.DistRange. The
// caller is expected to stash the returned DistRange onto the vector-index
// table function's IndexReaderParam so the predicate does not also re-run as a
// brute-force recompute on the base table scan after the JOIN.
//
// Applicable to any vector index (IVFFlat, CAGRA, IVFPQ) — caller passes the
// three bits of context needed to recognize its own `distfn(col, vec_lit)`
// expression.
func (builder *QueryBuilder) getDistRangeFromFilters(
	filters []*plan.Expr, partPos int32, origFuncName string, vecLitArg *plan.Expr,
) ([]*plan.Expr, *plan.DistRange) {
	var distRange *plan.DistRange

	currIdx := 0
	for _, filter := range filters {
		var (
			vecLit string
			fdist  *plan.Function
		)

		f := filter.GetF()
		if f == nil || len(f.Args) != 2 {
			goto NO_RANGE
		}

		fdist = f.Args[0].GetF()
		if fdist == nil || len(fdist.Args) != 2 {
			goto NO_RANGE
		}

		if partCol := fdist.Args[0].GetCol(); partCol == nil || partCol.ColPos != partPos {
			goto NO_RANGE
		}

		if fdist.Func.ObjName != origFuncName {
			goto NO_RANGE
		}

		vecLit = fdist.Args[1].GetLit().GetVecVal()
		if vecLit == "" || vecLit != vecLitArg.GetLit().GetVecVal() {
			goto NO_RANGE
		}

		// Fold every matching bound into the range, keeping the tightest bound per
		// side so the index enforces the intersection of all predicates regardless
		// of filter order (a looser same-side bound is redundant and dropped). If a
		// bound is not a comparable literal, the predicate is kept as a residual
		// filter instead. See issue #25639.
		switch f.Func.ObjName {
		case "<":
			if distRange == nil {
				distRange = &plan.DistRange{}
			}
			if !mergeUpperBound(distRange, f.Args[1], plan.BoundType_EXCLUSIVE) {
				goto NO_RANGE
			}

		case "<=":
			if distRange == nil {
				distRange = &plan.DistRange{}
			}
			if !mergeUpperBound(distRange, f.Args[1], plan.BoundType_INCLUSIVE) {
				goto NO_RANGE
			}

		case ">":
			if distRange == nil {
				distRange = &plan.DistRange{}
			}
			if !mergeLowerBound(distRange, f.Args[1], plan.BoundType_EXCLUSIVE) {
				goto NO_RANGE
			}

		case ">=":
			if distRange == nil {
				distRange = &plan.DistRange{}
			}
			if !mergeLowerBound(distRange, f.Args[1], plan.BoundType_INCLUSIVE) {
				goto NO_RANGE
			}

		default:
			goto NO_RANGE
		}

		continue

	NO_RANGE:
		filters[currIdx] = filter
		currIdx++
	}

	// If every matching predicate was non-literal (kept as a residual), the range
	// was allocated but never bounded; return nil so callers don't stash an empty
	// DistRange on the index reader.
	if distRange != nil &&
		distRange.LowerBoundType == plan.BoundType_UNBOUNDED &&
		distRange.UpperBoundType == plan.BoundType_UNBOUNDED {
		distRange = nil
	}

	return filters[:currIdx], distRange
}

// mergeUpperBound folds a new upper bound into dr, keeping the tighter (smaller,
// or exclusive on an equal value) bound so the range is the intersection of all
// upper bounds. The bound MUST be a numeric literal the index reader can
// evaluate; otherwise the predicate would be peeled off the filter list but the
// reader would later reject it and silently drop the constraint. It returns
// false for a non-literal bound (including the first one) so the caller keeps
// the predicate as a residual filter.
func mergeUpperBound(dr *plan.DistRange, bound *plan.Expr, boundType plan.BoundType) bool {
	newVal, ok := plan.GetLiteralFloat64(bound)
	if !ok {
		return false
	}
	if dr.UpperBoundType == plan.BoundType_UNBOUNDED {
		dr.UpperBoundType = boundType
		dr.UpperBound = bound
		return true
	}
	curVal, ok := plan.GetLiteralFloat64(dr.UpperBound)
	if !ok {
		return false
	}
	if newVal < curVal || (newVal == curVal && boundType == plan.BoundType_EXCLUSIVE) {
		dr.UpperBoundType = boundType
		dr.UpperBound = bound
	}
	return true
}

// mergeLowerBound folds a new lower bound into dr, keeping the tighter (larger,
// or exclusive on an equal value) bound. See mergeUpperBound.
func mergeLowerBound(dr *plan.DistRange, bound *plan.Expr, boundType plan.BoundType) bool {
	newVal, ok := plan.GetLiteralFloat64(bound)
	if !ok {
		return false
	}
	if dr.LowerBoundType == plan.BoundType_UNBOUNDED {
		dr.LowerBoundType = boundType
		dr.LowerBound = bound
		return true
	}
	curVal, ok := plan.GetLiteralFloat64(dr.LowerBound)
	if !ok {
		return false
	}
	if newVal > curVal || (newVal == curVal && boundType == plan.BoundType_EXCLUSIVE) {
		dr.LowerBoundType = boundType
		dr.LowerBound = bound
	}
	return true
}

// peelAndRewriteDistFnFilters scans `filters` for predicates of shape
// `origFuncName(col[partPos], vecLit) OP K` and, for each match:
//
//   - removes it from the returned remaining list so the base table scan no
//     longer re-evaluates the distance kernel;
//   - deep-copies the whole filter expression and swaps only `Args[0]`
//     (the distfn call) with a ColRef to the table function's score column
//     (RelPos=tableFuncTag, ColPos=1), leaving the comparison ObjRef and the
//     bound literal exactly as parsed (no rebind, no overload re-resolution,
//     no type coercion — so a `0.4` decimal literal stays `0.4`);
//   - returns the rewritten copy in `peeled` for the caller to append onto
//     `tableFuncNode.FilterList`. Node_FUNCTION_SCAN honors FilterList via
//     compileRestrict (pkg/sql/compile/compile.go Node_FUNCTION_SCAN case).
//
// Supported operators: `<`, `<=`, `>`, `>=`.
func (builder *QueryBuilder) peelAndRewriteDistFnFilters(
	filters []*plan.Expr,
	partPos int32, origFuncName string, vecLitArg *plan.Expr,
	tableFuncTag int32, scoreColType plan.Type,
) (remaining, peeled []*plan.Expr) {
	makeScoreCol := func() *plan.Expr {
		return &plan.Expr{
			Typ: scoreColType,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{RelPos: tableFuncTag, ColPos: 1, Name: "score"},
			},
		}
	}

	currIdx := 0
	for _, filter := range filters {
		var (
			vecLit string
			fdist  *plan.Function
		)

		f := filter.GetF()
		if f == nil || len(f.Args) != 2 {
			goto KEEP
		}
		switch f.Func.ObjName {
		case "<", "<=", ">", ">=":
		default:
			goto KEEP
		}

		fdist = f.Args[0].GetF()
		if fdist == nil || len(fdist.Args) != 2 {
			goto KEEP
		}
		if fdist.Func.ObjName != origFuncName {
			goto KEEP
		}
		if partCol := fdist.Args[0].GetCol(); partCol == nil || partCol.ColPos != partPos {
			goto KEEP
		}
		vecLit = fdist.Args[1].GetLit().GetVecVal()
		if vecLit == "" || vecLit != vecLitArg.GetLit().GetVecVal() {
			goto KEEP
		}

		{
			rewritten := DeepCopyExpr(filter)
			rewritten.GetF().Args[0] = makeScoreCol()
			peeled = append(peeled, rewritten)
		}
		continue

	KEEP:
		filters[currIdx] = filter
		currIdx++
	}
	return filters[:currIdx], peeled
}

// replaceDistFnExprsWithScoreCol walks each expression in exprs and substitutes every distance call
// on the base scan's vector column that uses the INDEX's distance function (origFuncName) with a
// direct ColRef to the table function's score column (RelPos=tableFuncTag, ColPos=1).
//
// Use this on SELECT-side projections so the user's `l2_distance(ec, ?)` — including one WRAPPED by a
// scalar (CAST/ROUND/arithmetic) or bound to an alias — reuses the table function's pre-computed
// score instead of leaving an orphaned ColRef to the base scan's vector column (which the base-scan
// removal cannot remap: "cannot find column reference", issue #26961) and re-running the distance
// kernel per row. The existing `replaceColumnsForNode` path only handles ORDER BY on an alias whose
// distance is the sortIdx entry in childNode.ProjectList; this walker covers the other combinations.
//
// A candidate distance (right column + origFuncName metric) is rewritten only when it is against the
// SAME query vector as vecLitArg — see sameQueryVector. This is a real value comparison: a distance
// on the same column but a DIFFERENT vector must NOT become this index's score (that would silently
// report the wrong distance).
func replaceDistFnExprsWithScoreCol(
	exprs []*plan.Expr,
	scanBindingTag, partPos int32,
	origFuncName string,
	vecLitArg *plan.Expr,
	tableFuncTag int32,
	scoreColType plan.Type,
) {
	for i := range exprs {
		exprs[i] = replaceDistFnInExpr(exprs[i], scanBindingTag, partPos,
			origFuncName, vecLitArg, tableFuncTag, scoreColType)
	}
}

func replaceDistFnInExpr(
	expr *plan.Expr,
	scanBindingTag, partPos int32,
	origFuncName string,
	vecLitArg *plan.Expr,
	tableFuncTag int32,
	scoreColType plan.Type,
) *plan.Expr {
	if expr == nil {
		return expr
	}
	if isVectorDistanceExpr(expr, scanBindingTag, partPos) && expr.GetF().Func.ObjName == origFuncName &&
		sameQueryVector(expr.GetF(), scanBindingTag, partPos, vecLitArg) {
		return &plan.Expr{
			Typ: scoreColType,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{RelPos: tableFuncTag, ColPos: 1, Name: "score"},
			},
		}
	}
	switch e := expr.Expr.(type) {
	case *plan.Expr_F:
		for i, arg := range e.F.Args {
			e.F.Args[i] = replaceDistFnInExpr(arg, scanBindingTag, partPos,
				origFuncName, vecLitArg, tableFuncTag, scoreColType)
		}
	case *plan.Expr_List:
		for i, sub := range e.List.List {
			e.List.List[i] = replaceDistFnInExpr(sub, scanBindingTag, partPos,
				origFuncName, vecLitArg, tableFuncTag, scoreColType)
		}
	}
	return expr
}

// sameQueryVector reports whether the distance function fn (already known to reference the base scan's
// vector column and use the index's metric) is computed against the SAME query vector as vecLitArg —
// the vector the index/ORDER BY search key uses. The query vector is the arg that is NOT the base-scan
// column. A distance on a DIFFERENT vector must NOT be rewritten to this index's score (it would
// silently report the wrong distance — 1 != 2). The comparison is a pure, executor-free parse done at
// the ACTUAL vector element type (vecLitArg.Typ): both the folded vecLitArg (VecVal) and the
// possibly-unfolded SELECT-side cast('[...]') resolve to the same canonical byte encoding, so format
// differences don't matter while distinct vectors never collide. A non-constant (param) query vector
// yields no key and is left alone (fail-safe).
func sameQueryVector(fn *plan.Function, scanBindingTag, partPos int32, vecLitArg *plan.Expr) bool {
	if fn == nil || len(fn.Args) != 2 || vecLitArg == nil {
		return false
	}
	elemType := types.T(vecLitArg.Typ.GetId())
	litKey, ok := vecFloatKey(vecLitArg, elemType)
	if !ok {
		return false
	}
	// The query vector is whichever arg is not the base-scan column.
	vecArg := fn.Args[1]
	if c := fn.Args[0].GetCol(); c == nil || c.RelPos != scanBindingTag || c.ColPos != partPos {
		vecArg = fn.Args[0]
	}
	argKey, ok := vecFloatKey(vecArg, elemType)
	return ok && argKey == litKey
}

// vecFloatKey parses a vector-literal expression into a canonical BYTE key so two references to the
// same query vector compare equal regardless of how each is encoded, decoding at the ACTUAL element
// type (elemType) rather than treating every non-f64 vector as float32. It unwraps a value-preserving
// CAST (one whose input is not itself a vector — a vector-to-vector cast changes the value and is not
// peeled; see the loop below), then:
//   - a constant-folded vector literal already stores the raw element bytes in VecVal; those bytes ARE
//     the canonical type-exact key and are compared directly (decoding narrow/int bytes as float32
//     collapses distinct vectors onto a shared NaN string — e.g. vecuint8 [0,0,192,127] and
//     [1,0,192,127] both become "[NaN]");
//   - an unfolded textual literal (the inner "[...]" of cast('[...]' as vec<T>)) is parsed at elemType
//     via the length-checked StringToArrayToBytes (returns an error — never panics — on empty /
//     whitespace / malformed input) and re-encoded to the same byte form.
//
// Returns ok=false for a non-constant (param), unsupported-type, or unparseable argument (fail-safe:
// no rewrite).
func vecFloatKey(e *plan.Expr, elemType types.T) (string, bool) {
	for e != nil {
		f := e.GetF()
		if f == nil || f.Func.ObjName != "cast" || len(f.Args) < 1 {
			break
		}
		// Only a cast whose INPUT is NOT itself a vector may be peeled — that is the textual
		// cast('[...]' as vec<T>) shape, where the inner literal spells the query vector verbatim and
		// parsing it at elemType reproduces the cast exactly. A vector-to-vector cast CONVERTS the
		// value (vecf16('[1.001]') is 0x3c01, but vecbf16('[1.001]') re-cast to vecf16 is 0x3c00), so
		// peeling it would parse the inner literal at the wrong type and equate two DIFFERENT vectors,
		// silently rewriting the SELECT distance to a score computed for the other one. Stop and yield
		// no key (fail-safe: no rewrite) rather than guess at the conversion.
		if vecElemByteSize(types.T(f.Args[0].Typ.GetId())) != 0 {
			return "", false
		}
		e = f.Args[0]
	}
	lit := e.GetLit()
	if lit == nil {
		return "", false
	}
	elemSize := vecElemByteSize(elemType)
	if elemSize == 0 {
		return "", false
	}
	// Folded vector literal: the raw element bytes are the canonical, type-exact key. Guard the length
	// against the element size so a malformed literal yields no key (fail-safe) rather than a bad decode.
	if raw := lit.GetVecVal(); raw != "" {
		if len(raw)%elemSize != 0 {
			return "", false
		}
		return raw, true
	}
	// Unfolded textual literal: "[...]" text in Sval, parsed at elemType into the same byte form.
	sv, ok := lit.Value.(*plan.Literal_Sval)
	if !ok {
		return "", false
	}
	b, ok := vecTextToBytes(elemType, sv.Sval)
	if !ok {
		return "", false
	}
	return string(b), true
}

// vecElemByteSize returns the byte width of a single element of a vector array type, or 0 for a
// non-vector / unsupported type (fail-safe: callers treat 0 as "no key").
func vecElemByteSize(t types.T) int {
	switch t {
	case types.T_array_float32:
		return 4
	case types.T_array_float64:
		return 8
	case types.T_array_bf16, types.T_array_float16:
		return 2
	case types.T_array_int8, types.T_array_uint8:
		return 1
	default:
		return 0
	}
}

// vecTextToBytes parses an unfolded "[...]" literal at the given vector element type into the folded
// byte encoding, using the length-checked StringToArrayToBytes (returns an error — never panics — on
// empty/whitespace/malformed input). ok=false on any parse failure (fail-safe: no rewrite).
func vecTextToBytes(t types.T, s string) ([]byte, bool) {
	var b []byte
	var err error
	switch t {
	case types.T_array_float32:
		b, err = types.StringToArrayToBytes[float32](s)
	case types.T_array_float64:
		b, err = types.StringToArrayToBytes[float64](s)
	case types.T_array_bf16:
		b, err = types.StringToArrayToBytes[types.BF16](s)
	case types.T_array_float16:
		b, err = types.StringToArrayToBytes[types.Float16](s)
	case types.T_array_int8:
		b, err = types.StringToArrayToBytes[int8](s)
	case types.T_array_uint8:
		b, err = types.StringToArrayToBytes[uint8](s)
	default:
		return nil, false
	}
	if err != nil {
		return nil, false
	}
	return b, true
}

// exprCallsFunc reports whether expr calls fnName anywhere inside it, including nested in
// another call's arguments or inside an expression list.
//
// Used to spot a predicate that WRAPS an index placeholder rather than being one --
// `MATCH(...) > 0`, `ROUND(l2_distance(...), 2) < 5` -- which the "is this expression exactly
// the placeholder?" tests used elsewhere step straight past.
func exprCallsFunc(expr *plan.Expr, fnName string) bool {
	if expr == nil {
		return false
	}
	switch e := expr.Expr.(type) {
	case *plan.Expr_F:
		if e.F.Func != nil && e.F.Func.ObjName == fnName {
			return true
		}
		for _, arg := range e.F.Args {
			if exprCallsFunc(arg, fnName) {
				return true
			}
		}
	case *plan.Expr_List:
		for _, sub := range e.List.List {
			if exprCallsFunc(sub, fnName) {
				return true
			}
		}
	}
	return false
}

// replaceScoreFnInExprBy walks expr and offers every function call to rewrite. A non-nil
// return replaces that call; nil leaves it in place and the walk descends into its arguments.
// Returns the rewritten expression.
//
// Letting the callback see the whole *plan.Function -- not just its name -- is what lets a
// caller with several candidate index scans decide WHICH one a given call belongs to, and
// leave alone the ones no scan answers.
func replaceScoreFnInExprBy(expr *plan.Expr, rewrite func(*plan.Function) *plan.Expr) *plan.Expr {
	if expr == nil {
		return expr
	}
	switch e := expr.Expr.(type) {
	case *plan.Expr_F:
		if repl := rewrite(e.F); repl != nil {
			return repl
		}
		for i, arg := range e.F.Args {
			e.F.Args[i] = replaceScoreFnInExprBy(arg, rewrite)
		}
	case *plan.Expr_List:
		for i, sub := range e.List.List {
			e.List.List[i] = replaceScoreFnInExprBy(sub, rewrite)
		}
	}
	return expr
}
