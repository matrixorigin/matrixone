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

func (builder *QueryBuilder) buildVectorSortContext(projNode *plan.Node) *vectorSortContext {
	sortNode := builder.resolveSortNode(projNode, 1)
	if sortNode == nil || len(sortNode.OrderBy) != 1 {
		return nil
	}

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
			distFnExpr = childNode.ProjectList[orderExpr.GetCol().ColPos].GetF()
		}
		if distFnExpr == nil {
			return nil
		}
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
		if indexplugin.IsVectorIndexAlgo(idx.IndexAlgo) {
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
		if idx.Unique && filterListHasConstEqualityOnCols(node.FilterList, node.TableDef, tag, idx.Parts) {
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

		switch f.Func.ObjName {
		case "<":
			if distRange == nil {
				distRange = &plan.DistRange{}
			}
			if distRange.UpperBoundType != plan.BoundType_UNBOUNDED {
				goto NO_RANGE
			}
			distRange.UpperBoundType = plan.BoundType_EXCLUSIVE
			distRange.UpperBound = f.Args[1]

		case "<=":
			if distRange == nil {
				distRange = &plan.DistRange{}
			}
			if distRange.UpperBoundType != plan.BoundType_UNBOUNDED {
				goto NO_RANGE
			}
			distRange.UpperBoundType = plan.BoundType_INCLUSIVE
			distRange.UpperBound = f.Args[1]

		case ">":
			if distRange == nil {
				distRange = &plan.DistRange{}
			}
			if distRange.LowerBoundType != plan.BoundType_UNBOUNDED {
				goto NO_RANGE
			}
			distRange.LowerBoundType = plan.BoundType_EXCLUSIVE
			distRange.LowerBound = f.Args[1]

		case ">=":
			if distRange == nil {
				distRange = &plan.DistRange{}
			}
			if distRange.LowerBoundType != plan.BoundType_UNBOUNDED {
				goto NO_RANGE
			}
			distRange.LowerBoundType = plan.BoundType_INCLUSIVE
			distRange.LowerBound = f.Args[1]

		default:
			goto NO_RANGE
		}

		continue

	NO_RANGE:
		filters[currIdx] = filter
		currIdx++
	}

	return filters[:currIdx], distRange
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

// replaceDistFnExprsWithScoreCol walks each expression in exprs and substitutes
// every `origFuncName(col[partPos, scanBindingTag], vecLit)` call with a direct
// ColRef to the table function's score column (RelPos=tableFuncTag, ColPos=1).
//
// Use this on SELECT-side projections so the user's `l2_distance(ec, ?) AS dist`
// reuses the table function's pre-computed score instead of re-running the
// distance kernel on every scanned row. The existing `replaceColumnsForNode`
// path only handles the case where ORDER BY uses an alias and the aliased
// distance expression is the sortIdx entry in childNode.ProjectList; this
// walker covers the other combinations.
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
	switch e := expr.Expr.(type) {
	case *plan.Expr_F:
		f := e.F
		if f.Func.ObjName == origFuncName && len(f.Args) == 2 {
			col := f.Args[0].GetCol()
			lit := f.Args[1].GetLit()
			if col != nil && col.ColPos == partPos && col.RelPos == scanBindingTag &&
				lit != nil && vecLitArg.GetLit() != nil &&
				lit.GetVecVal() != "" && lit.GetVecVal() == vecLitArg.GetLit().GetVecVal() {
				return &plan.Expr{
					Typ: scoreColType,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{RelPos: tableFuncTag, ColPos: 1, Name: "score"},
					},
				}
			}
		}
		for i, arg := range f.Args {
			f.Args[i] = replaceDistFnInExpr(arg, scanBindingTag, partPos,
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
