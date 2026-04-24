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

import "github.com/matrixorigin/matrixone/pkg/pb/plan"

type vectorSortContext struct {
	projNode      *plan.Node
	sortNode      *plan.Node
	scanNode      *plan.Node
	childNode     *plan.Node
	orderExpr     *plan.Expr
	distFnExpr    *plan.Function
	sortDirection plan.OrderBySpec_OrderByFlag
	limit         *plan.Expr
	rankOption    *plan.RankOption
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

	limit, rankOption := pickVectorLimit(sortNode, scanNode, projNode)
	if limit == nil {
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
		limit:         limit,
		rankOption:    rankOption,
	}
}

func pickVectorLimit(sortNode, scanNode, projNode *plan.Node) (*plan.Expr, *plan.RankOption) {
	if sortNode.Limit != nil {
		return sortNode.Limit, sortNode.RankOption
	}
	if scanNode.Limit != nil {
		return scanNode.Limit, scanNode.RankOption
	}
	if projNode.Limit != nil {
		return projNode.Limit, projNode.RankOption
	}
	return nil, nil
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
	if vecCtx == nil || !isDescendingVectorSort(vecCtx.sortDirection) {
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
			distRange.UpperBoundType = plan.BoundType_EXCLUSIVE
			distRange.UpperBound = f.Args[1]

		case "<=":
			if distRange == nil {
				distRange = &plan.DistRange{}
			}
			distRange.UpperBoundType = plan.BoundType_INCLUSIVE
			distRange.UpperBound = f.Args[1]

		case ">":
			if distRange == nil {
				distRange = &plan.DistRange{}
			}
			distRange.LowerBoundType = plan.BoundType_EXCLUSIVE
			distRange.LowerBound = f.Args[1]

		case ">=":
			if distRange == nil {
				distRange = &plan.DistRange{}
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
