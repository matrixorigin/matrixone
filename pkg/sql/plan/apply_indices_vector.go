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
	pkExpr := builder.buildPkExprFromNode(sortNode.Children[0], tableDef.Cols[pkPos].Typ, tableDef.Pkey.PkeyColName)
	if pkExpr == nil {
		pkExpr = builder.projectVectorSortTiebreak(vecCtx.childNode, tableDef.Cols[pkPos].Typ, tableDef.Pkey.PkeyColName)
	}
	if pkExpr == nil {
		return
	}

	// Exact vector search keeps the original sort path. Add the primary key as a
	// deterministic tiebreaker so equal-distance top-k queries stay stable after
	// reload/compaction changes the physical scan order.
	sortNode.OrderBy = append(sortNode.OrderBy, &plan.OrderBySpec{Expr: pkExpr})
}

func (builder *QueryBuilder) projectVectorSortTiebreak(projectNode *plan.Node, pkType plan.Type, pkName string) *plan.Expr {
	if builder == nil || projectNode == nil || projectNode.NodeType != plan.Node_PROJECT || len(projectNode.Children) != 1 || len(projectNode.BindingTags) == 0 {
		return nil
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
