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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// indexSearchAlias returns prefix_N for the smallest N not bound in ctx; the
// branches of an ADAPTIVE_TOP region bind their index searches in one context.
func indexSearchAlias(ctx *BindContext, prefix string) string {
	for n := 0; ; n++ {
		alias := fmt.Sprintf("%s_%d", prefix, n)
		if _, ok := ctx.bindingByTable[alias]; !ok {
			return alias
		}
	}
}

func (builder *QueryBuilder) adaptiveVectorReplaySafe(root int32) bool {
	visited := make(map[int32]struct{})
	var safe func(int32) bool
	safe = func(id int32) bool {
		if id < 0 || int(id) >= len(builder.qry.Nodes) {
			return false
		}
		if _, ok := visited[id]; ok {
			return true
		}
		visited[id] = struct{}{}
		node := builder.qry.Nodes[id]
		if containsSequenceNodeExpressions(node) {
			return false
		}
		exprLists := [][]*plan.Expr{
			node.ProjectList, node.OnList, node.FilterList, node.GroupBy,
			node.AggList, node.WinSpecList, node.TblFuncExprList,
			node.BlockFilterList, node.FillVal, node.OnUpdateExprs,
		}
		for _, exprs := range exprLists {
			for _, expr := range exprs {
				if ContainsVolatileFunction(expr) {
					return false
				}
			}
		}
		for _, spec := range node.OrderBy {
			if spec != nil && ContainsVolatileFunction(spec.Expr) {
				return false
			}
		}
		for _, child := range node.Children {
			if !safe(child) {
				return false
			}
		}
		return true
	}
	return safe(root)
}

func (builder *QueryBuilder) adaptiveVectorTopSupported(nodeID int32, vecCtx *vectorSortContext, allowMultiColumn bool) bool {
	return vecCtx != nil && vecCtx.projNode != nil && len(vecCtx.projNode.ProjectList) > 0 &&
		(allowMultiColumn || len(vecCtx.projNode.ProjectList) == 1) &&
		!vecCtx.hasMembership && vecCtx.providerNodeID < 0 &&
		builder.adaptiveVectorReplaySafe(nodeID)
}

// adaptiveVectorBranches builds the index branches of an ADAPTIVE_TOP region. post
// rebuilds the copy annotated "post"; pre, when set, the copy annotated "pre". Each
// returns the root of its rewritten copy.
type adaptiveVectorBranches struct {
	post func(root int32, vecCtx *vectorSortContext, idxColMap map[[2]int32]*plan.Expr) (int32, error)
	pre  func(root int32, vecCtx *vectorSortContext, idxColMap map[[2]int32]*plan.Expr) (int32, error)
}

// buildAdaptiveVectorTop replaces the vector top-k region at nodeID with ADAPTIVE_TOP
// over its post, pre and force copies; fallbackOnEmpty builds [post, force] where a
// non-empty post page wins. Without branches.pre it builds [post, force] where a full
// post page wins.
func (builder *QueryBuilder) buildAdaptiveVectorTop(
	nodeID int32,
	vecCtx *vectorSortContext,
	idxColMap map[[2]int32]*plan.Expr,
	branches adaptiveVectorBranches,
	fallbackOnEmpty bool,
) (int32, error) {
	ctx := builder.ctxByNode[nodeID]
	// Replay requires an explicit positional output boundary and no external
	// membership/provider dependency. A SORT alone has no output schema before
	// remapping; copying its empty ProjectList cannot define candidate layouts.
	// Keep unsupported regions intact and exact, before cloning or rewriting tags.
	if !builder.adaptiveVectorTopSupported(nodeID, vecCtx, fallbackOnEmpty) {
		builder.forceAdaptiveVectorRegion(vecCtx)
		return nodeID, nil
	}
	withPre := !fallbackOnEmpty && branches.pre != nil
	var preRoot int32
	if withPre {
		preRoot = builder.copyNode(ctx, nodeID)
	}
	forceRoot := builder.copyNode(ctx, nodeID)

	setVectorMode := func(root int32, mode string) {
		visited := make(map[int32]struct{})
		var walk func(int32)
		walk = func(id int32) {
			if _, ok := visited[id]; ok || id < 0 || int(id) >= len(builder.qry.Nodes) {
				return
			}
			visited[id] = struct{}{}
			node := builder.qry.Nodes[id]
			if node.RankOption != nil || node.NodeType == plan.Node_SORT ||
				node.NodeType == plan.Node_PROJECT || node.NodeType == plan.Node_TABLE_SCAN {
				node.RankOption = DeepCopyRankOption(node.RankOption)
				if node.RankOption == nil {
					node.RankOption = &plan.RankOption{}
				}
				node.RankOption.Mode = mode
			}
			for _, child := range node.Children {
				walk(child)
			}
		}
		walk(root)
	}
	contextFor := func(root int32) *vectorSortContext {
		if vecCtx.projNode != nil {
			return builder.buildVectorSortContext(builder.qry.Nodes[root])
		}
		return builder.buildVectorSortContextFromSort(builder.qry.Nodes[root])
	}

	setVectorMode(nodeID, "post")
	postCtx := contextFor(nodeID)
	if postCtx == nil {
		return nodeID, moerr.NewInternalErrorNoCtx("cannot rebuild adaptive POST vector context")
	}
	postMap := make(map[[2]int32]*plan.Expr)
	postRoot, err := branches.post(nodeID, postCtx, postMap)
	if err != nil {
		return nodeID, err
	}

	setVectorMode(forceRoot, "force")
	if withPre {
		setVectorMode(preRoot, "pre")
		preCtx := contextFor(preRoot)
		if preCtx == nil {
			return nodeID, moerr.NewInternalErrorNoCtx("cannot rebuild adaptive PRE vector context")
		}
		preRoot, err = branches.pre(preRoot, preCtx, make(map[[2]int32]*plan.Expr))
		if err != nil {
			return nodeID, err
		}
	}
	for key, expr := range postMap {
		idxColMap[key] = expr
	}

	resultLimit, _ := vectorResultPagination(vecCtx)
	if resultLimit == nil {
		return nodeID, moerr.NewInternalErrorNoCtx("adaptive vector result limit is missing")
	}
	postNode := builder.qry.Nodes[postRoot]
	adaptive := &plan.Node{
		NodeType:                   plan.Node_ADAPTIVE_TOP,
		Children:                   []int32{postRoot, forceRoot},
		Limit:                      DeepCopyExpr(resultLimit),
		ProjectList:                DeepCopyExprList(postNode.ProjectList),
		BindingTags:                append([]int32(nil), postNode.BindingTags...),
		Stats:                      DeepCopyStats(postNode.Stats),
		AdaptiveTopFallbackOnEmpty: fallbackOnEmpty,
	}
	if withPre {
		adaptive.Children = []int32{postRoot, preRoot, forceRoot}
	}
	if adaptive.Stats != nil && resultLimit.GetLit() != nil {
		limit := resultLimit.GetLit().GetU64Val()
		if adaptive.Stats.Outcnt > float64(limit) {
			adaptive.Stats.Outcnt = float64(limit)
		}
	}
	return builder.appendNode(adaptive, ctx), nil
}

// buildAdaptiveModeTop builds mode=auto for an algorithm whose plan rewrite is branch:
// ADAPTIVE_TOP [post, pre, force], or [post, force] without withPre, where pre would
// equal post. A region that is small, not replay-safe or not supported by
// ADAPTIVE_TOP runs force.
func (builder *QueryBuilder) buildAdaptiveModeTop(
	nodeID int32,
	vecCtx *vectorSortContext,
	idxColMap map[[2]int32]*plan.Expr,
	branch func(root int32, vecCtx *vectorSortContext, idxColMap map[[2]int32]*plan.Expr) (int32, error),
	withPre bool,
) (int32, error) {
	if builder.shouldUseForceMode(vecCtx) || !builder.adaptiveVectorReplaySafe(nodeID) {
		builder.forceAdaptiveVectorRegion(vecCtx)
		return nodeID, nil
	}
	branches := adaptiveVectorBranches{post: branch}
	if withPre {
		branches.pre = branch
	}
	return builder.buildAdaptiveVectorTop(nodeID, vecCtx, idxColMap, branches, false)
}
