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
	indexplugin "github.com/matrixorigin/matrixone/pkg/indexplugin"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// 至多一行并不意味着非 NULL；此上下文只能用于安装运行期选择器。
func (builder *QueryBuilder) scalarVectorContext(proj *plan.Node) (*vectorSortContext, *plan.Node) {
	if builder.sqlCalcFoundRows || proj == nil || proj.NodeType != plan.Node_PROJECT || len(proj.ProjectList) == 0 {
		return nil, nil
	}
	sort := builder.resolveSortNode(proj, 1)
	if sort == nil || len(sort.OrderBy) != 1 || sort.Limit == nil || len(sort.Children) != 1 ||
		isDescendingVectorSort(sort.OrderBy[0].Flag) || !vectorCandidateModeSupported(sort.RankOption) {
		return nil, nil
	}
	join := builder.qry.Nodes[sort.Children[0]]
	if join.NodeType != plan.Node_JOIN || len(join.Children) != 2 ||
		(join.JoinType != plan.Node_SINGLE && join.JoinType != plan.Node_LEFT) ||
		!isTrivialJoinOnList(join.OnList) || len(join.FilterList) != 0 || join.Limit != nil || join.Offset != nil {
		return nil, nil
	}
	scan := builder.directScanWithVectorIndex(builder.qry.Nodes[join.Children[0]])
	provider := builder.qry.Nodes[join.Children[1]]
	if scan == nil || scan.TableDef.Partition != nil || !builder.isSingleRowVectorProvider(provider) || !builder.scalarVectorProviderSafe(provider) {
		return nil, nil
	}
	fn := sort.OrderBy[0].Expr.GetF()
	providerTags := builder.collectBindingTags(provider)
	arg := extractJoinThroughProviderVectorArg(fn, scan.BindingTags[0], builder.collectBindingTags(scan), providerTags)
	if arg == nil || !builder.isJoinThroughProjectionSafe(proj, nil, sort.OrderBy[0].Expr, providerTags) {
		return nil, nil
	}
	limit, offset, _ := pickVectorPagination(sort, scan, proj)
	candidate, ok := buildCandidateLimit(limit, offset)
	if !ok {
		return nil, nil
	}
	return &vectorSortContext{
		projNode: proj, sortNode: sort, scanNode: scan,
		orderExpr: sort.OrderBy[0].Expr, distFnExpr: fn, sortDirection: sort.OrderBy[0].Flag,
		limit: candidate, resultLimit: DeepCopyExpr(limit), resultOffset: DeepCopyExpr(offset),
		rankOption: &plan.RankOption{Mode: "pre"}, providerNodeID: provider.NodeId, vecArgExpr: arg, membershipNodeID: -1,
	}, join
}

func (builder *QueryBuilder) scalarVectorProviderSafe(node *plan.Node) bool {
	for _, expr := range node.FilterList {
		if vectorCandidateExprUnsafe(expr) {
			return false
		}
	}
	switch node.NodeType {
	case plan.Node_TABLE_SCAN:
		if node.TableDef == nil || node.TableDef.Partition != nil {
			return false
		}
		for _, filter := range node.FilterList {
			fn := filter.GetF()
			if fn != nil && fn.Func != nil && fn.Func.ObjName == "=" && len(fn.Args) == 2 && fn.Args[0].Typ.Id != fn.Args[1].Typ.Id {
				return false
			}
		}
		return true
	case plan.Node_PROJECT, plan.Node_SORT:
		if len(node.Children) != 1 {
			return false
		}
		for _, expr := range node.ProjectList {
			if expr.GetCol() == nil {
				return false
			}
		}
		return builder.scalarVectorProviderSafe(builder.qry.Nodes[node.Children[0]])
	default:
		return false
	}
}

func (builder *QueryBuilder) detectScalarVectorGuard(proj *plan.Node) []int32 {
	vctx, _ := builder.scalarVectorContext(proj)
	if vctx == nil || !builder.vectorCandidateRegionSafe(proj.NodeId) {
		return nil
	}
	indexes, err := builder.collectVectorIndexes(vctx.scanNode)
	if err != nil {
		return nil
	}
	probe := *vctx
	probe.projNode, probe.scanNode, probe.sortNode = DeepCopyNode(vctx.projNode), DeepCopyNode(vctx.scanNode), DeepCopyNode(vctx.sortNode)
	// 插件探测会对查询向量作类型对齐，不能改写原 provider 的可空性。
	probe.orderExpr = DeepCopyExpr(vctx.orderExpr)
	probe.distFnExpr = probe.orderExpr.GetF()
	probe.vecArgExpr = DeepCopyExpr(vctx.vecArgExpr)
	for _, index := range indexes {
		plugin, ok := indexplugin.Get(index.IndexAlgo)
		if !ok {
			continue
		}
		vc, mti := toPlanplugin(&probe, index)
		if applicable, err := plugin.Plan().CanApply(builder, vc, mti); err == nil && applicable {
			ids := []int32{vctx.scanNode.NodeId}
			p := builder.qry.Nodes[vctx.providerNodeID]
			for len(p.Children) == 1 {
				p = builder.qry.Nodes[p.Children[0]]
			}
			return append(ids, p.NodeId)
		}
	}
	return nil
}

func (builder *QueryBuilder) applyScalarVectorIndex(nodeID int32) (int32, error) {
	original := builder.qry.Nodes[nodeID]
	vctx, _ := builder.scalarVectorContext(original)
	if vctx == nil || !builder.vectorCandidateRegionSafe(nodeID) {
		return nodeID, nil
	}
	ctx := builder.ctxByNode[nodeID]
	annRoot := builder.copyNode(ctx, nodeID)
	ann, annJoin := builder.scalarVectorContext(builder.qry.Nodes[annRoot])
	if ann == nil {
		return nodeID, nil
	}
	// 外层分页属于结果关系，不能复制 LIMIT 0 后把 source reader 优化掉。
	ann.projNode.Limit, ann.projNode.Offset = nil, nil
	sourceID, sourceTag := builder.genNewBindTag(), builder.genNewBindTag()
	queryType := vctx.vecArgExpr.Typ
	queryType.NotNullable = false
	newSource := func() int32 {
		return builder.appendNode(&plan.Node{
			NodeType: plan.Node_VECTOR_QUERY_SOURCE, VectorQuerySourceId: sourceID,
			BindingTags: []int32{sourceTag}, Stats: &plan.Stats{Outcnt: 1, TableCnt: 1, Selectivity: 1},
			TableDef: &plan.TableDef{Cols: []*plan.ColDef{{Name: "query_vector", Typ: queryType}}},
		}, ctx)
	}
	newArg := &plan.Expr{Typ: queryType, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: sourceTag, ColPos: 0}}}
	oldCol := vctx.vecArgExpr.GetCol()
	remap := map[[2]int32]*plan.Expr{{oldCol.RelPos, oldCol.ColPos}: newArg}
	var replace func(int32)
	replace = func(id int32) {
		n := builder.qry.Nodes[id]
		replaceColumnsForNode(n, remap)
		for _, child := range n.Children {
			replace(child)
		}
	}
	annSource := newSource()
	annJoin.Children[1] = annSource
	replace(annRoot)
	ann.providerNodeID, ann.vecArgExpr = annSource, newArg
	ann.distFnExpr = ann.sortNode.OrderBy[0].Expr.GetF()
	refs := make(map[[2]int32]int)
	builder.countColRefs(annRoot, refs)
	var applied bool
	var err error
	annRoot, applied, err = builder.applyVectorIndexForSortContext(annRoot, ann, refs, make(map[[2]int32]*plan.Expr))
	if err != nil || !applied {
		return nodeID, err
	}
	exactRoot := builder.copyNode(ctx, nodeID)
	exact, exactJoin := builder.scalarVectorContext(builder.qry.Nodes[exactRoot])
	exact.projNode.Limit, exact.projNode.Offset = nil, nil
	exactJoin.Children[1] = newSource()
	replace(exactRoot)
	// fallback 保留原关系，但不允许再进入任何向量改写或 AUTO。
	exact.sortNode.RankOption = &plan.RankOption{Mode: "force"}
	exact.scanNode.RankOption = &plan.RankOption{Mode: "force"}
	exact.projNode.RankOption = &plan.RankOption{Mode: "force"}
	provider := builder.appendNode(&plan.Node{
		NodeType: plan.Node_PROJECT, Children: []int32{vctx.providerNodeID},
		ProjectList: []*plan.Expr{DeepCopyExpr(vctx.vecArgExpr)}, BindingTags: []int32{builder.genNewBindTag()},
	}, ctx)
	annNode := builder.qry.Nodes[annRoot]
	selector := &plan.Node{
		NodeType: plan.Node_VECTOR_QUERY_TOP, VectorQuerySourceId: sourceID,
		Children: []int32{provider, annRoot, exactRoot}, Limit: DeepCopyExpr(vctx.resultLimit),
		ProjectList: DeepCopyExprList(original.ProjectList), BindingTags: append([]int32(nil), annNode.BindingTags...),
		Stats: DeepCopyStats(annNode.Stats),
	}
	if original.Limit == nil && original.Offset == nil {
		return builder.appendNode(selector, ctx), nil
	}
	selectorTag := builder.genNewBindTag()
	selector.BindingTags = []int32{selectorTag}
	selectorID := builder.appendNode(selector, ctx)
	projection := make([]*plan.Expr, len(original.ProjectList))
	for i, expr := range original.ProjectList {
		projection[i] = &plan.Expr{Typ: expr.Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: selectorTag, ColPos: int32(i)}}}
	}
	return builder.appendNode(&plan.Node{
		NodeType: plan.Node_PROJECT, Children: []int32{selectorID}, ProjectList: projection,
		BindingTags: append([]int32(nil), original.BindingTags...), Limit: DeepCopyExpr(original.Limit),
		Offset: DeepCopyExpr(original.Offset), Stats: DeepCopyStats(original.Stats),
	}, ctx), nil
}
