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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	indexplugin "github.com/matrixorigin/matrixone/pkg/indexplugin"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
)

// 等值 JOIN 的资格集合可以去重，但最终输出不能去重。这个上下文只证明
// 距离依赖其中一侧、每个合格候选至少贡献一行，尚不修改原关系计划。
type vectorEquiJoinContext struct {
	join       *plan.Node
	scan       *plan.Node
	other      *plan.Node
	scanSide   int
	distance   *plan.Expr
	limit      *plan.Expr
	rankOption *plan.RankOption
}

func (builder *QueryBuilder) buildVectorEquiJoinContext(proj *plan.Node) *vectorEquiJoinContext {
	if builder.sqlCalcFoundRows || proj == nil || proj.NodeType != plan.Node_PROJECT {
		return nil
	}
	sort := builder.resolveSortNode(proj, 1)
	if sort == nil || len(sort.OrderBy) != 1 || sort.Limit == nil || isDescendingVectorSort(sort.OrderBy[0].Flag) {
		return nil
	}
	if sort.RankOption != nil && sort.RankOption.Mode != "" && sort.RankOption.Mode != "pre" {
		return nil
	}
	join, child := builder.resolveJoinNodeForVectorSort(sort)
	if join == nil || join.JoinType != plan.Node_INNER || len(join.Children) != 2 || len(join.OnList) == 0 ||
		len(join.FilterList) != 0 || join.Limit != nil || join.Offset != nil {
		return nil
	}
	if child != nil && (len(child.FilterList) != 0 || child.Limit != nil || child.Offset != nil) {
		return nil
	}
	if !builder.adaptiveIvfReplaySafe(proj.NodeId) {
		return nil
	}
	left, right := builder.qry.Nodes[join.Children[0]], builder.qry.Nodes[join.Children[1]]
	if !plainVectorEquiScan(left) || !plainVectorEquiScan(right) || !vectorEquiJoinOn(join.OnList, left.BindingTags[0], right.BindingTags[0]) {
		return nil
	}
	distance := sort.OrderBy[0].Expr
	if child != nil && distance.GetCol() != nil {
		col := distance.GetCol()
		if len(child.BindingTags) != 1 || col.RelPos != child.BindingTags[0] || col.ColPos < 0 || int(col.ColPos) >= len(child.ProjectList) {
			return nil
		}
		distance = child.ProjectList[col.ColPos]
	}
	fn := distance.GetF()
	if fn == nil || fn.Func == nil || len(fn.Args) != 2 {
		return nil
	}
	for side, scan := range []*plan.Node{left, right} {
		if builder.directScanWithVectorIndex(scan) == nil || scan.TableDef.Pkey == nil {
			continue
		}
		for arg, expr := range fn.Args {
			col := expr.GetCol()
			if col == nil || col.RelPos != scan.BindingTags[0] || !builder.isNonNullVectorProviderArg(scan, expr) ||
				!rule.IsConstant(fn.Args[1-arg], false) {
				continue
			}
			// NULL 查询向量的排序不能等同于空 ANN 结果。没有非 NULL 证明的
			// 参数/变量保留旧路径；已绑定的常量可在隔离副本上折叠。
			value, err := ConstantFold(batch.EmptyForConstFoldBatch, DeepCopyExpr(fn.Args[1-arg]), builder.compCtx.GetProcess(), false, true)
			if err != nil || value == nil || value.GetLit() == nil || value.GetLit().Isnull {
				continue
			}
			limit, ok := buildCandidateLimit(sort.Limit, sort.Offset)
			if !ok {
				return nil
			}
			other := right
			if side == 1 {
				other = left
			}
			foldedDistance := DeepCopyExpr(distance)
			foldedDistance.GetF().Args[1-arg] = value
			return &vectorEquiJoinContext{
				join: join, scan: scan, other: other, scanSide: side,
				distance: foldedDistance, limit: limit, rankOption: sort.RankOption,
			}
		}
	}
	return nil
}

// guard 只保护真正具备 membership 能力的路径。CanApply 可能解析 AUTO，
// 所以使用私有 scan/sort，不允许试探改变原计划的模式或 filter。
func (builder *QueryBuilder) detectEquiVectorJoinGuard(proj *plan.Node) []int32 {
	vc := builder.buildVectorEquiJoinContext(proj)
	if vc == nil {
		return nil
	}
	indexes, err := builder.collectVectorIndexes(vc.scan)
	if err != nil {
		return nil
	}
	probe := &vectorSortContext{
		projNode: DeepCopyNode(proj), scanNode: DeepCopyNode(vc.scan),
		sortNode:   &plan.Node{Limit: DeepCopyExpr(vc.limit)},
		distFnExpr: DeepCopyExpr(vc.distance).GetF(), sortDirection: plan.OrderBySpec_ASC,
		limit: DeepCopyExpr(vc.limit), resultLimit: DeepCopyExpr(vc.limit),
		rankOption: DeepCopyRankOption(vc.rankOption), providerNodeID: -1, hasMembership: true,
	}
	for _, index := range indexes {
		if !vectorIndexSupportsContext(probe, index.IndexAlgo) {
			continue
		}
		plugin, ok := indexplugin.Get(index.IndexAlgo)
		if !ok {
			continue
		}
		vctx, mti := toPlanplugin(probe, index)
		if applicable, err := plugin.Plan().CanApply(builder, vctx, mti); err == nil && applicable {
			return []int32{vc.scan.NodeId}
		}
	}
	return nil
}

func plainVectorEquiScan(n *plan.Node) bool {
	return n != nil && n.NodeType == plan.Node_TABLE_SCAN && n.TableDef != nil &&
		(n.TableDef.TableType == "" || n.TableDef.TableType == catalog.SystemOrdinaryRel) && n.TableDef.Partition == nil &&
		len(n.BindingTags) == 1 && len(n.Children) == 0 && n.Limit == nil && n.Offset == nil &&
		len(n.RuntimeFilterBuildList) == 0 && len(n.RuntimeFilterProbeList) == 0 && !n.IndexScanInfo.GetIsIndexScan()
}

func vectorEquiJoinOn(exprs []*plan.Expr, left, right int32) bool {
	for _, expr := range exprs {
		fn := expr.GetF()
		if fn == nil || fn.Func == nil || fn.Func.ObjName != "=" || len(fn.Args) != 2 {
			return false
		}
		a, b := fn.Args[0].GetCol(), fn.Args[1].GetCol()
		if a == nil || b == nil || fn.Args[0].Typ.Id != fn.Args[1].Typ.Id ||
			!((a.RelPos == left && b.RelPos == right) || (a.RelPos == right && b.RelPos == left)) {
			return false
		}
	}
	return len(exprs) > 0
}

// 每个合格 A 至少产生一行 JOIN 输出，所以 K+OFFSET 个合格 A 足以覆盖
// 最终窗口；OFFSET 只留在原 Top-K。通过 SEMI 生产资格域，再保留原 INNER
// JOIN 展开重复，不能直接用 SEMI 替换最终 JOIN。
func (builder *QueryBuilder) applyVectorIndexForEquiJoin(nodeID int32, early bool) (int32, bool, error) {
	original := builder.qry.Nodes[nodeID]
	vc := builder.buildVectorEquiJoinContext(original)
	if vc == nil {
		return nodeID, false, nil
	}
	ctx := builder.ctxByNode[nodeID]
	// 所有插件试探只修改私有子树。尤其 B 要独立复制/重绑定，不能与保留
	// 的 INNER JOIN 共用可变节点、binding tag 或 runtime-filter 消息。
	a, b := DeepCopyNode(vc.scan), DeepCopyNode(vc.other)
	oldA, oldB := a.BindingTags[0], b.BindingTags[0]
	// appendNode 会注册 binding tag，必须先重绑定私有 scan，避免试探
	// 即使被拒绝也把原 A/B 的 tag2NodeID 覆盖成副本。
	builder.rebindScanNode(a)
	builder.rebindScanNode(b)
	aID, bID := builder.appendNode(a, ctx), builder.appendNode(b, ctx)
	on := DeepCopyExprList(vc.join.OnList)
	for _, expr := range on {
		replaceColRefTag(expr, oldA, a.BindingTags[0])
		replaceColRefTag(expr, oldB, b.BindingTags[0])
	}
	memberID := builder.appendNode(&plan.Node{
		NodeType: plan.Node_JOIN, JoinType: plan.Node_SEMI,
		Children: []int32{aID, bID}, OnList: on,
	}, ctx)
	distance := DeepCopyExpr(vc.distance)
	replaceColRefTag(distance, oldA, a.BindingTags[0])
	sortID := builder.appendNode(&plan.Node{
		NodeType: plan.Node_SORT, Children: []int32{memberID},
		OrderBy: []*plan.OrderBySpec{{Expr: distance, Flag: plan.OrderBySpec_ASC}},
		Limit:   DeepCopyExpr(vc.limit), RankOption: DeepCopyRankOption(vc.rankOption),
	}, ctx)
	// 保留 A 的列位置和输出 schema，包括原 JOIN/最终距离所需的列。
	// 不移动上层含 B 列的表达式，也不提前执行原混合投影。
	projection := make([]*plan.Expr, len(a.TableDef.Cols))
	for i, col := range a.TableDef.Cols {
		projection[i] = &plan.Expr{Typ: col.Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{
			RelPos: a.BindingTags[0], ColPos: int32(i), Name: col.Name,
		}}}
	}
	candidate := &plan.Node{
		NodeType: plan.Node_PROJECT, Children: []int32{sortID},
		ProjectList: projection, BindingTags: []int32{builder.genNewBindTag()},
	}
	candidateID := builder.appendNode(candidate, ctx)
	memberCtx := builder.buildVectorSortContextThroughJoin(candidate)
	if memberCtx == nil {
		return nodeID, false, nil
	}
	refs := make(map[[2]int32]int)
	builder.countColRefs(candidateID, refs)
	remap := make(map[[2]int32]*plan.Expr)
	var err error
	if early {
		_, _, err = builder.applyLogicalVectorIndexForSortContext(candidateID, memberCtx, refs, remap)
	} else {
		_, _, err = builder.applyVectorIndexForSortContext(candidateID, memberCtx, refs, remap)
	}
	if err != nil {
		return nodeID, false, err
	}
	if !builder.scalarVectorHasAccessPath(candidateID) {
		return nodeID, false, nil
	}
	// 仅在私有候选树确实产生索引路径后提交，原 B、ON、排序和最终分页不变。
	delete(builder.tag2NodeID, candidate.BindingTags[0])
	candidate.BindingTags[0] = oldA
	builder.tag2NodeID[oldA] = candidateID
	vc.join.Children[vc.scanSide] = candidateID
	return nodeID, true, nil
}
