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

// 资格集合只决定哪些 A 可以进入候选，不能替代最终保留重数的 INNER JOIN。
type vectorEquiJoinContext struct {
	join, scan, other *plan.Node
	scanSide          int
	distance          *plan.Expr
	limit             *plan.Expr
}

func (builder *QueryBuilder) buildVectorEquiJoinContext(proj *plan.Node) *vectorEquiJoinContext {
	if builder.sqlCalcFoundRows || proj == nil || proj.NodeType != plan.Node_PROJECT ||
		!builder.vectorCandidateRegionSafe(proj.NodeId) {
		return nil
	}
	sort := builder.resolveSortNode(proj, 1)
	if sort == nil || len(sort.OrderBy) != 1 || sort.Limit == nil ||
		isDescendingVectorSort(sort.OrderBy[0].Flag) || !vectorCandidateModeSupported(sort.RankOption) {
		return nil
	}
	join, child := builder.resolveJoinNodeForVectorSort(sort)
	if join == nil || join.JoinType != plan.Node_INNER || len(join.Children) != 2 ||
		len(join.FilterList) != 0 || join.Limit != nil || join.Offset != nil {
		return nil
	}
	if child != nil && (len(child.FilterList) != 0 || child.Limit != nil || child.Offset != nil) {
		return nil
	}
	left, right := builder.qry.Nodes[join.Children[0]], builder.qry.Nodes[join.Children[1]]
	if !plainVectorEquiScan(left) || !plainVectorEquiScan(right) ||
		!vectorEquiJoinOn(join.OnList, left.BindingTags[0], right.BindingTags[0]) {
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
			folded := DeepCopyExpr(distance)
			folded.GetF().Args[1-arg] = value
			return &vectorEquiJoinContext{join: join, scan: scan, other: other, scanSide: side, distance: folded, limit: limit}
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
		sortNode: &plan.Node{Limit: DeepCopyExpr(vc.limit)}, distFnExpr: DeepCopyExpr(vc.distance).GetF(),
		limit: DeepCopyExpr(vc.limit), resultLimit: DeepCopyExpr(vc.limit),
		rankOption: &plan.RankOption{Mode: "pre"}, providerNodeID: -1, hasMembership: true,
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
			// 两侧都须保持原 scan 形状，直到构造独立资格集合。
			return []int32{vc.scan.NodeId, vc.other.NodeId}
		}
	}
	return nil
}

func (builder *QueryBuilder) applyVectorIndexForEquiJoin(nodeID int32) (int32, bool, error) {
	vc := builder.buildVectorEquiJoinContext(builder.qry.Nodes[nodeID])
	if vc == nil {
		return nodeID, false, nil
	}
	ctx := builder.ctxByNode[nodeID]
	a, b := DeepCopyNode(vc.scan), DeepCopyNode(vc.other)
	oldA, oldB := a.BindingTags[0], b.BindingTags[0]
	builder.rebindScanNode(a)
	builder.rebindScanNode(b)
	aID, bID := builder.appendNode(a, ctx), builder.appendNode(b, ctx)
	on := DeepCopyExprList(vc.join.OnList)
	for _, expr := range on {
		replaceColRefTag(expr, oldA, a.BindingTags[0])
		replaceColRefTag(expr, oldB, b.BindingTags[0])
	}
	memberID := builder.appendNode(&plan.Node{NodeType: plan.Node_JOIN, JoinType: plan.Node_SEMI,
		Children: []int32{aID, bID}, OnList: on}, ctx)
	distance := DeepCopyExpr(vc.distance)
	replaceColRefTag(distance, oldA, a.BindingTags[0])
	sortID := builder.appendNode(&plan.Node{NodeType: plan.Node_SORT, Children: []int32{memberID},
		OrderBy: []*plan.OrderBySpec{{Expr: distance, Flag: plan.OrderBySpec_ASC}},
		Limit:   DeepCopyExpr(vc.limit), RankOption: &plan.RankOption{Mode: "pre"}}, ctx)
	projection := make([]*plan.Expr, len(a.TableDef.Cols))
	for i, col := range a.TableDef.Cols {
		projection[i] = &plan.Expr{Typ: col.Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{
			RelPos: a.BindingTags[0], ColPos: int32(i), Name: col.Name}}}
	}
	candidate := &plan.Node{NodeType: plan.Node_PROJECT, Children: []int32{sortID},
		ProjectList: projection, BindingTags: []int32{builder.genNewBindTag()}}
	candidateID := builder.appendNode(candidate, ctx)
	memberCtx := builder.buildVectorSortContextThroughJoin(candidate)
	if memberCtx == nil {
		return nodeID, false, nil
	}
	refs := make(map[[2]int32]int)
	builder.countColRefs(candidateID, refs)
	newID, applied, err := builder.applyVectorIndexForSortContext(candidateID, memberCtx, refs, make(map[[2]int32]*plan.Expr))
	if err != nil || !applied {
		return nodeID, false, err
	}
	candidate = builder.qry.Nodes[newID]
	delete(builder.tag2NodeID, candidate.BindingTags[0])
	candidate.BindingTags[0] = oldA
	builder.tag2NodeID[oldA] = newID
	vc.join.Children[vc.scanSide] = newID
	return nodeID, true, nil
}
