// Copyright 2023 Matrix Origin
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
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
)

func (builder *QueryBuilder) countColRefs(nodeID int32, colRefCnt map[[2]int32]int) {
	node := builder.qry.Nodes[nodeID]

	increaseRefCntForExprList(node.ProjectList, 1, colRefCnt)
	increaseRefCntForExprList(node.OnList, 1, colRefCnt)
	increaseRefCntForExprList(node.FilterList, 1, colRefCnt)
	increaseRefCntForExprList(node.GroupBy, 1, colRefCnt)
	increaseRefCntForExprList(node.PhysicalEqualityKeyList, 1, colRefCnt)
	increaseRefCntForExprList(node.AggList, 1, colRefCnt)
	increaseRefCntForExprList(node.WinSpecList, 1, colRefCnt)

	for i := range node.OrderBy {
		increaseRefCnt(node.OrderBy[i].Expr, 1, colRefCnt)
	}

	if node.DedupJoinCtx != nil {
		increaseRefCntForColRefList(node.DedupJoinCtx.OldColList, 2, colRefCnt)
		increaseRefCntForExprList(node.DedupJoinCtx.UpdateColExprList, 2, colRefCnt)
		for _, cap := range node.DedupJoinCtx.OldColCaptureList {
			colRefCnt[[2]int32{cap.BuildPlaceholder.RelPos, cap.BuildPlaceholder.ColPos}] += 2
			colRefCnt[[2]int32{cap.ProbeSource.RelPos, cap.ProbeSource.ColPos}] += 2
		}
	}

	for _, updateCtx := range node.UpdateCtxList {
		increaseRefCntForColRefList(updateCtx.InsertCols, 2, colRefCnt)
		increaseRefCntForColRefList(updateCtx.DeleteCols, 2, colRefCnt)
		increaseRefCntForColRefList(updateCtx.PartitionCols, 2, colRefCnt)
		if updateCtx.ChangedRowsCol != nil {
			colRefCnt[[2]int32{updateCtx.ChangedRowsCol.RelPos, updateCtx.ChangedRowsCol.ColPos}] += 2
		}
		increaseRefCntForColRefList(updateCtx.AffectedRowsCols, 2, colRefCnt)
	}

	if node.NodeType == plan.Node_LOCK_OP {
		for _, lockTarget := range node.LockTargets {
			colRefCnt[[2]int32{lockTarget.PrimaryColRelPos, lockTarget.PrimaryColIdxInBat}] += 1
		}
	}

	for _, childID := range node.Children {
		builder.countColRefs(childID, colRefCnt)
	}
}

// removeSimpleProjections On top of each subquery or view it has a PROJECT node, which interrupts optimizer rules such as join order.
func (builder *QueryBuilder) removeSimpleProjections(nodeID int32, parentType plan.Node_NodeType, flag bool, colRefCnt map[[2]int32]int) (int32, map[[2]int32]*plan.Expr) {
	node := builder.qry.Nodes[nodeID]
	if node.NodeType == plan.Node_SINK {
		return builder.removeSimpleProjections(node.Children[0], plan.Node_UNKNOWN, flag, colRefCnt)
	}
	projMap := make(map[[2]int32]*plan.Expr)

	switch node.NodeType {
	case plan.Node_JOIN:
		leftFlag := flag || node.JoinType == plan.Node_RIGHT || node.JoinType == plan.Node_OUTER
		rightFlag := flag || node.JoinType == plan.Node_LEFT || node.JoinType == plan.Node_OUTER ||
			node.JoinType == plan.Node_ASOF_LEFT

		newChildID, childProjMap := builder.removeSimpleProjections(node.Children[0], plan.Node_JOIN, leftFlag, colRefCnt)
		node.Children[0] = newChildID
		for ref, expr := range childProjMap {
			projMap[ref] = expr
		}

		origRightID := node.Children[1]
		newChildID, childProjMap = builder.removeSimpleProjections(node.Children[1], plan.Node_JOIN, rightFlag, colRefCnt)
		// When OldColCaptureList is set, the build-side (right child) PROJECT
		// contains NULL placeholder slots that the DEDUP JOIN writes captured
		// values into at runtime. Removing this PROJECT would lose those
		// slots and leave OldColCaptureList.BuildPlaceholder references
		// dangling (they point to Lit expressions that can't be remapped to
		// column references). Keep the original PROJECT in the tree.
		if node.DedupJoinCtx != nil && len(node.DedupJoinCtx.OldColCaptureList) > 0 && newChildID != origRightID {
			newChildID = origRightID
			childProjMap = nil
		}
		node.Children[1] = newChildID
		for ref, expr := range childProjMap {
			projMap[ref] = expr
		}

	case plan.Node_AGG, plan.Node_PROJECT, plan.Node_WINDOW, plan.Node_TIME_WINDOW, plan.Node_FILL:
		for i, childID := range node.Children {
			newChildID, childProjMap := builder.removeSimpleProjections(childID, node.NodeType, false, colRefCnt)
			node.Children[i] = newChildID
			for ref, expr := range childProjMap {
				projMap[ref] = expr
			}
		}

	case plan.Node_MULTI_UPDATE:
		for i, childID := range node.Children {
			newChildID, childProjMap := builder.removeSimpleProjections(childID, node.NodeType, true, colRefCnt)
			node.Children[i] = newChildID
			for ref, expr := range childProjMap {
				projMap[ref] = expr
			}
		}

	case plan.Node_LOCK_OP:
		childParentType := node.NodeType
		if _, preserve := builder.preserveLockProjection[nodeID]; preserve {
			// A preserved pass-through lock can feed positional consumers such as a
			// shared SINK. Keep its immediate PROJECT as the stable row-image
			// boundary; inlining that PROJECT changes the binding tag and can make
			// every downstream sink column resolve to input column zero.
			childParentType = plan.Node_UNKNOWN
		}
		for i, childID := range node.Children {
			newChildID, childProjMap := builder.removeSimpleProjections(childID, childParentType, true, colRefCnt)
			node.Children[i] = newChildID
			for ref, expr := range childProjMap {
				projMap[ref] = expr
			}
		}

	default:
		for i, childID := range node.Children {
			newChildID, childProjMap := builder.removeSimpleProjections(childID, node.NodeType, flag, colRefCnt)
			node.Children[i] = newChildID
			for ref, expr := range childProjMap {
				projMap[ref] = expr
			}
		}
	}

	replaceColumnsForNode(node, projMap)

	if builder.canRemoveProject(parentType, node) {
		allColRef := true
		tag := node.BindingTags[0]
		for i, proj := range node.ProjectList {
			refCnt := colRefCnt[[2]int32{tag, int32(i)}]
			if flag || refCnt > 1 {
				if proj.GetCol() == nil && (proj.GetLit() == nil || flag) {
					allColRef = false
					break
				}
			}
		}

		if allColRef {
			tag := node.BindingTags[0]
			for i, proj := range node.ProjectList {
				projMap[[2]int32{tag, int32(i)}] = proj
			}

			nodeID = node.Children[0]
		}
	}

	return nodeID, projMap
}

func increaseRefCntForExprList(exprs []*plan.Expr, inc int, colRefCnt map[[2]int32]int) {
	for _, expr := range exprs {
		if expr == nil {
			continue
		}
		increaseRefCnt(expr, inc, colRefCnt)
	}
}

func increaseRefCntForColRefList(cols []plan.ColRef, inc int, colRefCnt map[[2]int32]int) {
	for _, col := range cols {
		colRefCnt[[2]int32{col.RelPos, col.ColPos}] += inc
	}
}

// FIXME: We should remove PROJECT node for more cases, but keep them now to avoid intricate issues.
func (builder *QueryBuilder) canRemoveProject(parentType plan.Node_NodeType, node *plan.Node) bool {
	if node.NodeType != plan.Node_PROJECT || node.Limit != nil || node.Offset != nil {
		return false
	}
	if _, groupingSetExpand := DecodeGroupingSetExpandOption(node.ExtraOptions); groupingSetExpand {
		return false
	}

	if parentType == plan.Node_DISTINCT || parentType == plan.Node_UNKNOWN {
		return false
	}
	if parentType == plan.Node_UNION || parentType == plan.Node_UNION_ALL {
		return false
	}
	if parentType == plan.Node_MINUS || parentType == plan.Node_MINUS_ALL {
		return false
	}
	if parentType == plan.Node_INTERSECT || parentType == plan.Node_INTERSECT_ALL {
		return false
	}
	if parentType == plan.Node_FUNCTION_SCAN || parentType == plan.Node_EXTERNAL_FUNCTION {
		return false
	}
	if parentType == plan.Node_DELETE {
		return false
	}
	if parentType == plan.Node_INSERT || parentType == plan.Node_PRE_INSERT || parentType == plan.Node_PRE_INSERT_UK || parentType == plan.Node_PRE_INSERT_SK {
		return false
	}
	for _, expr := range node.ProjectList {
		if !exprCanRemoveProject(expr) {
			return false
		}
	}

	childType := builder.qry.Nodes[node.Children[0]].NodeType
	// A PROJECT is also the rewrite boundary for a fulltext-filtered scan.
	// Removing it can expose the scan directly under a WINDOW or an outer JOIN,
	// neither of which can safely perform the scan-local fulltext rewrite.
	if childType == plan.Node_TABLE_SCAN && builder.scanHasMatchedFullTextFilter(builder.qry.Nodes[node.Children[0]]) {
		return false
	}
	if childType == plan.Node_VALUE_SCAN || childType == plan.Node_EXTERNAL_SCAN {
		return parentType == plan.Node_PROJECT
	}
	if childType == plan.Node_FUNCTION_SCAN || childType == plan.Node_EXTERNAL_FUNCTION {
		return parentType == plan.Node_PROJECT
	}
	if childType == plan.Node_TABLE_SCAN {
		if parentType == plan.Node_PROJECT {
			return true
		}

		for _, proj := range node.ProjectList {
			if proj.GetLit() != nil {
				return false
			}
		}
	}

	return true
}

func exprCanRemoveProject(expr *Expr) bool {
	switch ne := expr.Expr.(type) {
	case *plan.Expr_F:
		// fulltext_match is a planner placeholder: applyIndices replaces it
		// with the score column produced by fulltext_index_scan.  Inlining a
		// projection that contains it can move the placeholder into a WINDOW
		// (for example through a multi-CTE query) where the fulltext rewrite
		// cannot associate it with the source scan anymore.  Keep that PROJECT
		// until applyIndices has performed the replacement; the second
		// removeSimpleProjections pass can remove it afterwards.
		if ne.F.Func.ObjName == "fulltext_match" {
			return false
		}
		overload, exists := function.GetFunctionByIdWithoutError(ne.F.Func.Obj)
		if !exists || overload.CannotFold() || overload.IsRealTimeRelated() {
			return false
		}
		for _, arg := range ne.F.GetArgs() {
			canRemove := exprCanRemoveProject(arg)
			if !canRemove {
				return canRemove
			}
		}
	case *plan.Expr_List:
		for _, item := range ne.List.List {
			if !exprCanRemoveProject(item) {
				return false
			}
		}
	case *plan.Expr_W:
		if !exprCanRemoveProject(ne.W.WindowFunc) {
			return false
		}
		for _, partitionBy := range ne.W.PartitionBy {
			if !exprCanRemoveProject(partitionBy) {
				return false
			}
		}
		for _, orderBy := range ne.W.OrderBy {
			if !exprCanRemoveProject(orderBy.Expr) {
				return false
			}
		}
	}
	return true
}

func replaceColumnsForNode(node *plan.Node, projMap map[[2]int32]*plan.Expr) {
	replaceColumnsForExprList(node.ProjectList, projMap)
	replaceColumnsForExprList(node.OnList, projMap)
	replaceColumnsForExprList(node.FilterList, projMap)
	replaceColumnsForExprList(node.GroupBy, projMap)
	replaceColumnsForExprList(node.PhysicalEqualityKeyList, projMap)
	replaceColumnsForExprList(node.AggList, projMap)
	replaceColumnsForExprList(node.WinSpecList, projMap)
	replaceColumnsForExprList(node.TimeWindowPartitionBy, projMap)

	for i := range node.OrderBy {
		node.OrderBy[i].Expr = replaceColumnsForExpr(node.OrderBy[i].Expr, projMap)
	}

	if node.DedupJoinCtx != nil {
		replaceColumnsForColRefList(node.DedupJoinCtx.OldColList, projMap)
		replaceColumnsForExprList(node.DedupJoinCtx.UpdateColExprList, projMap)
		for i := range node.DedupJoinCtx.OldColCaptureList {
			cap := &node.DedupJoinCtx.OldColCaptureList[i]
			if projExpr, ok := projMap[[2]int32{cap.BuildPlaceholder.RelPos, cap.BuildPlaceholder.ColPos}]; ok {
				if col := projExpr.GetCol(); col != nil {
					cap.BuildPlaceholder.RelPos = col.RelPos
					cap.BuildPlaceholder.ColPos = col.ColPos
				}
				// Lit expressions (NULL placeholders) are left unchanged —
				// they will be resolved by the fullProjTag PROJECT which
				// must not be removed (see removeSimpleProjections guard).
			}
			if projExpr, ok := projMap[[2]int32{cap.ProbeSource.RelPos, cap.ProbeSource.ColPos}]; ok {
				if col := projExpr.GetCol(); col != nil {
					cap.ProbeSource.RelPos = col.RelPos
					cap.ProbeSource.ColPos = col.ColPos
				}
			}
		}
	}

	for _, updateCtx := range node.UpdateCtxList {
		replaceColumnsForColRefList(updateCtx.InsertCols, projMap)
		replaceColumnsForColRefList(updateCtx.DeleteCols, projMap)
		replaceColumnsForColRefList(updateCtx.PartitionCols, projMap)
		if updateCtx.ChangedRowsCol != nil {
			cols := []plan.ColRef{*updateCtx.ChangedRowsCol}
			replaceColumnsForColRefList(cols, projMap)
			*updateCtx.ChangedRowsCol = cols[0]
		}
		replaceColumnsForColRefList(updateCtx.AffectedRowsCols, projMap)
	}

	if node.NodeType == plan.Node_LOCK_OP {
		for _, lockTarget := range node.LockTargets {
			colRef := [2]int32{lockTarget.PrimaryColRelPos, lockTarget.PrimaryColIdxInBat}
			if expr, ok := projMap[colRef]; ok {
				if e, ok := expr.Expr.(*plan.Expr_Col); ok {
					lockTarget.PrimaryColRelPos = e.Col.RelPos
					lockTarget.PrimaryColIdxInBat = e.Col.ColPos
				}
			}
		}
	}
}

func replaceColumnsForExprList(exprList []*plan.Expr, projMap map[[2]int32]*plan.Expr) {
	for i, expr := range exprList {
		if expr == nil {
			continue
		}
		exprList[i] = replaceColumnsForExpr(expr, projMap)
	}
}

func replaceColumnsForColRefList(cols []plan.ColRef, projMap map[[2]int32]*plan.Expr) {
	for i := range cols {
		mapID := [2]int32{cols[i].RelPos, cols[i].ColPos}
		if projExpr, ok := projMap[mapID]; ok {
			// A []plan.ColRef can only hold a column, so a mapping to any other
			// expression form (a CTE projecting `id+1`, a cast) has nowhere to go here.
			// Assert-and-panic would take down the CN; leaving the ref alone keeps the
			// list valid. Vector rewrites publish such maps into idxColMap, which
			// applyIndices then applies to every ancestor including nodes carrying
			// UpdateCtxList / DedupJoinCtx.OldColList.
			colExpr, isCol := projExpr.Expr.(*plan.Expr_Col)
			if !isCol || colExpr.Col == nil {
				continue
			}
			cols[i].RelPos = colExpr.Col.RelPos
			cols[i].ColPos = colExpr.Col.ColPos
		}
	}
}

func replaceColumnsForExpr(expr *plan.Expr, projMap map[[2]int32]*plan.Expr) *plan.Expr {
	if expr == nil {
		return nil
	}

	switch ne := expr.Expr.(type) {
	case *plan.Expr_Lit:
		if ne.Lit != nil {
			ne.Lit.Src = replaceColumnsForExpr(ne.Lit.Src, projMap)
		}

	case *plan.Expr_Col:
		if ne.Col == nil {
			return expr
		}
		mapID := [2]int32{ne.Col.RelPos, ne.Col.ColPos}
		if projExpr, ok := projMap[mapID]; ok {
			return DeepCopyExpr(projExpr)
		}

	case *plan.Expr_F:
		if ne.F == nil {
			return expr
		}
		for i, arg := range ne.F.Args {
			ne.F.Args[i] = replaceColumnsForExpr(arg, projMap)
		}

	case *plan.Expr_W:
		if ne.W == nil {
			return expr
		}
		ne.W.WindowFunc = replaceColumnsForExpr(ne.W.WindowFunc, projMap)
		for i, arg := range ne.W.PartitionBy {
			ne.W.PartitionBy[i] = replaceColumnsForExpr(arg, projMap)
		}
		for i, order := range ne.W.OrderBy {
			if order != nil {
				ne.W.OrderBy[i].Expr = replaceColumnsForExpr(order.Expr, projMap)
			}
		}
		if ne.W.Frame != nil {
			if ne.W.Frame.Start != nil {
				ne.W.Frame.Start.Val = replaceColumnsForExpr(ne.W.Frame.Start.Val, projMap)
			}
			if ne.W.Frame.End != nil {
				ne.W.Frame.End.Val = replaceColumnsForExpr(ne.W.Frame.End.Val, projMap)
			}
		}

	case *plan.Expr_List:
		if ne.List != nil {
			replaceColumnsForExprList(ne.List.List, projMap)
		}

	case *plan.Expr_Sub:
		if ne.Sub != nil {
			ne.Sub.Child = replaceColumnsForExpr(ne.Sub.Child, projMap)
		}
	}
	return expr
}

func (builder *QueryBuilder) swapJoinChildren(nodeID int32) {
	node := builder.qry.Nodes[nodeID]

	for _, child := range node.Children {
		builder.swapJoinChildren(child)
	}

	if node.IsRightJoin {
		node.Children[0], node.Children[1] = node.Children[1], node.Children[0]
		if node.JoinType == plan.Node_LEFT {
			node.JoinType = plan.Node_RIGHT
		}
	}
}

func (builder *QueryBuilder) remapHavingClause(
	expr *plan.Expr,
	groupTag, aggregateTag int32,
	groupSize, aggregateSize int32,
	aggPos []int32,
) error {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_Col:
		if exprImpl.Col.RelPos == groupTag {
			if exprImpl.Col.ColPos < 0 || exprImpl.Col.ColPos >= groupSize {
				return moerr.NewInternalErrorf(
					builder.GetContext(),
					"invalid group column %d in HAVING during column pruning",
					exprImpl.Col.ColPos,
				)
			}
			exprImpl.Col.Name = builder.nameByColRef[[2]int32{groupTag, exprImpl.Col.ColPos}]
			exprImpl.Col.RelPos = -1
		} else if exprImpl.Col.RelPos == aggregateTag {
			oldPos := exprImpl.Col.ColPos
			if oldPos < 0 || oldPos >= aggregateSize {
				return moerr.NewInternalErrorf(
					builder.GetContext(),
					"invalid aggregate column %d in HAVING during column pruning",
					oldPos,
				)
			}
			newPos := oldPos
			if aggPos != nil {
				if int(oldPos) >= len(aggPos) || aggPos[oldPos] < 0 {
					return moerr.NewInternalErrorf(
						builder.GetContext(),
						"invalid aggregate column %d in HAVING during column pruning",
						oldPos,
					)
				}
				newPos = aggPos[oldPos]
			}
			exprImpl.Col.Name = builder.nameByColRef[[2]int32{aggregateTag, oldPos}]
			exprImpl.Col.RelPos = -2
			exprImpl.Col.ColPos = newPos + groupSize
		} else {
			return moerr.NewInternalErrorf(
				builder.GetContext(),
				"invalid relation tag %d in HAVING during column pruning",
				exprImpl.Col.RelPos,
			)
		}

	case *plan.Expr_F:
		for _, arg := range exprImpl.F.Args {
			if err := builder.remapHavingClause(arg, groupTag, aggregateTag, groupSize, aggregateSize, aggPos); err != nil {
				return err
			}
		}

	case *plan.Expr_List:
		for _, item := range exprImpl.List.List {
			if err := builder.remapHavingClause(item, groupTag, aggregateTag, groupSize, aggregateSize, aggPos); err != nil {
				return err
			}
		}
	}

	return nil
}

func (builder *QueryBuilder) remapWindowClause(
	expr *plan.Expr,
	windowTag int32,
	windowIdx int32,
	projectionSize int32,
	colMap map[[2]int32][2]int32,
	remapInfo *RemapInfo,
) error {
	// Each Window node appends only its own window result after the child
	// projection list. Earlier window results share the same windowTag but
	// already belong to the child projection, so they must be remapped via
	// the child column map instead of being treated as the current node's
	// appended output.
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_Col:
		if exprImpl.Col.RelPos == windowTag && exprImpl.Col.ColPos == windowIdx {
			// Each Window node appends exactly one local output column, so the
			// current window result always lands at projectionSize regardless of
			// its global windowIdx under the shared windowTag.
			exprImpl.Col.Name = builder.nameByColRef[[2]int32{windowTag, windowIdx}]
			exprImpl.Col.RelPos = -1
			exprImpl.Col.ColPos = projectionSize
		} else {
			err := builder.remapSingleColRef(exprImpl.Col, colMap, remapInfo)
			if err != nil {
				return err
			}
		}

	case *plan.Expr_F:
		for _, arg := range exprImpl.F.Args {
			err := builder.remapWindowClause(arg, windowTag, windowIdx, projectionSize, colMap, remapInfo)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// if join cond is a=b and a=c, we can remove a=c to improve join performance
func (builder *QueryBuilder) removeRedundantJoinCond(nodeID int32, colMap map[[2]int32]int, colGroup []int) []int {
	if builder.optimizerHints != nil && builder.optimizerHints.removeRedundantJoinCond != 0 {
		return colGroup
	}
	node := builder.qry.Nodes[nodeID]
	for i := range node.Children {
		colGroup = builder.removeRedundantJoinCond(node.Children[i], colMap, colGroup)
	}
	if len(node.OnList) == 0 {
		return colGroup
	}

	newOnList := make([]*plan.Expr, 0)
	for _, expr := range node.OnList {
		if exprf := expr.GetF(); exprf != nil {
			if IsEqualFunc(exprf.Func.GetObj()) {
				leftcol := exprf.Args[0].GetCol()
				rightcol := exprf.Args[1].GetCol()
				if leftcol != nil && rightcol != nil {
					left, leftok := colMap[[2]int32{leftcol.RelPos, leftcol.ColPos}]
					if !leftok {
						left = len(colGroup)
						colGroup = append(colGroup, left)
						colMap[[2]int32{leftcol.RelPos, leftcol.ColPos}] = left
					}
					right, rightok := colMap[[2]int32{rightcol.RelPos, rightcol.ColPos}]
					if !rightok {
						right = len(colGroup)
						colGroup = append(colGroup, right)
						colMap[[2]int32{rightcol.RelPos, rightcol.ColPos}] = right
					}
					for colGroup[left] != colGroup[colGroup[left]] {
						colGroup[left] = colGroup[colGroup[left]]
					}
					for colGroup[right] != colGroup[colGroup[right]] {
						colGroup[right] = colGroup[colGroup[right]]
					}
					if colGroup[left] == colGroup[right] {
						continue
					}
					newOnList = append(newOnList, expr)
					colGroup[colGroup[left]] = colGroup[right]
				} else {
					newOnList = append(newOnList, expr)
				}
			} else {
				newOnList = append(newOnList, expr)
			}
		} else {
			newOnList = append(newOnList, expr)
		}
	}
	node.OnList = newOnList

	return colGroup
}

func (builder *QueryBuilder) removeEffectlessLeftJoins(nodeID int32, tagCnt map[int32]int) int32 {
	if builder.optimizerHints != nil && builder.optimizerHints.removeEffectLessLeftJoins != 0 {
		return nodeID
	}
	node := builder.qry.Nodes[nodeID]
	if len(node.Children) == 0 {
		return nodeID
	}

	increaseTagCntForExprList(node.ProjectList, 1, tagCnt)
	increaseTagCntForExprList(node.OnList, 1, tagCnt)
	increaseTagCntForExprList(node.FilterList, 1, tagCnt)
	increaseTagCntForExprList(node.GroupBy, 1, tagCnt)
	increaseTagCntForExprList(node.AggList, 1, tagCnt)
	increaseTagCntForExprList(node.WinSpecList, 1, tagCnt)

	for i := range node.OrderBy {
		increaseTagCnt(node.OrderBy[i].Expr, 1, tagCnt)
	}

	if node.DedupJoinCtx != nil {
		increaseTagCntForColRefList(node.DedupJoinCtx.OldColList, 2, tagCnt)
		increaseTagCntForExprList(node.DedupJoinCtx.UpdateColExprList, 2, tagCnt)
	}

	for _, updateCtx := range node.UpdateCtxList {
		increaseTagCntForColRefList(updateCtx.InsertCols, 2, tagCnt)
		increaseTagCntForColRefList(updateCtx.DeleteCols, 2, tagCnt)
		increaseTagCntForColRefList(updateCtx.PartitionCols, 2, tagCnt)
		if updateCtx.ChangedRowsCol != nil {
			tagCnt[updateCtx.ChangedRowsCol.RelPos] += 2
		}
		increaseTagCntForColRefList(updateCtx.AffectedRowsCols, 2, tagCnt)
	}

	for i, childID := range node.Children {
		node.Children[i] = builder.removeEffectlessLeftJoins(childID, tagCnt)
	}
	increaseTagCntForExprList(node.OnList, -1, tagCnt)

	if node.NodeType != plan.Node_JOIN || node.JoinType != plan.Node_LEFT {
		goto END
	}

	// if output column is in right, can not optimize this one
	for _, tag := range builder.enumerateTags(node.Children[1]) {
		if tagCnt[tag] > 0 {
			goto END
		}
	}

	//reuse hash on primary key logic
	if !node.Stats.HashmapStats.HashOnPK {
		goto END
	}

	nodeID = node.Children[0]

END:
	increaseTagCntForExprList(node.ProjectList, -1, tagCnt)
	increaseTagCntForExprList(node.FilterList, -1, tagCnt)
	increaseTagCntForExprList(node.GroupBy, -1, tagCnt)
	increaseTagCntForExprList(node.AggList, -1, tagCnt)
	increaseTagCntForExprList(node.WinSpecList, -1, tagCnt)

	for i := range node.OrderBy {
		increaseTagCnt(node.OrderBy[i].Expr, -1, tagCnt)
	}

	if node.DedupJoinCtx != nil {
		increaseTagCntForColRefList(node.DedupJoinCtx.OldColList, -2, tagCnt)
		increaseTagCntForExprList(node.DedupJoinCtx.UpdateColExprList, -2, tagCnt)
	}

	for _, updateCtx := range node.UpdateCtxList {
		increaseTagCntForColRefList(updateCtx.InsertCols, -2, tagCnt)
		increaseTagCntForColRefList(updateCtx.DeleteCols, -2, tagCnt)
		increaseTagCntForColRefList(updateCtx.PartitionCols, -2, tagCnt)
		if updateCtx.ChangedRowsCol != nil {
			tagCnt[updateCtx.ChangedRowsCol.RelPos] -= 2
		}
		increaseTagCntForColRefList(updateCtx.AffectedRowsCols, -2, tagCnt)
	}

	return nodeID
}

func increaseTagCntForExprList(exprs []*plan.Expr, inc int, tagCnt map[int32]int) {
	for _, expr := range exprs {
		if expr == nil {
			continue
		}
		increaseTagCnt(expr, inc, tagCnt)
	}
}

func increaseTagCntForColRefList(cols []plan.ColRef, inc int, tagCnt map[int32]int) {
	for _, col := range cols {
		tagCnt[col.RelPos] += inc
	}
}

func increaseTagCnt(expr *plan.Expr, inc int, tagCnt map[int32]int) {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_Col:
		tagCnt[exprImpl.Col.RelPos] += inc

	case *plan.Expr_F:
		for _, arg := range exprImpl.F.Args {
			increaseTagCnt(arg, inc, tagCnt)
		}
	case *plan.Expr_W:
		increaseTagCnt(exprImpl.W.WindowFunc, inc, tagCnt)
		for _, arg := range exprImpl.W.PartitionBy {
			increaseTagCnt(arg, inc, tagCnt)
		}
		for _, order := range exprImpl.W.OrderBy {
			increaseTagCnt(order.Expr, inc, tagCnt)
		}
	}
}

func findHashOnPKTable(nodeID, tag int32, builder *QueryBuilder) *plan.TableDef {
	node := builder.qry.Nodes[nodeID]
	if node.NodeType == plan.Node_TABLE_SCAN {
		if len(node.BindingTags) > 0 && node.BindingTags[0] == tag {
			return node.TableDef
		}
	} else if node.NodeType == plan.Node_JOIN && node.JoinType == plan.Node_INNER {
		if node.Stats.HashmapStats.HashOnPK {
			return findHashOnPKTable(node.Children[0], tag, builder)
		}
	}
	return nil
}

func determineHashOnPK(nodeID int32, builder *QueryBuilder) map[uint64][]uint64 {
	if builder.optimizerHints != nil && builder.optimizerHints.determineHashOnPK != 0 {
		return nil
	}
	node := builder.qry.Nodes[nodeID]

	if node.NodeType == plan.Node_TABLE_SCAN {
		pkPositions, ok := sqlEqualityCompatiblePrimaryKeyColumnPositions(node.TableDef)
		if !ok || len(node.BindingTags) == 0 {
			return nil
		}
		tag := uint64(node.BindingTags[0]) << 32
		colMap := make(map[uint64][]uint64)
		for _, pos := range pkPositions {
			k := tag | uint64(pos)
			colMap[k] = []uint64{k}
		}
		return colMap
	}

	if node.NodeType != plan.Node_JOIN {
		for i := range node.Children {
			determineHashOnPK(node.Children[i], builder)
		}
		return nil
	}

	leftColMap := determineHashOnPK(node.Children[0], builder)
	rightColMap := determineHashOnPK(node.Children[1], builder)
	if rightColMap == nil {
		return nil
	}

	leftTags := make(map[int32]bool)
	for _, tag := range builder.enumerateTags(node.Children[0]) {
		leftTags[tag] = true
	}

	rightTags := make(map[int32]bool)
	for _, tag := range builder.enumerateTags(node.Children[1]) {
		rightTags[tag] = true
	}

	exprs := make([]*plan.Expr, 0)
	for _, expr := range node.OnList {
		if equi := isEquiCond(expr, leftTags, rightTags); equi {
			exprs = append(exprs, expr)
		}
	}

	exprLeftCols := make([]uint64, len(exprs))
	exprRightCols := make([]uint64, len(exprs))
	for i, cond := range exprs {
		fn := cond.GetF()
		if fn == nil || len(fn.Args) != 2 {
			return nil
		}
		leftCol := fn.Args[0].GetCol()
		rightCol := fn.Args[1].GetCol()
		if leftCol == nil || rightCol == nil {
			// HashOnPK is an exact direct-column proof. A cast or other wrapper
			// may collapse storage-distinct primary keys under join equality.
			return nil
		}
		if !sqlEqualityJoinUsesOneIdentityDomain(fn.Args[0].Typ, fn.Args[1].Typ) {
			return nil
		}
		leftTableType, leftTypeOK := referencedTableColumnType(
			node.Children[0], leftCol, builder)
		rightTableType, rightTypeOK := referencedTableColumnType(
			node.Children[1], rightCol, builder)
		if !leftTypeOK || !rightTypeOK ||
			!sqlEqualityJoinUsesOneIdentityDomain(fn.Args[0].Typ, leftTableType) ||
			!sqlEqualityJoinUsesOneIdentityDomain(fn.Args[1].Typ, rightTableType) {
			// Mutually consistent join-expression metadata cannot substitute for
			// the concrete columns that the direct references identify.
			return nil
		}
		exprLeftCols[i] = (uint64(leftCol.RelPos) << 32) | uint64(leftCol.ColPos)
		// The build/right primary-key expression must remain non-nullable.
		if !fn.Args[1].Typ.NotNullable {
			return nil
		}
		exprRightCols[i] = (uint64(rightCol.RelPos) << 32) | uint64(rightCol.ColPos)
	}

	rightColKey := make([]uint64, len(exprs))
	for key, value := range rightColMap {
		find := false
		for _, col := range value {
			for i, rightCol := range exprRightCols {
				if col == rightCol {
					rightColKey[i] = ^key
					find = true
					break
				}
			}
			if find {
				break
			}
		}
		if !find {
			return nil
		}
	}

	node.Stats.HashmapStats.HashOnPK = true
	if leftColMap == nil {
		return nil
	}

	leftColKey := make([]uint64, len(exprs))
	for key, value := range leftColMap {
		find := false
		for _, col := range value {
			for i, leftCol := range exprLeftCols {
				if col == leftCol {
					leftColKey[i] = ^key
					find = true
					break
				}
			}
			if find {
				break
			}
		}
	}

	for i := range leftColKey {
		if leftColKey[i] != 0 && rightColKey[i] != 0 {
			leftColMap[^leftColKey[i]] = append(leftColMap[^leftColKey[i]], rightColMap[^rightColKey[i]]...)
		}
	}

	return leftColMap
}

func referencedTableColumnType(
	nodeID int32,
	col *plan.ColRef,
	builder *QueryBuilder,
) (plan.Type, bool) {
	if builder == nil || builder.qry == nil || col == nil {
		return plan.Type{}, false
	}
	visited := make(map[int32]struct{})
	var found plan.Type
	matches := 0
	var visit func(int32)
	visit = func(currentID int32) {
		if currentID < 0 || int(currentID) >= len(builder.qry.Nodes) {
			return
		}
		if _, seen := visited[currentID]; seen {
			return
		}
		visited[currentID] = struct{}{}
		current := builder.qry.Nodes[currentID]
		if current == nil {
			return
		}
		if current.NodeType == plan.Node_TABLE_SCAN {
			for _, tag := range current.BindingTags {
				if tag != col.RelPos {
					continue
				}
				if current.TableDef == nil || col.ColPos < 0 ||
					int(col.ColPos) >= len(current.TableDef.Cols) ||
					current.TableDef.Cols[col.ColPos] == nil {
					matches = 2
					return
				}
				found = current.TableDef.Cols[col.ColPos].Typ
				matches++
			}
		}
		for _, childID := range current.Children {
			visit(childID)
		}
	}
	visit(nodeID)
	return found, matches == 1
}

func getHashColsNDVRatio(nodeID int32, builder *QueryBuilder) (float64, bool) {
	node := builder.qry.Nodes[nodeID]
	if node.NodeType != plan.Node_JOIN {
		return 1, true
	}
	result, ok := getHashColsNDVRatio(builder.qry.Nodes[node.Children[1]].NodeId, builder)
	if !ok {
		return 1, false
	}

	leftTags := make(map[int32]bool)
	for _, tag := range builder.enumerateTags(node.Children[0]) {
		leftTags[tag] = true
	}

	rightTags := make(map[int32]bool)
	for _, tag := range builder.enumerateTags(node.Children[1]) {
		rightTags[tag] = true
	}

	exprs := make([]*plan.Expr, 0)
	for _, expr := range node.OnList {
		if equi := isEquiCond(expr, leftTags, rightTags); equi {
			exprs = append(exprs, expr)
		}
	}

	hashCols := make([]*plan.ColRef, 0)
	for _, cond := range exprs {
		switch condImpl := cond.Expr.(type) {
		case *plan.Expr_F:
			expr := condImpl.F.Args[1]
			switch exprImpl := expr.Expr.(type) {
			case *plan.Expr_Col:
				hashCols = append(hashCols, exprImpl.Col)
			}
		}
	}

	if len(hashCols) == 0 {
		return 1, false
	}

	tableDef := findHashOnPKTable(node.Children[1], hashCols[0].RelPos, builder)
	if tableDef == nil {
		return 1, false
	}
	hashColPos := make([]int32, len(hashCols))
	for i := range hashCols {
		hashColPos[i] = hashCols[i].ColPos
	}
	return builder.getColNDVRatio(hashColPos, tableDef) * result, true
}

func checkExprInTags(expr *plan.Expr, tags []int32) bool {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		for i := range exprImpl.F.Args {
			if !checkExprInTags(exprImpl.F.Args[i], tags) {
				return false
			}
		}
		return true

	case *plan.Expr_Col:
		for i := range tags {
			if tags[i] == exprImpl.Col.RelPos {
				return true
			}
		}
	}
	return false
}

func (builder *QueryBuilder) rewriteDistinctToAGG(nodeID int32) {
	node := builder.qry.Nodes[nodeID]
	if len(node.Children) > 0 {
		for _, child := range node.Children {
			builder.rewriteDistinctToAGG(child)
		}
	}
	if node.NodeType != plan.Node_DISTINCT {
		return
	}
	project := builder.qry.Nodes[node.Children[0]]
	if project.NodeType != plan.Node_PROJECT {
		return
	}
	if builder.qry.Nodes[project.Children[0]].NodeType == plan.Node_VALUE_SCAN {
		return
	}

	node.NodeType = plan.Node_AGG
	node.GroupBy = project.ProjectList
	if len(node.PhysicalEqualityKeyList) > 0 {
		visibleCount := len(node.GroupBy)
		node.ProjectList = make([]*plan.Expr, visibleCount)
		for i, expr := range project.ProjectList {
			node.ProjectList[i] = GetColExpr(expr.Typ, -1, int32(i))
		}
		for i, key := range node.PhysicalEqualityKeyList {
			node.GroupBy = append(node.GroupBy, DeepCopyExpr(key))
			node.GroupByHashKey = append(node.GroupByHashKey, int32(visibleCount+i))
		}
	}
	node.BindingTags = project.BindingTags
	node.BindingTags = append(node.BindingTags, builder.genNewBindTag())
	node.Children[0] = project.Children[0]
	node.SpillMem = builder.aggSpillMem
}

// reuse removeSimpleProjections to delete this plan node
func (builder *QueryBuilder) rewriteEffectlessAggToProject(nodeID int32) int32 {
	remap := make(map[[2]int32]*plan.Expr)
	rewritten := make(map[int32]struct{})
	builder.rewriteEffectlessAggToProjectImpl(nodeID, false, remap, rewritten)
	if len(rewritten) == 0 {
		return nodeID
	}
	if len(remap) > 0 {
		builder.applyEffectlessAggRemap(nodeID, remap)
	}
	return builder.removeConstantSortAfterSingletonGroup(nodeID, rewritten)
}

func (builder *QueryBuilder) rewriteEffectlessAggToProjectImpl(
	nodeID int32,
	limitDemand bool,
	remap map[[2]int32]*plan.Expr,
	rewritten map[int32]struct{},
) {
	node := builder.qry.Nodes[nodeID]
	childLimitDemand := false
	if len(node.Children) == 1 {
		boundedDemand := limitDemand || node.Limit != nil
		switch node.NodeType {
		case plan.Node_PROJECT, plan.Node_SORT:
			// Only unary operators that preserve a single input relation may
			// carry a bounded row demand to an aggregate below them.  In
			// particular, never let an outer LIMIT cross a join or a shared CTE
			// boundary and accidentally rewrite an unrelated aggregate.
			childLimitDemand = boundedDemand
		case plan.Node_FILTER:
			// A HAVING Filter is above Aggregate on the first rewrite pass. If
			// Aggregate is removed, filter pushdown can move that predicate onto
			// the scan before the second pass. Carry demand through only when the
			// predicate is total and side-effect free; otherwise the rewrite can
			// change which errors or volatile evaluations are reached.
			childLimitDemand = boundedDemand && !node.IsEnd &&
				!node.FilterIsBarrier && !node.RollupFilter &&
				areTruncationSafePredicates(node.FilterList)
		}
	}
	if len(node.Children) > 0 {
		for _, child := range node.Children {
			builder.rewriteEffectlessAggToProjectImpl(child, childLimitDemand, remap, rewritten)
		}
	}
	if node.NodeType != plan.Node_AGG {
		return
	}
	if node.ProjectList != nil ||
		len(node.Children) != 1 ||
		len(node.BindingTags) == 0 {
		return
	}
	if len(node.FilterList) > 0 &&
		(!limitDemand || !areTruncationSafePredicates(node.FilterList)) {
		// optimizeFilters can attach HAVING directly to Aggregate. It may move to
		// the input only when bounded demand exists and the complete predicate is
		// total; otherwise retain the established blocking evaluation boundary.
		return
	}
	if hasInactiveGroupingColumn(node.GroupingFlag) {
		// An inactive key emits NULL for this grouping-set branch.  A Project
		// cannot reproduce that row even when the branch has no aggregate
		// functions, so this is an unconditional semantic barrier.
		return
	}
	if len(node.GroupingFlag) > 0 && len(node.GroupingFlag) != len(node.GroupBy) {
		// GroupingFlag is positional. A truncated or extended vector cannot prove
		// that every logical key is active in this grouping-set branch.
		return
	}
	if len(node.AggList) > 0 && !limitDemand {
		// Aggregate-bearing rewrites are a limit-aware optimization, not a
		// general plan-shape canonicalization.  Requiring a bounded demand
		// keeps unlimited queries on their established physical path.
		return
	}
	scan := builder.qry.Nodes[node.Children[0]]
	if scan.NodeType != plan.Node_TABLE_SCAN || scan.TableDef == nil || scan.TableDef.Pkey == nil {
		return
	}
	pkPositions, ok := sqlEqualityCompatiblePrimaryKeyColumnPositions(scan.TableDef)
	if !ok || len(scan.BindingTags) != 1 {
		return
	}
	seenBindingTags := map[int32]struct{}{scan.BindingTags[0]: {}}
	for _, tag := range node.BindingTags {
		if _, duplicate := seenBindingTags[tag]; duplicate {
			// Output bindings must not alias the input or one another; otherwise a
			// remap cannot distinguish an Aggregate result from a scan column.
			return
		}
		seenBindingTags[tag] = struct{}{}
	}
	groupCol := make([]int32, 0)
	for _, expr := range node.GroupBy {
		if col := expr.GetCol(); col != nil && col.RelPos == scan.BindingTags[0] {
			if col.ColPos < 0 || int(col.ColPos) >= len(scan.TableDef.Cols) ||
				scan.TableDef.Cols[col.ColPos] == nil ||
				!sqlEqualityJoinUsesOneIdentityDomain(
					expr.Typ, scan.TableDef.Cols[col.ColPos].Typ) {
				return
			}
			groupCol = append(groupCol, col.ColPos)
		}
	}
	for _, pk := range pkPositions {
		found := false
		for _, group := range groupCol {
			if group == pk {
				found = true
				break
			}
		}
		if !found {
			return
		}
	}
	if limitDemand {
		for _, expr := range node.GroupBy {
			if !isTruncationSafeRowExpr(expr) {
				return
			}
		}
		if !areTruncationSafePredicates(scan.FilterList) {
			return
		}
	}

	projectList := make([]*plan.Expr, 0, len(node.GroupBy)+len(node.AggList))
	projectList = append(projectList, node.GroupBy...)
	rowAggExprs := make([]*plan.Expr, 0, len(node.AggList))
	if len(node.AggList) > 0 {
		if len(node.BindingTags) < 2 {
			return
		}
		for _, agg := range node.AggList {
			rowExpr, ok := builder.singleRowAggregateExpr(agg)
			if !ok {
				return
			}
			projectList = append(projectList, rowExpr)
			rowAggExprs = append(rowAggExprs, rowExpr)
		}

	}

	if len(node.FilterList) > 0 {
		inputRemap := make(map[[2]int32]*plan.Expr, len(projectList))
		groupTag := node.BindingTags[0]
		for i, groupExpr := range node.GroupBy {
			inputRemap[[2]int32{groupTag, int32(i)}] = groupExpr
		}
		if len(rowAggExprs) > 0 {
			aggTag := node.BindingTags[1]
			for i, rowExpr := range rowAggExprs {
				inputRemap[[2]int32{aggTag, int32(i)}] = rowExpr
			}
		}
		pushedHaving := make([]*plan.Expr, len(node.FilterList))
		for i, predicate := range node.FilterList {
			pushedHaving[i] = replaceColumnsForExpr(DeepCopyExpr(predicate), inputRemap)
			if containsTag(pushedHaving[i], groupTag) ||
				len(rowAggExprs) > 0 && containsTag(pushedHaving[i], node.BindingTags[1]) {
				return
			}
		}
		if !areTruncationSafePredicates(pushedHaving) {
			return
		}
		// Project filters execute before projection, but LIMIT pushdown can move
		// bounded demand to the direct scan. Put the row-equivalent HAVING beside
		// WHERE on that scan so both predicates remain before the new stop point.
		scan.FilterList = append(scan.FilterList, pushedHaving...)
	}
	if len(node.AggList) > 0 {
		// Publish ancestor remaps only after every proof and predicate rewrite has
		// succeeded. A failed node must leave both itself and its consumers intact.
		groupTag := node.BindingTags[0]
		aggTag := node.BindingTags[1]
		for i, agg := range node.AggList {
			remap[[2]int32{aggTag, int32(i)}] = GetColExpr(
				agg.Typ,
				groupTag,
				int32(len(node.GroupBy)+i),
			)
		}
	}

	node.NodeType = plan.Node_PROJECT
	node.BindingTags = node.BindingTags[:1]
	node.ProjectList = projectList
	node.FilterList = nil
	node.GroupBy = nil
	node.AggList = nil
	node.GroupingFlag = nil
	node.GroupByHashKey = nil
	node.SpillMem = 0
	rewritten[nodeID] = struct{}{}
}

// removeConstantSortAfterSingletonGroup removes only Sorts whose complete key
// tuple is proven constant by a Project produced in the same singleton-group
// rewrite pass. Keeping the provenance set local to this pass prevents the rule
// from becoming an unrelated global constant-ORDER-BY canonicalization.
func (builder *QueryBuilder) removeConstantSortAfterSingletonGroup(
	nodeID int32,
	rewritten map[int32]struct{},
) int32 {
	node := builder.qry.Nodes[nodeID]
	for i, childID := range node.Children {
		node.Children[i] = builder.removeConstantSortAfterSingletonGroup(childID, rewritten)
	}

	if node.NodeType != plan.Node_SORT || len(node.Children) != 1 ||
		node.Limit == nil || node.RankOption != nil || len(node.OrderBy) == 0 ||
		builder.sqlCalcFoundRows {
		return nodeID
	}

	childID := node.Children[0]
	child := builder.qry.Nodes[childID]
	if child.RankOption != nil {
		return nodeID
	}
	usesSingletonGroup := false
	for _, order := range node.OrderBy {
		if order == nil || order.Expr == nil {
			return nodeID
		}
		constant, usesSingleton := builder.isConstantSingletonGroupOrderExpr(
			order.Expr, childID, rewritten,
		)
		if !constant {
			return nodeID
		}
		usesSingletonGroup = usesSingletonGroup || usesSingleton
	}
	if !usesSingletonGroup {
		return nodeID
	}

	limit, offset, ok := composePagination(
		child.Limit, child.Offset, node.Limit, node.Offset,
	)
	if !ok {
		return nodeID
	}
	child.Limit, child.Offset = limit, offset
	return childID
}

func (builder *QueryBuilder) isConstantSingletonGroupOrderExpr(
	expr *plan.Expr,
	nodeID int32,
	rewritten map[int32]struct{},
) (constant, usesSingletonGroup bool) {
	resolved := DeepCopyExpr(expr)
	for {
		node := builder.qry.Nodes[nodeID]
		if node.NodeType == plan.Node_FILTER {
			if len(node.Children) != 1 {
				return false, false
			}
			// Filter changes row membership but forwards the input bindings. The
			// pagination window remains above it when Sort is bypassed, so tracing
			// a key through this node neither reorders nor suppresses its predicate.
			nodeID = node.Children[0]
			continue
		}
		if node.NodeType != plan.Node_PROJECT || len(node.BindingTags) != 1 ||
			len(node.Children) != 1 {
			break
		}
		if !containsTag(resolved, node.BindingTags[0]) {
			// A key may contain an independent safe constant alongside a key
			// derived from COUNT(*). The Sort as a whole still has to establish
			// singleton-group provenance before it can be removed.
			break
		}

		projectMap := make(map[[2]int32]*plan.Expr, len(node.ProjectList))
		for i, project := range node.ProjectList {
			if project == nil {
				return false, false
			}
			projectMap[[2]int32{node.BindingTags[0], int32(i)}] = project
		}
		resolved = replaceColumnsForExpr(resolved, projectMap)

		if _, ok := rewritten[nodeID]; ok {
			usesSingletonGroup = true
			break
		}
		nodeID = node.Children[0]
	}

	if !rule.IsConstant(resolved, false) {
		return false, usesSingletonGroup
	}
	if resolved.GetLit() != nil {
		return true, usesSingletonGroup
	}
	folded, err := ConstantFold(
		batch.EmptyForConstFoldBatch,
		DeepCopyExpr(resolved),
		builder.compCtx.GetProcess(),
		false,
		true,
	)
	return err == nil && folded != nil && folded.GetLit() != nil, usesSingletonGroup
}

func (builder *QueryBuilder) singleRowAggregateExpr(
	agg *plan.Expr,
) (*plan.Expr, bool) {
	if agg == nil {
		return nil, false
	}
	fn := agg.GetF()
	if fn == nil || fn.Func == nil || fn.AggConfigType != plan.AggregateConfigType_AGG_CONFIG_NONE ||
		len(fn.AggConfig) != 0 || uint64(fn.Func.Obj)&function.Distinct != 0 {
		return nil, false
	}
	expectedFunctionID, knownAggregate := singleRowAggregateFunctionID(fn.Func.ObjName)
	actualFunctionID, overloadIndex := function.DecodeOverloadID(fn.Func.Obj)
	if !knownAggregate || actualFunctionID != expectedFunctionID || overloadIndex != 0 {
		return nil, false
	}
	overload, registered := function.GetFunctionByIdWithoutError(fn.Func.Obj)
	if !registered || !overload.IsAgg() {
		return nil, false
	}
	rebound, err := BindFuncExprImplByPlanExpr(
		builder.GetContext(), fn.Func.ObjName, DeepCopyExprList(fn.Args))
	if err != nil || rebound == nil || rebound.GetF() == nil ||
		rebound.GetF().Func == nil || rebound.GetF().Func.Obj != fn.Func.Obj ||
		!isSameColumnType(rebound.Typ, agg.Typ) ||
		rebound.Typ.NotNullable != agg.Typ.NotNullable {
		// Validate the complete registered aggregate contract, not only the
		// function family. Otherwise a malformed overload index or a result type
		// that happens to admit a total cast could turn an invalid Aggregate into
		// a valid-looking Project and suppress the original failure.
		return nil, false
	}

	switch fn.Func.ObjName {
	case "starcount":
		// The binder represents COUNT(*) as starcount(1), and the plan-level
		// COUNT(non-null-column) rewrite preserves that column argument. Keep
		// the real one-argument IR contract and reject an argument whose
		// evaluation behavior could otherwise be hidden by a scan LIMIT.
		if len(fn.Args) != 1 || types.T(agg.Typ.Id) != types.T_int64 || !agg.Typ.NotNullable ||
			!fn.Args[0].Typ.NotNullable ||
			!isTruncationSafeRowExpr(fn.Args[0]) {
			return nil, false
		}
		return makePlan2Int64ConstExprWithType(1), true

	case "count":
		if len(fn.Args) != 1 || types.T(agg.Typ.Id) != types.T_int64 || !agg.Typ.NotNullable ||
			!isTruncationSafeRowExpr(fn.Args[0]) {
			return nil, false
		}
		// A direct non-null scan column has no evaluation behavior to retain.
		// Do not use type nullability alone for an arbitrary expression: turning
		// COUNT(expr) into a constant could suppress expression evaluation.
		if fn.Args[0].GetCol() != nil && fn.Args[0].Typ.NotNullable {
			return makePlan2Int64ConstExprWithType(1), true
		}
		isNull, err := BindFuncExprImplByPlanExpr(
			builder.GetContext(), "isnull", []*plan.Expr{DeepCopyExpr(fn.Args[0])})
		if err != nil {
			return nil, false
		}
		rowCount, err := BindFuncExprImplByPlanExpr(
			builder.GetContext(), "if", []*plan.Expr{
				isNull,
				makePlan2Int64ConstExprWithType(0),
				makePlan2Int64ConstExprWithType(1),
			})
		if err != nil {
			return nil, false
		}
		return rowCount, true

	case "sum", "avg":
		if len(fn.Args) != 1 || !isTruncationSafeRowExpr(fn.Args[0]) {
			return nil, false
		}
		if !singleRowSumOrAvgCastIsExact(fn.Func.ObjName, fn.Args[0].Typ, agg.Typ) {
			return nil, false
		}
		fallthrough

	case "min", "max", "any_value":
		if len(fn.Args) != 1 || !isTruncationSafeRowExpr(fn.Args[0]) {
			return nil, false
		}
		if !singleRowCastIsTotal(fn.Args[0].Typ, agg.Typ) {
			// The blocking Aggregate used to reach this conversion for every
			// singleton group. A scan LIMIT must not hide a later cast failure.
			return nil, false
		}
		target := makeTypeByPlan2Type(agg.Typ)
		rowValue, err := makePlan2CastExpr(
			builder.GetContext(),
			DeepCopyExpr(fn.Args[0]),
			makePlan2Type(&target),
		)
		if err != nil {
			return nil, false
		}
		return rowValue, true
	}
	return nil, false
}

func singleRowAggregateFunctionID(name string) (int32, bool) {
	switch name {
	case "starcount":
		return function.STARCOUNT, true
	case "count":
		return function.COUNT, true
	case "sum":
		return function.SUM, true
	case "avg":
		return function.AVG, true
	case "min":
		return function.MIN, true
	case "max":
		return function.MAX, true
	case "any_value":
		return function.ANY_VALUE, true
	default:
		return 0, false
	}
}

// isTruncationSafeRowExpr proves that moving an expression from below a
// blocking Aggregate to a streaming Project cannot suppress an error or an
// externally visible evaluation. Keep this proof deliberately structural: an
// arbitrary deterministic function may still fail for part of its input
// domain, so volatility metadata alone is not sufficient.
func isTruncationSafeRowExpr(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	if expr.GetCol() != nil || expr.GetLit() != nil {
		return true
	}

	fn := expr.GetF()
	if fn == nil || fn.Func == nil || fn.Func.ObjName != "cast" || len(fn.Args) != 2 ||
		fn.Args[1].GetT() == nil {
		return false
	}
	overload, ok := function.GetFunctionByIdWithoutError(fn.Func.Obj)
	if !ok || overload.CannotFold() || overload.IsRealTimeRelated() {
		return false
	}
	return isTruncationSafeRowExpr(fn.Args[0]) &&
		singleRowCastIsTotal(fn.Args[0].Typ, expr.Typ)
}

// isTruncationSafePredicateExpr proves that a row predicate below a blocking
// Aggregate is total and side-effect free. Removing the Aggregate allows a scan
// LIMIT to stop predicate evaluation early, so ordinary determinism metadata is
// insufficient: a deterministic scalar can still fail on a later row.
//
// FilterList is the semantic row-filter owner. BlockFilterList contains derived
// pruning copies and does not replace that evaluation, so proving every entry in
// FilterList closes both the row path and any derived block-filter path.
func isTruncationSafePredicateExpr(expr *plan.Expr) bool {
	if expr == nil || types.T(expr.Typ.Id) != types.T_bool {
		// Filter predicates are boolean in valid planner IR. Do not let malformed
		// result metadata turn an arbitrary value expression into a total predicate
		// and thereby move bounded demand across Aggregate.
		return false
	}
	if isTruncationSafePredicateValue(expr) {
		return true
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		return false
	}
	overload, ok := function.GetFunctionByIdWithoutError(fn.Func.Obj)
	if !ok || overload.CannotFold() || overload.IsRealTimeRelated() {
		return false
	}
	functionID, _ := function.DecodeOverloadID(fn.Func.Obj)

	switch functionID {
	case function.AND, function.OR, function.XOR:
		return len(fn.Args) == 2 &&
			isTruncationSafePredicateExpr(fn.Args[0]) &&
			isTruncationSafePredicateExpr(fn.Args[1])
	case function.NOT:
		return len(fn.Args) == 1 && isTruncationSafePredicateExpr(fn.Args[0])
	case function.EQUAL, function.NOT_EQUAL,
		function.GREAT_THAN, function.GREAT_EQUAL,
		function.LESS_THAN, function.LESS_EQUAL:
		return isTruncationSafeComparison(functionID, fn.Args)
	case function.BETWEEN:
		return isTruncationSafeBetween(fn.Args)
	case function.IN, function.NOT_IN:
		return isTruncationSafeIn(fn.Args)
	case function.IS, function.ISNOT:
		return isTruncationSafeComparison(functionID, fn.Args)
	case function.ISNULL, function.ISNOTNULL,
		function.ISTRUE, function.ISNOTTRUE,
		function.ISFALSE, function.ISNOTFALSE:
		return len(fn.Args) == 1 && isTruncationSafePredicateValue(fn.Args[0])
	default:
		return false
	}
}

func isTruncationSafeComparison(functionID int32, args []*plan.Expr) bool {
	return len(args) == 2 &&
		isTruncationSafePredicateValue(args[0]) &&
		isTruncationSafePredicateValue(args[1]) &&
		comparisonEvaluationIsTotal(functionID, args[0].Typ, args[1].Typ)
}

// comparisonEvaluationIsTotal proves the resolved comparison implementation,
// not merely the shape of its operands. Keep the supported type domains
// explicit so a newly added comparison overload is rejected until its failure
// behavior has been reviewed.
func comparisonEvaluationIsTotal(functionID int32, left, right plan.Type) bool {
	leftID := types.T(left.Id)
	rightID := types.T(right.Id)

	if functionID == function.IS || functionID == function.ISNOT {
		return leftID == types.T_bool && rightID == types.T_bool
	}
	if !isTruncationSafeEquality(functionID) && !isTruncationSafeOrdering(functionID) {
		return false
	}
	if isDatetimeTimestampTypePair(leftID, rightID) {
		return true
	}
	if leftID != rightID {
		// Equality has a direct JSON/BOOL overload.  Valid JSON containers can
		// fail its scalar coercion, so that mixed domain is intentionally not
		// included here.
		return false
	}

	if leftID.IsDecimal() {
		if !validDecimalPlanType(left) || !validDecimalPlanType(right) {
			return false
		}
		if leftID != types.T_decimal256 || left.Scale == right.Scale {
			return true
		}
		// Decimal256 comparisons align scales at execution time.  Scaling the
		// lower-scale coefficient is total only when every value in its declared
		// precision still fits the 76-digit Decimal256 domain.
		lowerScale := left
		if right.Scale < left.Scale {
			lowerScale = right
		}
		delta := left.Scale - right.Scale
		if delta < 0 {
			delta = -delta
		}
		return lowerScale.Width+delta <= types.T_decimal256.ToType().Width
	}

	switch leftID {
	case types.T_bool,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_float32, types.T_float64,
		types.T_char, types.T_varchar,
		types.T_date, types.T_datetime, types.T_timestamp, types.T_time,
		types.T_blob, types.T_text, types.T_datalink,
		types.T_binary, types.T_varbinary,
		types.T_json, types.T_uuid, types.T_Rowid,
		types.T_array_float32, types.T_array_float64,
		types.T_array_bf16, types.T_array_float16,
		types.T_array_int8, types.T_array_uint8,
		types.T_year:
		return true
	case types.T_bit:
		// notEqualFn currently routes BIT through the varlena executor even
		// though BIT is fixed-width.  Keep that resolved overload behind the
		// blocking Aggregate; equality and ordering use fixed-width executors.
		return functionID != function.NOT_EQUAL
	case types.T_geometry, types.T_geometry32:
		return isTruncationSafeEquality(functionID)
	case types.T_enum:
		// EQUAL has a concrete ENUM executor, while NOT_EQUAL is accepted by
		// the registry but has no execution case.
		return functionID == function.EQUAL
	default:
		return false
	}
}

func isTruncationSafeEquality(functionID int32) bool {
	return functionID == function.EQUAL || functionID == function.NOT_EQUAL
}

func isTruncationSafeOrdering(functionID int32) bool {
	switch functionID {
	case function.GREAT_THAN, function.GREAT_EQUAL,
		function.LESS_THAN, function.LESS_EQUAL:
		return true
	default:
		return false
	}
}

func isDatetimeTimestampTypePair(left, right types.T) bool {
	return left == types.T_datetime && right == types.T_timestamp ||
		left == types.T_timestamp && right == types.T_datetime
}

func isTruncationSafeBetween(args []*plan.Expr) bool {
	if len(args) != 3 {
		return false
	}
	for _, arg := range args {
		if !isTruncationSafePredicateValue(arg) {
			return false
		}
	}

	ids := [3]types.T{
		types.T(args[0].Typ.Id),
		types.T(args[1].Typ.Id),
		types.T(args[2].Typ.Id),
	}
	if ids[0] == types.T_datetime || ids[0] == types.T_timestamp {
		for _, id := range ids[1:] {
			if id != types.T_datetime && id != types.T_timestamp {
				return false
			}
		}
		return true
	}
	if ids[0] != ids[1] || ids[0] != ids[2] {
		return false
	}
	if ids[0].IsDecimal() {
		return validDecimalPlanType(args[0].Typ) &&
			validDecimalPlanType(args[1].Typ) &&
			validDecimalPlanType(args[2].Typ) &&
			comparisonEvaluationIsTotal(function.GREAT_EQUAL, args[0].Typ, args[1].Typ) &&
			comparisonEvaluationIsTotal(function.LESS_EQUAL, args[0].Typ, args[2].Typ)
	}

	// This is the exact total domain implemented by betweenImpl.  The
	// function registry accepts some additional comparison-capable types whose
	// BETWEEN executor currently has no case, so do not infer safety from the
	// registry's generic comparison check.
	switch ids[0] {
	case types.T_bool, types.T_bit,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_float32, types.T_float64,
		types.T_date, types.T_datetime, types.T_timestamp, types.T_time,
		types.T_uuid, types.T_Rowid,
		types.T_char, types.T_varchar,
		types.T_blob, types.T_text, types.T_datalink,
		types.T_binary, types.T_varbinary:
		return true
	default:
		return false
	}
}

func isTruncationSafeIn(args []*plan.Expr) bool {
	if len(args) != 2 ||
		!isTruncationSafePredicateValue(args[0]) {
		return false
	}

	left := args[0].Typ
	rightValues, ok := inRHSValues(args[1], left)
	if !ok || len(rightValues) == 0 {
		// A folded LiteralVec is executable payload, not just an opaque constant.
		// Decode it and prove its concrete vector type; otherwise a malformed or
		// cross-domain RHS error could be hidden by scan truncation.
		return false
	}
	for _, right := range rightValues {
		if !isTruncationSafePredicateValue(right) ||
			!sqlEqualityJoinUsesOneIdentityDomain(left, right.Typ) {
			return false
		}
	}
	leftID := types.T(left.Id)
	if leftID.IsDecimal() {
		return validDecimalPlanType(left)
	}
	// IN is implemented as a typed hash lookup.  This list mirrors its concrete
	// overloads; unknown future overloads remain behind the Aggregate barrier.
	switch leftID {
	case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_float32, types.T_float64,
		types.T_varchar, types.T_char,
		types.T_date, types.T_datetime, types.T_bool, types.T_timestamp,
		types.T_blob, types.T_uuid, types.T_text, types.T_time,
		types.T_binary, types.T_varbinary, types.T_year,
		types.T_array_float32, types.T_array_float64, types.T_enum:
		return true
	default:
		return false
	}
}

func areTruncationSafePredicates(exprs []*plan.Expr) bool {
	for _, expr := range exprs {
		if !isTruncationSafePredicateExpr(expr) {
			return false
		}
	}
	return true
}

func isTruncationSafePredicateValue(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	switch value := expr.Expr.(type) {
	case *plan.Expr_Col:
		return value.Col != nil
	case *plan.Expr_Lit:
		return value.Lit != nil
	case *plan.Expr_P:
		return value.P != nil
	case *plan.Expr_V:
		// Variables are resolved again for every input batch and resolution or
		// typed reconstruction can fail. A scan LIMIT would reduce those
		// externally observable resolver calls, unlike a prepared parameter
		// whose executor caches its first successful value.
		return false
	case *plan.Expr_Vec:
		// LiteralVec is executable encoded data and needs operator-specific type
		// validation. IN/NOT IN performs that validation in isTruncationSafeIn;
		// no other accepted predicate operator has a vector-valued scalar operand.
		return false
	case *plan.Expr_List:
		// Expr_List likewise belongs to the IN/NOT IN RHS contract. Treating it as
		// an arbitrary scalar would accept malformed comparison or Filter IR.
		return false
	default:
		return isTruncationSafeRowExpr(expr)
	}
}

func singleRowCastIsTotal(source, target plan.Type) bool {
	sourceID := types.T(source.Id)
	targetID := types.T(target.Id)
	if isSameColumnType(source, target) {
		return !sourceID.IsDecimal() || validDecimalPlanType(source)
	}
	// CHAR comparison binding casts text operands to the fixed CHAR domain.
	// With the same charset and a target at least as wide as the declared
	// source, that cast cannot reject or truncate any source value.
	if targetID == types.T_char &&
		(sourceID == types.T_char || sourceID == types.T_varchar) &&
		source.Charset == target.Charset && source.Width > 0 &&
		target.Width >= source.Width {
		return true
	}
	if sourceID.IsDecimal() {
		if !validDecimalPlanType(source) {
			return false
		}
		if targetID == types.T_float64 {
			// MatrixOne decimals have at most 76 integral digits, which is within
			// the finite float64 exponent range. Precision loss is allowed here:
			// the same cast is evaluated before the singleton aggregate.
			return true
		}
		return decimalDomainContains(source, target)
	}

	if sourceID == types.T_float32 {
		return targetID == types.T_float64
	}
	if !sourceID.IsInteger() {
		return false
	}
	if targetID == types.T_float32 || targetID == types.T_float64 {
		// Every supported integer domain is inside both floating-point exponent
		// ranges. Rounding is part of the original cast and is unchanged.
		return true
	}
	if targetID.IsDecimal() {
		digits := integerDecimalDigits(sourceID)
		return digits > 0 && validDecimalPlanType(target) &&
			digits <= target.Width-target.Scale
	}
	if !targetID.IsInteger() {
		return false
	}

	sourceBits, sourceSigned := integerTypeDomain(sourceID)
	targetBits, targetSigned := integerTypeDomain(targetID)
	if sourceBits == 0 || targetBits == 0 {
		return false
	}
	if sourceSigned == targetSigned {
		return targetBits >= sourceBits
	}
	return !sourceSigned && targetSigned && targetBits > sourceBits
}

func decimalDomainContains(source, target plan.Type) bool {
	sourceID := types.T(source.Id)
	targetID := types.T(target.Id)
	if !sourceID.IsDecimal() || !targetID.IsDecimal() ||
		!validDecimalPlanType(source) || !validDecimalPlanType(target) ||
		target.Scale < source.Scale {
		return false
	}
	return source.Width-source.Scale <= target.Width-target.Scale
}

func validDecimalPlanType(typ plan.Type) bool {
	oid := types.T(typ.Id)
	if !oid.IsDecimal() {
		return false
	}
	return typ.Width > 0 && typ.Width <= oid.ToType().Width &&
		typ.Scale >= 0 && typ.Scale <= typ.Width
}

func integerTypeDomain(oid types.T) (bits int, signed bool) {
	switch oid {
	case types.T_int8:
		return 8, true
	case types.T_int16:
		return 16, true
	case types.T_int32:
		return 32, true
	case types.T_int64:
		return 64, true
	case types.T_uint8:
		return 8, false
	case types.T_uint16:
		return 16, false
	case types.T_uint32:
		return 32, false
	case types.T_uint64:
		return 64, false
	default:
		return 0, false
	}
}

func integerDecimalDigits(oid types.T) int32 {
	switch oid {
	case types.T_int8, types.T_uint8:
		return 3
	case types.T_int16, types.T_uint16:
		return 5
	case types.T_int32, types.T_uint32:
		return 10
	case types.T_int64:
		return 19
	case types.T_uint64:
		return 20
	default:
		return 0
	}
}

func singleRowSumOrAvgCastIsExact(name string, source, target plan.Type) bool {
	sourceID := types.T(source.Id)
	if sourceID == types.T_float32 || sourceID == types.T_float64 {
		// SUM/AVG initialize an arithmetic state, which canonicalizes -0 to +0.
		// A direct cast preserves -0 and can therefore change later predicates.
		return false
	}

	if name != "avg" || !sourceID.IsDecimal() {
		return true
	}

	return decimalDomainContains(source, target)
}

func (builder *QueryBuilder) applyEffectlessAggRemap(
	nodeID int32,
	remap map[[2]int32]*plan.Expr,
) {
	node := builder.qry.Nodes[nodeID]
	replaceColumnsForNode(node, remap)
	for _, child := range node.Children {
		builder.applyEffectlessAggRemap(child, remap)
	}
}

func makeBetweenExprFromDateFormat(equalFunc *plan.Function, dateformatFunc *plan.Function, intervalStr string, builder *QueryBuilder) *plan.Expr {
	dateExpr := DeepCopyExpr(equalFunc.Args[1])
	if intervalStr == "year" {
		sval, _ := dateExpr.GetLit().GetValue().(*plan.Literal_Sval)
		sval.Sval = sval.Sval + "0101"
	}
	begin, err := forceCastExpr(builder.GetContext(), dateExpr, dateformatFunc.Args[0].Typ)
	if err != nil {
		return nil
	}
	begin, err = ConstantFold(batch.EmptyForConstFoldBatch, begin, builder.compCtx.GetProcess(), false, true)
	if err != nil {
		return nil
	}
	interval := MakeIntervalExpr(1, intervalStr)
	end, err := bindFuncExprAndConstFold(builder.GetContext(), builder.compCtx.GetProcess(), "+", []*Expr{DeepCopyExpr(begin), interval})
	if err != nil {
		return nil
	}
	interval = MakeIntervalExpr(1, "microsecond")
	end, err = bindFuncExprAndConstFold(builder.GetContext(), builder.compCtx.GetProcess(), "-", []*Expr{DeepCopyExpr(end), interval})
	if err != nil {
		return nil
	}
	args := []*Expr{dateformatFunc.Args[0], begin, end}
	newFilter, err := bindFuncExprAndConstFold(builder.GetContext(), builder.compCtx.GetProcess(), "between", args)
	if err != nil {
		return nil
	}
	return newFilter
}

func (builder *QueryBuilder) optimizeDateFormatExpr(nodeID int32) {
	if builder.optimizerHints != nil && builder.optimizerHints.optimizeDateFormatExpr != 0 {
		return
	}
	// for date_format(col,'%Y-%m-%d')= '2024-01-19', change this to col between [2024-01-19 00:00:00,2024-01-19 23:59:59]
	node := builder.qry.Nodes[nodeID]
	for _, childID := range node.Children {
		builder.optimizeDateFormatExpr(childID)
	}
	if node.NodeType != plan.Node_TABLE_SCAN || len(node.FilterList) == 0 {
		return
	}
	for i := range node.FilterList {
		expr := node.FilterList[i]
		equalFunc := expr.GetF()
		if equalFunc != nil && equalFunc.Func.ObjName == "=" {
			dateformatFunc := equalFunc.Args[0].GetF()
			if dateformatFunc == nil || dateformatFunc.Func.ObjName != "date_format" {
				continue
			}
			col := dateformatFunc.Args[0].GetCol()
			if col == nil {
				continue
			}
			if dateformatFunc.Args[1].GetLit() == nil {
				continue
			}
			str := dateformatFunc.Args[1].GetLit().GetSval()
			if len(str) == 0 {
				continue
			}
			if equalFunc.Args[1].GetLit() == nil {
				continue
			}
			dateSval := equalFunc.Args[1].GetLit().GetSval()
			var newFilter *plan.Expr
			switch str {
			case "%Y-%m-%d":
				if len(dateSval) != 10 || dateSval[4] != '-' || dateSval[7] != '-' {
					continue
				}
				newFilter = makeBetweenExprFromDateFormat(equalFunc, dateformatFunc, "day", builder)
			case "%Y%m%d":
				if len(dateSval) != 8 {
					continue
				}
				newFilter = makeBetweenExprFromDateFormat(equalFunc, dateformatFunc, "day", builder)
			case "%Y":
				if len(dateSval) != 4 {
					continue
				}
				newFilter = makeBetweenExprFromDateFormat(equalFunc, dateformatFunc, "year", builder)
			}
			if newFilter != nil {
				node.FilterList[i] = newFilter
			}
		}
	}
}

func (builder *QueryBuilder) optimizeLikeExpr(nodeID int32) {
	if builder.optimizerHints != nil && builder.optimizerHints.optimizeLikeExpr != 0 {
		return
	}
	// for a like "abc%", change it to prefix_equal(a,"abc")
	// for a like "abc%def", add an extra filter prefix_equal(a,"abc")
	node := builder.qry.Nodes[nodeID]

	for _, childID := range node.Children {
		builder.optimizeLikeExpr(childID)
	}
	if node.NodeType != plan.Node_TABLE_SCAN || len(node.FilterList) == 0 {
		return
	}
	var newFilters []*plan.Expr
	for i := range node.FilterList {
		expr := node.FilterList[i]
		fun := expr.GetF()
		if fun != nil && fun.Func.ObjName == "like" {
			// Explicit ESCAPE changes how wildcard bytes are interpreted. Keep
			// the original predicate intact instead of applying the two-argument
			// prefix rewrite with its hard-coded default escape semantics.
			if len(fun.Args) != 2 {
				continue
			}
			col := fun.Args[0].GetCol()
			if col == nil {
				continue
			}
			if fun.Args[1].GetLit() == nil {
				continue
			}
			str := fun.Args[1].GetLit().GetSval()
			if len(str) == 0 {
				continue
			}
			index1 := strings.IndexByte(str, '_')
			if index1 > 0 && str[index1-1] == '\\' {
				index1--
			}
			index2 := strings.IndexByte(str, '%')
			if index2 > 0 && str[index2-1] == '\\' {
				index2--
			}
			if index1 == -1 && index2 == -1 {
				// it's col like string without wildcard, can change to equal
				fun.Func.ObjName = function.EqualFunctionName
				fun.Func.Obj = function.EqualFunctionEncodedID
				continue
			}

			indexOfWildCard := index1
			if index1 == -1 {
				indexOfWildCard = index2
			}
			if index2 != -1 && index2 < index1 {
				indexOfWildCard = index2
			}
			if indexOfWildCard <= 0 {
				continue
			}
			newStr := str[:indexOfWildCard]

			newFilter := node.FilterList[i]
			// if no _ and % in the last, we can replace the origin filter
			replaceOrigin := (index1 == -1) && (index2 == len(str)-1)
			if !replaceOrigin {
				newFilter = DeepCopyExpr(newFilter)
				newFilters = append(newFilters, newFilter)
			}
			newFunc := newFilter.GetF()
			newFunc.Func.ObjName = function.PrefixEqualFunctionName
			newFunc.Func.Obj = function.PrefixEqualFunctionEncodedID
			newFunc.Args[1].GetLit().Value.(*plan.Literal_Sval).Sval = newStr
			if replaceOrigin {
				node.BlockFilterList = append(node.BlockFilterList, DeepCopyExpr(newFilter))
			}
		}
	}
	if len(newFilters) > 0 {
		node.FilterList = append(node.FilterList, newFilters...)
		node.BlockFilterList = append(node.BlockFilterList, DeepCopyExprList(newFilters)...)
	}
}

func (builder *QueryBuilder) forceJoinOnOneCN(nodeID int32, force bool) {
	if builder.optimizerHints != nil && builder.optimizerHints.forceOneCN != 0 {
		return
	}

	node := builder.qry.Nodes[nodeID]
	if node.NodeType == plan.Node_TABLE_SCAN {
		node.Stats.ForceOneCN = force
	} else if node.NodeType == plan.Node_JOIN {
		if node.JoinType == plan.Node_DEDUP && !node.Stats.HashmapStats.Shuffle {
			force = true
		}

		if len(node.RuntimeFilterBuildList) > 0 {
			policy := analyzeRuntimeFilterJoinPolicy(node)
			if policy.requiresLocalDelivery &&
				(node.JoinType == plan.Node_INDEX || !node.Stats.HashmapStats.Shuffle) {
				force = true
			}
		}
	}
	for _, childID := range node.Children {
		builder.forceJoinOnOneCN(childID, force)
	}
}

// splitOptimizerHint parses one comma-separated `key=value` entry of the optimizer_hints
// variable. Nothing is trimmed, so in `a=1, applyIndices=1` the second entry has the key
// " applyIndices" and matches no hint -- that hint is simply not applied.
//
// Anything else deciding whether a hint is in effect must call THIS, not re-split the string:
// a copy that trims believes a hint is on while the optimizer ignores it, and the two then
// disagree about what the plan actually did.
func splitOptimizerHint(str string) (key string, value int, ok bool) {
	strs := strings.Split(str, "=")
	if len(strs) != 2 {
		return "", 0, false
	}
	v, err := strconv.Atoi(strs[1])
	if err != nil {
		return "", 0, false
	}
	return strs[0], v, true
}

func handleOptimizerHints(str string, builder *QueryBuilder) {
	key, value, ok := splitOptimizerHint(str)
	if !ok {
		return
	}
	if builder.optimizerHints == nil {
		builder.optimizerHints = &OptimizerHints{}
	}
	switch key {
	case "pushDownLimitToScan":
		builder.optimizerHints.pushDownLimitToScan = value
	case "pushDownTopThroughLeftJoin":
		builder.optimizerHints.pushDownTopThroughLeftJoin = value
	case "pushDownSemiAntiJoins":
		builder.optimizerHints.pushDownSemiAntiJoins = value
	case "aggPushDown":
		builder.optimizerHints.aggPushDown = value
	case "aggPullUp":
		builder.optimizerHints.aggPullUp = value
	case "removeEffectLessLeftJoins":
		builder.optimizerHints.removeEffectLessLeftJoins = value
	case "removeRedundantJoinCond":
		builder.optimizerHints.removeRedundantJoinCond = value
	case "optimizeLikeExpr":
		builder.optimizerHints.optimizeLikeExpr = value
	case "optimizeDateFormatExpr":
		builder.optimizerHints.optimizeDateFormatExpr = value
	case "determineHashOnPK":
		builder.optimizerHints.determineHashOnPK = value
	case "sendMessageFromTopToScan":
		builder.optimizerHints.sendMessageFromTopToScan = value
	case "determineShuffle":
		builder.optimizerHints.determineShuffle = value
	case "blockFilter":
		builder.optimizerHints.blockFilter = value
	case "applyIndices":
		builder.optimizerHints.applyIndices = value
	case "runtimeFilter":
		builder.optimizerHints.runtimeFilter = value
	case "joinOrdering":
		builder.optimizerHints.joinOrdering = value
	case "forceOneCN":
		builder.optimizerHints.forceOneCN = value
	case "execType":
		builder.optimizerHints.execType = value
	case "disableRightJoin":
		builder.optimizerHints.disableRightJoin = value
	case "disableRightSingleRF":
		builder.optimizerHints.disableRightSingleRF = value
	case "sharedComputation":
		builder.optimizerHints.sharedComputation = value
	case "subqueryPredicatePlanning":
		builder.optimizerHints.subqueryPredicatePlanning = value
	case "printShuffle":
		builder.optimizerHints.printShuffle = value
	case "skipDedup":
		builder.optimizerHints.skipDedup = value
	case "outerAntiPlanning":
		builder.optimizerHints.outerAntiPlanning = value
	}
}

func (builder *QueryBuilder) sharedComputationDisabled() bool {
	return builder.optimizerHints != nil && builder.optimizerHints.sharedComputation == 1
}

func (builder *QueryBuilder) subqueryPredicatePlanningDisabled() bool {
	return builder.optimizerHints != nil && builder.optimizerHints.subqueryPredicatePlanning == 1
}

func (builder *QueryBuilder) parseOptimizeHints() {
	v, ok := runtime.ServiceRuntime(builder.compCtx.GetProcess().GetService()).GetGlobalVariables("optimizer_hints")
	if !ok {
		return
	}
	str := v.(string)
	if len(str) == 0 {
		return
	}
	kvs := strings.Split(str, ",")
	for i := range kvs {
		handleOptimizerHints(kvs[i], builder)
	}
}

func (builder *QueryBuilder) optimizeFilters(rootID int32) int32 {
	rootID, _ = builder.pushdownFilters(rootID, nil, false)
	transposeTableScanFilters(builder.compCtx.GetProcess(), builder.qry, rootID)
	foldTableScanFilters(builder.compCtx.GetProcess(), builder.qry, rootID, false)
	ReCalcNodeStats(rootID, builder, true, true, true)
	builder.rewriteInDomainNotInFilters(rootID)
	compositePartBlockFilters := builder.collectCompositePartBlockFilters(rootID)
	builder.mergeFiltersOnCompositeKey(rootID)
	builder.retainConsumedCompositePartBlockFilters(compositePartBlockFilters)
	foldTableScanFilters(builder.compCtx.GetProcess(), builder.qry, rootID, true)
	builder.optimizeDateFormatExpr(rootID)
	builder.optimizeLikeExpr(rootID)
	ReCalcNodeStats(rootID, builder, false, true, true)
	builder.appendCompoundKeyBlockFilters(rootID)
	builder.appendCompositePartBlockFilters(compositePartBlockFilters)
	sortFilterListByStats(builder.GetContext(), rootID, builder)
	return rootID
}

// plan for dml  don't go optimizer, which cause some problem, and this need refactoring
// this is a temp solution to work around some bugs
func (builder *QueryBuilder) tempOptimizeForDML() {
	for _, rootID := range builder.qry.Steps {
		ReCalcNodeStats(rootID, builder, true, false, true)
		builder.handleHashMapMessages(rootID)
	}
}

func (builder *QueryBuilder) lockTableIfLockNoRowsAtTheEndForDelAndUpdate() (err error) {
	query := builder.qry
	if !builder.isForUpdate {
		if query.StmtType != plan.Query_DELETE && query.StmtType != plan.Query_UPDATE {
			return
		}
	}

	baseNode := query.Nodes[0]
	if baseNode.NodeType != plan.Node_TABLE_SCAN {
		return
	}
	tableDef := baseNode.TableDef
	objRef := baseNode.ObjRef
	if builder.isForUpdate && query.StmtType == plan.Query_SELECT {
		if err = validateTableIndexDefinitions(tableDef); err != nil {
			return
		}
	}
	tableIDs := make(map[uint64]bool)
	tableIDs[tableDef.TblId] = true
	for _, idx := range tableDef.Indexes {
		if idx.TableExist {
			_, idxTableDef, e := builder.compCtx.ResolveIndexTableByRef(objRef, idx.IndexTableName, nil)
			if e != nil {
				err = e
				return
			}
			if idxTableDef == nil {
				return
			}
			tableIDs[idxTableDef.TblId] = false
		}
	}

	var lockTarget *plan.LockTarget

	for i := 1; i < len(query.Nodes); i++ {
		node := query.Nodes[i]
		if node.NodeType != plan.Node_LOCK_OP {
			continue
		}

		for _, target := range node.LockTargets {
			isMain, ok := tableIDs[target.TableId]
			if !ok {
				return //unsupport multi delete/multi update
			}
			if isMain && !target.LockTable { // do nothing if already a table lock
				lockTarget = target
			}
		}
	}

	if lockTarget != nil {
		var lockRows *Expr
		pkName := tableDef.Name + "." + tableDef.Pkey.Names[0]
		checkIsPkColExpr := func(e *plan.Expr) bool {
			if col_expr, ok := e.Expr.(*plan.Expr_Col); ok {
				if col_expr.Col.Name == pkName {
					return true
				}
			}
			return false
		}

		for _, expr := range baseNode.FilterList {
			if e, ok := expr.Expr.(*plan.Expr_F); ok {
				if e.F.Func.GetObjName() == "=" {
					//update t1 set a = 1 where pk = 1; then we allays lock rows pk=1, even pk=1 is not exists
					//delete from where pk = 1; then we allays lock rows pk=1, even pk=1 is not exists
					if checkIsPkColExpr(e.F.Args[0]) && rule.IsConstant(e.F.Args[1], true) {
						lockRows = e.F.Args[1]
					} else if checkIsPkColExpr(e.F.Args[1]) && rule.IsConstant(e.F.Args[0], true) {
						lockRows = e.F.Args[0]
					}
				} else if e.F.Func.GetObjName() == "in" {
					//update t1 set a = 1 where pk in (1,2); then we allays lock rows pk in (1,2), even pk=1 is not exists
					//delete from where pk in (1,2); then we allays lock rows pk in (1,2), even pk in (1,2) is not exists
					if checkIsPkColExpr(e.F.Args[0]) && rule.IsConstant(e.F.Args[1], true) {
						lockRows = e.F.Args[1]
					} else if checkIsPkColExpr(e.F.Args[1]) && rule.IsConstant(e.F.Args[0], true) {
						lockRows = e.F.Args[0]
					}
				}
			}
		}

		lockTarget.LockRows = lockRows
		lockTarget.LockTableAtTheEnd = false
	}

	return
}
