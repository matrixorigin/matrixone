// Copyright 2026 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// This file hosts the few helpers that used to live in
// apply_indices_ivfflat.go (deleted in Phase 4e) but are referenced from
// other algorithm-specific plan files (apply_indices_fulltext.go,
// apply_indices_vector.go) and from the plugin_builder facade wrappers.
// They are intentionally narrow utilities, not algorithm-specific.

// GetColName returns the column name for a ColRef, consulting the
// builder's nameByColRef table when col.Name is empty.
func (builder *QueryBuilder) GetColName(col *plan.ColRef) string {
	if col == nil {
		return ""
	}
	if builder == nil || builder.nameByColRef == nil {
		return col.Name
	}
	if name := builder.nameByColRef[[2]int32{col.RelPos, col.ColPos}]; name != "" {
		return name
	}
	return col.Name
}

// RebindScanNode reassigns the scan node's binding tag and updates every
// dependent ColRef in its FilterList / BlockFilterList. Used after a
// node is copied so the clone has distinct bindings.
func (builder *QueryBuilder) RebindScanNode(scanNode *plan.Node) {
	if scanNode == nil || len(scanNode.BindingTags) == 0 {
		return
	}
	oldTag := scanNode.BindingTags[0]
	newTag := builder.GenNewBindTag()
	scanNode.BindingTags[0] = newTag
	builder.AddNameByColRef(newTag, scanNode.TableDef)
	for _, expr := range scanNode.FilterList {
		replaceColRefTag(expr, oldTag, newTag)
	}
	// BlockFilterList was copied along with the scanNode and still
	// references the old binding tag.
	for _, expr := range scanNode.BlockFilterList {
		replaceColRefTag(expr, oldTag, newTag)
	}
}

// replaceColRefTag rewrites every ColRef in `expr` matching oldTag to
// newTag, in place. Recurses through Expr_F and Expr_List children.
func replaceColRefTag(expr *plan.Expr, oldTag, newTag int32) {
	if expr == nil {
		return
	}
	switch impl := expr.Expr.(type) {
	case *plan.Expr_Col:
		if impl.Col.RelPos == oldTag {
			impl.Col.RelPos = newTag
		}
	case *plan.Expr_F:
		for _, arg := range impl.F.Args {
			replaceColRefTag(arg, oldTag, newTag)
		}
	case *plan.Expr_List:
		for _, sub := range impl.List.List {
			replaceColRefTag(sub, oldTag, newTag)
		}
	}
}

// canApplyRegularIndex reports whether the regular-index optimizer can
// safely re-write `node`'s filter list. Bails when no filters exist or
// when any filter has out-of-range ColRefs.
func (builder *QueryBuilder) canApplyRegularIndex(node *plan.Node) bool {
	if node == nil || node.TableDef == nil {
		return false
	}
	colCnt := len(node.TableDef.Cols)
	if colCnt == 0 {
		return false
	}
	for _, expr := range node.FilterList {
		if !colRefsWithin(expr, colCnt) {
			return false
		}
	}
	return len(node.FilterList) > 0
}

// colRefsWithin reports whether every ColRef in `expr` has ColPos < colCnt.
func colRefsWithin(expr *plan.Expr, colCnt int) bool {
	if expr == nil {
		return true
	}
	switch impl := expr.Expr.(type) {
	case *plan.Expr_Col:
		return int(impl.Col.ColPos) < colCnt
	case *plan.Expr_F:
		for _, arg := range impl.F.Args {
			if !colRefsWithin(arg, colCnt) {
				return false
			}
		}
		return true
	case *plan.Expr_List:
		for _, sub := range impl.List.List {
			if !colRefsWithin(sub, colCnt) {
				return false
			}
		}
		return true
	default:
		return true
	}
}

// buildPkExprFromNode walks the subtree rooted at nodeID looking for the
// primary-key expression on the appropriate node. TABLE_SCAN synthesizes
// a ColRef against the scan's binding tag; PROJECT scans its project
// list (without recursing — child tags would be stale); others recurse
// into Children[0].
func (builder *QueryBuilder) buildPkExprFromNode(nodeID int32, pkType plan.Type, pkName string) *plan.Expr {
	if builder == nil || nodeID < 0 {
		return nil
	}
	node := builder.qry.Nodes[nodeID]
	switch node.NodeType {
	case plan.Node_TABLE_SCAN:
		if node.TableDef == nil || len(node.BindingTags) == 0 {
			return nil
		}
		colIdx, ok := node.TableDef.Name2ColIndex[pkName]
		if !ok {
			if node.IndexScanInfo.IsIndexScan {
				colIdx, ok = node.TableDef.Name2ColIndex[catalog.IndexTablePrimaryColName]
				if !ok {
					logutil.Debugf("buildPkExprFromNode: index primary column %q missing in table %q for node %d", catalog.IndexTablePrimaryColName, node.TableDef.Name, nodeID)
					return nil
				}
			} else {
				if node.TableDef.Pkey == nil {
					return nil
				}
				colIdx = node.TableDef.Name2ColIndex[node.TableDef.Pkey.PkeyColName]
			}
		}
		return &plan.Expr{
			Typ: pkType,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: node.BindingTags[0],
					ColPos: colIdx,
					Name:   pkName,
				},
			},
		}
	case plan.Node_PROJECT:
		for _, expr := range node.ProjectList {
			if col := expr.GetCol(); col != nil {
				if builder.GetColName(col) == pkName {
					return DeepCopyExpr(expr)
				}
			}
		}
		return nil
	case plan.Node_JOIN:
		if len(node.Children) > 0 {
			return builder.buildPkExprFromNode(node.Children[0], pkType, pkName)
		}
	default:
		if len(node.Children) > 0 {
			return builder.buildPkExprFromNode(node.Children[0], pkType, pkName)
		}
	}
	return nil
}
