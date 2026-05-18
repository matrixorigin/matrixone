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
	planplugin "github.com/matrixorigin/matrixone/pkg/vectorindex/plugin/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/vectorplan"
)

// colRefsWithin reports whether every ColRef in `expr` has ColPos < colCnt.
// Used to gate `canApplyRegularIndex`: a filter that references a column
// out of range can't be safely passed to the regular-index optimizer.
//
// Lifted from pkg/sql/plan/apply_indices_ivfflat.go:colRefsWithin.
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

// extractColRefs counts every ColRef in `expr` that matches `tag` and
// records it in `colRefCnt`. Used to build a minimal colRefCnt for the
// second-scan subtree's regular-index optimizer pass.
//
// Lifted from pkg/sql/plan/apply_indices_ivfflat.go:extractColRefs.
func extractColRefs(expr *plan.Expr, tag int32, colRefCnt map[[2]int32]int) {
	if expr == nil {
		return
	}
	switch impl := expr.Expr.(type) {
	case *plan.Expr_Col:
		if impl.Col.RelPos == tag {
			colRefCnt[[2]int32{tag, impl.Col.ColPos}]++
		}
	case *plan.Expr_F:
		for _, arg := range impl.F.Args {
			extractColRefs(arg, tag, colRefCnt)
		}
	case *plan.Expr_Sub:
		return
	case *plan.Expr_List:
		for _, sub := range impl.List.List {
			extractColRefs(sub, tag, colRefCnt)
		}
	}
}

// refsColumn reports whether `expr` contains a ColRef matching (tag, colPos).
// Used by the pre-mode rewrite to identify filters that reference only the
// vector column (and can therefore be dropped from the second scan's filter
// list).
//
// Lifted from pkg/sql/plan/apply_indices_ivfflat.go:refsColumn.
func refsColumn(expr *plan.Expr, tag int32, colPos int32) bool {
	if expr == nil {
		return false
	}
	switch impl := expr.Expr.(type) {
	case *plan.Expr_Col:
		return impl.Col.RelPos == tag && impl.Col.ColPos == colPos
	case *plan.Expr_F:
		for _, arg := range impl.F.Args {
			if refsColumn(arg, tag, colPos) {
				return true
			}
		}
	case *plan.Expr_Sub:
		return false
	case *plan.Expr_List:
		for _, sub := range impl.List.List {
			if refsColumn(sub, tag, colPos) {
				return true
			}
		}
	}
	return false
}

// canApplyRegularIndex reports whether the regular-index optimizer can
// safely re-write `node`'s filter list. Bails when no filters exist or
// when any filter has out-of-range ColRefs (e.g. references to a peer
// table from a join that was rewritten).
//
// Lifted from pkg/sql/plan/apply_indices_ivfflat.go:canApplyRegularIndex.
func canApplyRegularIndex(node *plan.Node) bool {
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

// clearLimitOffsetInSubtree recursively zeros Limit / Offset on every node
// rooted at nodeID. Used in pre-mode after the inner subtree has been
// optimized: the BloomFilter join must see all primary keys produced by
// the second scan, not a truncated subset.
//
// Lifted from pkg/sql/plan/apply_indices_ivfflat.go:clearLimitOffsetInSubtree.
func ClearLimitOffsetInSubtree(qry *plan.Query, nodeID int32) {
	if qry == nil || nodeID < 0 {
		return
	}
	node := qry.Nodes[nodeID]
	node.Limit = nil
	node.Offset = nil
	for _, childID := range node.Children {
		ClearLimitOffsetInSubtree(qry, childID)
	}
}

// findScanNodeByTag locates the TABLE_SCAN node carrying `tag` in the
// subtree rooted at nodeID. Returns -1 if not found. Used to wire the
// outer-join probe-side runtime filter onto the source scan.
//
// Lifted from pkg/sql/plan/apply_indices_ivfflat.go:findScanNodeByTag.
func FindScanNodeByTag(qry *plan.Query, nodeID, tag int32) int32 {
	return findScanNodeByTagVisited(qry, nodeID, tag, make(map[int32]struct{}))
}

func findScanNodeByTagVisited(qry *plan.Query, nodeID, tag int32, visited map[int32]struct{}) int32 {
	if qry == nil || nodeID < 0 {
		return -1
	}
	if _, seen := visited[nodeID]; seen {
		return -1
	}
	visited[nodeID] = struct{}{}
	node := qry.Nodes[nodeID]
	if node.NodeType == plan.Node_TABLE_SCAN && len(node.BindingTags) > 0 && node.BindingTags[0] == tag {
		return nodeID
	}
	for _, childID := range node.Children {
		if found := findScanNodeByTagVisited(qry, childID, tag, visited); found >= 0 {
			return found
		}
	}
	return -1
}

// buildPkExprFromNode walks the subtree rooted at nodeID looking for the
// primary-key expression on the appropriate node:
//   - TABLE_SCAN: synthesize a ColRef against the scan's binding tag
//   - PROJECT:    locate the PK in the project list (don't recurse — would
//                 produce stale ColRef tags)
//   - others:     recurse into Children[0]
//
// Used by IVF-FLAT pre-mode to build the join condition between the
// optimized outer scan and the inner ivf_search subtree.
//
// Lifted from pkg/sql/plan/apply_indices_ivfflat.go:buildPkExprFromNode.
func buildPkExprFromNode(pb planplugin.PlanBuilder, nodeID int32, pkType plan.Type, pkName string) *plan.Expr {
	qry := pb.Query()
	if qry == nil || nodeID < 0 {
		return nil
	}
	node := qry.Nodes[nodeID]
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
					logutil.Debugf("IVF buildPkExprFromNode: index primary column %q missing in table %q for node %d",
						catalog.IndexTablePrimaryColName, node.TableDef.Name, nodeID)
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
				if pb.GetColName(col) == pkName {
					return vectorplan.DeepCopyExpr(expr)
				}
			}
		}
		return nil
	case plan.Node_JOIN:
		if len(node.Children) > 0 {
			return buildPkExprFromNode(pb, node.Children[0], pkType, pkName)
		}
	default:
		if len(node.Children) > 0 {
			return buildPkExprFromNode(pb, node.Children[0], pkType, pkName)
		}
	}
	return nil
}
