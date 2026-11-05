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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

type boundedDeleteSource struct {
	scan  *plan.Node
	limit uint64
}

type deleteIndexScan struct {
	tableID      uint64
	rejectedJoin *plan.Node
	rejectedScan *plan.Node
}

func (builder *QueryBuilder) recordDeleteIndexCostRejection(join, scan *plan.Node) {
	if len(builder.deleteIndexScanTables) == 0 || len(scan.BindingTags) != 1 || scan.TableDef == nil {
		return
	}
	tag := scan.BindingTags[0]
	registered, ok := builder.deleteIndexScanTables[tag]
	if ok && registered.tableID == scan.TableDef.TblId && registered.rejectedJoin == nil {
		registered.rejectedJoin, registered.rejectedScan = join, scan
		builder.deleteIndexScanTables[tag] = registered
	}
}

// addBoundedDeleteIndexRuntimeFilters runs after the original filters and their
// placement. It never creates a new locality constraint or changes join costing.
func (builder *QueryBuilder) addBoundedDeleteIndexRuntimeFilters(rootID int32) {
	if len(builder.deleteIndexScanTables) == 0 || builder.qry.StmtType != plan.Query_DELETE ||
		len(builder.qry.Steps) != 1 || GetForceScanOnMultiCN() {
		return
	}
	hasRejectedInner := false
	for _, index := range builder.deleteIndexScanTables {
		if index.rejectedJoin != nil && index.rejectedJoin.JoinType == plan.Node_INNER {
			hasRejectedInner = true
			break
		}
	}
	if !hasRejectedInner {
		return
	}
	root := builder.qry.Nodes[rootID]
	if root.NodeType != plan.Node_MULTI_UPDATE || len(root.Children) != 1 ||
		len(root.UpdateCtxList) == 0 || root.UpdateCtxList[0].TableDef == nil {
		return
	}
	targetID := root.UpdateCtxList[0].TableDef.TblId
	// Most DELETEs cannot use this proof. Avoid constructing ownership maps
	// when no target scan has a positive, exactly representable literal bound.
	hasLimit := false
	for _, n := range builder.qry.Nodes {
		if n.NodeType == plan.Node_TABLE_SCAN && n.TableDef != nil && n.TableDef.TblId == targetID {
			limit, ok := getLiteralUint64(n.Limit)
			hasLimit = hasLimit || ok && limit > 0 && limit <= 1<<53
		}
	}
	if !hasLimit {
		return
	}
	parents, live := make(map[int32][]int32), make(map[int32]bool)
	builder.collectCTEParents(rootID, parents, live)
	hardLocal := false
	for id := range live {
		n := builder.qry.Nodes[id]
		if id != rootID && len(parents[id]) != 1 {
			return
		}
		switch n.NodeType {
		case plan.Node_MULTI_UPDATE, plan.Node_LOCK_OP, plan.Node_JOIN, plan.Node_TABLE_SCAN:
		default:
			return
		}
		hardLocal = hardLocal || n.Stats != nil && n.Stats.ForceOneCN
		if len(n.RuntimeFilterProbeList) > 0 {
			if n.NodeType != plan.Node_TABLE_SCAN || len(parents[id]) != 1 {
				return
			}
			parent := builder.qry.Nodes[parents[id][0]]
			if parent.NodeType != plan.Node_JOIN || len(parent.Children) != 2 || parent.Children[0] != id || parent.Stats == nil ||
				parent.Stats.HashmapStats == nil || parent.Stats.HashmapStats.Shuffle {
				return
			}
			for _, probe := range n.RuntimeFilterProbeList {
				matched := false
				for _, build := range parent.RuntimeFilterBuildList {
					matched = matched || probe != nil && build != nil && probe.Tag == build.Tag
				}
				if !matched {
					return
				}
			}
		}
	}
	if !hardLocal || GetExecType(builder.qry, false, false) == ExecTypeAP_MULTICN {
		return
	}
	var visit func(int32) boundedDeleteSource
	visit = func(id int32) boundedDeleteSource {
		n := builder.qry.Nodes[id]
		if n.NodeType == plan.Node_TABLE_SCAN {
			limit, ok := getLiteralUint64(n.Limit)
			if ok && limit > 0 && limit <= 1<<53 && len(n.ProjectList) == 0 &&
				len(n.BindingTags) == 1 && n.TableDef != nil && n.TableDef.TblId == targetID {
				return boundedDeleteSource{scan: n, limit: limit}
			}
			return boundedDeleteSource{}
		}
		if n.NodeType == plan.Node_LOCK_OP || n.NodeType == plan.Node_MULTI_UPDATE {
			if len(n.Children) == 1 {
				visit(n.Children[0])
			}
			return boundedDeleteSource{}
		}
		if len(n.Children) != 2 || len(n.ProjectList) != 0 || n.Limit != nil || n.Offset != nil {
			return boundedDeleteSource{}
		}
		for indexSide, child := range n.Children {
			index := builder.qry.Nodes[child]
			if !builder.isDeleteIndexScan(index) {
				continue
			}
			sourceSide := 1 - indexSide
			if n.JoinType != plan.Node_INNER &&
				!(n.JoinType == plan.Node_LEFT && sourceSide == 0) &&
				!(n.JoinType == plan.Node_RIGHT && sourceSide == 1) {
				return boundedDeleteSource{}
			}
			source := visit(n.Children[sourceSide])
			if source.scan == nil {
				return source
			}
			registered := builder.deleteIndexScanTables[index.BindingTags[0]]
			if registered.rejectedJoin == n && registered.rejectedScan == index &&
				n.JoinType == plan.Node_INNER && indexSide == 0 &&
				len(n.RuntimeFilterBuildList) == 0 && len(n.RuntimeFilterProbeList) == 0 &&
				len(index.RuntimeFilterProbeList) == 0 && n.Stats != nil &&
				n.Stats.HashmapStats != nil && !n.Stats.HashmapStats.HashOnPK &&
				!n.Stats.HashmapStats.Shuffle && deleteIndexJoinKeyIsBounded(n, index, source.scan) {
				builder.generateNodeRuntimeFilter(id, float64(source.limit))
			}
			return source
		}
		return boundedDeleteSource{}
	}
	visit(rootID)
}

func (builder *QueryBuilder) isDeleteIndexScan(n *plan.Node) bool {
	if n.NodeType != plan.Node_TABLE_SCAN || len(n.BindingTags) != 1 || n.TableDef == nil ||
		len(n.FilterList) != 0 || len(n.BlockFilterList) != 0 || len(n.ProjectList) != 0 ||
		n.Limit != nil || n.Offset != nil {
		return false
	}
	identity, registered := builder.deleteIndexScanTables[n.BindingTags[0]]
	return registered && identity.tableID == n.TableDef.TblId
}

func deleteIndexJoinKeyIsBounded(join, index, source *plan.Node) bool {
	if len(join.OnList) != 1 || join.OnList[0] == nil {
		return false
	}
	eq := join.OnList[0].GetF()
	if eq == nil || eq.Func == nil || !IsEqualFunc(eq.Func.Obj) || len(eq.Args) != 2 {
		return false
	}
	pk, ok := sqlEqualityCompatiblePrimaryKeyColumnPositions(index.TableDef)
	if !ok || len(pk) != 1 {
		return false
	}
	for side, operand := range eq.Args {
		col := operand.GetCol()
		if col == nil || col.RelPos != index.BindingTags[0] || col.ColPos != pk[0] ||
			!isSameColumnType(operand.Typ, index.TableDef.Cols[pk[0]].Typ) ||
			!sqlEqualityJoinUsesOneIdentityDomain(operand.Typ, eq.Args[1-side].Typ) {
			continue
		}
		return deleteIndexKeyUsesSource(eq.Args[1-side], source)
	}
	return false
}

// These are the binder's value-only compositions, not arbitrary SQL functions.
// RF construction consumes their already-evaluated hash keys without evaluating
// a second SQL expression or changing which build rows are consumed.
func deleteIndexKeyUsesSource(expr *plan.Expr, source *plan.Node) bool {
	if expr == nil {
		return false
	}
	if col := expr.GetCol(); col != nil {
		return col.RelPos == source.BindingTags[0] && col.ColPos >= 0 &&
			int(col.ColPos) < len(source.TableDef.Cols) && source.TableDef.Cols[col.ColPos] != nil &&
			isSameColumnType(expr.Typ, source.TableDef.Cols[col.ColPos].Typ)
	}
	f := expr.GetF()
	if f == nil || f.Func == nil || len(f.Args) == 0 {
		return false
	}
	id, _ := function.DecodeOverloadID(f.Func.Obj)
	if id != function.SERIAL && id != function.SERIAL_FULL {
		return false
	}
	if _, ok := function.GetFunctionByIdWithoutError(f.Func.Obj); !ok {
		return false
	}
	for _, arg := range f.Args {
		if !deleteIndexKeyUsesSource(arg, source) {
			return false
		}
	}
	return true
}
