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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
)

// pointRuntimeFilterContext validates the live tree only if a rejected
// candidate has a point-looking build. The tree coordinates stay valid during
// filter generation; its message edges are checked again for each candidate.
type pointRuntimeFilterContext struct {
	root    int32
	checked bool
	tree    *deepRuntimeFilterTree
}

func (ctx *pointRuntimeFilterContext) safe(builder *QueryBuilder) bool {
	if ctx == nil || builder.qry == nil {
		return false
	}
	if !ctx.checked {
		ctx.checked = true
		q := builder.qry
		if len(q.Steps) != 1 || q.Correlation != nil || len(q.BackgroundQueries) != 0 ||
			len(builder.irregularMaintIndexes) != 0 || len(builder.irregularMaintInsertOnlyIndexes) != 0 ||
			len(builder.irregularUpdateMaints) != 0 {
			return false
		}
		ctx.tree = newRuntimeFilterTree(q, ctx.root, pointRuntimeFilterTreeNode)
		if ctx.tree != nil {
			tags := make(map[int32]bool)
			for _, id := range ctx.tree.order {
				for _, tag := range q.Nodes[id].BindingTags {
					if tags[tag] {
						ctx.tree = nil
						return false
					}
					tags[tag] = true
				}
			}
		}
		return ctx.tree != nil
	}
	return ctx.tree != nil && ctx.tree.runtimeFilterEdgesAreAcyclic()
}

func pointRuntimeFilterTreeNode(node *plan.Node) bool {
	switch node.NodeType {
	case plan.Node_MULTI_UPDATE, plan.Node_LOCK_OP:
		if len(node.Children) != 1 || !runtimeFilterOrdinaryContext(node) || len(node.SendMsgList) != 0 ||
			len(node.RuntimeFilterProbeList) != 0 || len(node.RuntimeFilterBuildList) != 0 ||
			(node.NodeType == plan.Node_MULTI_UPDATE && len(node.LockTargets) != 0) ||
			(node.NodeType == plan.Node_LOCK_OP && len(node.UpdateCtxList) != 0) {
			return false
		}
		owner := *node
		owner.UpdateCtxList = nil // table definitions contain catalog defaults, not executable expressions
		return runtimeFilterExpressionsIndependent(&owner)
	default:
		return deepRuntimeFilterOrdinaryNode(node)
	}
}

// pointRuntimeFilterCandidate is a benefit estimate, not a cardinality proof.
// HashBuild collects actual keys and sends PASS when the one-key cap is exceeded.
func (builder *QueryBuilder) pointRuntimeFilterCandidate(
	ctx *pointRuntimeFilterContext, joinID int32, join, probe *plan.Node, probeExpr *plan.Expr,
) bool {
	key := pointRuntimeFilterProbe(join, probe)
	if key == nil || probeExpr == nil || probeExpr.GetCol() == nil ||
		key.GetCol().RelPos != probeExpr.GetCol().RelPos || key.GetCol().ColPos != probeExpr.GetCol().ColPos ||
		!isSameColumnType(key.Typ, probeExpr.Typ) || len(probe.RuntimeFilterProbeList) != 0 ||
		len(join.RuntimeFilterProbeList) != 0 || len(join.RuntimeFilterBuildList) != 0 {
		return false
	}
	if !builder.pointRuntimeFilterBuild(join.Children[1], 0) || !ctx.safe(builder) {
		return false
	}
	for _, id := range ctx.tree.order {
		node := builder.qry.Nodes[id]
		if (node.NodeType == plan.Node_MULTI_UPDATE || node.NodeType == plan.Node_LOCK_OP) &&
			!ctx.tree.contains(id, joinID) {
			return false
		}
	}
	return true
}

// pointRuntimeFilterProbe accepts only a discardable direct scan joined on its
// complete leading physical PK. The original cost-admitted path is unchanged.
func pointRuntimeFilterProbe(join, probe *plan.Node) *plan.Expr {
	if join == nil || probe == nil || !deepRuntimeFilterOrdinaryNode(join) ||
		!deepRuntimeFilterOrdinaryNode(probe) || join.NodeType != plan.Node_JOIN || probe.NodeType != plan.Node_TABLE_SCAN ||
		join.Stats == nil || join.Stats.HashmapStats == nil || join.Stats.HashmapStats.Shuffle ||
		len(join.OnList) != 1 || join.OnList[0] == nil || len(probe.BindingTags) != 1 ||
		probe.TableDef.Pkey == nil || probe.Limit != nil || probe.Offset != nil ||
		len(probe.FilterList) != 0 || len(probe.BlockFilterList) != 0 {
		return nil
	}
	table := probe.TableDef
	pos, ok := table.Name2ColIndex[table.Pkey.PkeyColName]
	if !ok || pos < 0 || int(pos) >= len(table.Cols) || table.Cols[pos] == nil ||
		table.Cols[pos].Name != table.Pkey.PkeyColName || GetSortOrder(table, pos) != 0 {
		return nil
	}
	for _, expr := range probe.ProjectList {
		if expr == nil || expr.GetCol() == nil {
			return nil
		}
		col := expr.GetCol()
		if col.RelPos != probe.BindingTags[0] || col.ColPos < 0 || int(col.ColPos) >= len(table.Cols) ||
			table.Cols[col.ColPos] == nil || !isSameColumnType(expr.Typ, table.Cols[col.ColPos].Typ) {
			return nil
		}
	}
	fn := join.OnList[0].GetF()
	if fn == nil || fn.Func == nil || !IsEqualFunc(fn.Func.Obj) || len(fn.Args) != 2 || fn.Args[0] == nil || fn.Args[1] == nil {
		return nil
	}
	var key *plan.Expr
	for _, arg := range fn.Args {
		if col := arg.GetCol(); col != nil && col.RelPos == probe.BindingTags[0] && col.ColPos == pos &&
			isSameColumnType(arg.Typ, table.Cols[pos].Typ) {
			if key != nil {
				return nil
			}
			key = arg
		}
	}
	return key
}

func (builder *QueryBuilder) pointRuntimeFilterBuild(nodeID int32, depth int) bool {
	if depth >= 64 || nodeID < 0 || int(nodeID) >= len(builder.qry.Nodes) {
		return false
	}
	node := builder.qry.Nodes[nodeID]
	if node == nil || !deepRuntimeFilterOrdinaryNode(node) || len(node.RuntimeFilterProbeList) != 0 {
		return false
	}
	switch node.NodeType {
	case plan.Node_PROJECT:
		return len(node.RuntimeFilterBuildList) == 0 && builder.pointRuntimeFilterBuild(node.Children[0], depth+1)
	case plan.Node_JOIN:
		probeID := node.Children[0]
		if probeID < 0 || int(probeID) >= len(builder.qry.Nodes) ||
			!builder.pointRuntimeFilterOwnedPair(node, builder.qry.Nodes[probeID]) {
			return false
		}
		return builder.pointRuntimeFilterBuild(node.Children[1], depth+1)
	case plan.Node_TABLE_SCAN:
		table := node.TableDef
		if len(node.RuntimeFilterBuildList) != 0 || node.ObjRef == nil || len(node.BindingTags) != 1 ||
			table.TableType != catalog.SystemOrdinaryRel || table.IsTemporary || table.IsDynamic ||
			table.Partition != nil || features.IsPartitioned(table.FeatureFlag) || table.ClusterBy != nil {
			return false
		}
		positions, ok := sqlEqualityCompatiblePrimaryKeyColumnPositions(table)
		if !ok {
			return false
		}
		for _, pos := range positions {
			if !filterListHasConstEqualityOnCol(node.FilterList, node.BindingTags[0], pos) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func (builder *QueryBuilder) pointRuntimeFilterOwnedPair(join, probe *plan.Node) bool {
	key := pointRuntimeFilterProbe(join, probe)
	if key == nil || len(join.RuntimeFilterProbeList) != 0 {
		return false
	}
	if len(probe.RuntimeFilterProbeList) == 0 {
		return len(join.RuntimeFilterBuildList) == 0
	}
	if len(probe.RuntimeFilterProbeList) != 1 || len(join.RuntimeFilterBuildList) != 1 {
		return false
	}
	probeSpec, buildSpec := probe.RuntimeFilterProbeList[0], join.RuntimeFilterBuildList[0]
	if !ordinaryRuntimeFilterSpec(probeSpec) || !ordinaryRuntimeFilterSpec(buildSpec) ||
		!builder.exactRuntimeFilterPairContractValid(probeSpec, buildSpec) || probeSpec.Expr.GetCol() == nil {
		return false
	}
	col, expected := probeSpec.Expr.GetCol(), key.GetCol()
	return col.RelPos == expected.RelPos && col.ColPos == expected.ColPos && isSameColumnType(probeSpec.Expr.Typ, key.Typ)
}
