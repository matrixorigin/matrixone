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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

const (
	InFilterCardLimitNonPK   = 10000
	InFilterCardLimitPK      = 1000000
	BloomFilterCardLimit     = 100 * InFilterCardLimitNonPK
	InFilterSelectivityLimit = 0.3
)

func GetInFilterCardLimit(sid string) int32 {
	v, ok := runtime.ServiceRuntime(sid).GetGlobalVariables("runtime_filter_limit_in")
	if ok {
		return int32(v.(int64))
	}
	return InFilterCardLimitNonPK
}

func GetInFilterCardLimitOnPK(
	sid string,
	tableCnt float64,
) int32 {
	upper := tableCnt * InFilterSelectivityLimit
	if upper > InFilterCardLimitPK {
		upper = InFilterCardLimitPK
	}
	lower := float64(GetInFilterCardLimit(sid))
	if upper < lower {
		upper = lower
	}
	return int32(upper)
}

func mustRuntimeFilter(n *plan.Node) bool {
	switch n.JoinType {
	case plan.Node_INDEX:
		return true

	case plan.Node_DEDUP:
		return !n.IsRightJoin
	}
	return false
}

func (builder *QueryBuilder) generateRuntimeFilters(nodeID int32) {
	node := builder.qry.Nodes[nodeID]
	sid := builder.compCtx.GetProcess().GetService()

	for _, childID := range node.Children {
		builder.generateRuntimeFilters(childID)
	}

	if builder.isMasterIndexInnerJoin(node) {
		return
	}

	// Build runtime filters only for broadcast join
	if node.NodeType != plan.Node_JOIN {
		return
	}

	// if this node has already pushed runtime filter, just return
	if len(node.RuntimeFilterBuildList) > 0 {
		return
	}

	if node.Stats.HashmapStats.Shuffle {
		rfTag := builder.genNewMsgTag()
		node.RuntimeFilterProbeList = append(node.RuntimeFilterProbeList, MakeRuntimeFilter(rfTag, false, 0, nil, false))
		node.RuntimeFilterBuildList = append(node.RuntimeFilterBuildList, MakeRuntimeFilter(rfTag, false, 0, nil, false))
		return
	}

	if node.JoinType == plan.Node_LEFT || node.JoinType == plan.Node_OUTER || node.JoinType == plan.Node_SINGLE || node.JoinType == plan.Node_MARK {
		return
	}

	if node.JoinType == plan.Node_ANTI && !node.IsRightJoin {
		return
	}

	if node.JoinType == plan.Node_DEDUP && node.IsRightJoin {
		return
	}

	leftChild := builder.qry.Nodes[node.Children[0]]

	// TODO: build runtime filters deeper than 1 level
	if leftChild.NodeType != plan.Node_TABLE_SCAN || leftChild.Limit != nil {
		return
	}

	rightChild := builder.qry.Nodes[node.Children[1]]
	if !mustRuntimeFilter(node) && rightChild.Stats.Outcnt > 5000000 {
		return
	}
	if node.Stats.HashmapStats.HashOnPK && rightChild.Stats.Outcnt > 320000 {
		return
	}

	leftTags := make(map[int32]bool)
	for _, tag := range builder.enumerateTags(node.Children[0]) {
		leftTags[tag] = true
	}

	rightTags := make(map[int32]bool)
	for _, tag := range builder.enumerateTags(node.Children[1]) {
		rightTags[tag] = true
	}

	var probeExprs, buildExprs []*plan.Expr

	for _, expr := range node.OnList {
		if isEquiCond(expr, leftTags, rightTags) {
			args := expr.GetF().Args
			if !ExprIsZonemappable(builder.GetContext(), args[0]) {
				return
			}
			probeExprs = append(probeExprs, args[0])
			buildExprs = append(buildExprs, args[1])

		}
	}

	// No equi condition found
	if probeExprs == nil {
		return
	}

	rfTag := builder.genNewMsgTag()

	for i := range probeExprs {
		exprType := makeTypeByPlan2Expr(probeExprs[i])
		args := []types.Type{exprType, exprType}
		_, err := function.GetFunctionByName(builder.GetContext(), "in", args)
		if err != nil {
			//don't support this type
			return
		}
	}

	if len(probeExprs) == 1 {
		convertToCPKey := false
		tableDef := leftChild.TableDef
		probeCol := probeExprs[0].GetCol()
		if probeCol == nil {
			return
		}
		sortOrder := GetSortOrder(tableDef, probeCol.ColPos)
		if node.JoinType != plan.Node_INDEX {
			probeNdv := getExprNdv(probeExprs[0], builder)
			if probeNdv <= 1 {
				//maybe not flushed yet, set at least 100 to continue calculation
				probeNdv = 100
			}
			if node.Stats.HashmapStats.HashmapSize/probeNdv >= 0.1 {
				return
			}
			if sortOrder != 0 {
				if node.Stats.HashmapStats.HashmapSize/probeNdv >= 0.1*probeNdv/leftChild.Stats.TableCnt {
					return
				}
			} else {
				if len(tableDef.Pkey.Names) > 1 && probeCol.Name != catalog.CPrimaryKeyColName {
					convertToCPKey = true
				}
			}
			//todo: need to fix this in the future
			//if probeCol.Name != tableDef.Pkey.PkeyColName && builder.getColOverlap(probeCol) > overlapThreshold {
			//	return
			//}
		}

		if builder.optimizerHints != nil && builder.optimizerHints.runtimeFilter != 0 && !mustRuntimeFilter(node) {
			return
		}

		notOnPk := probeCol.Name != tableDef.Pkey.PkeyColName
		if convertToCPKey {
			leftChild.RuntimeFilterProbeList = append(leftChild.RuntimeFilterProbeList, MakeCPKEYRuntimeFilter(rfTag, 0, DeepCopyExpr(probeExprs[0]), tableDef, notOnPk))
		} else {
			leftChild.RuntimeFilterProbeList = append(leftChild.RuntimeFilterProbeList, MakeRuntimeFilter(rfTag, false, 0, DeepCopyExpr(probeExprs[0]), notOnPk))
		}

		inLimit := GetInFilterCardLimit(sid)
		if sortOrder == 0 {
			inLimit = GetInFilterCardLimitOnPK(sid, leftChild.Stats.TableCnt)
		}
		buildExpr := &plan.Expr{
			Typ: buildExprs[0].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: -1,
					ColPos: 0,
				},
			},
		}
		if convertToCPKey {
			node.RuntimeFilterBuildList = append(node.RuntimeFilterBuildList, MakeSerialRuntimeFilter(builder.GetContext(), rfTag, false, inLimit, buildExpr, notOnPk))
		} else {
			node.RuntimeFilterBuildList = append(node.RuntimeFilterBuildList, MakeRuntimeFilter(rfTag, false, inLimit, buildExpr, notOnPk))
		}
		recalcStatsByRuntimeFilter(leftChild, node, builder)
		return
	}

	tableDef := leftChild.TableDef
	if len(tableDef.Pkey.Names) < len(probeExprs) {
		return
	}

	name2Pos := make(map[string]int)
	for i, name := range tableDef.Pkey.Names {
		name2Pos[name] = i
	}

	col2Probe := make([]int, len(tableDef.Pkey.Names))
	for i := range col2Probe {
		col2Probe[i] = -1
	}

	for i, expr := range probeExprs {
		col := expr.GetCol()
		if col == nil {
			return
		}
		if pos, ok := name2Pos[tableDef.Cols[col.ColPos].Name]; ok {
			col2Probe[pos] = i
		}
	}

	cnt := 0
	for ; cnt < len(col2Probe); cnt++ {
		if col2Probe[cnt] == -1 {
			break
		}
	}

	if cnt != len(probeExprs) {
		return
	}

	pkIdx, ok := tableDef.Name2ColIndex[tableDef.Pkey.PkeyColName]
	if !ok {
		return
	}

	if builder.optimizerHints != nil && builder.optimizerHints.runtimeFilter != 0 && node.JoinType != plan.Node_INDEX {
		return
	}

	probeExpr := &plan.Expr{
		Typ: tableDef.Cols[pkIdx].Typ,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: leftChild.BindingTags[0],
				ColPos: pkIdx,
			},
		},
	}
	leftChild.RuntimeFilterProbeList = append(node.RuntimeFilterProbeList, MakeRuntimeFilter(rfTag, cnt < len(tableDef.Pkey.Names), 0, probeExpr, false))

	buildArgs := make([]*plan.Expr, len(probeExprs))
	for i := range probeExprs {
		pos := col2Probe[i]
		buildArgs[i] = &plan.Expr{
			Typ: buildExprs[pos].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: -1,
					ColPos: int32(pos),
				},
			},
		}
	}

	buildExpr, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "serial", buildArgs)

	node.RuntimeFilterBuildList = append(node.RuntimeFilterBuildList, MakeRuntimeFilter(rfTag, cnt < len(tableDef.Pkey.Names), GetInFilterCardLimitOnPK(sid, leftChild.Stats.TableCnt), buildExpr, false))
	recalcStatsByRuntimeFilter(leftChild, node, builder)
}

func (builder *QueryBuilder) isMasterIndexInnerJoin(node *plan.Node) bool {
	// In Master Index, INNER Joins in the query plan should not have runtime filters, as it sets
	// input rows to 0 for right child, which is not expected.
	// https://github.com/matrixorigin/matrixone/issues/14876#issuecomment-2148824892
	if !(node.JoinType == plan.Node_INNER && len(node.Children) == 2) {
		return false
	}

	leftChild := builder.qry.Nodes[node.Children[0]]
	rightChild := builder.qry.Nodes[node.Children[1]]

	if leftChild.TableDef == nil || leftChild.TableDef.Cols == nil || len(leftChild.TableDef.Cols) != 3 {
		return false
	}

	if rightChild.TableDef == nil || rightChild.TableDef.Cols == nil || len(rightChild.TableDef.Cols) != 3 {
		return false
	}

	// In Master Index, both the children are from the same master index table.
	if leftChild.TableDef.Name != rightChild.TableDef.Name {
		return false
	}

	// Check if left child is a master/secondary index table
	//TODO: verify if Cols will contain  __mo_cpkey
	for _, column := range leftChild.TableDef.Cols {
		if column.Name == catalog.MasterIndexTablePrimaryColName {
			continue
		}
		if column.Name == catalog.MasterIndexTableIndexColName {
			continue
		}
		if column.Name == catalog.Row_ID {
			continue
		}
		return false
	}

	// Check if right child is a master/secondary index table
	for _, column := range rightChild.TableDef.Cols {
		if column.Name == catalog.MasterIndexTablePrimaryColName {
			continue
		}
		if column.Name == catalog.MasterIndexTableIndexColName {
			continue
		}
		if column.Name == catalog.Row_ID {
			continue
		}
		return false
	}

	return true

}
