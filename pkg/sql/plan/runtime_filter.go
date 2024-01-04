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
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

const (
	InFilterCardLimit    = 10000
	BloomFilterCardLimit = 100 * InFilterCardLimit

	MinProbeTableRows    = 8192 * 20 // Don't generate runtime filter for small tables
	SelectivityThreshold = 0.5
)

func GetInFilterCardLimit() int64 {
	v, ok := runtime.ProcessLevelRuntime().GetGlobalVariables("runtime_filter_limit_in")
	if ok {
		return v.(int64)
	}
	return InFilterCardLimit
}

func (builder *QueryBuilder) pushdownRuntimeFilters(nodeID int32) {
	node := builder.qry.Nodes[nodeID]

	for _, childID := range node.Children {
		builder.pushdownRuntimeFilters(childID)
	}

	// Build runtime filters only for broadcast join
	if node.NodeType != plan.Node_JOIN {
		return
	}

	if node.JoinType == plan.Node_LEFT || node.JoinType == plan.Node_OUTER || node.JoinType == plan.Node_SINGLE || node.JoinType == plan.Node_MARK {
		return
	}

	if node.JoinType == plan.Node_ANTI && !node.BuildOnLeft {
		return
	}

	if node.Stats.HashmapStats.Shuffle {
		leftChild := builder.qry.Nodes[node.Children[0]]
		if leftChild.NodeType != plan.Node_TABLE_SCAN {
			return
		}

		if leftChild.NodeType > 0 {
			return
		}

		rfTag := builder.genNewTag()

		node.RuntimeFilterBuildList = append(node.RuntimeFilterBuildList, &plan.RuntimeFilterSpec{Tag: rfTag})
		leftChild.RuntimeFilterProbeList = append(leftChild.RuntimeFilterProbeList, &plan.RuntimeFilterSpec{Tag: rfTag})

		return
	}

	rightChild := builder.qry.Nodes[node.Children[1]]
	if rightChild.Stats.Selectivity > SelectivityThreshold {
		return
	}

	leftChild := builder.qry.Nodes[node.Children[0]]

	// TODO: build runtime filters deeper than 1 level
	if leftChild.NodeType != plan.Node_TABLE_SCAN || leftChild.Stats.Cost < MinProbeTableRows {
		return
	}

	statsCache := builder.compCtx.GetStatsCache()
	if statsCache == nil {
		return
	}

	leftTags := make(map[int32]emptyType)
	for _, tag := range builder.enumerateTags(node.Children[0]) {
		leftTags[tag] = emptyStruct
	}

	rightTags := make(map[int32]emptyType)
	for _, tag := range builder.enumerateTags(node.Children[1]) {
		rightTags[tag] = emptyStruct
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

	rfTag := builder.genNewTag()

	if len(probeExprs) == 1 {
		probeNdv := getExprNdv(probeExprs[0], builder)
		if probeNdv == -1 || node.Stats.HashmapStats.HashmapSize/probeNdv >= 0.1 {
			return
		}

		if node.Stats.HashmapStats.HashmapSize/probeNdv >= 0.1*probeNdv/leftChild.Stats.TableCnt {
			switch col := probeExprs[0].Expr.(type) {
			case (*plan.Expr_Col):
				ctx := builder.ctxByNode[leftChild.NodeId]
				if ctx == nil {
					return
				}
				if binding, ok := ctx.bindingByTag[col.Col.RelPos]; ok {
					tableDef := builder.qry.Nodes[binding.nodeId].TableDef
					colName := tableDef.Cols[col.Col.ColPos].Name
					if GetSortOrder(tableDef, colName) != 0 {
						return
					}
				}

			default:
				return
			}
		}

		leftChild.RuntimeFilterProbeList = append(leftChild.RuntimeFilterProbeList, &plan.RuntimeFilterSpec{
			Tag:  rfTag,
			Expr: DeepCopyExpr(probeExprs[0]),
		})

		node.RuntimeFilterBuildList = append(node.RuntimeFilterBuildList, &plan.RuntimeFilterSpec{
			Tag: rfTag,
			Expr: &plan.Expr{
				Typ: DeepCopyType(buildExprs[0].Typ),
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: -1,
						ColPos: 0,
					},
				},
			},
		})
		recalcStatsByRuntimeFilter(leftChild, rightChild.Stats.Selectivity)
		return
	}

	tableDef := leftChild.TableDef

	if tableDef.Pkey == nil || len(tableDef.Pkey.Names) < len(probeExprs) {
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
		switch col := expr.Expr.(type) {
		case (*plan.Expr_Col):
			if pos, ok := name2Pos[col.Col.Name]; ok {
				col2Probe[pos] = i
			}

		default:
			return
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

	leftChild.RuntimeFilterProbeList = append(leftChild.RuntimeFilterProbeList, &plan.RuntimeFilterSpec{
		Tag: rfTag,
		Expr: &plan.Expr{
			Typ: DeepCopyType(tableDef.Cols[pkIdx].Typ),
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: leftChild.BindingTags[0],
					ColPos: pkIdx,
				},
			},
		},
	})

	buildArgs := make([]*plan.Expr, len(probeExprs))
	for i := range probeExprs {
		pos := col2Probe[i]
		buildArgs[i] = &plan.Expr{
			Typ: DeepCopyType(buildExprs[pos].Typ),
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: 0,
					ColPos: int32(pos),
				},
			},
		}
	}

	buildExpr, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "serial", buildArgs)
	node.RuntimeFilterBuildList = append(node.RuntimeFilterBuildList, &plan.RuntimeFilterSpec{
		Tag:  rfTag,
		Expr: buildExpr,
	})
	recalcStatsByRuntimeFilter(leftChild, rightChild.Stats.Selectivity)
}
