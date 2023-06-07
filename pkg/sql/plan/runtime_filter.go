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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

const (
	InFilterCardLimit    = 1024
	BloomFilterCardLimit = 1000 * InFilterCardLimit

	MinProbeTableRows    = 8192 * 20 // Don't generate runtime filter for small tables
	SelectivityThreshold = 0.5
)

func (builder *QueryBuilder) pushdownRuntimeFilters(nodeID int32) {
	node := builder.qry.Nodes[nodeID]

	for _, childID := range node.Children {
		builder.pushdownRuntimeFilters(childID)
	}

	// Build runtime filters only for broadcast join
	if node.NodeType != plan.Node_JOIN || node.Stats.Shuffle {
		return
	}

	if node.JoinType == plan.Node_OUTER || node.JoinType == plan.Node_MARK {
		return
	}

	if node.JoinType == plan.Node_ANTI && !node.BuildOnLeft {
		return
	}

	if node.Stats.Selectivity > SelectivityThreshold {
		return
	}

	leftChild := builder.qry.Nodes[node.Children[0]]

	// TODO: build runtime filters deeper than 1 level
	if leftChild.NodeType != plan.Node_TABLE_SCAN || leftChild.Stats.Outcnt < MinProbeTableRows {
		return
	}

	statsCache := builder.compCtx.GetStatsCache()
	if statsCache == nil {
		return
	}
	statsMap := statsCache.GetStatsInfoMap(leftChild.TableDef.TblId)

	leftTags := make(map[int32]any)
	for _, tag := range builder.enumerateTags(node.Children[0]) {
		leftTags[tag] = nil
	}

	rightTags := make(map[int32]any)
	for _, tag := range builder.enumerateTags(node.Children[1]) {
		rightTags[tag] = nil
	}

	var probeExpr, buildExpr *plan.Expr

	for _, expr := range node.OnList {
		if equi, leftFirst := isEquiCond(expr, leftTags, rightTags); equi {
			// TODO: build runtime filter on multiple columns, especially composite primary key
			if probeExpr != nil {
				return
			}

			args := expr.GetF().Args
			if leftFirst {
				if CheckExprIsMonotonic(builder.GetContext(), args[0]) {
					probeNdv := getExprNdv(args[0], statsMap.NdvMap, node.Children[0], builder)
					if probeNdv > 0 && node.Stats.HashmapSize/probeNdv < 0.1*probeNdv/leftChild.Stats.TableCnt {
						probeExpr, buildExpr = args[0], args[1]
					}
				}
			} else {
				if CheckExprIsMonotonic(builder.GetContext(), args[1]) {
					probeNdv := getExprNdv(args[1], statsMap.NdvMap, node.Children[0], builder)
					if probeNdv > 0 && node.Stats.HashmapSize/probeNdv < 0.1*probeNdv/leftChild.Stats.TableCnt {
						probeExpr, buildExpr = args[1], args[0]
					}
				}
			}
		}
	}

	// No equi condition found
	if probeExpr == nil {
		return
	}

	tag := builder.genNewTag()

	node.RuntimeFilterBuildList = append(node.RuntimeFilterBuildList, &plan.RuntimeFilterSpec{
		Tag:  tag,
		Expr: DeepCopyExpr(buildExpr),
	})

	leftChild.RuntimeFilterProbeList = append(leftChild.RuntimeFilterProbeList, &plan.RuntimeFilterSpec{
		Tag:  tag,
		Expr: DeepCopyExpr(probeExpr),
	})
}
