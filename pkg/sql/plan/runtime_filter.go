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
	InFilterCardLimit    = 1000
	BloomFilterCardLimit = 1000000

	MinProbeTableRows    = 8192 * 20 // Don't generate runtime filter for small tables
	SelectivityThreshold = 0.5
)

func (builder *QueryBuilder) pushdownRuntimeFilters(nodeID int32) {
	node := builder.qry.Nodes[nodeID]

	for _, childID := range node.Children {
		builder.pushdownRuntimeFilters(childID)
	}

	if node.NodeType != plan.Node_JOIN || node.Stats.Shuffle || node.BuildOnLeft {
		return
	}

	if node.JoinType != plan.Node_INNER && node.JoinType != plan.Node_LEFT && node.JoinType != plan.Node_SEMI && node.JoinType != plan.Node_SINGLE {
		return
	}

	if node.Stats.HashmapSize > InFilterCardLimit*10 || node.Stats.Selectivity > SelectivityThreshold {
		return
	}

	leftChild := builder.qry.Nodes[node.Children[0]]

	// TODO: build runtime filters deeper than 1 level
	if leftChild.NodeType != plan.Node_TABLE_SCAN || leftChild.Stats.Outcnt < MinProbeTableRows {
		return
	}

	leftTags := make(map[int32]any)
	for _, tag := range builder.enumerateTags(node.Children[0]) {
		leftTags[tag] = nil
	}

	rightTags := make(map[int32]any)
	for _, tag := range builder.enumerateTags(node.Children[1]) {
		rightTags[tag] = nil
	}

	var leftArg, rightArg *plan.Expr

	for _, expr := range node.OnList {
		if equi, leftFirst := isEquiCond(expr, leftTags, rightTags); equi {
			// TODO: build runtime filter on multiple columns, especially composite primary key
			if leftArg != nil {
				return
			}

			args := expr.GetF().Args
			if leftFirst {
				if CheckExprIsMonotonic(builder.GetContext(), args[0]) {
					leftArg, rightArg = args[0], args[1]
				}
			} else {
				if CheckExprIsMonotonic(builder.GetContext(), args[1]) {
					leftArg, rightArg = args[1], args[0]
				}
			}
		}
	}

	// No equi condition found
	if leftArg == nil {
		return
	}

	tag := builder.genNewTag()

	node.RuntimeFilterBuildList = append(node.RuntimeFilterBuildList, &plan.RuntimeFilterSpec{
		Tag:  tag,
		Expr: DeepCopyExpr(rightArg),
	})

	leftChild.RuntimeFilterList = append(leftChild.RuntimeFilterList, &plan.RuntimeFilterSpec{
		Tag:  tag,
		Expr: DeepCopyExpr(leftArg),
	})
}
