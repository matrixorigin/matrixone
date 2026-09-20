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

import "github.com/matrixorigin/matrixone/pkg/pb/plan"

// forceMultipleAdaptiveVectorRegions conservatively prevents replay across
// sibling Top-K regions. One region retains POST -> PRE -> FORCE adaptation;
// statements with multiple explicit AUTO regions use each region's exact FORCE
// plan once, so JOIN/set-operation dependencies cannot deadlock or duplicate
// volatile work.
func (builder *QueryBuilder) forceMultipleAdaptiveVectorRegions(root int32) {
	visited := make(map[int32]struct{})
	autoSorts := make([]*plan.Node, 0, 2)
	var walk func(int32)
	walk = func(id int32) {
		if _, ok := visited[id]; ok || id < 0 || int(id) >= len(builder.qry.Nodes) {
			return
		}
		visited[id] = struct{}{}
		node := builder.qry.Nodes[id]
		if node.NodeType == plan.Node_SORT && node.Limit != nil &&
			node.RankOption != nil && node.RankOption.Mode == "auto" {
			autoSorts = append(autoSorts, node)
		}
		for _, child := range node.Children {
			walk(child)
		}
	}
	walk(root)
	if len(autoSorts) < 2 {
		return
	}
	for _, node := range autoSorts {
		node.RankOption = DeepCopyRankOption(node.RankOption)
		node.RankOption.Mode = "force"
	}
}
