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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func (builder *QueryBuilder) remapVectorQuery(
	node *plan.Node, step int32, counts map[[2]int32]int, bools map[[2]int32]bool, sinks map[[2]int32]int,
) (*ColRefRemapping, error) {
	if len(node.Children) != 3 || len(node.BindingTags) != 1 || len(node.ProjectList) == 0 {
		return nil, moerr.NewInternalError(builder.GetContext(), "invalid scalar vector query topology")
	}
	provider := builder.qry.Nodes[node.Children[0]]
	if len(provider.BindingTags) != 1 || len(provider.ProjectList) != 1 {
		return nil, moerr.NewInternalError(builder.GetContext(), "invalid scalar vector provider projection")
	}
	counts[[2]int32{provider.BindingTags[0], 0}]++
	if _, err := builder.remapAllColRefs(provider.NodeId, step, counts, bools, sinks); err != nil {
		return nil, err
	}
	tag := node.BindingTags[0]
	needed := make([]int, 0, len(node.ProjectList))
	for i := range node.ProjectList {
		if counts[[2]int32{tag, int32(i)}] > 0 {
			needed = append(needed, i)
		}
	}
	if len(needed) == 0 {
		needed = append(needed, 0)
	}
	for _, id := range node.Children[1:] {
		child := builder.qry.Nodes[id]
		if child.NodeType != plan.Node_PROJECT || len(child.BindingTags) != 1 || len(child.ProjectList) != len(node.ProjectList) {
			return nil, moerr.NewInternalError(builder.GetContext(), "invalid scalar vector result projection")
		}
		// 两条结果分支保持相同输出槽；控制输入的向量不能参与输出裁剪。
		for _, pos := range needed {
			counts[[2]int32{child.BindingTags[0], int32(pos)}]++
		}
		if _, err := builder.remapAllColRefs(id, step, counts, bools, sinks); err != nil {
			return nil, err
		}
		if len(child.ProjectList) != len(needed) {
			return nil, moerr.NewInternalError(builder.GetContext(), "scalar vector branch width mismatch")
		}
	}
	remap := &ColRefRemapping{globalToLocal: make(map[[2]int32][2]int32), preserveRowCount: true}
	projection := make([]*plan.Expr, len(needed))
	for i, pos := range needed {
		remap.addColRef([2]int32{tag, int32(pos)})
		projection[i] = &plan.Expr{Typ: node.ProjectList[pos].Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{
			RelPos: 1, ColPos: int32(i), Name: node.ProjectList[pos].GetCol().GetName(),
			TblName: node.ProjectList[pos].GetCol().GetTblName(), DbName: node.ProjectList[pos].GetCol().GetDbName(),
		}}}
	}
	node.ProjectList, node.BindingTags = projection, nil
	return remap, nil
}
