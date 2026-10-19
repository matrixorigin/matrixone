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

// A pagination SORT demands its keys from every input row, but output-only
// scalar subqueries belong to the surviving rows. Preserve key/filter inputs
// below SORT and carry the original row columns for the delayed projection.
// This runs after physical remapping and before stage identities are frozen.
func (builder *QueryBuilder) deferDependentProjectionsAfterSort() error {
	q := builder.qry
	for _, n := range q.Nodes {
		if n.NodeType != plan.Node_SORT || len(n.Children) != 1 || (n.Limit == nil && n.Offset == nil) {
			continue
		}
		p := q.Nodes[n.Children[0]]
		if p.NodeType != plan.Node_PROJECT || len(p.Children) != 1 || len(p.FilterList) != 0 || p.Limit != nil || p.Offset != nil {
			continue
		}
		required := make(map[[2]int32]int)
		for _, key := range n.OrderBy {
			increaseRefCnt(key.Expr, 1, required)
		}
		for _, filter := range n.FilterList {
			increaseRefCnt(filter, 1, required)
		}
		delayed := make(map[int]*plan.Expr)
		input := q.Nodes[p.Children[0]]
		offset := int32(len(p.ProjectList))
		mapping := make(map[[2]int32][2]int32, len(input.ProjectList))
		for i := range input.ProjectList {
			mapping[[2]int32{0, int32(i)}] = [2]int32{0, offset + int32(i)}
		}
		for i, expr := range p.ProjectList {
			if required[[2]int32{0, int32(i)}] != 0 {
				continue
			}
			dependent := false
			_ = plan.VisitExprTree(expr, func(e *plan.Expr) error {
				dependent = dependent || e.GetDependent() != nil
				return nil
			})
			if !dependent {
				continue
			}
			copy := DeepCopyExpr(expr)
			if err := builder.remapColRefForExpr(copy, mapping, nil); err != nil {
				return err
			}
			// Multiple output aliases still observe this occurrence once.
			if copy.AuxId >= 0 {
				memo, err := (&baseBinder{builder: builder}).allocateVolatileExprMemoID()
				if err != nil {
					return err
				}
				copy.AuxId = memo
			}
			delayed[i] = copy
		}
		if len(delayed) == 0 {
			continue
		}
		pre := DeepCopyNode(p)
		pre.NodeId = int32(len(q.Nodes))
		postColumns := make([]*plan.Expr, len(p.ProjectList))
		for i, expr := range p.ProjectList {
			postColumns[i] = GetColExpr(expr.Typ, 0, int32(i))
			if deferred := delayed[i]; deferred != nil {
				postColumns[i] = deferred
				pre.ProjectList[i] = &plan.Expr{Typ: expr.Typ, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}}}
				pre.ProjectList[i].Typ.NotNullable = false
			}
		}
		for i, expr := range input.ProjectList {
			pre.ProjectList = append(pre.ProjectList, GetColExpr(expr.Typ, 0, int32(i)))
		}
		sort := DeepCopyNode(n)
		sort.NodeId = pre.NodeId + 1
		sort.Children = []int32{pre.NodeId}
		sort.ProjectList = make([]*plan.Expr, len(pre.ProjectList))
		for i, expr := range pre.ProjectList {
			sort.ProjectList[i] = GetColExpr(expr.Typ, 0, int32(i))
		}
		post := DeepCopyNode(n)
		post.NodeType = plan.Node_PROJECT
		post.Children = []int32{sort.NodeId}
		post.FilterList, post.OrderBy, post.Limit, post.Offset = nil, nil, nil, nil
		for i, expr := range post.ProjectList {
			post.ProjectList[i] = replaceColRefs(expr, 0, postColumns)
		}
		q.Nodes = append(q.Nodes, pre, sort)
		q.Nodes[n.NodeId] = post
	}
	return nil
}
