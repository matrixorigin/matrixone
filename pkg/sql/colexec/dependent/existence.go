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

package dependent

import (
	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// EXISTS observes row presence, including OFFSET and LIMIT, but neither the
// output tuple nor its ordering. Propagate this demand through the final unary
// projection chain. Filters retain their input columns; cardinality-changing
// operators retain their complete inputs. In particular DISTINCT with OFFSET
// must not collapse different tuples to the same placeholder.
//
// Only template-local copies change: a prepared plan and other program owners
// continue to observe their original expressions and column positions.
func existenceProjectionNodes(q *plan.Query, root int32) map[int32]*plan.Node {
	result := make(map[int32]*plan.Node)
	type demand struct {
		node    int32
		columns map[int32]bool
	}
	pending := []demand{{node: root, columns: map[int32]bool{}}}
	seen := make(map[int32]map[int32]bool)
	for len(pending) > 0 {
		d := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		required, visited := seen[d.node]
		changed := !visited
		if !visited {
			required = make(map[int32]bool)
			seen[d.node] = required
		}
		for col := range d.columns {
			if !required[col] {
				required[col] = true
				changed = true
			}
		}
		if !changed {
			continue
		}
		n := q.Nodes[d.node]
		switch n.NodeType {
		case plan.Node_PROJECT, plan.Node_FILTER, plan.Node_SORT, plan.Node_AGG, plan.Node_WINDOW, plan.Node_UNION_ALL:
		default:
			continue
		}
		copy := proto.Clone(n).(*plan.Node)
		for i, expr := range copy.ProjectList {
			if !required[int32(i)] {
				copy.ProjectList[i] = &plan.Expr{Typ: expr.Typ, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}}}
				copy.ProjectList[i].Typ.NotNullable = false
			}
		}
		if n.NodeType == plan.Node_AGG && len(n.GroupBy) == 0 && len(n.FilterList) == 0 && len(required) == 0 {
			// The aggregate itself supplies a row, even when its input is empty.
			copy.NodeType = plan.Node_VALUE_SCAN
			copy.Children, copy.AggList = nil, nil
			result[d.node] = copy
			continue
		}
		childRequired := make(map[int32]bool)
		collect := func(expr *plan.Expr) {
			_ = visitInputColumns(expr, func(col *plan.ColRef) error {
				if col.RelPos == 0 {
					childRequired[col.ColPos] = true
				}
				return nil
			})
		}
		outputs := append(append([]*plan.Expr(nil), copy.FilterList...), copy.ProjectList...)
		switch n.NodeType {
		case plan.Node_SORT:
			copy.OrderBy = nil
			copy.NodeType = plan.Node_FILTER
		case plan.Node_WINDOW:
			width := int32(len(q.Nodes[n.Children[0]].ProjectList))
			needsWindow := false
			for _, expr := range outputs {
				_ = visitInputColumns(expr, func(col *plan.ColRef) error {
					needsWindow = needsWindow || col.ColPos >= width
					return nil
				})
			}
			if needsWindow {
				continue
			}
			copy.NodeType = plan.Node_FILTER
			copy.WinSpecList = nil
			for _, expr := range outputs {
				_ = visitInputColumns(expr, func(col *plan.ColRef) error { col.RelPos = 0; return nil })
			}
		case plan.Node_AGG:
			groupWidth := int32(len(n.GroupBy))
			needed := make(map[int32]bool)
			for _, expr := range outputs {
				_ = visitInputColumns(expr, func(col *plan.ColRef) error {
					if col.ColPos >= groupWidth {
						needed[col.ColPos-groupWidth] = true
					}
					return nil
				})
			}
			remap := make(map[int32]int32)
			copy.AggList = nil
			for i, expr := range n.AggList {
				if needed[int32(i)] {
					remap[int32(i)+groupWidth] = int32(len(copy.AggList)) + groupWidth
					copy.AggList = append(copy.AggList, proto.Clone(expr).(*plan.Expr))
				}
			}
			for _, expr := range outputs {
				_ = visitInputColumns(expr, func(col *plan.ColRef) error {
					if pos, ok := remap[col.ColPos]; ok {
						col.ColPos = pos
					}
					return nil
				})
			}
			// Group keys determine cardinality; only retained aggregate arguments
			// consume input values. HAVING and projected aggregate columns above are
			// in the aggregate-output namespace, not the input namespace.
			for _, expr := range copy.GroupBy {
				collect(expr)
			}
			for _, expr := range copy.AggList {
				collect(expr)
			}
			outputs = nil
		}
		for _, expr := range outputs {
			collect(expr)
		}
		result[d.node] = copy
		for _, child := range n.Children {
			pending = append(pending, demand{node: child, columns: childRequired})
		}
	}
	return result
}

// A reducer comparison has its own lhs/rhs tuple namespace. Only the call's
// captures and lhs expressions read this relational input.
func visitInputColumns(expr *plan.Expr, visit func(*plan.ColRef) error) error {
	pending := []*plan.Expr{expr}
	for len(pending) > 0 {
		e := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if e == nil {
			continue
		}
		switch v := e.Expr.(type) {
		case *plan.Expr_Col:
			if err := visit(v.Col); err != nil {
				return err
			}
		case *plan.Expr_F:
			pending = append(pending, v.F.Args...)
		case *plan.Expr_List:
			pending = append(pending, v.List.List...)
		case *plan.Expr_Dependent:
			pending = append(pending, v.Dependent.Inputs...)
			pending = append(pending, v.Dependent.Lhs...)
		}
	}
	return nil
}
