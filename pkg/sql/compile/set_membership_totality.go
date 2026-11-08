// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
)

// setMembershipInputsAreTotal proves the remapped physical input graph. The
// planner's corresponding logical proof cannot be reused as-is: binding tags
// have already been removed and column positions refer to child outputs.
func setMembershipInputsAreTotal(query *plan.Query, nodeID int32) bool {
	if query == nil || query.Correlation != nil || nodeID < 0 || int(nodeID) >= len(query.Nodes) {
		return false
	}
	nodes := query.Nodes
	owners, seen := make(map[int32]int), make(map[int32]bool)
	var count func(int32) bool
	count = func(id int32) bool {
		if id < 0 || int(id) >= len(nodes) || nodes[id] == nil {
			return false
		}
		if seen[id] {
			return true
		}
		seen[id] = true
		for _, child := range nodes[id].Children {
			owners[child]++
			if !count(child) {
				return false
			}
		}
		return true
	}
	for _, root := range query.Steps {
		owners[root]++
		if !count(root) {
			return false
		}
	}
	node := nodes[nodeID]
	if !seen[nodeID] || owners[nodeID] != 1 || len(node.Children) != 2 {
		return false
	}
	for _, child := range node.Children {
		if !setMembershipOutputTypesMatch(node.ProjectList, nodes[child].ProjectList) {
			return false
		}
	}
	// Physical key expressions are evaluated on both complete inputs. Prove
	// their own evaluation as well as the identity subset used for routing.
	keys := &plan.Node{ProjectList: node.PhysicalEqualityKeyList}
	if !plan2.SetMembershipNodeEvaluationIsTotal(keys) ||
		!setMembershipExpressionsMatchInputs(keys.ProjectList, [][]*plan.Expr{node.ProjectList}) {
		return false
	}
	clear(seen)
	var visit func(int32) bool
	visit = func(id int32) bool {
		if id < 0 || int(id) >= len(nodes) || nodes[id] == nil || owners[id] != 1 || seen[id] {
			return false
		}
		seen[id] = true
		n := nodes[id]
		if len(n.OrderBy) != 0 || !plan2.SetMembershipNodeEvaluationIsTotal(n) {
			return false
		}
		var inputs [][]*plan.Expr
		switch n.NodeType {
		case plan.Node_TABLE_SCAN:
			// Block filters can contain metadata-only columns outside the compact
			// row schema; do not guess their physical positions.
			if len(n.Children) != 0 || n.TableDef == nil || len(n.BlockFilterList) != 0 {
				return false
			}
			columns := make([]*plan.Expr, len(n.TableDef.Cols))
			for i, col := range n.TableDef.Cols {
				if col == nil {
					return false
				}
				columns[i] = &plan.Expr{Typ: col.Typ}
			}
			inputs = [][]*plan.Expr{columns}
		case plan.Node_PROJECT, plan.Node_FILTER, plan.Node_JOIN:
			want := 1
			if n.NodeType == plan.Node_JOIN {
				want = 2
				if n.JoinType != plan.Node_INNER {
					return false
				}
			}
			if len(n.Children) != want {
				return false
			}
			for _, child := range n.Children {
				if !visit(child) {
					return false
				}
				inputs = append(inputs, nodes[child].ProjectList)
			}
		default:
			return false
		}
		return setMembershipExpressionsMatchInputs(n.ProjectList, inputs) &&
			setMembershipExpressionsMatchInputs(n.FilterList, inputs) &&
			setMembershipExpressionsMatchInputs(n.OnList, inputs)
	}
	return visit(node.Children[0]) && visit(node.Children[1])
}

func setMembershipOutputTypesMatch(output, input []*plan.Expr) bool {
	if len(output) == 0 || len(output) != len(input) {
		return false
	}
	for i := range output {
		if output[i] == nil || input[i] == nil {
			return false
		}
		out, err := types.TypeFromPlan(output[i].Typ)
		if err != nil {
			return false
		}
		in, err := types.TypeFromPlan(input[i].Typ)
		if err != nil || !out.Eq(in) {
			return false
		}
	}
	return true
}

func setMembershipExpressionsMatchInputs(exprs []*plan.Expr, inputs [][]*plan.Expr) bool {
	// Stop the visitor on rejection without reporting a normal plan fallback.
	invalid := func() error {
		return moerr.NewInternalError(moerr.NoReportContext(), "unproved set membership expression")
	}
	for _, expr := range exprs {
		if expr == nil {
			return false
		}
		err := plan.VisitExprTree(expr, func(e *plan.Expr) error {
			if col := e.GetCol(); col != nil {
				if col.RelPos < 0 || int(col.RelPos) >= len(inputs) || col.ColPos < 0 ||
					int(col.ColPos) >= len(inputs[col.RelPos]) || inputs[col.RelPos][col.ColPos] == nil {
					return invalid()
				}
				got, err := types.TypeFromPlan(e.Typ)
				if err != nil {
					return invalid()
				}
				want, err := types.TypeFromPlan(inputs[col.RelPos][col.ColPos].Typ)
				if err != nil || !got.Eq(want) {
					return invalid()
				}
			}
			if lit := e.GetLit(); lit != nil && !parallelUnionProjectionLiteralIsTotal(e.Typ, lit) {
				return invalid()
			}
			return nil
		})
		if err != nil {
			return false
		}
	}
	return true
}
