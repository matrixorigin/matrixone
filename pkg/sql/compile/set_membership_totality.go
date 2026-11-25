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
	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
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
	messageTags, filterTags := make(map[int32]bool), make(map[int32]bool)
	var visit func(int32) bool
	visit = func(id int32) bool {
		if id < 0 || int(id) >= len(nodes) || nodes[id] == nil || owners[id] != 1 || seen[id] {
			return false
		}
		seen[id] = true
		n := nodes[id]
		if n.NodeType == plan.Node_AGG {
			if len(n.Children) != 1 || !visit(n.Children[0]) {
				return false
			}
			return setMembershipDistinctInputIsTotal(n, nodes[n.Children[0]].ProjectList)
		}
		if len(n.OrderBy) != 0 || !setMembershipPhysicalInputIsTotal(n, messageTags, filterTags) {
			return false
		}
		var inputs [][]*plan.Expr
		switch n.NodeType {
		case plan.Node_TABLE_SCAN:
			if len(n.Children) != 0 || n.TableDef == nil {
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
		case plan.Node_PROJECT, plan.Node_FILTER, plan.Node_JOIN, plan.Node_INTERSECT, plan.Node_MINUS:
			want := 1
			if n.NodeType == plan.Node_JOIN || n.NodeType == plan.Node_INTERSECT || n.NodeType == plan.Node_MINUS {
				want = 2
				if n.NodeType == plan.Node_JOIN && n.JoinType != plan.Node_INNER {
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
				if (n.NodeType == plan.Node_INTERSECT || n.NodeType == plan.Node_MINUS) &&
					!setMembershipOutputTypesMatch(n.ProjectList, nodes[child].ProjectList) {
					return false
				}
				inputs = append(inputs, nodes[child].ProjectList)
			}
		default:
			return false
		}
		return setMembershipExpressionsMatchInputs(n.ProjectList, inputs) &&
			setMembershipExpressionsMatchInputs(n.FilterList, inputs) &&
			setMembershipExpressionsMatchInputs(n.OnList, inputs) &&
			// Some block predicates address metadata-only columns. Only accept
			// those whose actual compact-row origin can be proven here.
			setMembershipExpressionsMatchInputs(n.BlockFilterList, inputs)
	}
	return visit(node.Children[0]) && visit(node.Children[1])
}

// Inputs have already been compiled. Their private INNER-join wiring remains
// inside those unchanged scopes; it is not a dependency on another set owner.
// Do not generalize this exemption to receivers or executable RF attachments.
func setMembershipPhysicalInputIsTotal(node *plan.Node, messages, filters map[int32]bool) bool {
	proof := *node
	if node.NodeType == plan.Node_JOIN && node.JoinType == plan.Node_INNER {
		for _, header := range node.SendMsgList {
			if header.MsgType != int32(message.MsgJoinMap) || header.MsgTag <= 0 || messages[header.MsgTag] {
				return false
			}
			messages[header.MsgTag] = true
		}
		if len(node.RuntimeFilterBuildList) != len(node.RuntimeFilterProbeList) {
			return false
		}
		paired := make(map[int32]bool)
		for _, spec := range node.RuntimeFilterBuildList {
			if spec == nil || spec.Tag <= 0 || filters[spec.Tag] || paired[spec.Tag] ||
				!proto.Equal(spec, &plan.RuntimeFilterSpec{Tag: spec.Tag}) {
				return false
			}
			paired[spec.Tag], filters[spec.Tag] = true, true
		}
		for _, spec := range node.RuntimeFilterProbeList {
			if spec == nil || !paired[spec.Tag] || !proto.Equal(spec, &plan.RuntimeFilterSpec{Tag: spec.Tag}) {
				return false
			}
			delete(paired, spec.Tag)
		}
		proof.SendMsgList = nil
		proof.RuntimeFilterBuildList, proof.RuntimeFilterProbeList = nil, nil
	}
	if node.NodeType == plan.Node_INTERSECT || node.NodeType == plan.Node_MINUS {
		keys := &plan.Node{ProjectList: node.PhysicalEqualityKeyList}
		if !plan2.SetMembershipNodeEvaluationIsTotal(keys) ||
			!setMembershipExpressionsMatchInputs(keys.ProjectList, [][]*plan.Expr{node.ProjectList}) {
			return false
		}
		proof.PhysicalEqualityKeyList = nil
	}
	return plan2.SetMembershipNodeEvaluationIsTotal(&proof)
}

func setMembershipDistinctInputIsTotal(node *plan.Node, input []*plan.Expr) bool {
	if len(node.GroupBy) == 0 || len(node.AggList) != 0 || len(node.GroupByHashKey) != 0 ||
		len(node.PhysicalEqualityKeyList) != 0 || len(node.FilterList) != 0 ||
		len(node.BlockFilterList) != 0 || len(node.OrderBy) != 0 ||
		(len(node.GroupingFlag) != 0 && len(node.GroupingFlag) != len(node.GroupBy)) {
		return false
	}
	for _, active := range node.GroupingFlag {
		if !active {
			return false
		}
	}
	for _, key := range node.GroupBy {
		if key.GetCol() == nil {
			return false
		}
	}
	if !setMembershipExpressionsMatchInputs(node.GroupBy, [][]*plan.Expr{input}) {
		return false
	}
	// Group outputs use -1, not the child stream's 0. Check those outputs
	// separately instead of treating a grouped slot as an arbitrary scan field.
	proof := *node
	proof.GroupBy, proof.GroupingFlag, proof.GroupByHashKey = nil, nil, nil
	proof.ProjectList = plan2.DeepCopyExprList(node.ProjectList)
	for _, expr := range proof.ProjectList {
		if err := plan.VisitExprTree(expr, func(e *plan.Expr) error {
			if col := e.GetCol(); col != nil {
				if col.RelPos != -1 {
					return moerr.NewInternalError(moerr.NoReportContext(), "unproved grouped output")
				}
				col.RelPos = 0
			}
			return nil
		}); err != nil {
			return false
		}
	}
	return plan2.SetMembershipNodeEvaluationIsTotal(&proof) &&
		setMembershipExpressionsMatchInputs(proof.ProjectList, [][]*plan.Expr{node.GroupBy})
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
