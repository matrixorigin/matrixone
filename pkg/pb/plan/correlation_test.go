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
	"errors"
	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
	"testing"
)

func correlationTestQuery() *Query {
	typ := Type{Id: 23}
	slot := func(p uint32) *Expr { return &Expr{Typ: typ, Expr: &Expr_Slot{Slot: &CorrelationSlot{ProgramId: p}}} }
	call := func(p, stage uint32, input *Expr) *Expr {
		return &Expr{Typ: typ, Expr: &Expr_Dependent{Dependent: &DependentExpr{
			ProgramId: p, OccurrenceId: stage, StageId: stage, Inputs: []*Expr{input},
		}}}
	}
	return &Query{
		Steps: []int32{0, 1, 2},
		Nodes: []*Node{
			{NodeId: 0, NodeType: Node_PROJECT, ProjectList: []*Expr{call(1, 1, &Expr{Typ: typ, Expr: &Expr_Col{Col: &ColRef{}}})}},
			{NodeId: 1, NodeType: Node_PROJECT, ProjectList: []*Expr{call(2, 2, slot(1))}},
			{NodeId: 2, NodeType: Node_PROJECT, ProjectList: []*Expr{slot(2)}},
		},
		Correlation: &CorrelationSpec{
			Version: CorrelationVersion, MainOutputStep: 0, StepOwners: []uint32{0, 1, 2},
			Programs: []*CorrelationProgram{
				{RootNode: 1, InputTypes: []Type{typ}, OutputTypes: []Type{typ}},
				{RootNode: 2, InputTypes: []Type{typ}, OutputTypes: []Type{typ}},
			},
			Stages: []*CorrelationStage{{OwnerNode: 0}, {OwnerNode: 1}},
		},
	}
}

func TestCorrelationPlanRoundTrip(t *testing.T) {
	q := correlationTestQuery()
	require.NoError(t, q.ValidateCorrelation())
	data, err := q.Marshal()
	require.NoError(t, err)
	decoded := new(Query)
	require.NoError(t, decoded.Unmarshal(data))
	require.NoError(t, decoded.ValidateCorrelation())
	require.True(t, proto.Equal(q, decoded))
	require.Equal(t, int32(0), decoded.MainOutputStep())
	require.Equal(t, int32(1), (&Query{Steps: []int32{0, 1}}).MainOutputStep())
	require.Equal(t, int32(-1), (*Query)(nil).MainOutputStep())
	var nodes []int32
	require.NoError(t, WalkQueryNodes(q, func(id int32, _ *Node) error { nodes = append(nodes, id); return nil }))
	require.ElementsMatch(t, []int32{0, 1, 2}, nodes)
}

func TestCorrelationPlanRejectsInvalidOwnership(t *testing.T) {
	cases := []struct {
		name   string
		change func(*Query)
		want   string
	}{
		{"child_cycle", func(q *Query) { q.Nodes[2].Children = []int32{2} }, "child cycle"},
		{"version", func(q *Query) { q.Correlation.Version++ }, "version"},
		{"owner_count", func(q *Query) { q.Correlation.StepOwners = nil }, "step owner count"},
		{"main_is_child", func(q *Query) { q.Correlation.MainOutputStep = 1 }, "main output"},
		{"bad_owner", func(q *Query) { q.Correlation.StepOwners[1] = 100 }, "region"},
		{"missing_program", func(q *Query) { q.Correlation.Programs[0] = nil }, "no output"},
		{"invalid_root", func(q *Query) { q.Correlation.Programs[0].RootNode = -1 }, "node -1"},
		{"cross_owner_child", func(q *Query) { q.Nodes[0].Children = []int32{1} }, "two regions"},
		{"cross_owner_source", func(q *Query) { q.Nodes[1].SourceStep = []int32{0} }, "cross-region source"},
		{"slot_owner", func(q *Query) { q.Nodes[2].ProjectList[0].GetSlot().ProgramId = 1 }, "slot owner"},
		{"slot_ordinal", func(q *Query) { q.Nodes[2].ProjectList[0].GetSlot().Ordinal = 1 }, "slot type or ordinal"},
		{"slot_type", func(q *Query) { q.Nodes[2].ProjectList[0].Typ.Scale = 3 }, "slot type or ordinal"},
		{"stage_owner", func(q *Query) { q.Correlation.Stages[0].OwnerNode = 900 }, "stage 1 owner"},
		{"stage_kind", func(q *Query) { q.Correlation.Stages[0].Kind = 100 }, "stage 1 kind"},
		{"stage_missing", func(q *Query) { q.Nodes[0].ProjectList[0].GetDependent().StageId = 0 }, "stage"},
		{"occurrence_missing", func(q *Query) { q.Nodes[0].ProjectList[0].GetDependent().OccurrenceId = 0 }, "zero occurrence"},
		{"occurrence_collision", func(q *Query) { q.Nodes[1].ProjectList[0].GetDependent().OccurrenceId = 1 }, "conflicting occurrence"},
		{"callee_missing", func(q *Query) { q.Nodes[0].ProjectList[0].GetDependent().ProgramId = 0 }, "callee"},
		{"argument_count", func(q *Query) { q.Nodes[0].ProjectList[0].GetDependent().Inputs = nil }, "input count"},
		{"argument_type", func(q *Query) { q.Nodes[0].ProjectList[0].GetDependent().Inputs[0].Typ.Scale = 3 }, "input 0 type"},
		{"unknown_reducer", func(q *Query) { q.Nodes[0].ProjectList[0].GetDependent().Reducer = 100 }, "reducer"},
		{"unbound", func(q *Query) { q.Nodes[2].ProjectList[0].Expr = &Expr_Corr{Corr: &CorrColRef{Depth: 2}} }, "unbound"},
		{"no_descriptor", func(q *Query) { q.Correlation = nil }, "without descriptor"},
		{"cycle", func(q *Query) { q.Nodes[1].ProjectList[0].GetDependent().ProgramId = 1 }, "call cycle"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			q := correlationTestQuery()
			c.change(q)
			err := q.ValidateCorrelation()
			require.ErrorContains(t, err, c.want)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal))
		})
	}
}

func TestWalkQueryNodesRejectsInvalidGraph(t *testing.T) {
	for _, tc := range []struct {
		name  string
		query *Query
		want  string
	}{
		{"nil_program", &Query{Correlation: &CorrelationSpec{Programs: []*CorrelationProgram{nil}}}, "nil correlation program"},
		{"invalid_node", &Query{Steps: []int32{-1}}, "invalid query node -1"},
		{"invalid_source", &Query{Steps: []int32{0}, Nodes: []*Node{{SourceStep: []int32{1}}}}, "invalid source step 1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := WalkQueryNodes(tc.query, func(int32, *Node) error { return nil })
			require.ErrorContains(t, err, tc.want)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal))
		})
	}
}

func TestCorrelationQuantifiedComparison(t *testing.T) {
	for _, reducer := range []SubqueryRef_Type{SubqueryRef_IN, SubqueryRef_NOT_IN, SubqueryRef_ANY, SubqueryRef_ALL} {
		t.Run(reducer.String(), func(t *testing.T) {
			q := correlationTestQuery()
			e := q.Nodes[0].ProjectList[0]
			d := e.GetDependent()
			d.Reducer = reducer
			e.Typ = Type{Id: 10}
			d.Lhs = []*Expr{{Typ: Type{Id: 23}, Expr: &Expr_Lit{Lit: &Literal{Value: &Literal_I64Val{I64Val: 1}}}}}
			d.Comparison = &Expr{Typ: Type{Id: 10}, Expr: &Expr_F{F: &Function{Args: []*Expr{
				{Typ: Type{Id: 23}, Expr: &Expr_Col{Col: &ColRef{}}},
				{Typ: Type{Id: 23}, Expr: &Expr_Col{Col: &ColRef{RelPos: 1}}},
			}}}}
			require.NoError(t, q.ValidateCorrelation())
			d.Comparison.GetF().Args[1].GetCol().ColPos = 1
			require.ErrorContains(t, q.ValidateCorrelation(), "comparison rhs type")
		})
	}
}

func TestCorrelationExpressionWalk(t *testing.T) {
	q := correlationTestQuery()
	e := q.Nodes[0].ProjectList[0]
	d := e.GetDependent()
	d.Lhs = []*Expr{{AuxId: 2}}
	d.Comparison = &Expr{AuxId: 3}
	d.Inputs[0].AuxId = 1
	var visited []int32
	require.NoError(t, VisitExprTree(e, func(e *Expr) error { visited = append(visited, e.AuxId); return nil }))
	require.Equal(t, []int32{0, 1, 2, 3}, visited)
	stop := errors.New("stop")
	require.ErrorIs(t, VisitExprTree(e, func(e *Expr) error {
		if e.AuxId == 1 {
			return stop
		}
		return nil
	}), stop)
	// Depth must consume explicit traversal state, not one Go frame per level.
	for i := 0; i < 10000; i++ {
		e = &Expr{Expr: &Expr_F{F: &Function{Args: []*Expr{e}}}}
	}
	count := 0
	require.NoError(t, VisitExprTree(e, func(*Expr) error { count++; return nil }))
	require.Equal(t, 10004, count)
}
