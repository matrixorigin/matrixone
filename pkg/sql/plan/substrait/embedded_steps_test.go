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

package substrait

import (
	"os"
	"testing"

	gogoproto "github.com/gogo/protobuf/proto"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestEmbeddedSharedStepsHaveIndependentReadOccurrences(t *testing.T) {
	sql, err := os.ReadFile("../tpch/q15.sql")
	require.NoError(t, err)
	query := boundSQLQuery(t, string(sql))
	before := gogoproto.Clone(query)
	profile := NewEmbeddedExportProfile(31)
	original, err := ExportEmbeddedMO(query, profile)
	require.NoError(t, err)
	originalReads, err := original.EmbeddedMOReads()
	require.NoError(t, err)
	require.Len(t, originalReads, 2)
	expanded, err := ExpandEmbeddedMOSteps(query)
	require.NoError(t, err)
	require.Len(t, expanded.Steps, 1)
	require.True(t, gogoproto.Equal(before, query), "the public/native MO plan remains caller-owned")
	candidate, err := ExportEmbeddedMO(expanded, profile)
	require.NoError(t, err)
	reads, err := candidate.EmbeddedMOReads()
	require.NoError(t, err)
	require.Len(t, reads, 3, "supplier plus two independently consumed lineitem scans")
	bindings := make(map[int32]EmbeddedReadBinding)
	lineitem := 0
	for i, read := range reads {
		_, duplicate := bindings[read.NodeID]
		require.False(t, duplicate)
		bindings[read.NodeID] = EmbeddedReadBinding{BindingID: uint64(i + 1), Source: EmbeddedReadMO}
		if read.Table == "lineitem" {
			lineitem++
			require.Equal(t, originalReads[0].Columns, read.Columns)
		}
	}
	require.Equal(t, 2, lineitem)
	wire, err := candidate.BuildEmbedded(bindings)
	require.NoError(t, err)
	plan := embeddedPlan(t, wire)
	require.Len(t, plan.Relations, 1)
	require.NotNil(t, plan.Relations[0].GetRoot())
}

func sharedStepsQuery(project bool) *planpb.Query {
	query := scanQuery()
	var projection []*planpb.Expr
	if project {
		projection = []*planpb.Expr{col(0)}
	}
	query.Nodes = append(query.Nodes,
		&planpb.Node{NodeId: 1, NodeType: planpb.Node_SINK, Children: []int32{0}, ProjectList: projection},
		&planpb.Node{NodeId: 2, NodeType: planpb.Node_SINK_SCAN, SourceStep: []int32{0}, ProjectList: projection},
		&planpb.Node{NodeId: 3, NodeType: planpb.Node_SINK_SCAN, SourceStep: []int32{0}, ProjectList: projection},
		&planpb.Node{NodeId: 4, NodeType: planpb.Node_JOIN, Children: []int32{2, 3}, JoinType: planpb.Node_INNER},
	)
	query.Steps, query.Headings = []int32{1, 4}, []string{"left", "right"}
	return query
}

func TestEmbeddedSharedStepIdentityAndInvalidGraphs(t *testing.T) {
	for _, project := range []bool{true, false} {
		query := sharedStepsQuery(project)
		expanded, err := ExpandEmbeddedMOSteps(query)
		require.NoError(t, err)
		candidate, err := ExportEmbeddedMO(expanded, NewEmbeddedExportProfile(31))
		require.NoError(t, err)
		reads, err := candidate.EmbeddedMOReads()
		require.NoError(t, err)
		require.Len(t, reads, 2)
		require.NotEqual(t, reads[0].NodeID, reads[1].NodeID)
	}
	for _, mutate := range []func(*planpb.Query){
		func(q *planpb.Query) { q.Nodes[1].Children = []int32{1} },
		func(q *planpb.Query) { q.Nodes[2].SourceStep = []int32{1} },
		func(q *planpb.Query) { q.Nodes[1].NodeType = planpb.Node_PROJECT },
		func(q *planpb.Query) { q.Nodes[2].NodeId = 9 },
		func(q *planpb.Query) { q.Nodes[4].Children[0] = -1 },
	} {
		query := sharedStepsQuery(true)
		mutate(query)
		_, err := ExpandEmbeddedMOSteps(query)
		require.Error(t, err)
	}
	_, err := ExpandEmbeddedMOSteps(nil)
	require.Error(t, err)
	query := sharedStepsQuery(true)
	for len(query.Steps) < 6 {
		step := int32(len(query.Steps) - 1)
		producer := int32(len(query.Nodes))
		query.Nodes = append(query.Nodes,
			&planpb.Node{NodeId: producer, NodeType: planpb.Node_SINK, Children: []int32{query.Steps[step]}},
			&planpb.Node{NodeId: producer + 1, NodeType: planpb.Node_SINK_SCAN, SourceStep: []int32{step}},
			&planpb.Node{NodeId: producer + 2, NodeType: planpb.Node_SINK_SCAN, SourceStep: []int32{step}},
			&planpb.Node{NodeId: producer + 3, NodeType: planpb.Node_JOIN, Children: []int32{producer + 1, producer + 2}},
		)
		query.Steps[step] = producer
		query.Steps = append(query.Steps, producer+3)
	}
	_, err = ExpandEmbeddedMOSteps(query)
	require.ErrorContains(t, err, "sixteen read bindings")
}
