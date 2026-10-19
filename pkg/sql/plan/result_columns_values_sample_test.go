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

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestResultColumnValueScanProducers(t *testing.T) {
	col := func(pos int32) *pb.Expr {
		return &pb.Expr{Typ: pb.Type{Id: int32(types.T_bool)}, Expr: &pb.Expr_Col{Col: &pb.ColRef{ColPos: pos}}}
	}
	node := &pb.Node{NodeType: pb.Node_VALUE_SCAN, RowsetData: &pb.RowsetData{
		Cols: []*pb.ColData{
			nil,
			{Data: []*pb.RowsetExpr{nil, {Expr: col(0)}, {Expr: makePlan2BoolConstExprWithType(true)}}},
			{Data: []*pb.RowsetExpr{nil, {}}},
			{Data: []*pb.RowsetExpr{{Expr: col(3)}}},
		},
	}}
	kinds := newResultColumnKinds(&pb.Query{Nodes: []*pb.Node{node}})
	for _, tc := range []struct {
		position int32
		computed bool
	}{{-1, false}, {0, false}, {1, true}, {2, false}, {3, false}, {4, false}} {
		require.Equal(t, tc.computed, kinds.computed(0, col(tc.position)), "column %d", tc.position)
	}
	node.RowsetData = nil
	require.False(t, newResultColumnKinds(kinds.query).computed(0, col(0)))
}

func TestSampleProjectedGroupsKeepProducerOrdinals(t *testing.T) {
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	for _, tc := range []struct {
		name, selectList, groupList string
		groupSlots                  []int32
	}{
		{"omitted stored prefix", "sample(role_id,1 rows),role_id>0", "with_grant_option,role_id>0", []int32{1}},
		{"omitted computed prefix", "sample(role_id,1 rows),with_grant_option", "role_id>0,with_grant_option", []int32{1}},
		{"omitted two keys", "sample(role_id,1 rows),role_id>0", "with_grant_option,role_id+1,role_id>0", []int32{2}},
		{"selected prefix control", "sample(role_id,1 rows),role_id>0", "role_id>0,with_grant_option", []int32{0}},
		{"both keys control", "role_id>0,sample(role_id,1 rows),with_grant_option", "with_grant_option,role_id>0", []int32{0, 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := runOneStmt(mock, t, "select "+tc.selectList+" from mo_catalog.mo_role_privs group by "+tc.groupList)
			require.NoError(t, err)
			var sample *pb.Node
			for _, node := range p.GetQuery().Nodes {
				if node.NodeType == pb.Node_SAMPLE {
					sample = node
				}
			}
			require.NotNil(t, sample)
			var slots []int32
			for _, expr := range sample.ProjectList {
				if ref := expr.GetCol(); ref != nil && ref.RelPos == -1 {
					slots = append(slots, ref.ColPos)
				}
			}
			// SAMPLE emits all group vectors before its sampled vectors. An
			// omitted output must not renumber the underlying group producer.
			require.Equal(t, tc.groupSlots, slots)
		})
	}
}

func TestResultColumnValuesStopAtComputedProducer(t *testing.T) {
	rows := make([]*pb.RowsetExpr, 10000)
	for i := range rows {
		rows[i] = &pb.RowsetExpr{Expr: makePlan2BoolConstExprWithType(i%2 == 0)}
	}
	kinds := newResultColumnKinds(&pb.Query{Nodes: []*pb.Node{{
		NodeType: pb.Node_VALUE_SCAN,
		RowsetData: &pb.RowsetData{Cols: []*pb.ColData{{
			Data: rows,
		}}},
	}}})
	column := &pb.Expr{Expr: &pb.Expr_Col{Col: &pb.ColRef{ColPos: 0}}}
	require.True(t, kinds.computed(0, column))
	require.Len(t, kinds.states, 1, "VALUES metadata must not retain every scalar row expression")
}
