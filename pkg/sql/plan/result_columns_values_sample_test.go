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

func TestSampleForwardedBooleanResultKinds(t *testing.T) {
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	stored := pb.ResultColumnKind_RESULT_COLUMN_KIND_STORED
	computed := pb.ResultColumnKind_RESULT_COLUMN_KIND_COMPUTED
	input := "(select role_id,with_grant_option b,role_id>0 p from mo_catalog.mo_role_privs union all select role_id,with_grant_option b,role_id>0 p from mo_catalog.mo_role_privs) d"
	for _, tc := range []struct {
		projection string
		kinds      []pb.ResultColumnKind
	}{
		{"sample(role_id+1,100 percent),b,p", []pb.ResultColumnKind{computed, stored, computed}},
		{"sample(role_id+1,100 percent),p,b", []pb.ResultColumnKind{computed, computed, stored}},
		{"p,b,sample(role_id+1,100 percent)", []pb.ResultColumnKind{computed, stored, computed}},
		{"b,p,sample(role_id+1,100 percent)", []pb.ResultColumnKind{stored, computed, computed}},
		{"sample(role_id+1,100 percent),p,b,p,b", []pb.ResultColumnKind{computed, computed, stored, computed, stored}},
		{"sample(role_id+1,role_id+2,100 percent),b,p", []pb.ResultColumnKind{computed, computed, stored, computed}},
	} {
		t.Run(tc.projection, func(t *testing.T) {
			p, err := runOneStmt(mock, t, "select "+tc.projection+" from "+input)
			require.NoError(t, err)
			var forwarded int
			for _, node := range p.GetQuery().Nodes {
				if node.NodeType != pb.Node_SAMPLE {
					continue
				}
				offset := int32(len(node.GroupBy) + len(node.AggList))
				for _, expr := range node.ProjectList {
					if ref := expr.GetCol(); ref != nil && ref.RelPos == 0 {
						require.GreaterOrEqual(t, ref.ColPos, offset)
						forwarded++
					}
				}
			}
			require.Positive(t, forwarded, "regression must exercise forwarded SAMPLE slots")
			columns := GetResultColumnsFromPlan(p)
			require.Len(t, columns, len(tc.kinds))
			for i, column := range columns {
				if column.Typ.Id == int32(types.T_bool) {
					require.Equal(t, tc.kinds[i], column.ResultColumnKind, "column %d", i)
				}
			}
		})
	}
}

func TestSampleForwardedReferenceOffsets(t *testing.T) {
	col := func(pos int32) *pb.Expr {
		return &pb.Expr{Expr: &pb.Expr_Col{Col: &pb.ColRef{ColPos: pos}}}
	}
	query := &pb.Query{Nodes: []*pb.Node{
		{NodeType: pb.Node_TABLE_SCAN, ProjectList: []*pb.Expr{col(0)}},
		{NodeType: pb.Node_PROJECT, Children: []int32{0}, ProjectList: []*pb.Expr{col(0), makePlan2BoolConstExprWithType(true)}},
		{NodeType: pb.Node_SAMPLE, Children: []int32{1},
			GroupBy: []*pb.Expr{col(0), col(1)}, AggList: []*pb.Expr{col(0), col(1)}},
	}}
	for _, tc := range []struct {
		position int32
		computed bool
	}{
		{-1, false}, {0, false}, {1, false}, {3, false},
		{4, false}, {5, true}, {6, false},
	} {
		kinds := newResultColumnKinds(query)
		require.Equal(t, tc.computed, kinds.computed(2, col(tc.position)), "forwarded slot %d", tc.position)
	}
	query.Nodes[2].Children = nil
	require.False(t, newResultColumnKinds(query).computed(2, col(5)))
}
