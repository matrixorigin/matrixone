// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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

func TestTimeWindowBooleanResultKinds(t *testing.T) {
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	for _, tc := range []struct {
		name, projection, suffix string
		widths                   []int
	}{
		{"stored first", "with_grant_option,max(role_id>0)", "", []int{1, 8}},
		{"aggregate first", "max(role_id>0),with_grant_option", "", []int{8, 1}},
		{"repeated", "max(role_id>0),with_grant_option,max(role_id>0),with_grant_option", "", []int{8, 1, 8, 1}},
		{"boundaries", "_wend,with_grant_option,max(role_id>0),_wstart,_wend", "", []int{0, 1, 8, 0, 0}},
		{"aggregate only", "max(role_id>0)", "", []int{8}},
		{"partition and boundary", "with_grant_option,_wstart", "", []int{1, 0}},
		{"fill null", "with_grant_option,max(role_id>0)", " fill(null)", []int{1, 8}},
		{"fill value", "max(role_id>0),with_grant_option,_wstart", " fill(value,0)", []int{8, 1, 0}},
		{"fill prev", "with_grant_option,_wend,max(role_id>0)", " fill(prev)", []int{1, 0, 8}},
		{"fill next", "max(role_id>0),with_grant_option", " fill(next)", []int{8, 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := runOneStmt(mock, t, "select "+tc.projection+
				" from mo_catalog.mo_role_privs group by with_grant_option interval(granted_time,1,minute)"+tc.suffix)
			require.NoError(t, err)
			var window *pb.Node
			for _, node := range p.GetQuery().Nodes {
				if node.NodeType == pb.Node_TIME_WINDOW {
					window = node
				}
			}
			require.NotNil(t, window, "must exercise TIME_WINDOW, not an ordinary aggregate")
			require.NotEmpty(t, window.TimeWindowPartitionBy)
			columns := GetResultColumnsFromPlan(p)
			require.Len(t, columns, len(tc.widths))
			for i, width := range tc.widths {
				if width == 0 {
					require.NotEqual(t, int32(types.T_bool), columns[i].Typ.Id)
					continue
				}
				require.Equal(t, int32(types.T_bool), columns[i].Typ.Id)
				want := pb.ResultColumnKind_RESULT_COLUMN_KIND_STORED
				if width == 8 {
					want = pb.ResultColumnKind_RESULT_COLUMN_KIND_COMPUTED
				}
				require.Equal(t, want, columns[i].ResultColumnKind, "column %d", i)
			}
		})
	}
}

func TestTimeWindowProducerLayout(t *testing.T) {
	col := func(pos int32, name string) *pb.Expr {
		return &pb.Expr{Expr: &pb.Expr_Col{Col: &pb.ColRef{RelPos: -1, ColPos: pos, Name: name}}}
	}
	agg := func() *pb.Expr { return &pb.Expr{Expr: &pb.Expr_F{F: &pb.Function{}}} }
	query := &pb.Query{Nodes: []*pb.Node{
		{NodeType: pb.Node_TABLE_SCAN, ProjectList: []*pb.Expr{col(0, "stored")}},
		{NodeType: pb.Node_PROJECT, Children: []int32{0}, ProjectList: []*pb.Expr{col(0, "stored"), makePlan2BoolConstExprWithType(true)}},
		{NodeType: pb.Node_TIME_WINDOW, Children: []int32{1},
			AggList:               []*pb.Expr{col(0, TimeWindowEnd), agg(), col(0, TimeWindowStart), agg(), col(0, TimeWindowStart), col(0, "not a boundary")},
			TimeWindowPartitionBy: []*pb.Expr{col(1, "computed"), col(0, "stored")}},
	}}
	kinds := newResultColumnKinds(query)
	for _, tc := range []struct {
		position int32
		computed bool
	}{
		{-1, false}, {0, true}, {1, true}, // real aggregates
		{2, true}, {3, true}, // shared start/end slots, not AggList indices
		{4, true}, {5, false}, // partition producers, not child ordinals 4/5
		{6, false},
	} {
		require.Equal(t, tc.computed, kinds.computed(2, col(tc.position, "")), "slot %d", tc.position)
	}
	query.Nodes[2].Children = nil
	require.False(t, newResultColumnKinds(query).computed(2, col(4, "")))
	require.True(t, newResultColumnKinds(query).computed(2, col(0, "")), "aggregate owns its output even without a child")
}
