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

	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestReturningBooleanResultKinds(t *testing.T) {
	stored := pb.ResultColumnKind_RESULT_COLUMN_KIND_STORED
	computed := pb.ResultColumnKind_RESULT_COLUMN_KIND_COMPUTED
	for _, tc := range []struct {
		sql   string
		kinds []pb.ResultColumnKind
	}{
		{"insert into returning_bool values(1,true,7) returning flag", []pb.ResultColumnKind{stored}},
		{"insert into returning_bool values(1,null,7) returning flag", []pb.ResultColumnKind{stored}},
		{"insert into returning_bool select 1,1>0,7 returning flag", []pb.ResultColumnKind{stored}},
		{"update returning_bool set flag=false returning flag", []pb.ResultColumnKind{stored}},
		{"update returning_bool set flag=n>0 returning flag", []pb.ResultColumnKind{stored}},
		{"update returning_bool set flag=flag returning flag", []pb.ResultColumnKind{stored}},
		{"delete from returning_bool returning flag", []pb.ResultColumnKind{stored}},
		{"update returning_bool set flag=false returning flag,n>0", []pb.ResultColumnKind{stored, computed}},
		{"insert into returning_bool values(1,true,7) returning n>0,flag,n>0,flag", []pb.ResultColumnKind{computed, stored, computed, stored}},
		{"update returning_bool set flag=false returning not flag,flag,true", []pb.ResultColumnKind{computed, stored, computed}},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			p, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, tc.sql)
			require.NoError(t, err)
			require.True(t, p.GetQuery().HasReturning)
			columns := GetResultColumnsFromPlan(p)
			require.Len(t, columns, len(tc.kinds))
			for i, kind := range tc.kinds {
				require.Equal(t, kind, columns[i].ResultColumnKind, "column %d", i)
			}
		})
	}
}

func TestReturningImageDoesNotHideOtherSinkProducers(t *testing.T) {
	col := func() *pb.Expr { return &pb.Expr{Expr: &pb.Expr_Col{Col: &pb.ColRef{ColPos: 0}}} }
	image := &pb.TableDef{Cols: []*pb.ColDef{{Name: "flag"}}}
	query := &pb.Query{
		HasReturning: true, ReturningStep: 1, Steps: []int32{1, 3, 5, 6, 7},
		Nodes: []*pb.Node{
			{NodeType: pb.Node_VALUE_SCAN, ProjectList: []*pb.Expr{col()}, RowsetData: &pb.RowsetData{
				Cols: []*pb.ColData{{Data: []*pb.RowsetExpr{{Expr: makePlan2BoolConstExprWithType(true)}}}},
			}},
			{NodeType: pb.Node_SINK, Children: []int32{0}, ProjectList: []*pb.Expr{col()}},
			{NodeType: pb.Node_SINK_SCAN, SourceStep: []int32{0}, TableDef: image, ProjectList: []*pb.Expr{col()}},
			{NodeType: pb.Node_PROJECT, Children: []int32{2}, ProjectList: []*pb.Expr{col()}},
			// A materialized expression can also carry a schema. It must not
			// become stored merely because this query has a RETURNING step.
			{NodeType: pb.Node_SINK_SCAN, SourceStep: []int32{0}, TableDef: image, ProjectList: []*pb.Expr{col()}},
			{NodeType: pb.Node_PROJECT, Children: []int32{4}, ProjectList: []*pb.Expr{col()}},
			{NodeType: pb.Node_PROJECT, Children: []int32{6}}, // malformed cycle
			{NodeType: pb.Node_JOIN, Children: []int32{2, 4}},
		},
	}
	kinds := newResultColumnKinds(query)
	require.False(t, kinds.computed(3, col()), "declared target row image owns the returned column")
	require.True(t, kinds.computed(5, col()), "ordinary SINK_SCAN must still follow its expression producer")
	require.True(t, kinds.computed(3, makePlan2BoolConstExprWithType(true)), "RETURNING expressions remain computed")

	for _, step := range []int32{-1, 99, 2, 3, 4} {
		query.ReturningStep = step
		require.True(t, newResultColumnKinds(query).computed(3, col()), "step %d must not mark an unrelated scan", step)
	}
	query.ReturningStep = 1
	query.Nodes[2].TableDef = nil
	require.True(t, newResultColumnKinds(query).computed(3, col()), "a schema-less scan is not a declared row image")
	query.Nodes[2].TableDef = image
	query.HasReturning = false
	require.True(t, newResultColumnKinds(query).computed(3, col()), "ordinary queries retain producer provenance")
	query.HasReturning = true
	for _, root := range []int32{-1, 99, 8} {
		query.Steps[1] = root
		query.Nodes = append(query.Nodes[:8], nil)
		require.True(t, newResultColumnKinds(query).computed(3, col()), "invalid root %d", root)
	}
	require.False(t, newResultColumnKinds(nil).computed(0, col()))
}
