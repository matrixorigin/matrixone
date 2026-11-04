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
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestResultColumnCTEProducerSlotsAndCycles(t *testing.T) {
	col := func(pos int32) *pb.Expr {
		return &pb.Expr{Typ: pb.Type{Id: int32(types.T_bool)}, Expr: &pb.Expr_Col{Col: &pb.ColRef{ColPos: pos}}}
	}
	for _, kind := range []pb.Node_NodeType{pb.Node_SINK_SCAN, pb.Node_RECURSIVE_CTE, pb.Node_RECURSIVE_SCAN} {
		t.Run(kind.String(), func(t *testing.T) {
			q := &pb.Query{Steps: []int32{1, 2, -1, 10, 3}, Nodes: []*pb.Node{
				{NodeType: kind, ProjectList: []*pb.Expr{col(0), col(1)}, SourceStep: []int32{0}},
				{NodeType: pb.Node_SINK, ProjectList: []*pb.Expr{col(0), col(1)}, Children: []int32{0}},
				{NodeType: pb.Node_SINK, ProjectList: []*pb.Expr{col(0), makePlan2BoolConstExprWithType(true)}},
				nil,
			}}
			// A pure recursive back edge is not proof of a computed result.
			require.False(t, newResultColumnKinds(q).computed(0, col(1)))
			// Invalid/unresolved producers fail closed. A later valid producer
			// must still be inspected after visiting the recursive cycle.
			q.Nodes[0].SourceStep = []int32{-1, 99, 2, 3, 4, 0, 1}
			require.False(t, newResultColumnKinds(q).computed(0, col(0)))
			require.True(t, newResultColumnKinds(q).computed(0, col(1)))
			require.False(t, newResultColumnKinds(q).computed(0, col(5)))
		})
	}
}

func TestResultColumnKindsSharedGraphIsBounded(t *testing.T) {
	for _, computed := range []bool{false, true} {
		col := func() *pb.Expr {
			return &pb.Expr{Typ: pb.Type{Id: int32(types.T_bool)}, Expr: &pb.Expr_Col{Col: &pb.ColRef{ColPos: 0}}}
		}
		leaf := col()
		if computed {
			leaf = makePlan2BoolConstExprWithType(true)
		}
		q := &pb.Query{Nodes: []*pb.Node{{NodeType: pb.Node_TABLE_SCAN, ProjectList: []*pb.Expr{leaf}}}}
		for depth := 0; depth < 16; depth++ {
			child := int32(len(q.Nodes) - 1)
			q.Nodes = append(q.Nodes, &pb.Node{NodeType: pb.Node_UNION_ALL, ProjectList: []*pb.Expr{col()}, Children: []int32{child, child}})
		}
		kinds := newResultColumnKinds(q)
		root := int32(len(q.Nodes) - 1)
		expr := q.Nodes[root].ProjectList[0]
		require.Equal(t, computed, kinds.computed(root, expr))
		states := len(kinds.states)
		require.LessOrEqual(t, states, 2*len(q.Nodes))
		edges := 0
		for _, state := range kinds.states {
			edges += len(state.parents)
		}
		require.LessOrEqual(t, edges, 4*len(q.Nodes))
		require.Equal(t, computed, kinds.computed(root, expr))
		require.Equal(t, states, len(kinds.states), "repeated outputs reuse the same graph")
	}
}

func nestedResultColumnCTEPlan(t testing.TB, expression string, depth int) *pb.Plan {
	t.Helper()
	ctes := []string{"c0 AS (SELECT DISTINCT " + expression + " n FROM mo_catalog.mo_role_privs)"}
	for level := 1; level <= depth; level++ {
		ctes = append(ctes, fmt.Sprintf("c%d AS (SELECT n FROM c%d UNION ALL SELECT n FROM c%d)", level, level-1, level-1))
	}
	p, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t,
		"WITH "+strings.Join(ctes, ",")+fmt.Sprintf(" SELECT n FROM c%d", depth))
	require.NoError(t, err)
	return p
}

func TestIntegerResultColumnsSkipBooleanGraph(t *testing.T) {
	// Use the actual planner at the review's twelve-level scale, not a small
	// synthetic DAG. Plan construction is outside the measured extraction.
	p := nestedResultColumnCTEPlan(t, "role_id", 12)
	require.GreaterOrEqual(t, len(p.GetQuery().Nodes), 32000)
	var columns []*pb.ColDef
	allocs := testing.AllocsPerRun(3, func() { columns = GetResultColumnsFromPlan(p) })
	require.Len(t, columns, 1)
	require.NotEqual(t, int32(types.T_bool), columns[0].Typ.Id)
	require.Equal(t, pb.ResultColumnKind_RESULT_COLUMN_KIND_UNKNOWN, columns[0].ResultColumnKind)
	// A non-BOOLEAN output must not allocate one provenance state per node.
	// This generous constant permits ordinary column/origin bookkeeping.
	require.LessOrEqual(t, allocs, 32.0)
	t.Logf("nodes=%d metadata allocs/op=%.0f", len(p.GetQuery().Nodes), allocs)
}

func BenchmarkResultColumnsSharedProducers(b *testing.B) {
	for _, expression := range []string{"role_id", "with_grant_option", "role_id>0"} {
		b.Run(expression, func(b *testing.B) {
			p := nestedResultColumnCTEPlan(b, expression, 12)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				GetResultColumnsFromPlan(p)
			}
			b.StopTimer()
			b.ReportMetric(float64(len(p.GetQuery().Nodes)), "plan-nodes")
		})
	}
}

func TestResultColumnProjectionIndexRetainsOwnerLookup(t *testing.T) {
	col := func(rel, pos int32, name, table, database string) *pb.Expr {
		return &pb.Expr{Expr: &pb.Expr_Col{Col: &pb.ColRef{RelPos: rel, ColPos: pos, Name: name, TblName: table, DbName: database}}}
	}
	node := &pb.Node{ProjectList: []*pb.Expr{
		col(2, 0, "flag", "a", "db"), col(3, 0, "flag", "b", "db"), col(4, 0, "flag", "", ""),
		col(5, 0, "Σ", "unicode", "db"), col(6, 0, "K", "unicode", "db"), col(7, 0, "", "table_only", "db"),
	}}
	kinds := newResultColumnKinds(&pb.Query{Nodes: []*pb.Node{node}})
	for _, ref := range []*pb.ColRef{
		{RelPos: 2, ColPos: 0, Name: "different"},
		{RelPos: 99, Name: "flag"}, {RelPos: 99, Name: "FLAG", TblName: "B", DbName: "DB"},
		{RelPos: 99, Name: "flag", TblName: "missing"}, {RelPos: 99, Name: "ς", TblName: "unicode"},
		{RelPos: 99, Name: "k", TblName: "unicode"}, {RelPos: 99, TblName: "table_only"},
		{RelPos: 99}, {RelPos: 99, Name: "absent"},
	} {
		require.Same(t, resultColumnProjectionByIdentity(node, ref), kinds.projection(0, ref), "%v", ref)
	}
}

func BenchmarkBooleanJoinClassification(b *testing.B) {
	for _, width := range []int{32, 128, 512} {
		b.Run(fmt.Sprint(width), func(b *testing.B) {
			projections := make([]string, width)
			outputs := make([]string, width)
			for i := range projections {
				projections[i] = fmt.Sprintf("role_id>%d p%d", i, i)
				outputs[i] = fmt.Sprintf("a.p%d", i)
			}
			sql := "SELECT " + strings.Join(outputs, ",") + ",b.flag FROM (SELECT DISTINCT " + strings.Join(projections, ",") + " FROM mo_catalog.mo_role_privs) a CROSS JOIN (SELECT DISTINCT with_grant_option flag FROM mo_catalog.mo_role_privs) b"
			p, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(b)), b, sql)
			require.NoError(b, err)
			query := p.GetQuery()
			joinID := int32(-1)
			for id, node := range query.Nodes {
				if node.NodeType == pb.Node_JOIN && len(node.ProjectList) >= width {
					joinID = int32(id)
					break
				}
			}
			require.NotEqual(b, int32(-1), joinID)
			expressions := query.Nodes[joinID].ProjectList
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				kinds := newResultColumnKinds(query)
				for _, expr := range expressions {
					if expr.Typ.Id == int32(types.T_bool) {
						kinds.computed(joinID, expr)
					}
				}
			}
		})
	}
}

func TestResultColumnReferenceIdentityRemainsBounded(t *testing.T) {
	q := &pb.Query{Nodes: []*pb.Node{
		{NodeType: pb.Node_JOIN, Children: []int32{1}},
		{NodeType: pb.Node_JOIN, BindingTags: []int32{42}, Children: []int32{0}},
	}}
	first := &pb.Expr{Expr: &pb.Expr_Col{Col: &pb.ColRef{RelPos: 42, ColPos: 0, Name: "flag"}}}
	second := DeepCopyExpr(first)
	require.NotSame(t, first.GetCol(), second.GetCol())
	k := newResultColumnKinds(q)
	require.False(t, k.computed(0, first))
	firstStates := len(k.states)
	require.False(t, k.computed(0, second))
	require.LessOrEqual(t, len(k.states), 2*firstStates)
	states := len(k.states)
	require.False(t, k.computed(0, first))
	require.False(t, k.computed(0, second))
	require.Len(t, k.states, states)
}
