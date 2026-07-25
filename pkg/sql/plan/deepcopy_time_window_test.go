// Copyright 2021 - 2022 Matrix Origin
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

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func twTestColExpr(relPos, colPos int32) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{Id: 1},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{RelPos: relPos, ColPos: colPos},
		},
	}
}

// DeepCopyNode historically skipped every TIME_WINDOW/FILL field, so copying a
// subtree silently dropped the window definition and partition metadata. This
// pins the round-trip: all fields survive, and mutating the copy cannot reach
// the original.
func TestDeepCopyNodeTimeWindowFields(t *testing.T) {
	node := &plan.Node{
		NodeType:              plan.Node_TIME_WINDOW,
		Interval:              twTestColExpr(1, 0),
		Sliding:               twTestColExpr(1, 1),
		Timestamp:             twTestColExpr(1, 2),
		WEnd:                  twTestColExpr(1, 3),
		FillType:              plan.Node_PREV,
		FillVal:               []*plan.Expr{twTestColExpr(2, 0)},
		TimeWindowPartitionBy: []*plan.Expr{twTestColExpr(3, 0), twTestColExpr(3, 1)},
	}

	copied := DeepCopyNode(node)

	require.Equal(t, node.Interval, copied.Interval)
	require.Equal(t, node.Sliding, copied.Sliding)
	require.Equal(t, node.Timestamp, copied.Timestamp)
	require.Equal(t, node.WEnd, copied.WEnd)
	require.Equal(t, node.FillType, copied.FillType)
	require.Equal(t, node.FillVal, copied.FillVal)
	require.Equal(t, node.TimeWindowPartitionBy, copied.TimeWindowPartitionBy)

	// Deep, not shallow: the copy must not alias the original's expressions.
	copied.TimeWindowPartitionBy[0].GetCol().ColPos = 99
	copied.WEnd.GetCol().ColPos = 99
	copied.FillVal[0].GetCol().ColPos = 99
	require.Equal(t, int32(0), node.TimeWindowPartitionBy[0].GetCol().ColPos)
	require.Equal(t, int32(3), node.WEnd.GetCol().ColPos)
	require.Equal(t, int32(0), node.FillVal[0].GetCol().ColPos)
}

// twMarkVisitRule flags every expression it is offered, so a test can prove a
// node field is (or is not) reachable from the plan visitor.
type twMarkVisitRule struct {
	seen map[*plan.Expr]bool
}

func (r *twMarkVisitRule) MatchNode(*Node) bool  { return false }
func (r *twMarkVisitRule) IsApplyExpr() bool     { return true }
func (r *twMarkVisitRule) ApplyNode(*Node) error { return nil }
func (r *twMarkVisitRule) ApplyExpr(e *Expr) (*Expr, error) {
	r.seen[e] = true
	return e, nil
}

// The plan visitor rewrites expressions in place (parameter binding, constant
// folding); a partition key it cannot reach would keep stale references.
func TestVisitPlanReachesTimeWindowPartitionBy(t *testing.T) {
	partExpr := twTestColExpr(3, 0)
	node := &plan.Node{
		NodeType:              plan.Node_TIME_WINDOW,
		TimeWindowPartitionBy: []*plan.Expr{partExpr},
	}
	pl := &Plan{Plan: &plan.Plan_Query{Query: &Query{
		Steps: []int32{0},
		Nodes: []*plan.Node{node},
	}}}

	rule := &twMarkVisitRule{seen: make(map[*plan.Expr]bool)}
	require.NoError(t, NewVisitPlan(pl, []VisitPlanRule{rule}).Visit(t.Context()))
	require.True(t, rule.seen[partExpr], "partition keys must be visited")
}

// replaceColumnsForNode rewrites column references when a projection is
// inlined; partition keys decide row grouping, so missing them would leave
// stale references behind.
func TestReplaceColumnsForNodeTimeWindowPartitionBy(t *testing.T) {
	node := &plan.Node{
		NodeType:              plan.Node_TIME_WINDOW,
		TimeWindowPartitionBy: []*plan.Expr{twTestColExpr(5, 2)},
	}
	projMap := map[[2]int32]*plan.Expr{
		{5, 2}: twTestColExpr(7, 4),
	}

	replaceColumnsForNode(node, projMap)

	col := node.TimeWindowPartitionBy[0].GetCol()
	require.Equal(t, int32(7), col.RelPos)
	require.Equal(t, int32(4), col.ColPos)
}
