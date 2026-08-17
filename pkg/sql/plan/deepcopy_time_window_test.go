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
	"errors"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestDeepCopyPlanPreservesCompleteProtoSemantics(t *testing.T) {
	original := &plan.Plan{Plan: &plan.Plan_Ddl{Ddl: &plan.DataDefinition{
		DdlType: plan.DataDefinition_CREATE_TABLE,
		Query: &plan.Query{Steps: []int32{0}, LoadTag: true, LoadWriteS3: true, MaxDop: 8,
			Headings: []string{"h"}, BackgroundQueries: []*plan.Query{{Steps: []int32{1}}}, Nodes: []*plan.Node{{
				NodeType:          plan.Node_JOIN,
				ApplyType:         plan.Node_OUTERAPPLY,
				OnDuplicateAction: plan.Node_UPDATE,
				RecursiveSink:     true,
				RecursiveCte:      true,
				RollupFilter:      true,
				PartitionByCount:  2,
				ScanSnapshot: &plan.Snapshot{Tenant: &plan.SnapshotTenant{
					TenantName: "snapshot-tenant", TenantID: 42,
				}},
				SendMsgList: []plan.MsgHeader{{MsgTag: 17, MsgType: 3}},
			}}},
		Definition: &plan.DataDefinition_CreateTable{CreateTable: &plan.CreateTable{
			Database: "db", CreateAsSelectSql: "insert into dst select * from src",
		}},
	}}}

	copied := DeepCopyPlan(original)
	require.True(t, proto.Equal(original, copied))
	require.NotSame(t, original, copied)
	require.NotSame(t, original.GetDdl().Query.Nodes[0], copied.GetDdl().Query.Nodes[0])

	copied.GetDdl().Query.Nodes[0].ScanSnapshot.Tenant.TenantName = "changed"
	copied.GetDdl().GetCreateTable().CreateAsSelectSql = "changed"
	require.Equal(t, "snapshot-tenant", original.GetDdl().Query.Nodes[0].ScanSnapshot.Tenant.TenantName)
	require.Equal(t, "insert into dst select * from src", original.GetDdl().GetCreateTable().CreateAsSelectSql)
}

func TestDeepCopyQueryPlanPreservesCompleteProtoSemantics(t *testing.T) {
	original := &plan.Plan{TryRunTimes: 3, IsPrepare: true, Plan: &plan.Plan_Query{Query: &plan.Query{
		StmtType: plan.Query_SELECT, Steps: []int32{0}, Params: []*plan.Expr{twTestColExpr(9, 9)},
		Headings: []string{"h"}, LoadTag: true, LoadWriteS3: true, DetectSqls: []string{"detect"},
		BackgroundQueries: []*plan.Query{{Steps: []int32{1}}}, MaxDop: 8,
		HasForeignKeyAction: true, HasReturning: true, ReturningStep: 1,
		CatalogDependencies: []*plan.ObjectRef{{ObjName: "dependency"}},
		Nodes: []*plan.Node{{NodeType: plan.Node_AGG, RecursiveSink: true, RecursiveCte: true,
			RollupFilter: true, PartitionByCount: 2, GroupByHashKey: []int32{0},
			PreInsertSkCtx: &plan.PreInsertUkCtx{}, PostDmlCtx: &plan.PostDmlCtx{},
			SendMsgList: []plan.MsgHeader{{MsgTag: 1}}, RecvMsgList: []plan.MsgHeader{{MsgTag: 2}},
		}},
	}}}
	copied := DeepCopyPlan(original)
	require.True(t, proto.Equal(original, copied))
	require.NotSame(t, original.GetQuery(), copied.GetQuery())
	require.NotSame(t, original.GetQuery().Nodes[0], copied.GetQuery().Nodes[0])
	copied.GetQuery().BackgroundQueries[0].Steps[0] = 9
	copied.GetQuery().CatalogDependencies[0].ObjName = "changed"
	require.Equal(t, int32(1), original.GetQuery().BackgroundQueries[0].Steps[0])
	require.Equal(t, "dependency", original.GetQuery().CatalogDependencies[0].ObjName)
}

func BenchmarkDeepCopyNode(b *testing.B) {
	node := &plan.Node{NodeType: plan.Node_AGG, Children: []int32{1},
		GroupBy: []*plan.Expr{twTestColExpr(1, 0)}, AggList: []*plan.Expr{twTestColExpr(1, 1)},
		OrderBy: []*plan.OrderBySpec{{Expr: twTestColExpr(1, 2)}}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = DeepCopyNode(node)
	}
}

func BenchmarkDeepCopyPlan(b *testing.B) {
	pl := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{Steps: []int32{0}, Nodes: []*plan.Node{{
		NodeType: plan.Node_SORT, Children: []int32{1}, OrderBy: []*plan.OrderBySpec{{Expr: twTestColExpr(1, 0)}},
	}}}}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = DeepCopyPlan(pl)
	}
}

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
		GapFillStart:          twTestColExpr(2, 1),
		GapFillEnd:            twTestColExpr(2, 2),
		TimeWindowPartitionBy: []*plan.Expr{twTestColExpr(3, 0), twTestColExpr(3, 1)},
	}

	copied := DeepCopyNode(node)

	require.Equal(t, node.Interval, copied.Interval)
	require.Equal(t, node.Sliding, copied.Sliding)
	require.Equal(t, node.Timestamp, copied.Timestamp)
	require.Equal(t, node.WEnd, copied.WEnd)
	require.Equal(t, node.FillType, copied.FillType)
	require.Equal(t, node.FillVal, copied.FillVal)
	require.Equal(t, node.GapFillStart, copied.GapFillStart)
	require.Equal(t, node.GapFillEnd, copied.GapFillEnd)
	require.Equal(t, node.TimeWindowPartitionBy, copied.TimeWindowPartitionBy)

	// Deep, not shallow: the copy must not alias the original's expressions.
	copied.TimeWindowPartitionBy[0].GetCol().ColPos = 99
	copied.WEnd.GetCol().ColPos = 99
	copied.FillVal[0].GetCol().ColPos = 99
	copied.GapFillStart.GetCol().ColPos = 99
	copied.GapFillEnd.GetCol().ColPos = 99
	require.Equal(t, int32(0), node.TimeWindowPartitionBy[0].GetCol().ColPos)
	require.Equal(t, int32(3), node.WEnd.GetCol().ColPos)
	require.Equal(t, int32(0), node.FillVal[0].GetCol().ColPos)
	require.Equal(t, int32(1), node.GapFillStart.GetCol().ColPos)
	require.Equal(t, int32(2), node.GapFillEnd.GetCol().ColPos)
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
	startExpr := twTestColExpr(3, 1)
	endExpr := twTestColExpr(3, 2)
	node := &plan.Node{
		NodeType:              plan.Node_TIME_WINDOW,
		TimeWindowPartitionBy: []*plan.Expr{partExpr},
		GapFillStart:          startExpr,
		GapFillEnd:            endExpr,
	}
	pl := &Plan{Plan: &plan.Plan_Query{Query: &Query{
		Steps: []int32{0},
		Nodes: []*plan.Node{node},
	}}}

	rule := &twMarkVisitRule{seen: make(map[*plan.Expr]bool)}
	require.NoError(t, NewVisitPlan(pl, []VisitPlanRule{rule}).Visit(t.Context()))
	require.True(t, rule.seen[partExpr], "partition keys must be visited")
	require.True(t, rule.seen[startExpr], "GAPFILL start must be visited")
	require.True(t, rule.seen[endExpr], "GAPFILL finish must be visited")
}

type twFailVisitRule struct{}

func (*twFailVisitRule) MatchNode(*Node) bool  { return false }
func (*twFailVisitRule) IsApplyExpr() bool     { return true }
func (*twFailVisitRule) ApplyNode(*Node) error { return nil }
func (*twFailVisitRule) ApplyExpr(*Expr) (*Expr, error) {
	return nil, errors.New("time-window expression visit failed")
}

func TestVisitPlanPropagatesGapFillBoundErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		node *plan.Node
	}{
		{name: "start", node: &plan.Node{NodeType: plan.Node_TIME_WINDOW, GapFillStart: twTestColExpr(3, 1)}},
		{name: "finish", node: &plan.Node{NodeType: plan.Node_TIME_WINDOW, GapFillEnd: twTestColExpr(3, 2)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pl := &Plan{Plan: &plan.Plan_Query{Query: &Query{
				Steps: []int32{0},
				Nodes: []*plan.Node{tc.node},
			}}}

			err := NewVisitPlan(pl, []VisitPlanRule{&twFailVisitRule{}}).Visit(t.Context())
			require.EqualError(t, err, "time-window expression visit failed")
		})
	}
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
