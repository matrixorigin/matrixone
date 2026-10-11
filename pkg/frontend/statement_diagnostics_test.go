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

package frontend

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/models"
	"github.com/matrixorigin/matrixone/pkg/sql/schedule"
	"github.com/matrixorigin/matrixone/pkg/util/resource"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"
)

func compactDiagnosticFixture() (*plan.Plan, *models.PhyPlan) {
	q := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{Steps: []int32{0}, Nodes: []*plan.Node{
		{NodeId: 20, NodeType: plan.Node_JOIN, AnalyzeInfo: &plan.AnalyzeInfo{TimeConsumed: 10}},
		{NodeId: 10, NodeType: plan.Node_TABLE_SCAN, TableDef: &plan.TableDef{Name: "t"}, ObjRef: &plan.ObjectRef{SchemaName: "db", ObjName: "t"}, AnalyzeInfo: &plan.AnalyzeInfo{ReadSize: 1024, InputRows: 10}},
	}}}}
	child := &models.PhyOperator{OpName: "scan", NodeIdx: 1, OpStats: &process.OperatorStats{TimeConsumed: 300, ReadSize: 1024}}
	phy := &models.PhyPlan{LocalScope: []models.PhyScope{{RootOperator: &models.PhyOperator{OpName: "join", NodeIdx: 0, OpStats: &process.OperatorStats{TimeConsumed: 10}, Children: []*models.PhyOperator{nil, child}}}}}
	return q, phy
}
func decodeCompactDiagnostic(t *testing.T, h *jsonPlanHandler) *models.StatementDiagnostics {
	t.Helper()
	var p models.ExplainData
	raw := h.Marshal(context.Background())
	require.NoError(t, json.Unmarshal(raw, &p))
	require.NotNil(t, p.StatementDiagnostics)
	return p.StatementDiagnostics
}
func TestCompactDiagnosticSnapshotAndTerminalRefresh(t *testing.T) {
	defer gostub.Stub(&motrace.UseCompactStatementDiagnostics, func() bool { return true }).Reset()
	q, phy := compactDiagnosticFixture()
	phy.Resource = &resource.StatementResourceSummary{}
	stmt := &motrace.StatementInfo{ResponseAt: time.Now(), Duration: 17 * time.Second}
	h := newJsonPlanHandler(context.Background(), stmt, nil, q, phy, nil)
	defer h.Free()
	first := decodeCompactDiagnostic(t, h)
	require.Equal(t, 3, first.Level)
	require.NotEmpty(t, first.Physical)
	found := false
	for _, n := range first.Physical {
		found = found || n.Index == 1 && n.ReadBytes == 1024
	}
	require.True(t, found, "second child must be visited")
	q.GetQuery().Nodes[1].AnalyzeInfo.ReadSize = 0
	q.GetQuery().Nodes[1].ObjRef.ObjName = "reused"
	phy.LocalScope[0].RootOperator.Children[1].OpStats.ReadSize = 0
	require.Nil(t, h.marshalHandler.stmt)
	require.Nil(t, h.marshalHandler.query)
	summary := resource.StatementResourceSummary{StatementWallNS: uint64(17 * time.Second), AttemptCount: 2, Usage: resource.Usage{S3ReadBytes: 42}}
	require.True(t, h.SetStatementDiagnostics(context.Background(), summary, context.Canceled))
	final := decodeCompactDiagnostic(t, h)
	require.Equal(t, "cancelled", final.Outcome)
	require.Equal(t, uint64(42), final.Summary.S3ReadBytes)
	require.Equal(t, first.Logical, final.Logical)
	require.Equal(t, first.Physical, final.Physical)
	h.Free()
	h.Free()
	require.Nil(t, h.Marshal(context.Background()))
}

func TestCompactDiagnosticLateTriggerAndNormalControl(t *testing.T) {
	defer gostub.Stub(&motrace.UseCompactStatementDiagnostics, func() bool { return true }).Reset()
	q, phy := compactDiagnosticFixture()
	stmt := &motrace.StatementInfo{ResponseAt: time.Now(), Duration: 0}
	h := NewJsonPlanHandler(context.Background(), stmt, nil, q, phy)
	defer h.Free()
	require.Equal(t, "{}", string(h.Marshal(context.Background())))
	require.False(t, h.SetStatementDiagnostics(context.Background(), resource.StatementResourceSummary{StatementWallNS: 0}, nil))
	require.Nil(t, h.marshalHandler.marshalPlan)
	summary := resource.StatementResourceSummary{StatementWallNS: uint64(time.Millisecond), Memory: resource.MemoryTotals{MaxDomainPeakLiveBytes: 1 << 30}}
	require.True(t, h.SetStatementDiagnostics(context.Background(), summary, nil))
	d := decodeCompactDiagnostic(t, h)
	require.Equal(t, 3, d.Level)
	require.Zero(t, d.CapturedLevel)
	require.Equal(t, "not_selected_before_release", d.Detail.Capture)
	require.Empty(t, d.Logical)
	require.Empty(t, d.Physical)
}

func TestCompactDiagnosticPhysicalBounds(t *testing.T) {
	_, phy := compactDiagnosticFixture()
	root := phy.LocalScope[0].RootOperator
	root.Children = append(root.Children, root)
	rows, visited, bounded := projectDiagnosticPhysical(phy)
	require.True(t, bounded)
	require.LessOrEqual(t, visited, 4096)
	require.NotEmpty(t, rows)
	root.Children = make([]*models.PhyOperator, 10000)
	_, visited, bounded = projectDiagnosticPhysical(phy)
	require.True(t, bounded)
	require.Equal(t, 4096, visited, "nil edges also consume bounded work")
	root.Children = nil
	cur := root
	for i := 0; i < 100; i++ {
		n := &models.PhyOperator{OpName: "deep"}
		cur.Children = []*models.PhyOperator{n}
		cur = n
	}
	_, visited, bounded = projectDiagnosticPhysical(phy)
	require.True(t, bounded)
	require.LessOrEqual(t, visited, 65)
}

func TestCompactDiagnosticHugeIdentifiersAndErrorCapture(t *testing.T) {
	defer gostub.Stub(&motrace.UseCompactStatementDiagnostics, func() bool { return true }).Reset()
	q, phy := compactDiagnosticFixture()
	q.GetQuery().Nodes[1].ObjRef.ObjName = strings.Repeat("\x00界", 100000)
	phy.Resource = &resource.StatementResourceSummary{}
	stmt := &motrace.StatementInfo{ResponseAt: time.Now(), Duration: time.Millisecond}
	h := newJsonPlanHandler(context.Background(), stmt, nil, q, phy, context.DeadlineExceeded)
	defer h.Free()
	d := decodeCompactDiagnostic(t, h)
	require.Equal(t, 2, d.CapturedLevel)
	require.Equal(t, "timeout", d.Outcome)
	require.Nil(t, h.marshalHandler.schedulingTrace)
	require.Less(t, len(h.Marshal(context.Background())), models.DiagnosticsL2Budget)
}

func BenchmarkCompactStatementDiagnostics(b *testing.B) {
	defer gostub.Stub(&motrace.UseCompactStatementDiagnostics, func() bool { return true }).Reset()
	for _, tc := range []struct {
		name     string
		duration time.Duration
		preview  bool
	}{{"L0/no_preview", 0, false}, {"L0/executed_preview", 0, true}, {"L1", 2 * time.Second, true}, {"L2", 5 * time.Second, true}, {"L3", 17 * time.Second, true}} {
		b.Run(tc.name, func(b *testing.B) {
			q, phy := compactDiagnosticFixture()
			if tc.preview {
				phy.Resource = &resource.StatementResourceSummary{StatementWallNS: uint64(tc.duration), AttemptCount: 1}
			}
			s := &motrace.StatementInfo{ResponseAt: time.Now(), Duration: tc.duration}
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				h := NewJsonPlanHandler(ctx, s, nil, q, phy, WithWaitActiveCost(0))
				h.Marshal(ctx)
				h.Free()
			}
		})
	}
}

func TestCompactDiagnosticGenerationAnalysis(t *testing.T) {
	defer gostub.Stub(&motrace.UseCompactStatementDiagnostics, func() bool { return true }).Reset()
	for _, tc := range []struct {
		duration time.Duration
		err      error
		outcome  string
		missing  string
		level    int
	}{
		{time.Millisecond, context.Canceled, "cancelled", "execution_failed_before_analysis", 2},
		{17 * time.Second, context.Canceled, "cancelled", "execution_failed_before_analysis", 3},
		{2 * time.Second, nil, "success", "analysis_unavailable", 1},
		{5 * time.Second, nil, "success", "analysis_unavailable", 2},
		{17 * time.Second, nil, "success", "analysis_unavailable", 3},
	} {
		for _, state := range []string{"nil_plan", "retained_topology", "analyzed", "analyzed_zero"} {
			t.Run(tc.outcome+"/"+tc.duration.String()+"/"+state, func(t *testing.T) {
				q, phy := compactDiagnosticFixture()
				analyzed := strings.HasPrefix(state, "analyzed")
				if state == "nil_plan" {
					phy = nil
				} else if analyzed {
					phy.Resource = &resource.StatementResourceSummary{}
				}
				if state == "analyzed_zero" {
					for _, n := range q.GetQuery().Nodes {
						n.AnalyzeInfo = &plan.AnalyzeInfo{}
					}
					phy.LocalScope = nil
				}
				h := newJsonPlanHandler(context.Background(), &motrace.StatementInfo{ResponseAt: time.Now(), Duration: tc.duration}, nil, q, phy, tc.err)
				defer h.Free()
				// Terminal accounting must not turn missing analysis into availability.
				require.True(t, h.SetStatementDiagnostics(context.Background(), resource.StatementResourceSummary{StatementWallNS: uint64(tc.duration), AttemptCount: 1}, tc.err))
				d := decodeCompactDiagnostic(t, h)
				require.Equal(t, tc.outcome, d.Outcome)
				require.Equal(t, tc.level, d.CapturedLevel)
				require.Equal(t, len(q.GetQuery().Nodes), d.Detail.LogicalTotal)
				require.Equal(t, len(q.GetQuery().Nodes)-len(d.Logical), d.Detail.LogicalOmitted)
				if analyzed {
					require.Equal(t, "complete", d.Detail.Capture)
				}
				if analyzed && tc.level >= 2 {
					require.NotEmpty(t, d.Logical)
					require.True(t, d.Logical[0].AnalyzeAvailable)
					if state == "analyzed_zero" {
						require.Zero(t, d.Logical[0].ElapsedNS)
						require.Zero(t, d.Logical[0].OutputRows)
					}
					if d.Level == 3 && state != "analyzed_zero" {
						require.NotEmpty(t, d.Physical)
					}
				} else {
					if !analyzed {
						require.Equal(t, tc.missing, d.Detail.Capture)
						require.Contains(t, models.RenderStatementDiagnostics(d, models.VerboseOption), tc.missing)
					}
					require.Empty(t, d.Logical)
					require.Empty(t, d.Physical)
				}
				if tc.level == 1 {
					require.Contains(t, models.RenderStatementDiagnostics(d, models.NormalOption), "logical-omitted=2")
					require.LessOrEqual(t, len(h.Marshal(context.Background())), models.DiagnosticsHeaderBudget)
				}
			})
		}
	}
}

func TestCompactDiagnosticLoggerExclusionPreservesTopLevelStats(t *testing.T) {
	defer gostub.Stub(&motrace.UseCompactStatementDiagnostics, func() bool { return true }).Reset()
	q, phy := compactDiagnosticFixture()
	phy.Resource = &resource.StatementResourceSummary{}
	stmt := &motrace.StatementInfo{Account: "sys", User: "mo_logger", ResponseAt: time.Now()}
	require.True(t, stmt.IsMoLogger())
	h := NewJsonPlanHandler(context.Background(), stmt, nil, q, phy)
	defer h.Free()
	stats, scan := h.Stats(context.Background())
	require.Zero(t, stats.GetTimeConsumed(), "logger must not run discarded composite resource projection")
	require.Equal(t, int64(10), scan.RowsRead)
	require.Equal(t, "{}", string(h.Marshal(context.Background())))
}

func TestCompactDiagnosticCompileErrorWait(t *testing.T) {
	defer gostub.Stub(&motrace.UseCompactStatementDiagnostics, func() bool { return true }).Reset()
	for _, tc := range []struct {
		name          string
		handler, txn  bool
		wait, elapsed time.Duration
		known         bool
		level         int
	}{
		{"measured zero", true, true, 0, 17 * time.Second, true, 3},
		{"measured wait", true, true, 16 * time.Second, time.Second, true, 2},
		{"wait exceeds wall", true, true, 20 * time.Second, 0, true, 2},
		{"invalid measurement", true, true, -1, 17 * time.Second, false, 3},
		{"nil handler", false, false, 0, 17 * time.Second, false, 3},
		{"no transaction", true, false, 0, 17 * time.Second, false, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			stmt := &motrace.StatementInfo{}
			t.Cleanup(stmt.FreeExecPlan)
			ses := &Session{}
			ses.SetTStmt(stmt)
			if tc.handler {
				ses.txnHandler = &TxnHandler{}
			}
			if tc.txn {
				txn := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
				txn.EXPECT().GetWaitActiveCost().Return(tc.wait).Times(1)
				ses.txnHandler.txnOp, ses.txnHandler.txnCtx = txn, ctx
			}
			cwft := &TxnComputationWrapper{ses: ses}
			attempt := cwft.schedulingTrace.StartAttempt()
			cwft.schedulingTrace.RecordFailure(attempt, "candidate-discovery", schedule.Worker{})
			cwft.recordSchedulingTraceOnCompileError(ctx)
			require.NotNil(t, stmt.ExecPlan)
			require.True(t, stmt.ResponseAt.IsZero(), "capture must not mark response before terminal completion")
			// Only the copied scalar may survive transaction cleanup.
			ses.txnHandler = nil
			h := stmt.ExecPlan.(*jsonPlanHandler)
			require.True(t, h.SetStatementDiagnostics(ctx, resource.StatementResourceSummary{StatementWallNS: uint64(17 * time.Second)}, context.Canceled))
			d := decodeCompactDiagnostic(t, h)
			require.Equal(t, "cancelled", d.Outcome)
			require.Equal(t, tc.level, d.Level)
			require.Equal(t, tc.known, d.Summary.WaitActiveKnown)
			require.Equal(t, uint64(tc.elapsed), d.Summary.ElapsedNS)
			require.Equal(t, 1, d.Scheduling.FailureCount)
			if tc.known {
				require.Equal(t, uint64(tc.wait), d.Summary.WaitActiveNS)
			} else {
				require.NotContains(t, string(h.Marshal(ctx)), `"wait_active_ns"`)
			}
		})
	}
}

func TestCompactDiagnosticUnknownWaitAdmission(t *testing.T) {
	defer gostub.Stub(&motrace.UseCompactStatementDiagnostics, func() bool { return true }).Reset()
	threshold := motrace.GetLongQueryTime()
	for _, tc := range []struct {
		name string
		wall time.Duration
		opts []marshalPlanOptions
	}{
		{"unsupplied wait at boundary", threshold, nil},
		{"unknown wait at boundary", threshold, []marshalPlanOptions{WithWaitActiveCost(-1)}},
		{"wait exceeds wall", threshold, []marshalPlanOptions{WithWaitActiveCost(threshold + time.Second)}},
		{"negative wall", time.Duration(math.MinInt64), []marshalPlanOptions{WithWaitActiveCost(time.Duration(math.MaxInt64))}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, phy := compactDiagnosticFixture()
			phy.Resource = &resource.StatementResourceSummary{}
			h := NewJsonPlanHandler(context.Background(), &motrace.StatementInfo{ResponseAt: time.Now(), Duration: tc.wall}, nil, q, phy, tc.opts...)
			t.Cleanup(h.Free)
			require.Equal(t, "{}", string(h.Marshal(context.Background())))
		})
	}
}

func TestCompactDiagnosticUnknownWaitTerminalRefresh(t *testing.T) {
	defer gostub.Stub(&motrace.UseCompactStatementDiagnostics, func() bool { return true }).Reset()
	ctx := context.Background()
	stmt := &motrace.StatementInfo{}
	t.Cleanup(stmt.FreeExecPlan)
	ses := &Session{}
	ses.SetTStmt(stmt)
	cwft := &TxnComputationWrapper{ses: ses}
	require.NoError(t, cwft.RecordCompoundStmt(ctx, statistic.DefaultStatsArray))
	h := stmt.ExecPlan.(*jsonPlanHandler)
	for _, wall := range []time.Duration{5 * time.Second, time.Second} {
		require.True(t, h.SetStatementDiagnostics(ctx, resource.StatementResourceSummary{StatementWallNS: uint64(wall)}, errors.Join(context.Canceled, context.DeadlineExceeded)))
		d := decodeCompactDiagnostic(t, h) // Marshal between updates also challenges cache invalidation.
		require.Equal(t, "timeout", d.Outcome)
		require.False(t, d.Summary.WaitActiveKnown)
		require.Equal(t, uint64(wall), d.Summary.ElapsedNS)
		require.NotContains(t, string(h.Marshal(ctx)), `"wait_active_ns"`)
	}
}
