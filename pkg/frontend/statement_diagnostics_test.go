// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
package frontend

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/models"
	"github.com/matrixorigin/matrixone/pkg/util/resource"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
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
	}{{"L0", 0}, {"L1", 2 * time.Second}, {"L2", 5 * time.Second}, {"L3", 17 * time.Second}} {
		b.Run(tc.name, func(b *testing.B) {
			q, phy := compactDiagnosticFixture()
			s := &motrace.StatementInfo{ResponseAt: time.Now(), Duration: tc.duration}
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				h := NewJsonPlanHandler(ctx, s, nil, q, phy)
				h.Marshal(ctx)
				h.Free()
			}
		})
	}
}

func TestCompactDiagnosticFailedGenerationAnalysis(t *testing.T) {
	defer gostub.Stub(&motrace.UseCompactStatementDiagnostics, func() bool { return true }).Reset()
	for _, duration := range []time.Duration{time.Millisecond, 17 * time.Second} {
		for _, state := range []string{"nil_plan", "retained_topology", "analyzed"} {
			t.Run(duration.String()+"/"+state, func(t *testing.T) {
				q, phy := compactDiagnosticFixture()
				if state == "nil_plan" {
					phy = nil
				} else if state == "analyzed" {
					phy.Resource = &resource.StatementResourceSummary{}
				}
				h := newJsonPlanHandler(context.Background(), &motrace.StatementInfo{ResponseAt: time.Now(), Duration: duration}, nil, q, phy, context.Canceled)
				defer h.Free()
				// Terminal accounting must not turn missing analysis into availability.
				require.True(t, h.SetStatementDiagnostics(context.Background(), resource.StatementResourceSummary{StatementWallNS: uint64(duration), AttemptCount: 1}, context.Canceled))
				d := decodeCompactDiagnostic(t, h)
				require.Equal(t, "cancelled", d.Outcome)
				require.GreaterOrEqual(t, d.CapturedLevel, 2)
				require.Equal(t, len(q.GetQuery().Nodes), d.Detail.LogicalTotal)
				if state == "analyzed" {
					require.Equal(t, "complete", d.Detail.Capture)
					require.NotEmpty(t, d.Logical)
					require.True(t, d.Logical[0].AnalyzeAvailable)
					if d.Level == 3 {
						require.NotEmpty(t, d.Physical)
					}
				} else {
					require.Equal(t, "execution_failed_before_analysis", d.Detail.Capture)
					require.Empty(t, d.Logical)
					require.Empty(t, d.Physical)
					require.Contains(t, models.RenderStatementDiagnostics(d, models.VerboseOption), "execution_failed_before_analysis")
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
