// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

package frontend

import (
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/models"
	"github.com/matrixorigin/matrixone/pkg/sql/schedule"
	"github.com/matrixorigin/matrixone/pkg/util/resource"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
)

func (h *marshalPlanHandler) captureStatementDiagnostics(ctx context.Context, phy *models.PhyPlan, runErr error) {
	var preview *resource.StatementResourceSummary
	if phy != nil {
		preview = phy.Resource
	}
	elapsed := h.stmt.Duration - h.waitActiveCost
	if elapsed < 0 {
		elapsed = 0
	}
	level, reasons := models.SelectStatementDiagnosticLevel(elapsed, motrace.GetLongQueryTime(), preview, runErr != nil, h.persistSchedulingTrace)
	if level == 0 || h.stmt.IsMoLogger() {
		return
	}
	d := &models.StatementDiagnostics{Version: models.DiagnosticsVersion, Level: level, CapturedLevel: level, Reasons: reasons, Outcome: models.DiagnosticOutcome(runErr), Detail: models.DiagnosticDetail{Capture: "complete"}}
	if preview != nil {
		d.SetSummary(*preview, h.stmt.Duration, h.waitActiveCost, h.waitActiveCost >= 0)
	}
	d.SetPhases(statistic.StatsInfoFromContext(ctx))
	d.Scheduling = projectDiagnosticScheduling(h.schedulingTrace)
	if h.query == nil {
		d.Detail.Capture = "no_query"
	} else if level >= 2 {
		d.Logical = projectDiagnosticLogical(h.query, level)
		d.Detail.LogicalTotal = len(h.query.Nodes)
	}
	if level >= 3 {
		if phy == nil {
			d.Detail.Capture = "no_physical_plan"
		} else if runErr != nil && phy.Resource == nil {
			d.Detail.Capture = "execution_failed_before_analysis"
		} else {
			d.Physical, d.Detail.PhysicalVisited, d.Detail.PhysicalTruncated = projectDiagnosticPhysical(phy)
			if d.Detail.PhysicalTruncated {
				d.Detail.Capture = "bounded"
			}
		}
	}
	h.marshalPlan = &models.ExplainData{StatementDiagnostics: d}
	// Only the bounded scalar scheduling projection remains retained.
	h.schedulingTrace = nil
}

// SetStatementDiagnostics is called at the existing terminal publication
// boundary. Late triggers get a summary, never references to released plans.
func (h *jsonPlanHandler) SetStatementDiagnostics(ctx context.Context, summary resource.StatementResourceSummary, err error) bool {
	if !motrace.UseCompactStatementDiagnostics() {
		return false
	}
	if h.marshalHandler != nil && h.marshalHandler.isInternalSubStmt {
		return false
	}
	var wait time.Duration
	known := false
	if h.marshalHandler != nil {
		wait = h.marshalHandler.waitActiveCost
		known = wait >= 0
	}
	elapsed := time.Duration(summary.StatementWallNS) - wait
	if elapsed < 0 {
		elapsed = 0
	}
	level, reasons := models.SelectStatementDiagnosticLevel(elapsed, motrace.GetLongQueryTime(), &summary, err != nil, h.persistSchedulingTrace)
	var d *models.StatementDiagnostics
	if h.marshalHandler != nil && h.marshalHandler.marshalPlan != nil {
		d = h.marshalHandler.marshalPlan.StatementDiagnostics
	}
	if d == nil && level == 0 {
		return false
	}
	if d == nil {
		d = &models.StatementDiagnostics{Version: models.DiagnosticsVersion, Detail: models.DiagnosticDetail{Capture: "not_selected_before_release"}}
		if h.marshalHandler == nil {
			h.marshalHandler = &marshalPlanHandler{}
		}
		h.marshalHandler.marshalPlan = &models.ExplainData{StatementDiagnostics: d}
	}
	if level > d.CapturedLevel {
		d.Detail.Capture = "not_selected_before_release"
	}
	d.Level = max(level, d.Level)
	d.Reasons |= reasons
	d.Outcome = models.DiagnosticOutcome(err)
	d.SetSummary(summary, time.Duration(summary.StatementWallNS), wait, known)
	d.SetPhases(statistic.StatsInfoFromContext(ctx))
	if h.buffer != nil {
		releaseMarshalPlanBufferPool(h.buffer)
		h.buffer = nil
	}
	h.jsonBytes = nil
	return true
}

func projectDiagnosticScheduling(t *schedule.Trace) *models.DiagnosticScheduling {
	if t == nil || t.Empty() {
		return nil
	}
	s := &models.DiagnosticScheduling{AttemptCount: t.AttemptCount, Truncated: t.Truncated}
	if len(t.Attempts) == 0 {
		return s
	}
	a := t.Attempts[len(t.Attempts)-1]
	s.FailureCount = a.FailureCount
	s.Truncated = s.Truncated || a.Truncated || a.DetailsOmitted
	if q := a.Query; q != nil {
		s.ExecKind = models.BoundDiagnosticString(q.ExecKind, 64)
		s.Reason = models.BoundDiagnosticString(q.Reason, 64)
		s.SelectedCount = q.SelectedCount
		s.DroppedCount = q.DroppedCount
		s.Fallback = q.Fallback || q.PoolFallback
		s.RequestedPool = models.BoundDiagnosticString(q.RequestedPool, 64)
		s.ResolvedPool = models.BoundDiagnosticString(q.ResolvedPool, 64)
	}
	return s
}

// A fixed union of per-axis outliers preserves IO/memory/wait bottlenecks
// without ranking by an invented combined cost or retaining every node.
type diagnosticCandidate struct {
	index, scope, ordinal int
	remote                bool
	values                [6]int64
	logical               *plan.Node
	physical              *models.PhyOperator
}
type diagnosticCandidates struct {
	top   [6][4]diagnosticCandidate
	count [6]int
	limit int
}

func (r *diagnosticCandidates) add(c diagnosticCandidate) {
	for axis, value := range c.values {
		if value <= 0 {
			continue
		}
		at := r.count[axis]
		for i := 0; i < r.count[axis]; i++ {
			if value > r.top[axis][i].values[axis] {
				at = i
				break
			}
		}
		if at >= r.limit {
			continue
		}
		n := min(r.count[axis], r.limit-1)
		for i := n; i > at; i-- {
			r.top[axis][i] = r.top[axis][i-1]
		}
		r.top[axis][at] = c
		r.count[axis] = min(r.count[axis]+1, r.limit)
	}
}
func (r *diagnosticCandidates) selected() []diagnosticCandidate {
	out := make([]diagnosticCandidate, 0, 6*r.limit)
	for axis := range r.top {
		for i := 0; i < r.count[axis]; i++ {
			c := r.top[axis][i]
			duplicate := false
			for _, v := range out {
				if c.logical != nil && v.logical == c.logical || c.physical != nil && (v.physical == c.physical || v.physical.OpStats == c.physical.OpStats) {
					duplicate = true
					break
				}
			}
			if !duplicate {
				out = append(out, c)
			}
		}
	}
	return out
}
func diagnosticPositive(n int64) int64 {
	if n < 0 {
		return 0
	}
	return n
}
func diagnosticLogicalRow(c diagnosticCandidate) models.DiagnosticNode {
	n := c.logical
	r := models.DiagnosticNode{Index: c.index, Kind: models.BoundDiagnosticString(n.NodeType.String(), 64), AnalyzeAvailable: n.AnalyzeInfo != nil}
	if n.ObjRef != nil {
		r.Object = models.BoundDiagnosticString(models.BoundDiagnosticString(n.ObjRef.SchemaName, 128)+"."+models.BoundDiagnosticString(n.ObjRef.ObjName, 128), 128)
	}
	if a := n.AnalyzeInfo; a != nil {
		r.ElapsedNS = diagnosticPositive(a.TimeConsumed)
		r.WaitNS = diagnosticPositive(a.WaitTimeConsumed)
		r.InputRows = diagnosticPositive(a.InputRows)
		r.OutputRows = diagnosticPositive(a.OutputRows)
		r.InputBytes = diagnosticPositive(a.InputSize)
		r.OutputBytes = diagnosticPositive(a.OutputSize)
		r.MemoryBytes = diagnosticPositive(a.MemoryMax)
		r.SpillBytes = diagnosticPositive(a.SpillSize)
		r.ReadBytes = diagnosticPositive(a.ReadSize)
		r.S3ReadBytes = diagnosticPositive(a.S3ReadSize)
	}
	return r
}
func projectDiagnosticLogical(q *plan.Query, level int) []models.DiagnosticNode {
	rank := diagnosticCandidates{limit: 2}
	roots := 4
	if level >= 3 {
		rank.limit = 4
		roots = 8
	}
	for i, n := range q.Nodes {
		if n == nil || n.AnalyzeInfo == nil {
			continue
		}
		a := n.AnalyzeInfo
		rank.add(diagnosticCandidate{index: i, logical: n, values: [6]int64{a.TimeConsumed, a.WaitTimeConsumed, a.MemoryMax, a.SpillSize, a.ReadSize, a.OutputRows}})
	}
	candidates := rank.selected()
	for i, idx := range q.Steps {
		if i >= roots {
			break
		}
		if idx < 0 || int(idx) >= len(q.Nodes) || q.Nodes[idx] == nil {
			continue
		}
		duplicate := false
		for _, v := range candidates {
			if v.index == int(idx) {
				duplicate = true
				break
			}
		}
		if !duplicate {
			candidates = append(candidates, diagnosticCandidate{index: int(idx), logical: q.Nodes[idx]})
		}
	}
	rows := make([]models.DiagnosticNode, 0, len(candidates))
	for _, c := range candidates {
		rows = append(rows, diagnosticLogicalRow(c))
	}
	return rows
}

func projectDiagnosticPhysical(phy *models.PhyPlan) ([]models.DiagnosticNode, int, bool) {
	const maxVisits = 4096
	const maxScopes = 1024
	const maxDepth = 64
	rank := diagnosticCandidates{limit: 4}
	seen := make(map[*models.PhyOperator]struct{})
	var anomalies [8]diagnosticCandidate
	anomalyCount := 0
	visits, scopes := 0, 0
	bounded := false
	var walkOp func(*models.PhyOperator, int, bool, int)
	walkOp = func(op *models.PhyOperator, scope int, remote bool, depth int) {
		if visits >= maxVisits {
			bounded = true
			return
		}
		visits++
		if op == nil {
			return
		}
		if depth >= maxDepth {
			bounded = true
			return
		}
		if _, ok := seen[op]; ok {
			bounded = true
			return
		}
		seen[op] = struct{}{}
		if s := op.OpStats; s != nil {
			c := diagnosticCandidate{index: op.NodeIdx, scope: scope, ordinal: visits, remote: remote, physical: op, values: [6]int64{s.TimeConsumed, s.WaitTimeConsumed, s.MemorySize, s.SpillSize, s.ReadSize, s.OutputRows}}
			rank.add(c)
			if s.ResourceQuality != 0 && anomalyCount < len(anomalies) {
				anomalies[anomalyCount] = c
				anomalyCount++
			}
		}
		for _, child := range op.Children {
			if visits >= maxVisits {
				bounded = true
				break
			}
			walkOp(child, scope, remote, depth+1)
		}
	}
	var walkScopes func([]models.PhyScope, bool, int)
	walkScopes = func(ss []models.PhyScope, remote bool, depth int) {
		if depth >= maxDepth {
			if len(ss) > 0 {
				bounded = true
			}
			return
		}
		for i := range ss {
			if scopes >= maxScopes || visits >= maxVisits {
				bounded = true
				break
			}
			scopes++
			walkOp(ss[i].RootOperator, scopes, remote, 0)
			walkScopes(ss[i].PreScopes, remote, depth+1)
		}
	}
	walkScopes(phy.LocalScope, false, 0)
	walkScopes(phy.RemoteScope, true, 0)
	candidates := rank.selected()
	for i := 0; i < anomalyCount; i++ {
		c := anomalies[i]
		duplicate := false
		for _, v := range candidates {
			if v.physical.OpStats == c.physical.OpStats {
				duplicate = true
				break
			}
		}
		if !duplicate {
			candidates = append(candidates, c)
		}
	}
	rows := make([]models.DiagnosticNode, 0, len(candidates))
	for _, c := range candidates {
		s := c.physical.OpStats
		r := models.DiagnosticNode{Index: c.index, Scope: c.scope, Operator: c.ordinal, Remote: c.remote, Kind: models.BoundDiagnosticString(c.physical.OpName, 64), AnalyzeAvailable: true, ElapsedNS: diagnosticPositive(s.TimeConsumed), WaitNS: diagnosticPositive(s.WaitTimeConsumed), InputRows: diagnosticPositive(s.InputRows), OutputRows: diagnosticPositive(s.OutputRows), InputBytes: diagnosticPositive(s.InputSize), OutputBytes: diagnosticPositive(s.OutputSize), MemoryBytes: diagnosticPositive(s.MemorySize), SpillBytes: diagnosticPositive(s.SpillSize), ReadBytes: diagnosticPositive(s.ReadSize), S3ReadBytes: diagnosticPositive(s.S3ReadSize), NetworkBytes: diagnosticPositive(s.NetworkIO), CallCount: max(0, s.CallNum), Quality: s.ResourceQuality}
		rows = append(rows, r)
	}
	return rows, visits, bounded
}
