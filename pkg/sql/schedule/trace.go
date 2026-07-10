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

package schedule

import (
	"sort"
	"strings"
	"sync"
)

const (
	SchedulingTraceVersion = 1

	maxTraceAttempts           = 8
	maxTraceWorkersPerDecision = 64
	maxTraceWorkerRefs         = 512
	maxTraceScans              = 64
	maxTraceStages             = 128
	maxTraceFailures           = 16
	maxTraceReasonCounts       = 16
	maxTraceWorkerValueBytes   = 256
)

type TraceMode string

const (
	TraceModeExecution TraceMode = "execution"
	TraceModePreview   TraceMode = "preview"
)

type TraceAttemptID uint64

type StageKind string

const (
	StageKindSingleWorker   StageKind = "single-worker"
	StageKindQueryWorkerSet StageKind = "query-worker-set"
)

type Trace struct {
	Version      int            `json:"version"`
	Mode         TraceMode      `json:"mode"`
	Attempts     []AttemptTrace `json:"attempts,omitempty"`
	AttemptCount int            `json:"attemptCount"`
	Truncated    bool           `json:"truncated,omitempty"`
}

type AttemptTrace struct {
	Sequence       int            `json:"sequence"`
	Query          *QueryTrace    `json:"query,omitempty"`
	Scans          []ScanTrace    `json:"scans,omitempty"`
	ScanCount      int            `json:"scanCount,omitempty"`
	Stages         []StageTrace   `json:"stages,omitempty"`
	StageCount     int            `json:"stageCount,omitempty"`
	Failures       []FailureTrace `json:"failures,omitempty"`
	FailureCount   int            `json:"failureCount,omitempty"`
	Truncated      bool           `json:"truncated,omitempty"`
	DetailsOmitted bool           `json:"detailsOmitted,omitempty"`
}

type QueryTrace struct {
	ExecKind          string        `json:"execKind"`
	CurrentCNPolicy   string        `json:"currentCNPolicy"`
	CurrentCN         WorkerTrace   `json:"currentCN"`
	Reason            string        `json:"reason"`
	Satisfied         bool          `json:"satisfied"`
	Fallback          bool          `json:"fallback,omitempty"`
	CandidateSource   string        `json:"candidateSource"`
	PoolResolution    string        `json:"poolResolution"`
	DiscoveredCount   int           `json:"discoveredCount"`
	ResolvedCount     int           `json:"resolvedCount"`
	Selected          []WorkerTrace `json:"selected,omitempty"`
	SelectedCount     int           `json:"selectedCount"`
	SelectedOmitted   bool          `json:"selectedOmitted,omitempty"`
	SelectedTruncated bool          `json:"selectedTruncated,omitempty"`
	Dropped           []ReasonCount `json:"dropped,omitempty"`
	DroppedCount      int           `json:"droppedCount,omitempty"`
	DroppedTruncated  bool          `json:"droppedTruncated,omitempty"`
}

type ScanTrace struct {
	Reason            string        `json:"reason"`
	LocalOnly         bool          `json:"localOnly"`
	QueryWorkerCount  int           `json:"queryWorkerCount"`
	Selected          []WorkerTrace `json:"selected,omitempty"`
	SelectedCount     int           `json:"selectedCount"`
	SelectedTruncated bool          `json:"selectedTruncated,omitempty"`
	HasStats          bool          `json:"hasStats"`
	BlockCount        int32         `json:"blockCount,omitempty"`
	DOP               int32         `json:"dop,omitempty"`
	ForceOneCN        bool          `json:"forceOneCN,omitempty"`
	ForceSingle       bool          `json:"forceSingle,omitempty"`
	ForceMultiCN      bool          `json:"forceMultiCN,omitempty"`
}

type StageTrace struct {
	Kind              StageKind     `json:"kind"`
	Reason            string        `json:"reason"`
	QueryWorkerCount  int           `json:"queryWorkerCount"`
	Selected          []WorkerTrace `json:"selected,omitempty"`
	SelectedCount     int           `json:"selectedCount"`
	SelectedTruncated bool          `json:"selectedTruncated,omitempty"`
}

type FailureTrace struct {
	Category string       `json:"category"`
	Worker   *WorkerTrace `json:"worker,omitempty"`
}

type WorkerTrace struct {
	ID       string `json:"id,omitempty"`
	Routable bool   `json:"routable"`
	Mcpu     int    `json:"mcpu,omitempty"`
	State    string `json:"state,omitempty"`
}

type ReasonCount struct {
	Reason string `json:"reason"`
	Count  int    `json:"count"`
}

// TraceRecorder owns the mutable trace for one statement. It is safe for
// concurrent writers and readers because compile retries and statement export
// can cross goroutine boundaries.
type TraceRecorder struct {
	mu             sync.Mutex
	trace          Trace
	workerRefCount int
	pendingLocal   pendingLocalAttempt
}

// pendingLocalAttempt keeps the common first-attempt local path allocation
// free. It is materialized before a snapshot or as soon as the statement
// becomes interesting enough to persist independently.
type pendingLocalAttempt struct {
	sequence       TraceAttemptID
	query          pendingLocalQuery
	hasQuery       bool
	detailsOmitted bool
}

type pendingLocalQuery struct {
	execKind               QueryExecKind
	currentCNPolicy        CurrentCNPolicy
	currentCN              pendingWorkerTrace
	reason                 string
	candidateResolution    CandidateResolution
	resolvedCandidateCount int
	selectedCount          int
}

type pendingWorkerTrace struct {
	id       string
	routable bool
	mcpu     int
	state    WorkerState
}

func (r *TraceRecorder) StartAttempt() TraceAttemptID {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	r.ensureVersionLocked()
	r.trace.AttemptCount++
	id := TraceAttemptID(r.trace.AttemptCount)
	r.materializePendingLocalLocked()
	if len(r.trace.Attempts) >= maxTraceAttempts {
		r.trace.Truncated = true
		return id
	}
	if id == 1 {
		r.pendingLocal.sequence = id
		return id
	}
	r.trace.Attempts = append(r.trace.Attempts, AttemptTrace{Sequence: int(id)})
	return id
}

func (r *TraceRecorder) RecordQuery(
	attempt TraceAttemptID,
	decision QueryDecision,
) {
	if r == nil || attempt == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.pendingLocal.sequence == attempt {
		if r.pendingLocal.hasQuery {
			return
		}
		if canDeferLocalQuery(decision) {
			r.pendingLocal.query = pendingLocalQuery{
				execKind:        decision.ExecKind,
				currentCNPolicy: decision.CurrentCNPolicy,
				currentCN: pendingWorkerTrace{
					id:       decision.CurrentCN.ID,
					routable: decision.CurrentCN.Addr != "",
					mcpu:     decision.CurrentCN.Mcpu,
					state:    decision.CurrentCN.State,
				},
				reason:                 decision.Reason,
				candidateResolution:    decision.CandidateResolution,
				resolvedCandidateCount: max(decision.ResolvedCandidateCount, 0),
				selectedCount:          len(decision.Workers),
			}
			r.pendingLocal.hasQuery = true
			return
		}
		r.materializePendingLocalLocked()
	}
	target := r.attemptLocked(attempt)
	if target == nil {
		return
	}
	if target.Query != nil {
		return
	}
	var selected []WorkerTrace
	var selectedTruncated bool
	if decision.ExecKind == QueryExecAPMultiCN {
		selected, selectedTruncated = r.traceWorkersLocked(decision.Workers)
	}
	dropped, droppedTruncated := traceDroppedReasons(decision.Dropped)
	target.Query = &QueryTrace{
		ExecKind:          decision.ExecKind.String(),
		CurrentCNPolicy:   decision.CurrentCNPolicy.String(),
		CurrentCN:         traceWorker(decision.CurrentCN),
		Reason:            decision.Reason,
		Satisfied:         decision.Satisfied,
		Fallback:          decision.Reason == ReasonNoCandidateCN,
		CandidateSource:   string(decision.CandidateResolution.DiscoverySource),
		PoolResolution:    string(decision.CandidateResolution.PoolResolution),
		DiscoveredCount:   max(decision.CandidateResolution.DiscoveredCount, 0),
		ResolvedCount:     max(decision.ResolvedCandidateCount, 0),
		Selected:          selected,
		SelectedCount:     len(decision.Workers),
		SelectedOmitted:   decision.ExecKind != QueryExecAPMultiCN && len(decision.Workers) > 0,
		SelectedTruncated: selectedTruncated,
		Dropped:           dropped,
		DroppedCount:      len(decision.Dropped),
		DroppedTruncated:  droppedTruncated,
	}
	if selectedTruncated || droppedTruncated {
		target.Truncated = true
		r.trace.Truncated = true
	}
}

func (r *TraceRecorder) RecordScan(
	attempt TraceAttemptID,
	req ScanRequest,
	decision ScanDecision,
) {
	if r == nil || attempt == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.omitPendingLocalPlacementDetailsLocked(attempt) {
		return
	}
	target := r.attemptLocked(attempt)
	if target == nil {
		return
	}
	if omitLocalPlacementDetails(target) {
		target.DetailsOmitted = true
		return
	}
	target.ScanCount++
	if len(target.Scans) >= maxTraceScans {
		target.Truncated = true
		r.trace.Truncated = true
		return
	}
	selected, selectedTruncated := r.traceWorkersLocked(decision.Workers)
	trace := ScanTrace{
		Reason:            decision.Reason,
		LocalOnly:         decision.LocalOnly,
		QueryWorkerCount:  len(req.QueryWorkers),
		Selected:          selected,
		SelectedCount:     len(decision.Workers),
		SelectedTruncated: selectedTruncated,
		HasStats:          req.Stats != nil,
		ForceSingle:       req.ForceSingle,
		ForceMultiCN:      req.ForceMultiCN,
	}
	if req.Stats != nil {
		trace.BlockCount = req.Stats.BlockNum
		trace.DOP = req.Stats.Dop
		trace.ForceOneCN = req.Stats.ForceOneCN
	}
	target.Scans = append(target.Scans, trace)
	if selectedTruncated {
		target.Truncated = true
		r.trace.Truncated = true
	}
}

func (r *TraceRecorder) RecordStage(
	attempt TraceAttemptID,
	kind StageKind,
	queryWorkers Workers,
	decision StageWorkerSetDecision,
) {
	if r == nil || attempt == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.omitPendingLocalPlacementDetailsLocked(attempt) {
		return
	}
	r.recordStageLocked(attempt, kind, len(queryWorkers), decision.Workers, decision.Reason)
}

func (r *TraceRecorder) recordStageLocked(
	attempt TraceAttemptID,
	kind StageKind,
	queryWorkerCount int,
	workers Workers,
	reason string,
) {
	target := r.attemptLocked(attempt)
	if target == nil {
		return
	}
	if omitLocalPlacementDetails(target) {
		target.DetailsOmitted = true
		return
	}
	target.StageCount++
	if len(target.Stages) >= maxTraceStages {
		target.Truncated = true
		r.trace.Truncated = true
		return
	}
	selected, selectedTruncated := r.traceWorkersLocked(workers)
	target.Stages = append(target.Stages, StageTrace{
		Kind:              kind,
		Reason:            reason,
		QueryWorkerCount:  queryWorkerCount,
		Selected:          selected,
		SelectedCount:     len(workers),
		SelectedTruncated: selectedTruncated,
	})
	if selectedTruncated {
		target.Truncated = true
		r.trace.Truncated = true
	}
}

func (r *TraceRecorder) RecordSingleWorkerStage(
	attempt TraceAttemptID,
	queryWorkers Workers,
	decision StageDecision,
) {
	if r == nil || attempt == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.omitPendingLocalPlacementDetailsLocked(attempt) {
		return
	}
	workers := Workers(nil)
	if hasWorkerIdentity(decision.Worker) || decision.Worker.Mcpu != 0 {
		workers = Workers{decision.Worker}
	}
	r.recordStageLocked(attempt, StageKindSingleWorker, len(queryWorkers), workers, decision.Reason)
}

func (r *TraceRecorder) RecordFailure(
	attempt TraceAttemptID,
	category string,
	worker Worker,
) {
	if r == nil || attempt == 0 || category == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	target := r.attemptLocked(attempt)
	if target == nil {
		return
	}
	target.FailureCount++
	if len(target.Failures) >= maxTraceFailures {
		target.Truncated = true
		r.trace.Truncated = true
		return
	}
	failure := FailureTrace{Category: category}
	if hasWorkerIdentity(worker) || worker.Mcpu != 0 || worker.State != WorkerStateUnknown {
		trace := traceWorker(worker)
		failure.Worker = &trace
	}
	target.Failures = append(target.Failures, failure)
}

func (r *TraceRecorder) Snapshot() Trace {
	if r == nil {
		return Trace{Version: SchedulingTraceVersion, Mode: TraceModeExecution}
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	r.ensureVersionLocked()
	r.materializePendingLocalLocked()
	return cloneTrace(r.trace)
}

// SnapshotForExport returns an ownership-independent trace only when the
// caller needs normal local detail or when the trace must be persisted on its
// own. The normal short local path returns an empty trace without materializing
// or cloning its pending attempt.
func (r *TraceRecorder) SnapshotForExport(includeNormalLocal bool) Trace {
	if r == nil {
		return Trace{Version: SchedulingTraceVersion, Mode: TraceModeExecution}
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	r.ensureVersionLocked()
	// A pending attempt is normal local by construction, so it never changes
	// the standalone persistence decision.
	if !includeNormalLocal && !r.trace.PersistStandalone() {
		return Trace{Version: r.trace.Version, Mode: r.trace.Mode}
	}
	r.materializePendingLocalLocked()
	return cloneTrace(r.trace)
}

func (r *TraceRecorder) Reset() {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.trace = Trace{Version: SchedulingTraceVersion, Mode: TraceModeExecution}
	r.workerRefCount = 0
	r.pendingLocal = pendingLocalAttempt{}
	r.mu.Unlock()
}

func (r *TraceRecorder) SetMode(mode TraceMode) {
	if r == nil {
		return
	}
	if mode != TraceModePreview {
		mode = TraceModeExecution
	}
	r.mu.Lock()
	r.ensureVersionLocked()
	r.trace.Mode = mode
	r.mu.Unlock()
}

func (t Trace) Empty() bool {
	if t.Truncated {
		return false
	}
	for i := range t.Attempts {
		attempt := &t.Attempts[i]
		if attempt.Query != nil || attempt.ScanCount > 0 || attempt.StageCount > 0 || attempt.FailureCount > 0 {
			return false
		}
	}
	return true
}

// PersistStandalone reports whether a short statement should persist a
// schedule-only record. Normal local TP/AP-one-CN decisions stay in metrics to
// avoid amplifying statement_info storage on high-QPS workloads.
func (t Trace) PersistStandalone() bool {
	if t.Empty() {
		return false
	}
	if t.Truncated || t.AttemptCount > 1 {
		return true
	}
	for i := range t.Attempts {
		attempt := &t.Attempts[i]
		if attempt.Truncated || attempt.FailureCount > 0 {
			return true
		}
		if attempt.Query == nil {
			continue
		}
		query := attempt.Query
		if query.ExecKind == QueryExecAPMultiCN.String() || !query.Satisfied || query.Fallback || query.DroppedCount > 0 {
			return true
		}
	}
	return false
}

// Clone returns an ownership-independent copy suitable for asynchronous
// observability handoff.
func (t Trace) Clone() Trace {
	return cloneTrace(t)
}

func (r *TraceRecorder) ensureVersionLocked() {
	if r.trace.Version == 0 {
		r.trace.Version = SchedulingTraceVersion
	}
	if r.trace.Mode == "" {
		r.trace.Mode = TraceModeExecution
	}
}

func (r *TraceRecorder) attemptLocked(id TraceAttemptID) *AttemptTrace {
	if r.pendingLocal.sequence == id {
		r.materializePendingLocalLocked()
	}
	idx := int(id) - 1
	if idx < 0 || idx >= len(r.trace.Attempts) {
		return nil
	}
	return &r.trace.Attempts[idx]
}

func canDeferLocalQuery(decision QueryDecision) bool {
	return (decision.ExecKind == QueryExecTP || decision.ExecKind == QueryExecAPOneCN) &&
		decision.Satisfied &&
		decision.Reason != ReasonNoCandidateCN &&
		len(decision.Dropped) == 0
}

func (r *TraceRecorder) omitPendingLocalPlacementDetailsLocked(attempt TraceAttemptID) bool {
	if r.pendingLocal.sequence != attempt || !r.pendingLocal.hasQuery {
		return false
	}
	r.pendingLocal.detailsOmitted = true
	return true
}

func (r *TraceRecorder) materializePendingLocalLocked() {
	pending := r.pendingLocal
	if pending.sequence == 0 {
		return
	}
	attempt := AttemptTrace{
		Sequence:       int(pending.sequence),
		DetailsOmitted: pending.detailsOmitted,
	}
	if pending.hasQuery {
		query := pending.query
		attempt.Query = &QueryTrace{
			ExecKind:        query.execKind.String(),
			CurrentCNPolicy: query.currentCNPolicy.String(),
			CurrentCN: WorkerTrace{
				ID:       boundedTraceWorkerValue(query.currentCN.id),
				Routable: query.currentCN.routable,
				Mcpu:     query.currentCN.mcpu,
				State:    query.currentCN.state.String(),
			},
			Reason:          query.reason,
			Satisfied:       true,
			CandidateSource: string(query.candidateResolution.DiscoverySource),
			PoolResolution:  string(query.candidateResolution.PoolResolution),
			DiscoveredCount: max(query.candidateResolution.DiscoveredCount, 0),
			ResolvedCount:   query.resolvedCandidateCount,
			SelectedCount:   query.selectedCount,
			SelectedOmitted: query.selectedCount > 0,
		}
	}
	r.trace.Attempts = append(r.trace.Attempts, attempt)
	r.pendingLocal = pendingLocalAttempt{}
}

func omitLocalPlacementDetails(attempt *AttemptTrace) bool {
	return attempt.Query != nil && attempt.Query.ExecKind != QueryExecAPMultiCN.String()
}

func traceWorker(worker Worker) WorkerTrace {
	return WorkerTrace{
		ID:       boundedTraceWorkerValue(worker.ID),
		Routable: worker.Addr != "",
		Mcpu:     worker.Mcpu,
		State:    worker.State.String(),
	}
}

func boundedTraceWorkerValue(value string) string {
	if len(value) > maxTraceWorkerValueBytes {
		value = value[:maxTraceWorkerValueBytes]
	}
	return strings.Clone(value)
}

func (r *TraceRecorder) traceWorkersLocked(workers Workers) ([]WorkerTrace, bool) {
	if len(workers) == 0 {
		return nil, false
	}
	remaining := max(maxTraceWorkerRefs-r.workerRefCount, 0)
	limit := min(len(workers), maxTraceWorkersPerDecision, remaining)
	result := make([]WorkerTrace, 0, limit)
	for i := 0; i < limit; i++ {
		result = append(result, traceWorker(workers[i]))
	}
	r.workerRefCount += limit
	return result, len(workers) > limit
}

func traceDroppedReasons(dropped DroppedWorkers) ([]ReasonCount, bool) {
	if len(dropped) == 0 {
		return nil, false
	}
	counts := make(map[string]int, min(len(dropped), maxTraceReasonCounts))
	truncated := false
	for _, worker := range dropped {
		if _, ok := counts[worker.Reason]; ok {
			counts[worker.Reason]++
			continue
		}
		if len(counts) >= maxTraceReasonCounts {
			truncated = true
			continue
		}
		counts[worker.Reason] = 1
	}
	reasons := make([]string, 0, len(counts))
	for reason := range counts {
		reasons = append(reasons, reason)
	}
	sort.Strings(reasons)
	result := make([]ReasonCount, 0, len(reasons))
	for _, reason := range reasons {
		result = append(result, ReasonCount{Reason: reason, Count: counts[reason]})
	}
	return result, truncated
}

func cloneTrace(trace Trace) Trace {
	cloned := trace
	cloned.Attempts = append([]AttemptTrace(nil), trace.Attempts...)
	for i := range cloned.Attempts {
		src := &trace.Attempts[i]
		dst := &cloned.Attempts[i]
		if src.Query != nil {
			query := *src.Query
			query.Selected = append([]WorkerTrace(nil), src.Query.Selected...)
			query.Dropped = append([]ReasonCount(nil), src.Query.Dropped...)
			dst.Query = &query
		}
		dst.Scans = append([]ScanTrace(nil), src.Scans...)
		for j := range dst.Scans {
			dst.Scans[j].Selected = append([]WorkerTrace(nil), src.Scans[j].Selected...)
		}
		dst.Stages = append([]StageTrace(nil), src.Stages...)
		for j := range dst.Stages {
			dst.Stages[j].Selected = append([]WorkerTrace(nil), src.Stages[j].Selected...)
		}
		dst.Failures = append([]FailureTrace(nil), src.Failures...)
		for j := range dst.Failures {
			if src.Failures[j].Worker != nil {
				worker := *src.Failures[j].Worker
				dst.Failures[j].Worker = &worker
			}
		}
	}
	return cloned
}
