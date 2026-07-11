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
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTraceRecorderCapturesSchedulingDecisionChain(t *testing.T) {
	recorder := new(TraceRecorder)
	attempt := recorder.StartAttempt()
	current := Worker{ID: "cn-local", Addr: "local:6001", Mcpu: 4, State: WorkerStateWorking}
	selected := Workers{
		current,
		{ID: "cn-remote", Addr: "remote:6001", Mcpu: 8, State: WorkerStateWorking},
	}
	recorder.RecordQuery(attempt, QueryDecision{
		ExecKind:  QueryExecAPMultiCN,
		CurrentCN: current,
		Workers:   selected,
		Dropped:   DroppedWorkers{{Reason: ReasonDroppedDrainingCN}, {Reason: ReasonDroppedDrainingCN}},
		Reason:    ReasonRequiredCurrentCN,
		CandidateResolution: CandidateResolution{
			DiscoverySource: CandidateSourceEngineNodes,
			PoolResolution:  PoolResolutionLegacyEngineNodes,
			DiscoveredCount: 4,
		},
		ResolvedCandidateCount: 3,
		CurrentCNPolicy:        CurrentCNRequired,
		Satisfied:              true,
	})
	recorder.RecordScan(attempt, ScanRequest{
		QueryWorkers: selected,
		Stats:        &ScanStats{BlockNum: 128, Dop: 8},
		ForceMultiCN: true,
	}, ScanDecision{Workers: selected, Reason: ReasonScanMultiCN})
	recorder.RecordSingleWorkerStage(attempt, selected, StageDecision{
		Worker: current,
		Reason: ReasonStageCurrentCoordinator,
	})
	recorder.RecordStage(attempt, StageKindQueryWorkerSet, selected, StageWorkerSetDecision{
		Workers: selected,
		Reason:  ReasonStageQueryWorkers,
	})
	recorder.RecordFailure(attempt, "runtime-ineligible-selected-worker", selected[1])

	trace := recorder.Snapshot()
	require.Equal(t, SchedulingTraceVersion, trace.Version)
	require.Equal(t, 1, trace.AttemptCount)
	require.Len(t, trace.Attempts, 1)
	require.Equal(t, "ap-multi-cn", trace.Attempts[0].Query.ExecKind)
	require.Equal(t, "required", trace.Attempts[0].Query.CurrentCNPolicy)
	require.Equal(t, string(CandidateSourceEngineNodes), trace.Attempts[0].Query.CandidateSource)
	require.Equal(t, string(PoolResolutionLegacyEngineNodes), trace.Attempts[0].Query.PoolResolution)
	require.Equal(t, 4, trace.Attempts[0].Query.DiscoveredCount)
	require.Equal(t, 3, trace.Attempts[0].Query.ResolvedCount)
	require.Equal(t, []ReasonCount{{Reason: ReasonDroppedDrainingCN, Count: 2}}, trace.Attempts[0].Query.Dropped)
	require.Equal(t, int32(128), trace.Attempts[0].Scans[0].BlockCount)
	require.Equal(t, int32(8), trace.Attempts[0].Scans[0].DOP)
	require.Equal(t, StageKindSingleWorker, trace.Attempts[0].Stages[0].Kind)
	require.Equal(t, StageKindQueryWorkerSet, trace.Attempts[0].Stages[1].Kind)
	require.Equal(t, "runtime-ineligible-selected-worker", trace.Attempts[0].Failures[0].Category)
	require.True(t, trace.Attempts[0].Query.CurrentCN.Routable)
	require.True(t, trace.Attempts[0].Query.Selected[1].Routable)
	require.True(t, trace.PersistStandalone())
}

func TestTraceRecorderBoundsEveryAccumulation(t *testing.T) {
	recorder := new(TraceRecorder)
	workers := make(Workers, maxTraceWorkersPerDecision+5)
	for i := range workers {
		workers[i] = Worker{ID: "worker", Addr: "worker:6001", Mcpu: i + 1}
	}
	dropped := make(DroppedWorkers, maxTraceReasonCounts+5)
	for i := range dropped {
		dropped[i] = DroppedWorker{Reason: string(rune('a' + i))}
	}

	first := recorder.StartAttempt()
	recorder.RecordQuery(first, QueryDecision{
		ExecKind:  QueryExecAPMultiCN,
		CurrentCN: Worker{ID: "current"},
		Workers:   workers,
		Dropped:   dropped,
		Reason:    ReasonMultiCN,
		Satisfied: true,
		CandidateResolution: CandidateResolution{
			DiscoverySource: CandidateSourceEngineNodes,
			PoolResolution:  PoolResolutionLegacyEngineNodes,
			DiscoveredCount: len(workers),
		},
		ResolvedCandidateCount: len(workers),
	})
	for i := 0; i < maxTraceScans+5; i++ {
		recorder.RecordScan(first, ScanRequest{QueryWorkers: workers}, ScanDecision{Workers: workers, Reason: ReasonScanMultiCN})
	}
	for i := 0; i < maxTraceStages+5; i++ {
		recorder.RecordStage(first, StageKindQueryWorkerSet, workers, StageWorkerSetDecision{Workers: workers, Reason: ReasonStageQueryWorkers})
	}
	for i := 0; i < maxTraceFailures+5; i++ {
		recorder.RecordFailure(first, "failure", workers[0])
	}
	require.True(t, recorder.Snapshot().Truncated)
	for i := 1; i < maxTraceAttempts+5; i++ {
		recorder.StartAttempt()
	}

	trace := recorder.Snapshot()
	require.Equal(t, maxTraceAttempts+5, trace.AttemptCount)
	require.Len(t, trace.Attempts, maxTraceAttempts)
	require.True(t, trace.Truncated)
	require.Len(t, trace.Attempts[0].Query.Selected, maxTraceWorkersPerDecision)
	require.True(t, trace.Attempts[0].Query.SelectedTruncated)
	require.Equal(t, len(dropped), trace.Attempts[0].Query.DroppedCount)
	require.Len(t, trace.Attempts[0].Query.Dropped, maxTraceReasonCounts)
	require.True(t, trace.Attempts[0].Query.DroppedTruncated)
	require.Equal(t, maxTraceScans+5, trace.Attempts[0].ScanCount)
	require.Len(t, trace.Attempts[0].Scans, maxTraceScans)
	require.Equal(t, maxTraceStages+5, trace.Attempts[0].StageCount)
	require.Len(t, trace.Attempts[0].Stages, maxTraceStages)
	require.Equal(t, maxTraceFailures+5, trace.Attempts[0].FailureCount)
	require.Len(t, trace.Attempts[0].Failures, maxTraceFailures)
	require.True(t, trace.Attempts[0].Truncated)
	require.LessOrEqual(t, countTraceWorkerRefs(trace), maxTraceWorkerRefs)
}

func TestTraceRecorderBoundsWorkerIDAndRedactsAddress(t *testing.T) {
	recorder := new(TraceRecorder)
	attempt := recorder.StartAttempt()
	id := strings.Repeat("i", maxTraceWorkerValueBytes*4)
	addr := "private-pipeline-address:6001"
	recorder.RecordQuery(attempt, QueryDecision{
		ExecKind: QueryExecAPMultiCN,
		Workers: Workers{{
			ID:   id,
			Addr: addr,
		}},
		Satisfied: true,
	})

	trace := recorder.Snapshot()
	worker := trace.Attempts[0].Query.Selected[0]
	require.Len(t, worker.ID, maxTraceWorkerValueBytes)
	require.True(t, worker.Routable)
	payload, err := json.Marshal(trace)
	require.NoError(t, err)
	require.NotContains(t, string(payload), addr)
	require.NotContains(t, string(payload), `"addr"`)
}

func countTraceWorkerRefs(trace Trace) int {
	count := 0
	for _, attempt := range trace.Attempts {
		if attempt.Query != nil {
			count += len(attempt.Query.Selected)
		}
		for _, scan := range attempt.Scans {
			count += len(scan.Selected)
		}
		for _, stage := range attempt.Stages {
			count += len(stage.Selected)
		}
	}
	return count
}

func TestTraceRecorderSnapshotIsIndependent(t *testing.T) {
	recorder := new(TraceRecorder)
	attempt := recorder.StartAttempt()
	recorder.RecordQuery(attempt, QueryDecision{
		ExecKind:  QueryExecAPMultiCN,
		CurrentCN: Worker{ID: "current"},
		Workers:   Workers{{ID: "selected"}},
		Dropped:   DroppedWorkers{{Reason: "drop"}},
		Reason:    ReasonMultiCN,
		CandidateResolution: CandidateResolution{
			DiscoveredCount: 1,
		},
		ResolvedCandidateCount: 1,
	})
	recorder.RecordScan(attempt, ScanRequest{}, ScanDecision{Workers: Workers{{ID: "scan"}}})
	recorder.RecordStage(attempt, StageKindQueryWorkerSet, nil, StageWorkerSetDecision{Workers: Workers{{ID: "stage"}}})
	recorder.RecordFailure(attempt, "failure", Worker{ID: "failure"})

	first := recorder.Snapshot()
	first.Attempts[0].Query.Selected[0].ID = "changed"
	first.Attempts[0].Query.Dropped[0].Reason = "changed"
	first.Attempts[0].Scans[0].Selected[0].ID = "changed"
	first.Attempts[0].Stages[0].Selected[0].ID = "changed"
	first.Attempts[0].Failures[0].Worker.ID = "changed"

	second := recorder.Snapshot()
	require.Equal(t, "selected", second.Attempts[0].Query.Selected[0].ID)
	require.Equal(t, "drop", second.Attempts[0].Query.Dropped[0].Reason)
	require.Equal(t, "scan", second.Attempts[0].Scans[0].Selected[0].ID)
	require.Equal(t, "stage", second.Attempts[0].Stages[0].Selected[0].ID)
	require.Equal(t, "failure", second.Attempts[0].Failures[0].Worker.ID)
}

func TestTraceRecorderKeepsOneQueryDecisionPerAttempt(t *testing.T) {
	recorder := new(TraceRecorder)
	attempt := recorder.StartAttempt()
	recorder.RecordQuery(attempt, QueryDecision{
		ExecKind:  QueryExecAPMultiCN,
		CurrentCN: Worker{ID: "current"},
		Workers:   Workers{{ID: "first"}},
		Reason:    ReasonMultiCN,
		Satisfied: true,
		CandidateResolution: CandidateResolution{
			DiscoveredCount: 1,
		},
		ResolvedCandidateCount: 1,
	})
	recorder.RecordQuery(attempt, QueryDecision{
		ExecKind:  QueryExecAPMultiCN,
		CurrentCN: Worker{ID: "current"},
		Workers:   Workers{{ID: "second"}},
		Reason:    ReasonNoCandidateCN,
		Satisfied: true,
		CandidateResolution: CandidateResolution{
			DiscoveredCount: 2,
		},
		ResolvedCandidateCount: 2,
	})

	trace := recorder.Snapshot()
	require.Equal(t, ReasonMultiCN, trace.Attempts[0].Query.Reason)
	require.Equal(t, "first", trace.Attempts[0].Query.Selected[0].ID)
	require.Equal(t, 1, trace.Attempts[0].Query.DiscoveredCount)
}

func TestTraceRecorderPersistencePolicyAvoidsNormalLocalAmplification(t *testing.T) {
	recorder := new(TraceRecorder)
	attempt := recorder.StartAttempt()
	recorder.RecordQuery(attempt, QueryDecision{
		ExecKind:  QueryExecTP,
		CurrentCN: Worker{ID: "current"},
		Workers:   Workers{{ID: "current"}},
		Reason:    ReasonLocalExecType,
		CandidateResolution: CandidateResolution{
			DiscoverySource: CandidateSourceNotRequired,
			PoolResolution:  PoolResolutionNotRequired,
		},
		CurrentCNPolicy: CurrentCNAllowed,
		Satisfied:       true,
	})
	recorder.RecordScan(attempt, ScanRequest{}, ScanDecision{Workers: Workers{{ID: "current"}}})
	recorder.RecordStage(attempt, StageKindQueryWorkerSet, nil, StageWorkerSetDecision{Workers: Workers{{ID: "current"}}})
	trace := recorder.Snapshot()
	require.False(t, trace.PersistStandalone())
	require.Empty(t, trace.Attempts[0].Query.Selected)
	require.True(t, trace.Attempts[0].Query.SelectedOmitted)
	require.Zero(t, trace.Attempts[0].ScanCount)
	require.Zero(t, trace.Attempts[0].StageCount)
	require.True(t, trace.Attempts[0].DetailsOmitted)

	recorder.RecordFailure(attempt, "validation", Worker{ID: "current"})
	require.True(t, recorder.Snapshot().PersistStandalone())
}

func TestTraceRecorderDefersNormalLocalTraceUntilRequested(t *testing.T) {
	recorder := new(TraceRecorder)
	attempt := recorder.StartAttempt()
	decision := normalLocalTraceDecision()
	recorder.RecordQuery(attempt, decision)
	recorder.RecordScan(attempt, ScanRequest{}, ScanDecision{Workers: decision.Workers})
	recorder.RecordStage(attempt, StageKindQueryWorkerSet, decision.Workers, StageWorkerSetDecision{Workers: decision.Workers})

	require.True(t, recorder.SnapshotForExport(false).Empty())

	trace := recorder.SnapshotForExport(true)
	require.Len(t, trace.Attempts, 1)
	require.Equal(t, QueryExecTP.String(), trace.Attempts[0].Query.ExecKind)
	require.Equal(t, "current", trace.Attempts[0].Query.CurrentCN.ID)
	require.Equal(t, 1, trace.Attempts[0].Query.SelectedCount)
	require.True(t, trace.Attempts[0].Query.SelectedOmitted)
	require.True(t, trace.Attempts[0].DetailsOmitted)
	require.False(t, trace.PersistStandalone())
}

func TestTraceRecorderMaterializesDeferredAttemptOnFailureAndRetry(t *testing.T) {
	recorder := new(TraceRecorder)
	first := recorder.StartAttempt()
	recorder.RecordQuery(first, normalLocalTraceDecision())
	recorder.RecordFailure(first, "validation", Worker{ID: "current"})

	second := recorder.StartAttempt()
	recorder.RecordQuery(second, QueryDecision{
		ExecKind:  QueryExecAPMultiCN,
		Workers:   Workers{{ID: "remote", Addr: "remote:6001"}},
		Reason:    ReasonMultiCN,
		Satisfied: true,
	})

	trace := recorder.SnapshotForExport(false)
	require.Len(t, trace.Attempts, 2)
	require.Equal(t, "validation", trace.Attempts[0].Failures[0].Category)
	require.Equal(t, QueryExecAPMultiCN.String(), trace.Attempts[1].Query.ExecKind)
	require.True(t, trace.PersistStandalone())
}

func TestTraceRecorderExportMatchesStandalonePersistencePolicy(t *testing.T) {
	tests := []struct {
		name     string
		mutate   func(*QueryDecision)
		exported bool
	}{
		{
			name: "normal AP one CN remains deferred",
			mutate: func(decision *QueryDecision) {
				decision.ExecKind = QueryExecAPOneCN
			},
		},
		{
			name: "unsatisfied local decision",
			mutate: func(decision *QueryDecision) {
				decision.Satisfied = false
			},
			exported: true,
		},
		{
			name: "fallback local decision",
			mutate: func(decision *QueryDecision) {
				decision.Reason = ReasonNoCandidateCN
			},
			exported: true,
		},
		{
			name: "decision with dropped workers",
			mutate: func(decision *QueryDecision) {
				decision.Dropped = DroppedWorkers{{Reason: ReasonDroppedDrainingCN}}
			},
			exported: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := new(TraceRecorder)
			decision := normalLocalTraceDecision()
			test.mutate(&decision)
			attempt := recorder.StartAttempt()
			recorder.RecordQuery(attempt, decision)

			trace := recorder.SnapshotForExport(false)
			require.Equal(t, test.exported, !trace.Empty())
			require.Equal(t, test.exported, trace.PersistStandalone())
		})
	}
}

func TestTraceRecorderNormalLocalExportAllocations(t *testing.T) {
	recorder := new(TraceRecorder)
	decision := normalLocalTraceDecision()
	scanDecision := ScanDecision{Workers: decision.Workers}
	stageDecision := StageWorkerSetDecision{Workers: decision.Workers}
	singleWorkerDecision := StageDecision{Worker: decision.CurrentCN, Reason: ReasonStageCurrentCoordinator}
	allocs := testing.AllocsPerRun(1000, func() {
		recorder.Reset()
		attempt := recorder.StartAttempt()
		recorder.RecordQuery(attempt, decision)
		recorder.RecordScan(attempt, ScanRequest{}, scanDecision)
		recorder.RecordSingleWorkerStage(attempt, decision.Workers, singleWorkerDecision)
		recorder.RecordStage(attempt, StageKindQueryWorkerSet, decision.Workers, stageDecision)
		if !recorder.SnapshotForExport(false).Empty() {
			panic("normal local trace unexpectedly exported")
		}
	})
	require.Zero(t, allocs)
}

func BenchmarkTraceRecorderNormalLocalExport(b *testing.B) {
	recorder := new(TraceRecorder)
	decision := normalLocalTraceDecision()
	scanDecision := ScanDecision{Workers: decision.Workers}
	stageDecision := StageWorkerSetDecision{Workers: decision.Workers}
	singleWorkerDecision := StageDecision{Worker: decision.CurrentCN, Reason: ReasonStageCurrentCoordinator}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		recorder.Reset()
		attempt := recorder.StartAttempt()
		recorder.RecordQuery(attempt, decision)
		recorder.RecordScan(attempt, ScanRequest{}, scanDecision)
		recorder.RecordSingleWorkerStage(attempt, decision.Workers, singleWorkerDecision)
		recorder.RecordStage(attempt, StageKindQueryWorkerSet, decision.Workers, stageDecision)
		_ = recorder.SnapshotForExport(false)
	}
}

func normalLocalTraceDecision() QueryDecision {
	return QueryDecision{
		ExecKind:  QueryExecTP,
		CurrentCN: Worker{ID: "current"},
		Workers:   Workers{{ID: "current"}},
		Reason:    ReasonLocalExecType,
		CandidateResolution: CandidateResolution{
			DiscoverySource: CandidateSourceNotRequired,
			PoolResolution:  PoolResolutionNotRequired,
		},
		CurrentCNPolicy: CurrentCNAllowed,
		Satisfied:       true,
	}
}

func TestTraceRecorderHandlesNilInvalidAndConcurrentAccess(t *testing.T) {
	var nilRecorder *TraceRecorder
	require.Zero(t, nilRecorder.StartAttempt())
	require.True(t, nilRecorder.Snapshot().Empty())
	nilRecorder.RecordQuery(1, QueryDecision{})
	nilRecorder.RecordFailure(1, "failure", Worker{})

	recorder := new(TraceRecorder)
	recorder.RecordFailure(99, "ignored", Worker{})
	attempt := recorder.StartAttempt()
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			recorder.RecordFailure(attempt, "failure", Worker{ID: "worker"})
			_ = recorder.Snapshot()
		}()
	}
	wg.Wait()

	trace := recorder.Snapshot()
	require.Equal(t, 32, trace.Attempts[0].FailureCount)
	require.Len(t, trace.Attempts[0].Failures, maxTraceFailures)
	require.True(t, trace.Attempts[0].Truncated)
}

func TestTraceRecorderModeDefaultsAndResetsToExecution(t *testing.T) {
	recorder := new(TraceRecorder)
	require.Equal(t, TraceModeExecution, recorder.Snapshot().Mode)

	recorder.SetMode(TraceModePreview)
	require.Equal(t, TraceModePreview, recorder.Snapshot().Mode)

	recorder.Reset()
	require.Equal(t, TraceModeExecution, recorder.Snapshot().Mode)
	require.Equal(t, TraceModeExecution, (*TraceRecorder)(nil).Snapshot().Mode)
}

func TestTracePersistenceRequiresDataUnlessTraceIsTruncated(t *testing.T) {
	recorder := new(TraceRecorder)
	recorder.StartAttempt()
	recorder.StartAttempt()
	trace := recorder.Snapshot()
	require.True(t, trace.Empty())
	require.False(t, trace.PersistStandalone())

	for i := 2; i < maxTraceAttempts+1; i++ {
		recorder.StartAttempt()
	}
	trace = recorder.Snapshot()
	require.True(t, trace.Truncated)
	require.False(t, trace.Empty())
	require.True(t, trace.PersistStandalone())
}
