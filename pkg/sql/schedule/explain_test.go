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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExplainLinesRendersCandidateBoundaryAndDecision(t *testing.T) {
	recorder := new(TraceRecorder)
	recorder.SetMode(TraceModePreview)
	attempt := recorder.StartAttempt()
	recorder.RecordQuery(attempt, QueryDecision{
		ExecKind:  QueryExecAPMultiCN,
		CurrentCN: Worker{ID: "local", Addr: "local:6001", Mcpu: 4, State: WorkerStateWorking},
		Workers: Workers{
			{ID: "cn-a", Addr: "a:6001", Mcpu: 4, State: WorkerStateWorking},
			{ID: "cn-b", Addr: "b:6001", Mcpu: 8, State: WorkerStateWorking},
		},
		Dropped: DroppedWorkers{{Reason: ReasonDroppedDrainingCN}},
		Reason:  ReasonMultiCN,
		CandidateResolution: CandidateResolution{
			DiscoverySource: CandidateSourceEngineNodes,
			PoolResolution:  PoolResolutionLegacyEngineNodes,
			DiscoveredCount: 3,
		},
		ResolvedCandidateCount: 3,
		EligibleCount:          2,
		Intent: SchedulingIntent{
			Explicit:          true,
			RequestedPool:     "tenant-label:account=app",
			EmptyWorkerPolicy: EmptyWorkerFail,
			WorkerSet:         WorkerSetPolicy{Mode: WorkerSetMax, MaxWorkers: 2, AlgorithmVersion: WorkerSelectionAlgorithmV1},
		},
		ResolvedPool:    ResolvedPoolDecision{RequestedIdentity: "tenant-label:account=app", Identity: "shared-unlabeled", Resolution: PoolResolution("shared-unlabeled"), Fallback: true},
		CurrentCNPolicy: CurrentCNAllowed,
		Satisfied:       true,
	})

	lines := ExplainLines(recorder.Snapshot())
	output := strings.Join(lines, "\n")
	require.Contains(t, output, "Scheduling (preview):")
	require.Contains(t, output, "source=engine-nodes")
	require.Contains(t, output, "pool-resolution=legacy-engine-nodes")
	require.Contains(t, output, "discovered=3 resolved=3 selected=2 dropped=1")
	require.Contains(t, output, "requested-pool=tenant-label:account=app")
	require.Contains(t, output, "resolved-pool=shared-unlabeled")
	require.Contains(t, output, "resolved-pool-resolution=shared-unlabeled")
	require.Contains(t, output, "worker-set=max-workers max-workers=2")
	require.Contains(t, output, "id=cn-a")
	require.NotContains(t, output, "a:6001")
	require.Contains(t, output, "route=available")
	require.Contains(t, output, "draining-cn=1")
}

func TestExplainLinesIsIndependentlyBounded(t *testing.T) {
	trace := Trace{
		Version:      SchedulingTraceVersion,
		Mode:         TraceModeExecution,
		AttemptCount: 1,
		Attempts: []AttemptTrace{{
			Sequence: 1,
			Query: &QueryTrace{
				ExecKind:        QueryExecAPMultiCN.String(),
				CurrentCNPolicy: CurrentCNAllowed.String(),
				Reason:          ReasonMultiCN,
				Satisfied:       true,
			},
			Stages: make([]StageTrace, maxSchedulingExplainLines*2),
		}},
	}
	for i := range trace.Attempts[0].Stages {
		trace.Attempts[0].Stages[i] = StageTrace{
			Kind:   StageKindQueryWorkerSet,
			Reason: strings.Repeat("x", maxSchedulingExplainValue*2),
		}
	}

	lines := ExplainLines(trace)
	require.LessOrEqual(t, len(lines), maxSchedulingExplainLines)
	require.Equal(t, "  Scheduling explain output truncated", lines[len(lines)-1])
	for _, line := range lines {
		require.NotContains(t, line, strings.Repeat("x", maxSchedulingExplainValue+1))
	}
}

func TestExplainLinesHandlesEmptyAndFailureOnlyTrace(t *testing.T) {
	require.Nil(t, ExplainLines(Trace{}))

	recorder := new(TraceRecorder)
	attempt := recorder.StartAttempt()
	recorder.RecordFailure(attempt, "candidate-discovery", Worker{})

	lines := ExplainLines(recorder.Snapshot())
	require.Contains(t, strings.Join(lines, "\n"), "Failure: category=candidate-discovery")
}

func TestExplainLinesSanitizesControlCharacters(t *testing.T) {
	recorder := new(TraceRecorder)
	attempt := recorder.StartAttempt()
	recorder.RecordFailure(attempt, "bad\ncategory", Worker{ID: "worker\tid"})

	output := strings.Join(ExplainLines(recorder.Snapshot()), "\n")
	require.NotContains(t, output, "bad\ncategory")
	require.NotContains(t, output, "worker\tid")
	require.Contains(t, output, "bad?category")
	require.Contains(t, output, "worker?id")
}
