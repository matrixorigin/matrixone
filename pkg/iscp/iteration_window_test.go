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

package iscp

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestIndexCandidatesOwnTheirRanges(t *testing.T) {
	tests := []struct {
		name string
		spec TriggerSpec
	}{
		{"default", TriggerSpec{JobType: TriggerType_Default}},
		{"timed shared", TriggerSpec{JobType: TriggerType_Timed, Schedule: Schedule{Interval: time.Nanosecond, Share: true}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			table := NewTableEntry(newRuntimeTestExecutor(), 1, 2, 3, "db", "table")
			indexFrom := types.BuildTS(1, 0)
			consumerFrom := types.BuildTS(10, 0)
			for i, name := range []string{"index_a", "index_b", "consumer_a", "consumer_b"} {
				consumerType := ConsumerType_CNConsumer
				watermark := types.BuildTS(10, 0)
				if i < 2 {
					consumerType = ConsumerType_IndexSync
					watermark = types.BuildTS(int64(i+1), 0)
				}
				_, err := table.AddOrUpdateSinker(context.Background(), name,
					&JobSpec{ConsumerInfo: ConsumerInfo{ConsumerType: int8(consumerType)}, TriggerSpec: tc.spec},
					&JobStatus{Stage: JobStage_Running}, uint64(i+1), watermark, ISCPJobState_Completed, 0)
				require.NoError(t, err)
			}

			iters, minFrom := table.getCandidate()
			require.Len(t, iters, 3)
			require.Equal(t, indexFrom.Next(), minFrom)
			for _, iter := range iters {
				switch iter.jobNames[0] {
				case "index_a", "index_b":
					require.True(t, iter.indexJob)
					require.Len(t, iter.jobNames, 1)
					require.Equal(t, types.MaxTs(), iter.toTS)
				case "consumer_a", "consumer_b":
					require.False(t, iter.indexJob)
					require.ElementsMatch(t, []string{"consumer_a", "consumer_b"}, iter.jobNames)
					require.Equal(t, consumerFrom.Next(), iter.fromTS)
				default:
					t.Fatalf("unexpected iteration: %v", iter.jobNames)
				}
			}
		})
	}
}

func TestSharedCandidateCannotJoinIndexIteration(t *testing.T) {
	table := NewTableEntry(newRuntimeTestExecutor(), 1, 2, 3, "db", "table")
	watermark := types.BuildTS(10, 0)
	for i, consumerType := range []ConsumerType{ConsumerType_IndexSync, ConsumerType_CNConsumer} {
		name := []string{"index", "consumer"}[i]
		_, err := table.AddOrUpdateSinker(context.Background(), name,
			&JobSpec{ConsumerInfo: ConsumerInfo{ConsumerType: int8(consumerType)},
				TriggerSpec: TriggerSpec{JobType: TriggerType_Default}},
			&JobStatus{Stage: JobStage_Running}, uint64(i+1), watermark, ISCPJobState_Completed, 0)
		require.NoError(t, err)
	}
	iters, _ := table.getCandidate()
	require.Len(t, iters, 2)
	for _, iter := range iters {
		require.Len(t, iter.jobNames, 1)
	}
}

func TestIndexCandidatesShareOnlyEqualBudgets(t *testing.T) {
	table := NewTableEntry(newRuntimeTestExecutor(), 1, 2, 3, "db", "table")
	watermark := types.BuildTS(10, 0)
	for i, target := range []int{0, 100_000, 100_000, 200_000} {
		name := []string{"index_a", "index_b", "index_c", "index_d"}[i]
		_, err := table.AddOrUpdateSinker(context.Background(), name,
			&JobSpec{ConsumerInfo: ConsumerInfo{ConsumerType: int8(ConsumerType_IndexSync)},
				TriggerSpec: TriggerSpec{JobType: TriggerType_AlwaysUpdate, TargetRows: target}},
			&JobStatus{Stage: JobStage_Running}, uint64(i+1), watermark, ISCPJobState_Completed, 0)
		require.NoError(t, err)
	}
	iters, _ := table.getCandidate()
	require.Len(t, iters, 3)
	for _, iter := range iters {
		switch len(iter.jobNames) {
		case 1:
			require.Contains(t, []string{"index_a", "index_d"}, iter.jobNames[0])
		case 2:
			require.ElementsMatch(t, []string{"index_b", "index_c"}, iter.jobNames)
		default:
			t.Fatalf("unexpected index grouping: %v", iter.jobNames)
		}
	}
}

func TestLimitIterationTimeSpan(t *testing.T) {
	from := types.BuildTS(time.Hour.Nanoseconds(), 5)
	to := types.BuildTS(from.Physical()+2*DefaultMaxChangeInterval.Nanoseconds(), 0)
	iter := &IterationContext{indexJob: true, fromTS: from, toTS: to}
	limitIterationTimeSpan(iter)
	want := types.BuildTS(from.Physical()+DefaultMaxChangeInterval.Nanoseconds(), 0)
	require.Equal(t, want, iter.toTS)

	// The next range starts just after the persisted bound and is bounded too.
	next := &IterationContext{indexJob: true, fromTS: iter.toTS.Next(), toTS: to}
	limitIterationTimeSpan(next)
	require.Equal(t, types.BuildTS(next.fromTS.Physical()+DefaultMaxChangeInterval.Nanoseconds(), 0), next.toTS)

	within := &IterationContext{indexJob: true, fromTS: from, toTS: types.BuildTS(from.Physical()+time.Minute.Nanoseconds(), 0)}
	limitIterationTimeSpan(within)
	require.Equal(t, types.BuildTS(from.Physical()+time.Minute.Nanoseconds(), 0), within.toTS)

	ordinary := &IterationContext{fromTS: from, toTS: to}
	limitIterationTimeSpan(ordinary)
	require.Equal(t, want, ordinary.toTS)
}

func TestAdmittedIterationKeepsItsRowBudget(t *testing.T) {
	for _, tc := range []struct {
		consumer ConsumerType
		target   int
	}{
		{ConsumerType_IndexSync, 0},
		{ConsumerType_IndexSync, 100},
		{ConsumerType_CNConsumer, 0},
	} {
		target := tc.target
		ctx := context.Background()
		table := NewTableEntry(newRuntimeTestExecutor(), 1, 2, 3, "db", "table")
		watermark := types.BuildTS(10, 0)
		spec := JobSpec{ConsumerInfo: ConsumerInfo{ConsumerType: int8(tc.consumer)},
			TriggerSpec: TriggerSpec{JobType: TriggerType_Default, TargetRows: target}}
		names := []string{"first", "second"}
		for i, name := range names {
			_, err := table.AddOrUpdateSinker(ctx, name, &spec, &JobStatus{Stage: JobStage_Running},
				uint64(i+1), watermark, ISCPJobState_Completed, 0)
			require.NoError(t, err)
		}
		iters, _ := table.getCandidate()
		require.Len(t, iters, 1)
		admitted := iters[0]
		require.Len(t, admitted.jobIDs, 2)
		require.NoError(t, table.markIterationPending(admitted))

		changed := spec
		changed.TargetRows = target + 1
		changed.JobType = TriggerType_AlwaysUpdate
		_, err := table.AddOrUpdateSinker(ctx, "second", &changed, &JobStatus{Stage: JobStage_Running},
			2, watermark, ISCPJobState_Completed, 0)
		require.NoError(t, err)
		targetInUse, err := admitted.rowBudget([]*JobSpec{&spec, &changed})
		require.NoError(t, err)
		require.Equal(t, target, targetInUse)

		applied := types.BuildTS(11, 0)
		for i, nextSpec := range []*JobSpec{&spec, &changed} {
			_, err := table.AddOrUpdateSinker(ctx, names[i], nextSpec, &JobStatus{LSN: 1, Stage: JobStage_Running},
				uint64(i+1), applied, ISCPJobState_Completed, 0)
			require.NoError(t, err)
		}
		iters, _ = table.getCandidate()
		require.Len(t, iters, 2)
		got := make(map[string]int, len(iters))
		for _, iter := range iters {
			require.Len(t, iter.jobNames, 1)
			require.Equal(t, applied.Next(), iter.fromTS)
			got[iter.jobNames[0]] = iter.budget.target
		}
		require.Equal(t, map[string]int{"first": target, "second": target + 1}, got)
	}
}

func TestIterationStatusReplayKeepsCleanTableProgress(t *testing.T) {
	ctx := context.Background()
	table := NewTableEntry(newRuntimeTestExecutor(), 1, 2, 3, "db", "table")
	spec := &JobSpec{TriggerSpec: TriggerSpec{JobType: TriggerType_Default}}
	persisted := types.BuildTS(10, 0)
	cleanThrough := types.BuildTS(15, 0)
	_, err := table.AddOrUpdateSinker(ctx, "job", spec, &JobStatus{Stage: JobStage_Running}, 1,
		persisted, ISCPJobState_Completed, 0)
	require.NoError(t, err)
	job := table.jobs[JobKey{JobName: "job", JobID: 1}]
	require.NoError(t, job.UpdateWatermark(persisted.Next(), cleanThrough, 0))
	iters, _ := table.getCandidate()
	require.Len(t, iters, 1)
	require.NoError(t, table.markIterationPending(iters[0]))

	for _, state := range []int8{ISCPJobState_Running, ISCPJobState_Completed} {
		_, err = table.AddOrUpdateSinker(ctx, "job", spec,
			&JobStatus{LSN: 1, Stage: JobStage_Running}, 1, persisted, state, 0)
		require.NoError(t, err)
		require.Equal(t, state, job.state)
		require.Equal(t, persisted, job.persistedWatermark)
		require.Equal(t, cleanThrough, job.watermark)
	}
	iters, _ = table.getCandidate()
	require.Len(t, iters, 1)
	require.Equal(t, cleanThrough.Next(), iters[0].fromTS, "retry starts at the first unconsumed timestamp")
}
