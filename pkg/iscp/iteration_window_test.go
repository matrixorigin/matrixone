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

func TestLimitIndexIterationTimeSpan(t *testing.T) {
	from := types.BuildTS(time.Hour.Nanoseconds(), 5)
	to := types.BuildTS(from.Physical()+2*DefaultMaxChangeInterval.Nanoseconds(), 0)
	iter := &IterationContext{indexJob: true, fromTS: from, toTS: to}
	limitIndexIterationTimeSpan(iter)
	want := types.BuildTS(from.Physical()+DefaultMaxChangeInterval.Nanoseconds(), 0)
	require.Equal(t, want, iter.toTS)

	// The next range starts just after the persisted bound and is bounded too.
	next := &IterationContext{indexJob: true, fromTS: iter.toTS.Next(), toTS: to}
	limitIndexIterationTimeSpan(next)
	require.Equal(t, types.BuildTS(next.fromTS.Physical()+DefaultMaxChangeInterval.Nanoseconds(), 0), next.toTS)

	within := &IterationContext{indexJob: true, fromTS: from, toTS: types.BuildTS(from.Physical()+time.Minute.Nanoseconds(), 0)}
	limitIndexIterationTimeSpan(within)
	require.Equal(t, types.BuildTS(from.Physical()+time.Minute.Nanoseconds(), 0), within.toTS)

	ordinary := &IterationContext{fromTS: from, toTS: to}
	limitIndexIterationTimeSpan(ordinary)
	require.Equal(t, to, ordinary.toTS)
}
