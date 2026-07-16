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
	"errors"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func newRuntimeTestExecutor() *ISCPTaskExecutor {
	return &ISCPTaskExecutor{
		fencedJobs:       make(map[JobRuntimeKey]struct{}),
		runningConsumers: make(map[JobRuntimeKey]map[uint64]*RunningJobConsumer),
		cancelJobs:       make(chan JobRuntimeKey, DefaultCancelJobQueueSize),
		cancelingJobs:    make(map[JobRuntimeKey]struct{}),
		option:           fillDefaultOption(nil),
		ctx:              context.Background(),
		tables:           newISCPTableTree(),
	}
}

func encodeRuntimeCancelTestJSON(t *testing.T, value string) []byte {
	t.Helper()
	byteJSON, err := types.ParseStringToByteJson(value)
	require.NoError(t, err)
	encoded, err := types.EncodeJson(byteJSON)
	require.NoError(t, err)
	return encoded
}

func TestGetExecutorRuntimeRequiresExactCN(t *testing.T) {
	exec := newRuntimeTestExecutor()
	RegisterExecutorRuntime("runner-cn", exec)
	defer UnregisterExecutorRuntime("runner-cn", exec)

	found, ok := GetExecutorRuntime("ddl-cn")
	require.False(t, ok)
	require.Nil(t, found)

	found, ok = GetExecutorRuntime("runner-cn")
	require.True(t, ok)
	require.Same(t, exec, found)
}

func TestCancelAndDrainJobConsumerFencesAndWaitsForRunningConsumer(t *testing.T) {
	exec := newRuntimeTestExecutor()
	key := NewJobRuntimeKey(1, 2, "index_idx01", 7)
	consumerCtx, consumerCancel := context.WithCancel(context.Background())
	defer consumerCancel()
	retrieverCanceled := make(chan error, 1)

	h, ok := exec.RegisterRunningConsumer(key, 7, 11, consumerCancel, func(err error) {
		retrieverCanceled <- err
	})
	require.True(t, ok)

	exited := make(chan struct{})
	go func() {
		defer close(exited)
		<-consumerCtx.Done()
		time.Sleep(20 * time.Millisecond)
		exec.UnregisterRunningConsumer(h)
	}()

	start := time.Now()
	err := exec.CancelAndDrainJobConsumer(context.Background(), key.AccountID, key.TableID, key.JobName, key.JobID)
	require.NoError(t, err)
	require.GreaterOrEqual(t, time.Since(start), 20*time.Millisecond)
	require.True(t, exec.IsJobFenced(key))
	require.ErrorContains(t, <-retrieverCanceled, "iscp job consumer canceled")
	<-exited
}

func TestRegisterRunningConsumerRejectsFencedJob(t *testing.T) {
	exec := newRuntimeTestExecutor()
	key := NewJobRuntimeKey(1, 2, "index_idx01", 1)

	require.NoError(t, exec.CancelAndDrainJobConsumer(context.Background(), key.AccountID, key.TableID, key.JobName, key.JobID))
	_, ok := exec.RegisterRunningConsumer(key, 1, 1, func() {}, nil)

	require.False(t, ok)
}

func TestCancelAndDrainJobConsumerOnlyCancelsMatchingJob(t *testing.T) {
	exec := newRuntimeTestExecutor()
	key1 := NewJobRuntimeKey(1, 2, "index_idx01", 1)
	key2 := NewJobRuntimeKey(1, 2, "index_idx02", 2)
	ctx1, cancel1 := context.WithCancel(context.Background())
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	h1, ok := exec.RegisterRunningConsumer(key1, 1, 1, cancel1, nil)
	require.True(t, ok)
	_, ok = exec.RegisterRunningConsumer(key2, 2, 1, cancel2, nil)
	require.True(t, ok)
	go func() {
		<-ctx1.Done()
		exec.UnregisterRunningConsumer(h1)
	}()

	require.NoError(t, exec.CancelAndDrainJobConsumer(context.Background(), key1.AccountID, key1.TableID, key1.JobName, key1.JobID))

	select {
	case <-ctx2.Done():
		t.Fatal("non-matching job was canceled")
	default:
	}
	exec.runtimeMu.Lock()
	_, stillRunning := exec.runningConsumers[key2.consumerGroupKey()][uint64(2)]
	exec.runtimeMu.Unlock()
	require.True(t, stillRunning)
}

func TestFilterFencedIterationRemovesOnlyFencedJobs(t *testing.T) {
	exec := newRuntimeTestExecutor()
	iter := NewIterationContext(
		1,
		2,
		[]string{"index_idx01", "index_idx02"},
		[]uint64{10, 20},
		[]uint64{100, 200},
		types.BuildTS(1, 0),
		types.BuildTS(2, 0),
	)
	require.NoError(t, exec.CancelAndDrainJobConsumer(context.Background(), 1, 2, "index_idx01", 10))

	filtered := exec.filterFencedIteration(iter)

	require.NotNil(t, filtered)
	require.Equal(t, []string{"index_idx02"}, filtered.jobNames)
	require.Equal(t, []uint64{20}, filtered.jobIDs)
	require.Equal(t, []uint64{200}, filtered.lsn)
	require.True(t, filtered.fromTS.EQ(&iter.fromTS))
	require.True(t, filtered.toTS.EQ(&iter.toTS))
}

func TestFilterFencedIterationDropsAllJobs(t *testing.T) {
	exec := newRuntimeTestExecutor()
	iter := NewIterationContext(
		1,
		2,
		[]string{"index_idx01"},
		[]uint64{10},
		[]uint64{100},
		types.BuildTS(1, 0),
		types.BuildTS(2, 0),
	)
	require.NoError(t, exec.CancelAndDrainJobConsumer(context.Background(), 1, 2, "index_idx01", 10))

	require.Nil(t, exec.filterFencedIteration(iter))
}

func TestTableEntryGetCandidateSkipsFencedJob(t *testing.T) {
	exec := newRuntimeTestExecutor()
	table := NewTableEntry(exec, 1, 10, 2, "db", "tbl")
	spec := &JobSpec{TriggerSpec: TriggerSpec{JobType: TriggerType_Default}}
	table.jobs[JobKey{JobName: "index_idx01", JobID: 1}] = NewJobEntry(table, "index_idx01", spec, 1, types.BuildTS(1, 0), ISCPJobState_Completed, 0)
	table.jobs[JobKey{JobName: "index_idx02", JobID: 2}] = NewJobEntry(table, "index_idx02", spec, 2, types.BuildTS(1, 0), ISCPJobState_Completed, 0)
	require.NoError(t, exec.CancelAndDrainJobConsumer(context.Background(), 1, 2, "index_idx01", 1))

	iters, _ := table.getCandidate()

	require.Len(t, iters, 1)
	require.Equal(t, []string{"index_idx02"}, iters[0].jobNames)
}

func TestTableEntryGetCandidateDoesNotSkipRecreatedSameNameJob(t *testing.T) {
	exec := newRuntimeTestExecutor()
	table := NewTableEntry(exec, 1, 10, 2, "db", "tbl")
	spec := &JobSpec{TriggerSpec: TriggerSpec{JobType: TriggerType_Default}}
	table.jobs[JobKey{JobName: "index_idx01", JobID: 1}] = NewJobEntry(table, "index_idx01", spec, 1, types.BuildTS(1, 0), ISCPJobState_Completed, 0)
	table.jobs[JobKey{JobName: "index_idx01", JobID: 2}] = NewJobEntry(table, "index_idx01", spec, 2, types.BuildTS(1, 0), ISCPJobState_Completed, 0)
	require.NoError(t, exec.CancelAndDrainJobConsumer(context.Background(), 1, 2, "index_idx01", 1))

	iters, _ := table.getCandidate()

	require.Len(t, iters, 1)
	require.Equal(t, []uint64{2}, iters[0].jobIDs)
}

func TestCancelAndDrainJobConsumerHonorsCallerContext(t *testing.T) {
	exec := newRuntimeTestExecutor()
	key := NewJobRuntimeKey(1, 2, "index_idx01", 1)
	consumerCancelCalled := make(chan struct{})
	_, ok := exec.RegisterRunningConsumer(key, 1, 1, func() { close(consumerCancelCalled) }, nil)
	require.True(t, ok)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := exec.CancelAndDrainJobConsumer(ctx, key.AccountID, key.TableID, key.JobName, key.JobID)

	require.True(t, errors.Is(err, context.Canceled))
	<-consumerCancelCalled
}

func TestDroppedJobIsQueuedForCancel(t *testing.T) {
	exec := newRuntimeTestExecutor()
	table := NewTableEntry(exec, 1, 10, 2, "db", "tbl")
	exec.setTable(table)
	spec := &JobSpec{
		ConsumerInfo: ConsumerInfo{
			SrcTable: TableInfo{DBID: 10, TableID: 2, DBName: "db", TableName: "tbl"},
		},
		TriggerSpec: TriggerSpec{JobType: TriggerType_Default},
	}
	specJSON, err := MarshalJobSpec(spec)
	require.NoError(t, err)
	statusJSON, err := MarshalJobStatus(&JobStatus{})
	require.NoError(t, err)
	dropAt := types.Timestamp(time.Now().UnixNano())

	err = exec.addOrUpdateJob(
		1,
		2,
		"index_idx01",
		7,
		ISCPJobState_Canceling,
		types.BuildTS(1, 0).ToString(),
		encodeRuntimeCancelTestJSON(t, specJSON),
		encodeRuntimeCancelTestJSON(t, statusJSON),
		dropAt,
		true,
	)
	require.NoError(t, err)

	key := NewJobRuntimeKey(1, 2, "index_idx01", 7)
	require.Eventually(t, func() bool {
		select {
		case got := <-exec.cancelJobs:
			return got == key
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
	exec.runtimeMu.Lock()
	_, queued := exec.cancelingJobs[key]
	exec.runtimeMu.Unlock()
	require.True(t, queued)
}

func TestRecoveredDroppedJobBecomesCanceledWithoutQueue(t *testing.T) {
	exec := newRuntimeTestExecutor()
	table := NewTableEntry(exec, 1, 10, 2, "db", "tbl")
	exec.setTable(table)
	spec := &JobSpec{
		ConsumerInfo: ConsumerInfo{
			SrcTable: TableInfo{DBID: 10, TableID: 2, DBName: "db", TableName: "tbl"},
		},
		TriggerSpec: TriggerSpec{JobType: TriggerType_Default},
	}
	specJSON, err := MarshalJobSpec(spec)
	require.NoError(t, err)
	statusJSON, err := MarshalJobStatus(&JobStatus{})
	require.NoError(t, err)
	dropAt := types.Timestamp(time.Now().UnixNano())

	err = exec.addOrUpdateRecoveredJob(
		1,
		2,
		"index_idx01",
		7,
		ISCPJobState_Canceling,
		types.BuildTS(1, 0).ToString(),
		encodeRuntimeCancelTestJSON(t, specJSON),
		encodeRuntimeCancelTestJSON(t, statusJSON),
		dropAt,
		true,
	)
	require.NoError(t, err)

	select {
	case got := <-exec.cancelJobs:
		t.Fatalf("recovered dropped job should not be queued, got %+v", got)
	default:
	}
	require.Equal(t, int8(ISCPJobState_Canceled), table.jobs[JobKey{JobName: "index_idx01", JobID: 7}].state)
}
