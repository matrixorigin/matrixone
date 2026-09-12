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
	"fmt"
	"runtime"
	"testing"
	"testing/synctest"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestRetryReturnsCanceledContextBeforeFirstAttempt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	called := false
	err := retry(ctx, func() error {
		called = true
		return nil
	}, 3, time.Millisecond, time.Second)

	require.False(t, called)
	require.ErrorIs(t, err, context.Canceled)
}

func TestRetryBackoffHonorsRemainingBudget(t *testing.T) {
	for _, tc := range []struct {
		name     string
		interval time.Duration
		work     time.Duration
		calls    int
	}{
		{"exponential backoff", time.Second, 0, 12},
		{"initial delay exceeds budget", 2 * time.Hour, 0, 1},
		{"attempt exhausts budget", time.Second, time.Hour, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				wantErr := errors.New("transient failure")
				calls := 0
				start := time.Now()
				err := retry(context.Background(), func() error {
					calls++
					// Virtual work duration, not scheduler synchronization.
					time.Sleep(tc.work)
					return wantErr
				}, SubmitRetryTimes, tc.interval, time.Hour)
				require.ErrorIs(t, err, wantErr)
				require.Equal(t, tc.calls, calls)
				require.Equal(t, time.Hour, time.Since(start))
			})
		})
	}
}

func TestRetryReturnsCanceledContextWhenRetryTimesZero(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	called := false
	err := retry(ctx, func() error {
		called = true
		return nil
	}, 0, time.Millisecond, time.Second)

	require.False(t, called)
	require.ErrorIs(t, err, context.Canceled)
}

func TestRetryReturnsCanceledContextDuringAttempt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	called := false
	err := retry(ctx, func() error {
		called = true
		cancel()
		return errors.New("retryable")
	}, 3, time.Hour, time.Hour)

	require.True(t, called)
	require.ErrorIs(t, err, context.Canceled)
}

func TestRetryBackoffInterruptedByContextCancellation(t *testing.T) {
	const firstInterval = 20 * time.Millisecond

	for _, cancelAfterCalls := range []int{1, 2} {
		t.Run(fmt.Sprintf("backoff-%d", cancelAfterCalls), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				type retryResult struct {
					err   error
					calls int
				}
				attemptC := make(chan int)
				resultC := make(chan retryResult, 1)
				start := time.Now()
				go func() {
					calls := 0
					err := retry(ctx, func() error {
						calls++
						attemptC <- calls
						return errors.New("retryable")
					}, 3, firstInterval, time.Hour)
					resultC <- retryResult{err: err, calls: calls}
				}()

				for completedCalls := 1; completedCalls <= cancelAfterCalls; completedCalls++ {
					require.Equal(t, completedCalls, <-attemptC)
					if completedCalls < cancelAfterCalls {
						time.Sleep(firstInterval << (completedCalls - 1))
					}
				}

				// Freeze virtual time after the target attempt has entered its
				// backoff, then cancel without allowing that timer to elapse.
				synctest.Wait()
				cancel()

				result := <-resultC
				require.Equal(t, cancelAfterCalls, result.calls)
				require.ErrorIs(t, result.err, context.Canceled)

				wantElapsed := time.Duration(0)
				for completedCalls := 1; completedCalls < cancelAfterCalls; completedCalls++ {
					wantElapsed += firstInterval << (completedCalls - 1)
				}
				require.Equal(t, wantElapsed, time.Since(start))
			})
		})
	}
}

func TestRegisterJobReturnsCanceledContextBeforeFirstAttempt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ok, err := RegisterJob(ctx, "", nil, nil, nil, false)

	require.False(t, ok)
	require.ErrorIs(t, err, context.Canceled)
}

func TestUnregisterJobReturnsCanceledContextBeforeFirstAttempt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ok, err := UnregisterJob(ctx, "", nil, nil)

	require.False(t, ok)
	require.ErrorIs(t, err, context.Canceled)
}

func TestNewJobEntryRestoresPersistedLSN(t *testing.T) {
	exec := &ISCPTaskExecutor{}
	table := NewTableEntry(exec, 1, 2, 3, "db", "table")
	spec := &JobSpec{}
	status := &JobStatus{LSN: 5}
	legacy := NewJobEntry(
		table, "legacy", spec, 3, types.BuildTS(9, 0), ISCPJobState_Completed, 0,
	)
	require.Zero(t, legacy.currentLSN)
	restored := NewJobEntryWithStatus(
		table, "restored", spec, status, 4, types.BuildTS(10, 0), ISCPJobState_Completed, 0,
	)
	require.Equal(t, uint64(5), restored.currentLSN)

	table.AddOrUpdateSinker(
		context.Background(),
		"job",
		spec,
		status,
		4,
		types.BuildTS(10, 0),
		ISCPJobState_Completed,
		0,
	)

	job := table.jobs[JobKey{JobName: "job", JobID: 4}]
	require.Equal(t, uint64(5), job.currentLSN)
}

func TestTableEntryDoesNotShareInitIterations(t *testing.T) {
	exec := newRuntimeTestExecutor()
	table := NewTableEntry(exec, 1, 2, 3, "db", "table")
	spec := &JobSpec{
		ConsumerInfo: ConsumerInfo{InitSQL: "select 1"},
		TriggerSpec:  TriggerSpec{JobType: TriggerType_Default},
	}
	watermark := types.BuildTS(10, 0)
	for i, name := range []string{"index_ft", "index_hv"} {
		table.AddOrUpdateSinker(
			context.Background(),
			name,
			spec,
			&JobStatus{LSN: 5, Stage: JobStage_Init},
			uint64(i+1),
			watermark,
			ISCPJobState_Completed,
			0,
		)
	}

	iters, _ := table.getCandidate()
	require.Len(t, iters, 2)
	for _, iter := range iters {
		require.Len(t, iter.jobNames, 1)
	}

	// Once one job has initialized, it must still not share an iteration with
	// the other job while that job remains in the Init stage.
	table.AddOrUpdateSinker(
		context.Background(),
		"index_ft",
		spec,
		&JobStatus{LSN: 5, Stage: JobStage_Running},
		1,
		watermark,
		ISCPJobState_Completed,
		0,
	)
	require.Equal(t, int8(JobStage_Running), table.jobs[JobKey{JobName: "index_ft", JobID: 1}].stage)
	require.Equal(t, uint64(5), table.jobs[JobKey{JobName: "index_ft", JobID: 1}].currentLSN)
	iters, _ = table.getCandidate()
	require.Len(t, iters, 2)
	for _, iter := range iters {
		require.Len(t, iter.jobNames, 1)
	}

	// Normal running jobs retain the existing shared-iteration behavior.
	table.AddOrUpdateSinker(
		context.Background(),
		"index_hv",
		spec,
		&JobStatus{LSN: 5, Stage: JobStage_Running},
		2,
		watermark,
		ISCPJobState_Completed,
		0,
	)
	require.Equal(t, int8(JobStage_Running), table.jobs[JobKey{JobName: "index_hv", JobID: 2}].stage)
	iters, _ = table.getCandidate()
	require.Len(t, iters, 1)
	require.ElementsMatch(t, []string{"index_ft", "index_hv"}, iters[0].jobNames)
	require.Equal(t, []int8{JobStage_Running, JobStage_Running}, iters[0].stages)
}

func TestPublishRebuiltStateReplacesAbandonedGeneration(t *testing.T) {
	exec := &ISCPTaskExecutor{tables: newISCPTableTree()}
	oldTable := NewTableEntry(exec, 1, 2, 3, "db", "table")
	oldTable.AddOrUpdateSinker(
		context.Background(),
		"job",
		&JobSpec{},
		&JobStatus{LSN: 6},
		4,
		types.BuildTS(20, 0),
		ISCPJobState_Pending,
		0,
	)
	exec.tables.Set(oldTable)
	exec.tables.Set(NewTableEntry(exec, 1, 5, 6, "stale-db", "stale-table"))

	snapshot := &ISCPTaskExecutor{tables: newISCPTableTree()}
	persistedTable := NewTableEntry(snapshot, 1, 2, 3, "db", "table")
	persistedWatermark := types.BuildTS(10, 0)
	persistedTable.AddOrUpdateSinker(
		context.Background(),
		"job",
		&JobSpec{},
		&JobStatus{LSN: 5},
		4,
		persistedWatermark,
		ISCPJobState_Completed,
		0,
	)
	snapshot.tables.Set(persistedTable)

	replayWatermark := types.BuildTS(30, 0)
	exec.publishRebuiltState(snapshot, 42, replayWatermark)

	table, ok := exec.getTable(1, 3)
	require.True(t, ok)
	require.NotSame(t, oldTable, table)
	require.Same(t, exec, table.exec)
	job := table.jobs[JobKey{JobName: "job", JobID: 4}]
	require.Equal(t, uint64(5), job.currentLSN)
	require.Equal(t, ISCPJobState_Completed, job.state)
	require.True(t, job.watermark.EQ(&persistedWatermark))
	require.Equal(t, uint64(42), exec.prevISCPTableID)
	require.True(t, exec.iscpLogWm.EQ(&replayWatermark))
	_, ok = exec.getTable(1, 6)
	require.False(t, ok)
}

func TestMarkIterationPendingIsAtomic(t *testing.T) {
	exec := &ISCPTaskExecutor{}
	table := NewTableEntry(exec, 1, 2, 3, "db", "table")
	for i, name := range []string{"job-1", "job-2"} {
		table.AddOrUpdateSinker(
			context.Background(),
			name,
			&JobSpec{},
			&JobStatus{LSN: uint64(i + 5)},
			uint64(i+1),
			types.BuildTS(10, 0),
			ISCPJobState_Completed,
			0,
		)
	}
	assertUnchanged := func() {
		require.Equal(t, uint64(5), table.jobs[JobKey{JobName: "job-1", JobID: 1}].currentLSN)
		require.Equal(t, ISCPJobState_Completed, table.jobs[JobKey{JobName: "job-1", JobID: 1}].state)
		require.Equal(t, uint64(6), table.jobs[JobKey{JobName: "job-2", JobID: 2}].currentLSN)
		require.Equal(t, ISCPJobState_Completed, table.jobs[JobKey{JobName: "job-2", JobID: 2}].state)
	}

	missingStage := &IterationContext{
		jobNames: []string{"job-1", "job-2"},
		jobIDs:   []uint64{1, 2},
		lsn:      []uint64{6, 7},
		stages:   []int8{JobStage_Running},
	}
	require.Error(t, table.markIterationPending(missingStage))
	assertUnchanged()

	invalid := &IterationContext{
		jobNames: []string{"job-1", "job-2"},
		jobIDs:   []uint64{1, 2},
		lsn:      []uint64{6, 99},
		stages:   []int8{JobStage_Running, JobStage_Running},
	}
	require.Error(t, table.markIterationPending(invalid))
	assertUnchanged()

	valid := &IterationContext{
		jobNames: []string{"job-1", "job-2"},
		jobIDs:   []uint64{1, 2},
		lsn:      []uint64{6, 7},
		stages:   []int8{JobStage_Running, JobStage_Running},
	}
	require.NoError(t, table.markIterationPending(valid))
	require.Equal(t, uint64(6), table.jobs[JobKey{JobName: "job-1", JobID: 1}].currentLSN)
	require.Equal(t, ISCPJobState_Pending, table.jobs[JobKey{JobName: "job-1", JobID: 1}].state)
	require.Equal(t, uint64(7), table.jobs[JobKey{JobName: "job-2", JobID: 2}].currentLSN)
	require.Equal(t, ISCPJobState_Pending, table.jobs[JobKey{JobName: "job-2", JobID: 2}].state)
}

func TestTryFlushWatermarkSerializesWithReaders(t *testing.T) {
	table := NewTableEntry(nil, 1, 2, 3, "db", "table")
	jobKey := JobKey{JobName: "job", JobID: 1}
	table.jobs[jobKey] = NewJobEntry(
		table,
		jobKey.JobName,
		&JobSpec{},
		jobKey.JobID,
		types.BuildTS(1, 0),
		ISCPJobState_Pending,
		0,
	)
	table.mu.RLock()

	started := make(chan struct{})
	done := make(chan struct{})
	go func() {
		close(started)
		_, _ = table.tryFlushWatermark(context.Background(), nil, time.Hour)
		close(done)
	}()
	<-started

	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case <-done:
			table.mu.RUnlock()
			t.Fatal("watermark flush completed while a reader held the table lock")
		case <-deadline.C:
			table.mu.RUnlock()
			t.Fatal("watermark flush did not wait as an exclusive writer")
		default:
			if table.mu.TryRLock() {
				table.mu.RUnlock()
				runtime.Gosched()
				continue
			}
		}
		break
	}

	select {
	case <-done:
		table.mu.RUnlock()
		t.Fatal("watermark flush completed before the existing reader released the table lock")
	default:
	}

	table.mu.RUnlock()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("watermark flush did not complete after the reader released the table lock")
	}
}

func TestUpdateWatermarkDiscontinuityFencesWithoutAdvancing(t *testing.T) {
	oldFlush := FlushJobStatusOnIterationState
	t.Cleanup(func() { FlushJobStatusOnIterationState = oldFlush })

	exec := &ISCPTaskExecutor{ctx: context.Background()}
	table := NewTableEntry(exec, 1, 2, 3, "db", "table")
	job := NewJobEntryWithStatus(
		table,
		"index_idx",
		&JobSpec{},
		&JobStatus{LSN: 5, Stage: JobStage_Running},
		1,
		types.BuildTS(10, 0),
		ISCPJobState_Completed,
		0,
	)
	before := job.watermark

	FlushJobStatusOnIterationState = func(
		_ context.Context,
		_ string,
		_ engine.Engine,
		_ client.TxnClient,
		_ uint32,
		_ uint64,
		_ []string,
		_ []uint64,
		_ []uint64,
		statuses []*JobStatus,
		_ types.TS,
		state int8,
		_ []uint64,
	) error {
		require.Equal(t, ISCPJobState_Error, state)
		require.Len(t, statuses, 1)
		require.Equal(t, int8(JobStage_Running), statuses[0].Stage)
		require.Equal(t, PermanentErrorThreshold, statuses[0].ErrorCode)
		require.Contains(t, statuses[0].ErrorMsg, "update watermark failed")
		return nil
	}

	err := job.UpdateWatermark(types.BuildTS(20, 0), types.BuildTS(30, 0), time.Second)

	require.NoError(t, err)
	require.Equal(t, ISCPJobState_Error, job.state)
	require.True(t, job.watermark.EQ(&before))
}

func TestUpdateWatermarkDiscontinuityKeepsLocalStateWhenFenceFails(t *testing.T) {
	oldFlush := FlushJobStatusOnIterationState
	t.Cleanup(func() { FlushJobStatusOnIterationState = oldFlush })

	exec := &ISCPTaskExecutor{ctx: context.Background()}
	table := NewTableEntry(exec, 1, 2, 3, "db", "table")
	job := NewJobEntryWithStatus(
		table,
		"index_idx",
		&JobSpec{},
		&JobStatus{LSN: 5, Stage: JobStage_Running},
		1,
		types.BuildTS(10, 0),
		ISCPJobState_Completed,
		0,
	)
	before := job.watermark
	wantErr := errors.New("terminal status write failed")
	FlushJobStatusOnIterationState = func(
		context.Context, string, engine.Engine, client.TxnClient,
		uint32, uint64, []string, []uint64, []uint64, []*JobStatus,
		types.TS, int8, []uint64,
	) error {
		return wantErr
	}

	err := job.UpdateWatermark(types.BuildTS(20, 0), types.BuildTS(30, 0), time.Second)

	require.ErrorIs(t, err, wantErr)
	require.Equal(t, ISCPJobState_Completed, job.state)
	require.True(t, job.watermark.EQ(&before))
}

func TestJobEntryUpdateFencesDurableWatermarkRegression(t *testing.T) {
	oldFlush := FlushJobStatusOnIterationState
	t.Cleanup(func() { FlushJobStatusOnIterationState = oldFlush })

	exec := &ISCPTaskExecutor{ctx: context.Background()}
	table := NewTableEntry(exec, 1, 2, 3, "db", "table")
	job := NewJobEntryWithStatus(
		table,
		"index_idx",
		&JobSpec{},
		&JobStatus{LSN: 5, Stage: JobStage_Running},
		1,
		types.BuildTS(20, 0),
		ISCPJobState_Completed,
		0,
	)
	beforeWatermark := job.watermark
	beforePersisted := job.persistedWatermark
	incomingStatus := &JobStatus{LSN: 6, Stage: JobStage_Running}

	FlushJobStatusOnIterationState = func(
		_ context.Context,
		_ string,
		_ engine.Engine,
		_ client.TxnClient,
		_ uint32,
		_ uint64,
		_ []string,
		_ []uint64,
		lsns []uint64,
		statuses []*JobStatus,
		_ types.TS,
		state int8,
		prevLSN []uint64,
	) error {
		require.Equal(t, []uint64{6}, lsns)
		require.Equal(t, []uint64{6}, prevLSN)
		require.Equal(t, ISCPJobState_Error, state)
		require.Equal(t, PermanentErrorThreshold, statuses[0].ErrorCode)
		return nil
	}

	err := job.update(
		context.Background(),
		&JobSpec{},
		incomingStatus,
		types.BuildTS(10, 0),
		ISCPJobState_Completed,
		0,
	)

	require.NoError(t, err)
	require.Equal(t, uint64(6), job.currentLSN)
	require.Equal(t, ISCPJobState_Error, job.state)
	require.True(t, job.watermark.EQ(&beforeWatermark))
	require.True(t, job.persistedWatermark.EQ(&beforePersisted))
}

func TestJobEntryUpdateKeepsLocalStateWhenRegressionFenceFails(t *testing.T) {
	oldFlush := FlushJobStatusOnIterationState
	t.Cleanup(func() { FlushJobStatusOnIterationState = oldFlush })

	exec := &ISCPTaskExecutor{ctx: context.Background()}
	table := NewTableEntry(exec, 1, 2, 3, "db", "table")
	originalSpec := &JobSpec{TriggerSpec: TriggerSpec{JobType: TriggerType_Default}}
	job := NewJobEntryWithStatus(
		table,
		"index_idx",
		originalSpec,
		&JobStatus{LSN: 5, Stage: JobStage_Running},
		1,
		types.BuildTS(20, 0),
		ISCPJobState_Completed,
		0,
	)
	table.jobs[JobKey{JobName: job.jobName, JobID: job.jobID}] = job
	beforeSpec := job.jobSpec
	beforeWatermark := job.watermark
	beforePersisted := job.persistedWatermark
	wantErr := errors.New("terminal fence failed")
	FlushJobStatusOnIterationState = func(
		context.Context, string, engine.Engine, client.TxnClient,
		uint32, uint64, []string, []uint64, []uint64, []*JobStatus,
		types.TS, int8, []uint64,
	) error {
		return wantErr
	}

	_, err := table.AddOrUpdateSinker(
		context.Background(),
		job.jobName,
		&JobSpec{TriggerSpec: TriggerSpec{JobType: TriggerType_Timed}},
		&JobStatus{LSN: 6, Stage: JobStage_Running},
		job.jobID,
		types.BuildTS(10, 0),
		ISCPJobState_Completed,
		types.Timestamp(123),
	)

	require.ErrorIs(t, err, wantErr)
	require.Equal(t, uint64(5), job.currentLSN)
	require.Equal(t, ISCPJobState_Completed, job.state)
	require.True(t, job.watermark.EQ(&beforeWatermark))
	require.True(t, job.persistedWatermark.EQ(&beforePersisted))
	require.Same(t, beforeSpec, job.jobSpec)
	require.Zero(t, job.dropAt)
}

func TestJobEntryUpdateAcceptsDurableErrorWithoutRefencing(t *testing.T) {
	oldFlush := FlushJobStatusOnIterationState
	t.Cleanup(func() { FlushJobStatusOnIterationState = oldFlush })

	exec := &ISCPTaskExecutor{ctx: context.Background()}
	table := NewTableEntry(exec, 1, 2, 3, "db", "table")
	job := NewJobEntryWithStatus(
		table,
		"index_idx",
		&JobSpec{},
		&JobStatus{LSN: 5, Stage: JobStage_Running},
		1,
		types.BuildTS(20, 0),
		ISCPJobState_Completed,
		0,
	)
	beforeWatermark := job.watermark
	beforePersisted := job.persistedWatermark
	FlushJobStatusOnIterationState = func(
		context.Context, string, engine.Engine, client.TxnClient,
		uint32, uint64, []string, []uint64, []uint64, []*JobStatus,
		types.TS, int8, []uint64,
	) error {
		t.Fatal("an already durable Error must not be fenced again")
		return nil
	}

	err := job.update(
		context.Background(),
		&JobSpec{},
		&JobStatus{LSN: 6, Stage: JobStage_Running, ErrorCode: PermanentErrorThreshold},
		types.BuildTS(10, 0),
		ISCPJobState_Error,
		0,
	)

	require.NoError(t, err)
	require.Equal(t, uint64(6), job.currentLSN)
	require.Equal(t, ISCPJobState_Error, job.state)
	require.True(t, job.watermark.EQ(&beforeWatermark))
	require.True(t, job.persistedWatermark.EQ(&beforePersisted))
}

func TestShouldReplayISCPLogOnStatusCASLoss(t *testing.T) {
	require.False(t, shouldReplayISCPLog(nil))
	require.False(t, shouldReplayISCPLog(errors.New("temporary SQL failure")))
	require.True(t, shouldReplayISCPLog(
		newISCPStatusCASLostError("test", "job", 1, 0)))
}

func TestNestedStatusRetriesStopOnCASLoss(t *testing.T) {
	cas := newISCPStatusCASLostError("status", "job", 1, 0)
	cleanup := errors.New("status rollback failed")
	for _, state := range []int8{ISCPJobState_Completed, ISCPJobState_Error} {
		for _, failure := range []error{cas, errors.Join(cas, cleanup)} {
			t.Run(fmt.Sprintf("state=%d/%s", state, failure), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					oldFlush := FlushJobStatusOnIterationState
					t.Cleanup(func() { FlushJobStatusOnIterationState = oldFlush })
					writes := 0
					FlushJobStatusOnIterationState = func(
						_ context.Context, _ string, _ engine.Engine, _ client.TxnClient,
						_ uint32, _ uint64, _ []string, _ []uint64, _ []uint64,
						_ []*JobStatus, _ types.TS, gotState int8, _ []uint64,
					) error {
						writes++
						require.Equal(t, state, gotState)
						return failure
					}
					iter := &IterationContext{jobNames: []string{"job"}, jobIDs: []uint64{1}, lsn: []uint64{2}}
					start := time.Now()
					attempts := 0
					err := retryISCPTaskIteration(context.Background(), func() error {
						attempts++
						return flushFinalJobStatusOnIterationState(context.Background(), "", nil, nil,
							iter, 0, &JobStatus{Stage: JobStage_Running}, types.TS{}, state)
					})
					require.Equal(t, 1, writes)
					require.Equal(t, 1, attempts)
					require.Zero(t, time.Since(start))
					if errors.Is(failure, cleanup) {
						require.ErrorIs(t, err, cleanup)
					} else {
						require.NoError(t, err)
					}
				})
			})
		}
	}
}

func TestFlushPermanentErrorMessagePopulatesDefaultStatus(t *testing.T) {
	oldFlush := FlushJobStatusOnIterationState
	t.Cleanup(func() { FlushJobStatusOnIterationState = oldFlush })

	statuses := []*JobStatus{nil}
	FlushJobStatusOnIterationState = func(
		_ context.Context,
		_ string,
		_ engine.Engine,
		_ client.TxnClient,
		_ uint32,
		_ uint64,
		_ []string,
		_ []uint64,
		_ []uint64,
		got []*JobStatus,
		_ types.TS,
		state int8,
		_ []uint64,
	) error {
		require.Equal(t, ISCPJobState_Error, state)
		require.Len(t, got, 1)
		require.Equal(t, uint64(7), got[0].LSN)
		require.Equal(t, int8(JobStage_Init), got[0].Stage)
		require.Equal(t, PermanentErrorThreshold, got[0].ErrorCode)
		require.Equal(t, "invalid catalog status", got[0].ErrorMsg)
		return nil
	}

	err := FlushPermanentErrorMessage(
		context.Background(), "cn", nil, nil,
		1, 2, []string{"job"}, []uint64{3}, []uint64{7}, statuses,
		types.MaxTs(), "invalid catalog status", []uint64{6},
	)

	require.NoError(t, err)
	require.NotNil(t, statuses[0])
}

func TestTryFlushWatermarkPreservesRunningStage(t *testing.T) {
	oldExecWithResult := ExecWithResult
	t.Cleanup(func() { ExecWithResult = oldExecWithResult })

	table := NewTableEntry(&ISCPTaskExecutor{}, 1, 2, 3, "db", "table")
	job := NewJobEntryWithStatus(
		table,
		"index_idx",
		&JobSpec{},
		&JobStatus{LSN: 5, Stage: JobStage_Running},
		1,
		types.BuildTS(1, 0),
		ISCPJobState_Completed,
		0,
	)
	job.watermark = types.BuildTS(2, 0)
	table.jobs[JobKey{JobName: job.jobName, JobID: job.jobID}] = job

	var updateSQL string
	ExecWithResult = func(_ context.Context, sql string, _ string, _ client.TxnOperator) (executor.Result, error) {
		updateSQL = sql
		return executor.Result{AffectedRows: 1}, nil
	}

	flushed, err := job.tryFlushWatermark(context.Background(), nil, 0)
	require.NoError(t, err)
	require.True(t, flushed)
	require.Contains(t, updateSQL, "job_status = JSON_SET(job_status, '$.LSN', 6, '$.Stage'")
	require.Contains(t, updateSQL, "AS SIGNED), 1)")
	require.Contains(t, updateSQL, ") WHERE account_id")
	require.Equal(t, ISCPJobState_Pending, job.state)
	require.True(t, job.persistedWatermark.EQ(&job.watermark))
}

func TestTryFlushWatermarkRejectsLostCAS(t *testing.T) {
	oldExecWithResult := ExecWithResult
	t.Cleanup(func() { ExecWithResult = oldExecWithResult })

	table := NewTableEntry(&ISCPTaskExecutor{}, 1, 2, 3, "db", "table")
	job := NewJobEntryWithStatus(
		table,
		"index_idx",
		&JobSpec{},
		&JobStatus{LSN: 5, Stage: JobStage_Running},
		1,
		types.BuildTS(1, 0),
		ISCPJobState_Completed,
		0,
	)
	job.watermark = types.BuildTS(2, 0)
	persistedBefore := job.persistedWatermark

	ExecWithResult = func(_ context.Context, _ string, _ string, _ client.TxnOperator) (executor.Result, error) {
		return executor.Result{AffectedRows: 0}, nil
	}

	flushed, err := job.tryFlushWatermark(context.Background(), nil, 0)
	require.Error(t, err)
	require.ErrorIs(t, err, errISCPStatusCASLost)
	require.True(t, flushed)
	require.Equal(t, ISCPJobState_Completed, job.state)
	require.True(t, job.persistedWatermark.EQ(&persistedBefore))
}

func TestRollbackWatermarkFlushRestoresRetryableState(t *testing.T) {
	oldExecWithResult := ExecWithResult
	t.Cleanup(func() { ExecWithResult = oldExecWithResult })

	table := NewTableEntry(&ISCPTaskExecutor{}, 1, 2, 3, "db", "table")
	jobKey := JobKey{JobName: "index_idx", JobID: 1}
	job := NewJobEntryWithStatus(
		table,
		jobKey.JobName,
		&JobSpec{},
		&JobStatus{LSN: 5, Stage: JobStage_Running},
		jobKey.JobID,
		types.BuildTS(1, 0),
		ISCPJobState_Completed,
		0,
	)
	job.watermark = types.BuildTS(2, 0)
	table.jobs[jobKey] = job

	ExecWithResult = func(_ context.Context, _ string, _ string, _ client.TxnOperator) (executor.Result, error) {
		return executor.Result{AffectedRows: 1}, nil
	}
	reservations, err := table.tryFlushWatermark(context.Background(), nil, 0)
	require.NoError(t, err)
	require.Len(t, reservations, 1)
	require.Equal(t, ISCPJobState_Pending, job.state)
	require.True(t, job.persistedWatermark.EQ(&job.watermark))

	table.rollbackWatermarkFlushes(reservations)

	require.Equal(t, ISCPJobState_Completed, job.state)
	expectedPersisted := types.BuildTS(1, 0)
	require.True(t, job.persistedWatermark.EQ(&expectedPersisted))

	// A delayed transaction error must not undo a newer catalog replay.
	job.currentLSN = 6
	job.persistedWatermark = types.BuildTS(3, 0)
	table.rollbackWatermarkFlushes(reservations)
	require.Equal(t, uint64(6), job.currentLSN)
	expectedPersisted = types.BuildTS(3, 0)
	require.True(t, job.persistedWatermark.EQ(&expectedPersisted))
}

func newWatermarkFlushTestJob(
	exec *ISCPTaskExecutor,
	tableID uint64,
	jobName string,
) *JobEntry {
	table := NewTableEntry(exec, 1, 2, tableID, "db", "table")
	job := NewJobEntryWithStatus(
		table,
		jobName,
		&JobSpec{},
		&JobStatus{LSN: 5, Stage: JobStage_Running},
		1,
		types.BuildTS(1, 0),
		ISCPJobState_Completed,
		0,
	)
	job.watermark = types.BuildTS(2, 0)
	table.jobs[JobKey{JobName: jobName, JobID: 1}] = job
	exec.setTable(table)
	return job
}

func TestFlushWatermarkForAllTablesRollsBackLocalStateOnCommitError(t *testing.T) {
	oldExecWithResult := ExecWithResult
	t.Cleanup(func() { ExecWithResult = oldExecWithResult })

	exec := &ISCPTaskExecutor{ctx: context.Background(), tables: newISCPTableTree()}
	job := newWatermarkFlushTestJob(exec, 3, "index_idx")
	ExecWithResult = func(_ context.Context, _ string, _ string, _ client.TxnOperator) (executor.Result, error) {
		return executor.Result{AffectedRows: 1}, nil
	}
	commitErr := errors.New("commit failed")
	txn := &iscpTxnForTest{commitErr: commitErr}

	err := exec.flushWatermarkForAllTablesWithTxn(0, txn)

	require.ErrorIs(t, err, commitErr)
	require.True(t, txn.committed)
	require.False(t, txn.rolledBack)
	require.Equal(t, ISCPJobState_Completed, job.state)
	expected := types.BuildTS(1, 0)
	require.True(t, job.persistedWatermark.EQ(&expected))
}

func TestFlushWatermarkForAllTablesCommitsLocalState(t *testing.T) {
	oldExecWithResult := ExecWithResult
	t.Cleanup(func() { ExecWithResult = oldExecWithResult })

	exec := &ISCPTaskExecutor{ctx: context.Background(), tables: newISCPTableTree()}
	job := newWatermarkFlushTestJob(exec, 3, "index_idx")
	ExecWithResult = func(_ context.Context, _ string, _ string, _ client.TxnOperator) (executor.Result, error) {
		return executor.Result{AffectedRows: 1}, nil
	}
	txn := &iscpTxnForTest{}

	err := exec.flushWatermarkForAllTablesWithTxn(0, txn)

	require.NoError(t, err)
	require.True(t, txn.committed)
	require.False(t, txn.rolledBack)
	require.Equal(t, ISCPJobState_Pending, job.state)
	expected := types.BuildTS(2, 0)
	require.True(t, job.persistedWatermark.EQ(&expected))
}

func TestFlushWatermarkForAllTablesRollsBackPartialTransaction(t *testing.T) {
	oldExecWithResult := ExecWithResult
	t.Cleanup(func() { ExecWithResult = oldExecWithResult })

	exec := &ISCPTaskExecutor{ctx: context.Background(), tables: newISCPTableTree()}
	first := newWatermarkFlushTestJob(exec, 3, "first")
	second := newWatermarkFlushTestJob(exec, 4, "second")
	wantErr := errors.New("second update failed")
	calls := 0
	ExecWithResult = func(_ context.Context, _ string, _ string, _ client.TxnOperator) (executor.Result, error) {
		calls++
		if calls == 2 {
			return executor.Result{}, wantErr
		}
		return executor.Result{AffectedRows: 1}, nil
	}
	txn := &iscpTxnForTest{}

	err := exec.flushWatermarkForAllTablesWithTxn(0, txn)

	require.ErrorIs(t, err, wantErr)
	require.Equal(t, 2, calls)
	require.False(t, txn.committed)
	require.True(t, txn.rolledBack)
	for _, job := range []*JobEntry{first, second} {
		require.Equal(t, ISCPJobState_Completed, job.state)
		expected := types.BuildTS(1, 0)
		require.True(t, job.persistedWatermark.EQ(&expected))
	}
}

func TestUnprovenInitQuarantineSQLIsConservative(t *testing.T) {
	sql := unprovenInitQuarantineSQL()

	require.Contains(t, sql, fmt.Sprintf("SET job_state = %d", ISCPJobState_Error))
	require.Contains(t, sql, fmt.Sprintf("'$.ErrorCode', %d", PermanentErrorThreshold))
	require.Contains(t, sql, fmt.Sprintf("WHERE job_state = %d", ISCPJobState_Completed))
	require.Contains(t, sql, "JSON_EXTRACT(job_status, '$.Stage')")
	require.Contains(t, sql, "JSON_EXTRACT(job_status, '$.LifecycleVersion')")
	require.Contains(t, sql, fmt.Sprintf("AS SIGNED) < %d", atomicInitLifecycleVersion))
	require.Contains(t, sql, "JSON_EXTRACT(job_spec, '$.InitSQL')")
	// Legacy LSN and error fields do not identify which half of the old
	// split-transaction initialization committed, so they must not exempt a row.
	require.NotContains(t, sql, "JSON_EXTRACT(job_status, '$.LSN')")
	require.NotContains(t, sql, "JSON_EXTRACT(job_status, '$.ErrorCode')")
	require.NotContains(t, sql, "JSON_EXTRACT(job_status, '$.ErrorMsg')")
}

func TestUnprovenInitClassification(t *testing.T) {
	spec := &JobSpec{ConsumerInfo: ConsumerInfo{InitSQL: "init"}}
	ambiguous := []struct {
		name   string
		status *JobStatus
	}{
		{"crash after init commit before first status", &JobStatus{Stage: JobStage_Init}},
		{"legacy watermark advanced", &JobStatus{LSN: 1, Stage: JobStage_Init}},
		{"status flush failed after init commit", &JobStatus{LSN: 1, Stage: JobStage_Init, ErrorMsg: "retry"}},
		{"legacy error code", &JobStatus{LSN: 1, Stage: JobStage_Init, ErrorCode: 1}},
	}
	for _, test := range ambiguous {
		t.Run(test.name, func(t *testing.T) {
			require.True(t, isUnprovenInit(ISCPJobState_Completed, spec, test.status))
		})
	}
	status := ambiguous[1].status

	controls := []struct {
		name   string
		state  int8
		spec   *JobSpec
		status *JobStatus
	}{
		{"atomic retry", ISCPJobState_Completed, spec, &JobStatus{LSN: 1, Stage: JobStage_Init, LifecycleVersion: atomicInitLifecycleVersion, ErrorMsg: "retry"}},
		{"initialized", ISCPJobState_Completed, spec, &JobStatus{LSN: 1, Stage: JobStage_Running}},
		{"no init sql", ISCPJobState_Completed, &JobSpec{}, status},
		{"terminal", ISCPJobState_Error, spec, status},
		{"nil spec", ISCPJobState_Completed, nil, status},
		{"nil status", ISCPJobState_Completed, spec, nil},
	}
	for _, control := range controls {
		t.Run(control.name, func(t *testing.T) {
			require.False(t, isUnprovenInit(control.state, control.spec, control.status))
		})
	}
}

func TestISCPRecoveryQuarantinesAmbiguousInitStage(t *testing.T) {
	exec := &ISCPTaskExecutor{
		ctx:    context.Background(),
		tables: newISCPTableTree(),
	}
	spec, err := MarshalJobSpec(&JobSpec{
		ConsumerInfo: ConsumerInfo{
			InitSQL:  "create table init_marker(a int)",
			SrcTable: TableInfo{DBID: 2, TableID: 3, DBName: "db", TableName: "table"},
		},
		TriggerSpec: TriggerSpec{JobType: TriggerType_Default},
	})
	require.NoError(t, err)
	legacyStatus, err := MarshalJobStatus(&JobStatus{LSN: 5})
	require.NoError(t, err)
	legacyZeroStatus, err := MarshalJobStatus(&JobStatus{})
	require.NoError(t, err)
	legacyRetryStatus, err := MarshalJobStatus(&JobStatus{LSN: 5, ErrorMsg: "retry initialization"})
	require.NoError(t, err)
	atomicRetryStatus, err := MarshalJobStatus(&JobStatus{
		LSN: 5, LifecycleVersion: atomicInitLifecycleVersion, ErrorMsg: "retry initialization",
	})
	require.NoError(t, err)
	encodeJSON := func(value string) []byte {
		byteJSON, encodeErr := types.ParseStringToByteJson(value)
		require.NoError(t, encodeErr)
		encoded, encodeErr := types.EncodeJson(byteJSON)
		require.NoError(t, encodeErr)
		return encoded
	}

	require.NoError(t, exec.addOrUpdateRecoveredJob(
		1, 3, "legacy", 4, ISCPJobState_Completed, "10-0",
		encodeJSON(spec), encodeJSON(legacyStatus), 0, true,
	))
	require.NoError(t, exec.addOrUpdateRecoveredJob(
		1, 3, "legacy-lsn-zero", 5, ISCPJobState_Completed, "10-0",
		encodeJSON(spec), encodeJSON(legacyZeroStatus), 0, true,
	))
	require.NoError(t, exec.addOrUpdateRecoveredJob(
		1, 3, "pending", 6, ISCPJobState_Pending, "10-0",
		encodeJSON(spec), encodeJSON(legacyStatus), 0, true,
	))
	require.NoError(t, exec.addOrUpdateRecoveredJob(
		1, 3, "error", 7, ISCPJobState_Error, "10-0",
		encodeJSON(spec), encodeJSON(legacyStatus), 0, true,
	))
	require.NoError(t, exec.addOrUpdateRecoveredJob(
		1, 3, "legacy-retryable", 8, ISCPJobState_Completed, "10-0",
		encodeJSON(spec), encodeJSON(legacyRetryStatus), 0, true,
	))
	require.NoError(t, exec.addOrUpdateRecoveredJob(
		1, 3, "atomic-retryable", 9, ISCPJobState_Completed, "10-0",
		encodeJSON(spec), encodeJSON(atomicRetryStatus), 0, true,
	))

	table, ok := exec.getTable(1, 3)
	require.True(t, ok)
	recovered := table.jobs[JobKey{JobName: "legacy", JobID: 4}]
	require.Equal(t, ISCPJobState_Error, recovered.state)
	// No unversioned Init row can prove whether the old InitSQL transaction
	// committed. Recovery must neither guess success nor execute a possibly
	// non-idempotent InitSQL again.
	require.Equal(t, int8(JobStage_Init), recovered.stage)
	require.Equal(t, ISCPJobState_Error, table.jobs[JobKey{JobName: "legacy-lsn-zero", JobID: 5}].state)
	require.Equal(t, ISCPJobState_Error, table.jobs[JobKey{JobName: "pending", JobID: 6}].state)
	require.Equal(t, int8(JobStage_Init), table.jobs[JobKey{JobName: "pending", JobID: 6}].stage)
	require.Equal(t, ISCPJobState_Error, table.jobs[JobKey{JobName: "error", JobID: 7}].state)
	require.Equal(t, ISCPJobState_Error, table.jobs[JobKey{JobName: "legacy-retryable", JobID: 8}].state)

	// Only generations carrying durable atomic-protocol evidence remain
	// schedulable, including a genuine retry after an atomic rollback.
	iters, _ := table.getCandidate()
	require.Len(t, iters, 1)
	require.Equal(t, []string{"atomic-retryable"}, iters[0].jobNames)
	require.Equal(t, []int8{JobStage_Init}, iters[0].stages)
}

func TestInitIterationCannotUseCleanTableWatermarkPath(t *testing.T) {
	exec := newRuntimeTestExecutor()
	table := NewTableEntry(exec, 1, 2, 3, "db", "table")
	spec := &JobSpec{
		ConsumerInfo: ConsumerInfo{InitSQL: "rebuild index"},
		TriggerSpec:  TriggerSpec{JobType: TriggerType_Default},
	}
	watermark := types.BuildTS(10, 0)
	_, err := table.AddOrUpdateSinker(
		context.Background(), "index_idx", spec, &JobStatus{}, 1,
		watermark, ISCPJobState_Completed, 0,
	)
	require.NoError(t, err)

	iters, _ := table.getCandidate()
	require.Len(t, iters, 1)
	iter := iters[0]
	require.False(t, iter.fromTS.IsEmpty())
	require.Equal(t, []int8{JobStage_Init}, iter.stages)

	// A clean table at the same retention boundary would skip a normal tail
	// iteration. Init still enters the worker because it has lifecycle work.
	require.True(t, shouldProcessIteration(iter, false, iter.fromTS, nil))
	running := *iter
	running.stages = []int8{JobStage_Running}
	require.False(t, shouldProcessIteration(&running, false, running.fromTS, nil))

	before := table.jobs[JobKey{JobName: "index_idx", JobID: 1}].watermark
	iter.toTS = types.BuildTS(20, 0)
	err = table.UpdateWatermark(iter)
	require.Error(t, err)
	require.Contains(t, err.Error(), "before initialization")
	require.True(t, table.jobs[JobKey{JobName: "index_idx", JobID: 1}].watermark.EQ(&before))
}

func TestUpdateWatermarkValidatesSharedIterationBeforeAdvancing(t *testing.T) {
	oldFlush := FlushJobStatusOnIterationState
	t.Cleanup(func() { FlushJobStatusOnIterationState = oldFlush })

	exec := newRuntimeTestExecutor()
	table := NewTableEntry(exec, 1, 2, 3, "db", "table")
	for i, name := range []string{"first", "stale"} {
		watermark := types.BuildTS(10-int64(i), 0)
		_, err := table.AddOrUpdateSinker(
			context.Background(), name,
			&JobSpec{TriggerSpec: TriggerSpec{JobType: TriggerType_Default}},
			&JobStatus{LSN: 5, Stage: JobStage_Running}, uint64(i+1),
			watermark, ISCPJobState_Completed, 0,
		)
		require.NoError(t, err)
	}
	FlushJobStatusOnIterationState = func(
		context.Context, string, engine.Engine, client.TxnClient,
		uint32, uint64, []string, []uint64, []uint64, []*JobStatus,
		types.TS, int8, []uint64,
	) error {
		return nil
	}

	first := table.jobs[JobKey{JobName: "first", JobID: 1}]
	firstBefore := first.watermark
	iter := &IterationContext{
		jobNames: []string{"first", "stale"},
		jobIDs:   []uint64{1, 2},
		fromTS:   first.watermark.Next(),
		toTS:     types.BuildTS(20, 0),
	}

	require.NoError(t, table.UpdateWatermark(iter))
	require.True(t, first.watermark.EQ(&firstBefore))
	require.Equal(t, ISCPJobState_Error, table.jobs[JobKey{JobName: "stale", JobID: 2}].state)
}

func TestReconcileIterationStagesPreventsStaleCatalogInit(t *testing.T) {
	tests := []struct {
		name        string
		stages      []int8
		persisted   int8
		wantStage   int8
		wantInitSQL bool
	}{
		{
			name:        "repaired scheduler stage wins over stale catalog snapshot",
			stages:      []int8{JobStage_Running},
			persisted:   JobStage_Init,
			wantStage:   JobStage_Running,
			wantInitSQL: false,
		},
		{
			name:        "catalog stage never regresses to scheduler init",
			stages:      []int8{JobStage_Init},
			persisted:   JobStage_Running,
			wantStage:   JobStage_Running,
			wantInitSQL: false,
		},
		{
			name:        "genuine init remains eligible",
			stages:      []int8{JobStage_Init},
			persisted:   JobStage_Init,
			wantStage:   JobStage_Init,
			wantInitSQL: true,
		},
		{
			name:        "legacy direct caller without stages keeps catalog decision",
			stages:      nil,
			persisted:   JobStage_Init,
			wantStage:   JobStage_Init,
			wantInitSQL: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			iter := &IterationContext{
				jobNames: []string{"index_idx"},
				stages:   test.stages,
			}
			statuses := []*JobStatus{{Stage: test.persisted}}
			jobSpec := &JobSpec{ConsumerInfo: ConsumerInfo{InitSQL: "init sql"}}

			reconcileIterationStages(iter, statuses)

			require.Equal(t, test.wantStage, statuses[0].Stage)
			require.Equal(t, test.wantInitSQL, jobNeedsInitSQL(jobSpec, statuses[0]))
		})
	}
}
