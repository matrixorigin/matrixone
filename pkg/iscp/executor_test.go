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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
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
	for _, cancelAfterCalls := range []int{1, 2} {
		t.Run(fmt.Sprintf("backoff-%d", cancelAfterCalls), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			calls := 0
			start := time.Now()
			err := retry(ctx, func() error {
				calls++
				if calls == cancelAfterCalls {
					go func() {
						time.Sleep(10 * time.Millisecond)
						cancel()
					}()
				}
				return errors.New("retryable")
			}, 3, 20*time.Millisecond, time.Hour)

			require.Equal(t, cancelAfterCalls, calls)
			require.ErrorIs(t, err, context.Canceled)
			require.Less(t, time.Since(start), time.Second)
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

	invalid := &IterationContext{
		jobNames: []string{"job-1", "job-2"},
		jobIDs:   []uint64{1, 2},
		lsn:      []uint64{6, 99},
	}
	require.Error(t, table.markIterationPending(invalid))
	require.Equal(t, uint64(5), table.jobs[JobKey{JobName: "job-1", JobID: 1}].currentLSN)
	require.Equal(t, ISCPJobState_Completed, table.jobs[JobKey{JobName: "job-1", JobID: 1}].state)
	require.Equal(t, uint64(6), table.jobs[JobKey{JobName: "job-2", JobID: 2}].currentLSN)
	require.Equal(t, ISCPJobState_Completed, table.jobs[JobKey{JobName: "job-2", JobID: 2}].state)

	valid := &IterationContext{
		jobNames: []string{"job-1", "job-2"},
		jobIDs:   []uint64{1, 2},
		lsn:      []uint64{6, 7},
	}
	require.NoError(t, table.markIterationPending(valid))
	require.Equal(t, uint64(6), table.jobs[JobKey{JobName: "job-1", JobID: 1}].currentLSN)
	require.Equal(t, ISCPJobState_Pending, table.jobs[JobKey{JobName: "job-1", JobID: 1}].state)
	require.Equal(t, uint64(7), table.jobs[JobKey{JobName: "job-2", JobID: 2}].currentLSN)
	require.Equal(t, ISCPJobState_Pending, table.jobs[JobKey{JobName: "job-2", JobID: 2}].state)
}

func TestISCPRecoveryNormalizesPreRepairSnapshot(t *testing.T) {
	exec := &ISCPTaskExecutor{
		ctx:    context.Background(),
		tables: newISCPTableTree(),
	}
	spec, err := MarshalJobSpec(&JobSpec{
		ConsumerInfo: ConsumerInfo{
			SrcTable: TableInfo{DBID: 2, TableID: 3, DBName: "db", TableName: "table"},
		},
	})
	require.NoError(t, err)
	status, err := MarshalJobStatus(&JobStatus{LSN: 5})
	require.NoError(t, err)
	encodeJSON := func(value string) []byte {
		byteJSON, encodeErr := types.ParseStringToByteJson(value)
		require.NoError(t, encodeErr)
		encoded, encodeErr := types.EncodeJson(byteJSON)
		require.NoError(t, encodeErr)
		return encoded
	}

	require.NoError(t, exec.addOrUpdateRecoveredJob(
		1, 3, "pending", 4, ISCPJobState_Pending, "10-0",
		encodeJSON(spec), encodeJSON(status), 0, true,
	))
	require.NoError(t, exec.addOrUpdateRecoveredJob(
		1, 3, "error", 5, ISCPJobState_Error, "10-0",
		encodeJSON(spec), encodeJSON(status), 0, true,
	))

	table, ok := exec.getTable(1, 3)
	require.True(t, ok)
	require.Equal(t, ISCPJobState_Completed, table.jobs[JobKey{JobName: "pending", JobID: 4}].state)
	require.Equal(t, ISCPJobState_Error, table.jobs[JobKey{JobName: "error", JobID: 5}].state)
}
