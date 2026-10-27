// Copyright 2022 Matrix Origin
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

package service

import (
	"context"
	"errors"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"sync"
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// Cancellation is receiver-independent and precedes service teardown, including
// when startup or an assertion fails before an executor has started.
type taskTestCluster struct {
	Cluster
	cancel context.CancelFunc
}

func (c *taskTestCluster) Close() error { c.cancel(); return c.Cluster.Close() }

func newTaskTestCluster(t *testing.T, opt Options) (*taskTestCluster, context.Context) {
	t.Helper()
	owner, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	c, err := NewCluster(owner, t, opt)
	require.NoError(t, err)
	fixture := &taskTestCluster{Cluster: c, cancel: cancel}
	t.Cleanup(func() { require.NoError(t, fixture.Close()) })
	require.NoError(t, c.Start())
	ctx, stop := context.WithTimeout(owner, defaultTestTimeout)
	t.Cleanup(stop)
	return fixture, ctx
}

func createOwnedTask(t *testing.T, ctx context.Context, svc taskservice.TaskService, name string) task.AsyncTask {
	t.Helper()
	metadata := task.TaskMetadata{ID: name, Executor: task.TaskCode_TestOnly}
	require.NoError(t, svc.CreateAsyncTask(ctx, metadata))
	rows, err := svc.QueryAsyncTask(ctx, taskservice.WithTaskMetadataId(taskservice.EQ, metadata.ID), taskservice.WithTaskExecutorCond(taskservice.EQ, metadata.Executor))
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, metadata, rows[0].Metadata)
	return rows[0]
}

func waitOwnedTask(t *testing.T, ctx context.Context, svc taskservice.TaskService, original task.AsyncTask, ready func([]task.AsyncTask) bool, retryRead func(error) bool) task.AsyncTask {
	var last task.AsyncTask
	var lastErr error
	t.Helper()
	for {
		require.NoError(t, ctx.Err(), "task %d did not reach its expected owner; last=%s; read error=%v", original.ID, last.String(), lastErr)
		rows, err := svc.QueryAsyncTask(ctx, taskservice.WithTaskIDCond(taskservice.EQ, original.ID))
		require.NoError(t, ctx.Err(), "task %d last=%s; read error=%v", original.ID, last.String(), err)
		if err != nil {
			require.True(t, retryRead != nil && retryRead(err), "task %d read failed: %v", original.ID, err)
			lastErr = err
			rows = nil
		}
		require.LessOrEqual(t, len(rows), 1)
		if len(rows) == 1 {
			if last.ID != rows[0].ID || last.Status != rows[0].Status || last.TaskRunner != rows[0].TaskRunner || last.Epoch != rows[0].Epoch {
				t.Logf("task=%d status=%s runner=%s epoch=%d", rows[0].ID, rows[0].Status, rows[0].TaskRunner, rows[0].Epoch)
			}
			last = rows[0]
			require.Equal(t, original.ID, rows[0].ID)
			require.Equal(t, original.Metadata, rows[0].Metadata)

		}
		if err == nil && ready(rows) {
			if len(rows) == 1 {
				return rows[0]
			}
			return task.AsyncTask{}
		}
		timer := time.NewTimer(300 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			require.NoError(t, ctx.Err(), "task %d last=%s; read error=%v", original.ID, last.String(), lastErr)
		case <-timer.C:
		}
	}
}

func waitTaskScheduled(t *testing.T, ctx context.Context, svc taskservice.TaskService, original task.AsyncTask, runners ...string) task.AsyncTask {
	t.Helper()
	return waitOwnedTask(t, ctx, svc, original, func(rows []task.AsyncTask) bool {
		if len(rows) != 1 || rows[0].Status != task.TaskStatus_Running {
			return false
		}
		row := rows[0]
		for _, runner := range runners {
			if runner != "" && row.TaskRunner == runner {
				return true
			}
		}
		return false
	}, nil)
}

func waitTaskExpired(t *testing.T, ctx context.Context, svc taskservice.TaskService, original task.AsyncTask) {
	t.Helper()
	waitOwnedTask(t, ctx, svc, original, func(rows []task.AsyncTask) bool { return len(rows) == 0 }, retryTaskFailoverRead)
}

// Recovery reads cross an intentionally disrupted service lifecycle. Reuse the
// transport classifier; never retry business writes or semantic SQL failures.
func retryTaskFailoverRead(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	return errors.Is(err, taskservice.ErrNotReady) || morpc.IsConnectionError(err)
}

func TestTaskServiceCanCreate(t *testing.T) {
	t.Cleanup(leaktest.AfterTest(t))
	if testing.Short() {
		t.Skip("skipping in short mode.")
	}
	c, ctx := newTaskTestCluster(t, DefaultOptions().WithCNServiceNum(1).WithCNShardNum(1).WithTNServiceNum(1).WithTNShardNum(1).WithLogServiceNum(3).WithLogShardNum(1))
	require.Len(t, c.Cluster.(*testCluster).log.svcs, 3)
	c.WaitCNStoreTaskServiceCreatedIndexed(ctx, 0)
	c.WaitTNStoreTaskServiceCreatedIndexed(ctx, 0)
	for i := 0; i < 3; i++ {
		c.WaitLogStoreTaskServiceCreatedIndexed(ctx, i)
	}
}

func TestTaskSchedulerCanAllocateTask(t *testing.T) {
	t.Cleanup(leaktest.AfterTest(t))
	if testing.Short() {
		t.Skip("skipping in short mode.")
	}
	c, ctx := newTaskTestCluster(t, DefaultOptions())
	c.WaitCNStoreTaskServiceCreatedIndexed(ctx, 0)
	cn, err := c.GetCNServiceIndexed(0)
	require.NoError(t, err)
	cn.GetTaskRunner().RegisterExecutor(task.TaskCode_TestOnly, func(runCtx context.Context, _ task.Task) error {
		select {
		case <-runCtx.Done():
		case <-ctx.Done():
		}
		return nil
	})
	svc, ok := cn.GetTaskService()
	require.True(t, ok)
	original := createOwnedTask(t, ctx, svc, t.Name())
	waitTaskScheduled(t, ctx, svc, original, cn.ID())
}

func TestTaskSchedulerExpiresLostRunnerWithoutReplay(t *testing.T) {
	t.Cleanup(leaktest.AfterTest(t))
	if testing.Short() {
		t.Skip("skipping in short mode.")
	}
	c, ctx := newTaskTestCluster(t, DefaultOptions().WithCNServiceNum(2))
	for i := 0; i < 2; i++ {
		c.WaitCNStoreTaskServiceCreatedIndexed(ctx, i)
	}
	cn1, err := c.GetCNServiceIndexed(0)
	require.NoError(t, err)
	cn2, err := c.GetCNServiceIndexed(1)
	require.NoError(t, err)
	entered := make(chan uint64, 4)
	var mu sync.Mutex
	executions := make(map[uint64]int)
	executor := func(runCtx context.Context, row task.Task) error {
		mu.Lock()
		executions[row.GetID()]++
		mu.Unlock()
		select {
		case entered <- row.GetID():
		case <-runCtx.Done():
			return nil
		case <-ctx.Done():
			return nil
		}
		select {
		case <-runCtx.Done():
		case <-ctx.Done():
		}
		return nil
	}
	cn1.GetTaskRunner().RegisterExecutor(task.TaskCode_TestOnly, executor)
	cn2.GetTaskRunner().RegisterExecutor(task.TaskCode_TestOnly, executor)
	svc, ok := cn1.GetTaskService()
	require.True(t, ok)
	original := createOwnedTask(t, ctx, svc, t.Name())
	running := waitTaskScheduled(t, ctx, svc, original, cn1.ID(), cn2.ID())
	select {
	case id := <-entered:
		require.Equal(t, original.ID, id)
	case <-ctx.Done():
		require.NoError(t, ctx.Err())
	}
	require.NoError(t, c.CloseCNService(running.TaskRunner))
	survivor := cn1
	if running.TaskRunner == cn1.ID() {
		survivor = cn2
	}
	svc, ok = survivor.GetTaskService()
	require.True(t, ok)
	// Expired work is failed and truncated; it must not be replayed on another CN.
	waitTaskExpired(t, ctx, svc, running)
	followup := createOwnedTask(t, ctx, svc, "survivor-after-expiry")
	require.NotEqual(t, original.ID, followup.ID)
	active := waitTaskScheduled(t, ctx, svc, followup, survivor.ID())
	require.Equal(t, uint32(1), active.Epoch)
	select {
	case id := <-entered:
		require.Equal(t, followup.ID, id)
	case <-ctx.Done():
		require.NoError(t, ctx.Err())
	}
	rows, err := svc.QueryAsyncTask(ctx, taskservice.WithTaskIDCond(taskservice.EQ, original.ID))
	require.NoError(t, err)
	require.Empty(t, rows)
	mu.Lock()
	counts := make(map[uint64]int, len(executions))
	for id, n := range executions {
		counts[id] = n
	}
	mu.Unlock()
	require.Equal(t, map[uint64]int{original.ID: 1, followup.ID: 1}, counts, "no replay through the follow-up observation window")
}

func TestTaskRunner(t *testing.T) {
	t.Cleanup(leaktest.AfterTest(t))
	if testing.Short() {
		t.Skip("skipping in short mode.")
	}
	c, ctx := newTaskTestCluster(t, DefaultOptions().WithCNServiceNum(1).WithLogLevel(zap.DebugLevel))
	c.WaitCNStoreTaskServiceCreatedIndexed(ctx, 0)
	cn, err := c.GetCNServiceIndexed(0)
	require.NoError(t, err)
	executed := make(chan uint64)
	cn.GetTaskRunner().RegisterExecutor(task.TaskCode_TestOnly, func(runCtx context.Context, row task.Task) error {
		select {
		case executed <- row.GetID():
		case <-runCtx.Done():
		case <-ctx.Done():
		}
		return nil
	})
	svc, ok := cn.GetTaskService()
	require.True(t, ok)
	original := createOwnedTask(t, ctx, svc, t.Name())
	waitTaskScheduled(t, ctx, svc, original, cn.ID())
	select {
	case id := <-executed:
		require.Equal(t, original.ID, id)
	case <-ctx.Done():
		require.NoError(t, ctx.Err(), "task not running")
	}
}

func TestCronTask(t *testing.T) {
	t.Cleanup(leaktest.AfterTest(t))
	if testing.Short() {
		t.Skip("skipping in short mode.")
	}
	c, ctx := newTaskTestCluster(t, DefaultOptions().WithLogLevel(zap.DebugLevel))
	c.WaitCNStoreTaskServiceCreatedIndexed(ctx, 0)
	cn, err := c.GetCNServiceIndexed(0)
	require.NoError(t, err)
	executed := make(chan uint64)
	cn.GetTaskRunner().RegisterExecutor(task.TaskCode_TestOnly, func(runCtx context.Context, row task.Task) error {
		select {
		case executed <- row.GetID():
		case <-runCtx.Done():
		case <-ctx.Done():
		}
		return nil
	})
	svc, ok := cn.GetTaskService()
	require.True(t, ok)
	require.NoError(t, svc.CreateCronTask(ctx, task.TaskMetadata{ID: t.Name(), Executor: task.TaskCode_TestOnly}, "*/1 * * * * *"))
	for i := 0; i < 3; i++ {
		select {
		case <-executed:
		case <-ctx.Done():
			require.NoError(t, ctx.Err(), "cron task not repeated enough")
		}
	}
}
