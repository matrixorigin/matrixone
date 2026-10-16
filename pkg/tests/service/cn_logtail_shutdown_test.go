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

package service

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// Use the real CN constructor, frontend context, task runner, TN logtail and
// TimestampWaiter. The task deliberately finishes a transaction after runner
// cancellation, the same ownership boundary used by ISCP's commit cleanup.
func TestCNCloseKeepsLogtailForDrainingTask(t *testing.T) {
	c, err := NewCluster(context.Background(), t, DefaultOptions().WithRootDataDir(t.TempDir()).WithLogLevel(zap.InfoLevel))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	require.NoError(t, c.Start())
	// Start may wait for exclusive cluster admission. Give task readiness and
	// shutdown their own budget after that wait, as the task-service tests do.
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	c.WaitCNStoreTaskServiceCreatedIndexed(ctx, 0)
	cn, err := c.GetCNServiceIndexed(0)
	require.NoError(t, err)
	owner := cn.(*cnService).svc
	exec := owner.GetSQLExecutor()
	for _, sql := range []string{"create database close_logtail", "create table close_logtail.t (id int primary key)"} {
		result, err := exec.Exec(ctx, sql, executor.Options{}.WithWaitCommittedLogApplied())
		require.NoError(t, err)
		result.Close()
	}
	entered := make(chan struct{})
	finished := make(chan error, 1)
	var executions atomic.Int32
	cn.GetTaskRunner().RegisterExecutor(task.TaskCode_TestOnly, func(taskCtx context.Context, _ task.Task) error {
		if executions.Add(1) != 1 {
			return errors.New("shutdown task must not retry to obtain a passing result")
		}
		// A task's cleanup transaction must not inherit the canceled taskCtx.
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		err := exec.ExecTxn(cleanupCtx, func(tx executor.TxnExecutor) error {
			result, err := tx.Exec("insert into close_logtail.t values (1)", executor.StatementOption{})
			if err != nil {
				return err
			}
			result.Close()
			close(entered)
			select {
			case <-taskCtx.Done():
			case <-cleanupCtx.Done():
				return cleanupCtx.Err()
			}
			// This fresh server fence is created only after frontend shutdown.
			// A real TN heartbeat must still reach this CN's waiter.
			fence, _ := owner.GetClock().Now()
			_, err = owner.GetTimestampWaiter().GetTimestamp(cleanupCtx, fence)
			return err
		}, executor.Options{}.WithDatabase("close_logtail").WithTxnIsolation(txn.TxnIsolation_RC).WithWaitCommittedLogApplied())
		finished <- err
		return err
	})
	ts, ok := cn.GetTaskService()
	require.True(t, ok)
	require.NoError(t, ts.CreateAsyncTask(ctx, task.TaskMetadata{ID: "close-logtail-drain", Executor: task.TaskCode_TestOnly}))
	select {
	case <-entered:
	case err := <-finished:
		t.Fatalf("task failed before drain barrier: %v", err)
	case <-ctx.Done():
		t.Fatal("task did not enter transaction")
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- cn.Close() }()
	select {
	case err := <-finished:
		require.NoError(t, err, "logtail stopped before task transaction drained")
	case <-ctx.Done():
		t.Fatal("task did not drain")
	}
	select {
	case err := <-closeDone:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("CN close did not finish")
	}
	require.Equal(t, ServiceClosed, cn.Status())
	require.Equal(t, int32(1), executions.Load())
}
