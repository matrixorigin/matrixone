// Copyright 2021 - 2023 Matrix Origin
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

package taskservice

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/stretchr/testify/assert"
)

type mockActiveRoutine struct {
	pauseC  chan struct{}
	resumeC chan struct{}
	cancelC chan struct{}
}

func newMockActiveRoutine() *mockActiveRoutine {
	return &mockActiveRoutine{
		pauseC:  make(chan struct{}, 1),
		resumeC: make(chan struct{}, 1),
		cancelC: make(chan struct{}, 1),
	}
}

func (r *mockActiveRoutine) Pause() error {
	r.pauseC <- struct{}{}
	return nil
}

func (r *mockActiveRoutine) Resume() error {
	r.resumeC <- struct{}{}
	return nil
}

func (r *mockActiveRoutine) Cancel() error {
	r.cancelC <- struct{}{}
	return nil
}

func (r *mockActiveRoutine) Restart() error {
	return nil
}

func daemonTaskMetadata() task.TaskMetadata {
	return task.TaskMetadata{
		ID:       "-",
		Executor: task.TaskCode_ConnectorKafkaSink,
		Options: task.TaskOptions{
			MaxRetryTimes: 0,
			RetryInterval: 0,
			DelayDuration: 0,
			Concurrency:   0,
		},
	}
}

func newDaemonTaskForTest(id uint64, status task.TaskStatus, runner string) task.DaemonTask {
	nowTime := time.Now()
	t := task.DaemonTask{
		ID:         id,
		Metadata:   daemonTaskMetadata(),
		TaskStatus: status,
		TaskRunner: runner,
		CreateAt:   nowTime,
		UpdateAt:   nowTime,
		Details: &task.Details{
			AccountID: 0,
			Account:   "sys",
			Username:  "dump",
			Details: &task.Details_Connector{
				Connector: &task.ConnectorDetails{
					TableName: "d1.t1",
				},
			},
		},
	}
	return t
}

func TestRunDaemonTask(t *testing.T) {
	runTaskRunnerTest(t, func(r *taskRunner, s TaskService, store TaskStorage) {
		c := make(chan struct{})
		r.RegisterExecutor(task.TaskCode_ConnectorKafkaSink, func(ctx context.Context, task task.Task) error {
			defer close(c)
			return nil
		})
		mustAddTestDaemonTask(t, store, 1, newDaemonTaskForTest(1, task.TaskStatus_Created, r.runnerID))
		<-c
		tasks := mustGetTestDaemonTask(t, store, 1)
		assert.Equal(t, 1, len(tasks))
		tk := tasks[0]
		assert.Equal(t, task.TaskStatus_Running, tk.TaskStatus)
		assert.False(t, tk.CreateAt.IsZero())
		assert.False(t, tk.UpdateAt.IsZero())
		assert.Equal(t, r.runnerID, tk.TaskRunner)
		assert.False(t, tk.LastRun.IsZero())
	}, WithRunnerParallelism(1),
		WithRunnerFetchInterval(time.Millisecond))
}

func (r *taskRunner) testRegisterExecutor(t *testing.T, code task.TaskCode, started *atomic.Bool) {
	r.RegisterExecutor(code, func(ctx context.Context, task task.Task) error {
		ar := newMockActiveRoutine()
		assert.NoError(t, r.Attach(context.Background(), 1, ar))
		started.Store(true)
		for {
			select {
			case <-ar.cancelC:
				return nil

			case <-ar.pauseC:
				select {
				case <-ctx.Done():
					return nil
				case <-ar.cancelC:
					return nil
				case <-ar.resumeC:
				}

			case <-ctx.Done():
				return nil
			}
		}
	})
}

func expectTaskStatus(
	t *testing.T, store TaskStorage, dt task.DaemonTask, before task.TaskStatus, after task.TaskStatus,
) {
	dt.TaskStatus = before
	mustUpdateTestDaemonTask(t, store, 1, []task.DaemonTask{dt})
	timer := time.NewTimer(time.Second * 5)
	defer timer.Stop()
	ticker := time.NewTicker(time.Millisecond * 10)
	defer ticker.Stop()
FOR:
	for {
		select {
		case <-timer.C:
			panic("daemon task update timeout")
		case <-ticker.C:
			tasks := mustGetTestDaemonTask(t, store, 1, WithTaskIDCond(EQ, 1))
			assert.Equal(t, 1, len(tasks))
			tk := tasks[0]
			if tk.TaskStatus == after {
				break FOR
			}
		}
	}
}

func waitStarted(started *atomic.Bool, timeout time.Duration) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	ticker := time.NewTicker(time.Millisecond * 10)
	defer ticker.Stop()
	for {
		select {
		case <-timer.C:
			panic("start executor timeout")
		case <-ticker.C:
			if started.Load() {
				return
			}
		}
	}
}

func TestPauseResumeDaemonTask(t *testing.T) {
	runTaskRunnerTest(t, func(r *taskRunner, s TaskService, store TaskStorage) {
		dt := newDaemonTaskForTest(1, task.TaskStatus_Created, r.runnerID)
		mustAddTestDaemonTask(t, store, 1, dt)
		var started atomic.Bool
		r.testRegisterExecutor(t, task.TaskCode_ConnectorKafkaSink, &started)
		waitStarted(&started, time.Second*5)

		expectTaskStatus(t, store, dt, task.TaskStatus_PauseRequested, task.TaskStatus_Paused)
		expectTaskStatus(t, store, dt, task.TaskStatus_ResumeRequested, task.TaskStatus_Running)
	}, WithRunnerParallelism(1),
		WithRunnerFetchInterval(time.Millisecond))
}

func TestCancelDaemonTask(t *testing.T) {
	runTaskRunnerTest(t, func(r *taskRunner, s TaskService, store TaskStorage) {
		dt := newDaemonTaskForTest(1, task.TaskStatus_Created, r.runnerID)
		mustAddTestDaemonTask(t, store, 1, dt)
		var started atomic.Bool
		r.testRegisterExecutor(t, task.TaskCode_ConnectorKafkaSink, &started)
		waitStarted(&started, time.Second*5)

		expectTaskStatus(t, store, dt, task.TaskStatus_CancelRequested, task.TaskStatus_Canceled)
	}, WithRunnerParallelism(1),
		WithRunnerFetchInterval(time.Millisecond))
}
