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

package taskservice

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestRunTask(t *testing.T) {
	runTaskRunnerTest(t, func(r *taskRunner, s TaskService, store TaskStorage) {
		c := make(chan struct{})
		r.RegisterExecutor(0, func(ctx context.Context, task task.Task) error {
			defer close(c)
			return nil
		})
		mustAddTestTask(t, store, 1, newTestTask("t1"))
		mustAllocTestTask(t, s, store, map[string]string{"t1": r.runnerID})
		<-c
	}, WithRunnerParallelism(1),
		WithRunnerFetchInterval(time.Millisecond))
}

func TestRunTasksInParallel(t *testing.T) {
	runTaskRunnerTest(t, func(r *taskRunner, s TaskService, store TaskStorage) {
		wg := &sync.WaitGroup{}
		wg.Add(2)
		r.RegisterExecutor(0, func(ctx context.Context, task task.Task) error {
			defer wg.Done()
			time.Sleep(time.Millisecond * 200)
			return nil
		})
		mustAddTestTask(t, store, 1, newTestTask("t1"))
		mustAddTestTask(t, store, 1, newTestTask("t2"))
		mustAllocTestTask(t, s, store, map[string]string{"t1": r.runnerID, "t2": r.runnerID})
		wg.Wait()
	}, WithRunnerParallelism(2),
		WithRunnerFetchInterval(time.Millisecond))
}

func TestTooMuchTasksWillBlockAndEventuallyCanBeExecuted(t *testing.T) {
	runTaskRunnerTest(t, func(r *taskRunner, s TaskService, store TaskStorage) {
		c := make(chan struct{})
		continueC := make(chan struct{})
		v := uint32(0)
		wait := time.Millisecond * 200
		r.RegisterExecutor(0, func(ctx context.Context, task task.Task) error {
			n := atomic.AddUint32(&v, 1)
			if n == 2 {
				defer close(c) // second task close the chan
			}
			if n == 1 {
				time.Sleep(wait) // block first task
				<-continueC
			}

			return nil
		})
		mustAddTestTask(t, store, 1, newTestTask("t1"))
		mustAddTestTask(t, store, 1, newTestTask("t2"))
		mustAllocTestTask(t, s, store, map[string]string{"t1": r.runnerID, "t2": r.runnerID})
		select {
		case <-c:
			assert.Fail(t, "must block")
		case <-time.After(wait):
			assert.Equal(t, uint32(1), atomic.LoadUint32(&v))
			close(continueC) // second task can be run
		}
		<-c
		assert.Equal(t, uint32(2), atomic.LoadUint32(&v))
	}, WithRunnerParallelism(1),
		WithRunnerFetchInterval(time.Millisecond))
}

func TestHeartbeatWithRunningTask(t *testing.T) {
	runTaskRunnerTest(t, func(r *taskRunner, s TaskService, store TaskStorage) {
		c := make(chan struct{})
		completeC := make(chan struct{})
		n := uint32(0)
		r.RegisterExecutor(0, func(ctx context.Context, task task.Task) error {
			if atomic.AddUint32(&n, 1) == 2 {
				close(c)
			}
			<-completeC
			return nil
		})
		mustAddTestTask(t, store, 1, newTestTask("t1"))
		mustAddTestTask(t, store, 1, newTestTask("t2"))
		mustAllocTestTask(t, s, store, map[string]string{"t1": r.runnerID, "t2": r.runnerID})
		<-c
		mustWaitTestTaskHasHeartbeat(t, store, 2)
		close(completeC)
	}, WithRunnerParallelism(2),
		WithRunnerHeartbeatInterval(time.Millisecond),
		WithRunnerFetchInterval(time.Millisecond))
}

func TestRunTaskWithRetry(t *testing.T) {
	runTaskRunnerTest(t, func(r *taskRunner, s TaskService, store TaskStorage) {
		c := make(chan struct{})
		n := uint32(0)
		r.RegisterExecutor(0, func(ctx context.Context, task task.Task) error {
			if atomic.AddUint32(&n, 1) == 1 {
				return moerr.NewInternalError("error")
			}
			close(c)
			return nil
		})
		v := newTestTask("t1")
		v.Metadata.Options.MaxRetryTimes = 1
		mustAddTestTask(t, store, 1, v)
		mustAllocTestTask(t, s, store, map[string]string{"t1": r.runnerID})
		<-c
		assert.Equal(t, uint32(2), n)
	}, WithRunnerParallelism(2),
		WithRunnerHeartbeatInterval(time.Millisecond),
		WithRunnerFetchInterval(time.Millisecond))
}

func TestRunTaskWithDisableRetry(t *testing.T) {
	runTaskRunnerTest(t, func(r *taskRunner, s TaskService, store TaskStorage) {
		c := make(chan struct{})
		n := uint32(0)
		r.RegisterExecutor(0, func(ctx context.Context, task task.Task) error {
			close(c)
			if atomic.AddUint32(&n, 1) == 1 {
				return moerr.NewInternalError("error")
			}
			return nil
		})
		v := newTestTask("t1")
		v.Metadata.Options.MaxRetryTimes = 0
		mustAddTestTask(t, store, 1, v)
		mustAllocTestTask(t, s, store, map[string]string{"t1": r.runnerID})
		<-c
		mustWaitTestTaskHasExecuteResult(t, store, 1)
		v = mustGetTestTask(t, store, 1)[0]
		assert.Equal(t, task.ResultCode_Failed, v.ExecuteResult.Code)
	}, WithRunnerParallelism(2),
		WithRunnerHeartbeatInterval(time.Millisecond),
		WithRunnerFetchInterval(time.Millisecond))
}

func TestCancelRunningTask(t *testing.T) {
	runTaskRunnerTest(t, func(r *taskRunner, s TaskService, store TaskStorage) {
		c := make(chan struct{})
		cancelC := make(chan struct{})
		r.RegisterExecutor(0, func(ctx context.Context, task task.Task) error {
			close(c)
			<-ctx.Done()
			close(cancelC)
			return nil
		})
		v := newTestTask("t1")
		v.Metadata.Options.MaxRetryTimes = 0
		mustAddTestTask(t, store, 1, v)
		mustAllocTestTask(t, s, store, map[string]string{"t1": r.runnerID})
		<-c
		v = mustGetTestTask(t, store, 1)[0]
		v.Epoch++
		mustUpdateTestTask(t, store, 1, []task.Task{v})
		<-cancelC
		r.mu.RLock()
		defer r.mu.RUnlock()
		assert.Equal(t, 0, len(r.mu.runningTasks))
	}, WithRunnerParallelism(2),
		WithRunnerHeartbeatInterval(time.Millisecond),
		WithRunnerFetchInterval(time.Millisecond))
}

func runTaskRunnerTest(t *testing.T,
	testFunc func(r *taskRunner, s TaskService, store TaskStorage),
	opts ...RunnerOption) {
	store := NewMemTaskStorage()
	s := NewTaskService(runtime.DefaultRuntime(), store)
	defer func() {
		assert.NoError(t, s.Close())
	}()

	opts = append(opts, WithRunnerLogger(logutil.GetPanicLoggerWithLevel(zap.DebugLevel)))
	r := NewTaskRunner("r1", s, opts...)

	require.NoError(t, r.Start())
	defer func() {
		require.NoError(t, r.Stop())
	}()
	testFunc(r.(*taskRunner), s, store)
}

func mustAllocTestTask(t *testing.T, s TaskService, store TaskStorage, alloc map[string]string) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	tasks := mustGetTestTask(t, store, len(alloc), WithTaskStatusCond(EQ, task.TaskStatus_Created))
	n := 0
	for _, v := range tasks {
		if runner, ok := alloc[v.Metadata.ID]; ok {
			require.NoError(t, s.Allocate(ctx, v, runner))
			n++
		}
	}
	if n != len(alloc) {
		require.Fail(t, "task not found")
	}
}

func mustWaitTestTaskHasHeartbeat(t *testing.T, store TaskStorage, expectHasHeartbeatCount int) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			require.Fail(t, "wait heatbeat timeout")
		default:
			tasks := mustGetTestTask(t, store, expectHasHeartbeatCount,
				WithTaskStatusCond(EQ, task.TaskStatus_Running))
			n := 0
			for _, v := range tasks {
				if v.LastHeartbeat > 0 {
					n++
				}
			}
			if n == len(tasks) {
				return
			}
		}
	}
}

func mustWaitTestTaskHasExecuteResult(t *testing.T, store TaskStorage, expectCount int) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			require.Fail(t, "wait execute result timeout")
		default:
			tasks, err := store.Query(ctx, WithTaskStatusCond(EQ, task.TaskStatus_Completed))
			require.NoError(t, err)
			if len(tasks) != expectCount {
				break
			}
			n := 0
			for _, v := range tasks {
				if v.ExecuteResult != nil {
					n++
				}
			}
			if n == len(tasks) {
				return
			}
		}
	}
}
