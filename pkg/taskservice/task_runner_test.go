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

	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/util"
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
		mustAddTestAsyncTask(t, store, 1, newTestAsyncTask("t1"))
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
		mustAddTestAsyncTask(t, store, 1, newTestAsyncTask("t1"))
		mustAddTestAsyncTask(t, store, 1, newTestAsyncTask("t2"))
		mustAllocTestTask(t, s, store, map[string]string{"t1": r.runnerID, "t2": r.runnerID})
		wg.Wait()
	}, WithRunnerParallelism(2),
		WithRunnerFetchInterval(time.Millisecond))
}

func TestTooMuchTasksWillBlockAndEventuallyCanBeExecuted(t *testing.T) {
	runTaskRunnerTest(t, func(r *taskRunner, s TaskService, store TaskStorage) {
		c := make(chan struct{})
		continueC := make(chan struct{})
		v := atomic.Uint32{}
		wait := time.Millisecond * 200
		r.RegisterExecutor(0, func(ctx context.Context, task task.Task) error {
			n := v.Add(1)
			if n == 2 {
				defer close(c) // second task close the chan
			}
			if n == 1 {
				time.Sleep(wait) // block first task
				<-continueC
			}

			return nil
		})
		mustAddTestAsyncTask(t, store, 1, newTestAsyncTask("t1"))
		mustAddTestAsyncTask(t, store, 1, newTestAsyncTask("t2"))
		mustAllocTestTask(t, s, store, map[string]string{"t1": r.runnerID, "t2": r.runnerID})
		select {
		case <-c:
			assert.Fail(t, "must block")
		case <-time.After(wait):
			assert.Equal(t, uint32(1), v.Load())
			close(continueC) // second task can be run
		}
		<-c
		assert.Equal(t, uint32(2), v.Load())
	}, WithRunnerParallelism(1),
		WithRunnerFetchInterval(time.Millisecond))
}

func TestHeartbeatWithRunningTask(t *testing.T) {
	runTaskRunnerTest(t, func(r *taskRunner, s TaskService, store TaskStorage) {
		c := make(chan struct{})
		completeC := make(chan struct{})
		n := atomic.Uint32{}
		r.RegisterExecutor(0, func(ctx context.Context, task task.Task) error {
			if n.Add(1) == 2 {
				close(c)
			}
			<-completeC
			return nil
		})
		mustAddTestAsyncTask(t, store, 1, newTestAsyncTask("t1"))
		mustAddTestAsyncTask(t, store, 1, newTestAsyncTask("t2"))
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
		n := atomic.Uint32{}
		r.RegisterExecutor(0, func(ctx context.Context, task task.Task) error {
			if n.Add(1) == 1 {
				return moerr.NewInternalError(context.TODO(), "error")
			}
			close(c)
			return nil
		})
		v := newTestAsyncTask("t1")
		v.Metadata.Options.MaxRetryTimes = 1
		mustAddTestAsyncTask(t, store, 1, v)
		mustAllocTestTask(t, s, store, map[string]string{"t1": r.runnerID})
		<-c
		assert.Equal(t, uint32(2), n.Load())
	}, WithRunnerParallelism(2),
		WithRunnerHeartbeatInterval(time.Millisecond),
		WithRunnerFetchInterval(time.Millisecond))
}

func TestRunTaskWithDisableRetry(t *testing.T) {
	runTaskRunnerTest(t, func(r *taskRunner, s TaskService, store TaskStorage) {
		c := make(chan struct{})
		n := atomic.Uint32{}
		r.RegisterExecutor(0, func(ctx context.Context, task task.Task) error {
			close(c)
			if n.Add(1) == 1 {
				return moerr.NewInternalError(context.TODO(), "error")
			}
			return nil
		})
		v := newTestAsyncTask("t1")
		v.Metadata.Options.MaxRetryTimes = 0
		mustAddTestAsyncTask(t, store, 1, v)
		mustAllocTestTask(t, s, store, map[string]string{"t1": r.runnerID})
		<-c
		mustWaitTestTaskHasExecuteResult(t, store, 1)
		v = mustGetTestAsyncTask(t, store, 1)[0]
		assert.Equal(t, task.ResultCode_Failed, v.ExecuteResult.Code)
	}, WithRunnerParallelism(2),
		WithRunnerHeartbeatInterval(time.Millisecond),
		WithRunnerFetchInterval(time.Millisecond))
}

func TestCancelRunningTask(t *testing.T) {
	runTaskRunnerTest(t, func(r *taskRunner, s TaskService, store TaskStorage) {
		cancelC := make(chan struct{})
		r.RegisterExecutor(0, func(ctx context.Context, task task.Task) error {
			select {
			case <-ctx.Done():
			case <-cancelC:
			}
			return nil
		})
		v := newTestAsyncTask("t1")
		v.Metadata.Options.MaxRetryTimes = 0
		mustAddTestAsyncTask(t, store, 1, v)
		mustAllocTestTask(t, s, store, map[string]string{"t1": r.runnerID})
		v = mustGetTestAsyncTask(t, store, 1)[0]
		v.Epoch++
		mustUpdateTestAsyncTask(t, store, 1, []task.AsyncTask{v})
		close(cancelC)
		for v := mustGetTestAsyncTask(t, store, 1)[0]; v.Status != task.TaskStatus_Completed; v = mustGetTestAsyncTask(t, store, 1)[0] {
			time.Sleep(10 * time.Millisecond)
		}
		timeout := time.After(10 * time.Second)
		for {
			select {
			case <-timeout:
				require.Fail(t, "task not removed after 10 seconds")
			default:
			}
			r.runningTasks.RLock()
			if len(r.runningTasks.m) == 0 {
				r.runningTasks.RUnlock()
				break
			}
			r.runningTasks.RUnlock()
			time.Sleep(time.Millisecond * 10)
		}
		r.runningTasks.RLock()
		defer r.runningTasks.RUnlock()
		assert.Equal(t, 0, len(r.runningTasks.m))
	}, WithRunnerParallelism(2),
		WithRunnerHeartbeatInterval(time.Millisecond),
		WithRunnerFetchInterval(time.Millisecond))
}

func TestDoHeartbeatInvalidTask(t *testing.T) {
	runTaskRunnerTest(t, func(r *taskRunner, s TaskService, store TaskStorage) {
		ctx, cancel := context.WithCancelCause(context.TODO())
		r.runningTasks.Lock()
		r.runningTasks.m = make(map[uint64]runningTask)
		r.runningTasks.m[1] = runningTask{
			task:   task.AsyncTask{},
			ctx:    ctx,
			cancel: cancel,
		}
		r.runningTasks.Unlock()
		r.doHeartbeat(context.Background())

		r.runningTasks.RLock()
		defer r.runningTasks.RUnlock()
		assert.Equal(t, 1, len(r.runningTasks.m))
	}, WithRunnerParallelism(1),
		WithRunnerFetchInterval(time.Millisecond),
		WithRunnerHeartbeatInterval(time.Millisecond))
}

func TestRemoveRunningTask(t *testing.T) {
	runTaskRunnerTest(t, func(r *taskRunner, s TaskService, store TaskStorage) {
		r.RegisterExecutor(0, func(ctx context.Context, task task.Task) error {
			return nil
		})
		mustAddTestAsyncTask(t, store, 1, newTestAsyncTask("t1"))
		mustAllocTestTask(t, s, store, map[string]string{"t1": r.runnerID})

		task := mustGetTestAsyncTask(t, store, 1)[0]
		r.addToWait(context.Background(), task)
		r.removeRunningTask(task.ID)
		time.Sleep(1 * time.Second)
		timeout := time.After(5 * time.Second)
		for {
			select {
			case <-timeout:
				require.Fail(t, "timeout waiting for task to be removed and added to completedTasks")
			default:
				r.runningTasks.RLock()
				_, exists := r.runningTasks.m[task.ID]
				if len(r.runningTasks.completedTasks) != 0 {
					_, completed := r.runningTasks.completedTasks[task.ID]
					if !exists && completed {
						r.runningTasks.RUnlock()
						return
					}
				} else {
					if !exists {
						r.runningTasks.RUnlock()
						return
					}
				}
				r.runningTasks.RUnlock()
				time.Sleep(10 * time.Millisecond)
			}
		}
	}, WithRunnerParallelism(1),
		WithRunnerFetchInterval(time.Millisecond))
}

func TestRemoveRunningTaskNotExists(t *testing.T) {
	runTaskRunnerTest(t, func(r *taskRunner, s TaskService, store TaskStorage) {
		r.removeRunningTask(999)

		r.runningTasks.RLock()
		defer r.runningTasks.RUnlock()
		_, exists := r.runningTasks.m[999]
		assert.False(t, exists, "non-existent task should not be in runningTasks")
		_, completed := r.runningTasks.completedTasks[999]
		assert.True(t, completed, "non-existent task should be added to completedTasks")
	}, WithRunnerParallelism(1),
		WithRunnerFetchInterval(time.Hour))
}

func TestRunnerOptionSetters(t *testing.T) {
	r := &taskRunner{}
	getClient := func() util.HAKeeperClient { return nil }

	WithRunnerFetchLimit(11)(r)
	WithRunnerParallelism(3)(r)
	WithRunnerMaxWaitTasks(9)(r)
	WithRunnerFetchInterval(2 * time.Second)(r)
	WithRunnerFetchTimeout(3 * time.Second)(r)
	WithRunnerRetryInterval(4 * time.Second)(r)
	WithRunnerHeartbeatInterval(5 * time.Second)(r)
	WithRunnerHeartbeatTimeout(6 * time.Second)(r)
	WithHaKeeperClient(getClient)(r)
	WithCnUUID("cn-1")(r)

	require.Equal(t, 11, r.options.queryLimit)
	require.Equal(t, 3, r.options.parallelism)
	require.Equal(t, 9, r.options.maxWaitTasks)
	require.Equal(t, 2*time.Second, r.options.fetchInterval)
	require.Equal(t, 3*time.Second, r.options.fetchTimeout)
	require.Equal(t, 4*time.Second, r.options.retryInterval)
	require.Equal(t, 5*time.Second, r.options.heartbeatInterval)
	require.Equal(t, 6*time.Second, r.options.heartbeatTimeout)
	require.NotNil(t, r.getClient)
	require.Nil(t, r.getClient())
	require.Equal(t, "cn-1", r.cnUUID)
}

func TestRunnerWithOptionsSetter(t *testing.T) {
	r := &taskRunner{}
	WithOptions(
		13,
		7,
		17,
		8*time.Second,
		9*time.Second,
		10*time.Second,
		11*time.Second,
		12*time.Second,
	)(r)

	require.Equal(t, 13, r.options.queryLimit)
	require.Equal(t, 7, r.options.parallelism)
	require.Equal(t, 17, r.options.maxWaitTasks)
	require.Equal(t, 8*time.Second, r.options.fetchInterval)
	require.Equal(t, 9*time.Second, r.options.fetchTimeout)
	require.Equal(t, 10*time.Second, r.options.retryInterval)
	require.Equal(t, 11*time.Second, r.options.heartbeatInterval)
	require.Equal(t, 12*time.Second, r.options.heartbeatTimeout)
}

func TestTaskRunnerAccessorsAndIdempotentStartStop(t *testing.T) {
	store := NewMemTaskStorage()
	s := NewTaskService(runtime.DefaultRuntime(), store)
	r := NewTaskRunner("runner-test", s, func(string) bool { return true }, WithRunnerParallelism(2)).(*taskRunner)

	require.Equal(t, "runner-test", r.ID())
	require.Equal(t, 2, r.Parallelism())
	require.Nil(t, r.GetExecutor(task.TaskCode_TestOnly))

	r.RegisterExecutor(task.TaskCode_TestOnly, func(context.Context, task.Task) error { return nil })
	require.NotNil(t, r.GetExecutor(task.TaskCode_TestOnly))

	require.NoError(t, r.Start())
	require.NoError(t, r.Start())
	require.NoError(t, r.Stop())
	require.NoError(t, r.Stop())
	require.NoError(t, s.Close())
}

func TestReleaseParallelPanicAndSuccess(t *testing.T) {
	r := &taskRunner{
		parallelismC: make(chan struct{}, 1),
	}

	require.Panics(t, func() {
		r.releaseParallel()
	})

	r.parallelismC <- struct{}{}
	require.NotPanics(t, func() {
		r.releaseParallel()
	})
}

func runTaskRunnerTest(t *testing.T,
	testFunc func(r *taskRunner, s TaskService, store TaskStorage),
	opts ...RunnerOption) {
	defer leaktest.AfterTest(t)()
	store := NewMemTaskStorage()
	s := NewTaskService(runtime.DefaultRuntime(), store)
	defer func() {
		assert.NoError(t, s.Close())
	}()

	opts = append(opts, WithRunnerLogger(logutil.GetPanicLoggerWithLevel(zap.DebugLevel)))
	r := NewTaskRunner("r1", s, func(string) bool {
		return true
	}, opts...)

	require.NoError(t, r.Start())
	defer func() {
		require.NoError(t, r.Stop())
	}()
	testFunc(r.(*taskRunner), s, store)
}

func mustAllocTestTask(t *testing.T, s TaskService, store TaskStorage, alloc map[string]string) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	tasks := mustGetTestAsyncTask(t, store, len(alloc), WithTaskStatusCond(task.TaskStatus_Created))
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
			tasks := mustGetTestAsyncTask(t, store, expectHasHeartbeatCount,
				WithTaskStatusCond(task.TaskStatus_Running))
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
			tasks, err := store.QueryAsyncTask(ctx, WithTaskStatusCond(task.TaskStatus_Completed))
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
