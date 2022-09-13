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

	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunTask(t *testing.T) {
	runTaskRunnerTest(t, func(r *taskRunner, s TaskService, store TaskStorage) {
		c := make(chan struct{})
		r.RegisterExectuor(0, func(ctx context.Context, task task.Task) error {
			defer close(c)
			return nil
		})
		mustAddTestTask(t, store, 1, newTestTask("t1"))
		mustAllocTestTask(t, s, store, map[string]string{"t1": r.runnerID})
		<-c
	}, WithRunnerParallelism(1),
		WithRunnerFetchInterval(time.Millisecond))
}

func TestRunParalleTasks(t *testing.T) {
	runTaskRunnerTest(t, func(r *taskRunner, s TaskService, store TaskStorage) {
		wg := &sync.WaitGroup{}
		wg.Add(2)
		r.RegisterExectuor(0, func(ctx context.Context, task task.Task) error {
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
		r.RegisterExectuor(0, func(ctx context.Context, task task.Task) error {
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

func runTaskRunnerTest(t *testing.T,
	testFunc func(r *taskRunner, s TaskService, store TaskStorage),
	opts ...RunnerOption) {
	store := newMemTaskStorage()
	s := NewTaskService(store)
	defer func() {
		assert.NoError(t, s.Close())
	}()

	r, err := NewTaskRunner("r1", s, opts...)
	require.NoError(t, err)

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
	for _, v := range tasks {
		if runner, ok := alloc[v.Metadata.ID]; ok {
			require.NoError(t, s.Allocate(ctx, v, runner))
			return
		}
	}
	require.Fail(t, "task not found")
}
