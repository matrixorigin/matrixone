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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"io"
	"net"
	"syscall"
	"testing"
	"time"

	mruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/stretchr/testify/require"
)

type taskReadinessStorage struct {
	taskservice.TaskStorage
	query func(context.Context) error
}

func (s *taskReadinessStorage) QueryDaemonTask(ctx context.Context, _ ...taskservice.Condition) ([]task.DaemonTask, error) {
	return nil, s.query(ctx)
}

type taskReadyCN struct {
	CNService
	service taskservice.TaskService
}

func (s *taskReadyCN) ID() string                                      { return "cn" }
func (s *taskReadyCN) GetTaskService() (taskservice.TaskService, bool) { return s.service, true }

type taskReadyTN struct {
	TNService
	service taskservice.TaskService
}

func (s *taskReadyTN) ID() string                                      { return "tn" }
func (s *taskReadyTN) GetTaskService() (taskservice.TaskService, bool) { return s.service, true }

type taskReadyLog struct {
	LogService
	service taskservice.TaskService
}

func (s *taskReadyLog) ID() string                                      { return "log" }
func (s *taskReadyLog) GetTaskService() (taskservice.TaskService, bool) { return s.service, true }

func TestTaskServiceWaitAdaptersObserveStorage(t *testing.T) {
	for _, role := range []string{"cn", "tn", "log"} {
		t.Run(role, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			svc := taskservice.NewTaskService(mruntime.DefaultRuntime(), &taskReadinessStorage{TaskStorage: taskservice.NewMemTaskStorage(), query: func(readCtx context.Context) error {
				close(entered)
				select {
				case <-release:
					return nil
				case <-readCtx.Done():
					return readCtx.Err()
				}
			}})
			defer func() { cancel(); <-done; require.NoError(t, svc.Close()) }()
			c := &testCluster{t: t}
			var wait func()
			switch role {
			case "cn":
				c.cn.svcs = []CNService{&taskReadyCN{service: svc}}
				wait = func() { c.WaitCNStoreTaskServiceCreatedIndexed(ctx, 0) }
			case "tn":
				c.tn.svcs = []TNService{&taskReadyTN{service: svc}}
				wait = func() { c.WaitTNStoreTaskServiceCreatedIndexed(ctx, 0) }
			case "log":
				c.log.svcs = []LogService{&taskReadyLog{service: svc}}
				wait = func() { c.WaitLogStoreTaskServiceCreatedIndexed(ctx, 0) }
			}
			go func() { defer close(done); wait() }()
			select {
			case <-entered:
			case <-done:
				t.Fatal("holder publication admitted before storage observation")
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			select {
			case <-done:
				t.Fatal("admitted while storage read was blocked")
			default:
			}
			close(release)
			select {
			case <-done:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		})
	}
}

type taskClosingProbe struct {
	Cluster
	close func() error
}

func (c *taskClosingProbe) Close() error { return c.close() }

func TestTaskFixtureCancelsBeforeClose(t *testing.T) {
	for _, count := range []int{0, 3} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			guard, stop := context.WithTimeout(t.Context(), 5*time.Second)
			defer stop()
			entered, exited := make(chan struct{}, count), make(chan struct{}, count)
			for i := 0; i < count; i++ {
				go func() { entered <- struct{}{}; <-ctx.Done(); exited <- struct{}{} }()
			}
			for i := 0; i < count; i++ {
				select {
				case <-entered:
				case <-guard.Done():
					t.Fatal(guard.Err())
				}
			}
			closed := false
			fixture := &taskTestCluster{cancel: cancel, Cluster: &taskClosingProbe{close: func() error {
				require.ErrorIs(t, ctx.Err(), context.Canceled)
				for i := 0; i < count; i++ {
					select {
					case <-exited:
					case <-guard.Done():
						return guard.Err()
					}
				}
				closed = true
				return nil
			}}}
			require.NoError(t, fixture.Close())
			require.True(t, closed, "cluster cleanup must be reached without an executor receiver")
		})
	}
}

// The real in-memory storage applies the same task-ID predicates as SQL. A
// concurrently running system task must never satisfy the owned-task oracle.
func TestTaskWaitIgnoresUnrelatedAndTransientRows(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	store := taskservice.NewMemTaskStorage()
	var phases []task.AsyncTask
	observed := 0
	wrapped := &taskObservationStorage{TaskStorage: store}
	svc := taskservice.NewTaskService(mruntime.DefaultRuntime(), wrapped)
	defer func() { require.NoError(t, svc.Close()) }()
	wrapped.query = func(readCtx context.Context, conds ...taskservice.Condition) ([]task.AsyncTask, error) {
		return store.QueryAsyncTask(readCtx, conds...)
	}
	original := createOwnedTask(t, ctx, svc, t.Name())
	_, err := store.AddAsyncTask(ctx, task.AsyncTask{Metadata: task.TaskMetadata{ID: "system"}, Status: task.TaskStatus_Running, TaskRunner: "survivor", Epoch: 20})
	require.NoError(t, err)
	phases = []task.AsyncTask{original, original, original, original, original, original}
	phases[0].Status = task.TaskStatus_Created
	phases[1].Status = task.TaskStatus_Running
	phases[1].TaskRunner = ""
	phases[2].Status = task.TaskStatus_Running
	phases[2].TaskRunner = "old"
	phases[2].Epoch = 1
	phases[3].Status = task.TaskStatus_Running
	phases[3].TaskRunner = "wrong"
	phases[3].Epoch = 2
	phases[4].Status = task.TaskStatus_Running
	phases[4].TaskRunner = "survivor"
	phases[4].Epoch = 1
	phases[5].Status = task.TaskStatus_Running
	phases[5].TaskRunner = "survivor"
	phases[5].Epoch = 2
	// Each controlled read publishes the next state into the authoritative store.
	wrapped.query = func(readCtx context.Context, conds ...taskservice.Condition) ([]task.AsyncTask, error) {
		require.Equal(t, ctx, readCtx, "observations must retain the original deadline")
		if observed == 0 {
			observed++
			return []task.AsyncTask{{ID: 999}}, fmt.Errorf("refresh: %w", io.EOF)
		}
		index := observed - 1
		if index >= len(phases) {
			_, err := store.DeleteAsyncTask(readCtx, taskservice.WithTaskIDCond(taskservice.EQ, original.ID))
			if err != nil {
				return nil, err
			}
			observed++
			return store.QueryAsyncTask(readCtx, conds...)
		}
		_, err := store.UpdateAsyncTask(readCtx, []task.AsyncTask{phases[index]})
		if err != nil {
			return nil, err
		}
		observed++
		return store.QueryAsyncTask(readCtx, conds...)
	}
	waitTaskExpired(t, ctx, svc, original)
	require.Equal(t, len(phases)+2, observed)
	rows, err := store.QueryAsyncTask(ctx)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "system", rows[0].Metadata.ID)

}

type taskObservationStorage struct {
	taskservice.TaskStorage
	query func(context.Context, ...taskservice.Condition) ([]task.AsyncTask, error)
}

func (s *taskObservationStorage) QueryAsyncTask(ctx context.Context, conds ...taskservice.Condition) ([]task.AsyncTask, error) {
	return s.query(ctx, conds...)
}

func TestTaskFailoverReadPolicy(t *testing.T) {
	refused := &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
	for _, tc := range []struct {
		name  string
		err   error
		retry bool
	}{
		{"wrapped refusal", fmt.Errorf("read: %w", refused), true},
		{"eof", io.EOF, true}, {"reset", &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}, true},
		{"not ready", fmt.Errorf("refresh: %w", taskservice.ErrNotReady), true},
		{"semantic", moerr.NewInvalidStateNoCtx("other task state"), false},
		{"unclassified", errors.New("SQL operation failed"), false},
		{"closed client", moerr.NewClientClosed(context.Background()), false},
		{"cancel precedence", errors.Join(refused, context.Canceled), false},
		{"deadline precedence", fmt.Errorf("read: %w", context.DeadlineExceeded), false},
	} {
		t.Run(tc.name, func(t *testing.T) { require.Equal(t, tc.retry, retryTaskFailoverRead(tc.err)) })
	}
}
