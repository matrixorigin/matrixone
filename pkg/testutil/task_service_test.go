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

package testutil

import (
	"context"
	"errors"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	mruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

type delayedTaskServiceGetter struct {
	mu      sync.RWMutex
	service taskservice.TaskService
	called  chan struct{}
	once    sync.Once
}

func newDelayedTaskServiceGetter() *delayedTaskServiceGetter {
	return &delayedTaskServiceGetter{called: make(chan struct{})}
}

func (g *delayedTaskServiceGetter) GetTaskService() (taskservice.TaskService, bool) {
	g.once.Do(func() { close(g.called) })
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.service, g.service != nil
}

func (g *delayedTaskServiceGetter) set(service taskservice.TaskService) {
	g.mu.Lock()
	g.service = service
	g.mu.Unlock()
}

func (g *delayedTaskServiceGetter) Start() error { return nil }
func (g *delayedTaskServiceGetter) Close() error { return nil }

func TestWaitTaskServiceReadyObservesOwnedService(t *testing.T) {
	getter := newDelayedTaskServiceGetter()
	service := taskservice.NewTaskService(
		mruntime.DefaultRuntime(), taskservice.NewMemTaskStorage())
	defer func() { require.NoError(t, service.Close()) }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- WaitTaskServiceReady(ctx, getter, time.Millisecond)
	}()
	<-getter.called
	getter.set(service)
	require.NoError(t, <-done)
}

func TestWaitTaskServiceReadyHonorsCancellation(t *testing.T) {
	getter := newDelayedTaskServiceGetter()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- WaitTaskServiceReady(ctx, getter, time.Hour)
	}()
	<-getter.called
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

type readinessTaskStorage struct {
	taskservice.TaskStorage
	query func(context.Context) error
}

func (s *readinessTaskStorage) QueryDaemonTask(ctx context.Context, _ ...taskservice.Condition) ([]task.DaemonTask, error) {
	return nil, s.query(ctx)
}

func TestWaitTaskServiceReadyObservesStorage(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	queried, release := make(chan struct{}), make(chan struct{})
	store := taskservice.NewMemTaskStorage()
	calls := 0
	service := taskservice.NewTaskService(mruntime.DefaultRuntime(), &readinessTaskStorage{
		TaskStorage: store,
		query: func(ctx context.Context) error {
			calls++
			if calls == 1 {
				return fmt.Errorf("initial refresh: %w", taskservice.ErrNotReady)
			}
			close(queried)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	})
	defer func() { require.NoError(t, service.Close()) }()
	getter := newDelayedTaskServiceGetter()
	getter.set(service)
	done := make(chan error, 1)
	go func() {
		err := WaitTaskServiceReady(ctx, getter, 0)
		if err == nil {
			_, err = store.AddDaemonTask(ctx, task.DaemonTask{Metadata: task.TaskMetadata{ID: "once"}})
		}
		done <- err
	}()
	joined := false
	defer func() {
		cancel()
		if !joined {
			<-done
		}
	}()
	select {
	case <-queried:
	case err := <-done:
		joined = true
		t.Fatalf("fixture admitted before storage observation: %v", err)
	case <-ctx.Done():
		t.Fatal("storage observation did not start")
	}
	rows, err := store.QueryDaemonTask(ctx)
	require.NoError(t, err)
	require.Empty(t, rows, "no business write before readiness")
	close(release)
	err = <-done
	joined = true
	require.NoError(t, err)
	require.Equal(t, 2, calls)
	rows, err = store.QueryDaemonTask(ctx)
	require.NoError(t, err)
	require.Len(t, rows, 1, "one business write after readiness")
}

func TestWaitTaskServiceReadyStorageErrors(t *testing.T) {
	for _, name := range []string{"fatal", "same-code error", "cancel during read", "cancel while not ready"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			want := errors.New("storage failure")
			if name == "same-code error" {
				want = moerr.NewInvalidStateNoCtx("task store not ready")
			}
			calls := 0
			service := taskservice.NewTaskService(mruntime.DefaultRuntime(), &readinessTaskStorage{
				TaskStorage: taskservice.NewMemTaskStorage(),
				query: func(context.Context) error {
					calls++
					if name == "fatal" || name == "same-code error" {
						return want
					}
					cancel()
					if name == "cancel while not ready" {
						return taskservice.ErrNotReady
					}
					return nil
				},
			})
			defer func() { require.NoError(t, service.Close()) }()
			getter := newDelayedTaskServiceGetter()
			getter.set(service)
			if name != "fatal" && name != "same-code error" {
				want = context.Canceled
			}
			require.ErrorIs(t, WaitTaskServiceReady(ctx, getter, time.Hour), want)
			require.Equal(t, 1, calls, "fatal errors and cancellation must not be retried")
		})
	}
}

func TestWaitTaskServiceReadyCancelsInflightStorageRead(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	entered, stopped := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(stopped)
		select {
		case <-entered:
			cancel()
		case <-ctx.Done():
		}
	}()
	defer func() { cancel(); <-stopped }()
	service := taskservice.NewTaskService(mruntime.DefaultRuntime(), &readinessTaskStorage{TaskStorage: taskservice.NewMemTaskStorage(), query: func(readCtx context.Context) error { close(entered); <-readCtx.Done(); return readCtx.Err() }})
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	getter := newDelayedTaskServiceGetter()
	getter.set(service)
	require.ErrorIs(t, WaitTaskServiceReady(ctx, getter, time.Hour), context.Canceled)
}
