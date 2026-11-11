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
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	goruntime "runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestTaskHolderCanCreateTaskService(t *testing.T) {
	defer leaktest.AfterTest(t)()
	store := NewMemTaskStorage()
	h := NewTaskServiceHolderWithTaskStorageFactorySelector(
		runtime.DefaultRuntime(),
		func(ctx context.Context, random bool) (string, error) { return "", nil },
		func(s1, s2, s3 string) TaskStorageFactory {
			return NewFixedTaskStorageFactory(store)
		})
	require.NoError(t, h.Create(logservicepb.CreateTaskService{
		User:         logservicepb.TaskTableUser{Username: "u", Password: "p"},
		TaskDatabase: "d",
	}))
	defer func() {
		require.NoError(t, h.Close())
	}()
	s, ok := h.Get()
	assert.True(t, ok)
	assert.NotNil(t, s)
	assert.Equal(t, store, s.GetStorage().(*refreshableTaskStorage).mu.store)
}

func TestTaskHolderCreateWithEmptyCommandReturnError(t *testing.T) {
	store := NewMemTaskStorage()
	h := NewTaskServiceHolderWithTaskStorageFactorySelector(
		runtime.DefaultRuntime(),
		func(context.Context, bool) (string, error) { return "", nil },
		func(s1, s2, s3 string) TaskStorageFactory {
			return NewFixedTaskStorageFactory(store)
		})
	assert.Error(t, h.Create(logservicepb.CreateTaskService{}))
}

func TestTaskHolderNotCreatedCanClose(t *testing.T) {
	store := NewMemTaskStorage()
	h := NewTaskServiceHolderWithTaskStorageFactorySelector(
		runtime.DefaultRuntime(),
		func(context.Context, bool) (string, error) { return "", nil },
		func(s1, s2, s3 string) TaskStorageFactory {
			return NewFixedTaskStorageFactory(store)
		})
	assert.NoError(t, h.Close())
}

func TestTaskHolderRejectsCreateAfterClose(t *testing.T) {
	defer leaktest.AfterTest(t)()
	store := NewMemTaskStorage()
	h := NewTaskServiceHolderWithTaskStorageFactorySelector(
		runtime.DefaultRuntime(),
		func(context.Context, bool) (string, error) { return "", nil },
		func(string, string, string) TaskStorageFactory {
			return NewFixedTaskStorageFactory(store)
		})

	require.NoError(t, h.Close())
	err := h.Create(logservicepb.CreateTaskService{
		User:         logservicepb.TaskTableUser{Username: "u", Password: "p"},
		TaskDatabase: "d",
	})
	service, ok := h.Get()
	if ok {
		defer func() {
			require.NoError(t, service.Close())
		}()
	}
	require.ErrorIs(t, err, ErrNotReady)
	require.False(t, ok)
	require.Nil(t, service)
}

func TestTaskHolderConcurrentCreateAndClose(t *testing.T) {
	defer leaktest.AfterTest(t)()
	command := logservicepb.CreateTaskService{
		User:         logservicepb.TaskTableUser{Username: "u", Password: "p"},
		TaskDatabase: "d",
	}

	for range 50 {
		store := NewMemTaskStorage()
		h := NewTaskServiceHolderWithTaskStorageFactorySelector(
			runtime.DefaultRuntime(),
			func(context.Context, bool) (string, error) { return "", nil },
			func(string, string, string) TaskStorageFactory {
				return NewFixedTaskStorageFactory(store)
			})

		start := make(chan struct{})
		var wg sync.WaitGroup
		var createErr, closeErr error
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			createErr = h.Create(command)
		}()
		go func() {
			defer wg.Done()
			<-start
			closeErr = h.Close()
		}()
		close(start)
		wg.Wait()

		require.NoError(t, closeErr)
		if createErr != nil {
			require.ErrorIs(t, createErr, ErrNotReady)
		}
		service, ok := h.Get()
		if ok {
			require.NoError(t, service.Close())
		}
		require.False(t, ok)
		require.Nil(t, service)
	}
}

func TestTaskHolderCanClose(t *testing.T) {
	store := NewMemTaskStorage()
	h := NewTaskServiceHolderWithTaskStorageFactorySelector(
		runtime.DefaultRuntime(),
		func(context.Context, bool) (string, error) { return "", nil },
		func(s1, s2, s3 string) TaskStorageFactory {
			return NewFixedTaskStorageFactory(store)
		})
	require.NoError(t, h.Create(logservicepb.CreateTaskService{
		User:         logservicepb.TaskTableUser{Username: "u", Password: "p"},
		TaskDatabase: "d",
	}))
	assert.NoError(t, h.Close())
}

func TestRefreshTaskStorageCanRefresh(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.TODO()

	stores := map[string]TaskStorage{
		"s1": NewMemTaskStorage(),
		"s2": NewMemTaskStorage(),
	}
	address := "s1"
	s := newRefreshableTaskStorage(
		runtime.DefaultRuntime(),
		func(context.Context, bool) (string, error) { return address, nil },
		&testStorageFactory{stores: stores}).(*refreshableTaskStorage)
	defer func() {
		require.NoError(t, s.Close())
	}()

	s.mu.RLock()
	assert.Same(t, stores["s1"], s.mu.store)
	assert.Equal(t, "s1", s.mu.lastAddress)
	s.mu.RUnlock()

	s.refresh(ctx, "s2")
	s.mu.RLock()
	assert.Same(t, stores["s1"], s.mu.store)
	assert.Equal(t, "s1", s.mu.lastAddress)
	s.mu.RUnlock()

	address = "s2"
	s.refresh(ctx, "s1")
	s.mu.RLock()
	assert.Same(t, stores["s2"], s.mu.store)
	assert.Equal(t, "s2", s.mu.lastAddress)
	s.mu.RUnlock()
}

func TestRefreshTaskStorageFailureKeepsCurrentStore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	tests := []struct {
		name       string
		factoryErr error
	}{
		{name: "create error", factoryErr: assert.AnError},
		{name: "nil store"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			current := newTrackedTaskStorage()
			address := "s1"
			s := newRefreshableTaskStorage(
				runtime.DefaultRuntime(),
				func(context.Context, bool) (string, error) { return address, nil },
				&testStorageFactory{
					stores: map[string]TaskStorage{"s1": current},
					errs:   map[string]error{"s2": test.factoryErr},
				},
			).(*refreshableTaskStorage)
			t.Cleanup(func() { require.NoError(t, s.Close()) })

			address = "s2"
			s.refresh(ctx, "s1")

			require.Zero(t, current.closeCount.Load())
			s.mu.RLock()
			require.Same(t, current, s.mu.store)
			require.Equal(t, "s1", s.mu.lastAddress)
			s.mu.RUnlock()
		})
	}
}

func TestRefreshTaskStorageOldGenerationCannotReplaceNew(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	current := newTrackedTaskStorage()
	staleCandidate := newTrackedTaskStorage()
	latest := newTrackedTaskStorage()
	staleCreateStarted := make(chan struct{})
	releaseStaleCreate := make(chan struct{})
	var addressCalls atomic.Int64

	s := newRefreshableTaskStorage(
		runtime.DefaultRuntime(),
		func(context.Context, bool) (string, error) {
			switch addressCalls.Add(1) {
			case 1:
				return "s1", nil
			case 2:
				return "s2", nil
			default:
				return "s3", nil
			}
		},
		taskStorageFactoryFunc(func(address string) (TaskStorage, error) {
			switch address {
			case "s1":
				return current, nil
			case "s2":
				close(staleCreateStarted)
				<-releaseStaleCreate
				return staleCandidate, nil
			case "s3":
				return latest, nil
			default:
				return nil, assert.AnError
			}
		}),
	).(*refreshableTaskStorage)
	defer func() { require.NoError(t, s.Close()) }()

	staleDone := make(chan struct{})
	go func() {
		s.refresh(ctx, "s1")
		close(staleDone)
	}()
	<-staleCreateStarted

	// A later refresh wins while the older generation is still constructing.
	s.refresh(ctx, "s1")
	close(releaseStaleCreate)
	<-staleDone

	s.mu.RLock()
	require.Same(t, latest, s.mu.store)
	require.Equal(t, "s3", s.mu.lastAddress)
	s.mu.RUnlock()
	require.Equal(t, int64(1), current.closeCount.Load())
	require.Equal(t, int64(1), staleCandidate.closeCount.Load())
	require.Zero(t, latest.closeCount.Load())
}

func TestRefreshTaskStorageCloseRejectsConstructedReplacement(t *testing.T) {
	defer leaktest.AfterTest(t)()
	current := newTrackedTaskStorage()
	replacement := newTrackedTaskStorage()
	createStarted := make(chan struct{})
	releaseCreate := make(chan struct{})
	address := "s1"

	s := newRefreshableTaskStorage(
		runtime.DefaultRuntime(),
		func(context.Context, bool) (string, error) { return address, nil },
		taskStorageFactoryFunc(func(address string) (TaskStorage, error) {
			if address == "s1" {
				return current, nil
			}
			close(createStarted)
			<-releaseCreate
			return replacement, nil
		}),
	).(*refreshableTaskStorage)

	address = "s2"
	refreshDone := make(chan struct{})
	go func() {
		s.refresh(context.Background(), "s1")
		close(refreshDone)
	}()
	<-createStarted

	require.NoError(t, s.Close())
	close(releaseCreate)
	<-refreshDone

	require.Equal(t, int64(1), current.closeCount.Load())
	require.Equal(t, int64(1), replacement.closeCount.Load())
	s.mu.RLock()
	require.True(t, s.mu.closed)
	require.Same(t, current, s.mu.store)
	s.mu.RUnlock()
}

func TestRefreshTaskStorageCancellationRejectsConstructedReplacement(t *testing.T) {
	defer leaktest.AfterTest(t)()
	current := newTrackedTaskStorage()
	replacement := newTrackedTaskStorage()
	createStarted := make(chan struct{})
	releaseCreate := make(chan struct{})
	address := "s1"

	s := newRefreshableTaskStorage(
		runtime.DefaultRuntime(),
		func(context.Context, bool) (string, error) { return address, nil },
		taskStorageFactoryFunc(func(address string) (TaskStorage, error) {
			if address == "s1" {
				return current, nil
			}
			close(createStarted)
			<-releaseCreate
			return replacement, nil
		}),
	).(*refreshableTaskStorage)
	defer func() { require.NoError(t, s.Close()) }()

	address = "s2"
	ctx, cancel := context.WithCancel(context.Background())
	refreshDone := make(chan struct{})
	go func() {
		s.refresh(ctx, "s1")
		close(refreshDone)
	}()
	<-createStarted
	cancel()
	close(releaseCreate)
	<-refreshDone

	s.mu.RLock()
	require.Same(t, current, s.mu.store)
	require.Equal(t, "s1", s.mu.lastAddress)
	s.mu.RUnlock()
	require.Zero(t, current.closeCount.Load())
	require.Equal(t, int64(1), replacement.closeCount.Load())
}

func TestRefreshTaskStorageRejectedSharedCandidateDoesNotCloseCurrent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	current := newTrackedTaskStorage()
	address := "s1"
	s := newRefreshableTaskStorage(
		runtime.DefaultRuntime(),
		func(context.Context, bool) (string, error) { return address, nil },
		NewFixedTaskStorageFactory(current),
	).(*refreshableTaskStorage)
	defer func() { require.NoError(t, s.Close()) }()

	address = "s2"
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s.refresh(ctx, "s1")

	s.mu.RLock()
	require.Same(t, current, s.mu.store)
	require.Equal(t, "s1", s.mu.lastAddress)
	s.mu.RUnlock()
	require.Zero(t, current.closeCount.Load())
}

func TestRefreshTaskStorageClosedDoesNotConstructReplacement(t *testing.T) {
	defer leaktest.AfterTest(t)()
	current := newTrackedTaskStorage()
	var createCount atomic.Int64
	s := newRefreshableTaskStorage(
		runtime.DefaultRuntime(),
		func(context.Context, bool) (string, error) { return "s1", nil },
		taskStorageFactoryFunc(func(string) (TaskStorage, error) {
			createCount.Add(1)
			return current, nil
		}),
	).(*refreshableTaskStorage)

	require.NoError(t, s.Close())
	s.refresh(context.Background(), "s1")
	require.Equal(t, int64(1), createCount.Load())
	require.Equal(t, int64(1), current.closeCount.Load())
}

func TestRefreshTaskStoragePreviousCloseErrorDoesNotRollBackReplacement(t *testing.T) {
	defer leaktest.AfterTest(t)()
	current := newTrackedTaskStorage()
	current.closeErr = assert.AnError
	replacement := newTrackedTaskStorage()
	address := "s1"
	s := newRefreshableTaskStorage(
		runtime.DefaultRuntime(),
		func(context.Context, bool) (string, error) { return address, nil },
		&testStorageFactory{stores: map[string]TaskStorage{
			"s1": current,
			"s2": replacement,
		}},
	).(*refreshableTaskStorage)
	defer func() { require.NoError(t, s.Close()) }()

	address = "s2"
	s.refresh(context.Background(), "s1")

	s.mu.RLock()
	require.Same(t, replacement, s.mu.store)
	require.Equal(t, "s2", s.mu.lastAddress)
	s.mu.RUnlock()
	require.Equal(t, int64(1), current.closeCount.Load())
	require.Zero(t, replacement.closeCount.Load())
}

func TestRefreshTaskStorageSkipStaleRefreshKeepsCurrentStore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	current := newClosureSensitiveTaskStorage()
	var addressCalls, createCalls atomic.Int64
	address := "s1"
	s := newRefreshableTaskStorage(
		runtime.DefaultRuntime(),
		func(context.Context, bool) (string, error) {
			addressCalls.Add(1)
			return address, nil
		},
		taskStorageFactoryFunc(func(string) (TaskStorage, error) {
			createCalls.Add(1)
			return current, nil
		}),
	).(*refreshableTaskStorage)
	defer func() { require.NoError(t, s.Close()) }()

	// The initial creation resolved and created the s1 store once.
	require.Equal(t, int64(1), addressCalls.Load())
	require.Equal(t, int64(1), createCalls.Load())

	// Stale request: the holder serves s1 while the request carries s2.
	s.refresh(ctx, "s2")

	require.Zero(t, current.closeCount.Load(),
		"a stale refresh must not close the current store")
	require.Equal(t, int64(1), addressCalls.Load(),
		"a stale refresh must not resolve a new address")
	require.Equal(t, int64(1), createCalls.Load(),
		"a stale refresh must not create a replacement store")
	store, lastAddress := s.holderState()
	require.Same(t, current, store)
	require.Equal(t, "s1", lastAddress)

	// The kept store must still serve public SQL-task operations. A store that
	// refresh closed while installed fails the ping preceding every forwarded
	// operation, so success here proves the store was not retired.
	affected, err := s.AddSQLTask(ctx, newTestSQLTask("stale-kept-task", 1))
	require.NoError(t, err)
	require.Equal(t, 1, affected)
	tasks, err := s.QuerySQLTask(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.Equal(t, "stale-kept-task", tasks[0].TaskName)
}

func TestRefreshTaskStorageCreateFailureKeepsCurrentStore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	tests := []struct {
		name       string
		addressErr error
		createErr  error
		createNil  bool
	}{
		{name: "create error", createErr: assert.AnError},
		{name: "nil store", createNil: true},
		{name: "address error", addressErr: assert.AnError},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			current := newClosureSensitiveTaskStorage()
			address := "s1"
			s := newRefreshableTaskStorage(
				runtime.DefaultRuntime(),
				func(context.Context, bool) (string, error) {
					if address == "s2" && test.addressErr != nil {
						return "", test.addressErr
					}
					return address, nil
				},
				taskStorageFactoryFunc(func(addr string) (TaskStorage, error) {
					if addr == "s2" {
						if test.createNil {
							return nil, nil
						}
						return nil, test.createErr
					}
					return current, nil
				}),
			).(*refreshableTaskStorage)
			t.Cleanup(func() { require.NoError(t, s.Close()) })

			// A refresh attempt towards s2 fails while the holder serves s1.
			address = "s2"
			s.refresh(ctx, "s1")

			require.Zero(t, current.closeCount.Load(),
				"a failed refresh must not close the current store")
			store, lastAddress := s.holderState()
			require.Same(t, current, store)
			require.Equal(t, "s1", lastAddress,
				"lastAddress must not advance before a replacement is published")

			// The preserved store must still serve public SQL-task operations.
			affected, err := s.AddSQLTask(ctx, newTestSQLTask("failed-refresh-kept-task", 1))
			require.NoError(t, err)
			require.Equal(t, 1, affected)
		})
	}
}

func TestRefreshTaskStorageStaleRequestAfterReplacementKeepsNewStore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	current := newTrackedTaskStorage()
	replacement := newClosureSensitiveTaskStorage()
	address := "s1"
	s := newRefreshableTaskStorage(
		runtime.DefaultRuntime(),
		func(context.Context, bool) (string, error) { return address, nil },
		&testStorageFactory{stores: map[string]TaskStorage{
			"s1": current,
			"s2": replacement,
		}},
	).(*refreshableTaskStorage)
	defer func() { require.NoError(t, s.Close()) }()

	// A successful replacement retires the distinct previous store exactly once.
	address = "s2"
	s.refresh(ctx, "s1")
	require.Equal(t, int64(1), current.closeCount.Load())
	store, lastAddress := s.holderState()
	require.Same(t, replacement, store)
	require.Equal(t, "s2", lastAddress)

	// A later serialized request still carrying the old address s1 is stale
	// and must be skipped without closing the new store.
	s.refresh(ctx, "s1")
	require.Equal(t, int64(1), current.closeCount.Load())
	require.Zero(t, replacement.closeCount.Load(),
		"a stale request must not close the replacement store")

	affected, err := s.AddSQLTask(ctx, newTestSQLTask("replacement-kept-task", 1))
	require.NoError(t, err)
	require.Equal(t, 1, affected)
}

func TestRefreshTaskStorageFixedFactoryReuseKeepsStoreOpen(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	// A fixed factory hands back the active instance for every address; the
	// holder must keep serving it instead of retiring its own live store.
	current := newClosureSensitiveTaskStorage()
	address := "s1"
	s := newRefreshableTaskStorage(
		runtime.DefaultRuntime(),
		func(context.Context, bool) (string, error) { return address, nil },
		NewFixedTaskStorageFactory(current),
	).(*refreshableTaskStorage)
	defer func() { require.NoError(t, s.Close()) }()

	address = "s2"
	s.refresh(ctx, "s1")

	require.Zero(t, current.closeCount.Load(),
		"reusing the active instance must not retire it")
	store, lastAddress := s.holderState()
	require.Same(t, current, store)
	require.Equal(t, "s2", lastAddress)

	affected, err := s.AddSQLTask(ctx, newTestSQLTask("fixed-factory-task", 1))
	require.NoError(t, err)
	require.Equal(t, 1, affected)
}

func TestRefreshTaskStorageSameAddressReplacementPublishesNewInstance(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	current := newTrackedTaskStorage()
	replacement := newClosureSensitiveTaskStorage()
	var createCalls atomic.Int64
	s := newRefreshableTaskStorage(
		runtime.DefaultRuntime(),
		func(context.Context, bool) (string, error) { return "s1", nil },
		taskStorageFactoryFunc(func(string) (TaskStorage, error) {
			if createCalls.Add(1) == 1 {
				return current, nil
			}
			return replacement, nil
		}),
	).(*refreshableTaskStorage)
	defer func() { require.NoError(t, s.Close()) }()

	// A fresh instance for the same address is a distinct replacement.
	s.refresh(ctx, "s1")

	require.Equal(t, int64(1), current.closeCount.Load())
	store, lastAddress := s.holderState()
	require.Same(t, replacement, store)
	require.Equal(t, "s1", lastAddress)

	affected, err := s.AddSQLTask(ctx, newTestSQLTask("same-address-task", 1))
	require.NoError(t, err)
	require.Equal(t, 1, affected)
}

func TestRefreshTaskStorageRecoversAfterInitialFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	// The failure flag is read by the asynchronous refresh worker: factory
	// calls run outside mu, so transitions must be synchronized and the queued
	// refresh must be observed before the recovery flip.
	var failing atomic.Bool
	failing.Store(true)
	current := newClosureSensitiveTaskStorage()
	createDone := make(chan struct{}, 8)
	s := newRefreshableTaskStorage(
		runtime.DefaultRuntime(),
		func(context.Context, bool) (string, error) { return "s1", nil },
		taskStorageFactoryFunc(func(string) (TaskStorage, error) {
			defer func() { createDone <- struct{}{} }()
			if failing.Load() {
				return nil, assert.AnError
			}
			return current, nil
		}),
	).(*refreshableTaskStorage)
	defer func() { require.NoError(t, s.Close()) }()
	waitCreate := func(what string) {
		select {
		case <-createDone:
		case <-time.After(15 * time.Second):
			t.Fatalf("timed out waiting for factory attempt: %s", what)
		}
	}

	// The failed initial creation leaves the holder not ready.
	waitCreate("initial creation")
	store, lastAddress := s.holderState()
	require.Nil(t, store)
	require.Equal(t, "", lastAddress)

	// A public operation reports not-ready and queues an asynchronous refresh.
	// Wait for that queued refresh to run while creation still fails: it must
	// leave the holder not ready rather than half-refreshed.
	_, err := s.AddSQLTask(ctx, newTestSQLTask("not-ready-task", 1))
	require.ErrorIs(t, err, ErrNotReady)
	waitCreate("queued not-ready refresh")
	store, lastAddress = s.holderState()
	require.Nil(t, store, "a failed queued refresh must leave the holder not ready")
	require.Equal(t, "", lastAddress)

	// Only after the queued refresh has been observed does recovery start; a
	// later refresh publishes a working store.
	failing.Store(false)
	s.refresh(ctx, "")
	waitCreate("recovery refresh")
	store, lastAddress = s.holderState()
	require.Same(t, current, store)
	require.Equal(t, "s1", lastAddress)
	affected, err := s.AddSQLTask(ctx, newTestSQLTask("recovered-task", 1))
	require.NoError(t, err)
	require.Equal(t, 1, affected)
	require.Zero(t, current.closeCount.Load())
}

func TestRefreshTaskStorageClosedStateIsTerminal(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	current := newTrackedTaskStorage()
	var createCalls atomic.Int64
	s := newRefreshableTaskStorage(
		runtime.DefaultRuntime(),
		func(context.Context, bool) (string, error) { return "s1", nil },
		taskStorageFactoryFunc(func(string) (TaskStorage, error) {
			createCalls.Add(1)
			return current, nil
		}),
	).(*refreshableTaskStorage)

	require.NoError(t, s.Close())
	require.Equal(t, int64(1), current.closeCount.Load())

	// After closure no refresh can create a new store or close the retired one
	// a second time, and no refresh can be queued anymore.
	require.False(t, s.maybeRefresh("s1"))
	s.refresh(ctx, "s1")
	require.Equal(t, int64(1), createCalls.Load())
	require.Equal(t, int64(1), current.closeCount.Load())

	// Close is idempotent.
	require.NoError(t, s.Close())
	require.Equal(t, int64(1), current.closeCount.Load())
}

func TestRefreshTaskStorageStoreErrorReachesCaller(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	// A genuine unavailable-store error must reach the caller; preserving
	// ownership must not fabricate success.
	current := newTrackedTaskStorage()
	current.pingErr = assert.AnError
	s := newRefreshableTaskStorage(
		runtime.DefaultRuntime(),
		func(context.Context, bool) (string, error) { return "s1", nil },
		taskStorageFactoryFunc(func(string) (TaskStorage, error) {
			return current, nil
		}),
	).(*refreshableTaskStorage)
	defer func() { require.NoError(t, s.Close()) }()

	_, err := s.AddSQLTask(ctx, newTestSQLTask("error-task", 1))
	require.ErrorIs(t, err, assert.AnError)
	require.Zero(t, current.closeCount.Load())
}

func TestRefreshTaskStorageOperationCompletesBeforeRetirement(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	gated := newGatedTaskStorage()
	replacement := newTrackedTaskStorage()
	address := "s1"
	// The factory announces that the replacement candidate is being built,
	// then parks. In the current design creation happens before the
	// publication lock is acquired, so this is the observable refresh phase.
	createEntered := make(chan struct{}, 1)
	releaseCreate := make(chan struct{})
	doReleaseCreate := sync.OnceFunc(func() { close(releaseCreate) })
	doReleaseOp := sync.OnceFunc(func() { close(gated.release) })

	s := newRefreshableTaskStorage(
		runtime.DefaultRuntime(),
		func(context.Context, bool) (string, error) { return address, nil },
		taskStorageFactoryFunc(func(addr string) (TaskStorage, error) {
			if addr != "s2" {
				return gated, nil
			}
			createEntered <- struct{}{}
			<-releaseCreate
			return replacement, nil
		}),
	).(*refreshableTaskStorage)

	// Failure safety: defers run LIFO, so barriers are always released first
	// and all launched workers are joined, then the holder is closed, and
	// leaktest checks only drained workers. Each worker is registered
	// immediately before it is launched so a phase failure before a launch
	// never waits for a worker that does not exist.
	var workers sync.WaitGroup
	defer func() { require.NoError(t, s.Close()) }()
	defer func() { doReleaseCreate(); doReleaseOp(); workers.Wait() }()

	opValue := make(chan int, 1)
	opErr := make(chan error, 1)
	refreshDone := make(chan struct{})
	workers.Add(1)
	go func() {
		defer workers.Done()
		defer close(refreshDone)
		address = "s2"
		s.refresh(ctx, "s1")
	}()

	// Deterministic phase 1: the refresh passes its entry pre-check, resolves
	// s2, and parks inside the factory. At this point it holds no lock. An
	// early return without constructing a replacement fails immediately.
	select {
	case <-createEntered:
	case <-refreshDone:
		t.Fatal("refresh returned without constructing the replacement")
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for the refresh to construct the replacement; " +
			"the refresh path likely holds mu across factory work")
	}

	// Deterministic phase 2: only now does the public operation start. It
	// acquires mu.RLock and parks inside the store.
	workers.Add(1)
	go func() {
		defer workers.Done()
		var (
			v   int
			err error
		)
		v, err = s.AddSQLTask(ctx, newTestSQLTask("in-flight-task", 1))
		opValue <- v
		opErr <- err
	}()
	select {
	case <-gated.entered:
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for the in-flight operation to enter the store")
	}

	// Resume the refresh, then wait for the positive observation that it has
	// reached the publication lock while the operation is still parked. This
	// closes the scheduling hole where an early-retiring refresh might only
	// run after the operation completes: the operation is not released until
	// the refresh goroutine is observed parked on the write lock, and any
	// retirement attempt before that point (or completion of the refresh
	// while the reader is parked) fails the test immediately.
	doReleaseCreate()
	retireWhileInFlight := func() bool {
		select {
		case <-gated.closing:
			return true
		default:
			return false
		}
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		require.False(t, retireWhileInFlight(),
			"retirement was attempted before the in-flight operation completed")
		select {
		case <-refreshDone:
			t.Fatal("refresh finished while a public operation was still in flight")
		default:
		}
		if refreshParkedAtHolderLock() {
			// Re-check after the observation: an early-retiring refresh
			// announces closing before it parks, so a buffered announcement
			// must be visible by now.
			require.False(t, retireWhileInFlight(),
				"retirement was attempted before the in-flight operation completed")
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for the refresh to reach the publication lock")
		}
		time.Sleep(time.Millisecond)
	}

	doReleaseOp()
	select {
	case v := <-opValue:
		require.Equal(t, 1, v)
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for the in-flight operation to complete")
	}
	select {
	case err := <-opErr:
		require.NoError(t, err)
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for the in-flight operation result")
	}
	select {
	case <-refreshDone:
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for the refresh to complete")
	}

	// The in-flight operation completed strictly before the retirement close.
	// The wait is bounded so a regression that skips retirement fails promptly
	// instead of hanging until the package timeout, and the exact-sequence
	// assertion detects duplicated retirements as well.
	eventsDeadline := time.Now().Add(15 * time.Second)
	for len(gated.recordedEvents()) < 2 && time.Now().Before(eventsDeadline) {
		select {
		case <-gated.eventReady:
		case <-time.After(time.Until(eventsDeadline)):
		}
	}
	require.Len(t, gated.recordedEvents(), 2,
		"expected exactly the in-flight completion and one retirement")
	require.Equal(t, []string{"add-completed", "retired"}, gated.recordedEvents())

	// Cleanup happened exactly once and later operations use the replacement.
	require.Equal(t, int64(1), gated.closeCount.Load())
	require.Zero(t, replacement.closeCount.Load())
	store, _ := s.holderState()
	require.Same(t, replacement, store)
	affected, err := s.AddSQLTask(ctx, newTestSQLTask("after-refresh-task", 1))
	require.NoError(t, err)
	require.Equal(t, 1, affected)
}

func TestRefreshTaskStorageCanClose(t *testing.T) {
	stores := map[string]TaskStorage{
		"s1": NewMemTaskStorage(),
		"s2": NewMemTaskStorage(),
	}
	address := "s1"
	s := newRefreshableTaskStorage(
		runtime.DefaultRuntime(),
		func(context.Context, bool) (string, error) { return address, nil },
		&testStorageFactory{stores: stores}).(*refreshableTaskStorage)
	address = "s2"
	require.True(t, s.maybeRefresh("s1"))
	require.NoError(t, s.Close())
	<-s.refreshC
}

func Test_refreshAddCdcTask(t *testing.T) {
	storage, mock := newMockStorage(t)

	stores := map[string]TaskStorage{
		"s1": storage,
		"s2": NewMemTaskStorage(),
	}
	address := "s1"
	s := newRefreshableTaskStorage(
		runtime.DefaultRuntime(),
		func(context.Context, bool) (string, error) { return address, nil },
		&testStorageFactory{stores: stores}).(*refreshableTaskStorage)
	dt := newCdcInfo(t)

	mock.ExpectBegin()
	newInsertDaemonTaskExpect(t, mock)

	callback := func(context.Context, SqlExecutor) (int, error) {
		return 1, nil
	}

	mock.ExpectCommit()
	cnt, err := s.AddCDCTask(context.Background(), dt, callback)
	assert.NoError(t, err)
	assert.Greater(t, cnt, 0)

	mock.ExpectClose()

	address = "s2"
	require.True(t, s.maybeRefresh("s1"))
	require.NoError(t, s.Close())
	<-s.refreshC

	_ = storage.Close()
}

func TestNewTaskServiceHolderDefault(t *testing.T) {
	h := NewTaskServiceHolder(
		runtime.DefaultRuntime(),
		func(context.Context, bool) (string, error) { return "127.0.0.1:3306", nil },
	)
	require.NotNil(t, h)
}

func TestRefreshTaskStoragePingHeartbeatAndUpdateCdc(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store := NewMemTaskStorage()
	stores := map[string]TaskStorage{
		"s1": store,
	}
	s := newRefreshableTaskStorage(
		runtime.DefaultRuntime(),
		func(context.Context, bool) (string, error) { return "s1", nil },
		&testStorageFactory{stores: stores},
	).(*refreshableTaskStorage)
	defer func() {
		require.NoError(t, s.Close())
	}()

	require.NoError(t, s.PingContext(ctx))

	dt := newTestDaemonTask(1, "dt-1")
	dt.TaskStatus = task.TaskStatus_Running
	_, err := s.AddDaemonTask(ctx, dt)
	require.NoError(t, err)

	dt.LastHeartbeat = time.Now()
	affected, err := s.HeartbeatDaemonTask(ctx, []task.DaemonTask{dt})
	require.NoError(t, err)
	require.Equal(t, 1, affected)

	dt.Details = &task.Details{Error: "failed startup"}
	affected, err = s.UpdateDaemonTaskError(ctx, dt, false)
	require.NoError(t, err)
	require.Equal(t, 1, affected)
	stored, err := s.QueryDaemonTask(ctx, WithTaskIDCond(EQ, dt.ID))
	require.NoError(t, err)
	require.Len(t, stored, 1)
	require.Equal(t, dt.Details.Error, stored[0].Details.Error)
	require.Equal(t, dt.LastHeartbeat, stored[0].LastHeartbeat)

	affected, err = s.UpdateCDCTask(ctx, task.TaskStatus_Canceled, nil)
	require.NoError(t, err)
	require.Equal(t, 0, affected)

	affected, lastAddress, err := s.UpdateCdcTaskSub(ctx, task.TaskStatus_Canceled, nil)
	require.NoError(t, err)
	require.Equal(t, 0, affected)
	require.Equal(t, "s1", lastAddress)
}

func TestRefreshTaskStorageErrNotReadyBranches(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s := newRefreshableTaskStorage(
		runtime.DefaultRuntime(),
		func(context.Context, bool) (string, error) { return "", assert.AnError },
		&testStorageFactory{stores: map[string]TaskStorage{}},
	).(*refreshableTaskStorage)
	defer func() {
		require.NoError(t, s.Close())
	}()

	_, err := s.AddAsyncTask(ctx, newTestAsyncTask("a1"))
	require.ErrorIs(t, err, ErrNotReady)
	_, err = s.UpdateAsyncTask(ctx, []task.AsyncTask{newTestAsyncTask("a2")})
	require.ErrorIs(t, err, ErrNotReady)
	_, err = s.DeleteAsyncTask(ctx)
	require.ErrorIs(t, err, ErrNotReady)
	_, err = s.QueryAsyncTask(ctx)
	require.ErrorIs(t, err, ErrNotReady)
	_, err = s.AddCronTask(ctx, newTestCronTask("c1", "* * * * * *"))
	require.ErrorIs(t, err, ErrNotReady)
	_, err = s.QueryCronTask(ctx)
	require.ErrorIs(t, err, ErrNotReady)
	_, err = s.UpdateCronTask(ctx, task.CronTask{}, task.AsyncTask{})
	require.ErrorIs(t, err, ErrNotReady)
	_, err = s.AddDaemonTask(ctx, newTestDaemonTask(1, "d1"))
	require.ErrorIs(t, err, ErrNotReady)
	_, err = s.UpdateDaemonTask(ctx, []task.DaemonTask{newTestDaemonTask(1, "d2")})
	require.ErrorIs(t, err, ErrNotReady)
	_, err = s.UpdateDaemonTaskError(ctx, newTestDaemonTask(1, "d2"), false)
	require.ErrorIs(t, err, ErrNotReady)
	_, err = s.DeleteDaemonTask(ctx)
	require.ErrorIs(t, err, ErrNotReady)
	_, err = s.QueryDaemonTask(ctx)
	require.ErrorIs(t, err, ErrNotReady)
	_, err = s.HeartbeatDaemonTask(ctx, []task.DaemonTask{newTestDaemonTask(1, "d3")})
	require.ErrorIs(t, err, ErrNotReady)
	_, err = s.ValidateDaemonTask(ctx, newTestDaemonTask(1, "d3"))
	require.ErrorIs(t, err, ErrNotReady)
	_, err = s.AddSQLTask(ctx, newTestSQLTask("task-1", 1))
	require.ErrorIs(t, err, ErrNotReady)
	_, err = s.DeleteSQLTask(ctx)
	require.ErrorIs(t, err, ErrNotReady)
	_, err = s.QuerySQLTask(ctx)
	require.ErrorIs(t, err, ErrNotReady)
	_, err = s.QuerySQLTaskRun(ctx)
	require.ErrorIs(t, err, ErrNotReady)
	_, err = s.AcquireSQLTaskRun(ctx, newTestSQLTask("task-1", 1), newTestSQLTaskRun(1, "task-1", SQLTaskStatusRunning), testSQLTaskAdmission(nil, 0))
	require.ErrorIs(t, err, ErrNotReady)
	_, err = s.PublishSQLTaskReceipt(ctx, SQLTaskReceiptRequest{Run: newTestSQLTaskRun(1, "task-1", SQLTaskStatusSuccess)})
	require.ErrorIs(t, err, ErrNotReady)
	_, err = s.TriggerSQLTask(ctx, newTestSQLTask("task-1", 1), newTestAsyncTask("task-1:1"))
	require.ErrorIs(t, err, ErrNotReady)
}

func TestMySQLBasedTaskStorageFactoryCreate(t *testing.T) {
	factory := newMySQLBasedTaskStorageFactory("root", "111", "mo_task")
	store, err := factory.Create("127.0.0.1:3306")
	require.NoError(t, err)
	require.NotNil(t, store)
	require.NoError(t, store.Close())
}

func TestSQLTaskAdmissionRefreshesOnlyForStorageFailure(t *testing.T) {
	ctx := context.Background()
	base := NewMemTaskStorage()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	sqlTask := newTestSQLTask("held-task", 1)
	mustAddTestSQLTask(t, base, 1, sqlTask)
	sqlTask = mustGetTestSQLTask(t, base, 1)[0]
	run := newTestSQLTaskRun(sqlTask.TaskID, sqlTask.TaskName, SQLTaskStatusRunning)
	run.StartedAt = time.Now()
	_, err := base.AcquireSQLTaskRun(ctx, sqlTask, run, testSQLTaskAdmission(nil, 0))
	require.NoError(t, err)
	unavailable := errors.New("storage unavailable")
	internal := moerr.NewInternalErrorNoCtx("distinct storage error")
	joined := errors.Join(ErrSQLTaskOverlap, unavailable)
	invalid := moerr.NewInvalidInputNoCtx("invalid admission")
	joinedInvalid := errors.Join(invalid, unavailable)

	for _, tc := range []struct {
		name        string
		store       TaskStorage
		task        SQLTask
		wantErr     error
		wantRefresh bool
	}{
		{"overlap", base, sqlTask, ErrSQLTaskOverlap, false},
		{"missing", base, SQLTask{TaskID: sqlTask.TaskID + 1}, ErrSQLTaskNotFound, false},
		{"invalid input", &sqlTaskErrorStore{TaskStorage: base, acquireErr: invalid}, sqlTask, invalid, false},
		{"operation failure", &sqlTaskErrorStore{TaskStorage: base, acquireErr: unavailable}, sqlTask, unavailable, true},
		{"distinct internal error", &sqlTaskErrorStore{TaskStorage: base, acquireErr: internal}, sqlTask, internal, true},
		{"joined storage failure", &sqlTaskErrorStore{TaskStorage: base, acquireErr: joined}, sqlTask, joined, true},
		{"joined invalid input and storage failure", &sqlTaskErrorStore{TaskStorage: base, acquireErr: joinedInvalid}, sqlTask, joinedInvalid, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			holder := &refreshableTaskStorage{refreshC: make(chan string, 1)}
			holder.mu.store = tc.store
			holder.mu.lastAddress = "current"
			candidate := run
			candidate.TaskID, candidate.AccountID, candidate.TaskName = tc.task.TaskID, tc.task.AccountID, tc.task.TaskName
			_, err := holder.AcquireSQLTaskRun(ctx, tc.task, candidate, testSQLTaskAdmission(nil, 0))
			require.ErrorIs(t, err, tc.wantErr)
			select {
			case addr := <-holder.refreshC:
				require.True(t, tc.wantRefresh, "expected task admission result must not refresh storage")
				require.Equal(t, "current", addr)
			default:
				require.False(t, tc.wantRefresh, "storage failures must still request refresh")
			}
		})
	}
}

func TestSQLTaskCreationRefreshesOnlyForStorageFailure(t *testing.T) {
	ctx := context.Background()
	base := NewMemTaskStorage()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	unavailable := errors.New("storage unavailable")
	joined := errors.Join(ErrSQLTaskAccountMissing, unavailable)
	for _, tc := range []struct {
		name        string
		store       TaskStorage
		wantErr     error
		wantRefresh bool
	}{
		{"account missing", &sqlTaskErrorStore{TaskStorage: base, addErr: ErrSQLTaskAccountMissing}, ErrSQLTaskAccountMissing, false},
		{"sentinel from ping", &trackedTaskStorage{TaskStorage: base, pingErr: ErrSQLTaskAccountMissing}, ErrSQLTaskAccountMissing, true},
		{"operation failure", &sqlTaskErrorStore{TaskStorage: base, addErr: unavailable}, unavailable, true},
		{"joined storage failure", &sqlTaskErrorStore{TaskStorage: base, addErr: joined}, joined, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			holder := &refreshableTaskStorage{refreshC: make(chan string, 1)}
			holder.mu.store = tc.store
			holder.mu.lastAddress = "current"
			_, err := holder.AddSQLTask(ctx, newTestSQLTask("task", 1))
			require.ErrorIs(t, err, tc.wantErr)
			select {
			case addr := <-holder.refreshC:
				require.True(t, tc.wantRefresh, "expected task creation result must not refresh storage")
				require.Equal(t, "current", addr)
			default:
				require.False(t, tc.wantRefresh, "storage failures must still request refresh")
			}
		})
	}
}

type sqlTaskErrorStore struct {
	TaskStorage
	addErr     error
	acquireErr error
}

func (s *sqlTaskErrorStore) AddSQLTask(context.Context, ...SQLTask) (int, error) {
	return 0, s.addErr
}

func (s *sqlTaskErrorStore) AcquireSQLTaskRun(context.Context, SQLTask, SQLTaskRun, *SQLTaskRunAdmission) (uint64, error) {
	return 0, s.acquireErr
}

type testStorageFactory struct {
	stores map[string]TaskStorage
	errs   map[string]error
}

func (f *testStorageFactory) Create(address string) (TaskStorage, error) {
	if err := f.errs[address]; err != nil {
		return nil, err
	}
	return f.stores[address], nil
}

type taskStorageFactoryFunc func(address string) (TaskStorage, error)

func (f taskStorageFactoryFunc) Create(address string) (TaskStorage, error) {
	return f(address)
}

type trackedTaskStorage struct {
	TaskStorage
	closeCount    atomic.Int64
	pingCount     atomic.Int64
	validateCount atomic.Int64
	closeErr      error
	pingErr       error
}

func newTrackedTaskStorage() *trackedTaskStorage {
	return &trackedTaskStorage{TaskStorage: NewMemTaskStorage()}
}

func (s *trackedTaskStorage) Close() error {
	s.closeCount.Add(1)
	_ = s.TaskStorage.Close()
	return s.closeErr
}

func (s *trackedTaskStorage) PingContext(ctx context.Context) error {
	s.pingCount.Add(1)
	if s.pingErr != nil {
		return s.pingErr
	}
	return s.TaskStorage.PingContext(ctx)
}

func (s *trackedTaskStorage) ValidateDaemonTask(
	ctx context.Context, claim task.DaemonTask,
) (bool, error) {
	s.validateCount.Add(1)
	return s.TaskStorage.ValidateDaemonTask(ctx, claim)
}

// errStorageClosed mirrors the "sql: database is closed" failure that a real
// mysql task store surfaces from every forwarded operation once it is retired.
var errStorageClosed = errors.New("sql: database is closed")

// closureSensitiveTaskStorage makes retirement observable. memTaskStorage
// keeps serving no-op successes after Close, so pointer assertions alone cannot
// detect a store that refresh closed while still installed. Once this wrapper
// is closed, the ping that precedes every forwarded operation fails, exactly
// like a retired mysql task store.
type closureSensitiveTaskStorage struct {
	*trackedTaskStorage
}

func newClosureSensitiveTaskStorage() *closureSensitiveTaskStorage {
	return &closureSensitiveTaskStorage{trackedTaskStorage: newTrackedTaskStorage()}
}

func (s *closureSensitiveTaskStorage) PingContext(ctx context.Context) error {
	if s.closeCount.Load() > 0 {
		return errStorageClosed
	}
	return s.trackedTaskStorage.PingContext(ctx)
}

// gatedTaskStorage parks one in-flight AddSQLTask call and records whether the
// operation completed before the store was retired. Entry notification is
// buffered so the worker never blocks on a receiver the test may have
// abandoned; closing is announced before it happens so the test can detect a
// retirement attempted while an operation is still in flight; event recording
// never blocks on receiver progress so failure paths always drain.
type gatedTaskStorage struct {
	*trackedTaskStorage
	entered    chan struct{}
	release    chan struct{}
	closing    chan struct{}
	eventMu    sync.Mutex
	events     []string
	eventReady chan struct{}
}

func newGatedTaskStorage() *gatedTaskStorage {
	return &gatedTaskStorage{
		trackedTaskStorage: newTrackedTaskStorage(),
		entered:            make(chan struct{}, 1),
		release:            make(chan struct{}),
		closing:            make(chan struct{}, 1),
		eventReady:         make(chan struct{}, 1),
	}
}

// recordEvent appends an event under the mutex and signals readers with a
// nonblocking notification; a recording worker can never block here even if
// nothing ever reads the events back (regression failure paths).
func (g *gatedTaskStorage) recordEvent(event string) {
	g.eventMu.Lock()
	g.events = append(g.events, event)
	g.eventMu.Unlock()
	select {
	case g.eventReady <- struct{}{}:
	default:
	}
}

func (g *gatedTaskStorage) recordedEvents() []string {
	g.eventMu.Lock()
	defer g.eventMu.Unlock()
	return append([]string(nil), g.events...)
}

func (g *gatedTaskStorage) AddSQLTask(ctx context.Context, tasks ...SQLTask) (int, error) {
	g.entered <- struct{}{}
	<-g.release
	n, err := g.trackedTaskStorage.AddSQLTask(ctx, tasks...)
	g.recordEvent("add-completed")
	return n, err
}

func (g *gatedTaskStorage) Close() error {
	select {
	case g.closing <- struct{}{}:
	default:
	}
	err := g.trackedTaskStorage.Close()
	g.recordEvent("retired")
	return err
}

// refreshParkedAtHolderLock reports whether some goroutine is currently
// blocked inside refresh's publication lock acquisition. Stack inspection is
// the only way to observe a goroutine parked on a mutex; the caller polls for
// this positive observation, so scheduling delays only affect how long the
// poll runs, never whether the observation is made.
func refreshParkedAtHolderLock() bool {
	buf := make([]byte, 1<<20)
	n := goruntime.Stack(buf, true)
	for _, stack := range strings.Split(string(buf[:n]), "\n\n") {
		if !strings.Contains(stack, "taskservice.(*refreshableTaskStorage).refresh(") {
			continue
		}
		if strings.Contains(stack, "sync.(*RWMutex).Lock(") ||
			strings.Contains(stack, "sync.runtime_SemacquireRWMutex(") {
			return true
		}
	}
	return false
}

// holderState snapshots the installed store and address. Assertions must run
// outside mu.RLock: a failed require exits the goroutine via runtime.Goexit,
// which would leave the read lock held and deadlock the deferred Close.
func (s *refreshableTaskStorage) holderState() (TaskStorage, string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mu.store, s.mu.lastAddress
}

func TestRefreshTaskStorageValidationDoesNotDoubleRoundTrip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	store := newTrackedTaskStorage()
	claim := newTestDaemonTask(1, "claim")
	claim.TaskStatus = task.TaskStatus_Running
	claim.TaskRunner = "runner-1"
	claim.LastRun = time.Now().UTC().Truncate(time.Microsecond)
	_, err := store.AddDaemonTask(ctx, claim)
	require.NoError(t, err)

	refreshable := newRefreshableTaskStorage(
		runtime.DefaultRuntime(),
		func(context.Context, bool) (string, error) { return "s1", nil },
		&testStorageFactory{stores: map[string]TaskStorage{"s1": store}},
	).(*refreshableTaskStorage)
	defer func() { require.NoError(t, refreshable.Close()) }()

	valid, err := refreshable.ValidateDaemonTask(ctx, claim)
	require.NoError(t, err)
	require.True(t, valid)
	require.Equal(t, int64(1), store.validateCount.Load())
	require.Zero(t, store.pingCount.Load(),
		"validation is already a database round trip and must not be preceded by ping")
}

func TestSQLTaskMetadataHolderDoesNotWaitForRefresh(t *testing.T) {
	s := &refreshableTaskStorage{refreshC: make(chan string, 1)}
	s.mu.store = NewMemTaskStorage()
	s.mu.lastAddress = "old-address"
	called := false
	s.mu.Lock()
	err := s.withSQLTaskMetadataStore(context.Background(), func(TaskStorage) error {
		called = true
		return nil
	})
	s.mu.Unlock()
	require.ErrorIs(t, err, ErrNotReady)
	require.False(t, called)

	problem := errors.New("metadata unavailable")
	for i := 0; i < 2; i++ {
		err = s.withSQLTaskMetadataStore(context.Background(), func(TaskStorage) error { return problem })
		require.ErrorIs(t, err, problem)
	}
	require.Equal(t, "old-address", <-s.refreshC)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, s.withSQLTaskMetadataStore(ctx, func(TaskStorage) error {
		t.Fatal("canceled metadata operation called storage")
		return nil
	}), context.Canceled)
}

type cyclicSQLTaskMetadataError struct{}

func (e *cyclicSQLTaskMetadataError) Error() string { return "cyclic metadata error" }
func (e *cyclicSQLTaskMetadataError) Unwrap() error { return e }

type badConnectionSQLTaskMetadataError struct{ cause error }

func (e *badConnectionSQLTaskMetadataError) Error() string        { return "bad connection" }
func (e *badConnectionSQLTaskMetadataError) Unwrap() error        { return e.cause }
func (e *badConnectionSQLTaskMetadataError) Is(target error) bool { return target == driver.ErrBadConn }

type timeoutSQLTaskMetadataError struct{ cause error }

func (e *timeoutSQLTaskMetadataError) Error() string { return "timeout" }
func (e *timeoutSQLTaskMetadataError) Unwrap() error { return e.cause }
func (e *timeoutSQLTaskMetadataError) Is(target error) bool {
	return target == context.DeadlineExceeded
}

type hiddenNetworkSQLTaskMetadataError struct{ cause error }

func (e *hiddenNetworkSQLTaskMetadataError) Error() string { return "hidden network failure" }
func (e *hiddenNetworkSQLTaskMetadataError) Unwrap() error { return e.cause }
func (e *hiddenNetworkSQLTaskMetadataError) As(target any) bool {
	if out, ok := target.(*net.Error); ok {
		*out = &net.OpError{Op: "read", Err: errors.New("network failure")}
		return true
	}
	return false
}

func TestSQLTaskMetadataHolderRefreshClassification(t *testing.T) {
	base := NewMemTaskStorage()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	s := &refreshableTaskStorage{refreshC: make(chan string, 1)}
	s.mu.store, s.mu.lastAddress = base, "ready"

	// Exercise a real semantic conflict through the public holder method.
	_, err := s.PublishSQLTaskReceipt(context.Background(), SQLTaskReceiptRequest{Run: SQLTaskRun{RunID: 999}})
	require.ErrorIs(t, err, errSQLTaskReceiptConflict)
	require.Empty(t, s.refreshC)

	invalid := moerr.NewInvalidInputNoCtx("invalid task input")
	deep := error(errSQLTaskReceiptConflict)
	for range 33 {
		deep = fmt.Errorf("wrapper: %w", deep)
	}
	for _, tc := range []struct {
		name    string
		err     error
		refresh bool
	}{
		{"overlap", ErrSQLTaskOverlap, false},
		{"not found", ErrSQLTaskNotFound, false},
		{"claimed", ErrSQLTaskClaimed, false},
		{"stale firing", ErrSQLTaskStaleFiring, false},
		{"capacity", ErrSQLTaskRecoveryCapacity, false},
		{"sequence exhausted", ErrSQLTaskSequenceExhausted, false},
		{"wrapped conflict", fmt.Errorf("wrapper: %w", errSQLTaskReceiptConflict), false},
		{"wrapped overlap", fmt.Errorf("wrapper: %w", ErrSQLTaskOverlap), false},
		{"single joined conflict", errors.Join(errSQLTaskReceiptConflict), false},
		{"invalid input", invalid, false},
		{"wrapped invalid input", fmt.Errorf("wrapper: %w", invalid), false},
		{"other invalid state", moerr.NewInvalidStateNoCtx("other state"), true},
		{"unknown", errors.New("metadata unavailable"), true},
		{"wrapped connection", fmt.Errorf("connection: %w", driver.ErrBadConn), true},
		{"network wrapper", &net.OpError{Op: "read", Err: errSQLTaskReceiptConflict}, true},
		{"connection matcher", &badConnectionSQLTaskMetadataError{cause: errSQLTaskReceiptConflict}, true},
		{"timeout matcher", &timeoutSQLTaskMetadataError{cause: errSQLTaskReceiptConflict}, true},
		{"network As wrapper", &hiddenNetworkSQLTaskMetadataError{cause: errSQLTaskReceiptConflict}, true},
		{"unknown commit As wrapper", &sqlTaskUnknownCommitError{error: errSQLTaskReceiptConflict, unknown: moerr.NewTxnUnknown(context.Background(), "txn-test")}, true},
		{"conflict and connection", errors.Join(errSQLTaskReceiptConflict, driver.ErrBadConn), true},
		{"connection and conflict", errors.Join(driver.ErrBadConn, errSQLTaskReceiptConflict), true},
		{"two semantic causes", errors.Join(errSQLTaskReceiptConflict, ErrSQLTaskStaleFiring), true},
		{"wrapped rollback failure", fmt.Errorf("transaction: %w", errors.Join(errSQLTaskReceiptConflict, errors.New("rollback failed"))), true},
		{"cyclic wrapper", &cyclicSQLTaskMetadataError{}, true},
		{"deep wrapper", deep, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := s.withSQLTaskMetadataStore(context.Background(), func(TaskStorage) error { return tc.err })
			require.Same(t, tc.err, got, "refresh classification must not replace the original error")
			select {
			case address := <-s.refreshC:
				require.True(t, tc.refresh, "unexpected storage refresh")
				require.Equal(t, "ready", address)
			default:
				require.False(t, tc.refresh, "storage refresh was not requested")
			}
		})
	}
}

type sqlTaskNetworkError struct{}

func (sqlTaskNetworkError) Error() string   { return "network failure" }
func (sqlTaskNetworkError) Timeout() bool   { return false }
func (sqlTaskNetworkError) Temporary() bool { return false }

func TestSQLTaskFetchDoesNotRefreshForBadDataOrDeadline(t *testing.T) {
	s := &refreshableTaskStorage{refreshC: make(chan string, 1)}
	s.mu.store, s.mu.lastAddress = NewMemTaskStorage(), "old-address"
	s.mu.Lock()
	_, err := querySQLTaskAsync(context.Background(), s)
	s.mu.Unlock()
	require.ErrorIs(t, err, ErrNotReady)

	var value any
	decodeErr := json.Unmarshal([]byte("{"), &value)
	require.Error(t, decodeErr)
	for _, problem := range []error{decodeErr, context.DeadlineExceeded} {
		err = s.withSQLTaskFetchStore(context.Background(), func(TaskStorage) error { return problem })
		require.ErrorIs(t, err, problem)
		require.Empty(t, s.refreshC)
	}
	for _, problem := range []error{
		&net.OpError{Op: "dial", Err: errors.New("connection refused")},
		errors.Join(context.DeadlineExceeded, driver.ErrBadConn),
		errors.Join(context.DeadlineExceeded, io.EOF),
		errors.Join(context.DeadlineExceeded, &net.OpError{Op: "dial", Err: errors.New("connection refused")}),
		errors.Join(context.DeadlineExceeded, sqlTaskNetworkError{}),
	} {
		err = s.withSQLTaskFetchStore(context.Background(), func(TaskStorage) error { return problem })
		require.ErrorIs(t, err, problem)
		require.Equal(t, "old-address", <-s.refreshC)
	}
}

func TestSQLTaskFetchRecoversAfterInitialStorageOutage(t *testing.T) {
	store := NewMemTaskStorage()
	mustAddTestAsyncTask(t, store, 1, task.AsyncTask{
		Status: task.TaskStatus_Running, TaskRunner: "cn1",
		Metadata: task.TaskMetadata{ID: "ready", Executor: task.TaskCode_TestOnly},
	})
	var calls atomic.Int32
	holder := newRefreshableTaskStorage(runtime.DefaultRuntime(), func(context.Context, bool) (string, error) {
		if calls.Add(1) <= 2 {
			return "", errors.New("address unavailable")
		}
		return "ready", nil
	}, NewFixedTaskStorageFactory(store))
	service := NewTaskService(runtime.DefaultRuntime(), holder)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	runner := NewTaskRunner("cn1", service, nil, WithRunnerFetchLimit(1), WithRunnerFetchTimeout(time.Second)).(*taskRunner)
	require.Eventually(t, func() bool {
		values, err := runner.doFetch(context.Background(), context.Background())
		return err == nil && len(values) == 1 && values[0].Metadata.ID == "ready"
	}, 3*time.Second, 5*time.Millisecond, "periodic fetch must reconnect without another task-store operation")
	require.GreaterOrEqual(t, calls.Load(), int32(3))
}

func TestTaskStorageRefreshFailureLogsAreBounded(t *testing.T) {
	core, logs := observer.New(zapcore.ErrorLevel)
	rt := runtime.NewRuntime(metadata.ServiceType_CN, "refresh-log-test", zap.New(core))
	var calls atomic.Int32
	s := &refreshableTaskStorage{rt: rt, addressFactory: func(context.Context, bool) (string, error) {
		calls.Add(1)
		return "", errors.New("injected address failure")
	}}
	var wg sync.WaitGroup
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.refresh(context.Background(), "")
		}()
	}
	wg.Wait()
	require.Equal(t, int32(100), calls.Load())
	require.LessOrEqual(t, logs.FilterMessage(taskStorageRefreshFailed.Message).Len(), 3)
}

func TestTaskStorageFactoryTypedNilIsUnavailable(t *testing.T) {
	var nilStore *memTaskStorage
	holder := newRefreshableTaskStorage(runtime.DefaultRuntime(), func(context.Context, bool) (string, error) {
		return "address", nil
	}, NewFixedTaskStorageFactory(nilStore))
	t.Cleanup(func() { require.NoError(t, holder.Close()) })
	_, err := holder.AcquireSQLTaskRun(context.Background(), SQLTask{}, SQLTaskRun{}, nil)
	require.ErrorIs(t, err, ErrNotReady)
}
