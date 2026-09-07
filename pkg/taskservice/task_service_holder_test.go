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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
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
	_, err = s.UpdateSQLTask(ctx, []SQLTask{newTestSQLTask("task-2", 1)})
	require.ErrorIs(t, err, ErrNotReady)
	_, err = s.DeleteSQLTask(ctx)
	require.ErrorIs(t, err, ErrNotReady)
	_, err = s.QuerySQLTask(ctx)
	require.ErrorIs(t, err, ErrNotReady)
	_, err = s.AddSQLTaskRun(ctx, newTestSQLTaskRun(1, "task-1", SQLTaskStatusRunning))
	require.ErrorIs(t, err, ErrNotReady)
	_, err = s.UpdateSQLTaskRun(ctx, []SQLTaskRun{newTestSQLTaskRun(1, "task-1", SQLTaskStatusSuccess)})
	require.ErrorIs(t, err, ErrNotReady)
	_, err = s.QuerySQLTaskRun(ctx)
	require.ErrorIs(t, err, ErrNotReady)
	_, err = s.AcquireSQLTaskRun(ctx, newTestSQLTask("task-1", 1), newTestSQLTaskRun(1, "task-1", SQLTaskStatusRunning))
	require.ErrorIs(t, err, ErrNotReady)
	_, err = s.CompleteSQLTaskRun(ctx, newTestSQLTaskRun(1, "task-1", SQLTaskStatusSuccess))
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
	return s.TaskStorage.PingContext(ctx)
}

func (s *trackedTaskStorage) ValidateDaemonTask(
	ctx context.Context, claim task.DaemonTask,
) (bool, error) {
	s.validateCount.Add(1)
	return s.TaskStorage.ValidateDaemonTask(ctx, claim)
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
