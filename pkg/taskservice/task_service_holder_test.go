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
	assert.Equal(t, stores["s1"], s.mu.store)
	assert.Equal(t, "s1", s.mu.lastAddress)
	s.mu.RUnlock()

	s.refresh(ctx, "s2")
	s.mu.RLock()
	assert.Equal(t, stores["s1"], s.mu.store)
	assert.Equal(t, "s1", s.mu.lastAddress)
	s.mu.RUnlock()

	address = "s2"
	s.refresh(ctx, "s1")
	s.mu.RLock()
	assert.Equal(t, stores["s2"], s.mu.store)
	assert.Equal(t, "s2", s.mu.lastAddress)
	s.mu.RUnlock()
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
	_, err = s.DeleteDaemonTask(ctx)
	require.ErrorIs(t, err, ErrNotReady)
	_, err = s.QueryDaemonTask(ctx)
	require.ErrorIs(t, err, ErrNotReady)
	_, err = s.HeartbeatDaemonTask(ctx, []task.DaemonTask{newTestDaemonTask(1, "d3")})
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
}

func (f *testStorageFactory) Create(address string) (TaskStorage, error) {
	return f.stores[address], nil
}
