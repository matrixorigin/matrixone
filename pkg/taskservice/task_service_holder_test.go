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
	"database/sql"
	"github.com/DATA-DOG/go-sqlmock"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

type testClient struct {
	db *sql.DB

	addressFunc func() string

	stores map[string]TaskStorage
}

func newTestClient(t *testing.T) (*testClient, sqlmock.Sqlmock, sqlmock.Sqlmock) {
	db1, mock1, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	storage1, err := newMysqlTaskStorage(db1)
	require.NoError(t, err)

	db2, mock2, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	storage2, err := newMysqlTaskStorage(db2)
	require.NoError(t, err)

	return &testClient{
		stores: map[string]TaskStorage{"s1": storage1, "s2": storage2},
	}, mock1, mock2
}

func (t testClient) GetOrConnect(ctx context.Context, reuse bool) (*sql.DB, error) {
	if reuse && t.db != nil {
		return t.db, nil
	}

	return t.stores[t.addressFunc()].(*mysqlTaskStorage).db, nil
}

func (t testClient) Close() error {
	if t.db != nil {
		return t.db.Close()
	}
	return nil
}

func TestTaskHolderCreateWithEmptyCommandReturnError(t *testing.T) {
	h := NewTaskServiceHolder(runtime.DefaultRuntime(), func(context.Context, bool) (string, error) { return "", nil })
	require.Error(t, h.Create(logservicepb.CreateTaskService{}))
}

func TestTaskHolderNotCreatedCanClose(t *testing.T) {
	h := NewTaskServiceHolder(runtime.DefaultRuntime(), func(context.Context, bool) (string, error) { return "", nil })
	require.NoError(t, h.Close())
}

func TestTaskHolderCanClose(t *testing.T) {
	h := NewTaskServiceHolder(runtime.DefaultRuntime(), func(context.Context, bool) (string, error) { return "", nil })
	require.NoError(t, h.Create(logservicepb.CreateTaskService{
		User:         logservicepb.TaskTableUser{Username: "u", Password: "p"},
		TaskDatabase: "d",
	}))
	require.NoError(t, h.Close())
}

func TestRefreshTaskStorageCanRefresh(t *testing.T) {
	ctx := context.Background()

	client, mock1, mock2 := newTestClient(t)
	client.addressFunc = func() string { return "s1" }
	mock1.ExpectClose()
	mock2.ExpectClose()

	s := newRefreshableTaskStorage(runtime.DefaultRuntime(), client).(*refreshableTaskStorage)
	defer func() {
		require.NoError(t, s.Close())
	}()

	s.maybeRefresh(ctx)
	s.mu.RLock()
	require.Equal(t, client.stores["s1"], s.mu.store)
	s.mu.RUnlock()

	s.maybeRefresh(ctx)
	s.mu.RLock()
	require.Equal(t, client.stores["s1"], s.mu.store)
	s.mu.RUnlock()

	require.NoError(t, client.stores["s1"].Close())

	client.addressFunc = func() string { return "s2" }
	s.maybeRefresh(ctx)
	s.mu.RLock()
	require.Equal(t, client.stores["s2"], s.mu.store)
	s.mu.RUnlock()
}

func Test_refreshAddCdcTask(t *testing.T) {
	client, mock1, mock2 := newTestClient(t)

	mock1.ExpectBegin()
	newInsertDaemonTaskExpect(t, mock1)
	mock1.ExpectClose()
	mock2.ExpectClose()

	client.addressFunc = func() string { return "s1" }
	s := newRefreshableTaskStorage(
		runtime.DefaultRuntime(), client).(*refreshableTaskStorage)
	dt := newCdcInfo()

	callback := func(context.Context, SqlExecutor) (int, error) {
		return 1, nil
	}
	s.maybeRefresh(context.Background())
	cnt, err := s.AddCdcTask(context.Background(), dt, callback)
	require.NoError(t, err)
	require.Greater(t, cnt, 0)

	require.NoError(t, client.stores["s1"].Close())

	client.addressFunc = func() string { return "s2" }
	require.True(t, s.maybeRefresh(context.Background()))
	require.NoError(t, s.Close())
	<-s.refreshC

	_ = client.Close()
}
