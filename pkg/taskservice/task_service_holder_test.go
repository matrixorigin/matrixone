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
	"testing"

	"github.com/lni/goutils/leaktest"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTaskHolderCanCreateTaskService(t *testing.T) {
	defer leaktest.AfterTest(t)()
	store := NewMemTaskStorage()
	h := NewTaskServiceHolderWithTaskStorageFactorySelector(nil,
		func() (string, error) { return "", nil },
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
	h := NewTaskServiceHolderWithTaskStorageFactorySelector(nil,
		func() (string, error) { return "", nil },
		func(s1, s2, s3 string) TaskStorageFactory {
			return NewFixedTaskStorageFactory(store)
		})
	assert.Error(t, h.Create(logservicepb.CreateTaskService{}))
}

func TestTaskHolderNotCreatedCanClose(t *testing.T) {
	store := NewMemTaskStorage()
	h := NewTaskServiceHolderWithTaskStorageFactorySelector(nil,
		func() (string, error) { return "", nil },
		func(s1, s2, s3 string) TaskStorageFactory {
			return NewFixedTaskStorageFactory(store)
		})
	assert.NoError(t, h.Close())
}

func TestTaskHolderCanClose(t *testing.T) {
	store := NewMemTaskStorage()
	h := NewTaskServiceHolderWithTaskStorageFactorySelector(nil,
		func() (string, error) { return "", nil },
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

	stores := map[string]TaskStorage{
		"s1": NewMemTaskStorage(),
		"s2": NewMemTaskStorage(),
	}
	address := "s1"
	s := newRefreshableTaskStorage(nil, func() (string, error) { return address, nil }, &testStorageFactory{stores: stores}).(*refreshableTaskStorage)
	defer func() {
		require.NoError(t, s.Close())
	}()

	s.mu.RLock()
	assert.Equal(t, stores["s1"], s.mu.store)
	assert.Equal(t, "s1", s.mu.lastAddress)
	s.mu.RUnlock()

	s.refresh("s2")
	s.mu.RLock()
	assert.Equal(t, stores["s1"], s.mu.store)
	assert.Equal(t, "s1", s.mu.lastAddress)
	s.mu.RUnlock()

	address = "s2"
	s.refresh("s1")
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
	s := newRefreshableTaskStorage(nil, func() (string, error) { return address, nil }, &testStorageFactory{stores: stores}).(*refreshableTaskStorage)
	address = "s2"
	require.True(t, s.maybeRefresh("s1"))
	require.NoError(t, s.Close())
	<-s.refreshC
}

type testStorageFactory struct {
	stores map[string]TaskStorage
}

func (f *testStorageFactory) Create(address string) (TaskStorage, error) {
	return f.stores[address], nil
}
