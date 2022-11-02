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
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"go.uber.org/zap"
)

var (
	errNotReady = moerr.NewInvalidState("task store not ready")
)

type taskServiceHolder struct {
	logger                     *zap.Logger
	addressFactory             func() (string, error)
	taskStorageFactorySelector func(string, string, string) TaskStorageFactory
	mu                         struct {
		sync.RWMutex
		closed  bool
		store   TaskStorage
		service TaskService
	}
}

// NewTaskServiceHolder create a task service hold, it will create task storage and task service from the hakeeper's schedule command.
func NewTaskServiceHolder(logger *zap.Logger,
	addressFactory func() (string, error)) TaskServiceHolder {
	return NewTaskServiceHolderWithTaskStorageFactorySelector(logger, addressFactory, func(username, password, database string) TaskStorageFactory {
		return NewMySQLBasedTaskStorageFactory(username, password, database)
	})
}

// NewTaskServiceHolderWithTaskStorageFactorySelector is similar to NewTaskServiceHolder, but with a special
// task storage facroty selector
func NewTaskServiceHolderWithTaskStorageFactorySelector(logger *zap.Logger,
	addressFactory func() (string, error),
	selector func(string, string, string) TaskStorageFactory) TaskServiceHolder {
	return &taskServiceHolder{
		logger:                     logutil.Adjust(logger),
		addressFactory:             addressFactory,
		taskStorageFactorySelector: selector,
	}
}

func (h *taskServiceHolder) Close() error {
	defer logutil.LogClose(h.logger, "taskservice/service-holder")()

	h.mu.Lock()
	defer h.mu.Unlock()

	if h.mu.closed {
		return nil
	}
	h.mu.closed = true
	if h.mu.store == nil {
		return nil
	}
	if err := h.mu.store.Close(); err != nil {
		return err
	}
	return h.mu.service.Close()
}

func (h *taskServiceHolder) Create(command logservicepb.CreateTaskService) error {
	// TODO: In any case, the username and password are not printed in the log, morpc needs to fix
	if command.User.Username == "" || command.User.Password == "" {
		h.logger.Debug("start task runner skipped",
			zap.String("reason", "empty task user and passwd"))
		return moerr.NewInvalidState("empty task user and passwd")
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	if h.mu.service != nil {
		return nil
	}

	store := newRefreshableTaskStorage(h.logger,
		h.addressFactory,
		h.taskStorageFactorySelector(command.User.Username,
			command.User.Password,
			command.TaskDatabase))
	h.mu.store = store
	h.mu.service = NewTaskService(store, h.logger.Named("task"))
	return nil
}

func (h *taskServiceHolder) Get() (TaskService, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.mu.service == nil {
		return nil, false
	}
	return h.mu.service, true
}

type refreshableTaskStorage struct {
	logger         *zap.Logger
	refreshC       chan string
	stopper        *stopper.Stopper
	addressFactory func() (string, error)
	storeFactory   TaskStorageFactory
	mu             struct {
		sync.RWMutex
		closed      bool
		lastAddress string
		store       TaskStorage
	}
}

func newRefreshableTaskStorage(logger *zap.Logger,
	addressFactory func() (string, error),
	storeFactory TaskStorageFactory) TaskStorage {
	s := &refreshableTaskStorage{
		logger:         logutil.Adjust(logger),
		refreshC:       make(chan string, 1),
		addressFactory: addressFactory,
		storeFactory:   storeFactory,
		stopper:        stopper.NewStopper("refresh-taskstorage", stopper.WithLogger(logger)),
	}
	s.refresh("")
	if err := s.stopper.RunTask(s.refreshTask); err != nil {
		panic(err)
	}
	return s
}

func (s *refreshableTaskStorage) Close() error {
	defer logutil.LogClose(s.logger, "taskservice/refreshable-storage")()

	var err error
	s.mu.Lock()
	if s.mu.closed {
		s.mu.Unlock()
		return nil
	}
	s.mu.closed = true
	if s.mu.store != nil {
		err = s.mu.store.Close()
	}
	s.mu.Unlock()
	s.stopper.Stop()
	close(s.refreshC)
	return err
}

func (s *refreshableTaskStorage) Add(ctx context.Context, tasks ...task.Task) (int, error) {
	var v int
	var err error
	s.mu.RLock()
	lastAddress := s.mu.lastAddress
	if s.mu.store == nil {
		err = errNotReady
	} else {
		v, err = s.mu.store.Add(ctx, tasks...)
	}
	s.mu.RUnlock()
	if err != nil {
		s.maybeRefresh(lastAddress)
	}
	return v, err
}

func (s *refreshableTaskStorage) Update(ctx context.Context, tasks []task.Task, conditions ...Condition) (int, error) {
	var v int
	var err error
	s.mu.RLock()
	lastAddress := s.mu.lastAddress
	if s.mu.store == nil {
		err = errNotReady
	} else {
		v, err = s.mu.store.Update(ctx, tasks, conditions...)
	}
	s.mu.RUnlock()
	if err != nil {
		s.maybeRefresh(lastAddress)
	}
	return v, err
}

func (s *refreshableTaskStorage) Delete(ctx context.Context, conditions ...Condition) (int, error) {
	var v int
	var err error
	s.mu.RLock()
	lastAddress := s.mu.lastAddress
	if s.mu.store == nil {
		err = errNotReady
	} else {
		v, err = s.mu.store.Delete(ctx, conditions...)
	}
	s.mu.RUnlock()
	if err != nil {
		s.maybeRefresh(lastAddress)
	}
	return v, err
}

func (s *refreshableTaskStorage) Query(ctx context.Context, conditions ...Condition) ([]task.Task, error) {
	var v []task.Task
	var err error
	s.mu.RLock()
	lastAddress := s.mu.lastAddress
	if s.mu.store == nil {
		err = errNotReady
	} else {
		v, err = s.mu.store.Query(ctx, conditions...)
	}
	s.mu.RUnlock()
	if err != nil {
		s.maybeRefresh(lastAddress)
	}
	return v, err
}

func (s *refreshableTaskStorage) AddCronTask(ctx context.Context, tasks ...task.CronTask) (int, error) {
	var v int
	var err error
	s.mu.RLock()
	lastAddress := s.mu.lastAddress
	if s.mu.store == nil {
		err = errNotReady
	} else {
		v, err = s.mu.store.AddCronTask(ctx, tasks...)
	}
	s.mu.RUnlock()
	if err != nil {
		s.maybeRefresh(lastAddress)
	}
	return v, err
}

func (s *refreshableTaskStorage) QueryCronTask(ctx context.Context) ([]task.CronTask, error) {
	var v []task.CronTask
	var err error
	s.mu.RLock()
	lastAddress := s.mu.lastAddress
	if s.mu.store == nil {
		err = errNotReady
	} else {
		v, err = s.mu.store.QueryCronTask(ctx)
	}
	s.mu.RUnlock()
	if err != nil {
		s.maybeRefresh(lastAddress)
	}
	return v, err
}

func (s *refreshableTaskStorage) UpdateCronTask(ctx context.Context, cronTask task.CronTask, task task.Task) (int, error) {
	var v int
	var err error
	s.mu.RLock()
	lastAddress := s.mu.lastAddress
	if s.mu.store == nil {
		err = errNotReady
	} else {
		v, err = s.mu.store.UpdateCronTask(ctx, cronTask, task)
	}
	s.mu.RUnlock()
	if err != nil {
		s.maybeRefresh(lastAddress)
	}
	return v, err
}

func (s *refreshableTaskStorage) maybeRefresh(lastAddress string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.closed {
		return false
	}

	select {
	case s.refreshC <- lastAddress:
		return true
	default:
		return false
	}
}

func (s *refreshableTaskStorage) refreshTask(ctx context.Context) {
	defer logutil.LogAsyncTask(s.logger, "taskservice/refreshable-storage/refresh-task")()

	for {
		select {
		case <-ctx.Done():
			return
		case lastAddress := <-s.refreshC:
			s.refresh(lastAddress)
		}
	}
}

func (s *refreshableTaskStorage) refresh(lastAddress string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if lastAddress != "" && lastAddress != s.mu.lastAddress {
		return
	}
	connectAddress, err := s.addressFactory()
	if err != nil {
		s.logger.Error("refresh task storage failed",
			zap.Error(err))
		return
	}

	s.mu.lastAddress = connectAddress
	s.logger.Debug("try to refresh task storage", zap.String("address", connectAddress))
	store, err := s.storeFactory.Create(connectAddress)
	if err != nil {
		s.logger.Error("refresh task storage failed",
			zap.String("address", connectAddress),
			zap.Error(err))
		return
	}
	s.mu.store = store
	s.logger.Debug("refresh task storage completed", zap.String("sql-address", connectAddress))
}

type mysqlBasedStorageFactory struct {
	username string
	password string
	database string
}

// NewMySQLBasedTaskStorageFactory creates a mysql based task storage factory using the special username, password and database
func NewMySQLBasedTaskStorageFactory(username, password, database string) TaskStorageFactory {
	return &mysqlBasedStorageFactory{
		username: username,
		password: password,
		database: database,
	}
}

func (f *mysqlBasedStorageFactory) Create(address string) (TaskStorage, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/?readTimeout=5s&writeTimeout=5s&timeout=5s",
		f.username,
		f.password,
		address)
	return NewMysqlTaskStorage(dsn, f.database)
}

type fixedTaskStorageFactory struct {
	store TaskStorage
}

// NewFixedTaskStorageFactory creates a fixed task storage factory which always returns the special taskstorage
func NewFixedTaskStorageFactory(store TaskStorage) TaskStorageFactory {
	return &fixedTaskStorageFactory{
		store: store,
	}
}

func (f *fixedTaskStorageFactory) Create(address string) (TaskStorage, error) {
	return f.store, nil
}
