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
	"sync"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/moclient"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
)

var (
	ErrNotReady = moerr.NewInvalidStateNoCtx("task store not ready")
)

type taskServiceHolder struct {
	rt             runtime.Runtime
	addressFactory func(context.Context, bool) (string, error)

	mu struct {
		sync.RWMutex
		closed  bool
		service TaskService
	}
}

// NewTaskServiceHolder create a task service hold, it will create task storage and task service from the hakeeper's schedule command.
func NewTaskServiceHolder(
	rt runtime.Runtime,
	addressFactory func(context.Context, bool) (string, error),
) TaskServiceHolder {
	return &taskServiceHolder{
		rt:             rt,
		addressFactory: addressFactory,
	}
}

func (h *taskServiceHolder) Close() error {
	defer h.rt.Logger().LogAction("close service-holder",
		log.DefaultLogOptions().WithLevel(zap.DebugLevel))()

	h.mu.Lock()
	defer h.mu.Unlock()

	if h.mu.closed {
		return nil
	}
	h.mu.closed = true
	if h.mu.service == nil {
		return nil
	}
	return h.mu.service.Close()
}

func (h *taskServiceHolder) Create(command logservicepb.CreateTaskService) error {
	// TODO: In any case, the username and password are not printed in the log, morpc needs to fix
	if command.User.Username == "" || command.User.Password == "" {
		h.rt.Logger().Debug("start task runner skipped",
			zap.String("reason", "empty task user and passwd"))
		return moerr.NewInvalidStateNoCtx("empty task user and passwd")
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	if h.mu.service != nil {
		return nil
	}

	h.mu.service = NewTaskService(h.rt,
		newRefreshableTaskStorage(
			h.rt,
			moclient.NewMOConnector(h.addressFactory, moclient.DBDSNTemplate(
				command.User.Username,
				command.User.Password,
				command.TaskDatabase,
			)),
		))
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
	rt       runtime.Runtime
	refreshC chan struct{}

	moClient       moclient.MOClient
	storageFactory func(db *sql.DB) (TaskStorage, error)

	mu struct {
		sync.RWMutex
		closed bool
		store  TaskStorage
	}
}

func newRefreshableTaskStorage(
	rt runtime.Runtime,
	moClient moclient.MOClient) TaskStorage {
	s := &refreshableTaskStorage{
		rt:             rt,
		refreshC:       make(chan struct{}, 1),
		moClient:       moClient,
		storageFactory: newMysqlTaskStorage,
	}
	_, err := s.moClient.GetOrConnect(context.Background(), false)
	if err != nil {
		return nil
	}
	return s
}

func (s *refreshableTaskStorage) Close() error {
	defer s.rt.Logger().LogAction("close refreshable-storage",
		log.DefaultLogOptions().WithLevel(zap.DebugLevel))()

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
	close(s.refreshC)
	return err
}

func (s *refreshableTaskStorage) PingContext(ctx context.Context) error {
	if s.mu.store == nil {
		return ErrNotReady
	}
	return s.mu.store.PingContext(ctx)
}

func (s *refreshableTaskStorage) AddAsyncTask(ctx context.Context, tasks ...task.AsyncTask) (int, error) {
	var v int
	var err error
	s.mu.RLock()
	if s.mu.store == nil {
		err = ErrNotReady
	} else if err = s.mu.store.PingContext(ctx); err == nil {
		v, err = s.mu.store.AddAsyncTask(ctx, tasks...)
	}
	s.mu.RUnlock()
	if err != nil {
		s.maybeRefresh(ctx)
	}
	return v, err
}

func (s *refreshableTaskStorage) UpdateAsyncTask(ctx context.Context, tasks []task.AsyncTask, conditions ...Condition) (int, error) {
	var v int
	var err error
	s.mu.RLock()
	if s.mu.store == nil {
		err = ErrNotReady
	} else if err = s.mu.store.PingContext(ctx); err == nil {
		v, err = s.mu.store.UpdateAsyncTask(ctx, tasks, conditions...)
	}
	s.mu.RUnlock()
	if err != nil {
		s.maybeRefresh(ctx)
	}
	return v, err
}

func (s *refreshableTaskStorage) DeleteAsyncTask(ctx context.Context, conditions ...Condition) (int, error) {
	var v int
	var err error
	s.mu.RLock()
	if s.mu.store == nil {
		err = ErrNotReady
	} else if err = s.mu.store.PingContext(ctx); err == nil {
		v, err = s.mu.store.DeleteAsyncTask(ctx, conditions...)
	}
	s.mu.RUnlock()
	if err != nil {
		s.maybeRefresh(ctx)
	}
	return v, err
}

func (s *refreshableTaskStorage) QueryAsyncTask(ctx context.Context, conditions ...Condition) ([]task.AsyncTask, error) {
	var v []task.AsyncTask
	var err error
	s.mu.RLock()
	if s.mu.store == nil {
		err = ErrNotReady
	} else if err = s.mu.store.PingContext(ctx); err == nil {
		v, err = s.mu.store.QueryAsyncTask(ctx, conditions...)
	}
	s.mu.RUnlock()
	if err != nil {
		s.maybeRefresh(ctx)
	}
	return v, err
}

func (s *refreshableTaskStorage) AddCronTask(ctx context.Context, tasks ...task.CronTask) (int, error) {
	var v int
	var err error
	s.mu.RLock()
	if s.mu.store == nil {
		err = ErrNotReady
	} else if err = s.mu.store.PingContext(ctx); err == nil {
		v, err = s.mu.store.AddCronTask(ctx, tasks...)
	}
	s.mu.RUnlock()
	if err != nil {
		s.maybeRefresh(ctx)
	}
	return v, err
}

func (s *refreshableTaskStorage) QueryCronTask(ctx context.Context, c ...Condition) ([]task.CronTask, error) {
	var v []task.CronTask
	var err error
	s.mu.RLock()

	if s.mu.store == nil {
		err = ErrNotReady
	} else if err = s.mu.store.PingContext(ctx); err == nil {
		v, err = s.mu.store.QueryCronTask(ctx, c...)
	}
	s.mu.RUnlock()
	if err != nil {
		s.maybeRefresh(ctx)
	}
	return v, err
}

func (s *refreshableTaskStorage) UpdateCronTask(ctx context.Context, cronTask task.CronTask, task task.AsyncTask) (int, error) {
	var v int
	var err error
	s.mu.RLock()
	if s.mu.store == nil {
		err = ErrNotReady
	} else if err = s.mu.store.PingContext(ctx); err == nil {
		v, err = s.mu.store.UpdateCronTask(ctx, cronTask, task)
	}
	s.mu.RUnlock()
	if err != nil {
		s.maybeRefresh(ctx)
	}
	return v, err
}

func (s *refreshableTaskStorage) AddDaemonTask(ctx context.Context, tasks ...task.DaemonTask) (int, error) {
	var v int
	var err error
	s.mu.RLock()

	if s.mu.store == nil {
		err = ErrNotReady
	} else if err = s.mu.store.PingContext(ctx); err == nil {
		v, err = s.mu.store.AddDaemonTask(ctx, tasks...)
	}
	s.mu.RUnlock()
	if err != nil {
		s.maybeRefresh(ctx)
	}
	return v, err
}

func (s *refreshableTaskStorage) UpdateDaemonTask(ctx context.Context, tasks []task.DaemonTask, conditions ...Condition) (int, error) {
	var v int
	var err error
	s.mu.RLock()

	if s.mu.store == nil {
		err = ErrNotReady
	} else if err = s.mu.store.PingContext(ctx); err == nil {
		v, err = s.mu.store.UpdateDaemonTask(ctx, tasks, conditions...)
	}
	s.mu.RUnlock()
	if err != nil {
		s.maybeRefresh(ctx)
	}
	return v, err
}

func (s *refreshableTaskStorage) DeleteDaemonTask(ctx context.Context, conditions ...Condition) (int, error) {
	var v int
	var err error
	s.mu.RLock()

	if s.mu.store == nil {
		err = ErrNotReady
	} else if err = s.mu.store.PingContext(ctx); err == nil {
		v, err = s.mu.store.DeleteDaemonTask(ctx, conditions...)
	}
	s.mu.RUnlock()
	if err != nil {
		s.maybeRefresh(ctx)
	}
	return v, err
}

func (s *refreshableTaskStorage) QueryDaemonTask(ctx context.Context, conditions ...Condition) ([]task.DaemonTask, error) {
	var v []task.DaemonTask
	var err error
	s.mu.RLock()

	if s.mu.store == nil {
		err = ErrNotReady
	} else if err = s.mu.store.PingContext(ctx); err == nil {
		v, err = s.mu.store.QueryDaemonTask(ctx, conditions...)
	}
	s.mu.RUnlock()
	if err != nil {
		s.maybeRefresh(ctx)
	}
	return v, err
}

func (s *refreshableTaskStorage) HeartbeatDaemonTask(ctx context.Context, tasks []task.DaemonTask) (int, error) {
	var v int
	var err error
	s.mu.RLock()

	if s.mu.store == nil {
		err = ErrNotReady
	} else if err = s.mu.store.PingContext(ctx); err == nil {
		v, err = s.mu.store.HeartbeatDaemonTask(ctx, tasks)
	}
	s.mu.RUnlock()
	if err != nil {
		s.maybeRefresh(ctx)
	}
	return v, err
}

func (s *refreshableTaskStorage) AddCdcTask(ctx context.Context, dt task.DaemonTask, callback func(context.Context, SqlExecutor) (int, error)) (int, error) {
	v, err := s.AddCdcTaskSub(ctx, dt, callback)
	if err != nil {
		s.maybeRefresh(ctx)
	}
	return v, err
}

func (s *refreshableTaskStorage) AddCdcTaskSub(ctx context.Context, dt task.DaemonTask, callback func(context.Context, SqlExecutor) (int, error)) (int, error) {
	var v int
	var err error
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.mu.store == nil {
		err = ErrNotReady
	} else if err = s.mu.store.PingContext(ctx); err == nil {
		v, err = s.mu.store.AddCdcTask(ctx, dt, callback)
	}
	return v, err
}

func (s *refreshableTaskStorage) UpdateCdcTask(ctx context.Context, targetStatus task.TaskStatus, callback func(context.Context, task.TaskStatus, map[CdcTaskKey]struct{}, SqlExecutor) (int, error), conditions ...Condition) (int, error) {
	v, err := s.UpdateCdcTaskSub(ctx, targetStatus, callback, conditions...)
	if err != nil {
		s.maybeRefresh(ctx)
	}
	return v, err
}

func (s *refreshableTaskStorage) UpdateCdcTaskSub(ctx context.Context, targetStatus task.TaskStatus, callback func(context.Context, task.TaskStatus, map[CdcTaskKey]struct{}, SqlExecutor) (int, error), conditions ...Condition) (int, error) {
	var v int
	var err error
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.mu.store == nil {
		err = ErrNotReady
	} else if err = s.mu.store.PingContext(ctx); err == nil {
		v, err = s.mu.store.UpdateCdcTask(ctx, targetStatus, callback, conditions...)
	}
	return v, err
}

func (s *refreshableTaskStorage) maybeRefresh(ctx context.Context) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.closed {
		return false
	}

	if err := s.PingContext(ctx); err == nil {
		return false
	}
	db, err := s.moClient.GetOrConnect(ctx, true)
	if err != nil {
		s.rt.Logger().Error("failed to refresh task storage",
			zap.Error(err))
		return false
	}
	store, err := s.storageFactory(db)
	if err != nil {
		s.rt.Logger().Error("failed to refresh task storage",
			zap.Error(err))
		return false
	}
	if s.mu.store != nil {
		_ = s.mu.store.Close()
	}
	s.mu.store = store
	return true
}
