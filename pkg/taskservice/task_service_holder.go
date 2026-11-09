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
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"
)

var (
	ErrNotReady              = moerr.NewInvalidStateNoCtx("task store not ready")
	errSQLTaskStoreBusy      = errutil.Wrap(ErrNotReady, "task store holder is busy")
	taskStorageRefreshFailed = logutil.Event{Name: "task.storage.refresh.failed", Message: "task storage refresh failed"}
)

type taskServiceHolder struct {
	rt                         runtime.Runtime
	addressFactory             func(context.Context, bool) (string, error)
	taskStorageFactorySelector func(string, string, string) TaskStorageFactory
	mu                         struct {
		sync.RWMutex
		closed  bool
		service TaskService
	}
}

// NewTaskServiceHolder create a task service hold, it will create task storage and task service from the hakeeper's schedule command.
func NewTaskServiceHolder(
	rt runtime.Runtime,
	addressFactory func(context.Context, bool) (string, error)) TaskServiceHolder {
	return NewTaskServiceHolderWithTaskStorageFactorySelector(rt, addressFactory, func(username, password, database string) TaskStorageFactory {
		return newMySQLBasedTaskStorageFactory(username, password, database)
	})
}

// NewTaskServiceHolderWithTaskStorageFactorySelector is similar to NewTaskServiceHolder, but with a special
// task storage facroty selector
func NewTaskServiceHolderWithTaskStorageFactorySelector(
	rt runtime.Runtime,
	addressFactory func(context.Context, bool) (string, error),
	selector func(string, string, string) TaskStorageFactory) TaskServiceHolder {
	return &taskServiceHolder{
		rt:                         rt,
		addressFactory:             addressFactory,
		taskStorageFactorySelector: selector,
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
	if h.mu.closed {
		return ErrNotReady
	}
	if h.mu.service != nil {
		return nil
	}

	store := newRefreshableTaskStorage(
		h.rt,
		h.addressFactory,
		h.taskStorageFactorySelector(command.User.Username,
			command.User.Password,
			command.TaskDatabase))
	h.mu.service = NewTaskService(h.rt, store)
	return nil
}

func (h *taskServiceHolder) Get() (TaskService, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.mu.closed || h.mu.service == nil {
		return nil, false
	}
	return h.mu.service, true
}

type refreshableTaskStorage struct {
	rt             runtime.Runtime
	refreshC       chan string
	stopper        *stopper.Stopper
	addressFactory func(context.Context, bool) (string, error)
	storeFactory   TaskStorageFactory
	mu             struct {
		sync.RWMutex
		closed      bool
		lastAddress string
		store       TaskStorage
	}
}

func newRefreshableTaskStorage(
	rt runtime.Runtime,
	addressFactory func(context.Context, bool) (string, error),
	storeFactory TaskStorageFactory) TaskStorage {
	s := &refreshableTaskStorage{
		rt:             rt,
		refreshC:       make(chan string, 1),
		addressFactory: addressFactory,
		storeFactory:   storeFactory,
		stopper: stopper.NewStopper("refresh-taskstorage",
			stopper.WithLogger(rt.Logger().RawLogger())),
	}
	s.refresh(context.Background(), "")
	if err := s.stopper.RunTask(s.refreshTask); err != nil {
		panic(err)
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
	s.stopper.Stop()
	close(s.refreshC)
	return err
}

func (s *refreshableTaskStorage) PingContext(ctx context.Context) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.mu.store == nil {
		return nil
	}
	return s.mu.store.PingContext(ctx)
}

func (s *refreshableTaskStorage) AddAsyncTask(ctx context.Context, tasks ...task.AsyncTask) (int, error) {
	var v int
	var err error
	s.mu.RLock()
	lastAddress := s.mu.lastAddress
	if s.mu.store == nil {
		err = ErrNotReady
	} else if err = s.mu.store.PingContext(ctx); err == nil {
		v, err = s.mu.store.AddAsyncTask(ctx, tasks...)
	}
	s.mu.RUnlock()
	if err != nil {
		s.maybeRefresh(lastAddress)
	}
	return v, err
}

func (s *refreshableTaskStorage) UpdateAsyncTask(ctx context.Context, tasks []task.AsyncTask, conditions ...Condition) (int, error) {
	var v int
	var err error
	s.mu.RLock()
	lastAddress := s.mu.lastAddress
	if s.mu.store == nil {
		err = ErrNotReady
	} else if err = s.mu.store.PingContext(ctx); err == nil {
		v, err = s.mu.store.UpdateAsyncTask(ctx, tasks, conditions...)
	}
	s.mu.RUnlock()
	if err != nil {
		s.maybeRefresh(lastAddress)
	}
	return v, err
}

func (s *refreshableTaskStorage) DeleteAsyncTask(ctx context.Context, conditions ...Condition) (int, error) {
	var v int
	var err error
	s.mu.RLock()
	lastAddress := s.mu.lastAddress
	if s.mu.store == nil {
		err = ErrNotReady
	} else if err = s.mu.store.PingContext(ctx); err == nil {
		v, err = s.mu.store.DeleteAsyncTask(ctx, conditions...)
	}
	s.mu.RUnlock()
	if err != nil {
		s.maybeRefresh(lastAddress)
	}
	return v, err
}

func (s *refreshableTaskStorage) QueryAsyncTask(ctx context.Context, conditions ...Condition) ([]task.AsyncTask, error) {
	if ctx.Value(sqlTaskFairFetchContextKey{}) == true {
		return querySQLTaskAsync(ctx, s, conditions...)
	}
	v, err, _ := s.queryAsyncTaskPage(ctx, conditions...)
	return v, err
}

// Classify the same store that serves the normal query; it may be replaced
// on refresh. Keep the holder's normal ping and refresh behavior unchanged.
func (s *refreshableTaskStorage) queryAsyncTaskPage(ctx context.Context, conditions ...Condition) ([]task.AsyncTask, error, bool) {
	var v []task.AsyncTask
	var err error
	var trustedSource bool
	s.mu.RLock()
	lastAddress := s.mu.lastAddress
	if s.mu.store == nil {
		err = ErrNotReady
	} else if err = s.mu.store.PingContext(ctx); err == nil {
		v, err = s.mu.store.QueryAsyncTask(ctx, conditions...)
		switch s.mu.store.(type) {
		case *mysqlTaskStorage, *memTaskStorage:
			trustedSource = true
		}
	}
	s.mu.RUnlock()
	if err != nil {
		s.maybeRefresh(lastAddress)
	}
	return v, err, trustedSource
}

func (s *refreshableTaskStorage) AddCronTask(ctx context.Context, tasks ...task.CronTask) (int, error) {
	var v int
	var err error
	s.mu.RLock()
	lastAddress := s.mu.lastAddress
	if s.mu.store == nil {
		err = ErrNotReady
	} else if err = s.mu.store.PingContext(ctx); err == nil {
		v, err = s.mu.store.AddCronTask(ctx, tasks...)
	}
	s.mu.RUnlock()
	if err != nil {
		s.maybeRefresh(lastAddress)
	}
	return v, err
}

func (s *refreshableTaskStorage) QueryCronTask(ctx context.Context, c ...Condition) ([]task.CronTask, error) {
	var v []task.CronTask
	var err error
	s.mu.RLock()
	lastAddress := s.mu.lastAddress
	if s.mu.store == nil {
		err = ErrNotReady
	} else if err = s.mu.store.PingContext(ctx); err == nil {
		v, err = s.mu.store.QueryCronTask(ctx, c...)
	}
	s.mu.RUnlock()
	if err != nil {
		s.maybeRefresh(lastAddress)
	}
	return v, err
}

func (s *refreshableTaskStorage) UpdateCronTask(ctx context.Context, cronTask task.CronTask, task task.AsyncTask) (int, error) {
	var v int
	var err error
	s.mu.RLock()
	lastAddress := s.mu.lastAddress
	if s.mu.store == nil {
		err = ErrNotReady
	} else if err = s.mu.store.PingContext(ctx); err == nil {
		v, err = s.mu.store.UpdateCronTask(ctx, cronTask, task)
	}
	s.mu.RUnlock()
	if err != nil {
		s.maybeRefresh(lastAddress)
	}
	return v, err
}

func (s *refreshableTaskStorage) AddDaemonTask(ctx context.Context, tasks ...task.DaemonTask) (int, error) {
	var v int
	var err error
	s.mu.RLock()
	lastAddress := s.mu.lastAddress
	if s.mu.store == nil {
		err = ErrNotReady
	} else if err = s.mu.store.PingContext(ctx); err == nil {
		v, err = s.mu.store.AddDaemonTask(ctx, tasks...)
	}
	s.mu.RUnlock()
	if err != nil {
		s.maybeRefresh(lastAddress)
	}
	return v, err
}

func (s *refreshableTaskStorage) UpdateDaemonTask(ctx context.Context, tasks []task.DaemonTask, conditions ...Condition) (int, error) {
	var v int
	var err error
	s.mu.RLock()
	lastAddress := s.mu.lastAddress
	if s.mu.store == nil {
		err = ErrNotReady
	} else if err = s.mu.store.PingContext(ctx); err == nil {
		v, err = s.mu.store.UpdateDaemonTask(ctx, tasks, conditions...)
	}
	s.mu.RUnlock()
	if err != nil {
		s.maybeRefresh(lastAddress)
	}
	return v, err
}

func (s *refreshableTaskStorage) UpdateDaemonTaskError(ctx context.Context, claim task.DaemonTask, release bool) (int, error) {
	var n int
	var err error
	s.mu.RLock()
	lastAddress := s.mu.lastAddress
	if s.mu.store == nil {
		err = ErrNotReady
	} else if err = s.mu.store.PingContext(ctx); err == nil {
		n, err = s.mu.store.UpdateDaemonTaskError(ctx, claim, release)
	}
	s.mu.RUnlock()
	if err != nil {
		s.maybeRefresh(lastAddress)
	}
	return n, err
}

func (s *refreshableTaskStorage) UpdateDaemonTaskStatus(
	ctx context.Context,
	taskID uint64,
	status task.TaskStatus,
	updateAt time.Time,
	endAt time.Time,
	conditions ...Condition,
) (int, error) {
	var v int
	var err error
	s.mu.RLock()
	lastAddress := s.mu.lastAddress
	if s.mu.store == nil {
		err = ErrNotReady
	} else if err = s.mu.store.PingContext(ctx); err == nil {
		v, err = s.mu.store.UpdateDaemonTaskStatus(
			ctx, taskID, status, updateAt, endAt, conditions...)
	}
	s.mu.RUnlock()
	if err != nil {
		s.maybeRefresh(lastAddress)
	}
	return v, err
}

func (s *refreshableTaskStorage) RequestDaemonTaskStatus(ctx context.Context, observed task.DaemonTask, status task.TaskStatus) (int, error) {
	var n int
	var err error
	s.mu.RLock()
	lastAddress := s.mu.lastAddress
	if s.mu.store == nil {
		err = ErrNotReady
	} else if err = s.mu.store.PingContext(ctx); err == nil {
		n, err = s.mu.store.RequestDaemonTaskStatus(ctx, observed, status)
	}
	s.mu.RUnlock()
	if err != nil {
		s.maybeRefresh(lastAddress)
	}
	return n, err
}

func (s *refreshableTaskStorage) DeleteDaemonTask(ctx context.Context, conditions ...Condition) (int, error) {
	var v int
	var err error
	s.mu.RLock()
	lastAddress := s.mu.lastAddress
	if s.mu.store == nil {
		err = ErrNotReady
	} else if err = s.mu.store.PingContext(ctx); err == nil {
		v, err = s.mu.store.DeleteDaemonTask(ctx, conditions...)
	}
	s.mu.RUnlock()
	if err != nil {
		s.maybeRefresh(lastAddress)
	}
	return v, err
}

func (s *refreshableTaskStorage) QueryDaemonTask(ctx context.Context, conditions ...Condition) ([]task.DaemonTask, error) {
	var v []task.DaemonTask
	var err error
	s.mu.RLock()
	lastAddress := s.mu.lastAddress
	if s.mu.store == nil {
		err = ErrNotReady
	} else if err = s.mu.store.PingContext(ctx); err == nil {
		v, err = s.mu.store.QueryDaemonTask(ctx, conditions...)
	}
	s.mu.RUnlock()
	if err != nil {
		s.maybeRefresh(lastAddress)
	}
	return v, err
}

func (s *refreshableTaskStorage) AddSQLTask(ctx context.Context, tasks ...SQLTask) (int, error) {
	var v int
	var err error
	expected := false
	s.mu.RLock()
	lastAddress := s.mu.lastAddress
	if s.mu.store == nil {
		err = ErrNotReady
	} else if err = s.mu.store.PingContext(ctx); err == nil {
		v, err = s.mu.store.AddSQLTask(ctx, tasks...)
		expected = err == ErrSQLTaskAccountMissing
	}
	s.mu.RUnlock()
	if err != nil && !expected {
		s.maybeRefresh(lastAddress)
	}
	return v, err
}

func (s *refreshableTaskStorage) AlterSQLTask(ctx context.Context, observed, updated SQLTask, action SQLTaskAlterAction) (int, error) {
	var v int
	var err error
	var expected bool
	s.mu.RLock()
	lastAddress := s.mu.lastAddress
	if s.mu.store == nil {
		err = ErrNotReady
	} else if err = s.mu.store.PingContext(ctx); err == nil {
		v, err = s.mu.store.AlterSQLTask(ctx, observed, updated, action)
		expected = err == ErrSQLTaskSequenceExhausted
	}
	s.mu.RUnlock()
	if err != nil && !expected {
		s.maybeRefresh(lastAddress)
	}
	return v, err
}

func (s *refreshableTaskStorage) DeleteSQLTask(ctx context.Context, conditions ...Condition) (int, error) {
	var v int
	var err error
	s.mu.RLock()
	lastAddress := s.mu.lastAddress
	if s.mu.store == nil {
		err = ErrNotReady
	} else if err = s.mu.store.PingContext(ctx); err == nil {
		v, err = s.mu.store.DeleteSQLTask(ctx, conditions...)
	}
	s.mu.RUnlock()
	if err != nil {
		s.maybeRefresh(lastAddress)
	}
	return v, err
}

func (s *refreshableTaskStorage) QuerySQLTask(ctx context.Context, conditions ...Condition) ([]SQLTask, error) {
	var v []SQLTask
	var err error
	s.mu.RLock()
	lastAddress := s.mu.lastAddress
	if s.mu.store == nil {
		err = ErrNotReady
	} else if err = s.mu.store.PingContext(ctx); err == nil {
		v, err = s.mu.store.QuerySQLTask(ctx, conditions...)
	}
	s.mu.RUnlock()
	if err != nil {
		s.maybeRefresh(lastAddress)
	}
	return v, err
}

func (s *refreshableTaskStorage) QuerySQLTaskRun(ctx context.Context, conditions ...Condition) ([]SQLTaskRun, error) {
	var v []SQLTaskRun
	var err error
	s.mu.RLock()
	lastAddress := s.mu.lastAddress
	if s.mu.store == nil {
		err = ErrNotReady
	} else if err = s.mu.store.PingContext(ctx); err == nil {
		v, err = s.mu.store.QuerySQLTaskRun(ctx, conditions...)
	}
	s.mu.RUnlock()
	if err != nil {
		s.maybeRefresh(lastAddress)
	}
	return v, err
}

func (s *refreshableTaskStorage) QueryLatestSQLTaskRun(ctx context.Context, accountID uint32, taskIDs []uint64) ([]SQLTaskRun, error) {
	var v []SQLTaskRun
	var err error
	s.mu.RLock()
	lastAddress := s.mu.lastAddress
	if s.mu.store == nil {
		err = ErrNotReady
	} else if err = s.mu.store.PingContext(ctx); err == nil {
		v, err = s.mu.store.QueryLatestSQLTaskRun(ctx, accountID, taskIDs)
	}
	s.mu.RUnlock()
	if err != nil {
		s.maybeRefresh(lastAddress)
	}
	return v, err
}

func (s *refreshableTaskStorage) AcquireSQLTaskRun(ctx context.Context, sqlTask SQLTask, run SQLTaskRun, admission *SQLTaskRunAdmission) (uint64, error) {
	var v uint64
	err := s.withSQLTaskMetadataStore(ctx, func(store TaskStorage) error {
		var err error
		v, err = store.AcquireSQLTaskRun(ctx, sqlTask, run, admission)
		return err
	})
	return v, err
}

// withSQLTaskMetadataStore is used only by bounded SQL-task metadata
// operations. A refresh or Close must not leave a caller waiting indefinitely
// to acquire the holder lock or to signal another refresh.
func (s *refreshableTaskStorage) withSQLTaskMetadataStore(ctx context.Context, fn func(TaskStorage) error) error {
	return s.withSQLTaskStore(ctx, fn, true)
}

// Fetch decoding errors and the intentionally short fast-query deadline do
// not mean the storage endpoint changed. Avoid reconnecting on every bad row.
func (s *refreshableTaskStorage) withSQLTaskFetchStore(ctx context.Context, fn func(TaskStorage) error) error {
	return s.withSQLTaskStore(ctx, fn, false)
}

func (s *refreshableTaskStorage) withSQLTaskStore(ctx context.Context, fn func(TaskStorage) error, refreshAll bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !s.mu.TryRLock() {
		return errSQLTaskStoreBusy
	}
	defer s.mu.RUnlock()
	if s.mu.closed {
		return ErrNotReady
	}
	if s.mu.store == nil {
		s.signalSQLTaskRefreshLocked()
		return ErrNotReady
	}
	err := fn(s.mu.store)
	if err != nil && (refreshAll && !expectedSQLTaskMetadataError(err) ||
		!refreshAll && sqlTaskFetchConnectionError(ctx, err)) {
		s.signalSQLTaskRefreshLocked()
	}
	return err
}

// Only a single chain of known semantic errors proves that reconnecting cannot
// help. A joined error may also contain a rollback failure, even when one of
// its causes is a known semantic error. Limit traversal so cyclic or overly
// deep chains are treated as unknown.
func expectedSQLTaskMetadataError(err error) bool {
	for range 32 {
		if err == nil {
			return false
		}
		if _, ok := err.(net.Error); ok {
			return false
		}
		switch err {
		case ErrSQLTaskOverlap, ErrSQLTaskNotFound, ErrSQLTaskClaimed,
			ErrSQLTaskStaleFiring, ErrSQLTaskRecoveryCapacity,
			ErrSQLTaskSequenceExhausted, errSQLTaskReceiptConflict:
			return true
		}
		if moerr.IsMoErrCode(err, moerr.ErrInvalidInput) {
			return true
		}
		// Custom identity hooks can expose an independent connection or commit
		// error even when the unwrapped cause is an expected business error.
		if _, ok := err.(interface{ Is(error) bool }); ok {
			return false
		}
		if _, ok := err.(interface{ As(any) bool }); ok {
			return false
		}
		switch cause := err.(type) {
		case interface{ Unwrap() []error }:
			errs := cause.Unwrap()
			if len(errs) != 1 {
				return false
			}
			err = errs[0]
		case interface{ Unwrap() error }:
			err = cause.Unwrap()
		default:
			return false
		}
	}
	return false
}

// Caller holds the read lock, so Close cannot close refreshC while signaling.
func (s *refreshableTaskStorage) signalSQLTaskRefreshLocked() {
	select {
	case s.refreshC <- s.mu.lastAddress:
	default:
	}
}

func sqlTaskFetchConnectionError(ctx context.Context, err error) bool {
	if ctx.Err() != nil {
		return false
	}
	if errors.Is(err, driver.ErrBadConn) || errors.Is(err, sql.ErrConnDone) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, mysql.ErrInvalidConn) || errors.Is(err, mysql.ErrMalformPkt) {
		return true
	}
	return sqlTaskFetchNetworkError(err)
}

func sqlTaskFetchNetworkError(err error) bool {
	if err == nil || err == context.Canceled || err == context.DeadlineExceeded {
		return false
	}
	if _, ok := err.(net.Error); ok {
		return true
	}
	switch cause := err.(type) {
	case interface{ Unwrap() []error }:
		for _, nested := range cause.Unwrap() {
			if sqlTaskFetchNetworkError(nested) {
				return true
			}
		}
	case interface{ Unwrap() error }:
		return sqlTaskFetchNetworkError(cause.Unwrap())
	}
	return false
}

func (s *refreshableTaskStorage) TriggerSQLTask(ctx context.Context, sqlTask SQLTask, asyncTask task.AsyncTask) (int, error) {
	var v int
	var err error
	s.mu.RLock()
	lastAddress := s.mu.lastAddress
	if s.mu.store == nil {
		err = ErrNotReady
	} else if err = s.mu.store.PingContext(ctx); err == nil {
		v, err = s.mu.store.TriggerSQLTask(ctx, sqlTask, asyncTask)
	}
	s.mu.RUnlock()
	if err != nil {
		s.maybeRefresh(lastAddress)
	}
	return v, err
}

func (s *refreshableTaskStorage) HeartbeatDaemonTask(ctx context.Context, tasks []task.DaemonTask) (int, error) {
	var v int
	var err error
	s.mu.RLock()
	lastAddress := s.mu.lastAddress
	if s.mu.store == nil {
		err = ErrNotReady
	} else if err = s.mu.store.PingContext(ctx); err == nil {
		v, err = s.mu.store.HeartbeatDaemonTask(ctx, tasks)
	}
	s.mu.RUnlock()
	if err != nil {
		s.maybeRefresh(lastAddress)
	}
	return v, err
}

func (s *refreshableTaskStorage) ValidateDaemonTask(
	ctx context.Context,
	t task.DaemonTask,
) (bool, error) {
	var valid bool
	var err error
	s.mu.RLock()
	lastAddress := s.mu.lastAddress
	if s.mu.store == nil {
		err = ErrNotReady
	} else {
		// Validation is itself a read round trip. An additional PingContext would
		// double the CDC effect-fence hot path without strengthening the result;
		// the read error below drives the same asynchronous address refresh.
		valid, err = s.mu.store.ValidateDaemonTask(ctx, t)
	}
	s.mu.RUnlock()
	if err != nil {
		s.maybeRefresh(lastAddress)
	}
	return valid, err
}

func (s *refreshableTaskStorage) AddCDCTask(ctx context.Context, dt task.DaemonTask, callback func(context.Context, SqlExecutor) (int, error)) (int, error) {
	v, lastAddress, err := s.AddCdcTaskSub(ctx, dt, callback)
	if err != nil {
		s.maybeRefresh(lastAddress)
	}
	return v, err
}

func (s *refreshableTaskStorage) AddCdcTaskSub(ctx context.Context, dt task.DaemonTask, callback func(context.Context, SqlExecutor) (int, error)) (int, string, error) {
	var v int
	var err error
	s.mu.RLock()
	defer s.mu.RUnlock()
	lastAddress := s.mu.lastAddress
	if s.mu.store == nil {
		err = ErrNotReady
	} else if err = s.mu.store.PingContext(ctx); err == nil {
		v, err = s.mu.store.AddCDCTask(ctx, dt, callback)
	}
	return v, lastAddress, err
}

func (s *refreshableTaskStorage) UpdateCDCTask(ctx context.Context, targetStatus task.TaskStatus, callback func(context.Context, task.TaskStatus, map[CDCTaskKey]struct{}, SqlExecutor) (int, error), conditions ...Condition) (int, error) {
	v, lastAddress, err := s.UpdateCdcTaskSub(ctx, targetStatus, callback, conditions...)
	if err != nil {
		s.maybeRefresh(lastAddress)
	}
	return v, err
}

func (s *refreshableTaskStorage) UpdateCdcTaskSub(ctx context.Context, targetStatus task.TaskStatus, callback func(context.Context, task.TaskStatus, map[CDCTaskKey]struct{}, SqlExecutor) (int, error), conditions ...Condition) (int, string, error) {
	var v int
	var err error
	s.mu.RLock()
	defer s.mu.RUnlock()
	lastAddress := s.mu.lastAddress
	if s.mu.store == nil {
		err = ErrNotReady
	} else if err = s.mu.store.PingContext(ctx); err == nil {
		v, err = s.mu.store.UpdateCDCTask(ctx, targetStatus, callback, conditions...)
	}
	return v, lastAddress, err
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
	defer s.rt.Logger().LogAction("close refresh-task",
		log.DefaultLogOptions().WithLevel(zap.DebugLevel))()

	for {
		select {
		case <-ctx.Done():
			return
		case lastAddress := <-s.refreshC:
			s.refresh(ctx, lastAddress)
			// see pkg/logservice/service_commands.go#132
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}
}

func (s *refreshableTaskStorage) refresh(ctx context.Context, lastAddress string) {
	s.mu.Lock()
	if s.mu.closed {
		s.mu.Unlock()
		return
	}
	if lastAddress != "" && lastAddress != s.mu.lastAddress {
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()

	connectAddress, err := s.addressFactory(ctx, true)
	if err != nil {
		s.rt.Logger().ErrorEvent(taskStorageRefreshFailed,
			zap.String("address", lastAddress),
			zap.Error(err))
		return
	}

	s.rt.Logger().Debug(
		"taskservice.holder.refresh.trying",
		zap.String("address", connectAddress),
	)
	store, err := s.storeFactory.Create(connectAddress)
	if err != nil {
		s.rt.Logger().ErrorEvent(taskStorageRefreshFailed,
			zap.String("address", connectAddress),
			zap.Error(err))
		return
	}
	if isNilTaskValue(store) {
		s.rt.Logger().ErrorEvent(taskStorageRefreshFailed,
			zap.String("address", connectAddress),
			zap.Error(ErrNotReady))
		return
	}

	// Creating a replacement can block on DNS, dialing, or credentials. Recheck
	// the generation after that work so an old refresh cannot replace or close a
	// newer store, and a concurrent Close cannot publish a fresh resource.
	s.mu.Lock()
	if s.mu.closed || ctx.Err() != nil ||
		(lastAddress != "" && lastAddress != s.mu.lastAddress) {
		current := s.mu.store
		s.mu.Unlock()
		if store != current {
			_ = store.Close()
		}
		return
	}
	previous := s.mu.store
	s.mu.store = store
	s.mu.lastAddress = connectAddress
	s.mu.Unlock()

	if previous != nil && previous != store {
		if err := previous.Close(); err != nil {
			s.rt.Logger().Error(
				"taskservice.holder.refresh.close-previous.failed",
				zap.String("address", connectAddress),
				zap.Error(err))
		}
	}
	s.rt.Logger().Debug(
		"taskservice.holder.refresh.completed",
		zap.String("sql-address", connectAddress),
	)
}

type mysqlBasedStorageFactory struct {
	dsnTemplate string
}

// newMySQLBasedTaskStorageFactory creates a mysql based task storage factory using the special username, password and database
func newMySQLBasedTaskStorageFactory(username, password, database string) TaskStorageFactory {
	return &mysqlBasedStorageFactory{
		dsnTemplate: fmt.Sprintf("%s:%s@tcp(%s)/%s?readTimeout=15s&writeTimeout=15s&timeout=15s&parseTime=true&loc=Local",
			username,
			password,
			"%s", database),
	}
}

func (f *mysqlBasedStorageFactory) Create(address string) (TaskStorage, error) {
	return newMysqlTaskStorage(fmt.Sprintf(f.dsnTemplate, address))
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
