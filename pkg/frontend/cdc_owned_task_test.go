// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/stretchr/testify/require"
)

// ownedStateSQL uses the transaction arguments for the catalog state match, so
// recovery tests can exercise more than one attempted transition.
type ownedStateSQL struct{ catalog *cdcCatalogStateExecutor }

func (*ownedStateSQL) PrepareContext(context.Context, string) (*sql.Stmt, error) {
	return nil, errors.New("unexpected prepare")
}

func (*ownedStateSQL) QueryContext(context.Context, string, ...any) (*sql.Rows, error) {
	return nil, errors.New("unexpected query")
}

func (s *ownedStateSQL) ExecContext(_ context.Context, query string, args ...any) (sql.Result, error) {
	s.catalog.mu.Lock()
	defer s.catalog.mu.Unlock()
	var to, from string
	if strings.Contains(query, "state=state") {
		to = args[2].(string)
		from = to
	} else {
		to = args[0].(string)
		from = args[len(args)-1].(string)
	}
	if s.catalog.state != from {
		return ownedCatalogTestResult(0), nil
	}
	s.catalog.state = to
	return ownedCatalogTestResult(1), nil
}

type ownedStartupStorage struct {
	taskservice.TaskService
	catalog    *cdcCatalogStateExecutor
	firstErr   error
	owner      taskservice.CDCTaskOwner
	transition taskservice.CDCTaskTransition
}

func (s *ownedStartupStorage) UpdateOwnedCDCTask(ctx context.Context, owner taskservice.CDCTaskOwner, transition taskservice.CDCTaskTransition, mutate func(context.Context, taskservice.SqlExecutor) error) error {
	if s.firstErr != nil {
		err := s.firstErr
		s.firstErr = nil
		return err
	}
	if err := mutate(ctx, &ownedStateSQL{catalog: s.catalog}); err != nil {
		return err
	}
	s.owner, s.transition = owner, transition
	return nil
}

func TestCDCReopensPublishedRestart(t *testing.T) {
	for _, state := range []string{cdc.CDCState_Failed, cdc.CDCState_Restarting, cdc.CDCState_Running, cdc.CDCState_Paused} {
		t.Run(state, func(t *testing.T) {
			catalog := &cdcCatalogStateExecutor{state: state}
			storage := &ownedStartupStorage{catalog: catalog}
			exec := &CDCTaskExecutor{spec: &task.CreateCdcDetails{TaskId: "task", Accounts: []*task.Account{{Id: 1}}}, taskService: storage}
			err := exec.admitRestartCatalogState(context.Background())
			if state == cdc.CDCState_Paused {
				require.Error(t, err)
				require.Equal(t, state, catalog.getState())
				return
			}
			require.NoError(t, err)
			require.Equal(t, cdc.CDCState_Restarting, catalog.getState())
			require.Equal(t, task.TaskStatus_RestartRequested, storage.owner.Status)
			require.Equal(t, task.TaskStatus_RestartRequested, storage.transition.Status)
		})
	}
}

func TestCDCStartupRecoversFromTaskStoreOutage(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	mock.ExpectClose()
	require.NoError(t, db.Close())
	storeErr := db.PingContext(context.Background())
	require.Error(t, storeErr)
	catalog := &cdcCatalogStateExecutor{state: cdc.CDCState_Running}
	storage := &ownedStartupStorage{catalog: catalog, firstErr: &taskservice.CDCTaskStorageError{Err: storeErr}}
	exec := &CDCTaskExecutor{
		spec:          &task.CreateCdcDetails{TaskId: "task", Accounts: []*task.Account{{Id: 1}}},
		ie:            catalog,
		taskService:   storage,
		taskOwner:     taskservice.CDCTaskOwner{DaemonID: 1, Runner: "cn", Key: taskservice.CDCTaskKey{TaskId: "task", AccountId: 1}},
		stateMachine:  NewExecutorStateMachine(),
		activeRoutine: cdc.NewCdcActiveRoutine(),
		holdCh:        make(chan int, 1),
	}
	require.NoError(t, exec.stateMachine.Transition(TransitionStart))
	err = exec.Start(taskservice.WithRestartAdmission(context.Background()))
	require.ErrorIs(t, err, storeErr)
	require.NoError(t, exec.ReleaseFailedStart(context.Background(), err))
	require.Equal(t, cdc.CDCState_Restarting, catalog.getState())
	require.Equal(t, task.TaskStatus_RestartRequested, storage.transition.Status)
	require.True(t, storage.transition.ReleaseLease)
}

type blockedOwnedStore struct {
	taskservice.TaskService
	db      *sql.DB
	entered chan struct{}
}

func (s *blockedOwnedStore) UpdateOwnedCDCTask(ctx context.Context, _ taskservice.CDCTaskOwner, _ taskservice.CDCTaskTransition, _ func(context.Context, taskservice.SqlExecutor) error) error {
	close(s.entered)
	return s.db.PingContext(ctx)
}

func TestCDCRestartPoolWaitHonorsDeadlineAndShutdown(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		name := "deadline"
		if shutdown {
			name = "shutdown"
		}
		t.Run(name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			db.SetMaxOpenConns(1)
			held, err := db.Conn(context.Background())
			require.NoError(t, err)
			store := &blockedOwnedStore{db: db, entered: make(chan struct{})}
			root, cancel := context.WithCancel(context.Background())
			t.Cleanup(func() { cancel(); _ = held.Close(); _ = db.Close() })
			exec := &CDCTaskExecutor{
				spec:                  &task.CreateCdcDetails{TaskId: "task", Accounts: []*task.Account{{Id: 1}}},
				taskService:           store,
				stateMachine:          NewExecutorStateMachine(),
				holdCh:                make(chan int, 1),
				restartStartupTimeout: 80 * time.Millisecond,
				startFunc:             func(context.Context) error { return errors.New("unexpected replacement") },
			}
			exec.bindLifecycleContext(root)
			require.NoError(t, exec.stateMachine.Transition(TransitionStart))
			require.NoError(t, exec.stateMachine.Transition(TransitionStartFail))
			done := make(chan error, 1)
			go func() { done <- exec.Restart() }()
			select {
			case <-store.entered:
			case <-time.After(time.Second):
				t.Fatal("restart did not enter task-store admission")
			}
			if shutdown {
				cancel()
			}
			var restartErr error
			returned := false
			select {
			case restartErr = <-done:
				returned = true
			case <-time.After(400 * time.Millisecond):
			}
			require.NoError(t, held.Close())
			if !returned {
				restartErr = <-done
			}
			cancel()
			mock.ExpectClose()
			require.NoError(t, db.Close())
			require.True(t, returned, "restart remained blocked in the exhausted pool")
			if shutdown {
				require.ErrorIs(t, restartErr, context.Canceled)
			} else {
				require.ErrorIs(t, restartErr, context.DeadlineExceeded)
				require.ErrorIs(t, restartErr, moerr.CauseRestartTaskHandle)
			}
		})
	}
}

func TestCDCRestartCallbackWaitHonorsDeadlineAndShutdown(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		name := "deadline"
		if shutdown {
			name = "shutdown"
		}
		t.Run(name, func(t *testing.T) {
			root, cancel := context.WithCancel(context.Background())
			defer cancel()
			exec := &CDCTaskExecutor{stateMachine: NewExecutorStateMachine(), restartStartupTimeout: 80 * time.Millisecond}
			exec.bindLifecycleContext(root)
			require.NoError(t, exec.stateMachine.Transition(TransitionStart))
			require.NoError(t, exec.stateMachine.Transition(TransitionStartSuccess))
			exec.callbackMu.Lock()
			defer exec.callbackMu.Unlock()
			done := make(chan error, 1)
			go func() { done <- exec.Restart() }()
			if shutdown {
				cancel()
			}
			select {
			case err := <-done:
				if shutdown {
					require.ErrorIs(t, err, context.Canceled)
				} else {
					require.ErrorIs(t, err, context.DeadlineExceeded)
					require.ErrorIs(t, err, moerr.CauseRestartTaskHandle)
				}
			case <-time.After(time.Second):
				t.Fatal("restart did not stop waiting for an active callback")
			}
			require.Equal(t, StateRunning, exec.stateMachine.State())
		})
	}
}

func TestCDCRestartDeadlineDoesNotCancelStartedReplacement(t *testing.T) {
	root, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	exec := &CDCTaskExecutor{
		spec:                  &task.CreateCdcDetails{TaskId: "task", Accounts: []*task.Account{{Id: 1}}},
		activeRoutine:         cdc.NewCdcActiveRoutine(),
		stateMachine:          NewExecutorStateMachine(),
		holdCh:                make(chan int, 1),
		restartStartupTimeout: 80 * time.Millisecond,
	}
	exec.bindLifecycleContext(root)
	require.NoError(t, exec.stateMachine.Transition(TransitionStart))
	require.NoError(t, exec.stateMachine.Transition(TransitionStartSuccess))
	stopped := make(chan struct{})
	exec.startFunc = func(ctx context.Context) error {
		attempt := cdcStartAttemptFromContext(ctx)
		if attempt.completeRestart() {
			exec.finishRestartWaiter(attempt.generation, nil)
		}
		<-ctx.Done()
		close(stopped)
		return ctx.Err()
	}
	require.NoError(t, exec.Restart())
	select {
	case <-stopped:
		t.Fatal("successful replacement inherited the admission deadline")
	case <-time.After(150 * time.Millisecond):
	}
	cancel()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("replacement did not inherit lifecycle cancellation")
	}
}

func TestCDCStartupFailureRetryClassification(t *testing.T) {
	for _, tc := range []struct {
		name           string
		cause          error
		localRestart   bool
		restart        bool
		from           string
		wantState      string
		wantDaemon     task.TaskStatus
		expectedDaemon task.TaskStatus
	}{
		{name: "transient restart", cause: moerr.NewTxnNeedRetryNoCtx(), restart: true, from: cdc.CDCState_Restarting, wantState: cdc.CDCState_Restarting, wantDaemon: task.TaskStatus_Running, expectedDaemon: task.TaskStatus_Running},
		{name: "permanent restart", cause: errors.New("invalid source"), restart: true, from: cdc.CDCState_Restarting, wantState: cdc.CDCState_Failed, wantDaemon: task.TaskStatus_Error, expectedDaemon: task.TaskStatus_Running},
		{name: "permanent local restart", cause: errors.New("invalid source"), localRestart: true, from: cdc.CDCState_Restarting, wantState: cdc.CDCState_Failed, wantDaemon: task.TaskStatus_Error, expectedDaemon: task.TaskStatus_RestartRequested},
		{name: "transient ordinary start", cause: moerr.NewTxnNeedRetryNoCtx(), from: cdc.CDCState_Running, wantState: cdc.CDCState_Restarting, wantDaemon: task.TaskStatus_Running, expectedDaemon: task.TaskStatus_Running},
		{name: "unknown canceled commit", cause: &taskservice.CDCTaskStorageError{Err: context.Canceled, CommitUnknown: true}, restart: true, from: cdc.CDCState_Restarting, wantState: cdc.CDCState_Restarting, wantDaemon: task.TaskStatus_Running, expectedDaemon: task.TaskStatus_Running},
		{name: "store permission denied", cause: &taskservice.CDCTaskStorageError{Err: &mysql.MySQLError{Number: 1045}}, from: cdc.CDCState_Running, wantState: cdc.CDCState_Failed, wantDaemon: task.TaskStatus_Error, expectedDaemon: task.TaskStatus_Running},
		{name: "ordinary start failure", cause: errors.New("invalid source"), from: cdc.CDCState_Running, wantState: cdc.CDCState_Failed, wantDaemon: task.TaskStatus_Error, expectedDaemon: task.TaskStatus_Running},
	} {
		t.Run(tc.name, func(t *testing.T) {
			catalog := &cdcCatalogStateExecutor{state: tc.from, currentState: tc.from, targetState: tc.wantState}
			exec := &CDCTaskExecutor{spec: &task.CreateCdcDetails{TaskId: "task", Accounts: []*task.Account{{Id: 1}}}, ie: catalog}
			service := bindOwnedCatalogTest(exec, catalog)
			require.NoError(t, exec.reportStartupFailure(context.Background(), tc.cause, tc.from, tc.localRestart, tc.restart))
			require.Equal(t, tc.wantState, catalog.getState())
			require.Contains(t, catalog.execSQL, tc.cause.Error())
			service.mu.Lock()
			defer service.mu.Unlock()
			require.Len(t, service.transitions, 1)
			require.Equal(t, tc.expectedDaemon, service.owners[0].Status)
			require.Equal(t, tc.wantDaemon, service.transitions[0].Status)
		})
	}
}

func TestCDCFailedStartReleasePreservesOutcome(t *testing.T) {
	for _, tc := range []struct {
		name        string
		state       string
		cause       error
		wantState   string
		wantStatus  task.TaskStatus
		wantRelease bool
		wantWrites  int
		wantError   bool
	}{
		{name: "transient", state: cdc.CDCState_Restarting, cause: moerr.NewTxnNeedRetryNoCtx(), wantState: cdc.CDCState_Restarting, wantStatus: task.TaskStatus_RestartRequested, wantRelease: true, wantWrites: 1},
		{name: "transient before admission", state: cdc.CDCState_Running, cause: moerr.NewTxnNeedRetryNoCtx(), wantState: cdc.CDCState_Restarting, wantStatus: task.TaskStatus_RestartRequested, wantRelease: true, wantWrites: 1},
		{name: "shutdown", state: cdc.CDCState_Restarting, cause: context.Canceled, wantState: cdc.CDCState_Restarting, wantStatus: task.TaskStatus_RestartRequested, wantRelease: true, wantWrites: 1},
		{name: "running shutdown", state: cdc.CDCState_Running, cause: context.Canceled, wantState: cdc.CDCState_Running},
		{name: "unknown canceled commit", state: cdc.CDCState_Running, cause: &taskservice.CDCTaskStorageError{Err: context.Canceled, CommitUnknown: true}, wantState: cdc.CDCState_Restarting, wantStatus: task.TaskStatus_RestartRequested, wantRelease: true, wantWrites: 1},
		{name: "terminal failure", state: cdc.CDCState_Restarting, cause: errors.New("invalid source"), wantState: cdc.CDCState_Failed, wantStatus: task.TaskStatus_Error, wantWrites: 1},
		{name: "already failed", state: cdc.CDCState_Failed, cause: errors.New("invalid source"), wantState: cdc.CDCState_Failed},
		{name: "concurrent pause", state: cdc.CDCState_Paused, cause: context.Canceled, wantState: cdc.CDCState_Paused, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			catalog := &cdcCatalogStateExecutor{state: tc.state, currentState: tc.state, targetState: tc.wantState}
			exec := &CDCTaskExecutor{spec: &task.CreateCdcDetails{TaskId: "task", Accounts: []*task.Account{{Id: 1}}}, ie: catalog}
			service := bindOwnedCatalogTest(exec, catalog)
			err := exec.ReleaseFailedStart(context.Background(), tc.cause)
			if tc.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.wantState, catalog.getState())
			service.mu.Lock()
			defer service.mu.Unlock()
			require.Len(t, service.transitions, tc.wantWrites)
			if tc.wantWrites != 0 {
				require.Equal(t, task.TaskStatus_Running, service.owners[0].Status)
				require.Equal(t, tc.wantStatus, service.transitions[0].Status)
				require.Equal(t, tc.wantRelease, service.transitions[0].ReleaseLease)
			}
		})
	}
}

func TestCDCStartAdmissionUsesDurableState(t *testing.T) {
	for _, tc := range []struct {
		name            string
		state           string
		claimedRestart  bool
		wantRestart     bool
		wantState       string
		wantDaemon      task.TaskStatus
		wantTransitions int
		wantError       bool
	}{
		{name: "running", state: cdc.CDCState_Running, wantState: cdc.CDCState_Running},
		{name: "recover restart", state: cdc.CDCState_Restarting, wantRestart: true, wantState: cdc.CDCState_Restarting},
		{name: "terminal failure", state: cdc.CDCState_Failed, wantState: cdc.CDCState_Failed, wantDaemon: task.TaskStatus_Error, wantTransitions: 1, wantError: true},
		{name: "explicit restart of failed", state: cdc.CDCState_Failed, claimedRestart: true, wantRestart: true, wantState: cdc.CDCState_Restarting, wantDaemon: task.TaskStatus_Running, wantTransitions: 1},
		{name: "explicit restart after publication", state: cdc.CDCState_Running, claimedRestart: true, wantRestart: true, wantState: cdc.CDCState_Restarting, wantDaemon: task.TaskStatus_Running, wantTransitions: 1},
		{name: "paused", state: cdc.CDCState_Paused, wantState: cdc.CDCState_Paused, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			catalog := &cdcCatalogStateExecutor{state: tc.state, currentState: tc.state, targetState: tc.wantState}
			exec := &CDCTaskExecutor{spec: &task.CreateCdcDetails{TaskId: "task", Accounts: []*task.Account{{Id: 1}}}, ie: catalog}
			service := bindOwnedCatalogTest(exec, catalog)
			restart, terminal, err := exec.admitStart(context.Background(), 0, tc.claimedRestart)
			require.Equal(t, tc.name == "terminal failure", terminal)
			if tc.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.wantRestart, restart)
			}
			require.Equal(t, tc.wantState, catalog.getState())
			service.mu.Lock()
			defer service.mu.Unlock()
			require.Len(t, service.transitions, tc.wantTransitions)
			if tc.wantTransitions != 0 {
				require.Equal(t, task.TaskStatus_Running, service.owners[0].Status)
				require.Equal(t, tc.wantDaemon, service.transitions[0].Status)
			}
		})
	}
}

func TestCDCCompleteRestartPreservesNewerRequest(t *testing.T) {
	for _, tc := range []struct {
		name      string
		state     string
		wantReady bool
		wantError bool
	}{
		{name: "completed", state: cdc.CDCState_Running, wantReady: true},
		{name: "new restart", state: cdc.CDCState_Restarting, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			catalog := &cdcCatalogStateExecutor{state: tc.state, currentState: cdc.CDCState_Running, targetState: cdc.CDCState_Running}
			exec := &CDCTaskExecutor{spec: &task.CreateCdcDetails{TaskId: "task", Accounts: []*task.Account{{Id: 1}}}, ie: catalog}
			service := bindOwnedCatalogTest(exec, catalog)
			_, attempt := newCDCStartAttempt(context.Background(), 0)
			require.True(t, exec.installStartAttempt(attempt))
			t.Cleanup(func() { exec.finishStartAttempt(attempt) })
			require.True(t, attempt.completeRestart())
			err := exec.CompleteRestart(context.Background())
			if tc.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.wantReady, attempt.ready.Load())
			require.Equal(t, tc.state, catalog.getState())
			service.mu.Lock()
			defer service.mu.Unlock()
			if tc.wantReady {
				require.Len(t, service.transitions, 1)
				require.Equal(t, task.TaskStatus_RestartRequested, service.owners[0].Status)
				require.Equal(t, task.TaskStatus_Running, service.transitions[0].Status)
				require.False(t, service.transitions[0].StartedAt.IsZero())
			} else {
				require.Empty(t, service.transitions)
			}
		})
	}
}
