// Copyright 2021 - 2023 Matrix Origin
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

package frontend

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
)

func TestDaemonTaskStatementPrivileges(t *testing.T) {
	ses := newTestSession(t, gomock.NewController(t))
	defer ses.Close()
	ctx := context.Background()
	// No task service is installed: denied users must not reach task lookup,
	// regardless of whether an ID exists or the service is ready.
	require.Nil(t, getPu(ses.GetService()).GetTaskService())
	statements := []func(uint64) tree.Statement{
		func(id uint64) tree.Statement { return &tree.PauseDaemonTask{TaskID: id} },
		func(id uint64) tree.Statement { return &tree.ResumeDaemonTask{TaskID: id} },
		func(id uint64) tree.Statement { return &tree.CancelDaemonTask{TaskID: id} },
	}
	roles := []struct {
		account, role string
		allowed       bool
	}{
		{sysAccountName, moAdminRoleName, true},
		{"tenant", accountAdminRoleName, true},
		{sysAccountName, publicRoleName, false},
		{"tenant", publicRoleName, false},
		{sysAccountName, "custom", false},
		{"tenant", "custom", false},
		{sysAccountName, accountAdminRoleName, false},
		{"tenant", moAdminRoleName, false},
	}
	for _, statement := range statements {
		stmt := statement(1)
		t.Run(stmt.GetStatementType(), func(t *testing.T) {
			priv := determinePrivilegeSetOfStatement(stmt)
			require.Equal(t, objectTypeNone, priv.objectType())
			require.Equal(t, privilegeKindSpecial, priv.privilegeKind())
			require.Equal(t, specialTagAdmin, priv.special)
			require.True(t, priv.canExecInRestricted)
			require.False(t, priv.canExecInPasswordExpired)
			for _, role := range roles {
				t.Run(role.account+"/"+role.role, func(t *testing.T) {
					// Keep the same user while changing its active role. Authorization
					// must not infer authority from a username or a previous role.
					ses.SetTenantInfo(&TenantInfo{Tenant: role.account, User: "task_user", DefaultRole: role.role})
					for _, id := range []uint64{0, 1, ^uint64(0)} {
						stmt := statement(id)
						ses.SetPrivilege(determinePrivilegeSetOfStatement(stmt))
						allowed, _, err := authenticateUserCanExecuteStatementWithObjectTypeNone(ctx, ses, stmt)
						require.NoError(t, err)
						require.Equal(t, role.allowed, allowed)
						_, err = authenticateUserCanExecuteStatement(ctx, ses, stmt)
						if role.allowed {
							require.NoError(t, err)
						} else {
							require.EqualError(t, err, "internal error: do not have privilege to execute the statement")
							require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal))
						}
					}
				})
			}
		})
	}
}

func TestDaemonTaskStateTransitions(t *testing.T) {
	const service = "daemon-state-test"
	InitServerLevelVars(service)
	store := taskservice.NewMemTaskStorage()
	ts := taskservice.NewTaskService(runtime.DefaultRuntime(), store)
	t.Cleanup(func() { require.NoError(t, ts.Close()) })
	setPu(service, &config.ParameterUnit{SV: &config.FrontendParameters{}, TaskService: ts})
	ses := &Session{feSessionImpl: feSessionImpl{service: service}}
	ctx := defines.AttachAccountId(context.Background(), 1)
	row := task.DaemonTask{ID: 1, Metadata: task.TaskMetadata{ID: t.Name()}, AccountID: 1,
		Details: &task.Details{AccountID: 1, Account: "tenant"}}
	n, err := store.AddDaemonTask(ctx, row)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	tests := []struct {
		name          string
		run           func() error
		before, after task.TaskStatus
		wantError     string
	}{
		{"pause", func() error { return handlePauseDaemonTask(ctx, ses, &tree.PauseDaemonTask{TaskID: 1}) }, task.TaskStatus_Running, task.TaskStatus_PauseRequested, ""},
		{"pause paused", func() error { return handlePauseDaemonTask(ctx, ses, &tree.PauseDaemonTask{TaskID: 1}) }, task.TaskStatus_Paused, task.TaskStatus_Paused, ""},
		{"pause requested", func() error { return handlePauseDaemonTask(ctx, ses, &tree.PauseDaemonTask{TaskID: 1}) }, task.TaskStatus_PauseRequested, task.TaskStatus_PauseRequested, ""},
		{"pause canceled", func() error { return handlePauseDaemonTask(ctx, ses, &tree.PauseDaemonTask{TaskID: 1}) }, task.TaskStatus_Canceled, task.TaskStatus_Canceled, "now it is Canceled"},
		{"resume", func() error { return handleResumeDaemonTask(ctx, ses, &tree.ResumeDaemonTask{TaskID: 1}) }, task.TaskStatus_Paused, task.TaskStatus_ResumeRequested, ""},
		{"resume running", func() error { return handleResumeDaemonTask(ctx, ses, &tree.ResumeDaemonTask{TaskID: 1}) }, task.TaskStatus_Running, task.TaskStatus_Running, ""},
		{"resume requested", func() error { return handleResumeDaemonTask(ctx, ses, &tree.ResumeDaemonTask{TaskID: 1}) }, task.TaskStatus_ResumeRequested, task.TaskStatus_ResumeRequested, ""},
		{"resume canceled", func() error { return handleResumeDaemonTask(ctx, ses, &tree.ResumeDaemonTask{TaskID: 1}) }, task.TaskStatus_Canceled, task.TaskStatus_Canceled, "now it is Canceled"},
		{"cancel", func() error { return handleCancelDaemonTask(ctx, ses, 1) }, task.TaskStatus_Running, task.TaskStatus_CancelRequested, ""},
		{"cancel canceled", func() error { return handleCancelDaemonTask(ctx, ses, 1) }, task.TaskStatus_Canceled, task.TaskStatus_Canceled, ""},
		{"cancel requested", func() error { return handleCancelDaemonTask(ctx, ses, 1) }, task.TaskStatus_CancelRequested, task.TaskStatus_CancelRequested, ""},
		{"cancel error", func() error { return handleCancelDaemonTask(ctx, ses, 1) }, task.TaskStatus_Error, task.TaskStatus_Error, "task can not be canceled"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			row.TaskStatus = tc.before
			n, err := store.UpdateDaemonTask(ctx, []task.DaemonTask{row})
			require.NoError(t, err)
			require.Equal(t, 1, n)
			err = tc.run()
			if tc.wantError == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.wantError)
			}
			rows, err := store.QueryDaemonTask(ctx, taskservice.WithTaskIDCond(taskservice.EQ, row.ID))
			require.NoError(t, err)
			require.Len(t, rows, 1)
			want := row
			want.TaskStatus = tc.after
			require.Equal(t, want, rows[0])
		})
	}
}

type daemonTaskCountingStorage struct {
	taskservice.TaskStorage
	queries, updates int
}

func (s *daemonTaskCountingStorage) QueryDaemonTask(ctx context.Context, conds ...taskservice.Condition) ([]task.DaemonTask, error) {
	s.queries++
	return s.TaskStorage.QueryDaemonTask(ctx, conds...)
}

func (s *daemonTaskCountingStorage) RequestDaemonTaskStatus(ctx context.Context, observed task.DaemonTask, status task.TaskStatus) (int, error) {
	s.updates++
	return s.TaskStorage.RequestDaemonTaskStatus(ctx, observed, status)
}

func TestDaemonTaskAccountIdentity(t *testing.T) {
	const service = "daemon-identity-test"
	InitServerLevelVars(service)
	store := &daemonTaskCountingStorage{TaskStorage: taskservice.NewMemTaskStorage()}
	ts := taskservice.NewTaskService(runtime.DefaultRuntime(), store)
	t.Cleanup(func() { require.NoError(t, ts.Close()) })
	setPu(service, &config.ParameterUnit{SV: &config.FrontendParameters{}, TaskService: ts})
	ses := &Session{feSessionImpl: feSessionImpl{service: service}}
	rows := []task.DaemonTask{
		{ID: 1, Metadata: task.TaskMetadata{ID: "identity-sys"}, AccountID: 0, Details: &task.Details{Account: "sys"}},
		{ID: 2, Metadata: task.TaskMetadata{ID: "identity-tenant"}, AccountID: 1, Details: &task.Details{AccountID: 1, Account: "tenant"}},
	}
	n, err := store.AddDaemonTask(context.Background(), rows...)
	require.NoError(t, err)
	require.Equal(t, 2, n)
	for _, verb := range []struct {
		name          string
		run           func(context.Context, uint64) error
		before, after task.TaskStatus
	}{
		{"pause", func(ctx context.Context, id uint64) error {
			return handlePauseDaemonTask(ctx, ses, &tree.PauseDaemonTask{TaskID: id})
		}, task.TaskStatus_Running, task.TaskStatus_PauseRequested},
		{"resume", func(ctx context.Context, id uint64) error {
			return handleResumeDaemonTask(ctx, ses, &tree.ResumeDaemonTask{TaskID: id})
		}, task.TaskStatus_Paused, task.TaskStatus_ResumeRequested},
		{"cancel", func(ctx context.Context, id uint64) error {
			return handleCancelDaemonTask(ctx, ses, id)
		}, task.TaskStatus_Running, task.TaskStatus_CancelRequested},
	} {
		t.Run(verb.name, func(t *testing.T) {
			store.queries, store.updates = 0, 0
			ses.accountId = 0
			require.EqualError(t, verb.run(context.Background(), 1), "internal error: no account id in context")
			require.Zero(t, store.queries, "missing identity must fail before task lookup")
			require.Zero(t, store.updates)
			for _, account := range []struct {
				name             string
				id, cachedID     uint32
				ownID, foreignID uint64
			}{
				{"cold tenant", 1, 0, 2, 1},
				{"explicit sys", 0, 9, 1, 2},
			} {
				t.Run(account.name, func(t *testing.T) {
					ctx := defines.AttachAccountId(context.Background(), account.id)
					ses.accountId = account.cachedID
					for i := range rows {
						rows[i].TaskStatus = verb.before
					}
					n, err := store.TaskStorage.UpdateDaemonTask(ctx, rows)
					require.NoError(t, err)
					require.Equal(t, 2, n)
					store.queries, store.updates = 0, 0
					err = verb.run(ctx, account.foreignID)
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrTaskNotFound), "%v", err)
					require.Equal(t, 1, store.queries)
					require.Zero(t, store.updates)
					require.NoError(t, verb.run(ctx, account.ownID))
					require.Equal(t, 2, store.queries)
					require.Equal(t, 1, store.updates)
					actual, err := store.TaskStorage.QueryDaemonTask(ctx)
					require.NoError(t, err)
					want := append([]task.DaemonTask(nil), rows...)
					want[account.ownID-1].TaskStatus = verb.after
					require.Equal(t, want, actual, "only the context account's row may change")
				})
			}
		})
	}
}

type controlRaceService struct {
	taskservice.TaskService
	beforeRequest func() error
}

type controlRereadProbe struct {
	taskservice.TaskService
	matched      int
	requestErr   error
	rows         []task.DaemonTask
	readErr      error
	requestCalls int
	readCalls    int
}

func (s *controlRereadProbe) RequestDaemonTaskStatus(context.Context, task.DaemonTask, task.TaskStatus) (int, error) {
	s.requestCalls++
	return s.matched, s.requestErr
}

func (s *controlRereadProbe) QueryDaemonTask(context.Context, ...taskservice.Condition) ([]task.DaemonTask, error) {
	s.readCalls++
	return s.rows, s.readErr
}

func TestDaemonControlRereadAfterConflict(t *testing.T) {
	observed := task.DaemonTask{ID: 42, AccountID: 17, Metadata: task.TaskMetadata{ID: "original"}}
	for _, tc := range []struct {
		name               string
		requested, current task.TaskStatus
	}{
		{"pause requested", task.TaskStatus_PauseRequested, task.TaskStatus_PauseRequested},
		{"pause completed", task.TaskStatus_PauseRequested, task.TaskStatus_Paused},
		{"cancel requested", task.TaskStatus_CancelRequested, task.TaskStatus_CancelRequested},
		{"cancel completed", task.TaskStatus_CancelRequested, task.TaskStatus_Canceled},
		{"resume requested", task.TaskStatus_ResumeRequested, task.TaskStatus_ResumeRequested},
		{"resume completed", task.TaskStatus_ResumeRequested, task.TaskStatus_Running},
	} {
		t.Run(tc.name, func(t *testing.T) {
			current := observed
			current.TaskStatus = tc.current
			current.TaskRunner = "new-owner"
			probe := &controlRereadProbe{rows: []task.DaemonTask{current}}
			require.NoError(t, requestDaemonTaskStatus(context.Background(), probe, observed, tc.requested))
			require.Equal(t, 1, probe.requestCalls)
			require.Equal(t, 1, probe.readCalls)
		})
	}
	writeErr := errors.New("write failed")
	readErr := errors.New("read failed")
	for _, tc := range []struct {
		name      string
		probe     controlRereadProbe
		wantCode  uint16
		wantErr   error
		wantReads int
	}{
		{name: "matched", probe: controlRereadProbe{matched: 1}},
		{name: "write error", probe: controlRereadProbe{requestErr: writeErr}, wantErr: writeErr},
		{name: "invalid count", probe: controlRereadProbe{matched: 2}, wantCode: moerr.ErrInternal},
		{name: "read error", probe: controlRereadProbe{readErr: readErr}, wantErr: readErr, wantReads: 1},
		{name: "deleted", wantCode: moerr.ErrTaskNotFound, wantReads: 1},
		{name: "other state", probe: controlRereadProbe{rows: []task.DaemonTask{{ID: 42, Metadata: task.TaskMetadata{ID: "original"}, TaskStatus: task.TaskStatus_CancelRequested}}}, wantCode: moerr.ErrTxnWriteConflict, wantReads: 1},
		{name: "replacement", probe: controlRereadProbe{rows: []task.DaemonTask{{ID: 42, Metadata: task.TaskMetadata{ID: "replacement"}, TaskStatus: task.TaskStatus_Paused}}}, wantCode: moerr.ErrTxnWriteConflict, wantReads: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := requestDaemonTaskStatus(context.Background(), &tc.probe, observed, task.TaskStatus_PauseRequested)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else if tc.wantCode != 0 {
				require.True(t, moerr.IsMoErrCode(err, tc.wantCode), "%v", err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, 1, tc.probe.requestCalls)
			require.Equal(t, tc.wantReads, tc.probe.readCalls)
		})
	}
}

func (s *controlRaceService) RequestDaemonTaskStatus(ctx context.Context, observed task.DaemonTask, status task.TaskStatus) (int, error) {
	if s.beforeRequest != nil {
		if err := s.beforeRequest(); err != nil {
			return 0, err
		}
	}
	return s.TaskService.RequestDaemonTaskStatus(ctx, observed, status)
}

func TestDaemonControlConcurrentChange(t *testing.T) {
	for _, tc := range []struct {
		name, operation, change string
		initial                 task.TaskStatus
		wantStatus              task.TaskStatus
		wantConflict            bool
	}{
		{"pause preserves heartbeat", "pause", "heartbeat", task.TaskStatus_Running, task.TaskStatus_PauseRequested, false},
		{"cancel rejects new owner", "cancel", "owner", task.TaskStatus_Running, task.TaskStatus_Running, true},
		{"resume already completed concurrently", "resume", "status", task.TaskStatus_Paused, task.TaskStatus_Running, false},
		{"resume rejects concurrent cancellation", "resume", "cancel-request", task.TaskStatus_Paused, task.TaskStatus_CancelRequested, true},
		{"pause rejects another account", "pause", "wrong-account", task.TaskStatus_Running, task.TaskStatus_Running, false},
		{"pause reread stays account scoped", "pause", "account", task.TaskStatus_Running, task.TaskStatus_Running, false},
		{"pause already paused", "pause", "idempotent", task.TaskStatus_Paused, task.TaskStatus_Paused, false},
		{"cancel already canceled", "cancel", "idempotent", task.TaskStatus_Canceled, task.TaskStatus_Canceled, false},
		{"resume already running", "resume", "idempotent", task.TaskStatus_Running, task.TaskStatus_Running, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := taskservice.NewMemTaskStorage()
			ts := taskservice.NewTaskService(runtime.DefaultRuntime(), store)
			t.Cleanup(func() { require.NoError(t, ts.Close()) })
			observed := task.DaemonTask{ID: 42, AccountID: 17, TaskRunner: "owner", TaskStatus: tc.initial,
				Metadata: task.TaskMetadata{ID: t.Name()}, LastRun: time.Date(2026, 8, 1, 12, 0, 0, 123456000, time.UTC)}
			n, err := store.AddDaemonTask(ctx, observed)
			require.NoError(t, err)
			require.Equal(t, 1, n)
			svc := &controlRaceService{TaskService: ts}
			requests := 0
			svc.beforeRequest = func() error {
				requests++
				current := observed
				switch tc.change {
				case "heartbeat":
					current.LastHeartbeat = observed.LastRun.Add(time.Second)
				case "owner":
					current.TaskRunner = "new-owner"
					current.LastRun = observed.LastRun.Add(time.Microsecond)
				case "status":
					current.TaskStatus = task.TaskStatus_Running
				case "cancel-request":
					current.TaskStatus = task.TaskStatus_CancelRequested
				case "account":
					current.AccountID++
				}
				_, err := store.UpdateDaemonTask(ctx, []task.DaemonTask{current})
				return err
			}
			name := t.Name()
			InitServerLevelVars(name)
			t.Cleanup(func() { serverVarsMap.Delete(name) })
			pu := &config.ParameterUnit{}
			pu.SetTaskService(svc)
			setPu(name, pu)
			accountID := observed.AccountID
			if tc.change == "wrong-account" {
				accountID++
			}
			ctx = defines.AttachAccountId(ctx, accountID)
			ses := &Session{feSessionImpl: feSessionImpl{service: name, accountId: accountID}}
			switch tc.operation {
			case "pause":
				err = handlePauseDaemonTask(ctx, ses, &tree.PauseDaemonTask{TaskID: observed.ID})
			case "cancel":
				err = handleCancelDaemonTask(ctx, ses, observed.ID)
			case "resume":
				err = handleResumeDaemonTask(ctx, ses, &tree.ResumeDaemonTask{TaskID: observed.ID})
			}
			if tc.change == "wrong-account" {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrTaskNotFound), "%v", err)
				require.Zero(t, requests)
			} else if tc.change == "account" {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrTaskNotFound), "%v", err)
			} else if tc.wantConflict {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnWriteConflict), "%v", err)
			} else {
				require.NoError(t, err)
			}
			if tc.change == "idempotent" {
				require.Zero(t, requests)
			}
			rows, err := store.QueryDaemonTask(ctx, taskservice.WithTaskIDCond(taskservice.EQ, observed.ID))
			require.NoError(t, err)
			require.Len(t, rows, 1)
			require.Equal(t, tc.wantStatus, rows[0].TaskStatus)
			if tc.change == "heartbeat" {
				require.Equal(t, observed.LastRun.Add(time.Second), rows[0].LastHeartbeat)
			}
			if tc.change == "owner" {
				require.Equal(t, "new-owner", rows[0].TaskRunner)
				require.Equal(t, observed.LastRun.Add(time.Microsecond), rows[0].LastRun)
			}
			if tc.change == "account" {
				require.Equal(t, observed.AccountID+1, rows[0].AccountID)
			}
		})
	}
}

// TestHandlePauseDaemonTask_TaskServiceNil tests the error case in handlePauseDaemonTask function
// when the task service is nil
func TestHandlePauseDaemonTask_TaskServiceNil(t *testing.T) {
	// Initialize server level vars for the test service
	InitServerLevelVars("test-service")

	// Create a parameter unit with nil task service
	pu := &config.ParameterUnit{
		SV: &config.FrontendParameters{},
	}
	// Don't set the task service, so it will be nil

	// Set the parameter unit for the service
	setPu("test-service", pu)

	// Create a session that implements FeSession
	session := &Session{
		feSessionImpl: feSessionImpl{
			service:   "test-service",
			accountId: 1,
		},
	}

	// Create a PauseDaemonTask statement
	pauseStmt := &tree.PauseDaemonTask{
		TaskID: 123,
	}

	// Test the handlePauseDaemonTask function
	err := handlePauseDaemonTask(context.Background(), session, pauseStmt)

	// Verify that the expected error is returned
	require.Error(t, err)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal))
	assert.Contains(t, err.Error(), "task service not ready yet, please try again later.")
}

// TestHandleCancelDaemonTask_TaskServiceNil tests the error case in handleCancelDaemonTask function
// when the task service is nil
func TestHandleCancelDaemonTask_TaskServiceNil(t *testing.T) {
	// Initialize server level vars for the test service
	InitServerLevelVars("test-service")

	// Create a parameter unit with nil task service
	pu := &config.ParameterUnit{
		SV: &config.FrontendParameters{},
	}
	// Don't set the task service, so it will be nil

	// Set the parameter unit for the service
	setPu("test-service", pu)

	// Create a session that implements FeSession
	session := &Session{
		feSessionImpl: feSessionImpl{
			service:   "test-service",
			accountId: 1,
		},
	}

	// Test the handleCancelDaemonTask function
	err := handleCancelDaemonTask(context.Background(), session, 123)

	// Verify that the expected error is returned
	require.Error(t, err)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal))
	assert.Contains(t, err.Error(), "task service not ready yet, please try again later.")
}

// TestHandleResumeDaemonTask_TaskServiceNil tests the error case in handleResumeDaemonTask function
// when the task service is nil
func TestHandleResumeDaemonTask_TaskServiceNil(t *testing.T) {
	// Initialize server level vars for the test service
	InitServerLevelVars("test-service")

	// Create a parameter unit with nil task service
	pu := &config.ParameterUnit{
		SV: &config.FrontendParameters{},
	}
	// Don't set the task service, so it will be nil

	// Set the parameter unit for the service
	setPu("test-service", pu)

	// Create a session that implements FeSession
	session := &Session{
		feSessionImpl: feSessionImpl{
			service:   "test-service",
			accountId: 1,
		},
	}

	// Create a ResumeDaemonTask statement
	resumeStmt := &tree.ResumeDaemonTask{
		TaskID: 123,
	}

	// Test the handleResumeDaemonTask function
	err := handleResumeDaemonTask(context.Background(), session, resumeStmt)

	// Verify that the expected error is returned
	require.Error(t, err)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal))
	assert.Contains(t, err.Error(), "task service not ready yet, please try again later.")
}
