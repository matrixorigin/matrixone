// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
	"strings"
	"testing"
	"time"

	"github.com/prashantv/gostub"

	"github.com/stretchr/testify/require"
)

type authenticationResultExec struct {
	backgroundExecTest
	results   []interface{}
	err       error
	afterExec func()
	calls     int
}

func (b *authenticationResultExec) Exec(context.Context, string) error {
	b.calls++
	if b.afterExec != nil {
		b.afterExec()
	}
	return b.err
}
func (b *authenticationResultExec) GetExecResultSet() []interface{} { return b.results }

func authenticationRows(rows ...[]interface{}) *MysqlResultSet {
	result := &MysqlResultSet{Data: rows}
	if len(rows) > 0 {
		for range rows[0] {
			result.AddColumn(&MysqlColumn{})
		}
	}
	return result
}

type authenticationScriptExec struct {
	backgroundExecTest
	missingLock        bool
	missingExpiry      bool
	lockStatus         string
	attemptState       *authenticationAttemptState
	afterAttemptUpdate func()
}

// Model committed and pending rows separately: observing an UPDATE alone does
// not prove that the login failure survives transaction completion.
type authenticationAttemptState struct {
	attempts, pendingAttempts uint64
	status, pendingStatus     string
	lockTime, pendingLockTime string
}

func (b *authenticationScriptExec) Exec(ctx context.Context, sql string) error {
	if err := b.backgroundExecTest.Exec(ctx, sql); err != nil {
		return err
	}
	s := b.attemptState
	if s == nil {
		return nil
	}
	switch sql {
	case "begin;":
		s.pendingAttempts, s.pendingStatus, s.pendingLockTime = s.attempts, s.status, s.lockTime
	case "commit;":
		s.attempts, s.status, s.lockTime = s.pendingAttempts, s.pendingStatus, s.pendingLockTime
	case "rollback;":
		s.pendingAttempts, s.pendingStatus, s.pendingLockTime = s.attempts, s.status, s.lockTime
	case getSqlForUpdateLoginAttemptsOfUser("test_user"):
		s.pendingAttempts++
		if b.afterAttemptUpdate != nil {
			b.afterAttemptUpdate()
		}
	case getSqlForUpdateStatusLockOfUser(userStatusLock, "test_user"):
		s.pendingAttempts++
		s.pendingStatus = userStatusLock
		s.pendingLockTime = time.Now().UTC().Format("2006-01-02 15:04:05")
	case getSqlForUpdateLockTimeOfUser("test_user"):
		s.pendingLockTime = time.Now().UTC().Format("2006-01-02 15:04:05")
	}
	return nil
}

func (b *authenticationScriptExec) GetExecResultSet() []interface{} {
	var result *MysqlResultSet
	switch {
	case strings.Contains(b.currentSql, "from mo_catalog.mo_account"):
		result = authenticationRows([]interface{}{uint64(21), "auth_fixture", "open", uint64(1), "", "4.1.6"})
	case strings.Contains(b.currentSql, "authentication_string"):
		result = authenticationRows([]interface{}{int64(7), HashPassWord("password"), int64(1)})
	case strings.Contains(b.currentSql, "select role_name"):
		result = authenticationRows([]interface{}{"public"})
	case strings.Contains(b.currentSql, "status, login_attempts, lock_time"):
		if b.missingLock {
			return nil
		}
		if s := b.attemptState; s != nil {
			result = authenticationRows([]interface{}{s.pendingStatus, s.pendingAttempts, s.pendingLockTime})
		} else {
			result = authenticationRows([]interface{}{b.lockStatus, uint64(0), ""})
		}
	case strings.Contains(b.currentSql, "password_last_changed"):
		if b.missingExpiry {
			return nil
		}
		result = authenticationRows([]interface{}{"2026-11-12 00:00:00"})
	default:
		result = authenticationRows([]interface{}{"default_password_lifetime", "30"})
		if b.attemptState != nil {
			result.AddRow([]interface{}{ConnectionControlFailedConnectionsThreshold, "2"})
			result.AddRow([]interface{}{ConnectionControlMaxConnectionDelay, "3600000"})
		}
	}
	return []interface{}{result}
}

func TestAuthenticateUserRequiredResults(t *testing.T) {
	for _, name := range []string{"missing-lock", "missing-expiry", "locked-user", "wrong-password", "commit-failure", "success"} {
		t.Run(name, func(t *testing.T) {
			server, conn, ses, proto := newTestServerWithRoutine(t, 0)
			rt := NewRoutine(context.Background(), proto, getPu("").SV)
			rt.setSession(ses)
			ses.setRoutine(rt)
			ses.setRoutineManager(server.rm)
			server.rm.setRoutine(conn, 0, rt)
			t.Cleanup(func() { server.rm.Closed(conn) })
			vars := gostub.Stub(&GSysVarsMgr, &GlobalSysVarsMgr{accountsGlobalSysVarsMap: make(map[uint32]*SystemVariables)})
			t.Cleanup(vars.Reset)
			bh := &authenticationScriptExec{missingLock: name == "missing-lock", missingExpiry: name == "missing-expiry", lockStatus: "unlock"}
			bh.init()
			if name == "locked-user" {
				bh.lockStatus = userStatusLockForever
			}
			commitErr := errors.New("injected authentication commit failure")
			if name == "commit-failure" {
				bh.sql2err["commit;"] = commitErr
			}
			stub := gostub.StubFunc(&NewBackgroundExec, bh)
			t.Cleanup(stub.Reset)
			checks := 0
			_, err := ses.AuthenticateUser(context.Background(), "auth_fixture:test_user", "", nil, nil, func(_, _, _ []byte) bool { checks++; return name != "wrong-password" })
			if name == "success" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			if name == "commit-failure" {
				require.ErrorIs(t, err, commitErr)
			}
			if name == "missing-lock" || name == "locked-user" {
				require.Zero(t, checks)
			} else {
				require.Equal(t, 1, checks)
			}
			if name == "success" || name == "commit-failure" || name == "wrong-password" {
				require.Contains(t, bh.executedSQLs, "commit;")
			} else {
				require.Contains(t, bh.executedSQLs, "rollback;")
			}
		})
	}
}

func TestAuthenticationFailureUpdatesCommit(t *testing.T) {
	for _, mode := range []string{"lock-threshold", "update-error", "commit-error", "lock-time-refresh", "cancel-after-update"} {
		t.Run(mode, func(t *testing.T) {
			server, conn, ses, proto := newTestServerWithRoutine(t, 0)
			rt := NewRoutine(context.Background(), proto, getPu("").SV)
			rt.setSession(ses)
			ses.setRoutine(rt)
			ses.setRoutineManager(server.rm)
			server.rm.setRoutine(conn, 0, rt)
			t.Cleanup(func() { server.rm.Closed(conn) })
			vars := gostub.Stub(&GSysVarsMgr, &GlobalSysVarsMgr{accountsGlobalSysVarsMap: make(map[uint32]*SystemVariables)})
			t.Cleanup(vars.Reset)
			state := &authenticationAttemptState{status: userStatusUnlock}
			bh := &authenticationScriptExec{attemptState: state}
			bh.init()
			stub := gostub.StubFunc(&NewBackgroundExec, bh)
			t.Cleanup(stub.Reset)
			failure := errors.New("injected login persistence failure")
			if mode == "update-error" {
				bh.sql2err[getSqlForUpdateLoginAttemptsOfUser("test_user")] = failure
			}
			if mode == "commit-error" {
				bh.sql2err["commit;"] = failure
			}
			if mode == "lock-time-refresh" {
				state.status, state.lockTime = userStatusLock, "2000-01-01 00:00:00"
			}
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			if mode == "cancel-after-update" {
				bh.afterAttemptUpdate = cancel
			}
			checkCount := 0
			login := func() error {
				_, err := ses.AuthenticateUser(ctx, "auth_fixture:test_user", "", nil, nil, func(_, _, _ []byte) bool { checkCount++; return false })
				return err
			}
			err := login()
			require.Error(t, err)
			if mode == "cancel-after-update" {
				require.ErrorIs(t, err, context.Canceled)
				require.Zero(t, state.attempts)
				require.Contains(t, bh.executedSQLs, "rollback;")
				require.NotContains(t, bh.executedSQLs, "commit;")
				return
			}
			if mode == "update-error" || mode == "commit-error" {
				require.ErrorIs(t, err, failure)
				require.Zero(t, state.attempts)
				require.Equal(t, userStatusUnlock, state.status)
				require.Contains(t, bh.executedSQLs, "rollback;")
				return
			}
			require.ErrorContains(t, err, "check password failed")
			require.Contains(t, bh.executedSQLs, "commit;")
			if mode == "lock-time-refresh" {
				require.NotEqual(t, "2000-01-01 00:00:00", state.lockTime)
				require.Equal(t, userStatusLock, state.status)
				return
			}
			require.Equal(t, uint64(1), state.attempts, "first rejected password must persist its counter")
			require.ErrorContains(t, login(), "check password failed")
			require.Equal(t, uint64(2), state.attempts)
			require.Equal(t, userStatusLock, state.status)
			require.ErrorContains(t, login(), "user is locked")
			require.Equal(t, 2, checkCount, "locked login must not reach password verification")
			require.Equal(t, uint64(2), state.attempts)
		})
	}
}

func TestAuthenticationResultRequiresData(t *testing.T) {
	row := &MysqlResultSet{Data: [][]interface{}{{"unlocked", uint64(0), ""}}}
	for range 3 {
		row.AddColumn(&MysqlColumn{})
	}
	sentinel := errors.New("authentication execution failed")
	for _, tc := range []struct {
		name                       string
		results                    []interface{}
		err                        error
		cancelBefore, cancelDuring bool
	}{
		{name: "missing-result"},
		{name: "nil-result", results: []interface{}{nil}},
		{name: "empty-rows", results: []interface{}{&MysqlResultSet{}}},
		{name: "execution-error", err: sentinel},
		{name: "cancel-before", cancelBefore: true},
		{name: "cancel-during", results: []interface{}{row}, cancelDuring: true},
		{name: "valid", results: []interface{}{row}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			bh := &authenticationResultExec{results: tc.results, err: tc.err}
			if tc.cancelBefore {
				cancel()
			}
			if tc.cancelDuring {
				bh.afterExec = cancel
			}
			got, err := authenticationResult(ctx, bh, "select required authentication data")
			switch {
			case tc.name == "valid":
				require.NoError(t, err)
				require.Same(t, row, got)
				status, err := got.GetString(ctx, 0, 0)
				require.NoError(t, err)
				require.Equal(t, "unlocked", status)
			case tc.cancelBefore || tc.cancelDuring:
				require.ErrorIs(t, err, context.Canceled)
				require.Nil(t, got)
				if tc.cancelBefore {
					require.Zero(t, bh.calls)
				}
			case tc.err != nil:
				require.ErrorIs(t, err, sentinel)
				require.Nil(t, got)
			default:
				require.Error(t, err)
				require.Nil(t, got)
			}
		})
	}
}
