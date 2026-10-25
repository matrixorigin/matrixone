// Copyright 2021 Matrix Origin
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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/config"
	mo_config "github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
)

// TestConvertMiscOptToExpiredTime covers the translation of the PASSWORD
// EXPIRE misc options of CREATE/ALTER USER into the raw mo_user.expired_time
// SQL value.
func TestConvertMiscOptToExpiredTime(t *testing.T) {
	// PASSWORD EXPIRE: expires right now.
	v, ok := convertMiscOptToExpiredTime(&tree.UserMiscOptionPasswordExpireNone{})
	require.True(t, ok)
	require.True(t, strings.HasPrefix(v, `"`) && strings.HasSuffix(v, `"`), "expect quoted timestamp, got %q", v)
	ts := strings.Trim(v, `"`)
	parsed, err := time.ParseInLocation("2006-01-02 15:04:05", ts, time.UTC)
	require.NoError(t, err)
	require.False(t, parsed.After(time.Now().UTC()), "PASSWORD EXPIRE must expire immediately")

	// PASSWORD EXPIRE INTERVAL n DAY: expires n days later.
	v, ok = convertMiscOptToExpiredTime(&tree.UserMiscOptionPasswordExpireInterval{Value: 3})
	require.True(t, ok)
	ts = strings.Trim(v, `"`)
	parsed, err = time.ParseInLocation("2006-01-02 15:04:05", ts, time.UTC)
	require.NoError(t, err)
	require.True(t, parsed.After(time.Now().UTC().AddDate(0, 0, 2)), "INTERVAL 3 DAY must not expire within 2 days")
	require.True(t, parsed.Before(time.Now().UTC().AddDate(0, 0, 3).Add(time.Hour)), "INTERVAL 3 DAY must expire in about 3 days")

	// PASSWORD EXPIRE NEVER: far-future sentinel.
	v, ok = convertMiscOptToExpiredTime(&tree.UserMiscOptionPasswordExpireNever{})
	require.True(t, ok)
	require.Equal(t, `"`+passwordExpireNeverTime+`"`, v)

	// PASSWORD EXPIRE DEFAULT: NULL, follow the global default lifetime.
	v, ok = convertMiscOptToExpiredTime(&tree.UserMiscOptionPasswordExpireDefault{})
	require.True(t, ok)
	require.Equal(t, passwordExpireDefaultTime, v)
	require.Equal(t, "NULL", v)

	// Non expire options are not converted.
	_, ok = convertMiscOptToExpiredTime(&tree.UserMiscOptionAccountLock{})
	require.False(t, ok)
	_, ok = convertMiscOptToExpiredTime(&tree.UserMiscOptionAccountUnlock{})
	require.False(t, ok)
	_, ok = convertMiscOptToExpiredTime(&tree.UserMiscOptionPasswordHistoryDefault{})
	require.False(t, ok)
	_, ok = convertMiscOptToExpiredTime(nil)
	require.False(t, ok)
}

// TestCheckPasswordExpiredAt covers the login-time evaluation of the explicit
// mo_user.expired_time instant, including the PASSWORD EXPIRE NEVER sentinel.
func TestCheckPasswordExpiredAt(t *testing.T) {
	// An instant in the past: the password has expired.
	expired, err := checkPasswordExpiredAt("2000-01-01 00:00:00")
	require.NoError(t, err)
	require.True(t, expired)

	// An instant in the future: not expired yet.
	expired, err = checkPasswordExpiredAt(time.Now().UTC().AddDate(0, 0, 1).Format("2006-01-02 15:04:05"))
	require.NoError(t, err)
	require.False(t, expired)

	// The PASSWORD EXPIRE NEVER sentinel never elapses.
	expired, err = checkPasswordExpiredAt(passwordExpireNeverTime)
	require.NoError(t, err)
	require.False(t, expired)

	// Garbage values are rejected instead of being read as "not expired".
	_, err = checkPasswordExpiredAt("not-a-timestamp")
	require.Error(t, err)
}

// TestPasswordExpiredErrorMysqlCode: the sandbox error must surface to the
// client as MySQL ER_MUST_CHANGE_PASSWORD_LOGIN (1862).
func TestPasswordExpiredErrorMysqlCode(t *testing.T) {
	err := moerr.NewPasswordExpired(context.TODO())
	require.Equal(t, moerr.ErrPasswordExpired, err.ErrorCode())
	require.Equal(t, moerr.ER_MUST_CHANGE_PASSWORD_LOGIN, err.MySQLCode())
	require.Equal(t, uint16(1862), err.MySQLCode())
	require.Equal(t,
		"Your password has expired. To log in you must change it using a client that supports expired passwords.",
		err.Error())

	code, state, msg := RewriteError(err, "u1")
	require.Equal(t, uint16(1862), code)
	require.Equal(t, "HY000", state)
	require.Equal(t, err.Error(), msg)
}

// TestDeterminePrivilegeSetOfStatementPasswordExpiredSandbox: SET PASSWORD
// and ALTER USER must be executable in the expired-password sandbox (they are
// the ways out of it); ordinary statements and SET ROLE must not.
func TestDeterminePrivilegeSetOfStatementPasswordExpiredSandbox(t *testing.T) {
	require.True(t, determinePrivilegeSetOfStatement(&tree.SetPassword{}).canExecInPasswordExpired)
	require.True(t, determinePrivilegeSetOfStatement(&tree.AlterUser{}).canExecInPasswordExpired)
	require.False(t, determinePrivilegeSetOfStatement(&tree.SetRole{}).canExecInPasswordExpired)
	require.False(t, determinePrivilegeSetOfStatement(&tree.SetVar{}).canExecInPasswordExpired)

	selectNoTable := &tree.Select{}
	require.False(t, determinePrivilegeSetOfStatement(selectNoTable).canExecInPasswordExpired)
}

// TestDoAlterUserPasswordExpire covers wiring the PASSWORD EXPIRE misc option
// of ALTER USER down to the mo_user.expired_time update.
func TestDoAlterUserPasswordExpire(t *testing.T) {
	newAlterUser := func(miscOpt tree.UserMiscOption) *alterUser {
		return &alterUser{
			IfExists: false,
			Users: []*user{{
				Username:  "u1",
				Hostname:  "%",
				AuthExist: false,
			}},
			MiscOpt: miscOpt,
		}
	}

	newSessAndBg := func(t *testing.T) (*Session, *backgroundExecTest, *gomock.Controller, context.Context) {
		ctrl := gomock.NewController(t)

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		t.Cleanup(bhStub.Reset)

		ses := newSes(nil, ctrl)

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()
		pu.SV.KillRountinesInterval = 0
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		rm, _ := NewRoutineManager(ctx, "")
		ses.rm = rm

		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, err := getSqlForPasswordOfUser(context.TODO(), "u1")
		require.NoError(t, err)
		bh.sql2result[sql] = newMrsForPasswordOfUser([][]interface{}{{0, "111", 0}})

		sql, err = getSqlForCheckUserHasRole(context.TODO(), rootName, moAdminRoleID)
		require.NoError(t, err)
		bh.sql2result[sql] = newMrsForSqlForCheckUserHasRole([][]interface{}{{0, 0}})

		sql, err = getSqlForCheckUserHasRole(context.TODO(), "u1", moAdminRoleID)
		require.NoError(t, err)
		bh.sql2result[sql] = newMrsForSqlForCheckUserHasRole(nil)
		return ses, bh, ctrl, ctx
	}

	executedUpdateOfUser := func(bh *backgroundExecTest) []string {
		var updates []string
		for _, s := range bh.executedSQLs {
			if strings.HasPrefix(s, "update mo_catalog.mo_user") {
				updates = append(updates, s)
			}
		}
		return updates
	}

	miscOpts := []tree.UserMiscOption{
		&tree.UserMiscOptionPasswordExpireNone{},
		&tree.UserMiscOptionPasswordExpireInterval{Value: 7},
		&tree.UserMiscOptionPasswordExpireNever{},
		&tree.UserMiscOptionPasswordExpireDefault{},
	}
	for _, miscOpt := range miscOpts {
		name := strings.TrimPrefix(fmt.Sprintf("%T", miscOpt), "*tree.UserMiscOption")
		t.Run(name, func(t *testing.T) {
			ses, bh, ctrl, ctx := newSessAndBg(t)
			defer ctrl.Finish()

			err := doAlterUser(ctx, ses, newAlterUser(miscOpt))
			require.NoError(t, err)

			expireValue, ok := convertMiscOptToExpiredTime(miscOpt)
			require.True(t, ok)
			want := getSqlForUpdateExpiredTimeOfUser(expireValue, "u1")
			require.Contains(t, bh.executedSQLs, want)
			require.Contains(t, executedUpdateOfUser(bh), want)
		})
	}

	t.Run("reject identified with password expire", func(t *testing.T) {
		ses, _, ctrl, ctx := newSessAndBg(t)
		defer ctrl.Finish()

		au := newAlterUser(&tree.UserMiscOptionPasswordExpireNone{})
		au.Users[0].AuthExist = true
		au.Users[0].IdentTyp = tree.AccountIdentifiedByPassword
		au.Users[0].IdentStr = "123456"
		err := doAlterUser(ctx, ses, au)
		require.ErrorContains(t, err, "not support identified with password expire operation")
	})

	t.Run("still reject unsupported misc option", func(t *testing.T) {
		ses, _, ctrl, ctx := newSessAndBg(t)
		defer ctrl.Finish()

		err := doAlterUser(ctx, ses, newAlterUser(&tree.UserMiscOptionPasswordHistoryDefault{}))
		require.ErrorContains(t, err, "not support operation: password history default")
	})

	t.Run("account unlock still resets", func(t *testing.T) {
		ses, bh, ctrl, ctx := newSessAndBg(t)
		defer ctrl.Finish()

		err := doAlterUser(ctx, ses, newAlterUser(&tree.UserMiscOptionAccountUnlock{}))
		require.NoError(t, err)
		require.Contains(t, bh.executedSQLs, getSqlForUpdateUnlcokStatusOfUser(userStatusUnlock, "u1"))
	})

	t.Run("account lock still forbids", func(t *testing.T) {
		ses, bh, ctrl, ctx := newSessAndBg(t)
		defer ctrl.Finish()

		err := doAlterUser(ctx, ses, newAlterUser(&tree.UserMiscOptionAccountLock{}))
		require.NoError(t, err)
		require.Contains(t, bh.executedSQLs, getSqlForUpdateStatusLockOfUserForever(userStatusLockForever, "u1"))
	})

	newIdentifiedUser := func(userName string) *alterUser {
		return &alterUser{
			IfExists: false,
			Users: []*user{{
				Username:  userName,
				Hostname:  "%",
				AuthExist: true,
				IdentTyp:  tree.AccountIdentifiedByPassword,
				IdentStr:  "NewPass567",
			}},
		}
	}

	t.Run("self password change exits the expired sandbox", func(t *testing.T) {
		ses, bh, ctrl, ctx := newSessAndBg(t)
		defer ctrl.Finish()

		// the session user is root; root changes its own password while the
		// session is in the expired-password sandbox.
		ses.rt = &Routine{}
		ses.getRoutine().setExpired(true)

		sql, err := getSqlForPasswordOfUser(context.TODO(), rootName)
		require.NoError(t, err)
		bh.sql2result[sql] = newMrsForPasswordOfUser([][]interface{}{{0, "111", 0}})

		err = doAlterUser(ctx, ses, newIdentifiedUser(rootName))
		require.NoError(t, err)
		require.False(t, ses.getRoutine().isExpired(), "own password change must release the sandbox")

		sql, err = getSqlForUpdatePasswordOfUser(context.TODO(), HashPassWord("NewPass567"), rootName)
		require.NoError(t, err)
		require.Contains(t, bh.executedSQLs, sql)
	})

	t.Run("other user password change keeps the sandbox", func(t *testing.T) {
		ses, _, ctrl, ctx := newSessAndBg(t)
		defer ctrl.Finish()

		ses.rt = &Routine{}
		ses.getRoutine().setExpired(true)

		err := doAlterUser(ctx, ses, newIdentifiedUser("u1"))
		require.NoError(t, err)
		require.True(t, ses.getRoutine().isExpired(), "changing another user's password must not release the sandbox")
	})
}

// TestInitUserPasswordExpireAndLock covers wiring the misc options of CREATE
// USER into the mo_user insert: PASSWORD EXPIRE lands in expired_time and
// ACCOUNT LOCK lands in status as the permanent forbid lock.
func TestInitUserPasswordExpireAndLock(t *testing.T) {
	newSessAndBg := func(t *testing.T) (*Session, *backgroundExecTest, *gomock.Controller, context.Context) {
		ctrl := gomock.NewController(t)

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		t.Cleanup(bhStub.Reset)

		ses := newSes(nil, ctrl)

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()
		pu.SV.KillRountinesInterval = 0
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		rm, _ := NewRoutineManager(ctx, "")
		ses.rm = rm

		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil
		return ses, bh, ctrl, ctx
	}

	stubFreshUser := func(t *testing.T, bh *backgroundExecTest, userName string) {
		// The user-dedup and role-dedup probes must find nothing.
		sql, err := getSqlForPasswordOfUser(context.TODO(), userName)
		require.NoError(t, err)
		bh.sql2result[sql] = newMrsForPasswordOfUser(nil)

		sql, err = getSqlForRoleIdOfRole(context.TODO(), userName)
		require.NoError(t, err)
		bh.sql2result[sql] = newMrsForRoleIdOfRole(nil)

		// After the insert, the same password-of-user statement is re-issued
		// to fetch the auto id: hand back a row only once the insert ran.
		bh.beforeExec = func(s string) {
			if strings.HasPrefix(s, "insert into mo_catalog.mo_user(") {
				pwSql, err2 := getSqlForPasswordOfUser(context.TODO(), userName)
				if err2 == nil {
					bh.sql2result[pwSql] = newMrsForPasswordOfUser([][]interface{}{{7, "111", 0}})
				}
			}
		}
	}

	t.Run("password expire persisted", func(t *testing.T) {
		ses, bh, ctrl, ctx := newSessAndBg(t)
		defer ctrl.Finish()

		stubFreshUser(t, bh, "u1")

		cu := &createUser{
			Users: []*user{{
				Username:  "u1",
				AuthExist: true,
				IdentTyp:  tree.AccountIdentifiedByPassword,
				IdentStr:  "123456",
			}},
			MiscOpt: &tree.UserMiscOptionPasswordExpireNone{},
		}

		tenant := &TenantInfo{
			Tenant:        sysAccountName,
			User:          rootName,
			DefaultRole:   moAdminRoleName,
			TenantID:      sysAccountID,
			UserID:        rootID,
			DefaultRoleID: moAdminRoleID,
		}

		err := InitUser(ctx, ses, tenant, cu)
		require.NoError(t, err)

		var insert string
		for _, s := range bh.executedSQLs {
			if strings.HasPrefix(s, "insert into mo_catalog.mo_user(") {
				insert = s
			}
		}
		require.NotEmpty(t, insert, "expected the mo_user insert to be executed")
		// the password of an immediately-expired account expires now: the
		// insert must carry a concrete quoted timestamp instead of NULL, and
		// the status must stay unlock.
		require.NotContains(t, insert, ", NULL, ")
		require.Contains(t, insert, `"unlock"`)

		expireValue, ok := convertMiscOptToExpiredTime(&tree.UserMiscOptionPasswordExpireNone{})
		require.True(t, ok)
		require.Contains(t, insert, expireValue)
	})

	t.Run("account lock persisted as forbid", func(t *testing.T) {
		ses, bh, ctrl, ctx := newSessAndBg(t)
		defer ctrl.Finish()

		stubFreshUser(t, bh, "u1")

		cu := &createUser{
			Users: []*user{{
				Username:  "u1",
				AuthExist: true,
				IdentTyp:  tree.AccountIdentifiedByPassword,
				IdentStr:  "123456",
			}},
			MiscOpt: &tree.UserMiscOptionAccountLock{},
		}

		tenant := &TenantInfo{
			Tenant:        sysAccountName,
			User:          rootName,
			DefaultRole:   moAdminRoleName,
			TenantID:      sysAccountID,
			UserID:        rootID,
			DefaultRoleID: moAdminRoleID,
		}

		err := InitUser(ctx, ses, tenant, cu)
		require.NoError(t, err)

		var insert string
		for _, s := range bh.executedSQLs {
			if strings.HasPrefix(s, "insert into mo_catalog.mo_user(") {
				insert = s
			}
		}
		require.NotEmpty(t, insert, "expected the mo_user insert to be executed")
		// ACCOUNT LOCK creates an account that refuses to log in: the same
		// permanent forbid status that ALTER USER ... LOCK installs.
		require.Contains(t, insert, `"forbid"`)
		require.NotContains(t, insert, `"lock"`)
	})

	t.Run("password expire never persisted as sentinel", func(t *testing.T) {
		ses, bh, ctrl, ctx := newSessAndBg(t)
		defer ctrl.Finish()

		stubFreshUser(t, bh, "u1")

		cu := &createUser{
			Users: []*user{{
				Username:  "u1",
				AuthExist: true,
				IdentTyp:  tree.AccountIdentifiedByPassword,
				IdentStr:  "123456",
			}},
			MiscOpt: &tree.UserMiscOptionPasswordExpireNever{},
		}

		tenant := &TenantInfo{
			Tenant:        sysAccountName,
			User:          rootName,
			DefaultRole:   moAdminRoleName,
			TenantID:      sysAccountID,
			UserID:        rootID,
			DefaultRoleID: moAdminRoleID,
		}

		err := InitUser(ctx, ses, tenant, cu)
		require.NoError(t, err)

		var insert string
		for _, s := range bh.executedSQLs {
			if strings.HasPrefix(s, "insert into mo_catalog.mo_user(") {
				insert = s
			}
		}
		require.NotEmpty(t, insert, "expected the mo_user insert to be executed")
		require.Contains(t, insert, passwordExpireNeverTime)
	})
}

// TestPasswordChangeKeepsExpireNeverSentinel: a password change clears an
// explicit expiration but preserves PASSWORD EXPIRE NEVER (MySQL: the
// never-expire lifetime survives password changes).
func TestPasswordChangeKeepsExpireNeverSentinel(t *testing.T) {
	sql, err := getSqlForUpdatePasswordOfUser(context.TODO(), "*abc", "u1")
	require.NoError(t, err)
	require.Contains(t, sql, "expired_time = if(expired_time = '"+passwordExpireNeverTime+"', expired_time, null)")
	require.NotContains(t, sql, "expired_time = null where")
}

// TestLoginAttemptsCounterSqls covers the bookkeeping SQLs of the failed
// login counter: increment on every failure, reset on successful login.
func TestLoginAttemptsCounterSqls(t *testing.T) {
	require.Equal(t,
		`update mo_catalog.mo_user set login_attempts = login_attempts + 1  where user_name = "u1";`,
		getSqlForUpdateLoginAttemptsOfUser("u1"))
	require.Equal(t,
		`update mo_catalog.mo_user set login_attempts = 0 where user_name = "u1";`,
		getSqlForUpdateResetLoginAttemptsOfUser("u1"))
}

// newMrsForLockInfoOfUser builds the mocked catalog row for the login-time
// lock-info query (status, login_attempts, lock_time, expired_time). Pass a
// nil expiredTime for the NULL "follow the default policy" marker.
func newMrsForLockInfoOfUser(status string, loginAttempts uint64, lockTime string, expiredTime interface{}) *MysqlResultSet {
	mrs := &MysqlResultSet{}

	col1 := &MysqlColumn{}
	col1.SetName("status")
	col1.SetColumnType(defines.MYSQL_TYPE_VARCHAR)

	col2 := &MysqlColumn{}
	col2.SetName("login_attempts")
	col2.SetColumnType(defines.MYSQL_TYPE_LONGLONG)

	col3 := &MysqlColumn{}
	col3.SetName("lock_time")
	col3.SetColumnType(defines.MYSQL_TYPE_VARCHAR)

	col4 := &MysqlColumn{}
	col4.SetName("expired_time")
	col4.SetColumnType(defines.MYSQL_TYPE_VARCHAR)

	mrs.AddColumn(col1)
	mrs.AddColumn(col2)
	mrs.AddColumn(col3)
	mrs.AddColumn(col4)
	mrs.AddRow([]interface{}{status, loginAttempts, lockTime, expiredTime})
	return mrs
}

func newMrsForSystemVariables() *MysqlResultSet {
	mrs := &MysqlResultSet{}
	col1 := &MysqlColumn{}
	col1.SetName("variable_name")
	col1.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	col2 := &MysqlColumn{}
	col2.SetName("variable_value")
	col2.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	mrs.AddColumn(col1)
	mrs.AddColumn(col2)
	return mrs
}

// TestAuthenticateUserExpiryAndAttempts drives the full user-authentication
// path of AuthenticateUser with the catalog rows of the account-security
// columns and asserts the runtime behavior:
//   - an expired account (mo_user.expired_time reached) is login-able but the
//     session routine is put into the expired-password sandbox;
//   - the PASSWORD EXPIRE NEVER sentinel and future expirations do not
//     trigger the sandbox;
//   - a failed password check increments mo_user.login_attempts;
//   - a successful login of an unlocked user resets stale login_attempts;
//   - an account in the permanent forbid lock is rejected before the
//     password is even checked.
func TestAuthenticateUserExpiryAndAttempts(t *testing.T) {
	const (
		service  = "auth-expiry-" + tNameHint
		userName = "appu1"
		userID   = 7
	)

	setupService := func() {
		if moruntime.ServiceRuntime(service) != nil {
			return
		}
		rt := moruntime.NewRuntime(
			metadata.ServiceType_CN,
			service,
			nil,
			moruntime.WithClock(clock.NewHLCClock(
				func() int64 { return 100 },
				0,
			)),
		)
		moruntime.SetupServiceBasedRuntime(service, rt)
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion39)
		InitServerLevelVars(service)
	}
	setupService()

	newTestSession := func(t *testing.T) (*Session, *backgroundExecTest) {
		ctrl := gomock.NewController(t)
		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		txnClient.EXPECT().WaitLogTailAppliedAt(gomock.Any(), gomock.Any()).
			Return(timestamp.Timestamp{PhysicalTime: 100}, nil).AnyTimes()
		pu := &mo_config.ParameterUnit{
			TxnClient: txnClient,
			StorageEngine: &authenticationBarrierEngine{acquire: func(context.Context) (
				timestamp.Timestamp, error,
			) {
				return timestamp.Timestamp{PhysicalTime: 100}, nil
			}},
		}
		pu.SV = config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil).SV
		pu.SV.SetDefaultValues()
		setPu(service, pu)

		ses := &Session{
			feSessionImpl: feSessionImpl{service: service},
			rt:            &Routine{},
			timestampMap:  make(map[TS]time.Time),
		}
		rm, _ := NewRoutineManager(context.TODO(), service)
		ses.rm = rm

		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		// pin the global variables of the sys account to the defaults, so
		// tests of this file never observe values pushed by other tests.
		defaults := make(map[string]interface{})
		for name, sv := range gSysVarsDefs {
			defaults[name] = sv.Default
		}
		GSysVarsMgr.Put(uint32(sysAccountID), &SystemVariables{mp: defaults})

		// tenant exists and is open
		tenantSQL, err := getSqlForCheckTenant(t.Context(), sysAccountName)
		require.NoError(t, err)
		tenantResult := mock_frontend.NewMockExecResult(ctrl)
		tenantResult.EXPECT().GetRowCount().Return(uint64(1)).AnyTimes()
		tenantResult.EXPECT().GetInt64(gomock.Any(), uint64(0), uint64(0)).Return(int64(0), nil).AnyTimes()
		tenantResult.EXPECT().GetString(gomock.Any(), uint64(0), uint64(2)).Return("open", nil).AnyTimes()
		tenantResult.EXPECT().GetUint64(gomock.Any(), uint64(0), uint64(3)).Return(uint64(1), nil).AnyTimes()
		tenantResult.EXPECT().GetString(gomock.Any(), uint64(0), uint64(5)).Return("1.3.0", nil).AnyTimes()
		bh.sql2result[tenantSQL] = tenantResult

		// user exists; default_role column is public so the login falls back
		// to the implicit public grant.
		userSQL, err := getSqlForPasswordOfUser(t.Context(), userName)
		require.NoError(t, err)
		bh.sql2result[userSQL] = newMrsForPasswordOfUser([][]interface{}{{userID, "111", publicRoleID}})

		bh.sql2result[getSqlForRoleNameOfUserRole(userID, publicRoleID)] =
			newMrsForRoleName([][]interface{}{{publicRoleName}})
		bh.sql2result[getSqlForGetSystemVariablesWithAccount(uint64(sysAccountID))] =
			newMrsForSystemVariables()

		previous := NewBackgroundExec
		t.Cleanup(func() { NewBackgroundExec = previous })
		NewBackgroundExec = func(context.Context, FeSession, ...*BackgroundExecOption) BackgroundExec {
			return bh
		}
		return ses, bh
	}

	authenticate := func(t *testing.T, ses *Session, passwordOK bool) error {
		_, err := ses.AuthenticateUser(
			t.Context(),
			userName,
			"",
			nil,
			nil,
			func([]byte, []byte, []byte) bool { return passwordOK },
		)
		return err
	}

	t.Run("expired account enters the password-expired sandbox", func(t *testing.T) {
		ses, bh := newTestSession(t)
		bh.sql2result[getLockInfoOfUserSql(userName)] =
			newMrsForLockInfoOfUser(userStatusUnlock, 0, "2020-01-01 00:00:00", "2000-01-01 00:00:00")

		err := authenticate(t, ses, true)
		require.NoError(t, err)
		require.True(t, ses.getRoutine().isExpired(),
			"expired account must be put into the expired-password sandbox at login")
	})

	t.Run("future expiration does not enter the sandbox", func(t *testing.T) {
		ses, bh := newTestSession(t)
		bh.sql2result[getLockInfoOfUserSql(userName)] =
			newMrsForLockInfoOfUser(userStatusUnlock, 0, "2020-01-01 00:00:00",
				time.Now().UTC().AddDate(0, 0, 5).Format("2006-01-02 15:04:05"))

		err := authenticate(t, ses, true)
		require.NoError(t, err)
		require.False(t, ses.getRoutine().isExpired())
	})

	t.Run("expire-never sentinel does not enter the sandbox", func(t *testing.T) {
		ses, bh := newTestSession(t)
		bh.sql2result[getLockInfoOfUserSql(userName)] =
			newMrsForLockInfoOfUser(userStatusUnlock, 0, "2020-01-01 00:00:00", passwordExpireNeverTime)

		err := authenticate(t, ses, true)
		require.NoError(t, err)
		require.False(t, ses.getRoutine().isExpired())
	})

	t.Run("successful login resets stale login attempts", func(t *testing.T) {
		ses, bh := newTestSession(t)
		bh.sql2result[getLockInfoOfUserSql(userName)] =
			newMrsForLockInfoOfUser(userStatusUnlock, 3, "2020-01-01 00:00:00", nil)

		err := authenticate(t, ses, true)
		require.NoError(t, err)
		require.False(t, ses.getRoutine().isExpired())
		require.Contains(t, bh.executedSQLs, getSqlForUpdateResetLoginAttemptsOfUser(userName))
		require.NotContains(t, bh.executedSQLs, getSqlForUpdateUnlcokStatusOfUser(userStatusUnlock, userName))
	})

	t.Run("failed login increments login attempts when lockout is disabled", func(t *testing.T) {
		ses, bh := newTestSession(t)
		bh.sql2result[getLockInfoOfUserSql(userName)] =
			newMrsForLockInfoOfUser(userStatusUnlock, 0, "2020-01-01 00:00:00", nil)

		err := authenticate(t, ses, false)
		require.ErrorContains(t, err, "check password failed")
		require.True(t, isAuthenticationRejected(err))
		require.Contains(t, bh.executedSQLs, getSqlForUpdateLoginAttemptsOfUser(userName))
		require.NotContains(t, bh.executedSQLs, getSqlForUpdateStatusLockOfUser(userStatusLock, userName))
	})

	t.Run("permanently locked account is rejected before password check", func(t *testing.T) {
		ses, bh := newTestSession(t)
		bh.sql2result[getLockInfoOfUserSql(userName)] =
			newMrsForLockInfoOfUser(userStatusLockForever, 0, "2020-01-01 00:00:00", nil)

		err := authenticate(t, ses, false)
		require.ErrorContains(t, err, "user is locked, please ask the administrator to unlock")
		require.True(t, isAuthenticationRejected(err))
	})
}

// TestHandleSetPassword covers the SET PASSWORD executor introduced for the
// expired-password sandbox escape: it runs the ALTER USER password pipeline
// (so the password really changes and the explicit expiration is cleared),
// and releases the sandbox of the session when the current user resets its
// own password.
func TestHandleSetPassword(t *testing.T) {
	newSessAndBg := func(t *testing.T) (*Session, *backgroundExecTest, *gomock.Controller, context.Context) {
		ctrl := gomock.NewController(t)

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		t.Cleanup(bhStub.Reset)

		ses := newSes(nil, ctrl)

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()
		pu.SV.KillRountinesInterval = 0
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		rm, _ := NewRoutineManager(ctx, "")
		ses.rm = rm

		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, err := getSqlForPasswordOfUser(context.TODO(), rootName)
		require.NoError(t, err)
		bh.sql2result[sql] = newMrsForPasswordOfUser([][]interface{}{{0, "111", 0}})

		sql, err = getSqlForCheckUserHasRole(context.TODO(), rootName, moAdminRoleID)
		require.NoError(t, err)
		bh.sql2result[sql] = newMrsForSqlForCheckUserHasRole([][]interface{}{{0, 0}})

		return ses, bh, ctrl, ctx
	}

	t.Run("set password changes own password and exits the sandbox", func(t *testing.T) {
		ses, bh, ctrl, ctx := newSessAndBg(t)
		defer ctrl.Finish()

		ses.rt = &Routine{}
		ses.getRoutine().setExpired(true)

		execCtx := &ExecCtx{reqCtx: ctx}
		st := &tree.SetPassword{Password: "NewPass567"}
		err := handleSetPassword(ses, execCtx, st)
		require.NoError(t, err)
		require.False(t, ses.getRoutine().isExpired())

		sql, err := getSqlForUpdatePasswordOfUser(context.TODO(), HashPassWord("NewPass567"), rootName)
		require.NoError(t, err)
		require.Contains(t, bh.executedSQLs, sql)
	})

	t.Run("set password for other user keeps the sandbox", func(t *testing.T) {
		ses, bh, ctrl, ctx := newSessAndBg(t)
		defer ctrl.Finish()

		sql, err := getSqlForPasswordOfUser(context.TODO(), "u1")
		require.NoError(t, err)
		bh.sql2result[sql] = newMrsForPasswordOfUser([][]interface{}{{3, "111", 0}})

		sql, err = getSqlForCheckUserHasRole(context.TODO(), "u1", moAdminRoleID)
		require.NoError(t, err)
		bh.sql2result[sql] = newMrsForSqlForCheckUserHasRole(nil)

		ses.rt = &Routine{}
		ses.getRoutine().setExpired(true)

		execCtx := &ExecCtx{reqCtx: ctx}
		st := &tree.SetPassword{
			User:     &tree.User{Username: "u1", Hostname: "%"},
			Password: "NewPass567",
		}
		err = handleSetPassword(ses, execCtx, st)
		require.NoError(t, err)
		require.True(t, ses.getRoutine().isExpired())

		sql, err = getSqlForUpdatePasswordOfUser(context.TODO(), HashPassWord("NewPass567"), "u1")
		require.NoError(t, err)
		require.Contains(t, bh.executedSQLs, sql)
	})

	t.Run("set password rejects unknown user", func(t *testing.T) {
		ses, bh, ctrl, ctx := newSessAndBg(t)
		defer ctrl.Finish()

		sql, err := getSqlForPasswordOfUser(context.TODO(), "ghost")
		require.NoError(t, err)
		bh.sql2result[sql] = newMrsForPasswordOfUser(nil)

		execCtx := &ExecCtx{reqCtx: ctx}
		st := &tree.SetPassword{
			User:     &tree.User{Username: "ghost", Hostname: "%"},
			Password: "NewPass567",
		}
		err = handleSetPassword(ses, execCtx, st)
		require.ErrorContains(t, err, "user does't exist")
	})
}

const tNameHint = "expiry-attempts"
