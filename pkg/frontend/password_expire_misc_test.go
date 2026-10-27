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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
)

// TestConvertMiscOptToExpiredTime covers the translation of the PASSWORD
// EXPIRE misc options of CREATE/ALTER USER into the raw mo_user.expired_time
// SQL expression. Every non-NULL form must be a session-local now()
// (current_timestamp based) so the stored expiration instant is independent
// of the writing session's time zone.
func TestConvertMiscOptToExpiredTime(t *testing.T) {
	// PASSWORD EXPIRE: expires right now, in UTC.
	v, ok := convertMiscOptToExpiredTime(&tree.UserMiscOptionPasswordExpireNone{})
	require.True(t, ok)
	require.Equal(t, "current_timestamp()", v)

	// PASSWORD EXPIRE INTERVAL n DAY: expires n days later, in UTC.
	v, ok = convertMiscOptToExpiredTime(&tree.UserMiscOptionPasswordExpireInterval{Value: 3})
	require.True(t, ok)
	require.Equal(t, "current_timestamp() + interval 3 day", v)

	// PASSWORD EXPIRE NEVER: far-future UTC marker.
	v, ok = convertMiscOptToExpiredTime(&tree.UserMiscOptionPasswordExpireNever{})
	require.True(t, ok)
	require.Equal(t, passwordExpireNeverTimeSql, v)
	require.Contains(t, v, "current_timestamp()")

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

// TestPasswordExpireNeverHorizon: the NEVER write form must always land
// strictly beyond the preserve horizon used by updatePasswordOfUserFormat,
// otherwise a password change would clear explicit NEVER accounts.
func TestPasswordExpireNeverHorizon(t *testing.T) {
	require.Equal(t, "current_timestamp() + interval 90000 day", passwordExpireNeverTimeSql)
	require.Equal(t, "current_timestamp() + interval 40000 day", passwordExpireNeverHorizonSql)
	require.Contains(t, updatePasswordOfUserFormat, "expired_time > "+passwordExpireNeverHorizonSql)
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

	for _, auth := range []string{"identified by '123456'", "identified by ''", "identified by random password"} {
		for _, option := range []string{"password expire", "password expire default", "account lock"} {
			t.Run("reject "+auth+" with "+option, func(t *testing.T) {
				ses, bh, ctrl, ctx := newSessAndBg(t)
				defer ctrl.Finish()
				ses.rt = &Routine{}
				ses.getRoutine().setExpired(true)
				// Exercise the real parser and AST-to-authentication lowering:
				// empty/random passwords have an auth clause but empty IdentStr.
				stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, "alter user u1 "+auth+" "+option, 1)
				require.NoError(t, err)
				defer stmt.Free()
				err = handleAlterUser(ses, &ExecCtx{reqCtx: ctx}, stmt.(*tree.AlterUser))
				require.ErrorContains(t, err, "not support identified with")
				require.Empty(t, bh.executedSQLs, "reject before beginning a catalog transaction")
				require.True(t, ses.getRoutine().isExpired(), "rejection must not release the password sandbox")
			})
		}
	}

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
		// The never-expire marker is written as a UTC server-side expression
		// so that the stored instant is independent of the writing session's
		// time zone.
		require.Contains(t, insert, passwordExpireNeverTimeSql)
	})

	t.Run("creation keeps the legacy UTC last-changed default", func(t *testing.T) {
		ses, bh, ctrl, ctx := newSessAndBg(t)
		defer ctrl.Finish()

		stubFreshUser(t, bh, "u9")

		cu := &createUser{
			Users: []*user{{
				Username:  "u9",
				AuthExist: true,
				IdentTyp:  tree.AccountIdentifiedByPassword,
				IdentStr:  "123456",
			}},
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
		// Creation must keep the same UTC_TIMESTAMP column default as
		// pre-upgrade tenants and the password-reset writer.
		require.NotContains(t, insert, "password_last_changed")
		require.Contains(t, MoCatalogMoUserDDL, "password_last_changed timestamp default utc_timestamp")
	})
}

// Creation and password resets retain one timestamp convention on both sides
// of an upgrade, rather than inferring the writer from a stored instant.
func TestPasswordLastChangedKeepsLegacyConvention(t *testing.T) {
	for name, format := range map[string]string{
		"initMoUserFormat":          initMoUserFormat,
		"initMoUserWithoutIDFormat": initMoUserWithoutIDFormat,
	} {
		require.NotContains(t, format, "password_last_changed", name)
	}
	require.Contains(t, MoCatalogMoUserDDL, "password_last_changed timestamp default utc_timestamp")
	reset, err := getSqlForUpdatePasswordOfUser(t.Context(), "*hash", "u1")
	require.NoError(t, err)
	require.Contains(t, reset, "password_last_changed = utc_timestamp()")
	require.Equal(t, `select password_last_changed is not null and cast(password_last_changed as datetime) < utc_timestamp() - interval 1 day from mo_catalog.mo_user where user_name = "u1" order by user_id;`, getExpiredTimeOfUserSql("u1", 1))
}

// TestPasswordChangeKeepsExpireNeverSentinel: a password change clears an
// explicit expiration but preserves PASSWORD EXPIRE NEVER (MySQL: the
// never-expire lifetime survives password changes).
func TestPasswordChangeKeepsExpireNeverSentinel(t *testing.T) {
	sql, err := getSqlForUpdatePasswordOfUser(context.TODO(), "*abc", "u1")
	require.NoError(t, err)
	require.Contains(t, sql,
		"expired_time = if(expired_time > "+passwordExpireNeverHorizonSql+", expired_time, null)")
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
// lock-info query (status, login_attempts, lock_time, expired_time is not
// null, expired_time <= current_timestamp()). The last two columns are the
// server-computed UTC booleans; pass hasExplicitExpire=0 for the NULL
// "follow the default policy" marker.
func newMrsForLockInfoOfUser(status string, loginAttempts uint64, lockTime string, hasExplicitExpire, expireReached uint64) *MysqlResultSet {
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
	col4.SetName("expired_time is not null")
	col4.SetColumnType(defines.MYSQL_TYPE_LONGLONG)

	col5 := &MysqlColumn{}
	col5.SetName("expired_time <= current_timestamp()")
	col5.SetColumnType(defines.MYSQL_TYPE_LONGLONG)

	mrs.AddColumn(col1)
	mrs.AddColumn(col2)
	mrs.AddColumn(col3)
	mrs.AddColumn(col4)
	mrs.AddColumn(col5)
	mrs.AddRow([]interface{}{status, loginAttempts, lockTime, hasExplicitExpire, expireReached})
	return mrs
}

// newMrsForSystemVariablesWithLifetime adds a default_password_lifetime row
// to the account sysvar fixture, driving the login flow through the same
// catalog read production uses.
func newMrsForSystemVariablesWithLifetime(days string) *MysqlResultSet {
	mrs := newMrsForSystemVariables()
	mrs.AddRow([]interface{}{"default_password_lifetime", days})
	return mrs
}

// newMrsForDefaultLifetimeExpired builds the single boolean column of the
// server-side default-password-lifetime check.
func newMrsForDefaultLifetimeExpired(expired uint64) *MysqlResultSet {
	mrs := &MysqlResultSet{}
	col := &MysqlColumn{}
	col.SetName("default lifetime expired")
	col.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
	mrs.AddColumn(col)
	mrs.AddRow([]interface{}{expired})
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
		// Model an old tenant with no convention table. No authentication
		// statement may require that table before MaybeUpgradeTenant runs.
		bh.sqlPrefixErr[`select password_last_changed is not null and
  case when`] = moerr.NewNoSuchTable(t.Context(), "mo_catalog", "mo_pwd_ts_convention")
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
			newMrsForLockInfoOfUser(userStatusUnlock, 0, "2020-01-01 00:00:00", 1, 1)

		err := authenticate(t, ses, true)
		require.NoError(t, err)
		require.True(t, ses.getRoutine().isExpired(),
			"expired account must be put into the expired-password sandbox at login")
	})

	t.Run("future expiration does not enter the sandbox", func(t *testing.T) {
		ses, bh := newTestSession(t)
		bh.sql2result[getLockInfoOfUserSql(userName)] =
			newMrsForLockInfoOfUser(userStatusUnlock, 0, "2020-01-01 00:00:00", 1, 0)

		err := authenticate(t, ses, true)
		require.NoError(t, err)
		require.False(t, ses.getRoutine().isExpired())
	})

	t.Run("expire-never sentinel does not enter the sandbox", func(t *testing.T) {
		ses, bh := newTestSession(t)
		bh.sql2result[getLockInfoOfUserSql(userName)] =
			newMrsForLockInfoOfUser(userStatusUnlock, 0, "2020-01-01 00:00:00", 1, 0)

		err := authenticate(t, ses, true)
		require.NoError(t, err)
		require.False(t, ses.getRoutine().isExpired())
	})

	t.Run("successful login resets stale login attempts", func(t *testing.T) {
		ses, bh := newTestSession(t)
		bh.sql2result[getLockInfoOfUserSql(userName)] =
			newMrsForLockInfoOfUser(userStatusUnlock, 3, "2020-01-01 00:00:00", 0, 0)

		err := authenticate(t, ses, true)
		require.NoError(t, err)
		require.False(t, ses.getRoutine().isExpired())
		require.Contains(t, bh.executedSQLs, getSqlForUpdateResetLoginAttemptsOfUser(userName))
		require.NotContains(t, bh.executedSQLs, getSqlForUpdateUnlcokStatusOfUser(userStatusUnlock, userName))
	})

	t.Run("failed login increments login attempts when lockout is disabled", func(t *testing.T) {
		ses, bh := newTestSession(t)
		bh.sql2result[getLockInfoOfUserSql(userName)] =
			newMrsForLockInfoOfUser(userStatusUnlock, 0, "2020-01-01 00:00:00", 0, 0)

		err := authenticate(t, ses, false)
		require.ErrorContains(t, err, "check password failed")
		require.True(t, isAuthenticationRejected(err))
		require.Contains(t, bh.executedSQLs, getSqlForUpdateLoginAttemptsOfUser(userName))
		require.NotContains(t, bh.executedSQLs, getSqlForUpdateStatusLockOfUser(userStatusLock, userName))
	})

	t.Run("default lifetime reached enters the sandbox", func(t *testing.T) {
		ses, bh := newTestSession(t)
		bh.sql2result[getLockInfoOfUserSql(userName)] =
			newMrsForLockInfoOfUser(userStatusUnlock, 0, "2020-01-01 00:00:00", 0, 0)
		bh.sql2result[getSqlForGetSystemVariablesWithAccount(uint64(sysAccountID))] =
			newMrsForSystemVariablesWithLifetime("1")
		bh.sql2result[getExpiredTimeOfUserSql(userName, 1)] =
			newMrsForDefaultLifetimeExpired(1)

		err := authenticate(t, ses, true)
		require.NoError(t, err)
		require.True(t, ses.getRoutine().isExpired(),
			"server-side default-lifetime boolean must sandbox the session")
	})

	t.Run("default lifetime not reached does not sandbox", func(t *testing.T) {
		ses, bh := newTestSession(t)
		bh.sql2result[getLockInfoOfUserSql(userName)] =
			newMrsForLockInfoOfUser(userStatusUnlock, 0, "2020-01-01 00:00:00", 0, 0)
		bh.sql2result[getSqlForGetSystemVariablesWithAccount(uint64(sysAccountID))] =
			newMrsForSystemVariablesWithLifetime("30")
		bh.sql2result[getExpiredTimeOfUserSql(userName, 30)] =
			newMrsForDefaultLifetimeExpired(0)

		err := authenticate(t, ses, true)
		require.NoError(t, err)
		require.False(t, ses.getRoutine().isExpired())
	})

	t.Run("default lifetime catalog error must not allow login", func(t *testing.T) {
		ses, bh := newTestSession(t)
		bh.sql2result[getLockInfoOfUserSql(userName)] =
			newMrsForLockInfoOfUser(userStatusUnlock, 0, "2020-01-01 00:00:00", 0, 0)
		bh.sql2result[getSqlForGetSystemVariablesWithAccount(uint64(sysAccountID))] =
			newMrsForSystemVariablesWithLifetime("1")
		bh.sql2err[getExpiredTimeOfUserSql(userName, 1)] = context.Canceled
		require.ErrorIs(t, authenticate(t, ses, true), context.Canceled)
	})

	t.Run("wrong password does not read default lifetime", func(t *testing.T) {
		ses, bh := newTestSession(t)
		bh.sql2result[getLockInfoOfUserSql(userName)] =
			newMrsForLockInfoOfUser(userStatusUnlock, 0, "2020-01-01 00:00:00", 0, 0)
		bh.sql2result[getSqlForGetSystemVariablesWithAccount(uint64(sysAccountID))] =
			newMrsForSystemVariablesWithLifetime("1")
		require.ErrorContains(t, authenticate(t, ses, false), "check password failed")
		require.NotContains(t, bh.executedSQLs, getExpiredTimeOfUserSql(userName, 1))
	})

	t.Run("permanently locked account is rejected before password check", func(t *testing.T) {
		ses, bh := newTestSession(t)
		bh.sql2result[getLockInfoOfUserSql(userName)] =
			newMrsForLockInfoOfUser(userStatusLockForever, 0, "2020-01-01 00:00:00", 0, 0)

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

		// root is a special user on a live server; register it the same way
		// here so cross-user SET PASSWORD passes the shadow ALTER USER
		// privilege determination like production root does.
		SetSpecialUser(rootName, []byte("111"))
		t.Cleanup(func() {
			specialUsers.Lock()
			delete(specialUsers.users, rootName)
			specialUsers.Unlock()
		})

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

// TestRedactSetPasswordForLogging: the clear-text password of SET PASSWORD
// must never reach statement logging, both on the AST path (RecordStatement)
// and on the AST-less parse-failure log path.
func TestRedactSetPasswordForLogging(t *testing.T) {
	raw := "set password for u1@'%' = 'super-secret-value'"
	stmt := &tree.SetPassword{
		User:     &tree.User{Username: "u1", Hostname: "%"},
		Password: "super-secret-value",
	}
	redacted := redactStatementTextForLogging(stmt, raw)
	require.NotContains(t, redacted, "super-secret-value")
	require.Contains(t, redacted, "******")
	require.Contains(t, redacted, "set password for")

	// AST-less path (the terminal log of a parse failure): the SET PASSWORD
	// literal is masked lexically. Compact punctuation, leading comments and
	// the MySQL PASSWORD('secret') function form must not bypass the guard.
	for _, broken := range []string{
		"SET PASSWORD = 'super-secret-value'",
		"  set\n  password = 'super-secret-value'",
		"SET PASSWORD='super-secret-value' broken",
		"/* trace */ SET PASSWORD = 'super-secret-value' broken",
		"SET PASSWORD = PASSWORD('super-secret-value') unexpected id",
		"SET PASSWORD FOR u1 = PASSWORD('super-secret-value') trailing",
		// the secret may sit in a later fragment of a multi-statement text
		"select 1; SET PASSWORD = 'super-secret-value' broken",
		"; SET PASSWORD = 'super-secret-value' broken",
		// unterminated literal must fail closed
		"SET PASSWORD = 'super-secret-value",
	} {
		redacted = redactStatementTextForLogging(nil, broken)
		require.NotContains(t, redacted, "super-secret-value", broken)
	}

	// Doubled-quote literals mask by raw span: no part of the secret survives.
	redacted = redactStatementTextForLogging(nil, "SET PASSWORD = 'sec''ret-part42' broken")
	require.NotContains(t, redacted, "sec''ret-part42")
	require.NotContains(t, redacted, "ret-part42")
	require.Contains(t, redacted, "******")

	// Unrelated statements pass through untouched.
	ordinary := "select 1"
	require.Equal(t, ordinary, redactStatementTextForLogging(nil, ordinary))
}

// TestHandleSetPasswordRequiresAlterUserPrivilege: SET PASSWORD FOR other
// user is gated by the exact ALTER USER privilege determination (a session
// whose role does not hold ALTER USER gets denied) and must not touch the
// victim's password.
func TestHandleSetPasswordRequiresAlterUserPrivilege(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bh := &backgroundExecTest{}
	bh.init()

	bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
	defer bhStub.Reset()

	ses := newSes(nil, ctrl)
	// A non-admin caller in a non-sys tenant: the privilege graph resolves
	// zero ALTER USER grants for it from the (empty) catalog.
	ses.SetTenantInfo(&TenantInfo{
		Tenant:        "acc1",
		User:          "attacker",
		DefaultRole:   publicRoleName,
		TenantID:      1001,
		UserID:        42,
		DefaultRoleID: publicRoleID,
	})

	pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
	pu.SV.SetDefaultValues()
	pu.SV.KillRountinesInterval = 0
	ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

	rm, _ := NewRoutineManager(ctx, "")
	ses.rm = rm

	// The attacker holds no roles: the privilege catalog reads resolve to
	// empty grants, so the shadow ALTER USER determination must deny.
	bh.sql2result[getSqlForRoleIdOfUserId(42)] = newMrsForRoleIdOfUserId(nil)
	bh.sql2result[getSqlForCheckUserGrant(int64(publicRoleID), 42)] = newMrsForCheckUserGrant(nil)
	for _, priv := range []PrivilegeType{PrivilegeTypeAlterUser, PrivilegeTypeAccountAll} {
		for _, withSysScope := range []bool{false, true} {
			bh.sql2result[getSqlForCheckRoleHasAccountLevelForStarWithSysScope(
				int64(publicRoleID), priv, withSysScope)] = newMrsForCheckRoleHasPrivilege(nil)
		}
	}

	execCtx := &ExecCtx{reqCtx: ctx}
	st := &tree.SetPassword{
		User:     &tree.User{Username: "victim", Hostname: "%"},
		Password: "NewPass567",
	}
	err := handleSetPassword(ses, execCtx, st)
	if err == nil || !strings.Contains(err.Error(), "do not have privilege") {
		t.Fatalf("unexpected err %v; executed sqls: %v", err, bh.executedSQLs)
	}

	// The victim's password must not be touched at all.
	sql, err2 := getSqlForUpdatePasswordOfUser(context.TODO(), HashPassWord("NewPass567"), "victim")
	require.NoError(t, err2)
	require.NotContains(t, bh.executedSQLs, sql)
}

// TestDoAlterUserCommitFailureKeepsSandbox: the expired-password sandbox of
// the session is released only after the password change commits. A commit
// failure rolls the change back, so the session must stay sand-boxed.
func TestDoAlterUserCommitFailureKeepsSandbox(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bh := &backgroundExecTest{}
	bh.init()

	bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
	defer bhStub.Reset()

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
	bh.sql2err["commit;"] = moerr.NewInternalErrorNoCtx("injected commit failure")

	sql, err := getSqlForPasswordOfUser(context.TODO(), rootName)
	require.NoError(t, err)
	bh.sql2result[sql] = newMrsForPasswordOfUser([][]interface{}{{0, "111", 0}})

	sql, err = getSqlForCheckUserHasRole(context.TODO(), rootName, moAdminRoleID)
	require.NoError(t, err)
	bh.sql2result[sql] = newMrsForSqlForCheckUserHasRole([][]interface{}{{0, 0}})

	ses.rt = &Routine{}
	ses.getRoutine().setExpired(true)

	au := &alterUser{
		IfExists: false,
		Users: []*user{{
			Username:  rootName,
			Hostname:  "%",
			AuthExist: true,
			IdentTyp:  tree.AccountIdentifiedByPassword,
			IdentStr:  "NewPass567",
		}},
	}
	err = doAlterUser(ctx, ses, au)
	require.Error(t, err)
	require.True(t, ses.getRoutine().isExpired(),
		"a rolled-back password change must not release the expired-password sandbox")
}

// TestRedactParseErrorSecrets: a syntax error echo (`near "..."`) must not
// replay a masked secret into logs, records, or the client error.
func TestRedactParseErrorSecrets(t *testing.T) {
	raw := "SET PASSWORD = 'unterminated-secret42"
	_, err := parsers.ParseOne(context.TODO(), dialect.MYSQL, raw, 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unterminated-secret42", "the parser echo carries the raw secret")

	scrubbed := redactStatementErrorForLogging(err, raw)
	require.NotContains(t, scrubbed.Error(), "unterminated-secret42")
	require.Contains(t, scrubbed.Error(), "******")

	// The doubled-quote literal is scrubbed by raw span as well.
	raw = "SET PASSWORD = 'sec''ret-echo42' broken"
	_, err = parsers.ParseOne(context.TODO(), dialect.MYSQL, raw, 1)
	require.Error(t, err)
	scrubbed = redactStatementErrorForLogging(err, raw)
	require.NotContains(t, scrubbed.Error(), "ret-echo42")

	// Multi-statement input: the secret sits in the second fragment.
	raw = "select 1; SET PASSWORD = 'batch-echo42' broken"
	_, err = parsers.ParseOne(context.TODO(), dialect.MYSQL, raw, 1)
	require.Error(t, err)
	scrubbed = redactStatementErrorForLogging(err, raw)
	require.NotContains(t, scrubbed.Error(), "batch-echo42")

	// A comment between '=' and the literal is skipped by the lexer echo, so
	// the masked span (which contains the comment) cannot match verbatim:
	// the echo must still be scrubbed wholesale.
	raw = "SET PASSWORD = /* annotation */ 'comment-secret42"
	_, err = parsers.ParseOne(context.TODO(), dialect.MYSQL, raw, 1)
	require.Error(t, err)
	scrubbed = redactStatementErrorForLogging(err, raw)
	require.NotContains(t, scrubbed.Error(), "comment-secret42")
	require.Contains(t, scrubbed.Error(), "******")

	// A literal longer than the lexer echo limit is echoed truncated; the
	// partial echo must still be scrubbed.
	longSecret := strings.Repeat("a1b2c3d4", 200)
	raw = "SET PASSWORD = '" + longSecret + " broken"
	_, err = parsers.ParseOne(context.TODO(), dialect.MYSQL, raw, 1)
	require.Error(t, err)
	scrubbed = redactStatementErrorForLogging(err, raw)
	require.NotContains(t, scrubbed.Error(), longSecret[:64])

	// Composed multi-secret probe: the first password IS the framing
	// delimiter text, so replacing it verbatim first would destroy the
	// ` near "` boundary for the tail scrub and leak the second,
	// unterminated secret. The boundary is located on the RAW error before
	// any replacement, so the whole echo tail is dropped.
	raw = "SET PASSWORD = ' near '; SET PASSWORD = /*x*/'ReviewFramingSecret42"
	_, err = parsers.ParseOne(context.TODO(), dialect.MYSQL, raw, 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ReviewFramingSecret42")
	scrubbed = redactStatementErrorForLogging(err, raw)
	require.NotContains(t, scrubbed.Error(), "ReviewFramingSecret42")
	require.NotContains(t, scrubbed.Error(), "' near '")
	require.Contains(t, scrubbed.Error(), ` near "******"`)
	require.Equal(t, 1, strings.Count(scrubbed.Error(), ` near "`),
		"the sanitized error keeps exactly one, fixed echo frame")

	// Comment-prefixed control from the same review: still fully masked.
	raw = "SET PASSWORD = /* annotation */ 'ReviewCommentUnterminated42"
	_, err = parsers.ParseOne(context.TODO(), dialect.MYSQL, raw, 1)
	require.Error(t, err)
	scrubbed = redactStatementErrorForLogging(err, raw)
	require.NotContains(t, scrubbed.Error(), "ReviewCommentUnterminated42")

	// Attacker-controlled delimiter: the password itself contains
	// ` near "`, which used to make a delimiter search land inside the
	// secret and leave an unmatched short suffix behind. Fail closed: the
	// whole echo tail is replaced.
	raw = "SET PASSWORD = /* annotation */ 'Abcd1234 near \"x"
	_, err = parsers.ParseOne(context.TODO(), dialect.MYSQL, raw, 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Abcd1234", "the raw echo carries the secret")
	scrubbed = redactStatementErrorForLogging(err, raw)
	require.NotContains(t, scrubbed.Error(), "Abcd1234")
	require.Contains(t, scrubbed.Error(), ` near "******"`)
	require.NotContains(t, scrubbed.Error(), "annotation")

	// A short comment probe: the same fail-closed rule applies to any
	// comment the lexer echo skips, not only long annotations.
	raw = "SET PASSWORD = /*x*/'ShortPw1"
	_, err = parsers.ParseOne(context.TODO(), dialect.MYSQL, raw, 1)
	require.Error(t, err)
	scrubbed = redactStatementErrorForLogging(err, raw)
	require.NotContains(t, scrubbed.Error(), "ShortPw1")
	require.Contains(t, scrubbed.Error(), ` near "******"`)

	// Malformed SET PASSWORD client error boundary: the near-tail is always
	// the masked placeholder once a secret literal is present, regardless of
	// what the echo carried verbatim.
	raw = "SET PASSWORD = 'ClientErr42' trailing garbage"
	_, err = parsers.ParseOne(context.TODO(), dialect.MYSQL, raw, 1)
	require.Error(t, err)
	scrubbed = redactStatementErrorForLogging(err, raw)
	require.Contains(t, scrubbed.Error(), ` near "******"`)
	require.True(t, strings.HasSuffix(scrubbed.Error(), ` near "******"`) ||
		strings.HasSuffix(scrubbed.Error(), ` near "******";`), scrubbed.Error())

	// No secret shape: the original error passes through untouched.
	_, err = parsers.ParseOne(context.TODO(), dialect.MYSQL, "selec from", 1)
	require.Error(t, err)
	require.True(t, err == redactStatementErrorForLogging(err, "selec from"))

	// Control: the masker records the post-'=' token of a malformed SET
	// PASSWORD as a secret span even when it is not quoted, so the echo
	// tail is replaced rather than kept — the fail-closed rule leaves no
	// attacker-controlled remainder on this statement class either.
	_, err = parsers.ParseOne(context.TODO(), dialect.MYSQL, "SET PASSWORD = uuu", 1)
	require.Error(t, err)
	scrubbed = redactStatementErrorForLogging(err, "SET PASSWORD = uuu")
	require.NotContains(t, scrubbed.Error(), "uuu")
	require.Contains(t, scrubbed.Error(), ` near "******"`)
}
