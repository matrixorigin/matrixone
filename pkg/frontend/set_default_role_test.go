// Copyright 2026 Matrix Origin
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
	"fmt"
	"sort"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestSetDefaultRolePrivilegeAndSelfAuthorization(t *testing.T) {
	stmt := &tree.SetDefaultRole{
		Type:  tree.SET_DEFAULT_ROLE_TYPE_NORMAL,
		Roles: []*tree.Role{{UserName: "writer"}},
		Users: []*tree.User{{Username: "alice", Hostname: "%"}},
	}
	priv := determinePrivilegeSetOfStatement(stmt)
	require.Equal(t, objectTypeAccount, priv.objectType())
	require.Equal(t, privilegeKindGeneral, priv.kind)
	require.False(t, priv.canExecInPasswordExpired)
	require.Len(t, priv.entries, 2)

	ctrl := gomock.NewController(t)
	ses := newSes(priv, ctrl)
	t.Cleanup(ses.Close)
	ses.SetTenantInfo(&TenantInfo{
		Tenant:        "acme",
		User:          "alice",
		DefaultRole:   "public",
		TenantID:      17,
		UserID:        7,
		DefaultRoleID: 1,
	})
	ses.SetFromRealUser(true)
	bh := &backgroundExecTest{}
	bh.init()
	created := stubSetDefaultRoleBackgroundExec(t, bh, nil)

	self, err := setDefaultRoleTargetsAreCurrentUser(context.Background(), ses, stmt)
	require.NoError(t, err)
	require.True(t, self)

	other := *stmt
	other.Users = []*tree.User{{Username: "bob", Hostname: "%"}}
	self, err = setDefaultRoleTargetsAreCurrentUser(context.Background(), ses, &other)
	require.NoError(t, err)
	require.False(t, self)

	mixed := *stmt
	mixed.Users = []*tree.User{{Username: "alice", Hostname: "%"}, {Username: "bob", Hostname: "%"}}
	self, err = setDefaultRoleTargetsAreCurrentUser(context.Background(), ses, &mixed)
	require.NoError(t, err)
	require.False(t, self)

	withAuth := *stmt
	withAuth.Users = []*tree.User{{Username: "alice", Hostname: "%", AuthOption: &tree.AccountIdentified{}}}
	self, err = setDefaultRoleTargetsAreCurrentUser(context.Background(), ses, &withAuth)
	require.NoError(t, err)
	require.False(t, self)

	ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(context.Background(), ses, stmt)
	require.NoError(t, err)
	require.True(t, ok)
	for _, tc := range []struct {
		name, currentUser, targetUser string
	}{
		{"invalid authenticated name", "bad:name", "alice"},
		{"invalid target name", "alice", "bad:name"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			previousUser := ses.GetTenantInfo().User
			ses.GetTenantInfo().User = tc.currentUser
			t.Cleanup(func() { ses.GetTenantInfo().User = previousUser })
			ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(
				context.Background(), ses, setDefaultRoleTestStatement("writer", tc.targetUser))
			require.ErrorContains(t, err, "invalid")
			require.False(t, ok)
			require.Zero(t, *created, "invalid identities must not fall through to catalog authorization")
		})
	}
}

func TestSetDefaultRoleNoneThroughFrontendPreservesActiveRole(t *testing.T) {
	ctx := context.Background()
	ses := newSetDefaultRoleTestSession(t, "acme", "alice", 7)
	ses.GetTenantInfo().SetDefaultRole("writer")
	ses.GetTenantInfo().SetDefaultRoleID(9)
	bh := &setDefaultRoleLifecycleExec{backgroundExecTest: newSetDefaultRoleTestBackgroundExec(publicRoleName, 1, []setDefaultRoleTestUser{
		{id: 7, name: "alice", host: "localhost", grants: []int64{1}},
	})}
	var opts *BackgroundExecOption
	stubSetDefaultRoleBackgroundExec(t, bh, &opts)
	stmt, err := mysql.ParseOne(ctx, "set default role none to 'alice'@'localhost'", 1)
	require.NoError(t, err)
	defer stmt.Free()

	_, err = execInFrontend(ses, &ExecCtx{reqCtx: ctx, stmt: stmt})
	require.NoError(t, err)
	require.Contains(t, bh.executedSQLs, "update mo_catalog.mo_user set default_role = 1 where user_id = 7;")
	require.Equal(t, "commit;", bh.executedSQLs[len(bh.executedSQLs)-1])
	require.NotContains(t, bh.executedSQLs, "rollback;")
	require.Equal(t, "writer", ses.GetTenantInfo().GetDefaultRole())
	require.Equal(t, uint32(9), ses.GetTenantInfo().GetDefaultRoleID())
	require.NotNil(t, opts)
	require.True(t, opts.forcePessimisticRC)
	require.True(t, opts.cancelTxnCreateWithRequest)
	require.Equal(t, 1, bh.closeCalls)
	require.Equal(t, len(bh.executedSQLs), bh.sqlCountAtClose, "finalize the transaction before closing its executor")
}

func TestSetDefaultRoleFrontendPropagatesCatalogFailuresAndCloses(t *testing.T) {
	for _, tc := range []struct {
		name, failSQL string
	}{
		{"begin", "begin;"},
		{"role lookup", "select role_id, role_name from mo_catalog.mo_role where role_name = 'writer' order by role_id for update;"},
		{"user lookup", setDefaultRoleUserLookupSQL("alice")},
		{"direct grant lookup", setDefaultRoleMembershipLookupSQL(7, []int64{2, 9})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ses := newSetDefaultRoleTestSession(t, "acme", "alice", 7)
			bh := &setDefaultRoleLifecycleExec{backgroundExecTest: newSetDefaultRoleTestBackgroundExec("writer", 9, []setDefaultRoleTestUser{
				{id: 7, name: "alice", host: "localhost", grants: []int64{9}},
			})}
			injectedErr := errors.New("catalog operation failed")
			bh.sql2err[tc.failSQL] = injectedErr
			stubSetDefaultRoleBackgroundExec(t, bh, nil)

			_, err := execInFrontend(ses, &ExecCtx{reqCtx: context.Background(), stmt: setDefaultRoleTestStatement("writer", "alice")})
			require.ErrorIs(t, err, injectedErr)
			assertSetDefaultRoleNoWrites(t, bh.backgroundExecTest)
			require.NotContains(t, bh.executedSQLs, "commit;")
			if tc.failSQL == "begin;" {
				require.Equal(t, []string{"begin;"}, bh.executedSQLs)
			} else {
				require.Equal(t, []string{tc.failSQL, "rollback;"}, bh.executedSQLs[len(bh.executedSQLs)-2:],
					"only rollback may follow the failed operation")
			}
			require.Equal(t, 1, bh.closeCalls)
			require.Equal(t, len(bh.executedSQLs), bh.sqlCountAtClose)
		})
	}
}

func TestSetDefaultRoleWriterUsesPrivateLockingTxnAndLeavesSessionRoleUnchanged(t *testing.T) {
	ses := newSetDefaultRoleTestSession(t, "acme", "alice", 7)
	bh := newSetDefaultRoleTestBackgroundExec("writer", 9, []setDefaultRoleTestUser{
		{id: 7, name: "alice", host: "localhost", grants: []int64{9}},
	})
	var opts *BackgroundExecOption
	stubSetDefaultRoleBackgroundExec(t, bh, &opts)

	beforeRole, beforeRoleID := ses.GetTenantInfo().GetDefaultRole(), ses.GetTenantInfo().GetDefaultRoleID()
	stmt := setDefaultRoleTestStatement("writer", "alice")
	require.NoError(t, doSetDefaultRole(context.Background(), ses, stmt))

	require.NotNil(t, opts)
	require.True(t, opts.forcePessimisticRC)
	require.True(t, opts.cancelTxnCreateWithRequest)
	require.Equal(t, beforeRole, ses.GetTenantInfo().GetDefaultRole())
	require.Equal(t, beforeRoleID, ses.GetTenantInfo().GetDefaultRoleID())
	require.Equal(t, []string{
		"begin;",
		"select role_id, role_name from mo_catalog.mo_role where role_name = 'writer' order by role_id for update;",
		"select user_id, user_name, user_host from mo_catalog.mo_user where user_name = 'alice' order by user_id for update;",
		setDefaultRoleMembershipLookupSQL(7, []int64{2, 9}),
		"update mo_catalog.mo_user set default_role = 9 where user_id = 7;",
		"commit;",
	}, bh.executedSQLs)
}

func TestSetDefaultRoleWriterRejectsPartialAndUnsupportedRequests(t *testing.T) {
	t.Run("ungranted target rolls back before writes", func(t *testing.T) {
		ses := newSetDefaultRoleTestSession(t, "acme", "alice", 7)
		bh := newSetDefaultRoleTestBackgroundExec("writer", 9, []setDefaultRoleTestUser{
			{id: 7, name: "alice", host: "localhost", grants: []int64{9}},
			{id: 8, name: "bob", host: "localhost"},
		})
		stubSetDefaultRoleBackgroundExec(t, bh, nil)

		stmt := setDefaultRoleTestStatement("writer", "alice", "bob")
		err := doSetDefaultRole(context.Background(), ses, stmt)
		require.Error(t, err)
		require.Contains(t, bh.executedSQLs, "rollback;")
		for _, sql := range bh.executedSQLs {
			require.NotContains(t, sql, "update mo_catalog.mo_user set default_role")
		}
	})

	for _, tc := range []struct {
		name        string
		stmt        *tree.SetDefaultRole
		wantError   string
		currentUser string
	}{
		{
			name:      "all is unsupported",
			stmt:      &tree.SetDefaultRole{Type: tree.SET_DEFAULT_ROLE_TYPE_ALL, Users: []*tree.User{{Username: "alice", Hostname: "%"}}},
			wantError: "ALL is not supported",
		},
		{
			name: "multiple defaults are unsupported",
			stmt: &tree.SetDefaultRole{
				Type:  tree.SET_DEFAULT_ROLE_TYPE_NORMAL,
				Roles: []*tree.Role{{UserName: "writer"}, {UserName: "reader"}},
				Users: []*tree.User{{Username: "alice", Hostname: "%"}},
			},
			wantError: "exactly one role",
		},
		{name: "missing statement", wantError: "statement is missing"},
		{name: "NONE with roles", stmt: &tree.SetDefaultRole{Type: tree.SET_DEFAULT_ROLE_TYPE_NONE, Roles: []*tree.Role{{UserName: "writer"}}}, wantError: "NONE cannot include"},
		{name: "missing role", stmt: &tree.SetDefaultRole{Type: tree.SET_DEFAULT_ROLE_TYPE_NORMAL}, wantError: "exactly one role"},
		{name: "nil role", stmt: &tree.SetDefaultRole{Type: tree.SET_DEFAULT_ROLE_TYPE_NORMAL, Roles: []*tree.Role{nil}}, wantError: "exactly one role"},
		{name: "invalid role type", stmt: &tree.SetDefaultRole{Type: tree.SetDefaultRoleType(-1)}, wantError: "invalid SET DEFAULT ROLE type"},
		{name: "invalid role name", stmt: setDefaultRoleTestStatement("bad:name", "alice"), wantError: "invalid"},
		{name: "missing users", stmt: setDefaultRoleTestStatement("writer"), wantError: "at least one user"},
		{name: "nil user", stmt: &tree.SetDefaultRole{Type: tree.SET_DEFAULT_ROLE_TYPE_NONE, Users: []*tree.User{nil}}, wantError: "empty user"},
		{name: "authentication option", stmt: &tree.SetDefaultRole{Type: tree.SET_DEFAULT_ROLE_TYPE_NONE, Users: []*tree.User{{Username: "alice", Hostname: "%", AuthOption: &tree.AccountIdentified{}}}}, wantError: "authentication options"},
		{name: "invalid user name", stmt: setDefaultRoleTestStatement("writer", "bad:name"), wantError: "invalid"},
		{name: "blank host", stmt: &tree.SetDefaultRole{Type: tree.SET_DEFAULT_ROLE_TYPE_NONE, Users: []*tree.User{{Username: "alice", Hostname: " "}}}, wantError: "requires a user host"},
		{name: "invalid authenticated name", stmt: setDefaultRoleTestStatement("writer", "alice"), currentUser: "bad:name", wantError: "invalid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ses := newSetDefaultRoleTestSession(t, "acme", "alice", 7)
			if tc.currentUser != "" {
				ses.GetTenantInfo().User = tc.currentUser
			}
			bh := &backgroundExecTest{}
			bh.init()
			created := stubSetDefaultRoleBackgroundExec(t, bh, nil)
			require.ErrorContains(t, doSetDefaultRole(context.Background(), ses, tc.stmt), tc.wantError)
			require.Zero(t, *created, "reject invalid input before allocating an executor")
			require.Empty(t, bh.executedSQLs)
		})
	}
}

func TestSetDefaultRoleWriterRejectsMissingAndMalformedCatalogRows(t *testing.T) {
	const roleSQL = "select role_id, role_name from mo_catalog.mo_role where role_name = 'writer' order by role_id for update;"
	for _, tc := range []struct {
		name, target, sql, wantError string
		result                       ExecResult
	}{
		{name: "missing role", sql: roleSQL, result: newMrsForSetDefaultRoleRoleRows(nil), wantError: "there is no role"},
		{name: "ambiguous role", sql: roleSQL, result: newMrsForSetDefaultRoleRoleRows([][]interface{}{{int64(9), "writer"}, {int64(10), "writer"}}), wantError: "multiple catalog rows"},
		{name: "role id overflow", sql: roleSQL, result: newMrsForSetDefaultRoleRoleRows([][]interface{}{{int64(1) << 32, "writer"}}), wantError: "invalid catalog role"},
		{name: "role name mismatch", sql: roleSQL, result: newMrsForSetDefaultRoleRoleRows([][]interface{}{{int64(9), "reader"}}), wantError: "invalid catalog role"},
		{name: "malformed role id", sql: roleSQL, result: newMrsForSetDefaultRoleRoleRows([][]interface{}{{"not-an-id", "writer"}})},
		{name: "missing authenticated user", sql: setDefaultRoleUserLookupSQL("alice"), result: newMrsForSetDefaultRoleUserRows(nil), wantError: "current user no longer exists"},
		{name: "missing other target", target: "bob", sql: setDefaultRoleUserLookupSQL("bob"), result: newMrsForSetDefaultRoleUserRows(nil), wantError: "there is no user bob"},
		{name: "ambiguous user", sql: setDefaultRoleUserLookupSQL("alice"), result: newMrsForSetDefaultRoleUserRows([][]interface{}{{int64(7), "alice", "localhost"}, {int64(8), "alice", "localhost"}}), wantError: "multiple catalog rows"},
		{name: "user id overflow", sql: setDefaultRoleUserLookupSQL("alice"), result: newMrsForSetDefaultRoleUserRows([][]interface{}{{int64(1) << 32, "alice", "localhost"}}), wantError: "invalid catalog user"},
		{name: "user name mismatch", sql: setDefaultRoleUserLookupSQL("alice"), result: newMrsForSetDefaultRoleUserRows([][]interface{}{{int64(7), "bob", "localhost"}}), wantError: "invalid catalog user"},
		{name: "malformed user id", sql: setDefaultRoleUserLookupSQL("alice"), result: newMrsForSetDefaultRoleUserRows([][]interface{}{{"not-an-id", "alice", "localhost"}})},
		{name: "malformed grant id", sql: setDefaultRoleMembershipLookupSQL(7, []int64{2, 9}), result: newMrsForSetDefaultRoleGrantRows([][]interface{}{{"not-an-id"}})},
		{name: "unexpected grant", sql: setDefaultRoleMembershipLookupSQL(7, []int64{2, 9}), result: newMrsForSetDefaultRoleGrantRows([][]interface{}{{int64(99)}}), wantError: "unexpected role"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ses := newSetDefaultRoleTestSession(t, "acme", "alice", 7)
			bh := newSetDefaultRoleTestBackgroundExec("writer", 9, []setDefaultRoleTestUser{
				{id: 7, name: "alice", host: "localhost", grants: []int64{9}},
				{id: 8, name: "bob", host: "localhost", grants: []int64{9}},
			})
			bh.sql2result[tc.sql] = tc.result
			stubSetDefaultRoleBackgroundExec(t, bh, nil)
			target := tc.target
			if target == "" {
				target = "alice"
			}
			err := doSetDefaultRole(context.Background(), ses, setDefaultRoleTestStatement("writer", target))
			require.Error(t, err)
			if tc.wantError != "" {
				require.ErrorContains(t, err, tc.wantError)
			}
			assertSetDefaultRoleNoWrites(t, bh)
			require.NotContains(t, bh.executedSQLs, "commit;")
			require.Equal(t, []string{tc.sql, "rollback;"}, bh.executedSQLs[len(bh.executedSQLs)-2:])
		})
	}
}

func TestSetDefaultRoleWriterRollsBackOnWriteAndCommitFailures(t *testing.T) {
	t.Run("second target update fails", func(t *testing.T) {
		ses := newSetDefaultRoleTestSession(t, "acme", "alice", 7)
		bh := newSetDefaultRoleTestBackgroundExec("writer", 9, []setDefaultRoleTestUser{
			{id: 7, name: "alice", host: "localhost", grants: []int64{9}},
			{id: 8, name: "bob", host: "localhost", grants: []int64{9}},
		})
		writeErr := errors.New("second target update failed")
		bh.sql2err["update mo_catalog.mo_user set default_role = 9 where user_id = 8;"] = writeErr
		stubSetDefaultRoleBackgroundExec(t, bh, nil)

		err := doSetDefaultRole(context.Background(), ses, setDefaultRoleTestStatement("writer", "alice", "bob"))
		require.ErrorIs(t, err, writeErr)
		require.Contains(t, bh.executedSQLs, "update mo_catalog.mo_user set default_role = 9 where user_id = 7;")
		require.Contains(t, bh.executedSQLs, "update mo_catalog.mo_user set default_role = 9 where user_id = 8;")
		require.Contains(t, bh.executedSQLs, "rollback;")
		require.NotContains(t, bh.executedSQLs, "commit;")
	})

	t.Run("commit fails", func(t *testing.T) {
		ses := newSetDefaultRoleTestSession(t, "acme", "alice", 7)
		bh := newSetDefaultRoleTestBackgroundExec("writer", 9, []setDefaultRoleTestUser{
			{id: 7, name: "alice", host: "localhost", grants: []int64{9}},
		})
		commitErr := errors.New("commit failed")
		bh.sql2err["commit;"] = commitErr
		stubSetDefaultRoleBackgroundExec(t, bh, nil)

		err := doSetDefaultRole(context.Background(), ses, setDefaultRoleTestStatement("writer", "alice"))
		require.ErrorIs(t, err, commitErr)
		require.Contains(t, bh.executedSQLs, "update mo_catalog.mo_user set default_role = 9 where user_id = 7;")
		require.Contains(t, bh.executedSQLs, "commit;")
		require.Contains(t, bh.executedSQLs, "rollback;")
	})
}

func TestSetDefaultRoleWriterRejectsHostAndSelfIdentityMismatch(t *testing.T) {
	t.Run("explicit target host mismatch", func(t *testing.T) {
		ses := newSetDefaultRoleTestSession(t, "acme", "alice", 7)
		bh := newSetDefaultRoleTestBackgroundExec("writer", 9, []setDefaultRoleTestUser{
			{id: 7, name: "alice", host: "localhost"},
			{id: 8, name: "bob", host: "localhost", grants: []int64{9}},
		})
		stubSetDefaultRoleBackgroundExec(t, bh, nil)
		stmt := setDefaultRoleTestStatement("writer", "bob")
		stmt.Users[0].Hostname = "remotehost"

		err := doSetDefaultRole(context.Background(), ses, stmt)
		require.ErrorContains(t, err, "does not match the account catalog")
		require.Contains(t, bh.executedSQLs, "rollback;")
		for _, sql := range bh.executedSQLs {
			require.NotContains(t, sql, "update mo_catalog.mo_user set default_role")
		}
	})

	t.Run("case-folded self name resolves to another user id", func(t *testing.T) {
		ses := newSetDefaultRoleTestSession(t, "acme", "alice", 7)
		bh := newSetDefaultRoleTestBackgroundExec("writer", 9, []setDefaultRoleTestUser{
			{id: 7, name: "alice", host: "localhost"},
		})
		bh.sql2result[setDefaultRoleUserLookupSQL("ALICE")] = newMrsForSetDefaultRoleUserRows(
			[][]interface{}{{int64(8), "ALICE", "localhost"}},
		)
		stubSetDefaultRoleBackgroundExec(t, bh, nil)

		err := doSetDefaultRole(context.Background(), ses, setDefaultRoleTestStatement("writer", "ALICE"))
		require.ErrorContains(t, err, "current user identity changed")
		require.Contains(t, bh.executedSQLs, "rollback;")
		for _, sql := range bh.executedSQLs {
			require.NotContains(t, sql, "update mo_catalog.mo_user set default_role")
		}
	})
}

func TestSetDefaultRoleCatalogQueriesTreatEmptyRowsAsNoMatches(t *testing.T) {
	for _, tc := range []struct {
		name      string
		results   []interface{}
		wantError string
	}{
		{name: "no result sets"},
		{name: "non-result payload", results: []interface{}{42}, wantError: "not the type of result set"},
		{name: "multiple result sets", results: []interface{}{&MysqlResultSet{}, &MysqlResultSet{}}, wantError: "invalid result"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bh := mock_frontend.NewMockBackgroundExec(gomock.NewController(t))
			gomock.InOrder(
				bh.EXPECT().ClearExecResultSet(),
				bh.EXPECT().Exec(gomock.Any(), "select catalog row"),
				bh.EXPECT().GetExecResultSet().Return(tc.results),
			)
			result, err := querySetDefaultRoleResult(context.Background(), bh, "select catalog row")
			if tc.wantError == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.wantError)
			}
			require.Nil(t, result)
		})
	}
}

func TestSetDefaultRoleWriterAcceptsZeroRootIDsAndProtectsAdminTargets(t *testing.T) {
	t.Run("root can select moadmin for self", func(t *testing.T) {
		ses := newSetDefaultRoleTestSession(t, sysAccountName, rootName, rootID)
		bh := newSetDefaultRoleTestBackgroundExec(moAdminRoleName, int64(moAdminRoleID), []setDefaultRoleTestUser{
			{id: int64(rootID), name: rootName, host: "localhost", grants: []int64{int64(moAdminRoleID)}},
		})
		stubSetDefaultRoleBackgroundExec(t, bh, nil)
		require.NoError(t, doSetDefaultRole(context.Background(), ses, setDefaultRoleTestStatement(moAdminRoleName, rootName)))
		require.Contains(t, bh.executedSQLs, "update mo_catalog.mo_user set default_role = 0 where user_id = 0;")
	})

	t.Run("nonadmin cannot set another admin target even when the selected role is admin", func(t *testing.T) {
		ses := newSetDefaultRoleTestSession(t, sysAccountName, "app", 7)
		bh := newSetDefaultRoleTestBackgroundExec(moAdminRoleName, int64(moAdminRoleID), []setDefaultRoleTestUser{
			{id: 7, name: "app", host: "localhost"},
			{id: int64(rootID), name: rootName, host: "localhost", grants: []int64{int64(moAdminRoleID)}},
		})
		stubSetDefaultRoleBackgroundExec(t, bh, nil)
		err := doSetDefaultRole(context.Background(), ses, setDefaultRoleTestStatement(moAdminRoleName, rootName))
		require.Error(t, err)
		require.Contains(t, err.Error(), "administrator user")
		for _, sql := range bh.executedSQLs {
			require.NotContains(t, sql, "update mo_catalog.mo_user set default_role")
		}
		require.Contains(t, bh.executedSQLs, "rollback;")
	})
}

type setDefaultRoleTestUser struct {
	id     int64
	name   string
	host   string
	grants []int64
}

func newSetDefaultRoleTestSession(t *testing.T, tenant, username string, userID uint32) *Session {
	t.Helper()
	ctrl := gomock.NewController(t)
	ses := newSes(determinePrivilegeSetOfStatement(setDefaultRoleTestStatement("writer", username)), ctrl)
	t.Cleanup(ses.Close)
	ses.SetTenantInfo(&TenantInfo{
		Tenant:        tenant,
		User:          username,
		DefaultRole:   "public",
		TenantID:      17,
		UserID:        userID,
		DefaultRoleID: 1,
	})
	return ses
}

func setDefaultRoleTestStatement(role string, usernames ...string) *tree.SetDefaultRole {
	stmt := &tree.SetDefaultRole{
		Type:  tree.SET_DEFAULT_ROLE_TYPE_NORMAL,
		Roles: []*tree.Role{{UserName: role}},
	}
	for _, username := range usernames {
		stmt.Users = append(stmt.Users, &tree.User{Username: username, Hostname: "%"})
	}
	return stmt
}

func newSetDefaultRoleTestBackgroundExec(role string, roleID int64, users []setDefaultRoleTestUser) *backgroundExecTest {
	bh := &backgroundExecTest{}
	bh.init()
	bh.sql2result[fmt.Sprintf(
		"select role_id, role_name from mo_catalog.mo_role where role_name = %s order by role_id for update;",
		quoteSQLStringLiteral(role),
	)] = newMrsForSetDefaultRoleRoleRows([][]interface{}{{roleID, role}})

	adminRoleID := int64(accountAdminRoleID)
	if roleID == int64(moAdminRoleID) {
		adminRoleID = int64(moAdminRoleID)
	}
	roleIDs := []int64{roleID}
	if roleID != adminRoleID {
		roleIDs = append(roleIDs, adminRoleID)
	}
	sort.Slice(roleIDs, func(i, j int) bool { return roleIDs[i] < roleIDs[j] })
	for _, user := range users {
		bh.sql2result[setDefaultRoleUserLookupSQL(user.name)] = newMrsForSetDefaultRoleUserRows(
			[][]interface{}{{user.id, user.name, user.host}},
		)
		var grants [][]interface{}
		for _, grant := range user.grants {
			grants = append(grants, []interface{}{grant})
		}
		bh.sql2result[setDefaultRoleMembershipLookupSQL(user.id, roleIDs)] = newMrsForSetDefaultRoleGrantRows(grants)
	}
	return bh
}

func newMrsForSetDefaultRoleRoleRows(rows [][]interface{}) *MysqlResultSet {
	mrs := &MysqlResultSet{}
	for _, col := range []struct {
		name string
		typ  defines.MysqlType
	}{{"role_id", defines.MYSQL_TYPE_LONGLONG}, {"role_name", defines.MYSQL_TYPE_VARCHAR}} {
		column := &MysqlColumn{}
		column.SetName(col.name)
		column.SetColumnType(col.typ)
		mrs.AddColumn(column)
	}
	for _, row := range rows {
		mrs.AddRow(row)
	}
	return mrs
}

func newMrsForSetDefaultRoleUserRows(rows [][]interface{}) *MysqlResultSet {
	mrs := &MysqlResultSet{}
	for _, col := range []struct {
		name string
		typ  defines.MysqlType
	}{{"user_id", defines.MYSQL_TYPE_LONGLONG}, {"user_name", defines.MYSQL_TYPE_VARCHAR}, {"user_host", defines.MYSQL_TYPE_VARCHAR}} {
		column := &MysqlColumn{}
		column.SetName(col.name)
		column.SetColumnType(col.typ)
		mrs.AddColumn(column)
	}
	for _, row := range rows {
		mrs.AddRow(row)
	}
	return mrs
}

func newMrsForSetDefaultRoleGrantRows(rows [][]interface{}) *MysqlResultSet {
	mrs := &MysqlResultSet{}
	column := &MysqlColumn{}
	column.SetName("role_id")
	column.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
	mrs.AddColumn(column)
	for _, row := range rows {
		mrs.AddRow(row)
	}
	return mrs
}

type setDefaultRoleLifecycleExec struct {
	*backgroundExecTest
	closeCalls, sqlCountAtClose int
}

func (bh *setDefaultRoleLifecycleExec) Close() {
	bh.closeCalls++
	bh.sqlCountAtClose = len(bh.executedSQLs)
}

func assertSetDefaultRoleNoWrites(t *testing.T, bh *backgroundExecTest) {
	t.Helper()
	for _, sql := range bh.executedSQLs {
		require.NotContains(t, sql, "update mo_catalog.mo_user set default_role")
	}
}

func stubSetDefaultRoleBackgroundExec(t *testing.T, bh BackgroundExec, captured **BackgroundExecOption) *int {
	t.Helper()
	previous := NewBackgroundExec
	created := new(int)
	NewBackgroundExec = func(_ context.Context, _ FeSession, opts ...*BackgroundExecOption) BackgroundExec {
		*created++
		if captured != nil && len(opts) > 0 {
			*captured = opts[0]
		}
		return bh
	}
	t.Cleanup(func() { NewBackgroundExec = previous })
	return created
}
