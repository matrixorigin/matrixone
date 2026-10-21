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
		name string
		stmt *tree.SetDefaultRole
	}{
		{
			name: "all is unsupported",
			stmt: &tree.SetDefaultRole{Type: tree.SET_DEFAULT_ROLE_TYPE_ALL, Users: []*tree.User{{Username: "alice", Hostname: "%"}}},
		},
		{
			name: "multiple defaults are unsupported",
			stmt: &tree.SetDefaultRole{
				Type:  tree.SET_DEFAULT_ROLE_TYPE_NORMAL,
				Roles: []*tree.Role{{UserName: "writer"}, {UserName: "reader"}},
				Users: []*tree.User{{Username: "alice", Hostname: "%"}},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ses := newSetDefaultRoleTestSession(t, "acme", "alice", 7)
			bh := &backgroundExecTest{}
			bh.init()
			stubSetDefaultRoleBackgroundExec(t, bh, nil)
			require.Error(t, doSetDefaultRole(context.Background(), ses, tc.stmt))
			require.Empty(t, bh.executedSQLs)
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
	ctrl := gomock.NewController(t)
	bh := mock_frontend.NewMockBackgroundExec(ctrl)
	bh.EXPECT().ClearExecResultSet()
	bh.EXPECT().Exec(gomock.Any(), "select missing catalog row")
	bh.EXPECT().GetExecResultSet().Return([]interface{}{})

	result, err := querySetDefaultRoleResult(context.Background(), bh, "select missing catalog row")
	require.NoError(t, err)
	require.Nil(t, result)
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

func stubSetDefaultRoleBackgroundExec(t *testing.T, bh *backgroundExecTest, captured **BackgroundExecOption) {
	t.Helper()
	previous := NewBackgroundExec
	NewBackgroundExec = func(_ context.Context, _ FeSession, opts ...*BackgroundExecOption) BackgroundExec {
		if captured != nil && len(opts) > 0 {
			*captured = opts[0]
		}
		return bh
	}
	t.Cleanup(func() { NewBackgroundExec = previous })
}
