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
	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
	"math"
	"strings"
	"testing"
	"time"
)

func renameRows(data ...[]interface{}) *MysqlResultSet {
	r := &MysqlResultSet{Data: data}
	if len(data) > 0 {
		for range data[0] {
			r.Columns = append(r.Columns, &MysqlColumn{})
		}
	}
	return r
}
func seedAccountUser(bh *backgroundExecTest, id int64, name string) {
	row := renameRows([]interface{}{id, name, "localhost"})
	for _, predicate := range []string{"user_name = " + quoteSQLStringLiteral(name), fmt.Sprintf("user_id = %d", id)} {
		for _, lock := range []string{"", " for update"} {
			bh.sql2result["select user_id, user_name, user_host from mo_catalog.mo_user where "+predicate+" order by user_id"+lock+";"] = row
		}
	}
}
func renameStatement(names ...string) *tree.RenameUser {
	stmt := &tree.RenameUser{}
	for i := 0; i < len(names); i += 2 {
		stmt.Pairs = append(stmt.Pairs, tree.RenameUserPair{From: &tree.User{Username: names[i], Hostname: "%"}, To: &tree.User{Username: names[i+1], Hostname: "%"}})
	}
	return stmt
}
func newRenameFixture(t *testing.T) (*Session, *backgroundExecTest) {
	ses := newSetDefaultRoleTestSession(t, "tenant", "admin", 1)
	bh := &backgroundExecTest{}
	bh.init()
	seedAccountUser(bh, 1, "admin")
	seedAccountUser(bh, 8, "alice")
	bh.sql2result["select admin_name from mo_catalog.mo_account where account_id = 17;"] = renameRows([]interface{}{"admin"})
	// Explicit empty catalog results, never an implicit mock authentication bypass.
	bh.beforeExec = func(sql string) {
		if strings.HasPrefix(sql, "select ") {
			if _, ok := bh.sql2result[sql]; !ok {
				bh.sql2result[sql] = renameRows()
			}
		}
	}
	stubSetDefaultRoleBackgroundExec(t, bh, nil)
	return ses, bh
}
func TestRenameUserOrderedPairsAndRollback(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			ses, bh := newRenameFixture(t)
			old := bh.beforeExec
			bh.beforeExec = func(sql string) {
				old(sql)
				if sql == "update mo_catalog.mo_user set user_name = 'bob' where user_id = 8;" {
					seedAccountUser(bh, 8, "bob")
					bh.sql2result["select user_id, user_name, user_host from mo_catalog.mo_user where user_name = 'alice' order by user_id;"] = renameRows()
				}
			}
			if fail {
				bh.sql2err["update mo_catalog.mo_user set user_name = 'carol' where user_id = 8;"] = errors.New("injected second write")
			}
			err := doRenameUser(context.Background(), ses, renameStatement("alice", "bob", "bob", "carol"))
			if fail {
				require.Error(t, err)
				require.Contains(t, bh.executedSQLs, "rollback;")
			} else {
				require.NoError(t, err)
				require.Contains(t, bh.executedSQLs, "commit;")
			}
			require.Contains(t, bh.executedSQLs, "update mo_catalog.mo_user set user_name = 'bob' where user_id = 8;")
			for i, sql := range bh.executedSQLs {
				if strings.Contains(sql, "mo_account") {
					require.Equal(t, uint32(0), bh.executionAccountIDs[i])
				} else {
					require.Equal(t, uint32(17), bh.executionAccountIDs[i])
				}
			}
		})
	}
}
func TestRenameUserGuards(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*backgroundExecTest, *tree.RenameUser)
	}{
		{"missing", func(b *backgroundExecTest, s *tree.RenameUser) { s.Pairs[0].From.Username = "missing" }},
		{"occupied", func(b *backgroundExecTest, s *tree.RenameUser) { seedAccountUser(b, 9, "bob") }},
		{"host", func(b *backgroundExecTest, s *tree.RenameUser) { s.Pairs[0].To.Hostname = "remote" }},
		{"protected", func(b *backgroundExecTest, s *tree.RenameUser) { s.Pairs[0].To.Username = "root" }},
		{"definer", func(b *backgroundExecTest, s *tree.RenameUser) {
			b.sql2result["select definer from mo_catalog.mo_user_defined_function where definer in ('alice','bob') limit 1;"] = renameRows([]interface{}{"alice"})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ses, bh := newRenameFixture(t)
			s := renameStatement("alice", "bob")
			tc.setup(bh, s)
			require.Error(t, doRenameUser(context.Background(), ses, s))
			for _, sql := range bh.executedSQLs {
				require.False(t, strings.HasPrefix(sql, "update mo_catalog.mo_user"))
			}
		})
	}
}
func TestAccountUserBindingRevalidation(t *testing.T) {
	bh := &backgroundExecTest{}
	bh.init()
	seedAccountUser(bh, 8, "new")
	require.Error(t, lockAccountUserBindings(context.Background(), bh, []accountUserBinding{{8, "old", "localhost"}}))
	require.NoError(t, lockAccountUserBindings(context.Background(), bh, []accountUserBinding{{8, "new", "localhost"}}))
}
func TestCanonicalRoutineDefinerUsesCurrentID(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), 17)
	bh := &backgroundExecTest{}
	bh.init()
	seedAccountUser(bh, 8, "o'brien\\test")
	name, err := canonicalRoutineDefiner(ctx, bh, &TenantInfo{TenantID: 17, UserID: 8, User: "old"})
	require.NoError(t, err)
	require.Equal(t, "o'brien\\test", name)
	require.Contains(t, bh.executedSQLs[0], "user_id = 8 order by user_id for update")
}
func TestLoginBookkeepingRemainsOnResolvedID(t *testing.T) {
	bh := &backgroundExecTest{}
	bh.init()
	ctx := context.Background()
	for _, f := range []func(context.Context, int64, BackgroundExec) error{setUserUnlock, setUserLock, increaseLoginAttempts, updateLockTime} {
		require.NoError(t, f(ctx, 8, bh))
	}
	for _, sql := range bh.executedSQLs {
		require.Contains(t, sql, "where user_id = 8;")
		require.NotContains(t, sql, "user_name")
	}
	bh.sql2result[loginUserStateSQL(8, false)] = renameRows()
	_, err := executeLoginUserRead(ctx, bh, loginUserStateSQL(8, false))
	require.Error(t, err)
}
func TestCloneNoRoutineTransactionUnchanged(t *testing.T) {
	bh := &backgroundExecTest{}
	bh.init()
	require.NoError(t, validateCloneRoutineTransaction(context.Background(), bh, cloneDatabaseSource{}))
	require.Error(t, validateCloneRoutineTransaction(context.Background(), bh, cloneDatabaseSource{userDefinedFuncs: []userDefinedFunctionDefinition{{}}}))
	require.Error(t, validateCloneRoutineTransaction(context.Background(), bh, cloneDatabaseSource{storedProcedures: []storedProcedureDefinition{{}}}))
}

type renameTxnExec struct {
	*backgroundExecTest
	op TxnOperator
}

func (b *renameTxnExec) getTxn() TxnOperator { return b.op }
func TestCloneRoutineTransactionModesBeforeTargetCreation(t *testing.T) {
	for _, tc := range []struct {
		name      string
		mode      txn.TxnMode
		isolation txn.TxnIsolation
		ok        bool
	}{
		{"pessimistic RC", txn.TxnMode_Pessimistic, txn.TxnIsolation_RC, true},
		{"pessimistic SI", txn.TxnMode_Pessimistic, txn.TxnIsolation_SI, false},
		{"optimistic RC", txn.TxnMode_Optimistic, txn.TxnIsolation_RC, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			ses := newTestSession(t, ctrl)
			t.Cleanup(ses.Close)
			raw := &backgroundExecTest{}
			raw.init()
			seedAccountUser(raw, 0, "root")
			op := mock_frontend.NewMockTxnOperator(ctrl)
			op.EXPECT().Txn().Return(txn.TxnMeta{Mode: tc.mode, Isolation: tc.isolation}).AnyTimes()
			bh := &renameTxnExec{raw, op}
			source := &cloneDatabaseSource{srcResolveDBName: "source", opAccountId: sysAccountID, toAccountId: sysAccountID, userDefinedFuncs: []userDefinedFunctionDefinition{{name: "f", args: "{}", retType: "int", body: "select 42", lang: "sql", dbName: "source"}}}
			stmt := &tree.CloneDatabase{DstDatabase: tree.Identifier("destination"), SrcDatabase: tree.Identifier("source"), AtTsExpr: &tree.AtTimeStamp{}}
			_, err := handleCloneDatabaseWithSource(newTestExecCtx(defines.AttachAccountId(context.Background(), sysAccountID), ctrl), ses, bh, stmt, source)
			if tc.ok {
				require.NoError(t, err)
				require.Contains(t, raw.executedSQLs, "create database `destination`")
			} else {
				require.ErrorContains(t, err, "requires pessimistic RC")
				for _, sql := range raw.executedSQLs {
					require.False(t, strings.HasPrefix(sql, "create database"))
					require.False(t, strings.HasPrefix(sql, "insert "))
				}
			}
		})
	}
}
func TestCanonicalRoutineDefinerAuthenticatedSpecialIdentity(t *testing.T) {
	name := "rename_test_special"
	SetSpecialUser(name, []byte("test-only"))
	t.Cleanup(func() { specialUsers.Lock(); delete(specialUsers.users, name); specialUsers.Unlock() })
	for _, tc := range []struct {
		name                        string
		account, contextAccount, id uint32
		tenant                      string
		special                     bool
	}{
		{"authenticated sys", 0, 0, math.MaxUint32, "sys", true},
		{"normal sys same name", 0, 0, 8, "sys", false},
		{"normal tenant same name", 17, 17, 8, "tenant", false},
		{"cross context", 0, 17, math.MaxUint32, "sys", false},
		{"fake sys name", 17, 0, math.MaxUint32, "sys", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bh := &backgroundExecTest{}
			bh.init()
			tenant := &TenantInfo{Tenant: tc.tenant, TenantID: tc.account, UserID: tc.id, User: name}
			ctx := defines.AttachAccountId(context.Background(), tc.contextAccount)
			if tc.id == 8 {
				seedAccountUser(bh, 8, "canonical")
			}
			result, err := canonicalRoutineDefiner(ctx, bh, tenant)
			if tc.special {
				require.NoError(t, err)
				require.Equal(t, name, result)
				require.Empty(t, bh.executedSQLs)
			} else if tc.id == 8 {
				require.NoError(t, err)
				require.Equal(t, "canonical", result)
				require.Contains(t, bh.executedSQLs[0], "for update")
			} else {
				require.Error(t, err)
				require.NotEmpty(t, bh.executedSQLs)
			}
		})
	}
}

func TestAuthenticateUserNameReuseKeepsCredentialIdentity(t *testing.T) {
	for _, branch := range []string{"locked", "deleted", "bad password", "success"} {
		t.Run(branch, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			ses := newSes(determinePrivilegeSetOfStatement(renameStatement("alice", "bob")), ctrl)
			t.Cleanup(ses.Close)
			ses.rt = &Routine{}
			ses.rt.protocol.Store(&holder[MysqlRrWr]{value: ses.GetResponser().(*MysqlResp).mysqlRrWr})
			pu := getPu(ses.GetService())
			oldClient, oldEngine := pu.TxnClient, pu.StorageEngine
			t.Cleanup(func() { pu.TxnClient = oldClient; pu.StorageEngine = oldEngine })
			client := mock_frontend.NewMockTxnClient(ctrl)
			client.EXPECT().WaitLogTailAppliedAt(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, ts timestamp.Timestamp) (timestamp.Timestamp, error) { return ts, nil }).AnyTimes()
			pu.TxnClient = client
			pu.StorageEngine = &authenticationBarrierEngine{acquire: func(context.Context) (timestamp.Timestamp, error) { return timestamp.Timestamp{PhysicalTime: 100}, nil }}
			const accountID = 17008
			GSysVarsMgr.Lock()
			previousAccountVars, hadAccountVars := GSysVarsMgr.accountsGlobalSysVarsMap[accountID]
			GSysVarsMgr.Unlock()
			t.Cleanup(func() {
				GSysVarsMgr.Lock()
				if hadAccountVars {
					GSysVarsMgr.accountsGlobalSysVarsMap[accountID] = previousAccountVars
				} else {
					delete(GSysVarsMgr.accountsGlobalSysVarsMap, accountID)
				}
				GSysVarsMgr.Unlock()
			})
			previous := ExeSqlInBgSes
			t.Cleanup(func() { ExeSqlInBgSes = previous })
			ExeSqlInBgSes = func(context.Context, BackgroundExec, string) ([]ExecResult, error) {
				return []ExecResult{renameRows([]interface{}{ConnectionControlFailedConnectionsThreshold, "3"}, []interface{}{ConnectionControlMaxConnectionDelay, "1000"}, []interface{}{DefaultPasswordLifetime, "1"})}, nil
			}
			bh := &backgroundExecTest{}
			bh.init()
			stubSetDefaultRoleBackgroundExec(t, bh, nil)
			tenantSQL, e := getSqlForCheckTenant(context.Background(), "tenant")
			require.NoError(t, e)
			bh.sql2result[tenantSQL] = renameRows([]interface{}{int64(accountID), "tenant", "open", uint64(1), "", "1.0.0"})
			passwordSQL, e := getSqlForPasswordOfUser(context.Background(), "alice")
			require.NoError(t, e)
			bh.sql2result[passwordSQL] = newMrsForPasswordOfUser([][]interface{}{{int64(8), HashPassWord("password"), int64(publicRoleID)}})
			roleSQL, e := getSqlForRoleIdOfRole(context.Background(), publicRoleName)
			require.NoError(t, e)
			bh.sql2result[roleSQL] = renameRows([]interface{}{int64(publicRoleID)})
			membershipSQL, e := getSqlForRoleOfUser(context.Background(), 8, publicRoleName)
			require.NoError(t, e)
			bh.sql2result[membershipSQL] = renameRows([]interface{}{int64(publicRoleID)})
			status := userStatusUnlock
			if branch == "locked" {
				status = userStatusLockForever
			}
			bh.sql2result[loginUserStateSQL(8, false)] = renameRows([]interface{}{status, uint64(0), time.Now().UTC().Format("2006-01-02 15:04:05")})
			if branch == "deleted" {
				bh.sql2result[loginUserStateSQL(8, false)] = renameRows()
			}
			bh.sql2result[loginUserStateSQL(8, true)] = renameRows([]interface{}{time.Now().UTC().Format("2006-01-02 15:04:05")})
			switched := false
			bh.beforeExec = func(sql string) {
				if sql == roleSQL {
					switched = true
					bh.sql2result[passwordSQL] = newMrsForPasswordOfUser([][]interface{}{{int64(9), HashPassWord("other"), int64(publicRoleID)}})
				}
			}
			_, err := ses.AuthenticateUser(context.Background(), "tenant:alice:public", "", nil, nil, func([]byte, []byte, []byte) bool { return branch != "bad password" })
			require.True(t, switched)
			require.Equal(t, uint32(8), ses.GetTenantInfo().GetUserID())
			if branch == "success" {
				require.NoError(t, err)
				require.Contains(t, bh.executedSQLs, loginUserStateSQL(8, true))
			} else {
				require.Error(t, err)
				require.True(t, isAuthenticationRejected(err))
			}
			require.Contains(t, bh.executedSQLs, loginUserStateSQL(8, false))
			require.NotContains(t, bh.executedSQLs, getLockInfoOfUserSql("alice"))
			require.NotContains(t, bh.executedSQLs, getExpiredTimeOfUserSql("alice"))
			if branch == "deleted" {
				require.Contains(t, bh.executedSQLs, "rollback;")
			} else {
				require.Contains(t, bh.executedSQLs, "commit;")
			}
			if branch == "bad password" {
				require.Contains(t, bh.executedSQLs, "update mo_catalog.mo_user set login_attempts = login_attempts + 1 where user_id = 8;")
			}
		})
	}
}

func TestRoutineDefinerLiteralCreateAndReplace(t *testing.T) {
	for _, kind := range []string{"function", "procedure"} {
		for _, replace := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/%v", kind, replace), func(t *testing.T) {
				bh := &backgroundExecTest{}
				bh.init()
				name := "o'brien\\test"
				seedAccountUser(bh, 8, name)
				ctx := defines.AttachAccountId(context.Background(), 17)
				tenant := &TenantInfo{TenantID: 17, UserID: 8, User: "stale"}
				if kind == "function" {
					var id *int64
					if replace {
						x := int64(3)
						id = &x
					}
					require.NoError(t, persistUserDefinedFunction(ctx, bh, tenant, 1, userDefinedFunctionDefinition{name: "f", args: "{}", retType: "int", body: "select 1", lang: "sql", dbName: "db"}, id))
				} else {
					var results []ExecResult
					if replace {
						results = []ExecResult{renameRows([]interface{}{int64(3)})}
					}
					require.NoError(t, persistStoredProcedure(ctx, bh, tenant, storedProcedureDefinition{name: "p", args: "[]", lang: "sql", body: "begin select 1; end", dbName: "db"}, results))
				}
				sql := bh.executedSQLs[len(bh.executedSQLs)-1]
				require.Contains(t, sql, quoteSQLStringLiteral(name))
				require.NotContains(t, sql, "stale")
				statements, err := mysql.Parse(context.Background(), sql, 1)
				require.NoError(t, err)
				require.Len(t, statements, 1)
				statements[0].Free()
			})
		}
	}
}
func TestDropUserRejectsRenamedBinding(t *testing.T) {
	ses, bh := newRenameFixture(t)
	sql, e := getSqlForPasswordOfUser(context.Background(), "alice")
	require.NoError(t, e)
	bh.sql2result[sql] = newMrsForPasswordOfUser([][]interface{}{{int64(8), "password", int64(1)}})
	original := bh.beforeExec
	bh.beforeExec = func(sql string) {
		original(sql)
		if sql == "select user_id, user_name, user_host from mo_catalog.mo_user where user_id = 8 order by user_id for update;" {
			seedAccountUser(bh, 8, "bob")
		}
	}
	err := doDropUser(context.Background(), ses, &tree.DropUser{Users: []*tree.User{{Username: "alice", Hostname: "%"}}})
	require.Error(t, err)
	require.Contains(t, bh.executedSQLs, "rollback;")
	for _, sql := range bh.executedSQLs {
		require.False(t, strings.HasPrefix(sql, "delete "))
	}
}

func TestCloneProcedureRejectsBeforeSessionMutation(t *testing.T) {
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
	t.Cleanup(ses.Close)
	raw := &backgroundExecTest{}
	raw.init()
	op := mock_frontend.NewMockTxnOperator(ctrl)
	op.EXPECT().Txn().Return(txn.TxnMeta{Mode: txn.TxnMode_Pessimistic, Isolation: txn.TxnIsolation_SI}).AnyTimes()
	bh := &renameTxnExec{raw, op}
	before, err := ses.GetSessionSysVar("foreign_key_checks")
	require.NoError(t, err)
	source := &cloneDatabaseSource{srcResolveDBName: "source", opAccountId: sysAccountID, toAccountId: sysAccountID, hasFkCycle: true, storedProcedures: []storedProcedureDefinition{{name: "p", args: "[]", lang: "sql", body: "begin select 1; end", dbName: "source"}}}
	stmt := &tree.CloneDatabase{DstDatabase: "destination", SrcDatabase: "source", AtTsExpr: &tree.AtTimeStamp{}}
	_, err = handleCloneDatabaseWithSource(newTestExecCtx(defines.AttachAccountId(context.Background(), sysAccountID), ctrl), ses, bh, stmt, source)
	require.ErrorContains(t, err, "requires pessimistic RC")
	require.Empty(t, raw.executedSQLs)
	after, err := ses.GetSessionSysVar("foreign_key_checks")
	require.NoError(t, err)
	require.Equal(t, before, after)
}

// seedLegacyUserBindingFixtures makes the existing credential-row fixtures
// explicit for the new shared binding queries. It is opt-in per owning test;
// negative binding tests do not use it, and the executor has no auto-success rule.
func seedLegacyUserBindingFixtures(t *testing.T, bh *backgroundExecTest) {
	t.Helper()
	prefix := `select user_id, authentication_string, default_role from mo_catalog.mo_user where user_name = "`
	suffix := `" order by user_id;`
	for sql, result := range bh.sql2result {
		if !strings.HasPrefix(sql, prefix) || !strings.HasSuffix(sql, suffix) {
			continue
		}
		name := strings.TrimSuffix(strings.TrimPrefix(sql, prefix), suffix)
		if result == nil || result.GetRowCount() == 0 {
			bh.sql2result["select user_id, user_name, user_host from mo_catalog.mo_user where user_name = "+quoteSQLStringLiteral(name)+" order by user_id;"] = renameRows()
			continue
		}
		id, err := result.GetInt64(context.Background(), 0, 0)
		require.NoError(t, err)
		seedAccountUser(bh, id, name)
	}
}

func TestUserRoleWritersRejectRenamedBinding(t *testing.T) {
	for _, grant := range []bool{false, true} {
		t.Run(fmt.Sprint(grant), func(t *testing.T) {
			ses, bh := newRenameFixture(t)
			roleSQL, err := getSqlForRoleIdOfRole(context.Background(), "writer")
			require.NoError(t, err)
			bh.sql2result[roleSQL] = renameRows([]interface{}{int64(20)})
			passwordSQL, err := getSqlForPasswordOfUser(context.Background(), "alice")
			require.NoError(t, err)
			bh.sql2result[passwordSQL] = newMrsForPasswordOfUser([][]interface{}{{int64(8), "hash", int64(1)}})
			old := bh.beforeExec
			bh.beforeExec = func(sql string) {
				old(sql)
				if sql == "select user_id, user_name, user_host from mo_catalog.mo_user where user_id = 8 order by user_id for update;" {
					seedAccountUser(bh, 8, "renamed")
				}
			}
			roles := []*tree.Role{{UserName: "writer"}}
			users := []*tree.User{{Username: "alice", Hostname: "%"}}
			if grant {
				err = doGrantRole(context.Background(), ses, &tree.GrantRole{Roles: roles, Users: users})
			} else {
				err = doRevokeRole(context.Background(), ses, &tree.RevokeRole{Roles: roles, Users: users})
			}
			require.Error(t, err)
			require.Contains(t, bh.executedSQLs, "rollback;")
			for _, sql := range bh.executedSQLs {
				require.False(t, strings.HasPrefix(sql, "insert "))
				require.False(t, strings.HasPrefix(sql, "delete "))
			}
		})
	}
}

func TestRenameUserFailureFinalization(t *testing.T) {
	for _, failure := range []string{"query", "commit", "cancel", "deleted principal", "admin"} {
		t.Run(failure, func(t *testing.T) {
			ses, bh := newRenameFixture(t)
			lifecycle := &setDefaultRoleLifecycleExec{backgroundExecTest: bh}
			var opts *BackgroundExecOption
			stubSetDefaultRoleBackgroundExec(t, lifecycle, &opts)
			sentinel := errors.New("injected failure")
			switch failure {
			case "query":
				bh.sql2err["select user_id, user_name, user_host from mo_catalog.mo_user where user_name = 'alice' order by user_id;"] = sentinel
			case "commit":
				bh.sql2err["commit;"] = sentinel
			case "cancel":
				bh.sql2err["update mo_catalog.mo_user set user_name = 'bob' where user_id = 8;"] = context.Canceled
			case "deleted principal":
				bh.sql2result["select user_id, user_name, user_host from mo_catalog.mo_user where user_id = 1 order by user_id;"] = renameRows()
			case "admin":
				sql, e := getSqlForRoleOfUser(context.Background(), 8, accountAdminRoleName)
				require.NoError(t, e)
				bh.sql2result[sql] = renameRows([]interface{}{int64(accountAdminRoleID)})
			}
			err := doRenameUser(context.Background(), ses, renameStatement("alice", "bob"))
			require.Error(t, err)
			require.Contains(t, bh.executedSQLs, "rollback;")
			require.Equal(t, 1, lifecycle.closeCalls)
			require.True(t, opts.forcePessimisticRC)
			require.True(t, opts.cancelTxnCreateWithRequest)
			if failure == "query" || failure == "commit" {
				require.ErrorIs(t, err, sentinel)
			}
			if failure == "cancel" {
				require.ErrorIs(t, err, context.Canceled)
			}
		})
	}
}

func TestRenameUserDestinationLoginSafetyBeforeTransaction(t *testing.T) {
	for _, tc := range []struct{ name, destination string }{
		{"ASCII space", "new name"}, {"tab", "new\tname"},
		{"backtick", "new`name"}, {"single quote", "new'name"},
		{"double quote", "new\"name"}, {"backslash", "new\\name"},
		{"question mark", "new?label"}, {"NUL", "new\x00name"},
		{"encoded separator", "new%3Aname"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ses, bh := newRenameFixture(t)
			created := stubSetDefaultRoleBackgroundExec(t, bh, nil)
			// A later invalid pair must reject even the first otherwise-valid rename.
			err := doRenameUser(context.Background(), ses, renameStatement("alice", "bob", "other", tc.destination))
			require.Error(t, err)
			require.Zero(t, *created)
			require.Empty(t, bh.executedSQLs)
		})
	}
}

func TestRenameUserDestinationLoginSafetyPositiveControls(t *testing.T) {
	for _, name := range []string{"user_name", "user-name", "user.name", "用户", "user%name", "user%3aname"} {
		t.Run(name, func(t *testing.T) {
			// Exercise actual existing consumers, not just the new validator.
			for _, login := range []string{name, "tenant:" + name} {
				identity, err := GetTenantInfo(context.Background(), login)
				require.NoError(t, err)
				require.Equal(t, name, identity.GetUser())
			}
			lookup, err := getSqlForPasswordOfUser(context.Background(), name)
			require.NoError(t, err)
			statement, err := mysql.ParseOne(context.Background(), lookup, 1)
			require.NoError(t, err)
			defer statement.Free()
			ses, bh := newRenameFixture(t)
			require.NoError(t, doRenameUser(context.Background(), ses, renameStatement("alice", name)))
			require.Contains(t, bh.executedSQLs, fmt.Sprintf("update mo_catalog.mo_user set user_name = %s where user_id = 8;", quoteSQLStringLiteral(name)))
			require.Contains(t, bh.executedSQLs, "commit;")
		})
	}
}

func TestRenameUserLegacySourceCanMigrateToSafeName(t *testing.T) {
	// Source parsing/lookup is deliberately not narrowed with the new destination
	// grammar. These are legacy catalog labels, not new login promises.
	for _, name := range []string{"old name", "old\tname", "old`name", "o'brien", "old\"name", "old\\name", "old?label", "old%3Aname"} {
		t.Run(name, func(t *testing.T) {
			ses, bh := newRenameFixture(t)
			seedAccountUser(bh, 8, name)
			require.NoError(t, doRenameUser(context.Background(), ses, renameStatement(name, "safe_name")))
			require.Contains(t, bh.executedSQLs, "update mo_catalog.mo_user set user_name = 'safe_name' where user_id = 8;")
			require.Contains(t, bh.executedSQLs, "commit;")
		})
	}
}
