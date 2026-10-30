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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
)

func TestLoadTargetTableNameAndPrePlanStatementDiscovery(t *testing.T) {
	ses := newSes(nil, nil)
	ses.SetDatabaseName("session_db")

	qualified := tree.NewTableName("qualified_target", tree.ObjectNamePrefix{
		SchemaName:     "qualified_db",
		ExplicitSchema: true,
	}, nil)
	unqualified := tree.NewTableName("session_target", tree.ObjectNamePrefix{}, nil)

	for _, tc := range []struct {
		name      string
		stmt      *tree.Load
		wantDB    string
		wantTable string
		wantOK    bool
	}{
		{name: "nil statement", stmt: nil},
		{name: "missing table", stmt: &tree.Load{}},
		{
			name:      "missing table name",
			stmt:      &tree.Load{Table: tree.NewTableName("", tree.ObjectNamePrefix{SchemaName: "db"}, nil)},
			wantDB:    "",
			wantTable: "",
		},
		{
			name:      "qualified table",
			stmt:      &tree.Load{Table: qualified},
			wantDB:    "qualified_db",
			wantTable: "qualified_target",
			wantOK:    true,
		},
		{
			name:      "session database",
			stmt:      &tree.Load{Table: unqualified},
			wantDB:    "session_db",
			wantTable: "session_target",
			wantOK:    true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dbName, tableName, ok := getLoadTargetTableName(tc.stmt, ses)
			require.Equal(t, tc.wantDB, dbName)
			require.Equal(t, tc.wantTable, tableName)
			require.Equal(t, tc.wantOK, ok)
		})
	}

	load := &tree.Load{}
	for _, tc := range []struct {
		name string
		stmt tree.Statement
		want *tree.Load
	}{
		{name: "direct load", stmt: load, want: load},
		{name: "prepared load", stmt: &tree.PrepareStmt{Stmt: load}, want: load},
		{name: "explain load", stmt: tree.NewExplainStmt(load, "text"), want: load},
		{name: "explain analyze load", stmt: tree.NewExplainAnalyze(load, "text"), want: load},
		{name: "explain physical plan load", stmt: tree.NewExplainPhyPlan(load, "text"), want: load},
		{name: "unsupported statement", stmt: &tree.Select{}, want: nil},
		{name: "nil nested statement", stmt: &tree.PrepareStmt{}, want: nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Same(t, tc.want, loadStatementForPrePlanAuth(tc.stmt))
		})
	}
	require.Nil(t, loadStatementForPrePlanAuth(nil))
}

func TestAuthenticateLoadBeforePlanGuardPaths(t *testing.T) {
	ctx := context.Background()
	load := &tree.Load{}

	_, err := authenticateLoadBeforePlan(ctx, nil, load, "")
	require.NoError(t, err)

	ses := newSes(nil, nil)
	_, err = authenticateLoadBeforePlan(ctx, ses, nil, "")
	require.NoError(t, err)

	pu := getPu(ses.GetService())
	previous := pu.SV.SkipCheckPrivilege
	pu.SV.SkipCheckPrivilege = true
	t.Cleanup(func() { pu.SV.SkipCheckPrivilege = previous })
	_, err = authenticateLoadBeforePlan(ctx, ses, load, "")
	require.NoError(t, err)
}

func TestAuthenticateLoadBeforePlanUsesASTTarget(t *testing.T) {
	const (
		dbName    = "preplan_auth_db"
		tableName = "preplan_auth_table"
	)

	load := &tree.Load{Table: tree.NewTableName(tableName, tree.ObjectNamePrefix{
		SchemaName:     dbName,
		ExplicitSchema: true,
	}, nil)}

	for _, tc := range []struct {
		name  string
		stmt  tree.Statement
		allow bool
	}{
		{
			name:  "load denied",
			stmt:  load,
			allow: false,
		},
		{
			name:  "load allowed",
			stmt:  load,
			allow: true,
		},
		{
			name:  "insert target fallback",
			stmt:  &tree.Insert{Table: tree.NewTableName(tableName, tree.ObjectNamePrefix{SchemaName: dbName}, nil)},
			allow: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			withLoadPrivilegeFixture(t, tc.stmt, dbName, tableName, tc.allow,
				func(ses *Session, _ *backgroundExecTest) {
					ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeDatabaseAndTable(
						ses.GetTxnHandler().GetTxnCtx(), ses, tc.stmt, nil)
					require.NoError(t, err)
					require.Equal(t, tc.allow, ok)

					if loadStmt, ok := tc.stmt.(*tree.Load); ok {
						_, err = authenticateLoadBeforePlan(
							context.Background(), ses, loadStmt, "fallback_db")
						if tc.allow {
							require.NoError(t, err)
						} else {
							require.ErrorContains(t, err, "do not have privilege")
						}
					}
				})
		})
	}
}

func TestAuthenticateLoadBeforePlanPropagatesPrivilegeErrors(t *testing.T) {
	const (
		dbName    = "preplan_error_db"
		tableName = "preplan_error_table"
	)
	stmt := &tree.Load{Table: tree.NewTableName(tableName, tree.ObjectNamePrefix{
		SchemaName: dbName,
	}, nil)}
	wantErr := errors.New("pre-plan authorization failed")

	withLoadPrivilegeFixture(t, stmt, dbName, tableName, true,
		func(ses *Session, bh *backgroundExecTest) {
			bh.sql2err["begin;"] = wantErr
			_, err := authenticateLoadBeforePlan(context.Background(), ses, stmt, "")
			require.ErrorIs(t, err, wantErr)
		})

	for _, tc := range []struct {
		name         string
		stmt         tree.Statement
		forcePrepare bool
	}{
		{name: "direct", stmt: stmt},
		{name: "prepared rebuild", stmt: stmt, forcePrepare: true},
		{name: "prepare wrapper", stmt: &tree.PrepareStmt{Stmt: stmt}},
		{name: "explain wrapper", stmt: tree.NewExplainStmt(stmt, "text")},
		{name: "explain analyze wrapper", stmt: tree.NewExplainAnalyze(stmt, "text")},
		{name: "explain physical wrapper", stmt: tree.NewExplainPhyPlan(stmt, "text")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			withLoadPrivilegeFixture(t, stmt, dbName, tableName, false,
				func(ses *Session, _ *backgroundExecTest) {
					_, err := buildPlanWithPrepareMode(
						defines.AttachAccountId(context.Background(), ses.GetTenantInfo().GetTenantID()),
						ses, plan2.NewEmptyCompilerContext(nil), tc.stmt, tc.forcePrepare)
					require.ErrorContains(t, err, "do not have privilege")
				})
		})
	}
}

func TestBindSessionDatabaseForStatementPreservesBindings(t *testing.T) {
	for _, tc := range []struct {
		name, sessionDB, compilerDB, defaultDB string
	}{
		{"shared binding", "active_db", "active_db", "prepared_db"},
		{"prepared binding", "active_db", "prepared_db", "prepared_db"},
		{"independent binding", "active_db", "compiler_db", "prepared_db"},
		{"empty default", "active_db", "compiler_db", ""},
		{"current default", "active_db", "compiler_db", "active_db"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ses := newSes(nil, nil)
			ses.SetDatabaseName(tc.sessionDB)
			compiler := ses.GetTxnCompileCtx()
			compiler.SetDatabase(tc.compilerDB)
			restore := bindSessionDatabaseForStatement(ses, tc.defaultDB)
			if tc.defaultDB != "" && tc.defaultDB != tc.sessionDB {
				require.Equal(t, tc.defaultDB, ses.GetDatabaseName())
				require.Equal(t, tc.defaultDB, compiler.GetDatabase())
			}
			restore()
			require.Equal(t, tc.sessionDB, ses.GetDatabaseName())
			require.Equal(t, tc.compilerDB, compiler.GetDatabase())
		})
	}
}

func TestAuthenticateLoadBeforePlanPreservesDatabaseBindings(t *testing.T) {
	const dbName, tableName = "prepared_db", "binding_target"
	stmt := &tree.Load{Table: tree.NewTableName(tableName, tree.ObjectNamePrefix{}, nil)}
	wantErr := errors.New("authorization lookup failed")
	for _, tc := range []struct {
		name  string
		allow bool
		err   error
	}{
		{name: "allowed", allow: true},
		{name: "denied"},
		{name: "lookup error", allow: true, err: wantErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			withLoadPrivilegeFixture(t, stmt, dbName, tableName, tc.allow,
				func(ses *Session, bh *backgroundExecTest) {
					ses.SetDatabaseName("active_db")
					compiler := ses.GetTxnCompileCtx()
					compiler.SetDatabase(dbName)
					if tc.err != nil {
						bh.sql2err["begin;"] = tc.err
					}
					_, err := authenticateLoadBeforePlan(context.Background(), ses, stmt, dbName)
					switch {
					case tc.err != nil:
						require.ErrorIs(t, err, tc.err)
					case tc.allow:
						require.NoError(t, err)
					default:
						require.ErrorContains(t, err, "do not have privilege")
					}
					require.Equal(t, "active_db", ses.GetDatabaseName())
					require.Equal(t, dbName, compiler.GetDatabase())
				})
		})
	}
}

func withLoadPrivilegeFixture(
	t *testing.T,
	stmt tree.Statement,
	dbName string,
	tableName string,
	allowInsert bool,
	fn func(*Session, *backgroundExecTest),
) {
	t.Helper()
	priv := determinePrivilegeSetOfStatement(stmt)
	ses := newSes(priv, nil)
	ses.SetTenantInfo(&TenantInfo{
		Tenant:        "preplan_test_account",
		User:          "preplan_test_user",
		DefaultRole:   "preplan_test_role",
		TenantID:      1,
		UserID:        2,
		DefaultRoleID: 3,
	})
	ses.SetDatabaseName(dbName)

	roleID := int64(ses.GetTenantInfo().GetDefaultRoleID())
	sql2result := makeSql2ExecResult2(
		int(ses.GetTenantInfo().GetUserID()),
		[][]interface{}{{roleID, false}},
		nil,
		nil,
		nil,
		[]int{int(roleID)},
		[][][]interface{}{nil},
		nil,
		nil,
	)
	addTablePrivilegeResultsForRole(t, sql2result, roleID, dbName, tableName,
		map[PrivilegeType]bool{PrivilegeTypeInsert: allowInsert})

	bh := &backgroundExecTest{}
	bh.init()
	for sql, result := range sql2result {
		bh.sql2result[sql] = result
	}
	previous := NewBackgroundExec
	NewBackgroundExec = func(context.Context, FeSession, ...*BackgroundExecOption) BackgroundExec {
		return bh
	}
	t.Cleanup(func() { NewBackgroundExec = previous })

	fn(ses, bh)
}

func TestShowIndexIgnoredTableName(t *testing.T) {
	ses := newSes(nil, nil)
	ses.SetDatabaseName("session_db")

	for _, tc := range []struct {
		name      string
		stmt      tree.Statement
		wantDB    string
		wantTable string
		wantOK    bool
	}{
		{name: "nil statement"},
		{name: "other statement", stmt: &tree.ShowIndex{}},
		{name: "missing table", stmt: &tree.ShowIndexIgnored{}},
		{
			name:      "qualified table",
			stmt:      &tree.ShowIndexIgnored{TableName: tree.NewUnresolvedObjectName("qualified_db", "qualified_target")},
			wantDB:    "qualified_db",
			wantTable: "qualified_target",
			wantOK:    true,
		},
		{
			name:      "session database",
			stmt:      &tree.ShowIndexIgnored{TableName: tree.NewUnresolvedObjectName("session_target")},
			wantDB:    "session_db",
			wantTable: "session_target",
			wantOK:    true,
		},
		{
			name:      "show index status",
			stmt:      &tree.ShowIndexStatus{TableName: tree.NewUnresolvedObjectName("qualified_db", "status_target")},
			wantDB:    "qualified_db",
			wantTable: "status_target",
			wantOK:    true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dbName, tableName, ok := getShowIndexTableName(showIndexTableName(tc.stmt), ses)
			require.Equal(t, tc.wantDB, dbName)
			require.Equal(t, tc.wantTable, tableName)
			require.Equal(t, tc.wantOK, ok)
		})
	}

	ses.SetDatabaseName("")
	_, _, ok := getShowIndexTableName(tree.NewUnresolvedObjectName("t"), ses)
	require.False(t, ok)
}
