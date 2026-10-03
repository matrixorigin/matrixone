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

	withLoadPrivilegeFixture(t, stmt, dbName, tableName, false,
		func(ses *Session, _ *backgroundExecTest) {
			_, err := buildPlanWithPrepareMode(
				context.Background(), ses, plan2.NewEmptyCompilerContext(), stmt, false)
			require.ErrorContains(t, err, "do not have privilege")
		})
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
