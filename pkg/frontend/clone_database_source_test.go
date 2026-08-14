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
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/stretchr/testify/require"

	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

type accountRecordingBackgroundExec struct {
	*backgroundExecTest
	accountID uint32
}

func (bt *accountRecordingBackgroundExec) Exec(ctx context.Context, sql string) error {
	accountID, err := defines.GetAccountId(ctx)
	if err == nil {
		bt.accountID = accountID
	}
	return bt.backgroundExecTest.Exec(ctx, sql)
}

type erroringBackgroundExec struct {
	*backgroundExecTest
	err error
}

func (bt *erroringBackgroundExec) Exec(ctx context.Context, sql string) error {
	if err := bt.backgroundExecTest.Exec(ctx, sql); err != nil {
		return err
	}
	return bt.err
}

func newStoredProcedureMetadataResultSet(rows [][]interface{}) *MysqlResultSet {
	mrs := &MysqlResultSet{}
	for _, name := range []string{"name", "args", "lang", "body", "sql_mode"} {
		column := &MysqlColumn{}
		column.SetName(name)
		column.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
		mrs.AddColumn(column)
	}
	for _, row := range rows {
		mrs.AddRow(row)
	}
	return mrs
}

func newUserDefinedFunctionMetadataResultSet(rows [][]interface{}) *MysqlResultSet {
	mrs := &MysqlResultSet{}
	for _, name := range []string{"name", "args", "retType", "body", "language", "sql_mode"} {
		column := &MysqlColumn{}
		column.SetName(name)
		column.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
		mrs.AddColumn(column)
	}
	for _, row := range rows {
		mrs.AddRow(row)
	}
	return mrs
}

func TestGetStoredProcedureInfosUsesSnapshotAndTenant(t *testing.T) {
	const dbName = "source_db"
	snapshot := &plan.Snapshot{
		TS:     &timestamp.Timestamp{PhysicalTime: 42},
		Tenant: &plan.SnapshotTenant{TenantID: 7},
	}
	querySQL := "select name, args, lang, body, sql_mode from mo_catalog.mo_stored_procedure {MO_TS = 42} where db = 'source_db' order by name"
	base := &backgroundExecTest{}
	base.init()
	base.sql2result[querySQL] = newStoredProcedureMetadataResultSet([][]interface{}{
		{"p_answer", "[]", "sql", "begin select 42; end", "PIPES_AS_CONCAT"},
	})
	bh := &accountRecordingBackgroundExec{backgroundExecTest: base}

	procedures, err := getStoredProcedureInfos(context.Background(), bh, snapshot, dbName)
	require.NoError(t, err)
	require.Equal(t, uint32(7), bh.accountID)
	require.Equal(t, []storedProcedureDefinition{{
		name:    "p_answer",
		args:    "[]",
		lang:    "sql",
		body:    "begin select 42; end",
		sqlMode: "PIPES_AS_CONCAT",
		dbName:  dbName,
	}}, procedures)

	t.Run("catalog query failure is propagated", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		wantErr := errors.New("stored procedure query failed")
		bh.sql2err[querySQL] = wantErr

		procedures, err := getStoredProcedureInfos(context.Background(), bh, snapshot, dbName)
		require.ErrorIs(t, err, wantErr)
		require.Nil(t, procedures)
	})
}

func TestGetUserDefinedFunctionInfosUsesSnapshotAndTenant(t *testing.T) {
	const dbName = "source_db"
	snapshot := &plan.Snapshot{
		TS:     &timestamp.Timestamp{PhysicalTime: 42},
		Tenant: &plan.SnapshotTenant{TenantID: 7},
	}
	querySQL := "select name, args, retType, body, language, sql_mode from mo_catalog.mo_user_defined_function {MO_TS = 42} where db = 'source_db' order by name"
	base := &backgroundExecTest{}
	base.init()
	base.sql2result[querySQL] = newUserDefinedFunctionMetadataResultSet([][]interface{}{
		{"f_answer", "{}", "int", "select 42", "sql", "PIPES_AS_CONCAT"},
	})
	bh := &accountRecordingBackgroundExec{backgroundExecTest: base}

	functions, err := getUserDefinedFunctionInfos(context.Background(), bh, snapshot, dbName)
	require.NoError(t, err)
	require.Equal(t, uint32(7), bh.accountID)
	require.Equal(t, []userDefinedFunctionDefinition{{
		name:    "f_answer",
		args:    "{}",
		retType: "int",
		body:    "select 42",
		lang:    "sql",
		sqlMode: "PIPES_AS_CONCAT",
		dbName:  dbName,
	}}, functions)

	t.Run("catalog query failure is propagated", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		wantErr := errors.New("user defined function query failed")
		bh.sql2err[querySQL] = wantErr

		functions, err := getUserDefinedFunctionInfos(context.Background(), bh, snapshot, dbName)
		require.ErrorIs(t, err, wantErr)
		require.Nil(t, functions)
	})
}

func TestGetCloneDatabaseRoutineInfosRespectsSubscriptionBoundary(t *testing.T) {
	t.Run("database source collects functions and procedures", func(t *testing.T) {
		const dbName = "source_db"
		functionQuerySQL := "select name, args, retType, body, language, sql_mode from mo_catalog.mo_user_defined_function where db = 'source_db' order by name"
		procedureQuerySQL := "select name, args, lang, body, sql_mode from mo_catalog.mo_stored_procedure where db = 'source_db' order by name"
		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2result[functionQuerySQL] = newUserDefinedFunctionMetadataResultSet([][]interface{}{
			{"f_answer", "{}", "int", "select 42", "sql", "PIPES_AS_CONCAT"},
		})
		bh.sql2result[procedureQuerySQL] = newStoredProcedureMetadataResultSet([][]interface{}{
			{"p_answer", "[]", "sql", "begin select 42; end", ""},
		})

		functions, procedures, err := getCloneDatabaseRoutineInfos(context.Background(), bh, nil, dbName, nil)
		require.NoError(t, err)
		require.Equal(t, []userDefinedFunctionDefinition{{
			name:    "f_answer",
			args:    "{}",
			retType: "int",
			body:    "select 42",
			lang:    "sql",
			sqlMode: "PIPES_AS_CONCAT",
			dbName:  dbName,
		}}, functions)
		require.Equal(t, []storedProcedureDefinition{{
			name:   "p_answer",
			args:   "[]",
			lang:   "sql",
			body:   "begin select 42; end",
			dbName: dbName,
		}}, procedures)
		require.Equal(t, []string{functionQuerySQL, procedureQuerySQL}, bh.executedSQLs)
	})

	t.Run("subscription source skips publisher routine catalogs", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()

		functions, procedures, err := getCloneDatabaseRoutineInfos(
			context.Background(),
			bh,
			nil,
			"publisher_db",
			&plan.SubscriptionMeta{DbName: "publisher_db", Tables: "t1"},
		)
		require.NoError(t, err)
		require.Empty(t, functions)
		require.Empty(t, procedures)
		require.Empty(t, bh.executedSQLs)
	})
}

func TestRestoreCloneDatabaseUserDefinedFunctions(t *testing.T) {
	ctx := context.Background()
	tenant := &TenantInfo{User: "root"}
	function := userDefinedFunctionDefinition{
		name:    "f_answer",
		args:    "{}",
		retType: "int",
		body:    "select 42",
		lang:    "sql",
		sqlMode: "PIPES_AS_CONCAT",
	}

	bh := &backgroundExecTest{}
	bh.init()
	require.NoError(t, restoreCloneDatabaseUserDefinedFunctions(ctx, bh, tenant, []userDefinedFunctionDefinition{function}, "target_db"))
	require.Len(t, bh.executedSQLs, 1)
	require.Contains(t, bh.executedSQLs[0], "insert into mo_catalog.mo_user_defined_function")
	require.Contains(t, bh.executedSQLs[0], "\"f_answer\"")
	require.Contains(t, bh.executedSQLs[0], "\"target_db\"")
	// SQL literal quoting is an implementation detail of EscapeFormat; preserve
	// the behavior under test rather than coupling this regression to its style.
	require.Contains(t, bh.executedSQLs[0], "PIPES_AS_CONCAT")
	require.NotContains(t, bh.executedSQLs, "begin;")

	failingBase := &backgroundExecTest{}
	failingBase.init()
	wantErr := errors.New("function persistence failed")
	failing := &erroringBackgroundExec{backgroundExecTest: failingBase, err: wantErr}
	require.ErrorIs(t, restoreCloneDatabaseUserDefinedFunctions(ctx, failing, tenant, []userDefinedFunctionDefinition{function}, "target_db"), wantErr)
	require.Len(t, failing.executedSQLs, 1)
}

func TestRestoreCloneDatabaseStoredProcedures(t *testing.T) {
	ctx := context.Background()
	tenant := &TenantInfo{User: "root"}
	procedure := storedProcedureDefinition{
		name:    "p_double",
		args:    `[{"ArgName":"input_value","InOutType":0},{"ArgName":"output_value","InOutType":1}]`,
		lang:    "sql",
		body:    "begin set output_value = input_value * 2; end",
		sqlMode: "PIPES_AS_CONCAT",
	}
	checkSQL := getSqlForCheckProcedureExistence(procedure.name, "target_db")

	bh := &backgroundExecTest{}
	bh.init()
	bh.sql2result[checkSQL] = newMrsForPasswordOfUser(nil)
	require.NoError(t, restoreCloneDatabaseStoredProcedures(ctx, bh, tenant, []storedProcedureDefinition{procedure}, "target_db"))
	require.Len(t, bh.executedSQLs, 2)
	require.Equal(t, checkSQL, bh.executedSQLs[0])
	require.Contains(t, bh.executedSQLs[1], procedure.args)
	require.Contains(t, bh.executedSQLs[1], "'PIPES_AS_CONCAT'")
	require.Contains(t, bh.executedSQLs[1], "'target_db'")
	require.NotContains(t, bh.executedSQLs[1], "create procedure")
	require.NotContains(t, bh.executedSQLs, "begin;")

	bh = &backgroundExecTest{}
	bh.init()
	wantErr := errors.New("lookup failed")
	bh.sql2err[checkSQL] = wantErr
	require.ErrorIs(t, restoreCloneDatabaseStoredProcedures(ctx, bh, tenant, []storedProcedureDefinition{procedure}, "target_db"), wantErr)
	require.Equal(t, []string{checkSQL}, bh.executedSQLs)
}

func TestRewriteCloneStoredProcedureBodies(t *testing.T) {
	procedures := []storedProcedureDefinition{{
		name: "p_source_reference",
		lang: "sql",
		body: "begin if exists (select 1 from source_db.control_t) then select id from source_db.control_t; else select 'source_db' as marker from other_db.control_t; end if; call source_db.p_inner(); end",
	}}

	rewritten, err := rewriteCloneStoredProcedureBodies(
		context.Background(), procedures, "source_db", "target_db", 1,
	)
	require.NoError(t, err)
	require.Len(t, rewritten, 1)
	require.Contains(t, rewritten[0].body, "from `target_db`.`control_t`")
	require.Contains(t, rewritten[0].body, "call target_db.p_inner()")
	require.Contains(t, rewritten[0].body, "from `other_db`.`control_t`")
	require.Contains(t, rewritten[0].body, "'source_db'")
	require.NotContains(t, rewritten[0].body, "source_db.control_t")
	require.Equal(t, procedures[0].body, "begin if exists (select 1 from source_db.control_t) then select id from source_db.control_t; else select 'source_db' as marker from other_db.control_t; end if; call source_db.p_inner(); end")
}

func TestCloneDatabaseSourceBranchTableCount(t *testing.T) {
	source := cloneDatabaseSource{
		srcTblInfos: []*tableInfo{
			{tblName: "regular"},
			{tblName: "foreign_key"},
			{tblName: "view", typ: view},
		},
	}

	require.Equal(t, int64(2), source.branchTableCount())
}

func TestValidateCloneDatabaseAccounts(t *testing.T) {
	tests := []struct {
		name     string
		accounts cloneDatabaseAccountResolution
		wantErr  string
	}{
		{
			name: "same tenant",
			accounts: cloneDatabaseAccountResolution{
				opAccountId: 1,
				toAccountId: 1,
			},
		},
		{
			name: "cross tenant without snapshot",
			accounts: cloneDatabaseAccountResolution{
				opAccountId: sysAccountID,
				toAccountId: 1,
			},
			wantErr: "clone database between different accounts need a snapshot",
		},
		{
			name: "non sys cross tenant with snapshot",
			accounts: cloneDatabaseAccountResolution{
				opAccountId: 1,
				toAccountId: 2,
				snapshot:    &plan.Snapshot{},
			},
			wantErr: "only sys can clone table to another account",
		},
		{
			name: "sys cross tenant with snapshot",
			accounts: cloneDatabaseAccountResolution{
				opAccountId: sysAccountID,
				toAccountId: 1,
				snapshot:    &plan.Snapshot{},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateCloneDatabaseAccounts(context.Background(), test.accounts)
			if test.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, test.wantErr)
		})
	}
}

func TestValidateCloneDatabaseSourceAccess(t *testing.T) {
	for _, database := range catalog.SystemDatabases {
		t.Run("non sys cannot clone "+database, func(t *testing.T) {
			err := validateCloneDatabaseSourceAccess(1, database)
			require.EqualError(t, err, "internal error: non-sys account cannot clone data from system database")
		})
	}
	t.Run("system database matching is case insensitive", func(t *testing.T) {
		err := validateCloneDatabaseSourceAccess(1, strings.ToUpper(catalog.MO_CATALOG))
		require.EqualError(t, err, "internal error: non-sys account cannot clone data from system database")
	})
	t.Run("sys can clone system catalog", func(t *testing.T) {
		require.NoError(t, validateCloneDatabaseSourceAccess(sysAccountID, catalog.MO_CATALOG))
	})
	t.Run("non sys can clone user database", func(t *testing.T) {
		require.NoError(t, validateCloneDatabaseSourceAccess(1, "user_database"))
	})
}

func TestLockDataBranchCloneDatabaseSourcesSkipsSourcesWithoutTables(t *testing.T) {
	ctx := context.WithValue(context.Background(), dataBranchCloneLockCtxKey{}, true)
	for _, source := range []cloneDatabaseSource{
		{},
		{srcTblInfos: []*tableInfo{{tblName: "view", typ: view}}},
	} {
		require.NoError(t, lockDataBranchCloneDatabaseSources(ctx, nil, nil, source))
	}
}

func TestCloneFkTableOrder(t *testing.T) {
	t.Run("acyclic dependencies retain topological order", func(t *testing.T) {
		parent := genKey("db", "parent")
		child := genKey("db", "child")
		order, hasCycle := cloneFkTableOrder(map[string][]string{
			child: {parent},
		})

		require.False(t, hasCycle)
		require.Equal(t, []string{parent, child}, order)
	})

	t.Run("cyclic dependencies use deterministic forward-reference order", func(t *testing.T) {
		a := genKey("db", "a")
		b := genKey("db", "b")
		order, hasCycle := cloneFkTableOrder(map[string][]string{
			a: {b},
			b: {a},
		})

		require.True(t, hasCycle)
		require.Equal(t, []string{a, b}, order)
	})
}

func TestCloneSnapshotTxnOperator(t *testing.T) {
	ctrl := gomock.NewController(t)
	outerTxn := mock_frontend.NewMockTxnOperator(ctrl)
	branchTxn := mock_frontend.NewMockTxnOperator(ctrl)
	ses := newFeatureLimitTestSession(t)
	ses.proc.Base.TxnOperator = outerTxn

	t.Run("normal clone keeps frontend transaction", func(t *testing.T) {
		bh := ses.InitBackExec(branchTxn, "", fakeDataSetFetcher2)
		require.Same(t, outerTxn, cloneSnapshotTxnOperator(ses, bh))
	})

	t.Run("data branch uses owning background transaction", func(t *testing.T) {
		bh := ses.InitBackExec(branchTxn, "", fakeDataSetFetcher2, &BackgroundExecOption{
			forcePessimisticRC: true,
		})
		require.Same(t, branchTxn, cloneSnapshotTxnOperator(ses, bh))
	})
}

func TestDataBranchCloneLockProcessUsesOwningBackgroundTxn(t *testing.T) {
	ctrl := gomock.NewController(t)
	outerTxn := mock_frontend.NewMockTxnOperator(ctrl)
	branchTxn := mock_frontend.NewMockTxnOperator(ctrl)
	ses := newFeatureLimitTestSession(t)
	ses.proc.Base.TxnOperator = outerTxn
	bh := ses.InitBackExec(branchTxn, "", fakeDataSetFetcher2, &BackgroundExecOption{
		forcePessimisticRC: true,
	})

	lockProc := newDataBranchCloneLockProcess(context.Background(), ses, bh)
	defer lockProc.Free()
	require.Same(t, branchTxn, lockProc.GetTxnOperator())
	require.Same(t, outerTxn, ses.proc.GetTxnOperator())
}

func TestCloneDatabaseTargetLockProcessUsesOwningBackgroundTxn(t *testing.T) {
	ctrl := gomock.NewController(t)
	outerTxn := mock_frontend.NewMockTxnOperator(ctrl)
	cloneTxn := mock_frontend.NewMockTxnOperator(ctrl)
	ses := newFeatureLimitTestSession(t)
	ses.proc.Base.TxnOperator = outerTxn
	bh := ses.InitBackExec(cloneTxn, "", fakeDataSetFetcher2, &BackgroundExecOption{
		forcePessimisticRC: true,
	})

	lockProc, err := newCloneDatabaseTargetLockProcess(context.Background(), ses, bh)
	require.NoError(t, err)
	defer lockProc.Free()
	require.Same(t, cloneTxn, lockProc.GetTxnOperator())
	require.Same(t, outerTxn, ses.proc.GetTxnOperator())
}
