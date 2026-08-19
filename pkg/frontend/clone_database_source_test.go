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
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/stretchr/testify/require"

	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
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

// udfCreationGate models the database-row lock that serializes concurrent
// CREATE FUNCTION statements. Each fake executor owns one transaction; the
// gate is held from SELECT ... FOR UPDATE through COMMIT or ROLLBACK.
type udfCreationGate struct {
	txnMu            sync.Mutex
	callsMu          sync.Mutex
	lockCalls        int
	exists           bool
	inserts          int
	firstLocked      chan struct{}
	secondWaiting    chan struct{}
	allowFirstCommit chan struct{}
}

type lockingUDFBackgroundExec struct {
	*backgroundExecTest
	gate      *udfCreationGate
	holdsLock bool
	result    ExecResult
}

func (bt *lockingUDFBackgroundExec) Exec(ctx context.Context, sql string) error {
	bt.currentSql = sql
	bt.executedSQLs = append(bt.executedSQLs, sql)

	switch {
	case sql == "begin;":
		return nil
	case strings.HasPrefix(sql, "select dat_id from mo_catalog.mo_database"):
		bt.gate.callsMu.Lock()
		bt.gate.lockCalls++
		call := bt.gate.lockCalls
		bt.gate.callsMu.Unlock()

		if call == 1 {
			bt.gate.txnMu.Lock()
			close(bt.gate.firstLocked)
			<-bt.gate.allowFirstCommit
		} else {
			close(bt.gate.secondWaiting)
			bt.gate.txnMu.Lock()
		}
		bt.holdsLock = true
		bt.result = newMrsForPasswordOfUser([][]interface{}{{int64(1)}})
		return nil
	case strings.HasPrefix(sql, "select function_id from mo_catalog.mo_user_defined_function"):
		if bt.gate.exists {
			bt.result = newMrsForPasswordOfUser([][]interface{}{{int64(1)}})
		} else {
			bt.result = newMrsForPasswordOfUser(nil)
		}
		return nil
	case strings.HasPrefix(sql, "insert into mo_catalog.mo_user_defined_function"):
		bt.gate.exists = true
		bt.gate.inserts++
		return nil
	case sql == "commit;" || sql == "rollback;":
		if bt.holdsLock {
			bt.holdsLock = false
			bt.gate.txnMu.Unlock()
		}
		return nil
	default:
		return bt.backgroundExecTest.Exec(ctx, sql)
	}
}

func (bt *lockingUDFBackgroundExec) GetExecResultSet() []interface{} {
	return []interface{}{bt.result}
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

	t.Run("imported package functions are rejected before target creation", func(t *testing.T) {
		const dbName = "source_db"
		functionQuerySQL := "select name, args, retType, body, language, sql_mode from mo_catalog.mo_user_defined_function where db = 'source_db' order by name"
		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2result[functionQuerySQL] = newUserDefinedFunctionMetadataResultSet([][]interface{}{{
			"f_imported", "{}", "int", `{"handler":"f_imported","import":true,"body":"shared:udf/f_imported.py"}`, "python", "",
		}})

		functions, procedures, err := getCloneDatabaseRoutineInfos(context.Background(), bh, nil, dbName, nil)
		require.ErrorContains(t, err, "imported python function f_imported is not supported")
		require.Nil(t, functions)
		require.Nil(t, procedures)
		require.Equal(t, []string{functionQuerySQL}, bh.executedSQLs)
	})
}

func TestResolveCloneDatabaseRoutineTenantUsesTargetAdministrator(t *testing.T) {
	const targetAccountID = uint32(7)
	query := "select account_name, admin_name from mo_catalog.mo_account where account_id = 7"
	base := &backgroundExecTest{}
	base.init()
	base.sql2result[query] = newMrsForRestoreStringRows(
		[]string{"account_name", "admin_name"}, [][]interface{}{{"acc1", "root1"}},
	)
	bh := &accountRecordingBackgroundExec{backgroundExecTest: base}

	tenant, err := resolveCloneDatabaseRoutineTenant(
		context.Background(), bh, getDefaultAccount(), targetAccountID,
	)
	require.NoError(t, err)
	require.Equal(t, targetAccountID, tenant.GetTenantID())
	require.Equal(t, "acc1", tenant.GetTenant())
	require.Equal(t, "root1", tenant.GetUser())
	require.Equal(t, uint32(accountAdminRoleID), tenant.GetDefaultRoleID())
	require.Equal(t, uint32(sysAccountID), bh.accountID)

	sameAccount := &TenantInfo{Tenant: "acc1", User: "owner", TenantID: targetAccountID}
	got, err := resolveCloneDatabaseRoutineTenant(context.Background(), bh, sameAccount, targetAccountID)
	require.NoError(t, err)
	require.Same(t, sameAccount, got)
	require.Equal(t, []string{query}, bh.executedSQLs)
}

func TestInitFunctionSerializesConcurrentExactSignatures(t *testing.T) {
	const databaseName = "clone_target"
	gate := &udfCreationGate{
		firstLocked:      make(chan struct{}),
		secondWaiting:    make(chan struct{}),
		allowFirstCommit: make(chan struct{}),
	}
	newExecutor := func() *lockingUDFBackgroundExec {
		base := &backgroundExecTest{}
		base.init()
		return &lockingUDFBackgroundExec{backgroundExecTest: base, gate: gate}
	}
	firstExec := newExecutor()
	secondExec := newExecutor()

	originalNewBackgroundExec := NewBackgroundExec
	var execMu sync.Mutex
	executors := []BackgroundExec{firstExec, secondExec}
	NewBackgroundExec = func(context.Context, FeSession, ...*BackgroundExecOption) BackgroundExec {
		execMu.Lock()
		defer execMu.Unlock()
		executor := executors[0]
		executors = executors[1:]
		return executor
	}
	t.Cleanup(func() { NewBackgroundExec = originalNewBackgroundExec })

	locale := ""
	functionStatement := func() *tree.CreateFunction {
		return &tree.CreateFunction{
			Name: tree.NewFuncName("f_exact", tree.ObjectNamePrefix{
				SchemaName:     tree.Identifier(databaseName),
				ExplicitSchema: true,
			}),
			ReturnType: tree.NewReturnType(&tree.T{InternalType: tree.InternalType{
				Family:       tree.IntFamily,
				FamilyString: "INT",
				Width:        24,
				Locale:       &locale,
				Oid:          uint32(defines.MYSQL_TYPE_INT24),
			}}),
			Body:     "1",
			Language: string(tree.SQL),
		}
	}
	tenant := &TenantInfo{
		Tenant:        "acc1",
		User:          "root1",
		TenantID:      7,
		UserID:        1,
		DefaultRole:   accountAdminRoleName,
		DefaultRoleID: accountAdminRoleID,
	}
	newSession := func() *Session {
		return &Session{feSessionImpl: feSessionImpl{tenant: tenant}}
	}
	ctx := defines.AttachAccountId(context.Background(), tenant.GetTenantID())
	errs := make(chan error, 2)

	go func() {
		errs <- InitFunction(newSession(), &ExecCtx{reqCtx: ctx}, tenant, functionStatement())
	}()
	<-gate.firstLocked
	go func() {
		errs <- InitFunction(newSession(), &ExecCtx{reqCtx: ctx}, tenant, functionStatement())
	}()
	<-gate.secondWaiting
	close(gate.allowFirstCommit)

	firstErr := <-errs
	secondErr := <-errs
	require.NoError(t, firstErr)
	require.Error(t, secondErr)
	require.Equal(t, 1, gate.inserts)
	require.Equal(t, 2, gate.lockCalls)
}

func TestRestoreCloneDatabaseUserDefinedFunctions(t *testing.T) {
	ctx := context.Background()
	tenant := &TenantInfo{User: "root1", DefaultRoleID: accountAdminRoleID}
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
	require.Contains(t, bh.executedSQLs[0], "\"f_answer\",2")
	require.Contains(t, bh.executedSQLs[0], "\"target_db\"")
	require.Contains(t, bh.executedSQLs[0], "\"root1\"")
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
	tenant := &TenantInfo{User: "root1", DefaultRoleID: accountAdminRoleID}
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
	require.Contains(t, bh.executedSQLs[1], "'root1'")
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
		body: "begin if exists (select 1 from source_db.control_t) then select id from source_db.control_t; else select 'source_db' as marker from other_db.control_t; end if; call SOURCE_DB.p_inner(); end",
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
	require.Equal(t, procedures[0].body, "begin if exists (select 1 from source_db.control_t) then select id from source_db.control_t; else select 'source_db' as marker from other_db.control_t; end if; call SOURCE_DB.p_inner(); end")
}

func TestRewriteCloneStoredProcedureBodiesRewritesNestedControlFlow(t *testing.T) {
	procedures := []storedProcedureDefinition{{
		name: "p_nested_source_references",
		lang: "sql",
		body: `begin
			if exists (select 1 from source_db.if_table) then
				select id from source_db.then_table;
			elseif exists (select 1 from source_db.elif_table) then
				call source_db.p_inner();
			else
				select id from source_db.else_table;
			end if;
			case 1
				when 1 then select id from source_db.case_table;
				else select id from source_db.case_else_table;
			end case;
		end`,
	}}

	rewritten, err := rewriteCloneStoredProcedureBodies(
		context.Background(), procedures, "source_db", "target_db", 1,
	)
	require.NoError(t, err)
	require.Len(t, rewritten, 1)
	for _, table := range []string{
		"if_table", "then_table", "elif_table", "else_table", "case_table", "case_else_table",
	} {
		require.Contains(t, rewritten[0].body, "`target_db`.`"+table+"`")
		require.NotContains(t, rewritten[0].body, "source_db."+table)
	}
	require.Contains(t, rewritten[0].body, "call target_db.p_inner()")
	require.Equal(t, procedures[0].body, `begin
			if exists (select 1 from source_db.if_table) then
				select id from source_db.then_table;
			elseif exists (select 1 from source_db.elif_table) then
				call source_db.p_inner();
			else
				select id from source_db.else_table;
			end if;
			case 1
				when 1 then select id from source_db.case_table;
				else select id from source_db.case_else_table;
			end case;
		end`)
}

func TestRewriteCloneStoredProcedureBodiesRewritesExecutableWrappers(t *testing.T) {
	procedures := []storedProcedureDefinition{{
		name: "p_wrapped_source_references",
		lang: "sql",
		body: `begin
			explain select * from source_db.explain_table;
			explain analyze select * from source_db.analyze_table;
			lock tables source_db.lock_table read;
			check table source_db.check_table;
			show table status from source_db;
			show sequences from source_db;
			show tables from source_db;
			show triggers from source_db;
			show create database source_db;
			show table number from source_db;
			show databases;
			show variables;
			show status;
			use source_db;
		end`,
	}}

	rewritten, err := rewriteCloneStoredProcedureBodies(
		context.Background(), procedures, "source_db", "target_db", 1,
	)
	require.NoError(t, err)
	require.Len(t, rewritten, 1)
	for _, table := range []string{"explain_table", "analyze_table", "lock_table", "check_table"} {
		require.Contains(t, rewritten[0].body, "`target_db`.`"+table+"`")
	}
	for _, show := range []string{
		"show table status from target_db",
		"show sequences from target_db",
		"show tables from target_db",
		"show triggers from target_db",
		"show create database target_db",
		"show table number from target_db",
		"show databases",
		"show variables",
		"show status",
		"use target_db",
	} {
		require.Contains(t, rewritten[0].body, show)
	}
	require.NotContains(t, rewritten[0].body, "source_db.")
	require.NotContains(t, rewritten[0].body, "from source_db")
	require.NotContains(t, rewritten[0].body, "use source_db")
}

func TestRewriteCloneRoutineBodiesPreserveOpaqueLanguagesAndRejectInvalidSQL(t *testing.T) {
	ctx := context.Background()

	t.Run("opaque routine languages are not parsed or rewritten", func(t *testing.T) {
		procedures := []storedProcedureDefinition{{
			name: "p_external", lang: "javascript", body: "source_db.table",
		}}
		functions := []userDefinedFunctionDefinition{{
			name: "f_external", lang: "javascript", body: "select source_db.table",
		}}

		rewrittenProcedures, err := rewriteCloneStoredProcedureBodies(ctx, procedures, "source_db", "target_db", 1)
		require.NoError(t, err)
		require.Equal(t, procedures, rewrittenProcedures)

		rewrittenFunctions, err := rewriteCloneUserDefinedFunctionBodies(ctx, functions, "source_db", "target_db", 1)
		require.NoError(t, err)
		require.Equal(t, functions, rewrittenFunctions)
	})

	t.Run("invalid SQL aborts the clone", func(t *testing.T) {
		_, err := rewriteCloneStoredProcedureBodies(ctx, []storedProcedureDefinition{{
			name: "p_invalid", lang: "sql", body: "select from",
		}}, "source_db", "target_db", 1)
		require.Error(t, err)

		_, err = rewriteCloneUserDefinedFunctionBodies(ctx, []userDefinedFunctionDefinition{{
			name: "f_invalid", lang: "sql", body: "select from",
		}}, "source_db", "target_db", 1)
		require.Error(t, err)
	})

	t.Run("unhandled executable statement aborts the clone", func(t *testing.T) {
		_, err := rewriteCloneStoredProcedureBodies(ctx, []storedProcedureDefinition{{
			name: "p_unhandled", lang: "sql", body: "begin create database source_db; end",
		}}, "source_db", "target_db", 1)
		require.ErrorContains(t, err, "cannot be safely remapped")
	})

	t.Run("identity mapping does not parse the body", func(t *testing.T) {
		body, err := rewriteCloneSQLRoutineBody(ctx, "not valid SQL", "", "source_db", "source_db", 1)
		require.NoError(t, err)
		require.Equal(t, "not valid SQL", body)
	})
}

func TestRewriteCloneUserDefinedFunctionBodies(t *testing.T) {
	functions := []userDefinedFunctionDefinition{
		{
			name: "f_source_table",
			lang: "sql",
			body: "select count(*) from SOURCE_DB.control_t where id = $1",
		},
		{
			name: "f_expression",
			lang: "sql",
			body: "$1 + 1",
		},
		{
			name: "f_uppercase_select",
			lang: "sql",
			body: "SELECT count(*) FROM SOURCE_DB.uppercase_t",
		},
	}

	rewritten, err := rewriteCloneUserDefinedFunctionBodies(
		context.Background(), functions, "source_db", "target_db", 1,
	)
	require.NoError(t, err)
	require.Len(t, rewritten, 3)
	require.Contains(t, rewritten[0].body, "from `target_db`.`control_t`")
	require.Contains(t, rewritten[0].body, "$1")
	require.Equal(t, "$1 + 1", rewritten[1].body)
	require.Contains(t, rewritten[2].body, "from `target_db`.`uppercase_t`")
	require.NotContains(t, rewritten[2].body, "SOURCE_DB.uppercase_t")
	require.Equal(t, "select count(*) from SOURCE_DB.control_t where id = $1", functions[0].body)
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
