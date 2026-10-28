// Copyright 2025 Matrix Origin
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
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func parseSingleStatement(t *testing.T, sql string) tree.Statement {
	t.Helper()
	stmts, err := parsers.Parse(context.Background(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	return stmts[0]
}

func TestCantExecuteInReadOnlyTransactionErrorShape(t *testing.T) {
	err := moerr.NewCantExecuteInReadOnlyTransaction(context.Background())
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrCantExecuteInReadOnlyTransaction))
	require.Equal(t, uint16(1792), err.MySQLCode())
	require.Equal(t, moerr.ER_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION, err.MySQLCode())
	require.Equal(t, "25006", err.SqlState())
	require.Equal(t, "Cannot execute statement in a READ ONLY transaction.", err.Error())
	noCtxErr := moerr.NewCantExecuteInReadOnlyTransactionNoCtx()
	require.Equal(t, err.MySQLCode(), noCtxErr.MySQLCode())
	require.Equal(t, err.SqlState(), noCtxErr.SqlState())
	require.Equal(t, err.Error(), noCtxErr.Error())
}

func TestTxnReadOnlyAccessorAndNilResolution(t *testing.T) {
	// A nil handler is never read-only.
	var nilHandler *TxnHandler
	require.False(t, nilHandler.TxnReadOnly())

	th := &TxnHandler{}
	require.False(t, th.TxnReadOnly())
	th.txnReadOnly = true
	require.True(t, th.TxnReadOnly())

	// Resolution without a session or without an ExecCtx falls back to the
	// registered default 0, never to read-only.
	readOnly, err := resolveTxnReadOnlyAccessMode(nil)
	require.NoError(t, err)
	require.False(t, readOnly)
	readOnly, err = resolveTxnReadOnlyAccessMode(&ExecCtx{reqCtx: context.Background()})
	require.NoError(t, err)
	require.False(t, readOnly)
	readOnly, err = sessionTransactionReadOnly(&ExecCtx{reqCtx: context.Background()})
	require.NoError(t, err)
	require.False(t, readOnly)
}

func TestTransactionReadOnlyValueEnabled(t *testing.T) {
	require.False(t, transactionReadOnlyValueEnabled(nil))
	require.False(t, transactionReadOnlyValueEnabled(int64(0)))
	require.True(t, transactionReadOnlyValueEnabled(int64(1)))
	require.False(t, transactionReadOnlyValueEnabled(int32(0)))
	require.True(t, transactionReadOnlyValueEnabled(int32(1)))
	require.False(t, transactionReadOnlyValueEnabled(0))
	require.True(t, transactionReadOnlyValueEnabled(1))
	require.False(t, transactionReadOnlyValueEnabled(false))
	require.True(t, transactionReadOnlyValueEnabled(true))
	require.False(t, transactionReadOnlyValueEnabled("1"))
}

func TestResolveTxnReadOnlyAccessMode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	ctx := context.Background()

	mustBool := func(execCtx *ExecCtx) bool {
		readOnly, err := resolveTxnReadOnlyAccessMode(execCtx)
		require.NoError(t, err)
		return readOnly
	}

	newExecCtx := func(sql string, byBegin bool) *ExecCtx {
		stmt := parseSingleStatement(t, sql)
		return &ExecCtx{
			reqCtx: ctx,
			ses:    ses,
			stmt:   stmt,
			txnOpt: FeTxnOption{byBegin: byBegin},
		}
	}

	require.NoError(t, ses.SetSessionSysVar(ctx, transactionReadOnlySystemVariable, int64(0)))

	// Explicit START TRANSACTION modes decide the current transaction,
	// regardless of the session default.
	require.True(t, mustBool(newExecCtx("start transaction read only", true)))
	require.False(t, mustBool(newExecCtx("start transaction read write", true)))
	// A plain BEGIN inherits the session default.
	require.False(t, mustBool(newExecCtx("begin", true)))
	// Non-BEGIN statements (autocommit / implicit transaction) inherit it too.
	require.False(t, mustBool(newExecCtx("insert into t values (1)", false)))

	require.NoError(t, ses.SetSessionSysVar(ctx, transactionReadOnlySystemVariable, int64(1)))
	// START TRANSACTION READ WRITE overrides the READ ONLY session default.
	require.False(t, mustBool(newExecCtx("start transaction read write", true)))
	require.True(t, mustBool(newExecCtx("start transaction read only", true)))
	// A plain BEGIN under a READ ONLY session default is READ ONLY.
	require.True(t, mustBool(newExecCtx("begin", true)))
	require.True(t, mustBool(newExecCtx("insert into t values (1)", false)))

	require.NoError(t, ses.SetSessionSysVar(ctx, transactionReadOnlySystemVariable, int64(0)))
}

func TestStatementRequiresPersistentWriteAccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	ses.SetDatabaseName("testdb")

	writingSQLs := []string{
		"insert into t values (1)",
		"insert into testdb.t values (1)",
		"replace into t values (1)",
		"update t set a = 1",
		"update t, u set t.a = 1",
		"delete from t",
		"delete t from t join u",
		"truncate table t",
		"create table t2 (a int)",
		"create table t2 as select 1",
		"create index i on t (a)",
		"create database d2",
		"create view v as select 1",
		"create pitr p1 for database testdb range 1 'd'",
		"create sequence s1",
		"create stage st1 url='s3://load/files/'",
		"drop stage st1",
		"drop pitr p1",
		"alter pitr p1 range 2 'd'",
		"drop table t",
		"drop table t, u",
		"drop database d2",
		"drop view v",
		"drop index i on t",
		"drop sequence s1",
		"alter table t add column b int",
		"rename table t to t3",
		"insert all into t (a) values (1) into t2 (a) values (2) select 1 from dual",
		"create extension pg as myext file '/missing.so'",
		// EXPLAIN ANALYZE executes the wrapped statement; plain EXPLAIN does not.
		"explain analyze insert into t values (1)",
		"explain analyze delete from t",
		// Multi-table DELETE targets resolve through the FROM aliases.
		"delete x from t as x",
		"delete z from t as z",
		"delete t1 from t1 join t2 on t1.a = t2.a",
		"alter sequence s1 increment by 1",
		"load data infile 'x.csv' into table t",
	}
	for _, sql := range writingSQLs {
		stmt := parseSingleStatement(t, sql)
		require.True(t, statementRequiresPersistentWriteAccess(ses, stmt, "testdb"), sql)
	}

	nonWritingSQLs := []string{
		"select 1",
		"select * from t",
		"select * from t for update",
		"begin",
		"start transaction read only",
		"start transaction read write",
		"commit",
		"rollback",
		"set session transaction read only",
		"set session transaction_read_only = 1",
		"set net_write_timeout = 10",
		"show tables",
		"show create table t",
		"explain select * from t",
		"explain analyze select * from t",
		"grant select on table t.* to r1",
		"revoke alter account on account * from r1",
		"create user u1 identified by 'x'",
		"create role r1",
		"drop user u1",
		"alter account a1 comment 'x'",
		"use testdb",
		"prepare s from 'insert into t values (1)'",
		"deallocate prepare s",
		"kill query 1",
		"do 1",
		"analyze table t",
	}
	for _, sql := range nonWritingSQLs {
		stmt := parseSingleStatement(t, sql)
		require.False(t, statementRequiresPersistentWriteAccess(ses, stmt, "testdb"), sql)
	}
}

func TestStatementRequiresPersistentWriteAccessTemporaryTables(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	ses.SetDatabaseName("testdb")
	// Session-local temporary aliases shadow any persistent table of the
	// same name, exactly as resolution sees them at statement time.
	ses.AddTempTable("testdb", "tt", "temp_tt_real_name")
	ses.AddTempTable("testdb", "tt2", "temp_tt2_real_name")

	temporaryAllowedSQLs := []string{
		"create temporary table tt2 (a int)",
		"insert into tt values (1)",
		"replace into tt values (1)",
		"update tt set a = 1",
		"update tt as x set a = 1",
		"delete from tt",
		"drop table tt",
		"drop temporary table tt",
		"truncate table tt",
		"alter table tt add column b int",
		"load data infile 'x.csv' into table tt",
		// DELETE write targets that resolve to the temporary alias stay exempt.
		"delete tt from tt",
		"delete x from tt as x",
		"delete x, y from tt as x join tt2 as y on x.a = y.a",
	}
	for _, sql := range temporaryAllowedSQLs {
		stmt := parseSingleStatement(t, sql)
		require.False(t, statementRequiresPersistentWriteAccess(ses, stmt, "testdb"), sql)
	}

	// Mixed writes still touch persistent tables and must be gated.
	for _, sql := range []string{
		"insert into t values (1)",
		"update tt, t set tt.a = t.a",
		"drop table tt, t",
		// A DELETE target alias resolves to its FROM source first; a temporary
		// table sharing the alias' spelling must not shadow the persistent
		// victim (the x alias resolves to the persistent table here).
		"delete x from t as x",
		"delete tt, t from tt join t on tt.a = t.a",
	} {
		stmt := parseSingleStatement(t, sql)
		require.True(t, statementRequiresPersistentWriteAccess(ses, stmt, "testdb"), sql)
	}
}

func readOnlyGateExecCtx(ses FeSession, stmt tree.Statement, input *UserInput) *ExecCtx {
	return &ExecCtx{
		reqCtx: context.Background(),
		ses:    ses,
		stmt:   stmt,
		txnOpt: FeTxnOption{activeTxnAtStart: true, activeTxnAtStartKnown: true},
		input:  input,
	}
}

func TestEnforceReadOnlyTxnAccessMode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	ses.SetDatabaseName("testdb")
	ctx := context.Background()
	insert := parseSingleStatement(t, "insert into t values (1)")
	sel := parseSingleStatement(t, "select * from t")

	requireError1792 := func(err error) {
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrCantExecuteInReadOnlyTransaction))
		moErr := err.(*moerr.Error)
		require.Equal(t, uint16(1792), moErr.MySQLCode())
		require.Equal(t, "25006", moErr.SqlState())
	}

	// Current transaction READ ONLY: writes rejected, reads pass, and the
	// gate itself never mutates transaction state, so the transaction stays
	// usable for reads and COMMIT afterwards.
	ses.GetTxnHandler().txnReadOnly = true
	requireError1792(enforceReadOnlyTxnAccessMode(ses, readOnlyGateExecCtx(ses, insert, &UserInput{}), insert, "testdb"))
	// TRUNCATE is an implicit-commit DDL statement: the gate must reject it
	// while the active read-only transaction is still open, before the
	// implicit commit could happen (doComQuery wires the same gate in before
	// commitBeforeStatement for exactly this ordering).
	truncate := parseSingleStatement(t, "truncate table t")
	requireError1792(enforceReadOnlyTxnAccessMode(ses, readOnlyGateExecCtx(ses, truncate, &UserInput{}), truncate, "testdb"))
	rename := parseSingleStatement(t, "rename table t to t2")
	requireError1792(enforceReadOnlyTxnAccessMode(ses, readOnlyGateExecCtx(ses, rename, &UserInput{}), rename, "testdb"))
	require.NoError(t, enforceReadOnlyTxnAccessMode(ses, readOnlyGateExecCtx(ses, sel, &UserInput{}), sel, "testdb"))
	require.True(t, ses.GetTxnHandler().TxnReadOnly())

	// START TRANSACTION READ WRITE overrode the session default.
	ses.GetTxnHandler().txnReadOnly = false
	require.NoError(t, enforceReadOnlyTxnAccessMode(ses, readOnlyGateExecCtx(ses, insert, &UserInput{}), insert, "testdb"))

	// Autocommit path: no active transaction, session default READ ONLY.
	autocommitCtx := func(stmt tree.Statement) *ExecCtx {
		return &ExecCtx{reqCtx: ctx, ses: ses, stmt: stmt, txnOpt: FeTxnOption{}, input: &UserInput{}}
	}
	require.NoError(t, ses.SetSessionSysVar(ctx, transactionReadOnlySystemVariable, int64(1)))
	requireError1792(enforceReadOnlyTxnAccessMode(ses, autocommitCtx(insert), insert, "testdb"))
	require.NoError(t, enforceReadOnlyTxnAccessMode(ses, autocommitCtx(sel), sel, "testdb"))
	require.NoError(t, ses.SetSessionSysVar(ctx, transactionReadOnlySystemVariable, int64(0)))
	require.NoError(t, enforceReadOnlyTxnAccessMode(ses, autocommitCtx(insert), insert, "testdb"))

	// Internal statements and sessions are exempt.
	ses.GetTxnHandler().txnReadOnly = true
	internalInput := &UserInput{isInternalInput: true}
	require.NoError(t, enforceReadOnlyTxnAccessMode(ses, readOnlyGateExecCtx(ses, insert, internalInput), insert, "testdb"))
	require.NoError(t, enforceReadOnlyTxnAccessMode(&backSession{}, readOnlyGateExecCtx(&backSession{}, insert, &UserInput{}), insert, "testdb"))
	ses.isInternal = true
	require.NoError(t, enforceReadOnlyTxnAccessMode(ses, readOnlyGateExecCtx(ses, insert, &UserInput{}), insert, "testdb"))
	ses.isInternal = false
	migrationCtx := readOnlyGateExecCtx(ses, insert, &UserInput{})
	migrationCtx.inMigration = true
	require.NoError(t, enforceReadOnlyTxnAccessMode(ses, migrationCtx, insert, "testdb"))

	ses.GetTxnHandler().txnReadOnly = false
}

func parseProcedureBody(t *testing.T, body string) []tree.Statement {
	t.Helper()
	stmts, err := parsers.ParseWithSQLMode(context.Background(), dialect.MYSQL, body, 0, "")
	require.NoError(t, err)
	require.NotEmpty(t, stmts)
	return stmts
}

func TestCallerTxnReadOnly(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	ctx := context.Background()

	// Back executions without a user origin are trusted maintenance work.
	readOnly, err := callerTxnReadOnly(&backSession{})
	require.NoError(t, err)
	require.False(t, readOnly)

	// A user session without an active transaction follows its session default.
	readOnly, err = callerTxnReadOnly(ses)
	require.NoError(t, err)
	require.False(t, readOnly)
	require.NoError(t, ses.SetSessionSysVar(ctx, transactionReadOnlySystemVariable, int64(1)))
	readOnly, err = callerTxnReadOnly(ses)
	require.NoError(t, err)
	require.True(t, readOnly)
	// A background session acting on behalf of the user session inherits the
	// caller's mode through the upstream chain (procedure bodies, nested
	// procedure calls).
	readOnly, err = callerTxnReadOnly(&backSession{feSessionImpl: feSessionImpl{upstream: ses}})
	require.NoError(t, err)
	require.True(t, readOnly)
	require.NoError(t, ses.SetSessionSysVar(ctx, transactionReadOnlySystemVariable, int64(0)))
}

func TestEnforceReadOnlyOnProcedureBody(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	ses.SetDatabaseName("testdb")
	ctx := context.Background()

	writingBodies := []string{
		"begin insert into t values (2); end",
		"begin update t set a = 2; end",
		"begin delete from t; end",
		"begin if 1 = 1 then insert into t values (2); end if; end",
		"begin declare p1 int default 5; while p1 < 10 do update t set a = p1; set p1 = p1 + 1; end while; end",
		"begin declare p1 int default 10; declare v1 int default 5; repeat delete from t limit 1; set v1 = v1 + 1; until v1 > p1 end repeat; end",
		"begin label1: loop insert into t values (2); leave label1; end loop label1; end",
		"begin case 1 when 1 then truncate table t; end case; end",
	}
	// Inside a read-only caller every writing body is rejected with 1792.
	require.NoError(t, ses.SetSessionSysVar(ctx, transactionReadOnlySystemVariable, int64(1)))
	for _, body := range writingBodies {
		err := enforceReadOnlyOnProcedureBody(ctx, ses, parseProcedureBody(t, body), "testdb")
		require.Error(t, err, body)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrCantExecuteInReadOnlyTransaction), body)
	}

	readOnlyBodies := []string{
		"begin select count(*) from t; end",
		"begin set sql_mode = 'ANSI_QUOTES'; end",
		"begin call read_something(); end",
		"begin if 1 = 1 then select a from t; end if; end",
	}
	// Read-only procedures stay callable under a read-only caller.
	for _, body := range readOnlyBodies {
		require.NoError(t, enforceReadOnlyOnProcedureBody(ctx, ses, parseProcedureBody(t, body), "testdb"), body)
	}

	// Callers not in READ ONLY mode see no new restriction.
	require.NoError(t, ses.SetSessionSysVar(ctx, transactionReadOnlySystemVariable, int64(0)))
	for _, body := range writingBodies {
		require.NoError(t, enforceReadOnlyOnProcedureBody(ctx, ses, parseProcedureBody(t, body), "testdb"), body)
	}
	// Nested procedure execution on a background session inherits the
	// caller's read-only mode through the upstream chain.
	require.NoError(t, ses.SetSessionSysVar(ctx, transactionReadOnlySystemVariable, int64(1)))
	nested := &backSession{feSessionImpl: feSessionImpl{upstream: ses}}
	err := enforceReadOnlyOnProcedureBody(ctx, nested, parseProcedureBody(t, "begin insert into t values (2); end"), "testdb")
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrCantExecuteInReadOnlyTransaction))
	require.NoError(t, ses.SetSessionSysVar(ctx, transactionReadOnlySystemVariable, int64(0)))
}
