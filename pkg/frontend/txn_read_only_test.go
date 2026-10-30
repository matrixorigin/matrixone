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
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

// stubUpdateSourceColumns replaces catalog access for UPDATE assignment
// owner resolution with a fixed column map; unknown tables resolve with an
// error like the real compile context.
func stubUpdateSourceColumns(t *testing.T) {
	t.Helper()
	catalogColumns := map[string]map[string]struct{}{
		"t":   {"a": {}, "common": {}},
		"u":   {"a": {}},
		"t1":  {"a": {}},
		"t2":  {"a": {}},
		"tt":  {"v": {}, "common": {}},
		"tt2": {"a": {}},
	}
	stubs := gostub.Stub(&resolveSourceColumnNames, func(_ FeSession, _ string, tableName string, _ string) (map[string]struct{}, error) {
		cols, ok := catalogColumns[tableName]
		if !ok {
			return nil, moerr.NewNoSuchTable(context.Background(), "", tableName)
		}
		return cols, nil
	})
	// Classification under a valid catalog transaction resolves owners.
	resolvable := gostub.Stub(&catalogOwnershipResolvable, func(FeSession) bool { return true })
	t.Cleanup(resolvable.Reset)
	t.Cleanup(stubs.Reset)
}

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
	stubUpdateSourceColumns(t)

	writingSQLs := []string{
		"insert into t values (1)",
		"insert into testdb.t values (1)",
		"replace into t values (1)",
		"update t set a = 1",
		"update t as x set a = 1",
		"update t, u set t.a = 1",
		// Unqualified multi-source assignment owned by a persistent source,
		// genuinely ambiguous, or hidden behind an unresolvable persistent
		// source — all conservatively gated.
		"update t1, t2 set a = 1",
		"update ghost, t set a = 1",
		// Data branch catalog and table mutations.
		"data branch create table testdb.b1 from testdb.t",
		"data branch create database bdb1 from testdb",
		"data branch delete table testdb.b1",
		"data branch delete database bdb1",
		"data branch merge testdb.b1 into testdb.t",
		"data branch pick testdb.b1 into testdb.t keys (a)",
		"data branch diff testdb.t against testdb.u output as testdb.o1",
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
		// EXPLAIN PHYPLAN shares the executing branch with EXPLAIN ANALYZE.
		"explain phyplan insert into t values (1)",
		"explain phyplan delete from t",
		// Multi-table DELETE targets resolve through the FROM aliases.
		"delete x from t as x",
		"delete z from t as z",
		"delete t1 from t1 join t2 on t1.a = t2.a",
		// A schema qualifier does not exempt a target from alias resolution.
		"delete testdb.t from t as x",
		"delete testdb.t from t",
		"alter sequence s1 increment by 1",
		"load data infile 'x.csv' into table t",
	}
	for _, sql := range writingSQLs {
		stmt := parseSingleStatement(t, sql)
		require.Equal(t, writePersistent, classifyStatementWrite(ses, stmt, "testdb"), sql)
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
		"explain phyplan select * from t",
		// Data branch diffing without an output table only reads.
		"data branch diff testdb.t against testdb.u",
		"data branch diff testdb.t against testdb.u output count",
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
		require.Equal(t, writeAllowed, classifyStatementWrite(ses, stmt, "testdb"), sql)
	}
}

func TestStatementRequiresPersistentWriteAccessTemporaryTables(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	ses.SetDatabaseName("testdb")
	stubUpdateSourceColumns(t)
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
		// An UPDATE assigning only temporary-table columns stays allowed even
		// when a persistent table is merely read through a join: the planner
		// derives written tables from the SET targets and removes unmodified
		// sources. Unqualified assignments resolve to their unique owner, so
		// a column living only in the temporary table writes freely and a
		// column owned by nobody never writes (the planner rejects it).
		"update tt, t set tt.a = t.a",
		"update tt as x join t as y on x.a = y.a set x.a = 1",
		"update tt, t set v = 1",
		"update tt, t set nosuchcol = 1",
		// An assignment target the sources cannot name never writes.
		"update tt set noseuch.a = 1",
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
		// A schema-qualified target naming a temporary table stays exempt, and
		// an unqualified target resolves through the source schema.
		"delete testdb.tt from testdb.tt",
		"delete tt from testdb.tt",
		"delete x from (testdb.tt as x)",
		"delete tt from tt2 as tt",
	}
	for _, sql := range temporaryAllowedSQLs {
		stmt := parseSingleStatement(t, sql)
		require.Equal(t, writeAllowed, classifyStatementWrite(ses, stmt, "testdb"), sql)
	}

	// Mixed writes still touch persistent tables and must be gated.
	for _, sql := range []string{
		"insert into t values (1)",
		// A persistent assignment target writes even when joined with a
		// temporary source; an unqualified column owned by the persistent
		// source writes it, and a genuinely ambiguous column stays
		// conservatively gated. An assignment target the sources cannot name
		// never writes (the planner rejects it), so it stays allowed.
		"update tt, t set t.a = 1",
		"update tt as x join t as y on x.a = y.a set y.a = 1",
		"update tt, t set a = 1",
		"update tt, t set common = 1",
		"update tt, t set t.a = 1, v = 2",
		"drop table tt, t",
		// A DELETE target alias resolves to its FROM source first; a temporary
		// table sharing the alias' spelling must not shadow the persistent
		// victim (the x alias resolves to the persistent table here).
		"delete x from t as x",
		"delete tt, t from tt join t on tt.a = t.a",
		// A qualified target or same-named temporary table must not hide
		// a persistent source. Invalid targets remain conservatively gated.
		"delete testdb.tt from t as tt",
		"delete tt from otherdb.tt",
		"delete x from (otherdb.tt as x)",
		"delete tt from (select 1 as a) as tt",
	} {
		stmt := parseSingleStatement(t, sql)
		require.Equal(t, writePersistent, classifyStatementWrite(ses, stmt, "testdb"), sql)
	}
}

func TestDeleteReadOnlyClassificationReusesAST(t *testing.T) {
	ses := newTestSession(t, gomock.NewController(t))
	ses.SetDatabaseName("testdb")
	stmt := parseSingleStatement(t, "delete otherdb.x from t as x")
	defer stmt.Free()
	before := tree.String(stmt, dialect.MYSQL)

	// The alias names t in the effective database, not otherdb.x or testdb.x.
	ses.AddTempTable("testdb", "x", "temp_x")
	require.Equal(t, writePersistent, classifyStatementWrite(ses, stmt, "testdb"))
	ses.AddTempTable("testdb", "t", "temp_t")
	require.Equal(t, writeAllowed, classifyStatementWrite(ses, stmt, "testdb"))
	ses.removeTempTable("testdb", "t", "txnkey", "stmtkey")
	require.Equal(t, writePersistent, classifyStatementWrite(ses, stmt, "testdb"))
	require.Equal(t, before, tree.String(stmt, dialect.MYSQL))
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
	ses.isInternal = true
	require.NoError(t, enforceReadOnlyTxnAccessMode(ses, readOnlyGateExecCtx(ses, insert, &UserInput{}), insert, "testdb"))
	ses.isInternal = false
	migrationCtx := readOnlyGateExecCtx(ses, insert, &UserInput{})
	migrationCtx.inMigration = true
	require.NoError(t, enforceReadOnlyTxnAccessMode(ses, migrationCtx, insert, "testdb"))

	ses.GetTxnHandler().txnReadOnly = false
}

func TestUpdateOwnershipDeferral(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	ses.SetDatabaseName("testdb")
	ses.AddTempTable("testdb", "tt", "temp_tt_real_name")

	// Without a catalog transaction (autocommit admission before Create)
	// every unqualified multi-source assignment defers, qualified ones do
	// not, and EXPLAIN wrappers defer with their wrapped statement.
	resolvable := gostub.Stub(&catalogOwnershipResolvable, func(FeSession) bool { return false })
	defer resolvable.Reset()
	for _, sql := range []string{
		"update tt, t set v = 1",
		"update tt, t set a = 1",
		"update tt, t set common = 1",
		// Deferral wins even when an earlier qualified assignment is persistent.
		"update tt, t set t.a = 1, v = 2",
		"explain analyze update tt, t set a = 1",
		"explain phyplan update tt, t set a = 1",
	} {
		stmt := parseSingleStatement(t, sql)
		require.Equal(t, writeDeferred, classifyStatementWrite(ses, stmt, "testdb"), sql)
	}
	for _, sql := range []string{
		"update tt, t set t.a = 1",
		"update tt set v = 1",
		"update t set a = 1",
		"delete from t",
		"insert into t values (1)",
	} {
		stmt := parseSingleStatement(t, sql)
		require.NotEqual(t, writeDeferred, classifyStatementWrite(ses, stmt, "testdb"), sql)
	}

	// Deferred but not rejected up front: under a READ ONLY session default
	// the gate passes and arms the post-Create re-check instead of handing
	// back 1792 for a verdict it cannot yet classify; the bool-only
	// classifier stays conservative meanwhile.
	require.NoError(t, ses.SetSessionSysVar(context.Background(), transactionReadOnlySystemVariable, int64(1)))
	stmt := parseSingleStatement(t, "update tt, t set v = 1")
	execCtx := &ExecCtx{reqCtx: context.Background(), ses: ses, stmt: stmt, input: &UserInput{}}
	require.NoError(t, enforceReadOnlyTxnAccessMode(ses, execCtx, stmt, "testdb"))
	require.True(t, execCtx.deferReadOnlyOwnership)
	require.Equal(t, writeDeferred, classifyStatementWrite(ses, stmt, "testdb"))
	require.NoError(t, ses.SetSessionSysVar(context.Background(), transactionReadOnlySystemVariable, int64(0)))
}

func TestUpdateReadOnlyOwnershipLookupLifetime(t *testing.T) {
	ses := newTestSession(t, gomock.NewController(t))
	ses.SetDatabaseName("testdb")
	ses.AddTempTable("testdb", "tt", "temp_tt")
	stmt := parseSingleStatement(t, "update tt join t on tt.a=t.a set b=1,c=2")
	defer stmt.Free()
	resolvable := gostub.Stub(&catalogOwnershipResolvable, func(FeSession) bool { return true })
	defer resolvable.Reset()
	calls := map[string]int{}
	failFirst := false
	resolver := gostub.Stub(&resolveSourceColumnNames, func(_ FeSession, _, name, _ string) (map[string]struct{}, error) {
		calls[name]++
		if name == "tt" {
			return map[string]struct{}{"b": {}, "c": {}}, nil
		}
		if failFirst && calls[name] == 1 {
			return nil, moerr.NewInternalError(context.Background(), "catalog unavailable")
		}
		return map[string]struct{}{"a": {}}, nil
	})
	defer resolver.Reset()

	// READ WRITE needs no owner resolution after the access mode is stamped.
	ses.GetTxnHandler().txnReadOnly = false
	require.NoError(t, enforceReadOnlyTxnAccessMode(ses, readOnlyGateExecCtx(ses, stmt, &UserInput{}), stmt, "testdb"))
	require.Empty(t, calls)

	// READ ONLY resolves successful sources once per statement, not per SET.
	ses.GetTxnHandler().txnReadOnly = true
	for i := 1; i <= 2; i++ {
		require.NoError(t, enforceReadOnlyTxnAccessMode(ses, readOnlyGateExecCtx(ses, stmt, &UserInput{}), stmt, "testdb"))
		require.Equal(t, i, calls["tt"])
		require.Equal(t, i, calls["t"])
	}
	// Failed resolutions are retried for the next assignment, never cached.
	calls = map[string]int{}
	failFirst = true
	require.NoError(t, enforceReadOnlyTxnAccessMode(ses, readOnlyGateExecCtx(ses, stmt, &UserInput{}), stmt, "testdb"))
	require.Equal(t, 1, calls["tt"])
	require.Equal(t, 2, calls["t"])

	// A failed lookup on an earlier SET must not taint a later assignment.
	calls = map[string]int{}
	missing := parseSingleStatement(t, "update tt join t on tt.a=t.a set b=1,missing=2")
	defer missing.Free()
	require.NoError(t, enforceReadOnlyTxnAccessMode(ses, readOnlyGateExecCtx(ses, missing, &UserInput{}), missing, "testdb"))
	require.Equal(t, 1, calls["tt"])
	require.Equal(t, 2, calls["t"])
}

func TestEnforceReadOnlyTxnAccessModeBackSessionOrigin(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	ses.SetDatabaseName("testdb")
	ctx := context.Background()
	insert := parseSingleStatement(t, "insert into t values (1)")
	insertTT := parseSingleStatement(t, "insert into tt values (1)")
	dropTT := parseSingleStatement(t, "drop table tt")

	// A back session WITHOUT a user origin (daemon / maintenance work) stays
	// exempt even under a READ ONLY regime, as does internal input on a
	// user-origined back session.
	originLess := &backSession{}
	require.NoError(t, enforceReadOnlyTxnAccessMode(originLess, readOnlyGateExecCtx(originLess, insert, &UserInput{}), insert, "testdb"))
	back := &backSession{feSessionImpl: feSessionImpl{upstream: ses}}
	internalInput := &UserInput{isInternalInput: true}
	require.NoError(t, enforceReadOnlyTxnAccessMode(back, readOnlyGateExecCtx(back, insert, internalInput), insert, "testdb"))

	// With a user origin in READ ONLY mode, procedure-body statements are
	// gated at execution like the caller's own statements, and origin-less
	// freedom disappears.
	require.NoError(t, ses.SetSessionSysVar(ctx, transactionReadOnlySystemVariable, int64(1)))
	requireError1792 := func(err error) {
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrCantExecuteInReadOnlyTransaction))
	}
	requireError1792(enforceReadOnlyTxnAccessMode(back, readOnlyGateExecCtx(back, insert, &UserInput{}), insert, "testdb"))
	// The caller's temporary bindings are consulted live (backSession
	// delegates GetTempTable upstream): a temporary target stays writable...
	ses.AddTempTable("testdb", "tt", "temp_tt_real_name")
	require.NoError(t, enforceReadOnlyTxnAccessMode(back, readOnlyGateExecCtx(back, insertTT, &UserInput{}), insertTT, "testdb"))
	require.NoError(t, enforceReadOnlyTxnAccessMode(back, readOnlyGateExecCtx(back, dropTT, &UserInput{}), dropTT, "testdb"))
	// ...and when a body removes the binding first, the same statement shape
	// now resolves to the persistent table behind it and is rejected. This is
	// the stale-static-check bypass: classification must happen at execution
	// against the bindings live at that moment.
	ses.removeTempTable("testdb", "tt", "txnkey", "stmtkey")
	requireError1792(enforceReadOnlyTxnAccessMode(back, readOnlyGateExecCtx(back, insertTT, &UserInput{}), insertTT, "testdb"))

	// Callers not in READ ONLY mode see no restriction through a back
	// session either.
	require.NoError(t, ses.SetSessionSysVar(ctx, transactionReadOnlySystemVariable, int64(0)))
	require.NoError(t, enforceReadOnlyTxnAccessMode(back, readOnlyGateExecCtx(back, insert, &UserInput{}), insert, "testdb"))
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

	// Only dynamic SQL is rejected at call time: EXECUTE resolves its
	// statement at run time outside the per-statement read-only gate, so a
	// body carrying PREPARE/EXECUTE fails closed under a READ ONLY caller,
	// even inside nested control flow.
	dynamicBodies := []string{
		"begin prepare s from 'insert into t values (2)'; execute s; end",
		"begin prepare s from 'select 1'; end",
		"begin set @dyn_v = 'insert into t values (2)'; prepare s from @dyn_v; execute s; end",
		"begin execute s; end",
		"begin if 1 = 1 then prepare s from 'select 1'; execute s; end if; end",
	}
	require.NoError(t, ses.SetSessionSysVar(ctx, transactionReadOnlySystemVariable, int64(1)))
	for _, body := range dynamicBodies {
		err := enforceReadOnlyOnProcedureBody(ctx, ses, parseProcedureBody(t, body), "testdb")
		require.Error(t, err, body)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrCantExecuteInReadOnlyTransaction), body)
	}

	// Statically resolvable statements are deliberately NOT pre-checked at
	// call time: the caller-aware gate enforces each of them at execution
	// against the temporary bindings live at that moment, which keeps
	// persistent writes out without false-rejecting temporary-only bodies.
	staticBodies := []string{
		"begin insert into t values (2); end",
		"begin update t set a = 2; end",
		"begin delete from t; end",
		"begin if 1 = 1 then insert into t values (2); end if; end",
		"begin declare p1 int default 5; while p1 < 10 do update t set a = p1; set p1 = p1 + 1; end while; end",
		"begin declare p1 int default 10; declare v1 int default 5; repeat delete from t limit 1; set v1 = v1 + 1; until v1 > p1 end repeat; end",
		"begin label1: loop insert into t values (2); leave label1; end loop label1; end",
		"begin case 1 when 1 then truncate table t; end case; end",
		"begin explain phyplan insert into t values (2); end",
		"begin select count(*) from t; end",
		"begin set sql_mode = 'ANSI_QUOTES'; end",
		"begin call read_something(); end",
		"begin if 1 = 1 then select a from t; end if; end",
		"begin deallocate prepare s; end",
	}
	for _, body := range staticBodies {
		require.NoError(t, enforceReadOnlyOnProcedureBody(ctx, ses, parseProcedureBody(t, body), "testdb"), body)
	}

	// Callers not in READ ONLY mode see no restriction at all.
	require.NoError(t, ses.SetSessionSysVar(ctx, transactionReadOnlySystemVariable, int64(0)))
	for _, body := range dynamicBodies {
		require.NoError(t, enforceReadOnlyOnProcedureBody(ctx, ses, parseProcedureBody(t, body), "testdb"), body)
	}
	// A nested procedure on a background session inherits the caller's
	// read-only mode through the upstream chain: dynamic SQL fails closed.
	require.NoError(t, ses.SetSessionSysVar(ctx, transactionReadOnlySystemVariable, int64(1)))
	nested := &backSession{feSessionImpl: feSessionImpl{upstream: ses}}
	err := enforceReadOnlyOnProcedureBody(ctx, nested, parseProcedureBody(t, "begin execute s; end"), "testdb")
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrCantExecuteInReadOnlyTransaction))
	require.NoError(t, ses.SetSessionSysVar(ctx, transactionReadOnlySystemVariable, int64(0)))
}

func TestReadOnlyGateRemapEffectiveDatabase(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	ses.SetDatabaseName("r7_src")
	ctx := context.Background()

	remap := map[string]string{
		tree.NewCStr("r7_src", parserLowerCaseTableNames(ses)).Compare(): "r7_dst",
	}
	remapCtx := func(stmt tree.Statement) *ExecCtx {
		return &ExecCtx{reqCtx: ctx, ses: ses, stmt: stmt, txnOpt: FeTxnOption{}, input: &UserInput{}, remapDb: remap}
	}
	plainCtx := func(stmt tree.Statement) *ExecCtx {
		return &ExecCtx{reqCtx: ctx, ses: ses, stmt: stmt, txnOpt: FeTxnOption{}, input: &UserInput{}}
	}
	require.NoError(t, ses.SetSessionSysVar(ctx, transactionReadOnlySystemVariable, int64(1)))
	defer func() {
		_ = ses.SetSessionSysVar(ctx, transactionReadOnlySystemVariable, int64(0))
	}()
	require1792 := func(err error) {
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrCantExecuteInReadOnlyTransaction))
	}

	// The helper mirrors TxnCompilerContext.DefaultDatabase: the threaded
	// default keeps its prepared-binding meaning, an empty default falls back
	// to the session database, and only then does remapdb redirect; without
	// an installed policy the answer is unchanged.
	require.Equal(t, "r7_dst", effectiveDefaultDatabaseWithRemap(ses, remapCtx(nil), ""))
	require.Equal(t, "r7_dst", effectiveDefaultDatabaseWithRemap(ses, remapCtx(nil), "r7_src"))
	require.Equal(t, "other_src_db", effectiveDefaultDatabaseWithRemap(ses, remapCtx(nil), "other_src_db"))
	require.Equal(t, "r7_src", effectiveDefaultDatabaseWithRemap(ses, plainCtx(nil), "r7_src"))

	// Direction (i) leak: the temporary binding is keyed at the remap SOURCE,
	// so the un-remapped check pronounced the write temporary-only. Binding
	// redirects to the destination where no temporary binding exists, so the
	// statement writes persistently and must be gated.
	ses.AddTempTable("r7_src", "leak", "leak_temp_real")
	insertLeak := parseSingleStatement(t, "insert into leak values (1)")
	require1792(enforceReadOnlyTxnAccessMode(ses, remapCtx(insertLeak), insertLeak, ""))
	// Same verdict when the binding database arrives threaded as the
	// prepared binding database captured by PREPARE (the EXECUTE path).
	require1792(enforceReadOnlyTxnAccessMode(ses, remapCtx(insertLeak), insertLeak, "r7_src"))
	// Without the remap policy nothing changes: source-keyed temporary
	// bindings keep the write temporary-only.
	require.NoError(t, enforceReadOnlyTxnAccessMode(ses, plainCtx(insertLeak), insertLeak, ""))

	// Direction (ii) false 1792: the temporary binding lives at the remap
	// DESTINATION, exactly where binding resolves the unqualified target.
	// The write is temporary-only and must stay admitted.
	ses.AddTempTable("r7_dst", "okt", "okt_temp_real")
	insertOkt := parseSingleStatement(t, "insert into okt values (1)")
	require.NoError(t, enforceReadOnlyTxnAccessMode(ses, remapCtx(insertOkt), insertOkt, ""))
	require.NoError(t, enforceReadOnlyTxnAccessMode(ses, remapCtx(insertOkt), insertOkt, "r7_src"))

	// A prepared statement retains its threaded database after USE changes the session.
	ses.SetDatabaseName("other")
	require.Equal(t, "r7_dst", effectiveDefaultDatabaseWithRemap(ses, remapCtx(nil), "r7_src"))
	require.NoError(t, enforceReadOnlyTxnAccessMode(ses, remapCtx(insertOkt), insertOkt, "r7_src"))
	require.Equal(t, "other", effectiveDefaultDatabaseWithRemap(ses, remapCtx(nil), ""))
	require.Equal(t, "other", effectiveDefaultDatabaseWithRemap(ses, nil, ""))
	ses.sesSysVars.Set("lower_case_table_names", int64(2))
	mode2 := &ExecCtx{ses: ses, remapDb: map[string]string{"r7_src": "Destination"}}
	require.Equal(t, "Destination", effectiveDefaultDatabaseWithRemap(ses, mode2, "R7_SRC"))
	mode2.remapDb["r7_src"] = ""
	require.Empty(t, effectiveDefaultDatabaseWithRemap(ses, mode2, "R7_SRC"))
	ses.SetDatabaseName("")
	emptyKey := &ExecCtx{ses: ses, remapDb: map[string]string{"": "unexpected"}}
	require.Empty(t, effectiveDefaultDatabaseWithRemap(ses, emptyKey, ""))
}

func TestSequenceMutationClassification(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	ses.SetDatabaseName("testdb")
	ctx := context.Background()

	// Read-classified carriers of sequence-mutating builtins must be gated:
	// projection, expression containers, sub-selects, CTE bodies, window
	// frames, ORDER BY, LIMIT and user-variable assignment.
	for _, sql := range []string{
		"select nextval('s')",
		"select nextval(seq_col) from t",
		"select setval('s', 7)",
		"select setval('s', 7, false)",
		"select NEXTVAL('s')",
		"select 1 + nextval('s')",
		"select coalesce(nextval('s'), 0)",
		"select case when nextval('s') > 1 then 1 else 0 end",
		"select a from t where b = nextval('s')",
		"select a from t where b in (select nextval('s'))",
		"select a from (select nextval('s') as b) x",
		"with c as (select nextval('s') as b) select b from c",
		"select a from t join (select nextval('s') as b) x on a = x.b",
		"select sum(a) over (order by nextval('s')) from t",
		"select a from t order by nextval('s')",
		"select a from t limit nextval('s')",
		"explain analyze select nextval('s')",
		"explain phyplan select nextval('s')",
		"set @v = nextval('s')",
		`(select nextval('s'))`,
		"select setval('s', nextval('s'))",
	} {
		stmt := parseSingleStatement(t, sql)
		require.Equal(t, writePersistent, classifyStatementWrite(ses, stmt, "testdb"), sql)
	}

	// Session-local sequence reads and ordinary reads stay exempt.
	for _, sql := range []string{
		"select currval('s')",
		"select lastval()",
		"select curr_val('s')",
		"select a from t",
		"set @v = 1",
		"set @v = currval('s')",
	} {
		stmt := parseSingleStatement(t, sql)
		require.Equal(t, writeAllowed, classifyStatementWrite(ses, stmt, "testdb"), sql)
	}

	// Gate-level: a NEXTVAL read-carrier raises 1792 under the session
	// default, then a CURRVAL read still runs — the rejection leaves the
	// transaction usable.
	require.NoError(t, ses.SetSessionSysVar(ctx, transactionReadOnlySystemVariable, int64(1)))
	defer func() {
		_ = ses.SetSessionSysVar(ctx, transactionReadOnlySystemVariable, int64(0))
	}()
	nextval := parseSingleStatement(t, "select nextval('s')")
	execCtx := &ExecCtx{reqCtx: ctx, ses: ses, stmt: nextval, txnOpt: FeTxnOption{}, input: &UserInput{}}
	err := enforceReadOnlyTxnAccessMode(ses, execCtx, nextval, "testdb")
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrCantExecuteInReadOnlyTransaction))
	currval := parseSingleStatement(t, "select currval('s')")
	execCtx2 := &ExecCtx{reqCtx: ctx, ses: ses, stmt: currval, txnOpt: FeTxnOption{}, input: &UserInput{}}
	require.NoError(t, enforceReadOnlyTxnAccessMode(ses, execCtx2, currval, "testdb"))
}

func TestReadOnlyGateStampsProcessAccessMode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	ses.SetDatabaseName("testdb")
	ctx := context.Background()
	sel := parseSingleStatement(t, "select a from t")
	proc := testutil.NewProcess(t)

	// Admitted read-classified statements still stamp the effective caller
	// mode so post-binding mutations (sequence builtins via temp-target DML
	// or stored views) can enforce it at their boundary.
	require.NoError(t, ses.SetSessionSysVar(ctx, transactionReadOnlySystemVariable, int64(1)))
	execCtx := &ExecCtx{reqCtx: ctx, ses: ses, stmt: sel, txnOpt: FeTxnOption{}, input: &UserInput{}, proc: proc}
	require.NoError(t, enforceReadOnlyTxnAccessMode(ses, execCtx, sel, "testdb"))
	require.Equal(t, true, proc.ReadOnlyTxnAccessMode())

	require.NoError(t, ses.SetSessionSysVar(ctx, transactionReadOnlySystemVariable, int64(0)))
	require.NoError(t, enforceReadOnlyTxnAccessMode(ses, execCtx, sel, "testdb"))
	require.Equal(t, false, proc.ReadOnlyTxnAccessMode())

	// The active transaction's declared mode wins over the session default
	// changed after it started (MySQL).
	ses.GetTxnHandler().txnReadOnly = true
	activeCtx := &ExecCtx{reqCtx: ctx, ses: ses, stmt: sel, input: &UserInput{}, proc: proc,
		txnOpt: FeTxnOption{activeTxnAtStart: true, activeTxnAtStartKnown: true}}
	require.NoError(t, enforceReadOnlyTxnAccessMode(ses, activeCtx, sel, "testdb"))
	require.Equal(t, true, proc.ReadOnlyTxnAccessMode())
	ses.GetTxnHandler().txnReadOnly = false
}

// Retain fail-closed DELETE behavior when the classifier returns a disposition,
// not the boolean used before UPDATE ownership was consolidated.
func TestDeleteReadOnlyMissingSourceDisposition(t *testing.T) {
	for _, sql := range []string{"delete missing from t", "delete d from (select 1 as a) as d"} {
		t.Run(sql, func(t *testing.T) {
			stmt := parseSingleStatement(t, sql)
			defer stmt.Free()
			require.Equal(t, writePersistent, classifyStatementWrite(nil, stmt, "testdb"))
		})
	}
}
