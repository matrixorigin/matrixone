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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

// Existing cursor/long-data fixtures also need a real replayable scalar plan;
// an empty PrepareStmt is no longer evidence that SQL preserves its identity.
func crc32MigrationTestPlan(t testing.TB, sql string) *plan.Plan {
	t.Helper()
	stmt := tree.NewPrepareString("migration_scalar", sql)
	defer stmt.Free()
	p, err := plan2.BuildPlan(plan2.NewEmptyCompilerContext(newPlanTestProcess(t)), stmt, false)
	require.NoError(t, err)
	return p
}

func TestCRC32PrepareMigrationSQLClassification(t *testing.T) {
	for _, tc := range []struct {
		name, sql string
		safe      bool
	}{
		{"text", "select crc32('abc')", true},
		{"numeric", "select crc32(42)", true},
		{"scalar parameter", "select char_length(?), @digest", true},
		{"ordinary comparison and logic", "select (? = 1 and not (2 = 3)) or (4 xor 5)", true},
		{"ordinary null tests", "select ? is null, @digest is not null", true},
		{"ordinary range", "select ? between 1 and 3", true},
		{"searched case", "select case when ? = 1 then 2 else 3 end", true},
		{"simple case", "select case ? when 1 then 2 end", true},
		{"comparison hides CRC", "select crc32(cast(? as json)) = 1", false},
		{"pruned logic hides CRC", "select 0 and crc32(cast(? as json))", false},
		{"null test hides CRC", "select crc32(cast(? as json)) is null", false},
		{"range bound hides CRC", "select 1 between 0 and crc32(cast(? as json))", false},
		{"case value hides CRC", "select case crc32(cast(? as json)) when 1 then 2 end", false},
		{"case condition hides CRC", "select case when crc32(cast(? as json)) then 1 else 2 end", false},
		{"pruned case branch hides CRC", "select case when 1 then 2 else crc32(cast(? as json)) end", false},
		{"literal and comment are data", "select 'crc32(cast(? as json))' /* crc32(?) */", true},
		{"SQL prepare text", "prepare p from 'select crc32(42)'", true},
		{"native prepare text", "prepare p from select crc32('abc')", true},
		{"JSON", `select crc32(cast('{"t1":"a"}' as json))`, false},
		{"ANY", "select crc32(?)", false},
		{"nested", "select coalesce(crc32(cast(? as json)),0)", false},
		{"SQL prepare JSON", "prepare p from 'select crc32(cast(? as json))'", false},
		{"native prepare JSON", "prepare p from select crc32(cast(? as json))", false},
		{"prepare variable unresolved", "prepare p from @sql", false},
		{"column text not provable", "select crc32(s) from t", false},
		{"view hides identity", "select v from crc_view", false},
		{"UDF hides identity", "select udf_crc_wrapper(?)", false},
		{"qualified function", "select db.crc32('abc')", false},
		{"subquery", "select (select crc32(cast(? as json)))", false},
		{"pruned branch", "select coalesce(1,crc32(cast(? as json)))", false},
		{"malformed", "select crc32(", false},
		{"multiple statements", "select 1; select crc32(?)", false},
		{"empty", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.safe, crc32ReplaySQLSafe(context.Background(), tc.sql, "", true))
		})
	}
}

func TestCRC32PreparedMigrationPinsSourceAndPreservesExecute(t *testing.T) {
	const sql = `select crc32(cast('{"t1":"a"}' as json))`
	ses, prepared, cw, execCtx := newPreparedExecuteEnvForSQL(t, 151, sql)
	t.Cleanup(ses.Close)
	rt := &Routine{mc: newMigrateController()}
	rt.setSession(ses)
	original := prepared.PreparePlan.String()
	resp := &query.MigrateConnFromResponse{}
	err := rt.migrateConnectionFrom(resp)
	require.True(t, moerr.IsMoErrCode(err, moerr.OkExpectedNotSafeToStartTransfer))
	require.Empty(t, resp.PrepareStmts)
	require.Equal(t, original, prepared.PreparePlan.String())
	require.True(t, rt.mc.tryBeginRequest(), "pinning must release lifecycle admission")
	func() {
		defer rt.mc.endRequest()
		_, executionPlan, stmt, _, owned, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepared.Name)
		require.NoError(t, err)
		if owned {
			defer stmt.Free()
		}
		q := executionPlan.GetQuery()
		expr := q.Nodes[q.Steps[len(q.Steps)-1]].ProjectList[0]
		executor, err := colexec.NewExpressionExecutor(cw.proc, expr)
		require.NoError(t, err)
		defer executor.Free()
		value, err := executor.Eval(cw.proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
		require.NoError(t, err)
		require.Equal(t, uint64(4012824821), vector.GetFixedAtNoTypeCheck[uint64](value, 0))
	}()
	require.Equal(t, original, prepared.PreparePlan.String())
	require.True(t, ses.RemovePrepareStmt(prepared.Name))
	require.NoError(t, rt.migrateConnectionFrom(&query.MigrateConnFromResponse{}))
}

func TestCRC32PrepareHandlersPinSQLAndNativePrepare(t *testing.T) {
	for _, sql := range []string{
		"prepare crc_migration from 'select crc32(cast(? as json))'",
		wrapNativePrepareSQL("crc_migration", "select crc32(cast(? as json))"),
	} {
		t.Run(sql, func(t *testing.T) {
			ses, _, _, execCtx := newPreparedExecuteEnv(t, 152)
			t.Cleanup(ses.Close)
			parsed, err := mysql.Parse(execCtx.reqCtx, sql, 1)
			require.NoError(t, err)
			require.Len(t, parsed, 1)
			defer parsed[0].Free()
			var prepared *PrepareStmt
			switch st := parsed[0].(type) {
			case *tree.PrepareString:
				prepared, err = handlePrepareString(ses, execCtx, st)
			case *tree.PrepareStmt:
				prepared, err = handlePrepareStmt(ses, execCtx, st, sql)
			default:
				t.Fatalf("unexpected prepared wrapper %T", st)
			}
			require.NoError(t, err)
			require.NotNil(t, prepared.PrepareStmt)
			require.NotNil(t, prepared.PreparePlan)
			rt := &Routine{mc: newMigrateController()}
			rt.setSession(ses)
			resp := &query.MigrateConnFromResponse{}
			err = rt.migrateConnectionFrom(resp)
			require.True(t, moerr.IsMoErrCode(err, moerr.OkExpectedNotSafeToStartTransfer))
			require.Empty(t, resp.PrepareStmts)
		})
	}
}

func TestCRC32PreparedMigrationChecksFoldedAndBoundIdentity(t *testing.T) {
	ses, prepared, _, _ := newPreparedExecuteEnv(t, 153)
	t.Cleanup(ses.Close)
	rt := &Routine{mc: newMigrateController()}
	rt.setSession(ses)
	// Explicit pruning fixture: do not pretend this manually changed plan is
	// an observed optimizer trace. The original SQL must independently pin it.
	prepared.Sql = "select coalesce(1,crc32(cast(? as json)))"
	err := rt.migrateConnectionFrom(&query.MigrateConnFromResponse{})
	require.True(t, moerr.IsMoErrCode(err, moerr.OkExpectedNotSafeToStartTransfer))
	prepared.Sql = "select 1"
	for _, identity := range []struct {
		overload int32
		argType  types.T
	}{
		{plan.CRC32LegacyOverload, types.T_json},
		{plan.CRC32JSONTextOverload, types.T_varchar},
		{2, types.T_int64},
	} {
		// A preserved bound or Literal.Src identity must also pin even when the
		// rendered SQL/AST no longer contains a CRC call. These are typed fixtures.
		crc := &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: function.EncodeOverloadID(function.CRC32, identity.overload)},
			Args: []*plan.Expr{{Typ: plan.Type{Id: int32(identity.argType)}}},
		}}}
		prepared.PreparePlan = &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{
			Nodes: []*plan.Node{{ProjectList: []*plan.Expr{{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Src: crc}}}}}},
		}}}
		err = rt.migrateConnectionFrom(&query.MigrateConnFromResponse{})
		require.True(t, moerr.IsMoErrCode(err, moerr.OkExpectedNotSafeToStartTransfer))
	}
}

// Retain the lightweight writer's I/O behavior, but use the real protocol's
// database property for the zero-replay-effects oracle.
type crc32MigrationDatabaseWriter struct {
	*testMysqlWriter
	protocol MysqlProtocolImpl
}

func (w *crc32MigrationDatabaseWriter) GetStr(id PropertyID) string {
	if id == DBNAME {
		return w.protocol.GetStr(id)
	}
	return w.testMysqlWriter.GetStr(id)
}

func (w *crc32MigrationDatabaseWriter) SetStr(id PropertyID, value string) {
	if id == DBNAME {
		w.protocol.SetStr(id, value)
		return
	}
	w.testMysqlWriter.SetStr(id, value)
}

func TestCRC32PrepareMigrationRejectsBeforeReplay(t *testing.T) {
	for _, sql := range []string{
		"select crc32(?)", "prepare p from 'select crc32(cast(? as json))'", "select v from crc_view", "select crc32(",
	} {
		t.Run(sql, func(t *testing.T) {
			ses, existing, _, _ := newPreparedExecuteEnv(t, 154)
			t.Cleanup(ses.Close)
			response := ses.GetResponser().(*MysqlResp)
			response.mysqlRrWr = &crc32MigrationDatabaseWriter{testMysqlWriter: response.mysqlRrWr.(*testMysqlWriter)}
			ses.SetDatabaseName("original_db")
			ses.SetLastAffectedRows(7)
			ses.SetLastInsertID(11)
			ses.SetLastFoundRows(13)
			require.NoError(t, ses.SetUserDefinedVar("keep", int64(42), "set @keep=42"))
			require.Equal(t, "original_db", ses.GetDatabaseName())
			require.Equal(t, int64(7), ses.GetLastAffectedRows())
			require.Equal(t, uint64(11), ses.GetLastInsertID())
			require.Equal(t, uint64(13), ses.GetLastFoundRows())
			req := &query.MigrateConnToRequest{
				DB: "different_db", ConnID: 99, LastInsertIDExported: true,
				LastAffectedRows: 70, LastInsertID: 110, FoundRows: 130,
				PrepareStmts: []*query.PrepareStmt{
					{Name: "safe_first", SQL: "select 1"}, {Name: "unsafe_later", SQL: sql},
				},
				UserDefinedVarsExported: true,
				UserDefinedVars:         []*query.MigrateUserDefinedVar{{Name: "new", Value: plan2.MakePlan2StringConstExprWithType("new")}},
			}
			err := Migrate(context.Background(), ses, req)
			require.True(t, moerr.IsMoErrCode(err, moerr.OkExpectedNotSafeToStartTransfer))
			require.Equal(t, "original_db", ses.GetDatabaseName())
			require.Equal(t, int64(7), ses.GetLastAffectedRows())
			require.Equal(t, uint64(11), ses.GetLastInsertID())
			require.Equal(t, uint64(13), ses.GetLastFoundRows())
			require.Equal(t, []*PrepareStmt{existing}, ses.GetPrepareStmts())
			variable, err := ses.GetUserDefinedVar("keep")
			require.NoError(t, err)
			require.Equal(t, int64(42), variable.Value)
			require.NotContains(t, ses.userDefinedVars, "new")
			require.NotEqual(t, uint64(99), ses.proc.Base.SessionInfo.ConnectionID)
		})
	}
}

func TestCRC32PrepareMigrationKeepsEvaluatedNumericVariable(t *testing.T) {
	ses, _, _, _ := newPreparedExecuteEnvForSQL(t, 155, "select crc32('abc'),42")
	t.Cleanup(ses.Close)
	const assignmentSQL = `set @digest=crc32(cast('{"t1":"a"}' as json))`
	require.NoError(t, ses.setUserDefinedVarWithType(
		"digest", uint64(4012824821), assignmentSQL, false, plan.Type{Id: int32(types.T_uint64)}))
	rt := &Routine{mc: newMigrateController()}
	rt.setSession(ses)
	resp := &query.MigrateConnFromResponse{}
	require.NoError(t, rt.migrateConnectionFrom(resp))
	require.Len(t, resp.PrepareStmts, 1)
	restored, err := decodeUserDefinedVars(context.Background(), resp.UserDefinedVars, false)
	require.NoError(t, err)
	require.Equal(t, uint64(4012824821), restored["digest"].Value)
	require.Equal(t, assignmentSQL, restored["digest"].Sql)
	req := &query.MigrateConnToRequest{
		PrepareStmts: resp.PrepareStmts, UserDefinedVars: resp.UserDefinedVars,
		UserDefinedVarsExported: true,
	}
	require.NoError(t, checkCRC32PrepareMigration(context.Background(), ses, req))
}

func TestCRC32PrepareMigrationUsesReplaySQLModeAndCancellation(t *testing.T) {
	ses := &Session{}
	req := &query.MigrateConnToRequest{
		PrepareStmts:            []*query.PrepareStmt{{SQL: `select "crc32(?)"`}},
		SystemVariablesExported: true,
		SystemVariables:         []*query.MigrateSystemVariable{{Name: "sql_mode", Value: plan2.MakePlan2StringConstExprWithType("ANSI_QUOTES")}},
	}
	require.True(t, moerr.IsMoErrCode(checkCRC32PrepareMigration(context.Background(), ses, req), moerr.OkExpectedNotSafeToStartTransfer))
	req.SystemVariables = nil
	require.NoError(t, checkCRC32PrepareMigration(context.Background(), ses, req))
	req.SystemVariablesExported = false
	req.SetVarStmts = []string{"set sql_mode = @mode"}
	require.True(t, moerr.IsMoErrCode(checkCRC32PrepareMigration(context.Background(), ses, req), moerr.OkExpectedNotSafeToStartTransfer))
	req.PrepareStmts[0].SQL = "select ?"
	require.NoError(t, checkCRC32PrepareMigration(context.Background(), ses, req))
	// Under default mode this is one string. NO_BACKSLASH_ESCAPES exposes
	// a CRC JSON expression after it. Legacy SET replay must not open this hole.
	req.PrepareStmts[0].SQL = `select 'x\' ,crc32(cast(? as json)) -- '`
	require.True(t, crc32ReplaySQLSafe(context.Background(), req.PrepareStmts[0].SQL, "", true))
	require.False(t, crc32ReplaySQLSafe(context.Background(), req.PrepareStmts[0].SQL, "NO_BACKSLASH_ESCAPES", true))
	require.True(t, moerr.IsMoErrCode(checkCRC32PrepareMigration(context.Background(), ses, req), moerr.OkExpectedNotSafeToStartTransfer))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, checkCRC32PrepareMigration(ctx, ses, req), context.Canceled)
}
