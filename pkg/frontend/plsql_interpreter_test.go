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
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

func TestStoredProcedureDecimalVariableEvaluation(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	scopes := []map[string]interface{}{{
		"n1": nil,
		"p1": "10.00",
		"v1": "6.00",
	}}
	declaredType := plan.Type{Id: int32(types.T_decimal64), Width: 10, Scale: 2}
	typeScopes := []map[string]plan.Type{{
		"n1": declaredType,
		"p1": declaredType,
		"v1": declaredType,
	}}
	ctx := context.WithValue(context.Background(), defines.VarScopeKey{}, &scopes)
	ctx = context.WithValue(ctx, defines.VarScopeTypeKey{}, &typeScopes)
	ctx = context.WithValue(ctx, defines.InSp{}, true)

	ses := newTestSession(t, ctrl)
	defer ses.Close()
	execCtx := &ExecCtx{
		reqCtx: ctx,
		proc:   testutil.NewProcess(t),
		ses:    ses,
	}
	ses.GetTxnCompileCtx().execCtx = execCtx
	execCtx.proc.SetResolveVariableFunc(ses.GetTxnCompileCtx().ResolveVariable)
	execCtx.proc.SetResolveVariableIsBinFunc(ses.GetTxnCompileCtx().ResolveVariableIsBin)
	execCtx.proc.SetResolveVariableStringDomainFunc(ses.GetTxnCompileCtx().ResolveVariableStringDomain)

	tests := []struct {
		sql  string
		want interface{}
	}{
		{sql: "select v1 > p1", want: false},
		{sql: "select n1 is null", want: true},
	}
	for _, test := range tests {
		stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, test.sql, 1)
		require.NoError(t, err)
		expr := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr

		value, err := GetSimpleExprValue(ctx, expr, ses)
		require.NoError(t, err)
		require.Equal(t, test.want, value)
		stmt.Free()
	}
}

func TestInterpreterCoercesDecimalDeclarationAndAssignment(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	ses := newTestSession(t, ctrl)
	defer ses.Close()
	proc := testutil.NewProcess(t)
	ses.GetTxnCompileCtx().execCtx = &ExecCtx{reqCtx: ctx, proc: proc, ses: ses}
	proc.SetResolveVariableFunc(ses.GetTxnCompileCtx().ResolveVariable)
	proc.SetResolveVariableIsBinFunc(ses.GetTxnCompileCtx().ResolveVariableIsBin)
	proc.SetResolveVariableStringDomainFunc(ses.GetTxnCompileCtx().ResolveVariableStringDomain)

	stmt, err := parsers.ParseOne(
		ctx,
		dialect.MYSQL,
		"begin declare v1 decimal(10,2) default 6; declare n1 decimal(10,2) default null; set v1 = 10; end",
		1,
	)
	require.NoError(t, err)
	defer stmt.Free()
	statements := stmt.(*tree.CompoundStmt).Stmts

	back := &backgroundExecTest{}
	back.init()
	valueScopes := []map[string]interface{}{{}}
	typeScopes := []map[string]plan.Type{{}}
	interpreter := &Interpreter{
		ctx:          ctx,
		ses:          ses,
		bh:           back,
		varScope:     &valueScopes,
		varTypeScope: &typeScopes,
		fmtctx:       tree.NewFmtCtx(dialect.MYSQL, tree.WithQuoteString(true)),
	}

	status, err := interpreter.interpret(statements[0])
	require.NoError(t, err)
	require.Equal(t, SpOk, status)
	require.Equal(t, "6.00", valueScopes[0]["v1"])
	decimalType := typeScopes[0]["v1"]
	require.Equal(t, int32(types.T_decimal64), decimalType.Id)
	require.Equal(t, int32(10), decimalType.Width)
	require.Equal(t, int32(2), decimalType.Scale)

	status, err = interpreter.interpret(statements[1])
	require.NoError(t, err)
	require.Equal(t, SpOk, status)
	require.Nil(t, valueScopes[0]["n1"])
	require.Equal(t, decimalType, typeScopes[0]["n1"])

	status, err = interpreter.interpret(statements[2])
	require.NoError(t, err)
	require.Equal(t, SpOk, status)
	require.Equal(t, "10.00", valueScopes[0]["v1"])
}

func TestInterpreterYearAssignmentHonorsSQLMode(t *testing.T) {
	run := func(t *testing.T, sqlMode, body string) ([]tree.Statement, *Interpreter, []map[string]interface{}) {
		t.Helper()

		ctrl := gomock.NewController(t)
		ses := newTestSession(t, ctrl)
		t.Cleanup(ses.Close)
		proc := testutil.NewProcess(t)
		proc.SetResolveVariableFunc(func(name string, _, _ bool) (interface{}, error) {
			require.Equal(t, "sql_mode", name)
			return sqlMode, nil
		})
		ctx := context.Background()
		ses.GetTxnCompileCtx().execCtx = &ExecCtx{reqCtx: ctx, proc: proc, ses: ses}

		stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, body, 1)
		require.NoError(t, err)
		t.Cleanup(stmt.Free)
		statements := stmt.(*tree.CompoundStmt).Stmts

		valueScopes := []map[string]interface{}{{}}
		typeScopes := []map[string]plan.Type{{}}
		interpreter := &Interpreter{
			ctx:          ctx,
			ses:          ses,
			bh:           &evalCondBackgroundExec{},
			varScope:     &valueScopes,
			varTypeScope: &typeScopes,
			fmtctx:       tree.NewFmtCtx(dialect.MYSQL, tree.WithQuoteString(true)),
		}
		return statements, interpreter, valueScopes
	}

	t.Run("non-strict declaration and set adjust invalid years", func(t *testing.T) {
		statements, interpreter, scopes := run(
			t,
			"",
			"begin declare y year default 1900; set y = 2156; end",
		)
		for _, statement := range statements {
			status, err := interpreter.interpret(statement)
			require.NoError(t, err)
			require.Equal(t, SpOk, status)
			require.Equal(t, "0000", scopes[0]["y"])
		}
	})

	t.Run("strict declaration rejects invalid year", func(t *testing.T) {
		statements, interpreter, _ := run(
			t,
			"STRICT_TRANS_TABLES",
			"begin declare y year default 1900; end",
		)
		status, err := interpreter.interpret(statements[0])
		require.ErrorContains(t, err, "year value out of range")
		require.Equal(t, SpNotOk, status)
	})

	t.Run("strict set rejects invalid year", func(t *testing.T) {
		statements, interpreter, scopes := run(
			t,
			"STRICT_TRANS_TABLES",
			"begin declare y year default 1901; set y = 2156; end",
		)
		status, err := interpreter.interpret(statements[0])
		require.NoError(t, err)
		require.Equal(t, SpOk, status)
		require.Equal(t, "1901", scopes[0]["y"])

		status, err = interpreter.interpret(statements[1])
		require.ErrorContains(t, err, "year value out of range")
		require.Equal(t, SpNotOk, status)
		require.Equal(t, "1901", scopes[0]["y"])
	})
}

func TestInterpreterCoercesDecimalParameters(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	decimalType := plan.Type{Id: int32(types.T_decimal64), Width: 10, Scale: 2}
	callerValueScopes := []map[string]interface{}{{"outer_p": "10.00"}}
	callerTypeScopes := []map[string]plan.Type{{"outer_p": decimalType}}
	ctx := context.WithValue(context.Background(), defines.VarScopeKey{}, &callerValueScopes)
	ctx = context.WithValue(ctx, defines.VarScopeTypeKey{}, &callerTypeScopes)
	ctx = context.WithValue(ctx, defines.InSp{}, true)
	ses := newTestSession(t, ctrl)
	defer ses.Close()
	proc := testutil.NewProcess(t)
	ses.GetTxnCompileCtx().execCtx = &ExecCtx{reqCtx: ctx, proc: proc, ses: ses}
	proc.SetResolveVariableFunc(ses.GetTxnCompileCtx().ResolveVariable)
	proc.SetResolveVariableIsBinFunc(ses.GetTxnCompileCtx().ResolveVariableIsBin)
	proc.SetResolveVariableStringDomainFunc(ses.GetTxnCompileCtx().ResolveVariableStringDomain)
	require.NoError(t, ses.SetUserDefinedVar("io", "1.10", ""))

	callStmt, err := parsers.ParseOne(ctx, dialect.MYSQL, "call p(outer_p, @io, @ov)", 1)
	require.NoError(t, err)
	defer callStmt.Free()
	callArgs := callStmt.(*tree.CallStmt).Args
	body, err := parsers.ParseOne(ctx, dialect.MYSQL, "begin set io = io + 0.25; set ov = 12.3; end", 1)
	require.NoError(t, err)
	defer body.Free()

	valueScopes := []map[string]interface{}{}
	typeScopes := []map[string]plan.Type{}
	interpreter := &Interpreter{
		ctx:          ctx,
		ses:          ses,
		bh:           &evalCondBackgroundExec{},
		varScope:     &valueScopes,
		varTypeScope: &typeScopes,
		fmtctx:       tree.NewFmtCtx(dialect.MYSQL, tree.WithQuoteString(true)),
		argsMap: map[string]tree.Expr{
			"p1": callArgs[0],
			"io": callArgs[1],
			"ov": callArgs[2],
		},
		argsAttr: map[string]tree.InOutArgType{
			"p1": tree.TYPE_IN,
			"io": tree.TYPE_INOUT,
			"ov": tree.TYPE_OUT,
		},
		argsType: map[string]plan.Type{
			"p1": decimalType,
			"io": decimalType,
			"ov": decimalType,
		},
		outParamMap: make(map[string]interface{}),
	}

	require.NoError(t, interpreter.ExecuteSp(body, "db", false))
	ioValue, err := ses.GetUserDefinedVar("io")
	require.NoError(t, err)
	require.Equal(t, "1.35", ioValue.Value)
	outValue, err := ses.GetUserDefinedVar("ov")
	require.NoError(t, err)
	require.Equal(t, "12.30", outValue.Value)
	require.Equal(t, "10.00", valueScopes[0]["p1"])
}

func TestInterpreterSetUserVariable(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	ses := newTestSession(t, ctrl)
	defer ses.Close()
	ses.GetTxnCompileCtx().execCtx = &ExecCtx{
		reqCtx: ctx,
		proc:   testutil.NewProcess(t),
		ses:    ses,
	}
	stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, "begin set @sql = 'select 60 as prep_sum'; end", 1)
	require.NoError(t, err)
	defer stmt.Free()

	back := &backgroundExecTest{}
	back.init()
	varScope := []map[string]interface{}{}
	interpreter := &Interpreter{
		ctx:      ctx,
		ses:      ses,
		bh:       back,
		varScope: &varScope,
		fmtctx:   tree.NewFmtCtx(dialect.MYSQL, tree.WithQuoteString(true)),
	}
	interpreter.setAffectedRows(7)

	status, err := interpreter.interpret(stmt)
	require.NoError(t, err)
	require.Equal(t, SpOk, status)
	require.Empty(t, back.executedSQLs)
	userVar, err := ses.GetUserDefinedVar("sql")
	require.NoError(t, err)
	require.Equal(t, "select 60 as prep_sum", userVar.Value)
	require.Equal(t, "set @sql = \"select 60 as prep_sum\"", userVar.Sql)
	require.Zero(t, interpreter.lastAffectedRows)
}

func TestBackgroundPreparedStatementUsesClientRegistry(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	owner := newTestSession(t, ctrl)
	defer owner.Close()
	prepared := &PrepareStmt{Name: "inside_proc", Sql: "select 1"}
	require.NoError(t, owner.SetPrepareStmt(ctx, prepared.Name, prepared))

	backSes := &backSession{feSessionImpl: feSessionImpl{upstream: owner}}
	require.NoError(t, backSes.SetUserDefinedVar("sql", "select 1", "set @sql = 'select 1'"))
	userVar, err := owner.GetUserDefinedVar("sql")
	require.NoError(t, err)
	require.Equal(t, "select 1", userVar.Value)
	prepareVar := tree.NewPrepareVar("inside_proc", tree.NewVarExpr("sql", false, false, nil))
	defer prepareVar.Free()
	canExecute, err := statementCanBeExecutedInUncommittedTransaction(ctx, backSes, prepareVar)
	require.NoError(t, err)
	require.True(t, canExecute)
	resolvedOwner, err := preparedStatementOwner(ctx, backSes)
	require.NoError(t, err)
	require.Same(t, owner, resolvedOwner)
	got, err := backSes.GetPrepareStmt(ctx, prepared.Name)
	require.NoError(t, err)
	require.Same(t, prepared, got)
	require.True(t, backSes.RemovePrepareStmt(prepared.Name))
	_, err = owner.GetPrepareStmt(ctx, prepared.Name)
	require.Error(t, err)
}

// From issue #33194: a simple CASE whose WHEN condition fails to evaluate must
// stop the enclosing procedure with the original error instead of falling
// through to the next statement.
func TestInterpreterCaseStmtPropagatesConditionError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	ses := newTestSession(t, ctrl)
	defer ses.Close()
	ses.GetTxnCompileCtx().execCtx = &ExecCtx{
		reqCtx: ctx,
		proc:   testutil.NewProcess(t),
		ses:    ses,
	}

	// Shared surrounding statements; each scenario swaps in its own CASE.
	subqueryBody := `begin
insert into effects values(1,'before');
case 1 when (select id from choices) then
insert into effects values(2,'then');
else
insert into effects values(3,'else');
end case;
insert into effects values(4,'after');
end`

	nullElseBody := `begin
insert into effects values(1,'before');
case 1 when null then
insert into effects values(2,'then');
else
insert into effects values(3,'else');
end case;
insert into effects values(4,'after');
end`

	nullThenMatchBody := `begin
insert into effects values(1,'before');
case 1 when null then
insert into effects values(2,'then');
when 1 then
insert into effects values(5,'second');
else
insert into effects values(3,'else');
end case;
insert into effects values(4,'after');
end`

	nullThenSubqueryBody := `begin
insert into effects values(1,'before');
case 1 when null then
insert into effects values(2,'then');
when (select id from choices) then
insert into effects values(5,'second');
else
insert into effects values(3,'else');
end case;
insert into effects values(4,'after');
end`

	noElseBody := `begin
insert into effects values(1,'before');
case 1 when 9 then
insert into effects values(2,'then');
end case;
insert into effects values(4,'after');
end`

	statementsWith := func(bh BackgroundExec, body string) (*Interpreter, tree.Statement) {
		stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, body, 1)
		require.NoError(t, err)
		t.Cleanup(stmt.Free)
		valueScopes := []map[string]interface{}{}
		typeScopes := []map[string]plan.Type{}
		return &Interpreter{
			ctx:          ctx,
			ses:          ses,
			bh:           bh,
			varScope:     &valueScopes,
			varTypeScope: &typeScopes,
			fmtctx:       tree.NewFmtCtx(dialect.MYSQL, tree.WithQuoteString(true)),
		}, stmt
	}

	executedWithMarker := func(bh *backgroundExecTest, marker string) []string {
		var hit []string
		for _, sql := range bh.executedSQLs {
			if strings.Contains(sql, marker) {
				hit = append(hit, sql)
			}
		}
		return hit
	}

	t.Run("condition error stops procedure", func(t *testing.T) {
		back := &backgroundExecTest{}
		back.init()
		respondTo(back, map[string]ExecResult{
			"select 1 = (select id from choices)": &evalCondResult{value: 0},
		})
		condErr := moerr.NewErrSubqueryNo1Row(ctx)
		back.sql2err["select 1 = (select id from choices)"] = condErr

		interpreter, stmt := statementsWith(back, subqueryBody)
		status, err := interpreter.interpret(stmt)

		require.ErrorIs(t, err, condErr)
		require.Equal(t, SpNotOk, status)
		// Only the initial write before the CASE ran: no branch is entered
		// and the statement after the CASE must not execute.
		require.NotEmpty(t, executedWithMarker(back, "before"))
		require.Empty(t, executedWithMarker(back, "then"))
		require.Empty(t, executedWithMarker(back, "else"))
		require.Empty(t, executedWithMarker(back, "after"))
	})

	t.Run("matched condition enters when branch", func(t *testing.T) {
		back := &backgroundExecTest{}
		back.init()
		respondTo(back, map[string]ExecResult{
			"select 1 = (select id from choices)": &evalCondResult{value: 1},
		})

		interpreter, stmt := statementsWith(back, subqueryBody)
		status, err := interpreter.interpret(stmt)

		require.NoError(t, err)
		require.Equal(t, SpOk, status)
		require.NotEmpty(t, executedWithMarker(back, "before"))
		require.NotEmpty(t, executedWithMarker(back, "then"))
		require.Empty(t, executedWithMarker(back, "else"))
		require.NotEmpty(t, executedWithMarker(back, "after"))
	})

	t.Run("unmatched condition enters else branch", func(t *testing.T) {
		back := &backgroundExecTest{}
		back.init()
		respondTo(back, map[string]ExecResult{
			"select 1 = (select id from choices)": &evalCondResult{value: 0},
		})

		interpreter, stmt := statementsWith(back, subqueryBody)
		status, err := interpreter.interpret(stmt)

		require.NoError(t, err)
		require.Equal(t, SpOk, status)
		require.NotEmpty(t, executedWithMarker(back, "before"))
		require.Empty(t, executedWithMarker(back, "then"))
		require.NotEmpty(t, executedWithMarker(back, "else"))
		require.NotEmpty(t, executedWithMarker(back, "after"))
	})

	t.Run("null falls through to else branch", func(t *testing.T) {
		back := &backgroundExecTest{}
		back.init()
		respondTo(back, map[string]ExecResult{
			"select 1 = null": &nullCondResult{},
		})

		interpreter, stmt := statementsWith(back, nullElseBody)
		status, err := interpreter.interpret(stmt)

		// SQL NULL never matches, but it is not an evaluation error either:
		// the WHEN is skipped in order and, with no remaining WHEN, the
		// ELSE branch runs as the documented CASE contract requires.
		require.NoError(t, err)
		require.Equal(t, SpOk, status)
		require.NotEmpty(t, executedWithMarker(back, "before"))
		require.Empty(t, executedWithMarker(back, "then"))
		require.NotEmpty(t, executedWithMarker(back, "else"))
		require.NotEmpty(t, executedWithMarker(back, "after"))
	})

	t.Run("null falls through to later matching when", func(t *testing.T) {
		back := &backgroundExecTest{}
		back.init()
		respondTo(back, map[string]ExecResult{
			"select 1 = null": &nullCondResult{},
			"select 1 = 1":    &evalCondResult{value: 1},
		})

		interpreter, stmt := statementsWith(back, nullThenMatchBody)
		status, err := interpreter.interpret(stmt)

		// The NULL WHEN is skipped in order and the later matching WHEN
		// runs its own branch.
		require.NoError(t, err)
		require.Equal(t, SpOk, status)
		require.NotEmpty(t, executedWithMarker(back, "before"))
		require.Empty(t, executedWithMarker(back, "then"))
		require.NotEmpty(t, executedWithMarker(back, "second"))
		require.Empty(t, executedWithMarker(back, "else"))
		require.NotEmpty(t, executedWithMarker(back, "after"))
	})

	t.Run("null then later when error stops the procedure", func(t *testing.T) {
		back := &backgroundExecTest{}
		back.init()
		respondTo(back, map[string]ExecResult{
			"select 1 = null":                     &nullCondResult{},
			"select 1 = (select id from choices)": &evalCondResult{value: 0},
		})
		condErr := moerr.NewErrSubqueryNo1Row(ctx)
		back.sql2err["select 1 = (select id from choices)"] = condErr

		interpreter, stmt := statementsWith(back, nullThenSubqueryBody)
		status, err := interpreter.interpret(stmt)

		// A genuine evaluation error on a later WHEN must stop the procedure
		// with the original error even when earlier WHENs evaluated to NULL.
		require.ErrorIs(t, err, condErr)
		require.Equal(t, SpNotOk, status)
		require.NotEmpty(t, executedWithMarker(back, "before"))
		require.Empty(t, executedWithMarker(back, "then"))
		require.Empty(t, executedWithMarker(back, "second"))
		require.Empty(t, executedWithMarker(back, "else"))
		require.Empty(t, executedWithMarker(back, "after"))
	})

	t.Run("no match without else falls through", func(t *testing.T) {
		back := &backgroundExecTest{}
		back.init()
		respondTo(back, map[string]ExecResult{
			"select 1 = 9": &evalCondResult{value: 0},
		})

		interpreter, stmt := statementsWith(back, noElseBody)
		status, err := interpreter.interpret(stmt)

		// No WHEN matches and there is no ELSE: the CASE itself succeeds
		// and the procedure continues with the statement after it.
		require.NoError(t, err)
		require.Equal(t, SpOk, status)
		require.NotEmpty(t, executedWithMarker(back, "before"))
		require.Empty(t, executedWithMarker(back, "then"))
		require.NotEmpty(t, executedWithMarker(back, "after"))
	})
}

// respondTo preloads the mock background exec used by the CASE tests: each
// condition select starting with one of the given prefixes reads the mapped
// result (so ordered WHEN clauses can resolve differently), and every other
// statement reads an empty result set, like a real executor returns for
// INSERT statements.
func respondTo(back *backgroundExecTest, conds map[string]ExecResult) {
	prefixes := make([]string, 0, len(conds))
	for prefix := range conds {
		prefixes = append(prefixes, prefix)
	}
	back.beforeExec = func(s string) {
		for _, prefix := range prefixes {
			if strings.HasPrefix(s, prefix) {
				back.resultSets = []interface{}{conds[prefix]}
				return
			}
		}
		back.resultSets = []interface{}{newMrsForStoredProcedure([][]interface{}{})}
	}
}

// nullCondResult is a typed SQL NULL scalar condition result: the typed result
// state the real backend returns for an equality whose value is NULL. Its
// int64 conversion mirrors MysqlResultSet.GetInt64 on a nil value, which
// reports an error instead of a match decision.
type nullCondResult struct {
	ExecResult
}

func (e *nullCondResult) GetRowCount() uint64    { return 1 }
func (e *nullCondResult) GetColumnCount() uint64 { return 1 }

func (e *nullCondResult) ColumnIsNull(context.Context, uint64, uint64) (bool, error) {
	return true, nil
}

func (e *nullCondResult) GetInt64(ctx context.Context, rindex, cindex uint64) (int64, error) {
	return 0, moerr.NewInternalErrorf(ctx, "unsupported type %d ", interface{}(nil))
}
