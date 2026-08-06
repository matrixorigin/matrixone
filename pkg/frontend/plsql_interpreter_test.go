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
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

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

func TestInterpreterOutputDecimalPreservesDeclaredRuntimeType(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	defer ses.Close()
	interpreter := &Interpreter{
		ses: ses,
		argsType: map[string]plan.Type{
			"amount": {Id: int32(types.T_decimal256), Width: 30, Scale: 2},
		},
	}
	require.NoError(t, interpreter.setOutputUserVariable("out_amount", "amount", "9007199254740993.25"))
	userVar, err := ses.GetUserDefinedVar("out_amount")
	require.NoError(t, err)
	require.Equal(t, types.T_decimal256, userVar.RuntimeType)
	require.Equal(t, "9007199254740993.25", userVar.Value)

	interpreter.argsType["flag"] = plan.Type{Id: int32(types.T_bool)}
	require.NoError(t, interpreter.setOutputUserVariable("out_flag", "flag", true))
	flag, err := ses.GetUserDefinedVar("out_flag")
	require.NoError(t, err)
	require.Equal(t, types.T_int64, flag.RuntimeType)
	require.Equal(t, int64(1), flag.Value)
}
