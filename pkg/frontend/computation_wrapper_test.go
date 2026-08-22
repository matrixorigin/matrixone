// Copyright 2021 Matrix Origin
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
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/schedule"
	util2 "github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockCompile struct {
	runFunc     func(uint64) (*util2.RunResult, error)
	getPlanFunc func() *plan.Plan
	releaseFunc func()
}

func TestResourceAttemptOwnerEligible(t *testing.T) {
	require.True(t, resourceAttemptOwnerEligible(&Session{}))
	require.False(t, resourceAttemptOwnerEligible(&backSession{}))
	derived := &Session{}
	derived.ReplaceDerivedStmt(true)
	require.False(t, resourceAttemptOwnerEligible(derived))
}

func (m *mockCompile) Run(ts uint64) (*util2.RunResult, error) { return m.runFunc(ts) }
func (m *mockCompile) GetPlan() *plan.Plan                     { return m.getPlanFunc() }
func (m *mockCompile) Release()                                { m.releaseFunc() }
func (m *mockCompile) SetOriginSQL(s string)                   {}

func TestTxnComputationWrapper_Run(t *testing.T) {
	expectedResult := &util2.RunResult{AffectRows: 10}
	expectedPlan := &plan.Plan{}

	mockComp := &mockCompile{
		runFunc: func(ts uint64) (*util2.RunResult, error) {
			return expectedResult, nil
		},
		getPlanFunc: func() *plan.Plan {
			return expectedPlan
		},
		releaseFunc: func() {},
	}

	cwft := &TxnComputationWrapper{
		compile: mockComp,
	}

	// Test successful run
	res, err := cwft.Run(100)
	assert.NoError(t, err)
	assert.Equal(t, expectedResult, res)
	assert.Equal(t, expectedPlan, cwft.plan)
	assert.Equal(t, expectedResult, cwft.runResult)
	assert.Nil(t, cwft.compile) // Should be cleared after Run
}

func TestTxnComputationWrapper_Run_Error(t *testing.T) {
	expectedErr := assert.AnError
	expectedPlan := &plan.Plan{}

	mockComp := &mockCompile{
		runFunc: func(ts uint64) (*util2.RunResult, error) {
			return nil, expectedErr
		},
		getPlanFunc: func() *plan.Plan {
			return expectedPlan
		},
		releaseFunc: func() {},
	}

	cwft := &TxnComputationWrapper{
		compile: mockComp,
	}

	// Test error run
	res, err := cwft.Run(100)
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	assert.Nil(t, res)
	assert.Equal(t, expectedPlan, cwft.plan)
	assert.Nil(t, cwft.compile)
}

// newPreparedExecuteEnv sets up a session holding a prepared "select 1" and a
// computation wrapper that executes it through the binary protocol, so tests
// can drive cw.Compile through initExecuteStmtParam.
func newPreparedExecuteEnv(t testing.TB, stmtID uint32) (*Session, *PrepareStmt, *TxnComputationWrapper, *ExecCtx) {
	return newPreparedExecuteEnvForSQL(t, stmtID, "select 1")
}

func newPreparedExecuteEnvForSQL(t testing.TB, stmtID uint32, sql string) (*Session, *PrepareStmt, *TxnComputationWrapper, *ExecCtx) {
	ctx := statistic.ContextWithStatsInfo(context.Background(), statistic.NewStatsInfo())
	ctx = defines.AttachAccount(ctx, sysAccountID, rootID, moAdminRoleID)
	setPu("", config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil))

	ses := NewSession(ctx, "", &testMysqlWriter{}, nil)
	proc := ses.GetProc()
	require.NotNil(t, proc)
	proc.Base.SessionInfo.StorageEngine = &disttae.Engine{}

	stmtName := getPrepareStmtName(stmtID)
	prepareString := tree.NewPrepareString(tree.Identifier(stmtName), sql)
	stmts, err := mysql.Parse(ctx, prepareString.Sql, 1)
	require.NoError(t, err)
	preparePlan, err := buildPlan(ctx, nil, plan2.NewEmptyCompilerContext(), prepareString)
	require.NoError(t, err)

	prepareStmt := &PrepareStmt{
		Name:                stmtName,
		Sql:                 prepareString.Sql,
		PreparePlan:         preparePlan,
		PrepareStmt:         stmts[0],
		NativeMode:          ses.sqlModeHasMatrixOneNative(),
		OnlyFullGroupBy:     ses.sqlModeHasOnlyFullGroupBy(),
		onlyFullGroupBySet:  true,
		getFromSendLongData: make(map[int]struct{}),
		protocolVersion:     currentProtocolVersion(proc),
	}
	require.NoError(t, ses.SetPrepareStmt(ctx, stmtName, prepareStmt))

	cw := InitTxnComputationWrapper(ses, stmts[0], proc)
	cw.plan = preparePlan.GetDcl().GetPrepare().Plan
	cw.binaryPrepare = true
	cw.stmtBorrowed = true
	execCtx := &ExecCtx{
		reqCtx: ctx,
		ses:    ses,
		proc:   proc,
		resper: ses.GetResponser(),
		input: &UserInput{
			stmtName:            stmtName,
			isBinaryProtExecute: true,
		},
	}
	ses.GetTxnCompileCtx().SetExecCtx(execCtx)
	proc.SetResolveVariableFunc(ses.txnCompileCtx.ResolveVariable)
	proc.SetResolveVariableIsBinFunc(ses.txnCompileCtx.ResolveVariableIsBin)
	proc.SetResolveVariablePrepareParamKindFunc(ses.txnCompileCtx.ResolveVariablePrepareParamKind)
	return ses, prepareStmt, cw, execCtx
}

func TestInitExecuteStmtParamPreservesBinaryFlagPerUserVariable(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(t, 102, "select ?, ?")
	defer prepareStmt.Close()

	require.NoError(t, ses.setUserDefinedVar("binary_param", "AB\x00\x00", "", true))
	require.NoError(t, ses.SetUserDefinedVar("text_param", "text", ""))
	isBin, err := ses.txnCompileCtx.ResolveVariableIsBin("binary_param", false, false)
	require.NoError(t, err)
	require.True(t, isBin)
	isBin, err = ses.txnCompileCtx.ResolveVariableIsBin("text_param", false, false)
	require.NoError(t, err)
	require.False(t, isBin)
	isBin, err = ses.txnCompileCtx.ResolveVariableIsBin("system_var", true, false)
	require.NoError(t, err)
	require.False(t, isBin)
	value, err := ses.txnCompileCtx.ResolveVariable("missing", false, false)
	require.NoError(t, err)
	require.Nil(t, value)
	isBin, err = ses.txnCompileCtx.ResolveVariableIsBin("missing", false, false)
	require.NoError(t, err)
	require.False(t, isBin)
	cw.proc.SetResolveVariableFunc(func(name string, _, _ bool) (interface{}, error) {
		variable, err := ses.GetUserDefinedVar(name)
		if err != nil {
			return nil, err
		}
		return variable.Value, nil
	})
	execPlan := &plan.Execute{
		Name: prepareStmt.Name,
		Args: []*plan.Expr{
			{Expr: &plan.Expr_V{V: &plan.VarRef{Name: "binary_param"}}},
			{Expr: &plan.Expr_V{V: &plan.VarRef{Name: "text_param"}}},
		},
	}

	_, _, _, _, _, err = initExecuteStmtParam(execCtx, ses, cw, execPlan, "")
	require.NoError(t, err)
	require.True(t, cw.proc.GetPrepareParamIsBin(0))
	require.False(t, cw.proc.GetPrepareParamIsBin(1))
	require.Equal(t, plan2.ParamValue{
		Value: "AB\x00\x00", IsBin: true, EnableNumericPrefix: true,
	}, cw.paramVals[0])
	require.Equal(t, plan2.ParamValue{
		Value: "text", IsBin: false, EnableNumericPrefix: true,
	}, cw.paramVals[1])

	params := cw.proc.GetPrepareParams()
	require.NoError(t, ses.SetUserDefinedVar("binary_param", "now-text", ""))
	_, _, _, _, _, err = initExecuteStmtParam(execCtx, ses, cw, execPlan, "")
	require.NoError(t, err)
	require.Zero(t, params.Length(), "the previous owned params must be released on successful replacement")
	require.Nil(t, params.GetData())
	require.False(t, cw.proc.GetPrepareParamIsBin(0))
	require.Equal(t, "now-text", cw.proc.GetPrepareParams().GetStringAt(0))

	current := cw.proc.GetPrepareParams()
	cw.proc.SetPrepareParams(vector.NewVec(types.T_text.ToType()))
	require.Zero(t, current.Length())
	require.Nil(t, current.GetData())
	require.False(t, cw.proc.GetPrepareParamIsBin(0), "binary metadata must not leak into the next execution")
	cw.proc.GetPrepareParams().Free(cw.proc.Mp())
	cw.proc.SetPrepareParams(nil)
}

func TestInitExecuteStmtParamPreservesNumericProtocolProvenance(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(t, 104, "select ?, ?, ?, ?, ?, ?, ?, ?")
	defer func() {
		cw.proc.SetPrepareParams(nil)
		prepareStmt.Close()
	}()

	prepareStmt.params = vector.NewVec(types.T_text.ToType())
	for _, value := range []string{
		"5", "18446744073709551615", "2024", "5.5", "5.5", "5.9", "5.9", "5",
	} {
		require.NoError(t, vector.AppendBytes(prepareStmt.params, []byte(value), false, cw.proc.Mp()))
	}
	prepareStmt.ParamTypes = []byte{
		byte(defines.MYSQL_TYPE_LONGLONG), 0,
		byte(defines.MYSQL_TYPE_LONGLONG), 0x80,
		byte(defines.MYSQL_TYPE_YEAR), 0,
		byte(defines.MYSQL_TYPE_FLOAT), 0,
		byte(defines.MYSQL_TYPE_DOUBLE), 0,
		byte(defines.MYSQL_TYPE_DECIMAL), 0,
		byte(defines.MYSQL_TYPE_NEWDECIMAL), 0,
		byte(defines.MYSQL_TYPE_VAR_STRING), 0,
	}

	_, _, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	for i := 0; i < prepareStmt.params.Length(); i++ {
		param, ok := cw.paramVals[i].(plan2.ParamValue)
		require.True(t, ok)
		require.True(t, param.IsBinaryProtocol, "parameter %d lost COM_STMT provenance", i)
	}
	for i, want := range []vector.PrepareParamKind{
		vector.PrepareParamInteger,
		vector.PrepareParamInteger,
		vector.PrepareParamInteger,
		vector.PrepareParamFloat,
		vector.PrepareParamFloat,
		vector.PrepareParamDecimal,
		vector.PrepareParamDecimal,
		vector.PrepareParamNone,
	} {
		require.Equal(t, want, cw.proc.GetPrepareParamKind(i), "parameter %d", i)
	}
	require.Equal(t, vector.PrepareParamNone, cw.proc.GetPrepareParamKind(8))
	// An invalid parameter index must not bleed into another packed section.
	require.False(t, cw.proc.GetPrepareParamIsBin(8))

	prepareStmt.ParamTypes = []byte{
		byte(defines.MYSQL_TYPE_VAR_STRING), 0,
		byte(defines.MYSQL_TYPE_VAR_STRING), 0,
		byte(defines.MYSQL_TYPE_VAR_STRING), 0,
		byte(defines.MYSQL_TYPE_VAR_STRING), 0,
		byte(defines.MYSQL_TYPE_VAR_STRING), 0,
		byte(defines.MYSQL_TYPE_VAR_STRING), 0,
		byte(defines.MYSQL_TYPE_VAR_STRING), 0,
		byte(defines.MYSQL_TYPE_VAR_STRING), 0,
	}
	_, _, _, _, _, err = initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	for i := 0; i < prepareStmt.params.Length(); i++ {
		require.Equal(t, vector.PrepareParamNone, cw.proc.GetPrepareParamKind(i),
			"parameter %d retained stale numeric metadata", i)
	}
}

func TestPreparedParamValuesPreservesNullProtocolProvenance(t *testing.T) {
	_, prepareStmt, cw, _ := newPreparedExecuteEnvForSQL(t, 112, "select ?")
	defer prepareStmt.Close()

	params := vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(params, nil, true, cw.proc.Mp()))
	cw.proc.SetPrepareParamsWithMeta(
		params,
		[]bool{false},
		[]vector.PrepareParamKind{vector.PrepareParamDecimal},
	)
	defer func() {
		cw.proc.SetPrepareParams(nil)
		params.Free(cw.proc.Mp())
	}()

	values, err := preparedParamValues(cw.proc, []byte{byte(defines.MYSQL_TYPE_NEWDECIMAL), 0})
	require.NoError(t, err)
	require.Equal(t, []any{plan2.ParamValue{
		IsBinaryProtocol:    true,
		PrepareParamKind:    vector.PrepareParamDecimal,
		EnableNumericPrefix: true,
	}}, values)
}

func TestInitExecuteStmtParamRestoresBooleanRuntimeType(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(t, 105, "select ?")
	defer func() {
		cw.proc.SetPrepareParams(nil)
		prepareStmt.Close()
	}()

	prepareStmt.params = vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(prepareStmt.params, []byte("1"), false, cw.proc.Mp()))
	prepareStmt.ParamTypes = []byte{byte(defines.MYSQL_TYPE_TINY), 0}

	_, _, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	require.Len(t, cw.paramVals, 1)
	param, ok := cw.paramVals[0].(plan2.ParamValue)
	require.True(t, ok)
	require.Equal(t, vector.PrepareParamBoolean, param.PrepareParamKind)
	require.True(t, param.HasRuntimeType)
	require.Equal(t, types.T_bool.ToType(), param.RuntimeType)
}

func TestBinaryProtocolPrepareParamKind(t *testing.T) {
	for _, test := range []struct {
		mysqlType  defines.MysqlType
		isUnsigned bool
		value      string
		want       vector.PrepareParamKind
	}{
		{defines.MYSQL_TYPE_TINY, false, "0", vector.PrepareParamBoolean},
		{defines.MYSQL_TYPE_TINY, false, "1", vector.PrepareParamBoolean},
		{defines.MYSQL_TYPE_TINY, true, "1", vector.PrepareParamInteger},
		{defines.MYSQL_TYPE_TINY, false, "2", vector.PrepareParamInteger},
		{defines.MYSQL_TYPE_TINY, false, "-1", vector.PrepareParamInteger},
		{defines.MYSQL_TYPE_SHORT, false, "1", vector.PrepareParamInteger},
		{defines.MYSQL_TYPE_INT24, false, "1", vector.PrepareParamInteger},
		{defines.MYSQL_TYPE_LONG, false, "1", vector.PrepareParamInteger},
		{defines.MYSQL_TYPE_LONGLONG, false, "1", vector.PrepareParamInteger},
		{defines.MYSQL_TYPE_YEAR, false, "2024", vector.PrepareParamInteger},
		{defines.MYSQL_TYPE_FLOAT, false, "1.5", vector.PrepareParamFloat},
		{defines.MYSQL_TYPE_DOUBLE, false, "1.5", vector.PrepareParamFloat},
		{defines.MYSQL_TYPE_DECIMAL, false, "1.5", vector.PrepareParamDecimal},
		{defines.MYSQL_TYPE_NEWDECIMAL, false, "1.5", vector.PrepareParamDecimal},
		{defines.MYSQL_TYPE_BIT, false, "1", vector.PrepareParamInteger},
		{defines.MYSQL_TYPE_VAR_STRING, false, "1", vector.PrepareParamNone},
	} {
		require.Equal(t, test.want,
			binaryProtocolPrepareParamKind(test.mysqlType, test.isUnsigned, []byte(test.value)),
			"type %v unsigned %t value %q", test.mysqlType, test.isUnsigned, test.value)
	}
}

func TestBinaryProtocolPrepareParamType(t *testing.T) {
	decimal, ok := binaryProtocolPrepareParamType(
		defines.MYSQL_TYPE_NEWDECIMAL,
		false,
		[]byte("-12345678901234567890.123456789"),
	)
	require.True(t, ok)
	require.Equal(t, types.T_decimal128, decimal.Oid)
	require.Equal(t, int32(29), decimal.Width)
	require.Equal(t, int32(9), decimal.Scale)

	for _, test := range []struct {
		name       string
		mysqlType  defines.MysqlType
		isUnsigned bool
		want       types.T
	}{
		{name: "signed integer", mysqlType: defines.MYSQL_TYPE_LONG, want: types.T_int32},
		{name: "unsigned integer", mysqlType: defines.MYSQL_TYPE_LONGLONG, isUnsigned: true, want: types.T_uint64},
		{name: "double", mysqlType: defines.MYSQL_TYPE_DOUBLE, want: types.T_float64},
		{name: "string", mysqlType: defines.MYSQL_TYPE_VAR_STRING, want: types.T_text},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, ok := binaryProtocolPrepareParamType(test.mysqlType, test.isUnsigned, []byte("5"))
			require.True(t, ok)
			require.Equal(t, test.want, got.Oid)
		})
	}

	_, ok = binaryProtocolPrepareParamType(defines.MYSQL_TYPE_NULL, false, nil)
	require.False(t, ok)
}

func TestInitExecuteStmtParamSpecializesBinaryRuntimePlan(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(t, 110, "select ?")
	defer prepareStmt.Close()

	originalPlan := prepareStmt.PreparePlan.GetDcl().GetPrepare().Plan
	params := vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(
		params,
		[]byte("-12345678901234567890.123456789"),
		false,
		cw.proc.Mp(),
	))
	prepareStmt.params = params
	prepareStmt.ParamTypes = []byte{byte(defines.MYSQL_TYPE_NEWDECIMAL), 0}

	var resultColumns []*plan.ColDef
	writer := execCtx.resper.MysqlRrWr().(*testMysqlWriter)
	writer.makeColumnDefDataFunc = func(_ context.Context, columns []*plan.ColDef) ([][]byte, error) {
		resultColumns = columns
		return [][]byte{[]byte("runtime-decimal")}, nil
	}

	_, runtimePlan, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	require.NotNil(t, runtimePlan)
	require.NotSame(t, originalPlan, runtimePlan)
	projectNode := runtimePlan.GetQuery().Nodes[runtimePlan.GetQuery().Steps[len(runtimePlan.GetQuery().Steps)-1]]
	require.Equal(t, int32(types.T_decimal128), projectNode.ProjectList[0].Typ.Id)
	require.Equal(t, int32(29), projectNode.ProjectList[0].Typ.Width)
	require.Equal(t, int32(9), projectNode.ProjectList[0].Typ.Scale)
	require.Len(t, resultColumns, 1)
	require.Equal(t, int32(types.T_decimal128), resultColumns[0].Typ.Id)
	require.Equal(t, [][]byte{[]byte("runtime-decimal")}, execCtx.prepareColDef)
	value, parseErr := types.ParseDecimal128("-12345678901234567890.123456789", 29, 9)
	require.NoError(t, parseErr)
	executor, execErr := colexec.NewExpressionExecutor(cw.proc, projectNode.ProjectList[0])
	require.NoError(t, execErr)
	defer executor.Free()
	input := batch.New(nil)
	input.SetRowCount(1)
	vec, evalErr := executor.Eval(cw.proc, []*batch.Batch{input}, nil)
	require.NoError(t, evalErr)
	require.Equal(t, types.T_decimal128, vec.GetType().Oid)
	require.Equal(t, value, vector.GetFixedAtNoTypeCheck[types.Decimal128](vec, 0))
	originalProjectNode := originalPlan.GetQuery().Nodes[originalPlan.GetQuery().Steps[len(originalPlan.GetQuery().Steps)-1]]
	require.Equal(t, int32(types.T_text), originalProjectNode.ProjectList[0].Typ.Id)
}

func TestInitExecuteStmtParamSpecializesSQLExecuteCommonTypePlan(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(
		t, 111, "select ?")
	defer prepareStmt.Close()
	decimalType := types.New(types.T_decimal64, 10, 2)
	decimalPeer, parseErr := types.ParseDecimal64("1.25", decimalType.Width, decimalType.Scale)
	require.NoError(t, parseErr)
	commonExpr, bindErr := plan2.BindFuncExprImplByPlanExpr(context.Background(), "coalesce", []*plan.Expr{
		{
			Typ:  plan.Type{Id: int32(types.T_text)},
			Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}},
		},
		{
			Typ: plan.Type{Id: int32(decimalType.Oid), Width: decimalType.Width, Scale: decimalType.Scale},
			Expr: &plan.Expr_Lit{Lit: &plan.Literal{
				Value: &plan.Literal_Decimal64Val{Decimal64Val: &plan.Decimal64{A: int64(decimalPeer)}},
			}},
		},
	})
	require.NoError(t, bindErr)
	manualPlan := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{
		StmtType: plan.Query_SELECT,
		Steps:    []int32{0},
		Headings: []string{"coalesce"},
		Nodes: []*plan.Node{{
			NodeType:    plan.Node_VALUE_SCAN,
			ProjectList: []*plan.Expr{commonExpr},
		}},
	}}, IsPrepare: true}
	prepareStmt.PreparePlan.GetDcl().GetPrepare().Plan = manualPlan
	cw.plan = manualPlan

	require.NoError(t, ses.SetUserDefinedVar("numeric_text", "12.5tail", ""))
	execCtx.input.isBinaryProtExecute = false
	execPlan := &plan.Execute{
		Name: prepareStmt.Name,
		Args: []*plan.Expr{{Expr: &plan.Expr_V{V: &plan.VarRef{Name: "numeric_text"}}}},
	}
	originalPlan := prepareStmt.PreparePlan.GetDcl().GetPrepare().Plan
	sentinel := compile.NewCompile(
		"", "", prepareStmt.Sql, "", "", nil,
		cw.proc, prepareStmt.PrepareStmt, false, nil, time.Now())
	prepareStmt.compile = sentinel
	writer := execCtx.resper.MysqlRrWr().(*testMysqlWriter)
	writer.makeColumnDefDataFunc = func(context.Context, []*plan.ColDef) ([][]byte, error) {
		return nil, errors.New("SQL EXECUTE must not build binary-protocol column definitions")
	}

	retComp, runtimePlan, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, execPlan, "")
	require.NoError(t, err)
	require.Nil(t, retComp,
		"the prepare-time compile cannot execute a common-type-specialized plan: plan=%s params=%#v",
		runtimePlan.String(), cw.paramVals)
	require.Same(t, sentinel, prepareStmt.compile, "specialization must not replace the cached compile")
	require.NotSame(t, originalPlan, runtimePlan)
	require.Len(t, cw.paramVals, 1)
	paramValue, ok := cw.paramVals[0].(plan2.ParamValue)
	require.True(t, ok)
	require.False(t, paramValue.IsBinaryProtocol)
	require.True(t, paramValue.EnableNumericPrefix)

	projectNode := runtimePlan.GetQuery().Nodes[runtimePlan.GetQuery().Steps[len(runtimePlan.GetQuery().Steps)-1]]
	project := projectNode.ProjectList[0]
	require.True(t, types.T(project.Typ.Id).IsDecimal(), project.String())
	requiresV25, scanErr := plan.RequiresMORPCVersion25NumericPrefix(project)
	require.NoError(t, scanErr)
	require.True(t, requiresV25, project.String())

	executor, execErr := colexec.NewExpressionExecutor(cw.proc, project)
	require.NoError(t, execErr)
	defer executor.Free()
	input := batch.New(nil)
	input.SetRowCount(1)
	vec, evalErr := executor.Eval(cw.proc, []*batch.Batch{input}, nil)
	require.NoError(t, evalErr)
	require.Equal(t, types.T_decimal64, vec.GetType().Oid)
	want, parseErr := types.ParseDecimal64("12.50", project.Typ.Width, project.Typ.Scale)
	require.NoError(t, parseErr)
	require.Equal(t, want, vector.GetFixedAtNoTypeCheck[types.Decimal64](vec, 0))

	originalProjectNode := originalPlan.GetQuery().Nodes[originalPlan.GetQuery().Steps[len(originalPlan.GetQuery().Steps)-1]]
	requiresV25, scanErr = plan.RequiresMORPCVersion25NumericPrefix(originalProjectNode.ProjectList[0])
	require.NoError(t, scanErr)
	require.False(t, requiresV25)
}

func TestSpecializePreparedExecutionPlanSkipsIneligibleSQLPlan(t *testing.T) {
	ctx := context.Background()
	floatType := types.T_float32.ToType()
	predicate, err := plan2.BindFuncExprImplByPlanExpr(ctx, "=", []*plan.Expr{
		{
			Typ:  plan.Type{Id: int32(floatType.Oid)},
			Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 0}},
		},
		{
			Typ:  plan.Type{Id: int32(types.T_text)},
			Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}},
		},
	})
	require.NoError(t, err)
	original := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{
		StmtType: plan.Query_SELECT,
		Steps:    []int32{0},
		Nodes: []*plan.Node{{
			NodeType:   plan.Node_VALUE_SCAN,
			FilterList: []*plan.Expr{predicate},
		}},
	}}, IsPrepare: true}

	runtimePlan, specialized, applied, err := specializePreparedExecutionPlan(ctx, original, []any{
		plan2.ParamValue{
			Value: "1.2345678", PrepareParamKind: vector.PrepareParamDecimal, EnableNumericPrefix: true,
		},
	}, false)
	require.NoError(t, err)
	require.False(t, specialized)
	require.False(t, applied)
	require.Same(t, original, runtimePlan, "ineligible SQL EXECUTE must not deep-copy the cached plan")
}

func TestSQLVariablePrepareParamKind(t *testing.T) {
	for _, test := range []struct {
		oid  types.T
		want vector.PrepareParamKind
	}{
		{types.T_bool, vector.PrepareParamBoolean},
		{types.T_bit, vector.PrepareParamInteger},
		{types.T_int64, vector.PrepareParamInteger},
		{types.T_uint64, vector.PrepareParamInteger},
		{types.T_year, vector.PrepareParamInteger},
		{types.T_float64, vector.PrepareParamFloat},
		{types.T_decimal128, vector.PrepareParamDecimal},
		{types.T_varchar, vector.PrepareParamNone},
	} {
		require.Equal(t, test.want, prepareParamKindFromType(test.oid), "type %v", test.oid)
	}
	require.Equal(t, vector.PrepareParamBoolean, prepareParamKindFromValue(true))
	require.Equal(t, vector.PrepareParamInteger, prepareParamKindFromValue(uint64(5)))
	require.Equal(t, vector.PrepareParamFloat, prepareParamKindFromValue(float64(5)))
	require.Equal(t, vector.PrepareParamNone, prepareParamKindFromValue("5"))
}

func TestTransparentPrepareParamKind(t *testing.T) {
	ses, prepareStmt, cw, _ := newPreparedExecuteEnvForSQL(t, 109, "select ?")
	defer prepareStmt.Close()

	params := vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(params, []byte("5"), false, cw.proc.Mp()))
	defer params.Free(cw.proc.Mp())
	cw.proc.SetPrepareParamsWithMeta(
		params, nil, []vector.PrepareParamKind{vector.PrepareParamFloat})
	defer cw.proc.SetPrepareParams(nil)

	kind, err := transparentPrepareParamKind(
		&tree.ParenExpr{Expr: tree.NewParamExpr(1)}, ses)
	require.NoError(t, err)
	require.Equal(t, vector.PrepareParamFloat, kind)

	require.NoError(t, ses.setUserDefinedVarWithKind(
		"decimal_value", "5.9", "", false, vector.PrepareParamDecimal))
	kind, err = transparentPrepareParamKind(
		tree.NewVarExpr("decimal_value", false, false, nil), ses)
	require.NoError(t, err)
	require.Equal(t, vector.PrepareParamDecimal, kind)

	kind, err = transparentPrepareParamKind(tree.NewParamExpr(2), ses)
	require.NoError(t, err)
	require.Equal(t, vector.PrepareParamNone, kind)
}

func TestPreparedSetExpressionParamsAfterInit(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(
		t, 108, "set @prepared_set_value = ? + 1")
	defer prepareStmt.Close()

	install := func(value string) *vector.Vector {
		params := vector.NewVec(types.T_text.ToType())
		require.NoError(t, vector.AppendBytes(params, []byte(value), false, cw.proc.Mp()))
		prepareStmt.params = params

		_, _, stmt, _, _, err := initExecuteStmtParam(
			execCtx, ses, cw, nil, prepareStmt.Name)
		require.NoError(t, err)
		require.IsType(t, &tree.SetVar{}, stmt)
		require.Equal(t, value, cw.proc.GetPrepareParams().GetStringAt(0))
		return params
	}

	first := install("41")
	second := install("9")
	first.Free(cw.proc.Mp())
	require.Equal(t, "9", cw.proc.GetPrepareParams().GetStringAt(0))
	require.Same(t, second, prepareStmt.params)
}

func TestInitExecuteStmtParamFreesParamsOnResolveError(t *testing.T) {
	ses, prepareStmt, cw, _ := newPreparedExecuteEnvForSQL(t, 103, "select ?, ?")
	defer prepareStmt.Close()
	require.NoError(t, ses.SetUserDefinedVar("first", "allocated", ""))
	cw.proc.SetResolveVariableFunc(func(name string, _, _ bool) (interface{}, error) {
		if name == "second" {
			return nil, assert.AnError
		}
		variable, err := ses.GetUserDefinedVar(name)
		if err != nil {
			return nil, err
		}
		return variable.Value, nil
	})
	execPlan := &plan.Execute{
		Name: prepareStmt.Name,
		Args: []*plan.Expr{
			{Expr: &plan.Expr_V{V: &plan.VarRef{Name: "first"}}},
			{Expr: &plan.Expr_V{V: &plan.VarRef{Name: "second"}}},
		},
	}
	params, _, _, _, err := buildExecuteUserParams(cw.proc, execPlan.Args)
	require.ErrorIs(t, err, assert.AnError)
	require.Zero(t, params.Length())
	require.Nil(t, params.GetData())
	require.Nil(t, params.GetArea())
}

func TestResolveVariableIsBinHonorsStoredProcedureScope(t *testing.T) {
	ses, prepareStmt, _, execCtx := newPreparedExecuteEnv(t, 104)
	defer prepareStmt.Close()
	require.NoError(t, ses.setUserDefinedVar("v1", "session-binary", "", true))
	require.NoError(t, ses.setUserDefinedVar("session_only", "session-binary", "", true))
	scopes := []map[string]interface{}{
		{"v1": int64(10), "declared_only_outer": "5.0"},
		{
			"v1":                  int64(20),
			"inner":               int64(30),
			"decimal_value":       "5.00",
			"year_value":          "2024",
			"string_value":        "5",
			"declared_only_outer": "5.0",
		},
	}
	typeScopes := []map[string]plan.Type{
		{"declared_only_outer": {Id: int32(types.T_decimal64)}},
		{
			"v1":            {Id: int32(types.T_int64)},
			"decimal_value": {Id: int32(types.T_decimal64)},
			"year_value":    {Id: int32(types.T_year)},
			"string_value":  {Id: int32(types.T_varchar)},
		},
	}
	execCtx.reqCtx = context.WithValue(execCtx.reqCtx, defines.VarScopeKey{}, &scopes)
	execCtx.reqCtx = context.WithValue(execCtx.reqCtx, defines.VarScopeTypeKey{}, &typeScopes)
	execCtx.reqCtx = context.WithValue(execCtx.reqCtx, defines.InSp{}, true)

	value, err := ses.txnCompileCtx.ResolveVariable("V1", false, false)
	require.NoError(t, err)
	require.Equal(t, int64(20), value)
	isBin, err := ses.txnCompileCtx.ResolveVariableIsBin("V1", false, false)
	require.NoError(t, err)
	require.False(t, isBin)
	kind, err := ses.txnCompileCtx.ResolveVariablePrepareParamKind("V1", false, false)
	require.NoError(t, err)
	require.Equal(t, vector.PrepareParamInteger, kind)

	for _, test := range []struct {
		name     string
		expected vector.PrepareParamKind
	}{
		{name: "decimal_value", expected: vector.PrepareParamDecimal},
		{name: "year_value", expected: vector.PrepareParamInteger},
		{name: "string_value", expected: vector.PrepareParamNone},
		{name: "declared_only_outer", expected: vector.PrepareParamNone},
	} {
		kind, err = ses.txnCompileCtx.ResolveVariablePrepareParamKind(test.name, false, false)
		require.NoError(t, err)
		require.Equal(t, test.expected, kind, test.name)
	}

	value, err = ses.txnCompileCtx.ResolveVariable("session_only", false, false)
	require.NoError(t, err)
	require.Equal(t, "session-binary", value)
	isBin, err = ses.txnCompileCtx.ResolveVariableIsBin("session_only", false, false)
	require.NoError(t, err)
	require.True(t, isBin)
	kind, err = ses.txnCompileCtx.ResolveVariablePrepareParamKind("session_only", false, false)
	require.NoError(t, err)
	require.Equal(t, vector.PrepareParamNone, kind)

	value, err = ses.txnCompileCtx.ResolveVariable("missing_user_var", false, false)
	require.NoError(t, err)
	require.Nil(t, value)
	isBin, err = ses.txnCompileCtx.ResolveVariableIsBin("missing_user_var", false, false)
	require.NoError(t, err)
	require.False(t, isBin)
	kind, err = ses.txnCompileCtx.ResolveVariablePrepareParamKind("missing_user_var", false, false)
	require.NoError(t, err)
	require.Equal(t, vector.PrepareParamNone, kind)
}

func TestBuildExecuteUserParamsHonorsStoredProcedureScope(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnv(t, 105)
	defer prepareStmt.Close()
	require.NoError(t, ses.setUserDefinedVar("local_shadow", "session-binary", "", true))
	require.NoError(t, ses.setUserDefinedVar("session_only", "session-binary", "", true))
	scopes := []map[string]interface{}{
		{"local_only": int64(10), "local_shadow": int64(20)},
	}
	execCtx.reqCtx = context.WithValue(execCtx.reqCtx, defines.VarScopeKey{}, &scopes)
	execCtx.reqCtx = context.WithValue(execCtx.reqCtx, defines.InSp{}, true)

	args := []*plan.Expr{
		{Expr: &plan.Expr_V{V: &plan.VarRef{Name: "local_only"}}},
		{Expr: &plan.Expr_V{V: &plan.VarRef{Name: "local_shadow"}}},
		{Expr: &plan.Expr_V{V: &plan.VarRef{Name: "session_only"}}},
	}
	params, paramVals, paramIsBin, paramKinds, err := buildExecuteUserParams(cw.proc, args)
	require.NoError(t, err)
	defer params.Free(cw.proc.Mp())

	require.Equal(t, []bool{false, false, true}, paramIsBin)
	require.Equal(t, []vector.PrepareParamKind{
		vector.PrepareParamInteger,
		vector.PrepareParamInteger,
		vector.PrepareParamNone,
	}, paramKinds)
	require.Equal(t, []any{
		plan2.ParamValue{
			Value: int64(10), IsBin: false, PrepareParamKind: vector.PrepareParamInteger, EnableNumericPrefix: true,
		},
		plan2.ParamValue{
			Value: int64(20), IsBin: false, PrepareParamKind: vector.PrepareParamInteger, EnableNumericPrefix: true,
		},
		plan2.ParamValue{Value: "session-binary", IsBin: true, EnableNumericPrefix: true},
	}, paramVals)
	require.Equal(t, "10", params.GetStringAt(0))
	require.Equal(t, "20", params.GetStringAt(1))
	require.Equal(t, "session-binary", params.GetStringAt(2))
}

// A nil cached compile means the statement was rejected for prepare-time
// compile (e.g. AP query hitting ErrCantCompileForPrepare). Execute must not
// retry that doomed compile on every run; the cache stays nil and the regular
// compile path (isPrepare=false) takes over.
func TestPreparedDDLNeedsCatalogRefresh(t *testing.T) {
	testCases := []struct {
		sql      string
		expected bool
	}{
		{sql: "create pitr p for account range 1 'd'", expected: true},
		{sql: "create database db", expected: false},
		{sql: "create database sub from pub publication p", expected: true},
		{sql: "drop database db", expected: true},
		{sql: "create table dst clone src", expected: true},
		{sql: "truncate table t", expected: false},
	}
	for _, testCase := range testCases {
		statements, err := mysql.Parse(context.Background(), testCase.sql, 1)
		require.NoError(t, err)
		require.Len(t, statements, 1)
		require.Equal(t, testCase.expected, preparedDDLNeedsCatalogRefresh(statements[0]))
		statements[0].Free()
	}
}

func TestPrepareSchemaAccountID(t *testing.T) {
	require.Equal(t, uint32(7), prepareSchemaAccountID(7, &plan.ObjectRef{SchemaName: "db", ObjName: "t"}))
	require.Equal(t, uint32(11), prepareSchemaAccountID(7, &plan.ObjectRef{
		SchemaName: "db", ObjName: "t", PubInfo: &plan.PubInfo{TenantId: 11},
	}))
	require.Equal(t, uint32(sysAccountID), prepareSchemaAccountID(7, &plan.ObjectRef{
		SchemaName: catalog.MO_SYSTEM, ObjName: catalog.MO_STATEMENT,
	}))
}

func TestPreparedSubscriptionSchemaChanged(t *testing.T) {
	expected := &plan.ObjectRef{
		Server: 4, Db: 2, Obj: 3, SchemaName: "publisher_db", ObjName: "src",
		SubscriptionName: "sub", PubInfo: &plan.PubInfo{TenantId: 11},
	}
	resolve := func(
		ref *plan.ObjectRef,
		def *plan.TableDef,
		err error,
	) preparedSchemaResolver {
		return func(databaseName, tableName string, _ *plan.Snapshot) (*plan.ObjectRef, *plan.TableDef, error) {
			require.Equal(t, "sub", databaseName)
			require.Equal(t, "src", tableName)
			return ref, def, err
		}
	}
	stableRef := &plan.ObjectRef{
		Obj: 3, SchemaName: "publisher_db", ObjName: "src",
		SubscriptionName: "sub", PubInfo: &plan.PubInfo{TenantId: 11},
	}
	stableDef := &plan.TableDef{DbId: 2, TblId: 3, Version: 4}
	stableResolver := resolve(stableRef, stableDef, nil)

	changed, err := preparedSubscriptionSchemaChanged(stableResolver, expected)
	require.NoError(t, err)
	require.False(t, changed)

	changed, err = preparedSubscriptionSchemaChanged(stableResolver, &plan.ObjectRef{
		SubscriptionName: "sub",
		ObjName:          "src",
	})
	require.NoError(t, err)
	require.True(t, changed)

	changed, err = preparedSubscriptionSchemaChanged(resolve(nil, nil, nil), expected)
	require.NoError(t, err)
	require.True(t, changed)

	changed, err = preparedSubscriptionSchemaChanged(resolve(
		&plan.ObjectRef{
			Obj: 3, SchemaName: "publisher_db", ObjName: "src",
			SubscriptionName: "sub", PubInfo: &plan.PubInfo{TenantId: 12},
		},
		stableDef,
		nil,
	), expected)
	require.NoError(t, err)
	require.True(t, changed)

	changed, err = preparedSubscriptionSchemaChanged(resolve(
		stableRef,
		&plan.TableDef{DbId: 2, TblId: 3, Version: 5},
		nil,
	), expected)
	require.NoError(t, err)
	require.True(t, changed)

	changed, err = preparedSubscriptionSchemaChanged(resolve(
		nil,
		nil,
		assert.AnError,
	), expected)
	require.ErrorIs(t, err, assert.AnError)
	require.False(t, changed)
}

func TestPreparedSubscriptionSchemaChangedUsesLogicalName(t *testing.T) {
	expected := &plan.ObjectRef{
		Server: 4, Db: 2, Obj: 3, SchemaName: "publisher_db", ObjName: "src",
		SubscriptionName: "sub", PubInfo: &plan.PubInfo{TenantId: 11},
	}
	changed, err := preparedSubscriptionSchemaChanged(func(
		databaseName, tableName string, _ *plan.Snapshot,
	) (*plan.ObjectRef, *plan.TableDef, error) {
		require.Equal(t, "sub", databaseName)
		require.Equal(t, "src", tableName)
		return &plan.ObjectRef{
				Obj: 3, SchemaName: "publisher_db", ObjName: "src",
				SubscriptionName: "sub", PubInfo: &plan.PubInfo{TenantId: 11},
			},
			&plan.TableDef{DbId: 2, TblId: 3, Version: 4}, nil
	}, expected)
	require.NoError(t, err)
	require.False(t, changed)
}

func TestPreparedSubscriptionSchemaChangedUsesDependencySnapshot(t *testing.T) {
	snapshot := &plan.Snapshot{
		TS: &timestamp.Timestamp{PhysicalTime: 42, LogicalTime: 7},
	}
	expected := &plan.ObjectRef{
		Server: 4, Db: 2, Obj: 3, SchemaName: "publisher_db", ObjName: "src",
		SubscriptionName: "sub", PubInfo: &plan.PubInfo{TenantId: 11},
		Snapshot: snapshot,
	}
	calls := 0
	changed, err := preparedSubscriptionSchemaChanged(func(
		databaseName, tableName string, gotSnapshot *plan.Snapshot,
	) (*plan.ObjectRef, *plan.TableDef, error) {
		calls++
		if calls == 1 {
			require.Nil(t, gotSnapshot)
		} else {
			require.Equal(t, snapshot, gotSnapshot)
		}
		return &plan.ObjectRef{
				Obj: 3, SchemaName: "publisher_db", ObjName: "src",
				SubscriptionName: "sub", PubInfo: &plan.PubInfo{TenantId: 11},
			},
			&plan.TableDef{DbId: 2, TblId: 3, Version: 4}, nil
	}, expected)
	require.NoError(t, err)
	require.False(t, changed)
	require.Equal(t, 2, calls)
}

func TestPreparedSubscriptionsNeedValidation(t *testing.T) {
	require.False(t, preparedSubscriptionsNeedValidation(
		timestamp.Timestamp{PhysicalTime: 100},
		timestamp.Timestamp{PhysicalTime: 100},
		timestamp.Timestamp{},
	))
	require.True(t, preparedSubscriptionsNeedValidation(
		timestamp.Timestamp{PhysicalTime: 200},
		timestamp.Timestamp{PhysicalTime: 100},
		timestamp.Timestamp{},
	))
	require.False(t, preparedSubscriptionsNeedValidation(
		timestamp.Timestamp{PhysicalTime: 200},
		timestamp.Timestamp{PhysicalTime: 100},
		timestamp.Timestamp{PhysicalTime: 200},
	))
}

func TestPreparedNamedSnapshotsNeedValidation(t *testing.T) {
	namedSnapshot := &plan.Snapshot{
		TS: &timestamp.Timestamp{PhysicalTime: 42},
		ExtraInfo: &plan.SnapshotExtraInfo{
			Name: "snap",
		},
	}
	unnamedSnapshot := &plan.Snapshot{
		TS: &timestamp.Timestamp{PhysicalTime: 42},
	}
	metadataTS := timestamp.Timestamp{PhysicalTime: 200}
	prepareTS := timestamp.Timestamp{PhysicalTime: 100}

	require.True(t, preparedNamedSnapshotsNeedValidation(
		[]*plan.ObjectRef{{Snapshot: namedSnapshot}},
		metadataTS,
		prepareTS,
		timestamp.Timestamp{},
	))
	require.False(t, preparedNamedSnapshotsNeedValidation(
		[]*plan.ObjectRef{{Snapshot: unnamedSnapshot}},
		metadataTS,
		prepareTS,
		timestamp.Timestamp{},
	))
	require.False(t, preparedNamedSnapshotsNeedValidation(
		[]*plan.ObjectRef{{Snapshot: namedSnapshot}},
		metadataTS,
		prepareTS,
		metadataTS,
	))
}

func TestCurrentTxnSnapshotTS(t *testing.T) {
	ses, prepareStmt, _, _ := newPreparedExecuteEnv(t, 100)
	defer prepareStmt.Close()

	snapshot := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 7}
	ctrl := gomock.NewController(t)
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().SnapshotTS().Return(snapshot)
	ses.proc.Base.TxnOperator = txnOperator

	require.Equal(t, snapshot, currentTxnSnapshotTS(ses))
}

func TestInitExecuteStmtParamUsesTxnSnapshotAfterRebuild(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnv(t, 101)
	defer prepareStmt.Close()

	snapshot := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 7}
	ctrl := gomock.NewController(t)
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().SnapshotTS().Return(snapshot)
	txnOperator.EXPECT().NextSequence().Return(uint64(1)).AnyTimes()
	ses.proc.Base.TxnOperator = txnOperator
	ses.advanceDDLVersion()

	_, _, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	require.Equal(t, snapshot, prepareStmt.Ts)
	require.Equal(t, ses.getDDLVersion(), prepareStmt.ddlVersion)
}

func TestInitExecuteStmtParamSkipsPrepareCompileWithoutCache(t *testing.T) {
	_, prepareStmt, cw, execCtx := newPreparedExecuteEnv(t, 100)
	defer prepareStmt.Close()

	ret, err := cw.Compile(execCtx, nil)
	require.NoError(t, err)
	require.NotNil(t, ret)
	require.Nil(t, prepareStmt.compile)
}

// Without a schema change the cached compile must be reused as-is instead of
// being released and rebuilt on every execute: the per-execution recompilation
// regressed TPCC by 25%. Stale pipeline state is cleared by Compile.Reset
// (see Scope.resetForReuse) before the reused compile runs.
// See https://github.com/matrixorigin/matrixone/issues/25614.
func TestInitExecuteStmtParamReusesCachedCompileWhenNoSchemaChange(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnv(t, 101)
	defer prepareStmt.Close()

	// The sentinel cached compile carries no plan; a recompilation would
	// replace it with a freshly built compile.
	sentinel := compile.NewCompile(
		"", "", prepareStmt.Sql, "", "", nil,
		cw.proc, prepareStmt.PrepareStmt, false, nil, time.Now())
	prepareStmt.compile = sentinel

	retComp, retPlan, retStmt, _, _, err := initExecuteStmtParam(
		execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	require.Same(t, sentinel, retComp)
	require.Same(t, sentinel, prepareStmt.compile)
	require.NotNil(t, retPlan)
	require.NotNil(t, retStmt)
}

func TestInitExecuteStmtParamReusesCachedCompileForTextParameter(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(t, 111, "select ?")
	defer prepareStmt.Close()

	params := vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(params, []byte("plain text"), false, cw.proc.Mp()))
	prepareStmt.params = params
	prepareStmt.ParamTypes = []byte{byte(defines.MYSQL_TYPE_VAR_STRING), 0}
	sentinel := compile.NewCompile(
		"", "", prepareStmt.Sql, "", "", nil,
		cw.proc, prepareStmt.PrepareStmt, false, nil, time.Now())
	prepareStmt.compile = sentinel

	retComp, retPlan, _, _, _, err := initExecuteStmtParam(
		execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	require.Same(t, sentinel, retComp,
		"unchanged text parameter domain must not force a full recompilation")
	require.Same(t, sentinel, prepareStmt.compile)
	require.NotNil(t, retPlan)
}

func TestInitExecuteStmtParamTransfersFreshCloneOwnership(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnv(t, 101)
	defer prepareStmt.Close()

	clone := &tree.CloneTable{}
	clone.SrcTable.ObjectName = "src"
	clone.CreateTable.Table.ObjectName = "dst"
	prepareStmt.cloneSQL = preparedCloneSQL(clone, "prepare_db")

	_, _, executionStmt, _, owned, err := initExecuteStmtParam(
		execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	require.True(t, owned)

	executionClone := executionStmt.(*tree.CloneTable)
	require.Equal(t, tree.Identifier("prepare_db"), executionClone.SrcTable.SchemaName)
	cw.stmt = executionStmt
	cw.stmtBorrowed = !owned
	cw.Free()

	require.Empty(t, executionClone.SrcTable.SchemaName)
	require.Empty(t, executionClone.SrcTable.ObjectName)
	require.Empty(t, executionClone.CreateTable.Table.SchemaName)
	require.Empty(t, executionClone.CreateTable.Table.ObjectName)
}

func TestTxnComputationWrapperKeepsSharedPreparedStatement(t *testing.T) {
	stmt := &trackedStatement{}
	cw := &TxnComputationWrapper{
		stmt:         stmt,
		stmtBorrowed: true,
	}

	cw.Free()
	require.Zero(t, stmt.freed)
}

func TestGetComputationWrapperMarksBinaryPreparedStatementBorrowed(t *testing.T) {
	ses, prepareStmt, _, execCtx := newPreparedExecuteEnv(t, 101)
	defer prepareStmt.Close()

	stmt := &trackedStatement{}
	execCtx.input.stmt = stmt
	execCtx.input.preparePlan = prepareStmt.PreparePlan

	wrappers, err := GetComputationWrapper(execCtx, "", "root", nil, execCtx.proc, ses)
	require.NoError(t, err)
	require.Len(t, wrappers, 1)

	wrapper := wrappers[0].(*TxnComputationWrapper)
	require.True(t, wrapper.stmtBorrowed)
	wrapper.Free()
	require.Zero(t, stmt.freed, "an early binary EXECUTE error must not free the shared prepared AST")
}

func TestTxnComputationWrapperResetTransfersStatementOwnership(t *testing.T) {
	borrowed := &trackedStatement{}
	replanned := &trackedStatement{}
	wrapper := &TxnComputationWrapper{
		stmt:         borrowed,
		stmtBorrowed: true,
	}

	wrapper.ResetPlanAndStmt(replanned)
	require.Zero(t, borrowed.freed)
	require.False(t, wrapper.stmtBorrowed)

	wrapper.Free()
	require.Equal(t, 1, replanned.freed)
}

func TestCompilerContextReleasesFreshCloneForExplainExecute(t *testing.T) {
	ses, prepareStmt, cw, _ := newPreparedExecuteEnv(t, 101)
	defer prepareStmt.Close()
	ses.GetTxnCompileCtx().tcw = cw

	clone := &tree.CloneTable{}
	clone.SrcTable.ObjectName = "src"
	clone.CreateTable.Table.ObjectName = "dst"
	prepareStmt.cloneSQL = preparedCloneSQL(clone, "prepare_db")

	_, stmt, err := ses.GetTxnCompileCtx().InitExecuteStmtParam(
		&plan.Execute{Name: prepareStmt.Name},
	)
	require.NoError(t, err)
	require.Nil(t, stmt, "EXPLAIN consumes only the prepared plan and must release its fresh CLONE AST")
}

func TestInitExecuteStmtParamReusesStableSubscriptionSelect(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnv(t, 101)
	defer prepareStmt.Close()

	expected := &plan.ObjectRef{
		Server: 4, Db: 2, Obj: 3, SchemaName: "publisher_db", ObjName: "src",
		SubscriptionName: "sub", PubInfo: &plan.PubInfo{TenantId: 11},
	}
	prepareStmt.PreparePlan.GetDcl().GetPrepare().Schemas = []*plan.ObjectRef{expected}
	sentinel := compile.NewCompile(
		"", "", prepareStmt.Sql, "", "", nil,
		cw.proc, prepareStmt.PrepareStmt, false, nil, time.Now())
	prepareStmt.compile = sentinel

	retComp, _, _, _, _, err := initExecuteStmtParamWithResolver(
		execCtx, ses, cw, nil, prepareStmt.Name,
		func(string, string, *plan.Snapshot) (*plan.ObjectRef, *plan.TableDef, error) {
			return &plan.ObjectRef{
					Obj: 3, SchemaName: "publisher_db", ObjName: "src",
					SubscriptionName: "sub", PubInfo: &plan.PubInfo{TenantId: 11},
				},
				&plan.TableDef{DbId: 2, TblId: 3, Version: 4}, nil
		},
	)
	require.NoError(t, err)
	require.Same(t, sentinel, retComp)
	require.Same(t, sentinel, prepareStmt.compile)
}

func BenchmarkInitExecuteStmtParamReusesStableSubscriptionSelect(b *testing.B) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnv(b, 101)
	defer prepareStmt.Close()

	expected := &plan.ObjectRef{
		Server: 4, Db: 2, Obj: 3, SchemaName: "publisher_db", ObjName: "src",
		SubscriptionName: "sub", PubInfo: &plan.PubInfo{TenantId: 11},
	}
	prepareStmt.PreparePlan.GetDcl().GetPrepare().Schemas = []*plan.ObjectRef{expected}
	resolve := func(string, string, *plan.Snapshot) (*plan.ObjectRef, *plan.TableDef, error) {
		return &plan.ObjectRef{
				Obj: 3, SchemaName: "publisher_db", ObjName: "src",
				SubscriptionName: "sub", PubInfo: &plan.PubInfo{TenantId: 11},
			},
			&plan.TableDef{DbId: 2, TblId: 3, Version: 4}, nil
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, _, _, _, _, err := initExecuteStmtParamWithResolver(
			execCtx, ses, cw, nil, prepareStmt.Name, resolve)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestInitExecuteStmtParamRebuildsWhenTempTableMappingChanges(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnv(t, 102)
	defer prepareStmt.Close()

	oldColDefData := [][]byte{[]byte("old-int-column")}
	newColDefData := [][]byte{[]byte("new-varchar-column")}
	prepareStmt.ColDefData = oldColDefData
	execCtx.prepareColDef = oldColDefData
	w := execCtx.resper.MysqlRrWr().(*testMysqlWriter)
	w.makeColumnDefDataFunc = func(context.Context, []*plan.ColDef) ([][]byte, error) {
		return newColDefData, nil
	}

	oldPlan := prepareStmt.PreparePlan
	ses.AddTempTable("db1", "unrelated", "temp-unrelated")

	retComp, retPlan, retStmt, _, _, err := initExecuteStmtParam(
		execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	require.Nil(t, retComp)
	require.NotSame(t, oldPlan, prepareStmt.PreparePlan)
	require.Same(t, prepareStmt.PreparePlan.GetDcl().GetPrepare().Plan, retPlan)
	require.NotNil(t, retStmt)
	require.Equal(t, ses.GetTempTableVersion(), prepareStmt.tempTableVersion)
	require.Equal(t, newColDefData, prepareStmt.ColDefData)
	require.Equal(t, newColDefData, execCtx.prepareColDef)
}

func TestInitExecuteStmtParamRebuildsWhenProtocolVersionChanges(t *testing.T) {
	rt := moruntime.ServiceRuntime("")
	defer rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)

	for i, test := range []struct {
		name string
		from int64
		to   int64
	}{
		{name: "existing upgrade", from: defines.MORPCVersion4, to: defines.MORPCVersion5},
		{name: "existing rollback", from: defines.MORPCVersion5, to: defines.MORPCVersion4},
		{name: "upgrade", from: defines.MORPCVersion7, to: defines.MORPCVersion8},
		{name: "rollback", from: defines.MORPCVersion8, to: defines.MORPCVersion7},
		{name: "numeric prefix upgrade", from: defines.MORPCVersion24, to: defines.MORPCVersion25},
		{name: "numeric prefix rollback", from: defines.MORPCVersion25, to: defines.MORPCVersion24},
	} {
		t.Run(test.name, func(t *testing.T) {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, test.from)
			ses, prepareStmt, cw, execCtx := newPreparedExecuteEnv(t, uint32(107+i))
			defer prepareStmt.Close()
			require.Equal(t, test.from, prepareStmt.protocolVersion)
			oldPlan := prepareStmt.PreparePlan

			rt.SetGlobalVariables(moruntime.MOProtocolVersion, test.to)
			retComp, retPlan, retStmt, _, _, err := initExecuteStmtParam(
				execCtx, ses, cw, nil, prepareStmt.Name)
			require.NoError(t, err)
			require.Nil(t, retComp)
			require.NotNil(t, retPlan)
			require.NotNil(t, retStmt)
			require.NotSame(t, oldPlan, prepareStmt.PreparePlan)
			require.Equal(t, test.to, prepareStmt.protocolVersion)
		})
	}
}

func TestInitExecuteStmtParamKeepsOldStateWhenColumnMetadataRefreshFails(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnv(t, 103)
	defer prepareStmt.Close()

	oldPlan := prepareStmt.PreparePlan
	oldColDefData := [][]byte{[]byte("old-int-column")}
	prepareStmt.ColDefData = oldColDefData
	execCtx.prepareColDef = oldColDefData
	w := execCtx.resper.MysqlRrWr().(*testMysqlWriter)
	w.makeColumnDefDataFunc = func(context.Context, []*plan.ColDef) ([][]byte, error) {
		return nil, errors.New("column metadata refresh failed")
	}

	ses.AddTempTable("db1", "unrelated", "temp-unrelated")
	_, _, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
	require.EqualError(t, err, "column metadata refresh failed")
	require.Same(t, oldPlan, prepareStmt.PreparePlan)
	require.Equal(t, oldColDefData, prepareStmt.ColDefData)
	require.Equal(t, oldColDefData, execCtx.prepareColDef)
	require.NotEqual(t, ses.GetTempTableVersion(), prepareStmt.tempTableVersion)
}

func TestInitExecuteStmtParamRebuildsPreparedPlanWhenSQLModePresenceChanges(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnv(t, 106)
	defer prepareStmt.Close()

	originalPlan := prepareStmt.PreparePlan
	require.False(t, prepareStmt.NativeMode)

	execCtx.reqCtx = defines.AttachAccountId(execCtx.reqCtx, catalog.System_Account)
	require.NoError(t, ses.SetSessionSysVar(execCtx.reqCtx, "sql_mode", "MATRIXONE_NATIVE"))

	retComp, retPlan, retStmt, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	require.Nil(t, retComp)
	require.NotNil(t, retPlan)
	require.NotNil(t, retStmt)
	require.True(t, prepareStmt.NativeMode)
	require.NotSame(t, originalPlan, prepareStmt.PreparePlan)
}

func TestInitExecuteStmtParamRebuildsPreparedPlanWhenOnlyFullGroupByChanges(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnv(t, 109)
	defer prepareStmt.Close()

	execCtx.reqCtx = defines.AttachAccountId(execCtx.reqCtx, catalog.System_Account)
	require.NoError(t, ses.SetSessionSysVar(execCtx.reqCtx, "sql_mode", ""))
	prepareStmt.OnlyFullGroupBy = false
	prepareStmt.onlyFullGroupBySet = true
	originalPlan := prepareStmt.PreparePlan
	require.NoError(t, ses.SetSessionSysVar(execCtx.reqCtx, "sql_mode", "ONLY_FULL_GROUP_BY"))

	retComp, retPlan, retStmt, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	require.Nil(t, retComp)
	require.NotNil(t, retPlan)
	require.NotNil(t, retStmt)
	require.True(t, prepareStmt.OnlyFullGroupBy)
	require.NotSame(t, originalPlan, prepareStmt.PreparePlan)
}

func TestInitExecuteStmtParamBypassesButRetainsCachedTopologyForExplicitSchedulingIntent(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnv(t, 102)
	defer prepareStmt.Close()

	sentinel := compile.NewCompile(
		"", "", prepareStmt.Sql, "", "", nil,
		cw.proc, prepareStmt.PrepareStmt, false, nil, time.Now())
	prepareStmt.compile = sentinel
	require.NoError(t, ses.SetSessionSysVar(context.Background(), queryMaxWorkers, int64(2)))

	retComp, retPlan, retStmt, _, _, err := initExecuteStmtParam(
		execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	require.Nil(t, retComp)
	require.Same(t, sentinel, prepareStmt.compile)
	require.NotNil(t, retPlan)
	require.NotNil(t, retStmt)
}

func TestInitExecuteStmtParamBypassesCachedTopologyForPreparedSiriusExecution(t *testing.T) {
	for _, protocol := range []struct {
		name     string
		execPlan func(string) *plan.Execute
		stmtName func(string) string
	}{
		{
			name:     "binary",
			execPlan: func(string) *plan.Execute { return nil },
			stmtName: func(name string) string { return name },
		},
		{
			name:     "text",
			execPlan: func(name string) *plan.Execute { return &plan.Execute{Name: name} },
			stmtName: func(string) string { return "" },
		},
	} {
		t.Run(protocol.name, func(t *testing.T) {
			ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(
				t, 110, "/*+ SIDECAR */ select 1")
			defer prepareStmt.Close()

			sentinel := compile.NewCompile(
				"", "", prepareStmt.Sql, "", "", nil,
				cw.proc, prepareStmt.PrepareStmt, false, nil, time.Now())
			prepareStmt.compile = sentinel

			for range 2 {
				retComp, retPlan, retStmt, originSQL, _, err := initExecuteStmtParam(
					execCtx,
					ses,
					cw,
					protocol.execPlan(prepareStmt.Name),
					protocol.stmtName(prepareStmt.Name),
				)
				require.NoError(t, err)
				require.Nil(t, retComp)
				require.Same(t, sentinel, prepareStmt.compile)
				require.NotNil(t, retPlan)
				require.NotNil(t, retStmt)
				require.True(t, siriusStatementSelected(originSQL, retStmt))
			}
		})
	}
}

func TestCompileStatementContextsPreserveCounterWithoutLeakingSelection(t *testing.T) {
	for _, test := range []struct {
		name          string
		sql           string
		separateChild bool
	}{
		{name: "selected", sql: "/*+ SIDECAR */ select 1", separateChild: true},
		{name: "unselected", sql: "select 1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			counter := new(perfcounter.CounterSet)
			requestCtx, compileCtx := compileStatementContexts(
				context.Background(), test.sql, &tree.Select{}, counter)

			attached, ok := compileCtx.Value(perfcounter.CompilePlanMarkKey{}).(*perfcounter.CounterSet)
			require.True(t, ok)
			require.Same(t, counter, attached)
			require.Equal(t, test.separateChild, requestCtx != compileCtx)
			perfcounter.Update(compileCtx, func(set *perfcounter.CounterSet) {
				set.FileService.S3.Get.Add(1)
			})
			require.Equal(t, int64(1), counter.FileService.S3.Get.Load())
		})
	}

	requestCtx, _ := compileStatementContexts(
		context.Background(), "/*+ SIDECAR */ select 1", &tree.Select{}, new(perfcounter.CounterSet))
	requestCtx, unselectedCompileCtx := compileStatementContexts(
		requestCtx, "select 2", &tree.Select{}, new(perfcounter.CounterSet))
	require.True(t, requestCtx == unselectedCompileCtx,
		"the selected statement's Sirius marker must not enter its sibling's request context")
}

func TestRebuildPreparePlanUsesPreparedRootSQL(t *testing.T) {
	const preparedSQL = "create view v as select 1"
	const executeSQL = "execute prepared_view"
	ses, prepareStmt, _, execCtx := newPreparedExecuteEnvForSQL(t, 106, preparedSQL)
	defer prepareStmt.Close()
	prepareStmt.Name = "prepared_view"
	ses.SetSql(executeSQL)

	rebuilt, err := rebuildPreparePlan(
		execCtx,
		ses,
		prepareStmt,
		func(_ context.Context, _ FeSession, compilerCtx plan2.CompilerContext, stmt tree.Statement) (*plan2.Plan, error) {
			return plan2.BuildPlan(&preparedViewCompilerContext{CompilerContext: compilerCtx}, stmt, false)
		},
	)
	require.NoError(t, err)
	prepareStmt.PreparePlan = rebuilt
	requirePreparedViewRootSQL(t, prepareStmt, preparedSQL)
	require.Equal(t, executeSQL, ses.GetSql())
	require.Equal(t, executeSQL, ses.GetTxnCompileCtx().GetRootSql())
}

func TestRebuildPreparePlanUsesFreshCloneStatement(t *testing.T) {
	ses, prepareStmt, _, execCtx := newPreparedExecuteEnv(t, 108)
	defer prepareStmt.Close()
	prepareStmt.PrepareStmt.Free()
	clone := &tree.CloneTable{}
	clone.SrcTable.ObjectName = "src"
	clone.CreateTable.Table.ObjectName = "dst"
	prepareStmt.PrepareStmt = clone
	prepareStmt.cloneSQL = preparedCloneSQL(clone, "prepare_db")
	prepareStmt.defaultDatabase = "prepare_db"

	buildCount := 0
	buildFn := func(
		_ context.Context,
		_ FeSession,
		_ plan2.CompilerContext,
		stmt tree.Statement,
	) (*plan2.Plan, error) {
		buildCount++
		inner := stmt.(*tree.PrepareStmt).Stmt.(*tree.CloneTable)
		require.Equal(t, tree.Identifier("prepare_db"), inner.SrcTable.SchemaName)
		require.Equal(t, tree.Identifier("prepare_db"), inner.CreateTable.Table.SchemaName)
		inner.SrcTable.SchemaName = "planner_mutation"
		inner.CreateTable.Table.SchemaName = "planner_mutation"
		return &plan2.Plan{}, nil
	}

	_, err := rebuildPreparePlan(execCtx, ses, prepareStmt, buildFn)
	require.NoError(t, err)
	_, err = rebuildPreparePlan(execCtx, ses, prepareStmt, buildFn)
	require.NoError(t, err)
	require.Equal(t, 2, buildCount)
	require.Empty(t, clone.SrcTable.SchemaName)
	require.Empty(t, clone.CreateTable.Table.SchemaName)
}

func TestModeMismatchRebuildsPreparedViewWithPreparedRootSQL(t *testing.T) {
	const preparedSQL = "create view v as select 1"
	const executeSQL = "execute prepared_view"
	ses, prepareStmt, _, execCtx := newPreparedExecuteEnvForSQL(t, 107, preparedSQL)
	defer prepareStmt.Close()
	ses.SetSql(executeSQL)
	execCtx.reqCtx = defines.AttachAccountId(execCtx.reqCtx, catalog.System_Account)
	require.NoError(t, ses.SetSessionSysVar(execCtx.reqCtx, "sql_mode", "MATRIXONE_NATIVE"))
	modeMismatch := prepareStmt.NativeMode != ses.sqlModeHasMatrixOneNative()
	require.True(t, modeMismatch)
	require.True(t, preparePlanNeedsRebuild(false, modeMismatch, false))
	require.True(t, preparePlanNeedsRebuild(false, false, true))

	rebuilt, err := rebuildPreparePlan(
		execCtx,
		ses,
		prepareStmt,
		func(_ context.Context, _ FeSession, compilerCtx plan2.CompilerContext, stmt tree.Statement) (*plan2.Plan, error) {
			return plan2.BuildPlan(&preparedViewCompilerContext{CompilerContext: compilerCtx}, stmt, false)
		},
	)
	require.NoError(t, err)
	prepareStmt.PreparePlan = rebuilt
	requirePreparedViewRootSQL(t, prepareStmt, preparedSQL)
	require.Equal(t, executeSQL, ses.GetSql())
	require.Equal(t, executeSQL, ses.GetTxnCompileCtx().GetRootSql())
}

func TestTxnComputationWrapperRunPanicStillReleases(t *testing.T) {
	var released bool
	mockComp := &mockCompile{
		runFunc: func(uint64) (*util2.RunResult, error) {
			panic("run panic")
		},
		getPlanFunc: func() *plan.Plan {
			return nil
		},
		releaseFunc: func() {
			released = true
		},
	}
	cwft := &TxnComputationWrapper{compile: mockComp}

	assert.PanicsWithValue(t, "run panic", func() {
		_, _ = cwft.Run(100)
	})
	assert.True(t, released)
	assert.Nil(t, cwft.compile)
}

func TestTxnComputationWrapperPreservesSchedulingTraceOnCompileError(t *testing.T) {
	const failureCategory = "candidate-discovery"
	stmt := &motrace.StatementInfo{
		RequestAt:     time.Now(),
		Status:        motrace.StatementStatusSuccess,
		StatementType: "Select",
		SqlSourceType: "external_sql",
	}
	assert.True(t, motrace.StatementInfoFilter(stmt))
	ses := &Session{}
	ses.SetTStmt(stmt)
	cwft := &TxnComputationWrapper{ses: ses}
	attempt := cwft.schedulingTrace.StartAttempt()
	cwft.schedulingTrace.RecordFailure(attempt, failureCategory, schedule.Worker{})

	cwft.recordSchedulingTraceOnCompileError(context.Background())
	if assert.NotNil(t, stmt.ExecPlan) {
		defer stmt.ExecPlan.Free()
		jsonBytes := stmt.ExecPlan.Marshal(context.Background())
		var payload struct {
			Scheduling schedule.Trace `json:"scheduling"`
		}
		assert.NoError(t, json.Unmarshal(jsonBytes, &payload))
		assert.Equal(t, failureCategory, payload.Scheduling.Attempts[0].Failures[0].Category)
		assert.Nil(t, payload.Scheduling.Attempts[0].Failures[0].Worker)
	}
	assert.True(t, stmt.ResponseAt.IsZero())
	assert.False(t, motrace.StatementInfoFilter(stmt))
}

func TestTxnComputationWrapperDoesNotPersistNormalLocalTraceOnCompileError(t *testing.T) {
	stmt := &motrace.StatementInfo{
		RequestAt:     time.Now(),
		Status:        motrace.StatementStatusSuccess,
		StatementType: "Select",
		SqlSourceType: "external_sql",
	}
	assert.True(t, motrace.StatementInfoFilter(stmt))
	ses := &Session{}
	ses.SetTStmt(stmt)
	cwft := &TxnComputationWrapper{ses: ses}
	attempt := cwft.schedulingTrace.StartAttempt()
	cwft.schedulingTrace.RecordQuery(attempt, schedule.QueryDecision{
		ExecKind:  schedule.QueryExecTP,
		CurrentCN: schedule.Worker{ID: "local"},
		Workers:   schedule.Workers{{ID: "local"}},
		Reason:    schedule.ReasonLocalExecType,
		CandidateResolution: schedule.CandidateResolution{
			DiscoverySource: schedule.CandidateSourceNotRequired,
			PoolResolution:  schedule.PoolResolutionNotRequired,
		},
		CurrentCNPolicy: schedule.CurrentCNAllowed,
		Satisfied:       true,
	})

	cwft.recordSchedulingTraceOnCompileError(context.Background())
	assert.Nil(t, stmt.ExecPlan)
	assert.True(t, motrace.StatementInfoFilter(stmt))
}
