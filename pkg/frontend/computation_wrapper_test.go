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
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
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
		Name:                 stmtName,
		Sql:                  prepareString.Sql,
		PreparePlan:          preparePlan,
		PrepareStmt:          stmts[0],
		getFromSendLongData:  make(map[int]struct{}),
		protocolVersion:      currentProtocolVersion(proc),
		dynamicNumericParams: plan2.HasPreparedDynamicNumericParams(preparePlan.GetDcl().GetPrepare().Plan),
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
	_, err = ses.txnCompileCtx.ResolveVariableIsBin("missing", false, false)
	require.Error(t, err)
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
	require.Equal(t, plan2.ParamValue{Value: "AB\x00\x00", IsBin: true, RuntimeType: types.T_varchar}, cw.paramVals[0])
	require.Equal(t, plan2.ParamValue{Value: "text", IsBin: false, RuntimeType: types.T_varchar}, cw.paramVals[1])

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

func TestPreparedPaginationUsesRuntimeParamType(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(t, 109, "select 1 limit ?")
	defer prepareStmt.Close()

	execPlan := &plan.Execute{
		Name: prepareStmt.Name,
		Args: []*plan.Expr{{Expr: &plan.Expr_V{V: &plan.VarRef{Name: "limit_param"}}}},
	}
	execute := func(value any) error {
		require.NoError(t, ses.SetUserDefinedVar("limit_param", value, ""))
		_, _, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, execPlan, "")
		return err
	}

	err := execute("3")
	require.ErrorContains(t, err, "Incorrect arguments to EXECUTE")
	moErr, ok := err.(*moerr.Error)
	require.True(t, ok)
	require.Equal(t, moerr.ER_WRONG_ARGUMENTS, moErr.MySQLCode())
	require.NoError(t, execute(int64(2)), "a rejected execution must not poison the prepared plan")
}

func TestPreparedBinaryParamRuntimeType(t *testing.T) {
	tests := []struct {
		name     string
		typ      defines.MysqlType
		unsigned bool
		want     types.T
	}{
		{name: "bit", typ: defines.MYSQL_TYPE_BIT, want: types.T_bit},
		{name: "tiny integer", typ: defines.MYSQL_TYPE_TINY, want: types.T_int8},
		{name: "short integer", typ: defines.MYSQL_TYPE_SHORT, want: types.T_int16},
		{name: "year", typ: defines.MYSQL_TYPE_YEAR, want: types.T_int16},
		{name: "medium integer", typ: defines.MYSQL_TYPE_INT24, want: types.T_int32},
		{name: "long integer", typ: defines.MYSQL_TYPE_LONG, want: types.T_int32},
		{name: "signed integer", typ: defines.MYSQL_TYPE_LONGLONG, want: types.T_int64},
		{name: "unsigned tiny integer", typ: defines.MYSQL_TYPE_TINY, unsigned: true, want: types.T_uint8},
		{name: "unsigned short integer", typ: defines.MYSQL_TYPE_SHORT, unsigned: true, want: types.T_uint16},
		{name: "unsigned long integer", typ: defines.MYSQL_TYPE_LONG, unsigned: true, want: types.T_uint32},
		{name: "unsigned longlong integer", typ: defines.MYSQL_TYPE_LONGLONG, unsigned: true, want: types.T_uint64},
		{name: "float", typ: defines.MYSQL_TYPE_FLOAT, want: types.T_float32},
		{name: "double", typ: defines.MYSQL_TYPE_DOUBLE, want: types.T_float64},
		{name: "legacy decimal", typ: defines.MYSQL_TYPE_DECIMAL, want: types.T_decimal128},
		{name: "decimal", typ: defines.MYSQL_TYPE_NEWDECIMAL, want: types.T_decimal128},
		{name: "string", typ: defines.MYSQL_TYPE_VAR_STRING, want: types.T_varchar},
		{name: "null", typ: defines.MYSQL_TYPE_NULL, want: types.T_any},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			flag := byte(0)
			if test.unsigned {
				flag = 0x80
			}
			require.Equal(t, test.want, preparedBinaryParamRuntimeType([]byte{byte(test.typ), flag}, 0))
		})
	}
	require.Equal(t, types.T_any, preparedBinaryParamRuntimeType(nil, 0))
	require.Equal(t, types.T_any, preparedBinaryParamRuntimeType([]byte{byte(defines.MYSQL_TYPE_LONG)}, -1))
	require.Equal(t, types.T_any, preparedBinaryParamRuntimeType([]byte{byte(defines.MYSQL_TYPE_LONG)}, 1))
}

func TestPreparedTextParamRuntimeType(t *testing.T) {
	require.Equal(t, types.T_int8, preparedTextParamRuntimeType(int8(1)))
	require.Equal(t, types.T_int16, preparedTextParamRuntimeType(int16(1)))
	require.Equal(t, types.T_int32, preparedTextParamRuntimeType(int32(1)))
	require.Equal(t, types.T_int64, preparedTextParamRuntimeType(int64(1)))
	require.Equal(t, types.T_uint8, preparedTextParamRuntimeType(uint8(1)))
	require.Equal(t, types.T_uint16, preparedTextParamRuntimeType(uint16(1)))
	require.Equal(t, types.T_uint32, preparedTextParamRuntimeType(uint32(1)))
	require.Equal(t, types.T_uint64, preparedTextParamRuntimeType(uint64(1)))
	require.Equal(t, types.T_float32, preparedTextParamRuntimeType(float32(1e10)))
	require.Equal(t, types.T_float64, preparedTextParamRuntimeType(float64(1e100)))
	require.Equal(t, types.T_varchar, preparedTextParamRuntimeType("1e10"))
	require.Equal(t, types.T_varchar, preparedTextParamRuntimeType([]byte("2.5")))
}

func TestPreparedBinaryPaginationUsesProtocolParamType(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(t, 110, "select 1 limit ?")
	defer prepareStmt.Close()
	defer cw.proc.SetPrepareParams(nil)

	params := vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(params, []byte("2"), false, cw.proc.Mp()))
	prepareStmt.params = params
	prepareStmt.ParamTypes = []byte{byte(defines.MYSQL_TYPE_VAR_STRING), 0}

	_, _, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
	require.ErrorContains(t, err, "Incorrect arguments to EXECUTE")
	require.Nil(t, cw.proc.GetPrepareParams(), "a rejected borrowed binary parameter must not remain on the process")

	prepareStmt.ParamTypes = []byte{byte(defines.MYSQL_TYPE_LONGLONG), 0}
	_, _, _, _, _, err = initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	require.Same(t, params, cw.proc.GetPrepareParams())
}

func TestPreparedDynamicNumericPlanUsesCurrentTextAndBinaryValue(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(t, 111, "select ? + 1")
	defer prepareStmt.Close()
	defer cw.proc.SetPrepareParams(nil)
	canonical := prepareStmt.PreparePlan.GetDcl().GetPrepare().Plan
	require.True(t, plan2.HasPreparedDynamicNumericParams(canonical))

	execPlan := &plan.Execute{
		Name: prepareStmt.Name,
		Args: []*plan.Expr{{Expr: &plan.Expr_V{V: &plan.VarRef{Name: "numeric_param"}}}},
	}
	for _, value := range []string{
		"12345678901234567890123456789012345678901234567890123456789012345",
		"0.123456789012345678901234567890",
	} {
		require.NoError(t, ses.setUserDefinedVarWithType("numeric_param", value, "", false, types.T_decimal256))
		comp, executionPlan, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, execPlan, "")
		require.NoError(t, err)
		require.Nil(t, comp)
		require.False(t, plan2.HasPreparedDynamicNumericParams(executionPlan))
	}

	for _, value := range []float64{1e10, 1e-10, 1e100, -1e10} {
		require.NoError(t, ses.SetUserDefinedVar("numeric_param", value, ""))
		comp, executionPlan, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, execPlan, "")
		require.NoError(t, err)
		require.Nil(t, comp)
		root := executionPlan.GetQuery().Nodes[executionPlan.GetQuery().Steps[0]]
		require.True(t, types.T(root.ProjectList[0].Typ.Id).IsFloat())
	}
	for _, value := range []string{
		"2.5", "9007199254740993", "1e10", "1e-10", "-1e10",
		" 1e10 ", "\t-1e10", "1e-10 ", "1e-10000", "-1e-10000",
	} {
		require.NoError(t, ses.SetUserDefinedVar("numeric_param", value, ""))
		comp, executionPlan, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, execPlan, "")
		require.NoError(t, err)
		require.Nil(t, comp)
		root := executionPlan.GetQuery().Nodes[executionPlan.GetQuery().Steps[0]]
		require.True(t, types.T(root.ProjectList[0].Typ.Id).IsFloat())
	}

	for _, value := range []int64{2, 3} {
		require.NoError(t, ses.SetUserDefinedVar("numeric_param", value, ""))
		_, executionPlan, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, execPlan, "")
		require.NoError(t, err)
		root := executionPlan.GetQuery().Nodes[executionPlan.GetQuery().Steps[0]]
		resultType := types.T(root.ProjectList[0].Typ.Id)
		require.True(t, resultType.IsInteger(), resultType.String())
	}
	require.NoError(t, ses.SetUserDefinedVar("numeric_param", int64(math.MaxInt64), ""))
	_, executionPlan, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, execPlan, "")
	require.NoError(t, err)
	root := executionPlan.GetQuery().Nodes[executionPlan.GetQuery().Steps[0]]
	require.Equal(t, int32(types.T_int64), root.ProjectList[0].Typ.Id)

	for _, value := range []string{
		"-12345678901234567890123456789012345678901234567890123456789012345",
		"-0.123456789012345678901234567890",
	} {
		params := vector.NewVec(types.T_text.ToType())
		require.NoError(t, vector.AppendBytes(params, []byte(value), false, cw.proc.Mp()))
		prepareStmt.params = params
		prepareStmt.ParamTypes = []byte{byte(defines.MYSQL_TYPE_NEWDECIMAL), 0}
		comp, executionPlan, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
		require.NoError(t, err)
		require.Nil(t, comp)
		require.False(t, plan2.HasPreparedDynamicNumericParams(executionPlan))
		cw.proc.SetPrepareParams(nil)
		params.Free(cw.proc.Mp())
		prepareStmt.params = nil
	}

	for _, test := range []struct {
		value string
		typ   defines.MysqlType
	}{
		{value: "1e+10", typ: defines.MYSQL_TYPE_FLOAT},
		{value: "1e-10", typ: defines.MYSQL_TYPE_DOUBLE},
		{value: "1e+100", typ: defines.MYSQL_TYPE_DOUBLE},
		{value: "-1e+10", typ: defines.MYSQL_TYPE_DOUBLE},
		{value: "+Inf", typ: defines.MYSQL_TYPE_DOUBLE},
		{value: "NaN", typ: defines.MYSQL_TYPE_DOUBLE},
	} {
		params := vector.NewVec(types.T_text.ToType())
		require.NoError(t, vector.AppendBytes(params, []byte(test.value), false, cw.proc.Mp()))
		prepareStmt.params = params
		prepareStmt.ParamTypes = []byte{byte(test.typ), 0}
		comp, executionPlan, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
		require.NoError(t, err)
		require.Nil(t, comp)
		root := executionPlan.GetQuery().Nodes[executionPlan.GetQuery().Steps[0]]
		require.True(t, types.T(root.ProjectList[0].Typ.Id).IsFloat())
		cw.proc.SetPrepareParams(nil)
		params.Free(cw.proc.Mp())
		prepareStmt.params = nil
	}
	for _, value := range []string{
		"2.5", "9007199254740993", "1e10", "1e-10", "-1e10",
		" 1e10 ", "\t-1e10", "1e-10 ", "1e-10000", "-1e-10000",
	} {
		params := vector.NewVec(types.T_text.ToType())
		require.NoError(t, vector.AppendBytes(params, []byte(value), false, cw.proc.Mp()))
		prepareStmt.params = params
		prepareStmt.ParamTypes = []byte{byte(defines.MYSQL_TYPE_VAR_STRING), 0}
		comp, executionPlan, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
		require.NoError(t, err)
		require.Nil(t, comp)
		root := executionPlan.GetQuery().Nodes[executionPlan.GetQuery().Steps[0]]
		require.True(t, types.T(root.ProjectList[0].Typ.Id).IsFloat())
		cw.proc.SetPrepareParams(nil)
		params.Free(cw.proc.Mp())
		prepareStmt.params = nil
	}

	for _, test := range []struct {
		value    string
		unsigned bool
		wantType types.T
	}{
		{value: strconv.FormatInt(math.MaxInt64, 10), wantType: types.T_int64},
		{value: strconv.FormatUint(math.MaxUint64, 10), unsigned: true, wantType: types.T_uint64},
	} {
		params := vector.NewVec(types.T_text.ToType())
		require.NoError(t, vector.AppendBytes(params, []byte(test.value), false, cw.proc.Mp()))
		prepareStmt.params = params
		flag := byte(0)
		if test.unsigned {
			flag = 0x80
		}
		prepareStmt.ParamTypes = []byte{byte(defines.MYSQL_TYPE_LONGLONG), flag}
		_, executionPlan, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
		require.NoError(t, err)
		root := executionPlan.GetQuery().Nodes[executionPlan.GetQuery().Steps[0]]
		require.Equal(t, int32(test.wantType), root.ProjectList[0].Typ.Id)
		cw.proc.SetPrepareParams(nil)
		params.Free(cw.proc.Mp())
		prepareStmt.params = nil
	}

	require.True(t, plan2.HasPreparedDynamicNumericParams(canonical))
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
	params, _, _, err := buildExecuteUserParams(ses, cw.proc, execPlan.Args)
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
		{"v1": int64(10)},
		{"v1": int64(20), "inner": int64(30)},
	}
	execCtx.reqCtx = context.WithValue(execCtx.reqCtx, defines.VarScopeKey{}, &scopes)
	execCtx.reqCtx = context.WithValue(execCtx.reqCtx, defines.InSp{}, true)

	value, err := ses.txnCompileCtx.ResolveVariable("V1", false, false)
	require.NoError(t, err)
	require.Equal(t, int64(20), value)
	isBin, err := ses.txnCompileCtx.ResolveVariableIsBin("V1", false, false)
	require.NoError(t, err)
	require.False(t, isBin)

	value, err = ses.txnCompileCtx.ResolveVariable("session_only", false, false)
	require.NoError(t, err)
	require.Equal(t, "session-binary", value)
	isBin, err = ses.txnCompileCtx.ResolveVariableIsBin("session_only", false, false)
	require.NoError(t, err)
	require.True(t, isBin)
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
	params, paramVals, paramIsBin, err := buildExecuteUserParams(ses, cw.proc, args)
	require.NoError(t, err)
	defer params.Free(cw.proc.Mp())

	require.Equal(t, []bool{false, false, true}, paramIsBin)
	require.Equal(t, []any{
		plan2.ParamValue{Value: int64(10), IsBin: false, RuntimeType: types.T_int64},
		plan2.ParamValue{Value: int64(20), IsBin: false, RuntimeType: types.T_int64},
		plan2.ParamValue{Value: "session-binary", IsBin: true, RuntimeType: types.T_varchar},
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
