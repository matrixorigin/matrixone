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
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
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
	require.Equal(t, plan2.ParamValue{Value: "AB\x00\x00", IsBin: true}, cw.paramVals[0])
	require.Equal(t, plan2.ParamValue{Value: "text", IsBin: false}, cw.paramVals[1])

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

func TestPreparedParamBindingType(t *testing.T) {
	tests := []struct {
		name  string
		kind  vector.PrepareParamKind
		value []byte
		want  types.T
	}{
		{name: "native float large", kind: vector.PrepareParamFloat, value: []byte("1e100"), want: types.T_float64},
		{name: "native float small", kind: vector.PrepareParamFloat, value: []byte("1e-40"), want: types.T_float64},
		{name: "numeric text", value: []byte("1.234567"), want: types.T_text},
		{name: "numeric text range", value: []byte("1e1000"), want: types.T_text},
		{name: "integer", kind: vector.PrepareParamInteger, value: []byte("42"), want: types.T_text},
		{name: "signed integer", kind: vector.PrepareParamInteger, value: []byte("-42"), want: types.T_text},
		{name: "decimal", kind: vector.PrepareParamDecimal, value: []byte("1.2"), want: types.T_text},
		{name: "boolean", kind: vector.PrepareParamBoolean, value: []byte("1"), want: types.T_text},
		{name: "time value", value: []byte("2026-08-10 12:34:56"), want: types.T_text},
		{name: "ordinary string", value: []byte("matrixone"), want: types.T_text},
		{name: "nan string", value: []byte("NaN"), want: types.T_text},
		{name: "infinity string", value: []byte("+Inf"), want: types.T_text},
		{name: "empty string", value: []byte(""), want: types.T_text},
		{name: "null", value: nil, want: types.T_text},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, preparedParamBindingType(test.kind, test.value).Oid)
		})
	}
	for _, kind := range []vector.PrepareParamKind{
		vector.PrepareParamInteger,
		vector.PrepareParamBoolean,
	} {
		binding := preparedParamBindingType(kind, []byte("1"))
		require.Equal(t, preparedNumericTextBindingCharset, binding.Charset)
		require.Equal(t, preparedNumericProtocolExact, binding.Size)
		require.Equal(t, int32(65), binding.Width)
		require.Equal(t, int32(30), binding.Scale)
	}
}

func TestNativeDecimalPreparedParamBindingUsesStablePayloadCategory(t *testing.T) {
	tests := []struct {
		value     string
		wantMode  int32
		wantWidth int32
		wantScale int32
	}{
		{value: "123456789012345678901234567890123456", wantMode: preparedNumericExact, wantWidth: 36},
		{value: "1E+35", wantMode: preparedNumericExact, wantWidth: 36},
		{value: "1E-31", wantMode: preparedNumericTextPrefix, wantWidth: 65, wantScale: 30},
		{value: "1E-40", wantMode: preparedNumericTextPrefix, wantWidth: 65, wantScale: 30},
		{value: "  123456789012345678901234567890123456", wantMode: preparedNumericExact, wantWidth: 36},
		{value: "\t123456789012345678901234567890123456", wantMode: preparedNumericExact, wantWidth: 36},
		{value: "-12.3400", wantMode: preparedNumericTextPrefix, wantWidth: 65, wantScale: 30},
		{value: "0.001", wantMode: preparedNumericTextPrefix, wantWidth: 65, wantScale: 30},
		{value: "000123", wantMode: preparedNumericTextPrefix, wantWidth: 65, wantScale: 30},
		{value: "0.00", wantMode: preparedNumericTextPrefix, wantWidth: 65, wantScale: 30},
		{value: "0", wantMode: preparedNumericTextPrefix, wantWidth: 65, wantScale: 30},
	}
	for _, test := range tests {
		t.Run(test.value, func(t *testing.T) {
			binding := preparedParamBindingType(vector.PrepareParamDecimal, []byte(test.value))
			require.Equal(t, types.T_text, binding.Oid)
			require.Equal(t, preparedNumericTextBindingCharset, binding.Charset)
			require.Equal(t, test.wantMode, binding.Size)
			require.Equal(t, test.wantWidth, binding.Width)
			require.Equal(t, test.wantScale, binding.Scale)
		})
	}
}

func TestNormalizePreparedDecimalPayload(t *testing.T) {
	for _, test := range []struct {
		input string
		want  string
	}{
		{input: "1e+35", want: "1e+35"},
		{input: "1E+35", want: "1e+35"},
		{input: "1E-30", want: "1e-30"},
		{input: "  123", want: "123"},
		{input: "\t123", want: "123"},
		{input: "\v\f\r\n-1E+1", want: "-1e+1"},
		{input: "12.3tail", want: "12.3"},
		{input: "", want: "0"},
		{input: "abc", want: "0"},
	} {
		t.Run(test.input, func(t *testing.T) {
			require.Equal(t, test.want, string(normalizePreparedDecimalPayload([]byte(test.input))))
		})
	}
}

func TestPreparedParamBindingCategoryIgnoresExactDecimalDomain(t *testing.T) {
	require.True(t, preparedParamBindingCategoryEqual(
		types.New(types.T_decimal256, 7, 6), types.New(types.T_decimal256, 13, 2)))
	left := types.T_text.ToType()
	left.Charset = preparedNumericTextBindingCharset
	left.Size = preparedNumericTextPrefix
	left.Width, left.Scale = 7, 6
	right := left
	right.Width, right.Scale = 13, 2
	require.True(t, preparedParamBindingCategoryEqual(left, right))
	right.Size = preparedNumericTextFloat
	require.False(t, preparedParamBindingCategoryEqual(left, right))

	exact := left
	exact.Size = preparedNumericExact
	exact.Width, exact.Scale = 46, 10
	require.True(t, preparedParamBindingCategoryEqual(exact, exact))
	differentExact := exact
	differentExact.Scale = 11
	require.False(t, preparedParamBindingCategoryEqual(exact, differentExact))
}

func TestPreparedDecimalBindingUsesStableNonNarrowingCategories(t *testing.T) {
	tests := []struct {
		name      string
		width     int32
		scale     int32
		full      bool
		exponent  bool
		wantMode  int32
		wantWidth int32
		wantScale int32
	}{
		{name: "ordinary", width: 13, scale: 2, full: true, wantMode: preparedNumericTextPrefix, wantWidth: 65, wantScale: 30},
		{name: "wide 67 plus 9", width: 76, scale: 9, full: true, wantMode: preparedNumericExact, wantWidth: 76, wantScale: 9},
		{name: "overflowing numeric prefix", width: 101, full: false, exponent: true, wantMode: preparedNumericPrefixMax, wantWidth: 74, wantScale: 9},
		{name: "65 integral digits remain exact", width: 65, full: true, wantMode: preparedNumericExact, wantWidth: 65},
		{name: "76 integral digits remain exact", width: 76, full: true, wantMode: preparedNumericExact, wantWidth: 76},
		{name: "complete huge exponent", width: 101, full: true, exponent: true, wantMode: preparedNumericTextFloat},
		{name: "complete 77 digit ordinary", width: 77, full: true, wantMode: preparedNumericTextFloat},
		{name: "complete 77 digit fractional", width: 77, scale: 41, full: true, wantMode: preparedNumericTextFloat},
		{name: "36 integral plus 10 scale", width: 46, scale: 10, full: true, wantMode: preparedNumericExact, wantWidth: 46, wantScale: 10},
		{name: "36 integral plus 9 scale", width: 45, scale: 9, full: true, wantMode: preparedNumericExact, wantWidth: 45, wantScale: 9},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := preparedDecimalBindingType(test.width, test.scale, test.full, test.exponent)
			require.Equal(t, test.wantMode, got.Size)
			require.Equal(t, test.wantWidth, got.Width)
			require.Equal(t, test.wantScale, got.Scale)
		})
	}
}

func TestNormalizedPreparedDecimalPayloadExecutes(t *testing.T) {
	for _, test := range []struct {
		input string
		typ   types.Type
		want  string
	}{
		{input: "1E+1", typ: types.New(types.T_decimal256, 2, 0), want: "10"},
		{input: "1E+35", typ: types.New(types.T_decimal256, 36, 0), want: "100000000000000000000000000000000000"},
		{input: "1E-30", typ: types.New(types.T_decimal256, 30, 30), want: "0.000000000000000000000000000001"},
		{input: "  123456789012345678901234567890123456", typ: types.New(types.T_decimal256, 36, 0), want: "123456789012345678901234567890123456"},
		{input: "\t123456789012345678901234567890123456", typ: types.New(types.T_decimal256, 36, 0), want: "123456789012345678901234567890123456"},
	} {
		t.Run(test.input, func(t *testing.T) {
			value, err := getDecimal256FromRowValue(normalizePreparedDecimalPayload([]byte(test.input)), test.typ)
			require.NoError(t, err)
			require.Equal(t, test.want, value.Format(test.typ.Scale))
		})
	}
}

func TestPreparedNumericTextDomainIsBoundedAndClassified(t *testing.T) {
	tests := []struct {
		value           string
		wantWidth       int32
		wantScale       int32
		wantFull        bool
		wantExponent    bool
		wantBindingMode int32
		wantBindWidth   int32
		wantBindScale   int32
	}{
		{value: "1.234567", wantWidth: 7, wantScale: 6, wantFull: true, wantBindingMode: preparedNumericTextPrefix, wantBindWidth: 65, wantBindScale: 30},
		{value: "12.5tail", wantWidth: 3, wantScale: 1, wantBindingMode: preparedNumericTextPrefix, wantBindWidth: 65, wantBindScale: 30},
		{value: "abc", wantWidth: 1, wantBindingMode: preparedNumericTextPrefix, wantBindWidth: 65, wantBindScale: 30},
		{value: "001.200e2", wantWidth: 3, wantFull: true, wantExponent: true, wantBindingMode: preparedNumericTextPrefix},
		{value: "0.1e35", wantWidth: 35, wantFull: true, wantExponent: true, wantBindingMode: preparedNumericTextPrefix},
		{value: ".1e35", wantWidth: 35, wantFull: true, wantExponent: true, wantBindingMode: preparedNumericTextPrefix},
		{value: "0.0001e35", wantWidth: 32, wantFull: true, wantExponent: true, wantBindingMode: preparedNumericTextPrefix},
		{value: "0e100", wantWidth: 1, wantFull: true, wantExponent: true, wantBindingMode: preparedNumericTextPrefix},
		{value: "0e-100", wantWidth: 1, wantFull: true, wantExponent: true, wantBindingMode: preparedNumericTextPrefix},
		{value: "000000000000000000000000000000000000000000000000000000000000000000000000000001", wantWidth: 1, wantFull: true, wantBindingMode: preparedNumericTextPrefix},
		{value: "\v1.25", wantWidth: 3, wantScale: 2, wantFull: true, wantBindingMode: preparedNumericTextPrefix},
		{value: "\f1.25", wantWidth: 3, wantScale: 2, wantFull: true, wantBindingMode: preparedNumericTextPrefix},
		{value: "123456789012345678901234567890123456", wantWidth: 36, wantFull: true, wantBindingMode: preparedNumericExact},
		{value: "999999999999999999999999999999999999.0000000000", wantWidth: 36, wantFull: true, wantBindingMode: preparedNumericExact},
		{value: "10000000000000000000000000000000000000000000000000000000000000000000", wantWidth: 68, wantFull: true, wantBindingMode: preparedNumericExact},
		{value: "10000000000000000000000000000000000000000000000000000000000000000000000000000", wantWidth: 77, wantFull: true, wantBindingMode: preparedNumericTextFloat},
		{value: "1e35", wantWidth: 36, wantFull: true, wantExponent: true, wantBindingMode: preparedNumericWide},
		{value: "1e100tail", wantWidth: 77, wantExponent: true, wantBindingMode: preparedNumericPrefixMax},
		{value: "1e100", wantWidth: 77, wantFull: true, wantExponent: true, wantBindingMode: preparedNumericTextFloat},
		{value: "1e-31", wantWidth: 31, wantScale: 31, wantFull: true, wantExponent: true, wantBindingMode: preparedNumericTextPrefix, wantBindWidth: 65, wantBindScale: 30},
		{value: "1e999999999999999999999999999999", wantWidth: 77, wantFull: true, wantExponent: true, wantBindingMode: preparedNumericTextFloat},
	}
	for _, test := range tests {
		t.Run(test.value, func(t *testing.T) {
			width, scale, full, exponent := preparedNumericTextDomain([]byte(test.value))
			require.Equal(t, test.wantWidth, width)
			require.Equal(t, test.wantScale, scale)
			require.Equal(t, test.wantFull, full)
			require.Equal(t, test.wantExponent, exponent)
			binding := preparedParamBindingType(vector.PrepareParamNone, []byte(test.value))
			require.Equal(t, preparedNumericTextBindingCharset, binding.Charset)
			require.Equal(t, test.wantBindingMode, binding.Size)
			if test.wantBindingMode == preparedNumericTextPrefix {
				if test.wantBindWidth != 0 {
					require.Equal(t, test.wantBindWidth, binding.Width)
					require.Equal(t, test.wantBindScale, binding.Scale)
				}
			}
		})
	}
}

func TestPreparedParamBindingTypesSkipPlansWithoutDependencies(t *testing.T) {
	// A nil vector is deliberately safe here only when dependency inspection
	// happens before parameter value access. This also proves the fast path does
	// not allocate a result slice.
	require.Nil(t, preparedParamBindingTypes(nil, nil, nil, 1))
}

func TestPreparedExecuteParamStateOwnership(t *testing.T) {
	_, prepareStmt, cw, _ := newPreparedExecuteEnv(t, 112)
	defer prepareStmt.Close()
	proc := cw.proc

	params := vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(params, []byte("owned"), false, proc.Mp()))
	state := &preparedExecuteParamState{params: params, owned: true}
	state.release(proc)
	require.False(t, state.owned)
	require.Nil(t, state.params)
	require.Zero(t, params.Length())
	require.Nil(t, params.GetData())
	state.release(proc)

	transferred := vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(transferred, []byte("transferred"), false, proc.Mp()))
	state = &preparedExecuteParamState{params: transferred, owned: true}
	state.apply(proc)
	require.False(t, state.owned)
	require.Same(t, transferred, proc.GetPrepareParams())
	state.release(proc)
	require.Equal(t, 1, transferred.Length(), "release after transfer must not free the process-owned vector")
	proc.SetPrepareParams(nil)
	require.Zero(t, transferred.Length())
	require.Nil(t, transferred.GetData())
}

func TestInitExecuteStmtParamRebuildsForRuntimeBindingCategory(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(
		t, 110, "select coalesce(?, cast(2 as decimal(10,2)))")
	defer func() {
		cw.proc.SetPrepareParams(nil)
		prepareStmt.Close()
	}()

	prepareStmt.params = vector.NewVec(types.T_text.ToType())
	setParam := func(value string, mysqlType defines.MysqlType) {
		prepareStmt.params.CleanOnlyData()
		require.NoError(t, vector.AppendBytes(prepareStmt.params, []byte(value), false, cw.proc.Mp()))
		prepareStmt.ParamTypes = []byte{byte(mysqlType), 0}
	}
	execute := func() (*plan.Plan, plan.Type) {
		_, queryPlan, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
		require.NoError(t, err)
		columns := plan2.GetResultColumnsFromPlan(queryPlan)
		require.Len(t, columns, 1)
		return prepareStmt.PreparePlan, columns[0].Typ
	}

	setParam("1e100", defines.MYSQL_TYPE_DOUBLE)
	floatPlan, resultType := execute()
	require.Equal(t, int32(types.T_float64), resultType.Id)
	require.Equal(t, []types.Type{types.T_float64.ToType()}, prepareStmt.paramBindingTypes)
	_, found := ses.GetTxnCompileCtx().ResolvePreparedParamBindingType(0)
	require.False(t, found, "temporary binding hints must not escape the rebuild generation")

	setParam("1e-40", defines.MYSQL_TYPE_DOUBLE)
	sameFloatPlan, resultType := execute()
	require.Same(t, floatPlan, sameFloatPlan, "the same runtime category must reuse its plan")
	require.Equal(t, int32(types.T_float64), resultType.Id)

	setParam("2026-08-10 12:34:56", defines.MYSQL_TYPE_STRING)
	decimalPlan, resultType := execute()
	require.NotSame(t, floatPlan, decimalPlan)
	require.True(t, types.T(resultType.Id).IsDecimal())
	require.Len(t, prepareStmt.paramBindingTypes, 1)
	require.Equal(t, preparedNumericTextPrefix, prepareStmt.paramBindingTypes[0].Size)

	setParam("1.234567", defines.MYSQL_TYPE_VAR_STRING)
	exactDecimalPlan, resultType := execute()
	require.Same(t, decimalPlan, exactDecimalPlan,
		"exact DECIMAL width and scale changes must reuse the stable plan")
	require.True(t, types.T(resultType.Id).IsDecimal())
	require.Len(t, prepareStmt.paramBindingTypes, 1)
	require.Equal(t, preparedNumericTextPrefix, prepareStmt.paramBindingTypes[0].Size)
	require.Equal(t, decimalPlan.GetDcl().GetPrepare().Plan, exactDecimalPlan.GetDcl().GetPrepare().Plan)

	setParam("1e100", defines.MYSQL_TYPE_DOUBLE)
	secondFloatPlan, resultType := execute()
	require.NotSame(t, exactDecimalPlan, secondFloatPlan)
	require.Equal(t, int32(types.T_float64), resultType.Id)

	w := execCtx.resper.MysqlRrWr().(*testMysqlWriter)
	w.makeColumnDefDataFunc = func(context.Context, []*plan.ColDef) ([][]byte, error) {
		return nil, errors.New("column metadata refresh failed")
	}
	setParam("2026-08-10 12:34:56", defines.MYSQL_TYPE_STRING)
	_, _, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
	require.EqualError(t, err, "column metadata refresh failed")
	require.Same(t, secondFloatPlan, prepareStmt.PreparePlan)
	require.Equal(t, []types.Type{types.T_float64.ToType()}, prepareStmt.paramBindingTypes)
	_, found = ses.GetTxnCompileCtx().ResolvePreparedParamBindingType(0)
	require.False(t, found, "failed rebuild must clear temporary binding hints")
}

func TestInitExecuteStmtParamUsesNativeDecimalPayloadDomain(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(
		t, 117, "select coalesce(?, cast(2 as decimal(10,2)))")
	defer func() {
		cw.proc.SetPrepareParams(nil)
		prepareStmt.Close()
	}()

	prepareStmt.params = vector.NewVec(types.T_text.ToType())
	tests := []struct {
		value     string
		wantType  types.T
		wantWidth int32
		wantScale int32
	}{
		{value: "123456789012345678901234567890123456", wantType: types.T_decimal128, wantWidth: 38, wantScale: 2},
		{value: "1E+35", wantType: types.T_decimal128, wantWidth: 38, wantScale: 2},
		{value: "1E-31", wantType: types.T_decimal256, wantWidth: 65, wantScale: 30},
		{value: "1E-40", wantType: types.T_decimal256, wantWidth: 65, wantScale: 30},
		{value: "999999999999999999999999999999999999.1234567890", wantType: types.T_decimal256, wantWidth: 46, wantScale: 10},
	}
	for _, mysqlType := range []defines.MysqlType{defines.MYSQL_TYPE_DECIMAL, defines.MYSQL_TYPE_NEWDECIMAL} {
		for _, test := range tests {
			t.Run(fmt.Sprintf("type_%d/%s", mysqlType, test.value), func(t *testing.T) {
				prepareStmt.ParamTypes = []byte{byte(mysqlType), 0}
				prepareStmt.params.CleanOnlyData()
				require.NoError(t, vector.AppendBytes(
					prepareStmt.params, []byte(test.value), false, cw.proc.Mp()))
				_, queryPlan, _, _, _, err := initExecuteStmtParam(
					execCtx, ses, cw, nil, prepareStmt.Name)
				require.NoError(t, err)
				columns := plan2.GetResultColumnsFromPlan(queryPlan)
				require.Len(t, columns, 1)
				require.Equal(t, int32(test.wantType), columns[0].Typ.Id)
				require.Equal(t, test.wantWidth, columns[0].Typ.Width)
				require.Equal(t, test.wantScale, columns[0].Typ.Scale)
			})
		}
	}
}

func TestInitExecuteStmtParamKeepsWideIntegerTextInExactCommonDomain(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(
		t, 119, "select coalesce(?, cast(2.0000000000 as decimal(46,10)))")
	defer func() {
		cw.proc.SetPrepareParams(nil)
		prepareStmt.Close()
	}()

	prepareStmt.params = vector.NewVec(types.T_text.ToType())
	prepareStmt.ParamTypes = []byte{byte(defines.MYSQL_TYPE_VAR_STRING), 0}
	for _, value := range []string{
		"999999999999999999999999999999999999",
		"999999999999999999999999999999999999.0000000000",
	} {
		t.Run(value, func(t *testing.T) {
			prepareStmt.params.CleanOnlyData()
			require.NoError(t, vector.AppendBytes(
				prepareStmt.params, []byte(value), false, cw.proc.Mp()))
			_, queryPlan, _, _, _, err := initExecuteStmtParam(
				execCtx, ses, cw, nil, prepareStmt.Name)
			require.NoError(t, err)
			columns := plan2.GetResultColumnsFromPlan(queryPlan)
			require.Len(t, columns, 1)
			require.Equal(t, int32(types.T_decimal256), columns[0].Typ.Id)
			require.Equal(t, int32(46), columns[0].Typ.Width)
			require.Equal(t, int32(10), columns[0].Typ.Scale)
		})
	}
}

func TestInitExecuteStmtParamReusesBinaryIntegerSemanticCategory(t *testing.T) {
	for i, test := range []struct {
		name     string
		unsigned bool
	}{
		{name: "signed"},
		{name: "unsigned", unsigned: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(
				t, uint32(120+i), "select coalesce(?, cast(2 as decimal(10,2)))")
			defer func() {
				cw.proc.SetPrepareParams(nil)
				prepareStmt.Close()
			}()

			prepareStmt.params = vector.NewVec(types.T_text.ToType())
			require.NoError(t, vector.AppendBytes(
				prepareStmt.params, []byte("42"), false, cw.proc.Mp()))
			flags := byte(0)
			if test.unsigned {
				flags = 0x80
			}
			prepareStmt.ParamTypes = []byte{byte(defines.MYSQL_TYPE_LONGLONG), flags}
			execute := func() *plan.Plan {
				_, _, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
				require.NoError(t, err)
				return prepareStmt.PreparePlan
			}

			firstPlan := execute()
			require.Len(t, prepareStmt.paramBindingTypes, 1)
			require.Equal(t, preparedNumericProtocolExact, prepareStmt.paramBindingTypes[0].Size)
			require.Same(t, firstPlan, execute(),
				"an unchanged binary integer semantic category must reuse its prepared plan")
		})
	}
}

func TestInitExecuteStmtParamRebuildsDCLForTextUserVariable(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(
		t, 116, "set @out = coalesce(?, cast(2 as decimal(10,2)))")
	defer prepareStmt.Close()
	require.NoError(t, ses.SetUserDefinedVar("p", "1e100", ""))
	execPlan := &plan.Execute{
		Name: prepareStmt.Name,
		Args: []*plan.Expr{{Expr: &plan.Expr_V{V: &plan.VarRef{Name: "p"}}}},
	}

	_, rebuiltPlan, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, execPlan, "")
	require.NoError(t, err)
	require.Equal(t, []bool{true}, prepareStmt.paramBindingDependencies)
	require.Len(t, prepareStmt.paramBindingTypes, 1)
	require.Equal(t, preparedNumericTextFloat, prepareStmt.paramBindingTypes[0].Size)
	item := rebuiltPlan.GetDcl().GetSetVariables().GetItems()[0]
	require.Equal(t, int32(types.T_float64), item.GetValue().Typ.Id)
}

func TestInitExecuteStmtParamMapsRuntimeBindingCategoriesByPosition(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(t, 111,
		"select coalesce(?, cast(2 as decimal(10,2))), coalesce(?, cast(2 as decimal(10,2)))")
	defer func() {
		cw.proc.SetPrepareParams(nil)
		prepareStmt.Close()
	}()

	prepareStmt.params = vector.NewVec(types.T_text.ToType())
	setParams := func(first string, firstType defines.MysqlType, second string, secondType defines.MysqlType) {
		prepareStmt.params.CleanOnlyData()
		require.NoError(t, vector.AppendBytes(prepareStmt.params, []byte(first), false, cw.proc.Mp()))
		require.NoError(t, vector.AppendBytes(prepareStmt.params, []byte(second), false, cw.proc.Mp()))
		prepareStmt.ParamTypes = []byte{byte(firstType), 0, byte(secondType), 0}
	}
	executeTypes := func() []types.T {
		_, queryPlan, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
		require.NoError(t, err)
		columns := plan2.GetResultColumnsFromPlan(queryPlan)
		require.Len(t, columns, 2)
		return []types.T{types.T(columns[0].Typ.Id), types.T(columns[1].Typ.Id)}
	}

	setParams("1.25", defines.MYSQL_TYPE_VAR_STRING, "1e100", defines.MYSQL_TYPE_DOUBLE)
	resultTypes := executeTypes()
	require.True(t, resultTypes[0].IsDecimal())
	require.Equal(t, types.T_float64, resultTypes[1])
	require.Equal(t, preparedNumericTextPrefix, prepareStmt.paramBindingTypes[0].Size)
	require.Equal(t, types.T_float64.ToType(), prepareStmt.paramBindingTypes[1])

	setParams("1e-40", defines.MYSQL_TYPE_DOUBLE, "1.25", defines.MYSQL_TYPE_VAR_STRING)
	resultTypes = executeTypes()
	require.Equal(t, types.T_float64, resultTypes[0])
	require.True(t, resultTypes[1].IsDecimal())
	require.Equal(t, types.T_float64.ToType(), prepareStmt.paramBindingTypes[0])
	require.Equal(t, preparedNumericTextPrefix, prepareStmt.paramBindingTypes[1].Size)
}

func TestInitExecuteStmtParamMasksNonDependentTargetFunctionBindings(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(t, 114,
		"select coalesce(?, 'fallback'), coalesce(?, cast(2 as decimal(10,2)))")
	defer func() {
		cw.proc.SetPrepareParams(nil)
		prepareStmt.Close()
	}()

	prepareStmt.params = vector.NewVec(types.T_text.ToType())
	setParams := func(first string, firstType defines.MysqlType) {
		prepareStmt.params.CleanOnlyData()
		require.NoError(t, vector.AppendBytes(prepareStmt.params, []byte(first), false, cw.proc.Mp()))
		require.NoError(t, vector.AppendBytes(prepareStmt.params, []byte("1.25"), false, cw.proc.Mp()))
		prepareStmt.ParamTypes = []byte{byte(firstType), 0, byte(defines.MYSQL_TYPE_VAR_STRING), 0}
	}
	execute := func() (*plan.Plan, []types.T) {
		_, queryPlan, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
		require.NoError(t, err)
		columns := plan2.GetResultColumnsFromPlan(queryPlan)
		return prepareStmt.PreparePlan, []types.T{types.T(columns[0].Typ.Id), types.T(columns[1].Typ.Id)}
	}

	setParams("10", defines.MYSQL_TYPE_LONGLONG)
	firstPlan, resultTypes := execute()
	require.Equal(t, types.T_text, resultTypes[0])
	require.True(t, resultTypes[1].IsDecimal())
	require.Equal(t, []bool{false, true}, prepareStmt.paramBindingDependencies)

	setParams("abc", defines.MYSQL_TYPE_VAR_STRING)
	secondPlan, resultTypes := execute()
	require.Same(t, firstPlan, secondPlan)
	require.Equal(t, types.T_text, resultTypes[0])
	require.True(t, resultTypes[1].IsDecimal())
}

func TestInitExecuteStmtParamIgnoresUnrelatedRuntimeCategories(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(t, 113, "select ? + 1")
	defer func() {
		cw.proc.SetPrepareParams(nil)
		prepareStmt.Close()
	}()

	prepareStmt.params = vector.NewVec(types.T_text.ToType())
	setParam := func(value string, mysqlType defines.MysqlType) {
		prepareStmt.params.CleanOnlyData()
		require.NoError(t, vector.AppendBytes(prepareStmt.params, []byte(value), false, cw.proc.Mp()))
		prepareStmt.ParamTypes = []byte{byte(mysqlType), 0}
	}
	execute := func() *plan.Plan {
		_, _, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
		require.NoError(t, err)
		return prepareStmt.PreparePlan
	}

	initialPlan := prepareStmt.PreparePlan
	setParam("1", defines.MYSQL_TYPE_VAR_STRING)
	require.Same(t, initialPlan, execute(), "an unrelated text parameter must not trigger the first rebuild")
	setParam("1", defines.MYSQL_TYPE_DOUBLE)
	require.Same(t, initialPlan, execute(), "an unrelated category transition must reuse the plan")
	require.Empty(t, prepareStmt.paramBindingDependencies)
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
		plan2.ParamValue{Value: int64(10), IsBin: false, PrepareParamKind: vector.PrepareParamInteger},
		plan2.ParamValue{Value: int64(20), IsBin: false, PrepareParamKind: vector.PrepareParamInteger},
		plan2.ParamValue{Value: "session-binary", IsBin: true},
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

func TestProtocolUpgradeRebuildUsesPreparedDecimalBinding(t *testing.T) {
	rt := moruntime.ServiceRuntime("")
	defer rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion16)

	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(t, 115,
		"select coalesce(?, cast(2 as decimal(10,2)))")
	defer func() {
		cw.proc.SetPrepareParams(nil)
		prepareStmt.Close()
	}()
	require.Equal(t, []bool{true}, plan2.PreparedParamCommonTypeDependencies(
		prepareStmt.PreparePlan.GetDcl().GetPrepare().Plan, 1))

	prepareStmt.params = vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(prepareStmt.params, []byte("1e100"), false, cw.proc.Mp()))
	prepareStmt.ParamTypes = []byte{byte(defines.MYSQL_TYPE_DOUBLE), 0}
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion20)

	_, queryPlan, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	columns := plan2.GetResultColumnsFromPlan(queryPlan)
	require.Len(t, columns, 1)
	require.Equal(t, int32(types.T_float64), columns[0].Typ.Id)
	require.Equal(t, []bool{true}, prepareStmt.paramBindingDependencies)
}

func TestBinaryIntegerAndBooleanRebuildUseStableDecimalDomain(t *testing.T) {
	rt := moruntime.ServiceRuntime("")
	defer rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion20)

	for _, tc := range []struct {
		name      string
		mysqlType defines.MysqlType
		value     string
	}{
		{name: "signed", mysqlType: defines.MYSQL_TYPE_LONGLONG, value: "-42"},
		{name: "unsigned", mysqlType: defines.MYSQL_TYPE_LONGLONG, value: "18446744073709551615"},
		{name: "boolean", mysqlType: defines.MYSQL_TYPE_TINY, value: "1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(t, 200,
				"select coalesce(?, cast(2 as decimal(10,2)))")
			defer prepareStmt.Close()
			serviceRuntime := moruntime.ServiceRuntime(cw.proc.GetService())
			oldVersion, hadVersion := serviceRuntime.GetGlobalVariables(moruntime.MOProtocolVersion)
			defer func() {
				if hadVersion {
					serviceRuntime.SetGlobalVariables(moruntime.MOProtocolVersion, oldVersion)
				} else {
					serviceRuntime.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
				}
			}()
			serviceRuntime.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion20)
			prepareStmt.params = vector.NewVec(types.T_text.ToType())
			require.NoError(t, vector.AppendBytes(prepareStmt.params, []byte(tc.value), false, cw.proc.Mp()))
			prepareStmt.ParamTypes = []byte{byte(tc.mysqlType), 0}

			_, queryPlan, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
			require.NoError(t, err)
			columns := plan2.GetResultColumnsFromPlan(queryPlan)
			require.Len(t, columns, 1)
			require.Equal(t, int32(types.T_decimal256), columns[0].Typ.Id)
			require.Equal(t, int32(65), columns[0].Typ.Width)
			require.Equal(t, int32(30), columns[0].Typ.Scale)
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
