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

package plan

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestPreparedSignRebindsRuntimeNumericDomain(t *testing.T) {
	ctx := context.Background()
	prepared, err := runOneStmt(NewMockOptimizer(false), t,
		"prepare stmt_sign from 'select sign(?)'")
	require.NoError(t, err)
	preparePlan := prepared.GetDcl().GetPrepare().Plan

	fn := findPlanFunctionExpr(preparePlan, "sign")
	require.NotNil(t, fn)
	require.Equal(t, int32(types.T_float64), fn.GetF().Args[0].Typ.Id)
	require.Equal(t, []int32{0}, PreparedPlanNumericFallbackParamPositions(preparePlan))
	require.True(t, PreparedPlanHasDeferredNumericFunction(preparePlan))

	ordinary, err := runOneStmt(NewMockOptimizer(false), t,
		"prepare stmt_sign_column from 'select sign(n_regionkey) from nation'")
	require.NoError(t, err)
	ordinaryFn := findPlanFunctionExpr(ordinary.GetDcl().GetPrepare().Plan, "sign")
	require.NotNil(t, ordinaryFn)
	require.NotEqual(t, int32(types.T_float64), ordinaryFn.GetF().Args[0].Typ.Id)

	for _, test := range []struct {
		name string
		sql  string
	}{
		{name: "explicit decimal", sql: "prepare stmt_sign_decimal from 'select sign(cast(? as decimal(20,5)))'"},
		{name: "explicit double", sql: "prepare stmt_sign_double from 'select sign(cast(? as double))'"},
		{name: "explicit integer", sql: "prepare stmt_sign_integer from 'select sign(cast(? as signed))'"},
	} {
		t.Run(test.name, func(t *testing.T) {
			prepared, err := runOneStmt(NewMockOptimizer(false), t, test.sql)
			require.NoError(t, err)
			plan := prepared.GetDcl().GetPrepare().Plan
			require.Empty(t, PreparedPlanNumericFallbackParamPositions(plan),
				"an explicit numeric cast owns SIGN's input domain")
		})
	}

	for _, test := range []struct {
		name    string
		value   string
		kind    vector.PrepareParamKind
		isBin   bool
		wantArg types.T
		wantOL  int32
	}{
		{name: "integer", value: "-3", kind: vector.PrepareParamInteger, wantArg: types.T_int64, wantOL: 0},
		{name: "float", value: "-0.1", kind: vector.PrepareParamFloat, wantArg: types.T_float64, wantOL: 2},
		{name: "decimal", value: "-0.1", kind: vector.PrepareParamDecimal, wantArg: types.T_decimal64, wantOL: 3},
		{name: "binary string decimal", value: "-0.1", kind: vector.PrepareParamNone, isBin: true, wantArg: types.T_decimal64, wantOL: 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtimePlan, specialized, err := FillValuesOfParamsInPlanWithPreparedNumericOverload(
				ctx, preparePlan, []any{ParamValue{
					Value:            test.value,
					IsBinaryProtocol: test.isBin,
					PrepareParamKind: test.kind,
				}})
			require.NoError(t, err)
			require.True(t, specialized)
			fn := findPlanFunctionExpr(runtimePlan, "sign")
			require.NotNil(t, fn)
			require.Equal(t, test.wantOL, signOverloadForTest(fn))
			require.Equal(t, int32(test.wantArg), fn.GetF().Args[0].Typ.Id)
		})
	}
}

func signOverloadForTest(expr *Expr) int32 {
	_, overload := function.DecodeOverloadID(expr.GetF().GetFunc().GetObj())
	return overload
}

func TestPreparedEltRebindsRuntimeNumericDomain(t *testing.T) {
	ctx := context.Background()
	prepared, err := runOneStmt(NewMockOptimizer(false), t,
		"prepare stmt_elt from 'select elt(?, ''a'', ''b'', ''c'')'")
	require.NoError(t, err)
	preparePlan := prepared.GetDcl().GetPrepare().Plan

	fn := findPlanFunctionExpr(preparePlan, "elt")
	require.NotNil(t, fn)
	require.Equal(t, int32(types.T_int64), fn.GetF().Args[0].Typ.Id)
	require.Equal(t, []int32{0}, PreparedPlanNumericFallbackParamPositions(preparePlan))
	require.True(t, PreparedPlanHasDeferredNumericFunction(preparePlan))
	prepareSnapshot := preparePlan.String()

	for _, test := range []struct {
		name        string
		value       any
		kind        vector.PrepareParamKind
		want        string
		wantType    types.T
		wantNull    bool
		specialized bool
	}{
		{name: "decimal rounds one point four", value: "1.4", kind: vector.PrepareParamDecimal, want: "a", wantType: types.T_int64, specialized: true},
		{name: "decimal rounds one point five", value: "1.5", kind: vector.PrepareParamDecimal, want: "b", wantType: types.T_int64, specialized: true},
		{name: "decimal rounds two point five", value: "2.5", kind: vector.PrepareParamDecimal, want: "c", wantType: types.T_int64, specialized: true},
		{name: "decimal rounds two point six", value: "2.6", kind: vector.PrepareParamDecimal, want: "c", wantType: types.T_int64, specialized: true},
		{name: "numeric text follows decimal conversion", value: "1.6", kind: vector.PrepareParamNone, want: "b", wantType: types.T_int64, specialized: true},
		{name: "numeric prefix remains accepted", value: "2tail", kind: vector.PrepareParamNone, want: "b", wantType: types.T_int64, specialized: true},
		{name: "integer remains exact", value: "1", kind: vector.PrepareParamInteger, want: "a", wantType: types.T_int64, specialized: true},
		{name: "out of range index", value: "4", kind: vector.PrepareParamInteger, wantNull: true, wantType: types.T_int64, specialized: true},
		{name: "non-numeric text maps to zero", value: "foo", kind: vector.PrepareParamNone, wantNull: true, wantType: types.T_int64, specialized: true},
		{name: "null index", value: nil, kind: vector.PrepareParamNone, wantNull: true, specialized: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtimePlan, specialized, err := FillValuesOfParamsInPlanWithPreparedNumericOverload(
				ctx, preparePlan, []any{ParamValue{
					Value: test.value, PrepareParamKind: test.kind,
				}})
			require.NoError(t, err)
			require.Equal(t, test.specialized, specialized)
			bound := findPlanFunctionExpr(runtimePlan, "elt")
			require.NotNil(t, bound, runtimePlan.String())
			if test.wantType != types.T_any {
				require.Equal(t, int32(test.wantType), bound.GetF().Args[0].Typ.Id,
					runtimePlan.String())
			}

			proc := testutil.NewProc(t)
			defer proc.Free()
			executor, err := colexec.NewExpressionExecutor(proc, bound)
			require.NoError(t, err)
			defer executor.Free()
			result, err := executor.Eval(proc, nil, nil)
			require.NoError(t, err)
			if test.wantNull {
				require.True(t, result.IsNull(0))
			} else {
				require.Equal(t, test.want, result.GetStringAt(0))
			}
			require.Equal(t, prepareSnapshot, preparePlan.String(),
				"execute-time rebinding must not mutate the cached prepared plan")
		})
	}

	preparedDouble, err := runOneStmt(NewMockOptimizer(false), t,
		"prepare stmt_elt_double from 'select elt(cast(? as double), ''a'', ''b'')'")
	require.NoError(t, err)
	doublePlan := preparedDouble.GetDcl().GetPrepare().Plan
	require.Equal(t, []int32{0}, PreparedPlanNumericFallbackParamPositions(doublePlan))
	filled, specialized, err := FillValuesOfParamsInPlanWithPreparedNumericOverload(
		ctx, doublePlan, []any{ParamValue{
			Value: "1.5", PrepareParamKind: vector.PrepareParamFloat,
		}})
	require.NoError(t, err)
	require.True(t, specialized)
	doubleFn := findPlanFunctionExpr(filled, "elt")
	require.NotNil(t, doubleFn)
	require.Equal(t, int32(types.T_float64), doubleFn.GetF().Args[0].GetF().Args[0].Typ.Id)
	proc := testutil.NewProcess(t)
	defer proc.Free()
	executor, err := colexec.NewExpressionExecutor(proc, doubleFn)
	require.NoError(t, err)
	defer executor.Free()
	result, err := executor.Eval(proc, nil, nil)
	require.NoError(t, err)
	require.Equal(t, "a", result.GetStringAt(0), "explicit DOUBLE uses truncation, not implicit rounding")
}
