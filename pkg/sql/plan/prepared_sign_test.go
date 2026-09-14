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
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
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
