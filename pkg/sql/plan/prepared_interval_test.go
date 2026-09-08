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

package plan

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestPreparedIntervalMarkerRebindsInternalDateFunction(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name         string
		unit         string
		value        ParamValue
		expectedUnit types.IntervalType
		normalized   bool
	}{
		{name: "integer", unit: "second", value: ParamValue{Value: "3", SourceType: types.T_int64.ToType(), HasSourceType: true}, expectedUnit: types.Second},
		{name: "negative integer", unit: "second", value: ParamValue{Value: "-3", SourceType: types.T_int64.ToType(), HasSourceType: true}, expectedUnit: types.Second},
		{name: "NULL integer", unit: "second", value: ParamValue{SourceType: types.T_int64.ToType(), HasSourceType: true}, expectedUnit: types.Second},
		{name: "valid string", unit: "second", value: ParamValue{Value: "3", SourceType: types.T_varchar.ToType(), HasSourceType: true}, expectedUnit: types.Second, normalized: true},
		{name: "invalid string", unit: "second", value: ParamValue{Value: "not-an-interval", SourceType: types.T_varchar.ToType(), HasSourceType: true}, expectedUnit: types.Second, normalized: true},
		{name: "DAY_SECOND string", unit: "day_second", value: ParamValue{Value: "1 02:03:04", SourceType: types.T_varchar.ToType(), HasSourceType: true}, expectedUnit: types.Second, normalized: true},
		{name: "YEAR_MONTH string", unit: "year_month", value: ParamValue{Value: "1-2", SourceType: types.T_varchar.ToType(), HasSourceType: true}, expectedUnit: types.Month, normalized: true},
		{name: "binary integer", unit: "second", value: ParamValue{Value: "3", RuntimeType: types.T_int64.ToType(), HasRuntimeType: true, IsBinaryProtocol: true}, expectedUnit: types.Second},
		{name: "binary string", unit: "second", value: ParamValue{Value: "3", RuntimeType: types.T_text.ToType(), HasRuntimeType: true, IsBinaryProtocol: true}, expectedUnit: types.Second, normalized: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, name := range []string{"date_add", "date_sub"} {
				t.Run(name, func(t *testing.T) {
					prepared, err := runOneStmt(NewMockOptimizer(false), t,
						"prepare stmt_interval from select "+name+"('2026-01-01', interval ? "+tc.unit+")")
					require.NoError(t, err)

					preparedPlan := prepared.GetDcl().GetPrepare().GetPlan()
					dateFunction := findPlanFunctionExpr(preparedPlan, name)
					require.NotNil(t, dateFunction, preparedPlan.String())
					require.Len(t, dateFunction.GetF().GetArgs(), 3, dateFunction.String())
					require.NotNil(t, findPlanFunctionExpr(preparedPlan, "to_interval"), preparedPlan.String())

					filled, _, err := FillValuesOfParamsInPlanWithSpecialization(
						ctx,
						preparedPlan,
						[]any{tc.value},
					)
					require.NoError(t, err)
					dateFunction = findPlanFunctionExpr(filled, name)
					require.NotNil(t, dateFunction, filled.String())
					require.Len(t, dateFunction.GetF().GetArgs(), 3, dateFunction.String())
					require.Equal(t, int32(types.T_int64), dateFunction.GetF().GetArgs()[1].Typ.Id, dateFunction.String())
					require.Equal(t, int64(tc.expectedUnit), dateFunction.GetF().GetArgs()[2].GetLit().GetI64Val(), dateFunction.String())
					if tc.normalized {
						require.NotNil(t, findPlanFunctionExpr(filled, "to_interval"), filled.String())
					}
				})
			}
		})
	}
}

func TestPreparedIntervalMarkerRepeatedExecutionsDoNotMutatePlan(t *testing.T) {
	ctx := context.Background()
	prepared, err := runOneStmt(NewMockOptimizer(false), t,
		"prepare stmt_interval_reuse from select date_add('2026-01-01', interval ? second)")
	require.NoError(t, err)
	preparedPlan := prepared.GetDcl().GetPrepare().GetPlan()
	require.True(t, preparedExprContainsParam(findPlanFunctionExpr(preparedPlan, "date_add")))

	for _, value := range []ParamValue{
		{Value: "3", SourceType: types.T_int64.ToType(), HasSourceType: true},
		{Value: "-3", SourceType: types.T_int64.ToType(), HasSourceType: true},
		{SourceType: types.T_int64.ToType(), HasSourceType: true},
		{Value: "4", RuntimeType: types.T_int64.ToType(), HasRuntimeType: true, IsBinaryProtocol: true},
		{Value: "5", RuntimeType: types.T_text.ToType(), HasRuntimeType: true, IsBinaryProtocol: true},
	} {
		filled, _, fillErr := FillValuesOfParamsInPlanWithSpecialization(
			ctx, preparedPlan, []any{value})
		require.NoError(t, fillErr)
		require.False(t, preparedExprContainsParam(findPlanFunctionExpr(filled, "date_add")), filled.String())
		require.True(t, preparedExprContainsParam(findPlanFunctionExpr(preparedPlan, "date_add")), preparedPlan.String())
	}
}

func TestPreparedIntervalMarkerRelatedSyntaxes(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name         string
		sql          string
		functionName string
	}{
		{name: "DATE_ADD", sql: "date_add('2026-01-01', interval ? second)", functionName: "date_add"},
		{name: "DATE_SUB", sql: "date_sub('2026-01-01', interval ? second)", functionName: "date_sub"},
		{name: "ADDDATE", sql: "adddate('2026-01-01', interval ? second)", functionName: "date_add"},
		{name: "SUBDATE", sql: "subdate('2026-01-01', interval ? second)", functionName: "date_sub"},
		{name: "plus operator", sql: "'2026-01-01' + interval ? second", functionName: "date_add"},
		{name: "minus operator", sql: "'2026-01-01' - interval ? second", functionName: "date_sub"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prepared, err := runOneStmt(NewMockOptimizer(false), t,
				"prepare stmt_interval_syntax from select "+tc.sql)
			require.NoError(t, err)

			filled, _, err := FillValuesOfParamsInPlanWithSpecialization(
				ctx,
				prepared.GetDcl().GetPrepare().GetPlan(),
				[]any{ParamValue{Value: "3", SourceType: types.T_int64.ToType(), HasSourceType: true}},
			)
			require.NoError(t, err)
			require.Len(t, findPlanFunctionExpr(filled, tc.functionName).GetF().GetArgs(), 3, filled.String())
		})
	}
}

func TestDateFunctionsKeepInternalArityPrivate(t *testing.T) {
	ctx := context.Background()
	for _, name := range []string{"date_add", "date_sub"} {
		_, err := BindFuncExprImplByPlanExpr(ctx, name, []*Expr{
			makeVarcharConst("2026-01-01"),
			makePlan2Int64ConstExprWithType(3),
			makePlan2Int64ConstExprWithType(int64(types.Second)),
		})
		require.ErrorContains(t, err, "date_add/date_sub function need two args")
	}
}
