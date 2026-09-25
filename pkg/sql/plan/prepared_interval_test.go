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
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
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
		{name: "integer", unit: "second", value: ParamValue{Value: "3", SourceType: types.T_int64.ToType(), HasSourceType: true}, expectedUnit: types.MicroSecond},
		{name: "negative integer", unit: "second", value: ParamValue{Value: "-3", SourceType: types.T_int64.ToType(), HasSourceType: true}, expectedUnit: types.MicroSecond},
		{name: "NULL integer", unit: "second", value: ParamValue{SourceType: types.T_int64.ToType(), HasSourceType: true}, expectedUnit: types.MicroSecond},
		{name: "valid string", unit: "second", value: ParamValue{Value: "3", SourceType: types.T_varchar.ToType(), HasSourceType: true}, expectedUnit: types.MicroSecond, normalized: true},
		{name: "invalid string", unit: "second", value: ParamValue{Value: "not-an-interval", SourceType: types.T_varchar.ToType(), HasSourceType: true}, expectedUnit: types.MicroSecond, normalized: true},
		{name: "DAY_SECOND string", unit: "day_second", value: ParamValue{Value: "1 02:03:04", SourceType: types.T_varchar.ToType(), HasSourceType: true}, expectedUnit: types.MicroSecond, normalized: true},
		{name: "YEAR_MONTH string", unit: "year_month", value: ParamValue{Value: "1-2", SourceType: types.T_varchar.ToType(), HasSourceType: true}, expectedUnit: types.Month, normalized: true},
		{name: "binary integer", unit: "second", value: ParamValue{Value: "3", RuntimeType: types.T_int64.ToType(), HasRuntimeType: true, IsBinaryProtocol: true}, expectedUnit: types.MicroSecond},
		{name: "binary string", unit: "second", value: ParamValue{Value: "3", RuntimeType: types.T_text.ToType(), HasRuntimeType: true, IsBinaryProtocol: true}, expectedUnit: types.MicroSecond, normalized: true},
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
					require.NotNil(t, findPlanFunctionExpr(preparedPlan, "to_interval_microsecond"), preparedPlan.String())

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
						require.NotNil(t, findPlanFunctionExpr(filled, "to_interval_microsecond"), filled.String())
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

// TIME's interval eligibility belongs to the original SQL unit. Dynamic
// strings, decimals and prepared markers must not hide a DAY-containing unit
// by first normalizing its numeric value to MICROSECOND.
func TestTimeIntervalOriginalUnitBindingMatrix(t *testing.T) {
	ctx := context.Background()
	timeExpr := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_time)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}
	dateExpr := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_date)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}
	inputs := []struct {
		name string
		expr *planpb.Expr
	}{
		{"string one", makePlan2StringConstExprWithType("1")},
		{"string zero", makePlan2StringConstExprWithType("0")},
		{"string fractional", makePlan2StringConstExprWithType("1.5")},
		{"integer one", makePlan2Int64ConstExprWithType(1)},
		{"integer zero", makePlan2Int64ConstExprWithType(0)},
		{"floating one", makePlan2Float64ConstExprWithType(1.5)},
		{"NULL", makePlan2NullConstExprWithType()},
		{"varchar column", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_varchar)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 1}}}},
		{"integer column", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 1}}}},
		{"marker", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_text)}, Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}},
	}
	bind := func(date *planpb.Expr, value *planpb.Expr, unit string) ([]*planpb.Expr, error) {
		interval := &planpb.Expr{Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{
			value, makePlan2StringConstExprWithType(unit),
		}}}}
		return resetDateFunctionArgs(ctx, date, interval)
	}
	for _, unit := range []string{
		"day", "week", "month", "quarter", "year", "year_month",
		"day_hour", "day_minute", "day_second", "day_microsecond",
	} {
		for _, input := range inputs {
			t.Run(unit+"/"+input.name, func(t *testing.T) {
				_, err := bind(timeExpr, input.expr, unit)
				require.ErrorContains(t, err, "time interval unit")
			})
		}
	}
	for _, unit := range []string{
		"microsecond", "second", "minute", "hour",
		"second_microsecond", "minute_microsecond", "minute_second",
		"hour_microsecond", "hour_second", "hour_minute",
	} {
		for _, input := range inputs {
			t.Run("supported/"+unit+"/"+input.name, func(t *testing.T) {
				args, err := bind(timeExpr, input.expr, unit)
				require.NoError(t, err)
				require.Len(t, args, 3)
				require.Equal(t, int32(types.T_time), args[0].Typ.Id)
				// A numeric column cannot reach the TIME executor with an
				// unsupported compound unit after losing its SQL spelling.
				if input.name == "integer column" {
					switch unit {
					case "second_microsecond", "minute_microsecond", "minute_second",
						"hour_microsecond", "hour_second":
						require.Equal(t, int64(types.MicroSecond), args[2].GetLit().GetI64Val())
						require.Greater(t, args[1].GetF().Args[0].Typ.Width, int32(0))
					case "hour_minute":
						require.Equal(t, int64(types.Minute), args[2].GetLit().GetI64Val())
						require.Greater(t, args[1].GetF().Args[0].Typ.Width, int32(0))
					}
				}
			})
		}
	}
	for _, unit := range []string{"day", "day_second", "month", "year_month"} {
		_, err := bind(dateExpr, makePlan2StringConstExprWithType("1"), unit)
		require.NoError(t, err, unit)
	}
}

func TestTimeCalendarIntervalRejectedAcrossSyntaxes(t *testing.T) {
	for _, tc := range []struct {
		name    string
		sql     string
		wantErr string
	}{
		{"add literal", "date_add(cast('12:00:00' as time), interval '1' day)", "time interval unit"},
		{"sub literal", "date_sub(cast('12:00:00' as time), interval 1 day)", "time interval unit"},
		{"add marker", "date_add(cast('12:00:00' as time), interval ? day)", "time interval unit"},
		{"sub marker", "date_sub(cast('12:00:00' as time), interval ? day_second)", "time interval unit"},
		{"adddate alias", "adddate(cast('12:00:00' as time), interval ? day)", "time interval unit"},
		{"subdate alias", "subdate(cast('12:00:00' as time), interval ? day)", "time interval unit"},
		{"plus operator", "cast('12:00:00' as time) + interval ? day", "bad value [TIME INTERVAL]"},
		{"minus operator", "cast('12:00:00' as time) - interval ? day", "bad value [TIME INTERVAL]"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := runOneStmt(NewMockOptimizer(false), t,
				"prepare stmt_time_unit from select "+tc.sql)
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
	for _, name := range []string{"date_add", "date_sub"} {
		prepared, err := runOneStmt(NewMockOptimizer(false), t,
			"prepare stmt_time_hour from select "+name+"(cast('12:00:00' as time), interval ? hour)")
		require.NoError(t, err)
		fn := findPlanFunctionExpr(prepared.GetDcl().GetPrepare().GetPlan(), name)
		require.NotNil(t, fn)
		require.Equal(t, int32(types.T_time), fn.Typ.Id)
		prepared, err = runOneStmt(NewMockOptimizer(false), t,
			"prepare stmt_time_compound from select "+name+"(cast('12:00:00' as time), interval ? hour_second)")
		require.NoError(t, err)
		fn = findPlanFunctionExpr(prepared.GetDcl().GetPrepare().GetPlan(), name)
		require.NotNil(t, fn)
		require.Equal(t, int32(types.T_time), fn.Typ.Id)
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
