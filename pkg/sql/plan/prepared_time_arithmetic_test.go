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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func formatDecimalResult(v *vector.Vector) string {
	switch v.GetType().Oid {
	case types.T_decimal64:
		return vector.GetFixedAtWithTypeCheck[types.Decimal64](v, 0).Format(v.GetType().Scale)
	case types.T_decimal128:
		return vector.GetFixedAtWithTypeCheck[types.Decimal128](v, 0).Format(v.GetType().Scale)
	case types.T_decimal256:
		return vector.GetFixedAtWithTypeCheck[types.Decimal256](v, 0).Format(v.GetType().Scale)
	default:
		panic(fmt.Sprintf("expected decimal result, got %v", v.GetType()))
	}
}

func TestPreparedTimeArithmeticUsesNumericParameterContext(t *testing.T) {
	for _, tc := range []struct {
		name  string
		op    string
		left  string
		right string
	}{
		{name: "add time left", op: "+", left: "n_nationkey", right: "?"},
		{name: "add time right", op: "+", left: "?", right: "n_nationkey"},
		{name: "subtract time left", op: "-", left: "n_nationkey", right: "?"},
		{name: "subtract time right", op: "-", left: "?", right: "n_nationkey"},
		{name: "multiply time left", op: "*", left: "n_nationkey", right: "?"},
		{name: "multiply time right", op: "*", left: "?", right: "n_nationkey"},
		{name: "divide time left", op: "/", left: "n_nationkey", right: "?"},
		{name: "divide time right", op: "/", left: "?", right: "n_nationkey"},
		{name: "mod time left", op: "%", left: "n_nationkey", right: "?"},
		{name: "mod time right", op: "%", left: "?", right: "n_nationkey"},
		{name: "integer divide time left", op: "div", left: "n_nationkey", right: "?"},
		{name: "integer divide time right", op: "div", left: "?", right: "n_nationkey"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false)
			mock.ctxt.tables["nation"].Cols[0].Typ = planpb.Type{
				Id: int32(types.T_time), Width: 6, Scale: 3,
			}
			query := buildPreparedQuery(t, mock, fmt.Sprintf(
				"prepare stmt1 from 'select %s %s %s from nation'",
				tc.left, tc.op, tc.right))

			paramTypes := collectUniquePlanParamTypes(t, &Plan{
				Plan: &planpb.Plan_Query{Query: query},
			})
			require.Len(t, paramTypes, 1)
			for _, typ := range paramTypes {
				require.Equal(t, int32(types.T_decimal64), typ.Id)
				require.Equal(t, int32(18), typ.Width)
				require.Equal(t, int32(3), typ.Scale)
			}
		})
	}
}

func TestPreparedTimeArithmeticKeepsBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name  string
		time  types.T
		scale int32
		want  bool
	}{
		{name: "time zero scale", time: types.T_time, scale: 0, want: true},
		{name: "time microsecond scale", time: types.T_time, scale: 6, want: true},
		{name: "date remains unsupported", time: types.T_date, scale: 0, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false)
			mock.ctxt.tables["nation"].Cols[0].Typ = planpb.Type{
				Id: int32(tc.time), Width: 6, Scale: tc.scale,
			}
			_, err := runOneStmt(mock, t,
				"prepare stmt1 from 'select n_nationkey * ? from nation'")
			if tc.want {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
		})
	}
}

func TestPreparedTimeArithmeticCoversModAndNestedParameters(t *testing.T) {
	mock := NewMockOptimizer(false)
	mock.ctxt.tables["nation"].Cols[0].Typ = planpb.Type{
		Id: int32(types.T_time), Width: 6, Scale: 3,
	}

	modQuery := buildPreparedQuery(t, mock,
		"prepare stmt_mod from 'select mod(n_nationkey, ?) from nation'")
	require.Len(t, collectUniquePlanParamTypes(t, &Plan{
		Plan: &planpb.Plan_Query{Query: modQuery},
	}), 1)

	nestedQuery := buildPreparedQuery(t, mock,
		"prepare stmt_nested from 'select n_nationkey * (? + ?) from nation'")
	paramTypes := collectUniquePlanParamTypes(t, &Plan{
		Plan: &planpb.Plan_Query{Query: nestedQuery},
	})
	require.Len(t, paramTypes, 2)
	for _, typ := range paramTypes {
		require.Equal(t, int32(types.T_decimal64), typ.Id)
		require.Equal(t, int32(18), typ.Width)
		require.Equal(t, int32(3), typ.Scale)
	}
}

func TestPreparedTimeArithmeticRespectsExplicitStringCast(t *testing.T) {
	mock := NewMockOptimizer(false)
	mock.ctxt.tables["nation"].Cols[0].Typ = planpb.Type{
		Id: int32(types.T_time), Width: 6, Scale: 3,
	}
	_, err := runOneStmt(mock, t,
		"prepare stmt_char from 'select n_nationkey * cast(? as char) from nation'")
	require.Error(t, err)
}

func TestPreparedTimeArithmeticFillsAndExecutes(t *testing.T) {
	prepared, err := runOneStmt(NewMockOptimizer(false), t,
		"prepare stmt_runtime from 'select cast(''03:04:05.123456'' as time(6)) * ?'")
	require.NoError(t, err)
	preparePlan := prepared.GetDcl().GetPrepare().Plan

	for _, tc := range []struct {
		value string
		kind  vector.PrepareParamKind
		want  string
	}{
		{value: "10", kind: vector.PrepareParamInteger, want: "304051.234560"},
		{value: "1.25", kind: vector.PrepareParamDecimal, want: "38006.40432000"},
	} {
		t.Run(tc.value, func(t *testing.T) {
			filled, _, err := FillValuesOfParamsInPlanWithSpecialization(context.Background(), preparePlan, []any{
				ParamValue{Value: tc.value, PrepareParamKind: tc.kind},
			})
			require.NoError(t, err)

			proc := testutil.NewProc(t)
			defer proc.Free()
			expr := filled.GetQuery().Nodes[len(filled.GetQuery().Nodes)-1].ProjectList[0]
			executor, err := colexec.NewExpressionExecutor(proc, expr)
			require.NoError(t, err)
			defer executor.Free()
			result, err := executor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
			require.NoError(t, err)
			require.False(t, result.IsNull(0))
			require.Equal(t, types.T_decimal128, result.GetType().Oid)
			got := vector.GetFixedAtWithTypeCheck[types.Decimal128](result, 0)
			require.Equal(t, tc.want, got.Format(result.GetType().Scale))
		})
	}
}

func TestPreparedTimeArithmeticPreservesTemporalIntegerDomain(t *testing.T) {
	for _, op := range []string{"+", "-", "%"} {
		t.Run(op, func(t *testing.T) {
			ordinary, err := runOneStmt(NewMockOptimizer(false), t,
				"select cast('00:00:01' as time(0)) "+op+" 10")
			require.NoError(t, err)
			ordinaryExpr := ordinary.GetQuery().Nodes[len(ordinary.GetQuery().Nodes)-1].ProjectList[0]

			prepared, err := runOneStmt(NewMockOptimizer(false), t,
				"prepare stmt_runtime from 'select cast(''00:00:01'' as time(0)) "+op+" ?'")
			require.NoError(t, err)
			filled, _, err := FillValuesOfParamsInPlanWithSpecialization(context.Background(),
				prepared.GetDcl().GetPrepare().Plan, []any{
					ParamValue{Value: "10", PrepareParamKind: vector.PrepareParamInteger},
				})
			require.NoError(t, err)
			preparedExpr := filled.GetQuery().Nodes[len(filled.GetQuery().Nodes)-1].ProjectList[0]

			for _, expr := range []*planpb.Expr{ordinaryExpr, preparedExpr} {
				require.Equal(t, int32(types.T_decimal64), expr.Typ.Id)
				require.Equal(t, int32(18), expr.Typ.Width)
				require.Equal(t, int32(0), expr.Typ.Scale)
			}

			if op != "+" {
				return
			}
			ordinaryMax, err := runOneStmt(NewMockOptimizer(false), t,
				"select cast('00:00:01' as time(0)) + 9223372036854775807")
			require.NoError(t, err)
			ordinaryMaxExpr := ordinaryMax.GetQuery().Nodes[len(ordinaryMax.GetQuery().Nodes)-1].ProjectList[0]
			proc := testutil.NewProc(t)
			executor, err := colexec.NewExpressionExecutor(proc, ordinaryMaxExpr)
			require.NoError(t, err)
			_, ordinaryErr := executor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
			executor.Free()
			proc.Free()
			require.Error(t, ordinaryErr)

			preparedMax, _, err := FillValuesOfParamsInPlanWithSpecialization(context.Background(),
				prepared.GetDcl().GetPrepare().Plan, []any{
					ParamValue{Value: "9223372036854775807", PrepareParamKind: vector.PrepareParamInteger},
				})
			require.NoError(t, err)
			preparedMaxExpr := preparedMax.GetQuery().Nodes[len(preparedMax.GetQuery().Nodes)-1].ProjectList[0]
			proc = testutil.NewProc(t)
			executor, err = colexec.NewExpressionExecutor(proc, preparedMaxExpr)
			require.NoError(t, err)
			_, preparedErr := executor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
			executor.Free()
			proc.Free()
			require.Error(t, preparedErr)
		})
	}

	mock := NewMockOptimizer(false)
	mock.ctxt.tables["nation"].Cols[0].Typ = planpb.Type{
		Id: int32(types.T_time), Width: 6, Scale: 0,
	}
	for _, op := range []string{"+", "-", "%"} {
		t.Run("column/"+op, func(t *testing.T) {
			ordinary, err := runOneStmt(mock, t, "select n_nationkey "+op+" 10 from nation")
			require.NoError(t, err)
			ordinaryExpr := ordinary.GetQuery().Nodes[len(ordinary.GetQuery().Nodes)-1].ProjectList[0]
			prepared, err := runOneStmt(mock, t,
				"prepare stmt_runtime from 'select n_nationkey "+op+" ? from nation'")
			require.NoError(t, err)
			filled, _, err := FillValuesOfParamsInPlanWithSpecialization(context.Background(),
				prepared.GetDcl().GetPrepare().Plan, []any{
					ParamValue{Value: "10", PrepareParamKind: vector.PrepareParamInteger},
				})
			require.NoError(t, err)
			preparedExpr := filled.GetQuery().Nodes[len(filled.GetQuery().Nodes)-1].ProjectList[0]
			require.Equal(t, ordinaryExpr.Typ.Id, preparedExpr.Typ.Id)
			require.Equal(t, ordinaryExpr.Typ.Width, preparedExpr.Typ.Width)
			require.Equal(t, ordinaryExpr.Typ.Scale, preparedExpr.Typ.Scale)
		})
	}
}

func TestPreparedTimeArithmeticCoercesNestedIntegerAtBoundary(t *testing.T) {
	const ordinarySQL = "select cast('00:00:01.000000' as time(6)) + (10000000000000 - 9999999999999)"
	ordinary, err := runOneStmt(NewMockOptimizer(false), t, ordinarySQL)
	require.NoError(t, err)

	prepared, err := runOneStmt(NewMockOptimizer(false), t,
		"prepare stmt_nested_scale from 'select cast(''00:00:01.000000'' as time(6)) + (? - ?)'")
	require.NoError(t, err)
	filled, _, err := FillValuesOfParamsInPlanWithSpecialization(context.Background(),
		prepared.GetDcl().GetPrepare().Plan, []any{
			ParamValue{Value: "10000000000000", PrepareParamKind: vector.PrepareParamInteger},
			ParamValue{Value: "9999999999999", PrepareParamKind: vector.PrepareParamInteger},
		})
	require.NoError(t, err)

	evaluate := func(expr *planpb.Expr) (types.Type, string) {
		proc := testutil.NewProc(t)
		defer proc.Free()
		executor, err := colexec.NewExpressionExecutor(proc, expr)
		require.NoError(t, err)
		defer executor.Free()
		result, err := executor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
		require.NoError(t, err)
		require.False(t, result.IsNull(0))
		return *result.GetType(), formatDecimalResult(result)
	}

	ordinaryExpr := ordinary.GetQuery().Nodes[len(ordinary.GetQuery().Nodes)-1].ProjectList[0]
	preparedExpr := filled.GetQuery().Nodes[len(filled.GetQuery().Nodes)-1].ProjectList[0]
	ordinaryType, ordinaryValue := evaluate(ordinaryExpr)
	preparedType, preparedValue := evaluate(preparedExpr)
	require.Equal(t, types.T_decimal64, ordinaryType.Oid)
	require.Equal(t, int32(18), ordinaryType.Width)
	require.Equal(t, int32(6), ordinaryType.Scale)
	require.Equal(t, ordinaryType, preparedType)
	require.Equal(t, "2.000000", ordinaryValue)
	require.Equal(t, ordinaryValue, preparedValue)
}

func TestPreparedTimeArithmeticPreservesUnsignedDomain(t *testing.T) {
	ordinary, err := runOneStmt(NewMockOptimizer(false), t,
		"select cast('00:00:01' as time(0)) + cast(10 as unsigned)")
	require.NoError(t, err)
	prepared, err := runOneStmt(NewMockOptimizer(false), t,
		"prepare stmt_unsigned from 'select cast(''00:00:01'' as time(0)) + ?'")
	require.NoError(t, err)
	filled, _, err := FillValuesOfParamsInPlanWithSpecialization(context.Background(),
		prepared.GetDcl().GetPrepare().Plan, []any{
			ParamValue{
				Value: "10", PrepareParamKind: vector.PrepareParamInteger,
				RuntimeType: types.T_uint64.ToType(), HasRuntimeType: true,
				IsBinaryProtocol: true,
			},
		})
	require.NoError(t, err)

	ordinaryExpr := ordinary.GetQuery().Nodes[len(ordinary.GetQuery().Nodes)-1].ProjectList[0]
	preparedExpr := filled.GetQuery().Nodes[len(filled.GetQuery().Nodes)-1].ProjectList[0]
	require.Equal(t, int32(types.T_decimal64), ordinaryExpr.Typ.Id)
	require.Equal(t, int32(18), ordinaryExpr.Typ.Width)
	require.Equal(t, int32(0), ordinaryExpr.Typ.Scale)
	require.Equal(t, ordinaryExpr.Typ.Id, preparedExpr.Typ.Id)
	require.Equal(t, ordinaryExpr.Typ.Width, preparedExpr.Typ.Width)
	require.Equal(t, ordinaryExpr.Typ.Scale, preparedExpr.Typ.Scale)
	require.False(t, preparedExpr.Typ.NotNullable)

	proc := testutil.NewProc(t)
	defer proc.Free()
	executor, err := colexec.NewExpressionExecutor(proc, preparedExpr)
	require.NoError(t, err)
	defer executor.Free()
	result, err := executor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
	require.NoError(t, err)
	require.False(t, result.IsNull(0))
	require.Equal(t, "11", formatDecimalResult(result))
}

func TestPreparedTimeArithmeticPreservesNullEnvelope(t *testing.T) {
	prepared, err := runOneStmt(NewMockOptimizer(false), t,
		"prepare stmt_null_envelope from 'select cast(''00:00:01'' as time(0)) + (? + ?)'")
	require.NoError(t, err)
	preparePlan := prepared.GetDcl().GetPrepare().Plan
	cachedPlan, err := preparePlan.Marshal()
	require.NoError(t, err)

	for _, tc := range []struct {
		name   string
		values []any
		width  int32
		id     types.T
	}{
		{name: "left null", values: []any{
			ParamValue{Value: nil, PrepareParamKind: vector.PrepareParamNone},
			ParamValue{Value: "2", PrepareParamKind: vector.PrepareParamInteger},
		}, width: 65, id: types.T_decimal256},
		{name: "right null", values: []any{
			ParamValue{Value: "1", PrepareParamKind: vector.PrepareParamInteger},
			ParamValue{Value: nil, PrepareParamKind: vector.PrepareParamNone},
		}, width: 65, id: types.T_decimal256},
		{name: "both null", values: []any{
			ParamValue{Value: nil, PrepareParamKind: vector.PrepareParamNone},
			ParamValue{Value: nil, PrepareParamKind: vector.PrepareParamNone},
		}, width: 65, id: types.T_decimal256},
		{name: "both integers", values: []any{
			ParamValue{Value: "1", PrepareParamKind: vector.PrepareParamInteger},
			ParamValue{Value: "2", PrepareParamKind: vector.PrepareParamInteger},
		}, width: 18, id: types.T_decimal64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			filled, _, err := FillValuesOfParamsInPlanWithSpecialization(
				context.Background(), preparePlan, tc.values)
			require.NoError(t, err)
			expr := filled.GetQuery().Nodes[len(filled.GetQuery().Nodes)-1].ProjectList[0]
			require.Equal(t, int32(tc.id), expr.Typ.Id)
			require.Equal(t, tc.width, expr.Typ.Width)
			require.Equal(t, int32(0), expr.Typ.Scale)
			if tc.name != "both integers" {
				require.False(t, expr.Typ.NotNullable)
			}
		})
	}
	after, err := preparePlan.Marshal()
	require.NoError(t, err)
	require.Equal(t, cachedPlan, after, "NULL specialization must not mutate the cached prepared plan")
}

func TestPreparedTimeArithmeticPreservesNullEnvelopeThroughAbs(t *testing.T) {
	prepared, err := runOneStmt(NewMockOptimizer(false), t,
		"prepare stmt_null_envelope_abs from 'select abs(cast(''00:00:01'' as time(0)) + (? + ?))'")
	require.NoError(t, err)
	filled, _, err := FillValuesOfParamsInPlanWithSpecialization(
		context.Background(), prepared.GetDcl().GetPrepare().Plan, []any{
			ParamValue{Value: nil, PrepareParamKind: vector.PrepareParamNone},
			ParamValue{Value: "2", PrepareParamKind: vector.PrepareParamInteger},
		})
	require.NoError(t, err)
	expr := filled.GetQuery().Nodes[len(filled.GetQuery().Nodes)-1].ProjectList[0]
	require.Equal(t, int32(types.T_decimal256), expr.Typ.Id)
	require.Equal(t, int32(65), expr.Typ.Width)
	require.Equal(t, int32(0), expr.Typ.Scale)
	require.False(t, expr.Typ.NotNullable)
}

func TestPreparedTimeArithmeticPreservesExplicitIntegerBoundary(t *testing.T) {
	prepared, err := runOneStmt(NewMockOptimizer(false), t,
		"prepare stmt_null_envelope_signed from 'select cast(''00:00:01'' as time(0)) + cast((? + ?) as signed)'")
	require.NoError(t, err)
	filled, _, err := FillValuesOfParamsInPlanWithSpecialization(
		context.Background(), prepared.GetDcl().GetPrepare().Plan, []any{
			ParamValue{Value: nil, PrepareParamKind: vector.PrepareParamNone},
			ParamValue{Value: "2", PrepareParamKind: vector.PrepareParamInteger},
		})
	require.NoError(t, err)
	expr := filled.GetQuery().Nodes[len(filled.GetQuery().Nodes)-1].ProjectList[0]
	require.Equal(t, int32(types.T_decimal64), expr.Typ.Id)
	require.Equal(t, int32(18), expr.Typ.Width)
	require.Equal(t, int32(0), expr.Typ.Scale)

	for _, tc := range []struct {
		name  string
		query string
		want  string
	}{
		{
			name:  "folded typed null",
			query: "prepare stmt_null_envelope_folded_signed from 'select cast(''00:00:01'' as time(0)) + (cast(NULL as signed) + ?)'",
		},
		{
			name:  "reverse outer order",
			query: "prepare stmt_null_envelope_folded_signed_reverse from 'select (cast(NULL as signed) + ?) + cast(''00:00:01'' as time(0))'",
		},
		{
			name:  "non-null signed control",
			query: "prepare stmt_null_envelope_signed_value from 'select cast(''00:00:01'' as time(0)) + (cast(1 as signed) + ?)'",
			want:  "4",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			folded, err := runOneStmt(NewMockOptimizer(false), t, tc.query)
			require.NoError(t, err)
			foldedPlan, _, err := FillValuesOfParamsInPlanWithSpecialization(
				context.Background(), folded.GetDcl().GetPrepare().Plan, []any{
					ParamValue{Value: "2", PrepareParamKind: vector.PrepareParamInteger},
				})
			require.NoError(t, err)
			foldedExpr := foldedPlan.GetQuery().Nodes[len(foldedPlan.GetQuery().Nodes)-1].ProjectList[0]
			require.Equal(t, int32(types.T_decimal64), foldedExpr.Typ.Id)
			require.Equal(t, int32(18), foldedExpr.Typ.Width)
			require.Equal(t, int32(0), foldedExpr.Typ.Scale)
			if tc.want == "" {
				return
			}

			proc := testutil.NewProc(t)
			defer proc.Free()
			executor, err := colexec.NewExpressionExecutor(proc, foldedExpr)
			require.NoError(t, err)
			defer executor.Free()
			result, err := executor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
			require.NoError(t, err)
			require.False(t, result.IsNull(0))
			require.Equal(t, tc.want, formatDecimalResult(result))
		})
	}
}

func TestPreparedTimeArithmeticPositionScopeKeepsUnselectedMarkers(t *testing.T) {
	prepared, err := runOneStmt(NewMockOptimizer(false), t,
		"prepare stmt_position_scope from 'select ? as direct, cast(''00:00:01'' as time(0)) + (? + ?) as nested'")
	require.NoError(t, err)
	preparePlan := prepared.GetDcl().GetPrepare().Plan
	originalRoot := preparePlan.GetQuery().Nodes[len(preparePlan.GetQuery().Nodes)-1]
	originalNested := originalRoot.ProjectList[1].String()

	runtimePlan, specialized, err := FillValuesOfParamsInPlanWithSpecializationAtPositions(
		context.Background(), preparePlan, []any{
			ParamValue{Value: "7", RuntimeType: types.T_int64.ToType(), HasRuntimeType: true, IsBinaryProtocol: true},
			ParamValue{Value: nil, PrepareParamKind: vector.PrepareParamNone},
			ParamValue{Value: "2", PrepareParamKind: vector.PrepareParamInteger},
		}, []int32{0})
	require.NoError(t, err)
	require.True(t, specialized)
	runtimeRoot := runtimePlan.GetQuery().Nodes[len(runtimePlan.GetQuery().Nodes)-1]
	require.Equal(t, originalNested, runtimeRoot.ProjectList[1].String(),
		"unselected TIME arithmetic markers must not be treated as execute-time NULL")
}

func TestPreparedTimeArithmeticPreservesExplicitDecimalAndNestedDomains(t *testing.T) {
	mock := NewMockOptimizer(false)
	mock.ctxt.tables["nation"].Cols[0].Typ = planpb.Type{
		Id: int32(types.T_time), Width: 6, Scale: 0,
	}
	ordinary, err := runOneStmt(mock, t,
		"select cast(n_nationkey as decimal(10,2)) + 10 from nation")
	require.NoError(t, err)
	ordinaryExpr := ordinary.GetQuery().Nodes[len(ordinary.GetQuery().Nodes)-1].ProjectList[0]
	require.Equal(t, int32(types.T_decimal128), ordinaryExpr.Typ.Id)
	require.Equal(t, int32(38), ordinaryExpr.Typ.Width)
	require.Equal(t, int32(2), ordinaryExpr.Typ.Scale)

	prepared, err := runOneStmt(mock, t,
		"prepare stmt_decimal from 'select cast(n_nationkey as decimal(10,2)) + ? from nation'")
	require.NoError(t, err)
	for _, value := range []string{"10", "9223372036854775807"} {
		filled, _, err := FillValuesOfParamsInPlanWithSpecialization(context.Background(),
			prepared.GetDcl().GetPrepare().Plan, []any{
				ParamValue{Value: value, PrepareParamKind: vector.PrepareParamInteger},
			})
		require.NoError(t, err)
		preparedExpr := filled.GetQuery().Nodes[len(filled.GetQuery().Nodes)-1].ProjectList[0]
		require.Equal(t, ordinaryExpr.Typ.Id, preparedExpr.Typ.Id, value)
		require.Equal(t, ordinaryExpr.Typ.Width, preparedExpr.Typ.Width, value)
		require.Equal(t, ordinaryExpr.Typ.Scale, preparedExpr.Typ.Scale, value)
	}

	for _, tc := range []struct {
		name string
		op   string
	}{
		{name: "nested add", op: "+"},
		{name: "nested subtract", op: "-"},
		{name: "nested modulo", op: "%"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ordinary, err := runOneStmt(mock, t,
				"select n_nationkey "+tc.op+" (10 "+tc.op+" 10) from nation")
			require.NoError(t, err)
			ordinaryExpr := ordinary.GetQuery().Nodes[len(ordinary.GetQuery().Nodes)-1].ProjectList[0]

			prepared, err := runOneStmt(mock, t, fmt.Sprintf(
				"prepare stmt_nested from 'select n_nationkey %s (? %s ?) from nation'",
				tc.op, tc.op))
			require.NoError(t, err)
			filled, _, err := FillValuesOfParamsInPlanWithSpecialization(context.Background(),
				prepared.GetDcl().GetPrepare().Plan, []any{
					ParamValue{Value: "1", PrepareParamKind: vector.PrepareParamInteger},
					ParamValue{Value: "2", PrepareParamKind: vector.PrepareParamInteger},
				})
			require.NoError(t, err)
			preparedExpr := filled.GetQuery().Nodes[len(filled.GetQuery().Nodes)-1].ProjectList[0]
			require.Equal(t, ordinaryExpr.Typ.Id, preparedExpr.Typ.Id)
			require.Equal(t, ordinaryExpr.Typ.Width, preparedExpr.Typ.Width)
			require.Equal(t, ordinaryExpr.Typ.Scale, preparedExpr.Typ.Scale)
		})
	}

}

func TestPreparedTimeArithmeticTimeZeroFractionalParameter(t *testing.T) {
	prepared, err := runOneStmt(NewMockOptimizer(false), t,
		"prepare stmt_runtime from 'select cast(''00:00:01'' as time(0)) * ?'")
	require.NoError(t, err)

	for _, tc := range []struct {
		name  string
		param ParamValue
		want  string
	}{
		{
			name: "text transport",
			param: ParamValue{
				Value: "1.2345678901234", PrepareParamKind: vector.PrepareParamDecimal,
			},
			want: "1.2345678901234",
		},
		{
			name: "SQL decimal source",
			param: ParamValue{
				Value: "1.2345678901234", PrepareParamKind: vector.PrepareParamDecimal,
				SourceType: types.New(types.T_decimal64, 14, 13), HasSourceType: true,
			},
			want: "1.2345678901234",
		},
		{
			name: "binary decimal source",
			param: ParamValue{
				Value: "1.2345678901234", PrepareParamKind: vector.PrepareParamDecimal,
				RuntimeType: types.New(types.T_decimal64, 14, 13), HasRuntimeType: true,
				IsBinaryProtocol: true,
			},
			want: "1.2345678901234",
		},
		{
			name: "large integer transport",
			param: ParamValue{
				Value: "1000000", PrepareParamKind: vector.PrepareParamInteger,
			},
			want: "1000000",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			filled, _, err := FillValuesOfParamsInPlanWithSpecialization(context.Background(),
				prepared.GetDcl().GetPrepare().Plan, []any{tc.param})
			require.NoError(t, err)

			proc := testutil.NewProc(t)
			defer proc.Free()
			expr := filled.GetQuery().Nodes[len(filled.GetQuery().Nodes)-1].ProjectList[0]
			executor, err := colexec.NewExpressionExecutor(proc, expr)
			require.NoError(t, err)
			defer executor.Free()
			result, err := executor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
			require.NoError(t, err)
			require.False(t, result.IsNull(0))
			require.Equal(t, tc.want, formatDecimalResult(result))
		})
	}
}

func TestOrdinaryTimeZeroFractionalLiteral(t *testing.T) {
	query, err := runOneStmt(NewMockOptimizer(false), t,
		"select cast('00:00:01' as time(0)) * 1.25")
	require.NoError(t, err)
	proc := testutil.NewProc(t)
	defer proc.Free()
	expr := query.GetQuery().Nodes[len(query.GetQuery().Nodes)-1].ProjectList[0]
	executor, err := colexec.NewExpressionExecutor(proc, expr)
	require.NoError(t, err)
	defer executor.Free()
	result, err := executor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
	require.NoError(t, err)
	require.False(t, result.IsNull(0))
	got := vector.GetFixedAtWithTypeCheck[types.Decimal128](result, 0)
	require.Equal(t, "1.25", got.Format(result.GetType().Scale))
}

func TestPreparedTimeArithmeticOrdinaryLiteralControl(t *testing.T) {
	mock := NewMockOptimizer(false)
	mock.ctxt.tables["nation"].Cols[0].Typ = planpb.Type{
		Id: int32(types.T_time), Width: 6, Scale: 3,
	}
	_, err := runOneStmt(mock, t, "select n_nationkey * 10 from nation")
	require.NoError(t, err)
}
