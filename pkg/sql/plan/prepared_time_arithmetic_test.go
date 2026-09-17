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
