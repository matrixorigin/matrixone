// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func TestPrepareCoalesceNumericPeerMetadata(t *testing.T) {
	for _, tc := range []struct {
		name, sql    string
		oid          types.T
		width, scale int32
	}{
		{"decimal", "select coalesce(?,cast(0 as decimal(20,6)))", types.T_decimal128, 20, 6},
		{"reversed decimal", "select coalesce(cast(0 as decimal(20,6)),?)", types.T_decimal128, 20, 6},
		{"multiple markers and NULL", "select coalesce(?,null,?,cast(0 as decimal(20,6)))", types.T_decimal128, 20, 6},
		{"all peer scales", "select coalesce(?,cast(0 as decimal(20,6)),cast(0 as decimal(30,10)))", types.T_decimal128, 30, 10},
		{"all peer integral digits", "select coalesce(cast(0 as decimal(38,0)),?,cast(0 as decimal(20,6)))", types.T_decimal256, 44, 6},
		{"decimal64", "select coalesce(?,cast(0 as decimal(10,2)))", types.T_decimal64, 10, 2},
		{"integer", "select coalesce(?,1)", types.T_int64, 0, 0},
		{"unsigned", "select coalesce(?,cast(1 as unsigned))", types.T_uint64, 0, 0},
		{"float", "select coalesce(?,cast(1 as double))", types.T_float64, 0, 0},
		{"column peer", "select coalesce(?,cast(n_nationkey as decimal(20,6))) from nation", types.T_decimal128, 20, 6},
		{"nested resolved peer", "select coalesce(coalesce(?,cast(0 as decimal(20,6))),?)", types.T_decimal128, 20, 6},
		{"enclosing arithmetic", "select coalesce(?,cast(0 as decimal(20,6)))+cast(0 as decimal(20,6))", types.T_decimal128, 38, 6},
		{"explicit CAST control", "select cast(? as decimal(20,6))", types.T_decimal128, 20, 6},
		{"concrete string boundary", `select coalesce(?,cast(0 as decimal(20,6)),"s")`, types.T_varchar, 0, 0},
		{"explicit string boundary", "select coalesce(cast(? as char),cast(0 as decimal(20,6)))", types.T_varchar, 0, 0},
		{"binary boundary", `select coalesce(?,cast("s" as binary),cast(0 as decimal(20,6)))`, types.T_varchar, 0, 0},
		{"unsupported numeric union falls back", "select coalesce(?,cast(0 as decimal(65,0)),cast(0 as decimal(65,30)))", types.T_varchar, 0, 0},
		{"all unknown", "select coalesce(?,?)", types.T_text, 0, 0},
		{"unknown and NULL", "select coalesce(?,null)", types.T_text, 0, 0},
		{"greatest control", "select greatest(?,cast(0 as decimal(20,6)))", types.T_text, 0, 0},
		{"least control", "select least(?,cast(0 as decimal(20,6)))", types.T_text, 0, 0},
		{"ifnull control", "select ifnull(?,cast(0 as decimal(20,6)))", types.T_varchar, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prepared := buildPreparedAggregatePlan(t, tc.sql)
			columns := GetResultColumnsFromPlan(prepared.Plan)
			require.Len(t, columns, 1)
			require.Equal(t, int32(tc.oid), columns[0].Typ.Id)
			if tc.oid.IsDecimal() {
				require.Equal(t, tc.width, columns[0].Typ.Width)
				require.Equal(t, tc.scale, columns[0].Typ.Scale)
			}
		})
	}
}

func TestPrepareCoalesceNumericHintPreservesRuntimeSource(t *testing.T) {
	prepared := buildPreparedAggregatePlan(t, "select coalesce(?,cast(0 as decimal(20,6)))")
	require.Equal(t, []int32{int32(types.T_any)}, prepared.ParamTypes)
	expr := findPlanFunctionExpr(prepared.Plan, "coalesce")
	require.NotNil(t, expr)
	arg := expr.GetF().Args[0]
	require.True(t, arg.GetPreparedNumeric().GetProvisionalResultCast())
	cast := arg.GetF()
	require.NotNil(t, cast)
	require.Equal(t, "cast", cast.GetFunc().GetObjName())
	_, overload := function.DecodeOverloadID(cast.Func.Obj)
	require.Zero(t, overload)
	require.False(t, cast.SyntaxExplicitCast)
	marker := cast.Args[0]
	require.NotNil(t, marker.GetP())
	require.Zero(t, marker.GetP().Pos)
	require.Equal(t, int32(types.T_text), marker.Typ.Id)
	snapshot := prepared.Plan.String()

	// The legacy plan-specialization path must discard the hint for concrete
	// text as well as decimals. Actual binary JDBC values use the AST rebind
	// path and are covered by the public probe.
	for _, tc := range []struct {
		name  string
		value ParamValue
		want  types.T
	}{
		{"decimal", ParamValue{Value: "1.25", SourceType: types.New(types.T_decimal128, 20, 6), HasSourceType: true}, types.T_decimal128},
		{"NULL", ParamValue{Value: nil, SourceType: types.T_any.ToType(), HasSourceType: true, IsBinaryProtocol: true}, types.T_decimal128},
		{"text", ParamValue{Value: "text", SourceType: types.T_varchar.ToType(), HasSourceType: true}, types.T_varchar},
		{"binary", ParamValue{Value: "text", SourceType: types.T_varbinary.ToType(), HasSourceType: true}, types.T_varchar},
		{"decimal after text", ParamValue{Value: "1.25", SourceType: types.New(types.T_decimal128, 20, 6), HasSourceType: true}, types.T_decimal128},
	} {
		t.Run(tc.name, func(t *testing.T) {
			filled, _, err := FillValuesOfParamsInPlanWithSpecialization(context.Background(), prepared.Plan, []any{tc.value})
			require.NoError(t, err)
			columns := GetResultColumnsFromPlan(filled)
			require.Len(t, columns, 1)
			require.Equal(t, int32(tc.want), columns[0].Typ.Id)
			require.Equal(t, snapshot, prepared.Plan.String(), "specialization must not mutate the prepared template")
		})
	}
}
