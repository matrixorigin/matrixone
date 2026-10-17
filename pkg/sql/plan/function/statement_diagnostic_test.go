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

package function

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestStatementParameterDiagnosticCapabilities(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []types.T
		want bool
	}{
		{"time", []types.T{types.T_varchar}, true},
		{"date", []types.T{types.T_varchar}, true},
		{"maketime", []types.T{types.T_int64, types.T_int64, types.T_int64}, true},
		{"sec_to_time", []types.T{types.T_int64}, true},
		{"timestamp", []types.T{types.T_varchar, types.T_varchar}, true},
		{"date_add", []types.T{types.T_datetime, types.T_int64, types.T_int64}, true},
		{"date_sub", []types.T{types.T_datetime, types.T_int64, types.T_int64}, true},
		{"date_add", []types.T{types.T_time, types.T_varchar, types.T_int64}, true},
		{"date_sub", []types.T{types.T_time, types.T_varchar, types.T_int64}, true},
		{"date_add", []types.T{types.T_time, types.T_float64, types.T_int64}, true},
		{"date_sub", []types.T{types.T_time, types.T_decimal128, types.T_int64}, true},
		{"timestampadd", []types.T{types.T_varchar, types.T_int64, types.T_datetime}, true},
		{"addtime", []types.T{types.T_time, types.T_varchar}, true},
		{"subtime", []types.T{types.T_time, types.T_varchar}, true},
		{"timediff", []types.T{types.T_time, types.T_time}, true},
		{"period_add", []types.T{types.T_int64, types.T_int64}, true},
		{"period_diff", []types.T{types.T_int64, types.T_int64}, true},
		{"abs", []types.T{types.T_int64}, false},
		// Interval normalization communicates NULL/sentinel to its arithmetic
		// consumer. It does not itself publish a SQL evaluation diagnostic.
		{"to_interval_microsecond", []types.T{types.T_varchar, types.T_int64}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expr := diagnosticTestCall(t, tc.name, tc.args...)
			require.True(t, IsStatementConstantInput(expr))
			require.Equal(t, tc.want, MayDiagnoseStatementParameter(expr))
			expr.GetF().Args[0].Expr = &plan.Expr_Col{Col: &plan.ColRef{}}
			require.False(t, IsStatementConstantInput(expr))
			require.False(t, MayDiagnoseStatementParameter(expr))
		})
	}
	// These calls own diagnostics per evaluated row. Parameters do not make
	// them statement-level diagnostic owners; safe folding still uses a probe.
	for _, tc := range []struct {
		name string
		args []types.T
	}{
		{"ln", []types.T{types.T_float64}},
		{"log", []types.T{types.T_float64}},
		{"log", []types.T{types.T_float64, types.T_float64}},
		{"log2", []types.T{types.T_float64}},
		{"log10", []types.T{types.T_float64}},
		{"format", []types.T{types.T_varchar, types.T_int64, types.T_varchar}},
		{"str_to_date", []types.T{types.T_varchar, types.T_varchar, types.T_datetime}},
		{"str_to_date", []types.T{types.T_varchar, types.T_varchar, types.T_date}},
		{"str_to_date", []types.T{types.T_varchar, types.T_varchar, types.T_time}},
	} {
		t.Run("row_diagnostics/"+tc.name, func(t *testing.T) {
			expr := diagnosticTestCall(t, tc.name, tc.args...)
			require.False(t, IsStatementConstantInput(expr))
			require.False(t, MayDiagnoseStatementParameter(expr))
			require.True(t, IsFoldCandidate(expr, true))
		})
	}
	typedDate := diagnosticTestCall(t, "date", types.T_datetime)
	require.False(t, MayDiagnoseStatementParameter(typedDate))
	for _, target := range []types.T{types.T_time, types.T_date, types.T_datetime, types.T_timestamp, types.T_float64} {
		t.Run("cast/"+target.String(), func(t *testing.T) {
			expr := diagnosticTestCall(t, "cast", types.T_varchar, target)
			expr.Typ.Id = int32(target)
			expr.GetF().Args[1].Expr = &plan.Expr_T{T: &plan.TargetType{}}
			require.True(t, MayDiagnoseStatementParameter(expr))
			expr.GetF().SyntaxExplicitCast = true
			require.Equal(t, target != types.T_float64, MayDiagnoseStatementParameter(expr))
			require.Equal(t, target == types.T_float64, ContainsRowScopedConversion(expr))
		})
	}
	for _, name := range []string{"cast_strict", "cast_assign", "cast_ignore"} {
		t.Run(name, func(t *testing.T) {
			expr := diagnosticTestCall(t, name, types.T_varchar, types.T_time)
			expr.Typ.Id = int32(types.T_time)
			expr.GetF().Args[1].Expr = &plan.Expr_T{T: &plan.TargetType{}}
			require.True(t, ContainsRowScopedConversion(expr))
			require.False(t, MayDiagnoseStatementParameter(expr))
		})
	}
	for _, name := range []string{"rand", "now"} {
		t.Run(name, func(t *testing.T) {
			expr := diagnosticTestCall(t, name)
			require.False(t, IsStatementConstantInput(expr))
			require.False(t, MayDiagnoseStatementParameter(expr))
		})
	}
	constant := diagnosticTestCall(t, "time", types.T_varchar)
	constant.GetF().Args[0].Expr = &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "bad"}}}
	require.True(t, IsStatementConstantInput(constant))
	require.False(t, MayDiagnoseStatementParameter(constant), "literals use isolated evaluation probing")
	require.False(t, IsStatementConstantInput(nil))
	require.False(t, MayDiagnoseStatementParameter(nil))
}

func TestExplicitDecimalCastDiagnosticOwnership(t *testing.T) {
	for _, source := range []types.T{types.T_int64, types.T_float64, types.T_decimal128, types.T_varchar} {
		for _, target := range []types.T{types.T_decimal64, types.T_decimal128, types.T_decimal256} {
			for _, representation := range []string{"implicit", "explicit overload", "explicit syntax"} {
				t.Run(source.String()+"/"+target.String()+"/"+representation, func(t *testing.T) {
					expr := diagnosticTestCall(t, "cast", source, target)
					expr.Typ.Id = int32(target)
					fn := expr.GetF()
					fn.Args[1].Expr = &plan.Expr_T{T: &plan.TargetType{}}
					if representation == "explicit overload" {
						fn.Func.Obj = EncodeOverloadID(CAST, 1)
					}
					fn.SyntaxExplicitCast = representation == "explicit syntax"
					want := representation != "implicit"
					require.Equal(t, want, IsExplicitDecimalCast(expr))
					require.Equal(t, want, ContainsRowScopedConversion(expr))
					if want {
						require.False(t, MayDiagnoseStatementParameter(expr))
					}
				})
			}
		}
	}
	require.False(t, IsExplicitDecimalCast(nil))
	require.False(t, IsExplicitDecimalCast(&plan.Expr{Typ: plan.Type{Id: int32(types.T_decimal64)}}))
	require.False(t, IsExplicitDecimalCast(&plan.Expr{
		Typ: plan.Type{Id: int32(types.T_decimal64)},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: EncodeOverloadID(CAST, 1)},
			Args: []*plan.Expr{{Typ: plan.Type{Id: int32(types.T_interval)}}},
		}},
	}), "classification must reject unsupported source domains without constructing a runtime type")
}

func diagnosticTestCall(t *testing.T, name string, kinds ...types.T) *plan.Expr {
	t.Helper()
	args := make([]*plan.Expr, len(kinds))
	argTypes := make([]types.Type, len(kinds))
	for i, kind := range kinds {
		argTypes[i] = kind.ToType()
		args[i] = &plan.Expr{Typ: plan.Type{Id: int32(kind)}, Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: int32(i)}}}
	}
	fn, err := GetFunctionByName(context.Background(), name, argTypes)
	require.NoError(t, err)
	return &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{Obj: fn.GetEncodedOverloadID(), ObjName: name}, Args: args,
	}}}
}
