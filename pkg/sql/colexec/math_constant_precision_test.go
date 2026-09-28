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

package colexec

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// This fixture intentionally retains the unfurled parameter CAST tree: folding
// the warning-producing conversion would hide the constant-control regression.
func TestMathConstantPrecisionWarningsAndAdmission(t *testing.T) {
	for _, name := range []string{"ceil", "floor"} {
		t.Run(name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			baseline := proc.Mp().CurrNB()
			defer func() { require.Equal(t, baseline, proc.Mp().CurrNB()) }()
			proc.SetBaseProcessRunningStatus(true)
			proc.GetSessionInfo().MySQLNumericCompatibilityMode = true
			session := &preparedCastWarningSession{}
			proc.Session = session

			call := func(name string, args ...*plan.Expr) *plan.Expr {
				inputs := make([]types.Type, len(args))
				for i, arg := range args {
					inputs[i] = types.New(types.T(arg.Typ.Id), arg.Typ.Width, arg.Typ.Scale)
				}
				fn, err := function.GetFunctionByName(proc.Ctx, name, inputs)
				require.NoError(t, err)
				typ := fn.GetReturnType()
				return &plan.Expr{Typ: plan.Type{Id: int32(typ.Oid), Width: typ.Width, Scale: typ.Scale},
					Expr: &plan.Expr_F{F: &plan.Function{
						Func: &plan.ObjectRef{Obj: fn.GetEncodedOverloadID(), ObjName: name}, Args: args,
					}}}
			}
			cast := func(arg *plan.Expr, target types.T) *plan.Expr {
				return call("cast", arg, &plan.Expr{Typ: plan.Type{Id: int32(target)},
					Expr: &plan.Expr_T{T: &plan.TargetType{}}})
			}
			value := &plan.Expr{Typ: plan.Type{Id: int32(types.T_float64)},
				Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Dval{Dval: 12.345}}}}
			parameter := &plan.Expr{Typ: plan.Type{Id: int32(types.T_text)},
				Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}}}
			double := cast(parameter, types.T_float64)
			// Exercise the explicit-DOUBLE boundary inside ordinary INT64 precision.
			double.GetF().Func.Obj = function.EncodeOverloadID(function.CAST, 1)
			executor, err := NewExpressionExecutor(proc, call(name, value, cast(double, types.T_int64)))
			require.NoError(t, err)
			defer executor.Free()

			eval := func(text *string, rows int, selected []bool) (*vector.Vector, error) {
				params := vector.NewVec(types.T_text.ToType())
				defer params.Free(proc.Mp())
				if text == nil {
					require.NoError(t, vector.AppendBytes(params, nil, true, proc.Mp()))
				} else {
					require.NoError(t, vector.AppendBytes(params, []byte(*text), false, proc.Mp()))
				}
				proc.SetPrepareParamsWithMeta(params, nil, []vector.PrepareParamKind{vector.PrepareParamNone})
				defer proc.SetPrepareParams(nil)
				input := batch.New(nil)
				input.SetRowCount(rows)
				executor.ResetForNextQuery()
				return executor.Eval(proc, []*batch.Batch{input}, selected)
			}
			text := "2.5tail"
			for _, tc := range []struct {
				name      string
				selection []bool
				warnings  int
			}{
				{name: "all rows", warnings: 4},
				{name: "masked first row", selection: []bool{false, true, false, true}, warnings: 2},
				{name: "all masked", selection: []bool{false, false, false, false}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					before := session.warningCount
					result, err := eval(&text, 4, tc.selection)
					require.NoError(t, err)
					require.Equal(t, 4, result.Length())
					require.Equal(t, tc.warnings, session.warningCount-before)
					for row := 0; row < 4; row++ {
						if tc.selection != nil && !tc.selection[row] {
							require.True(t, result.IsNull(uint64(row)))
						} else {
							require.False(t, result.IsNull(uint64(row)))
							require.InDelta(t, 12.345, vector.GetFixedAtNoTypeCheck[float64](result, row), 1e-10)
						}
					}
				})
			}
			// Empty batches must never read the first precision element.
			_, err = eval(&text, 0, nil)
			require.ErrorContains(t, err, "not const", "retain empty flat-vector rejection without reading row zero")
			_, err = eval(nil, 4, nil)
			require.ErrorContains(t, err, "not const")
			proc.GetSessionInfo().MySQLNumericCompatibilityMode = false
			bad := "bad"
			_, err = eval(&bad, 4, nil)
			require.ErrorContains(t, err, "invalid numeric string")
			valid := "2.5"
			result, err := eval(&valid, 4, nil)
			require.NoError(t, err)
			require.InDelta(t, 12.345, vector.GetFixedAtNoTypeCheck[float64](result, 0), 1e-10)

			// Equal values do not make a row-dependent expression a constant.
			column := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)},
				Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 0}}}
			for _, tc := range []struct {
				name      string
				precision *plan.Expr
			}{
				{name: "equal column", precision: column},
				{name: "volatile", precision: cast(call("rand"), types.T_int64)},
			} {
				t.Run(tc.name, func(t *testing.T) {
					rejected, err := NewExpressionExecutor(proc, call(name, value, tc.precision))
					require.NoError(t, err)
					defer rejected.Free()
					input := batch.NewWithSize(1)
					input.Vecs[0] = vector.NewVec(types.T_int64.ToType())
					defer input.Clean(proc.Mp())
					require.NoError(t, vector.AppendFixedList(input.Vecs[0], []int64{3, 3}, nil, proc.Mp()))
					input.SetRowCount(2)
					_, err = rejected.Eval(proc, []*batch.Batch{input}, nil)
					require.ErrorContains(t, err, "not const")
				})
			}
		})
	}
}
