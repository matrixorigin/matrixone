// Copyright 2021 - 2026 Matrix Origin
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

package function

import (
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestHyperbolicValues(t *testing.T) {
	// 固定参考值来自独立的 libc 数学实现，而非执行器使用的 Go math 函数。
	for _, tc := range []struct {
		name  string
		fn    executeLogicOfOverload
		input []float64
		want  []float64
		nulls []bool
	}{
		{
			name: "tanh", fn: builtInTanh,
			input: []float64{-2, -1, -1e-20, 0, 1e-20, 1, 2, -math.MaxFloat64, math.MaxFloat64},
			want:  []float64{-0.9640275800758169, -0.7615941559557649, -1e-20, 0, 1e-20, 0.7615941559557649, 0.9640275800758169, -1, 1},
		},
		{
			name: "cosh", fn: builtInCosh,
			input: []float64{-2, -1, -710, 0, 1, 2, 710},
			want:  []float64{3.7621956910836314, 1.5430806348152437, 1.1169973830808555e308, 1, 1.5430806348152437, 3.7621956910836314, 1.1169973830808555e308},
		},
		{
			name: "asinh", fn: builtInASinh,
			input: []float64{-math.MaxFloat64, -2, -1, -1e-20, 0, 1e-20, 1, 2, math.MaxFloat64},
			want:  []float64{-710.4758600739439, -1.4436354751788103, -0.881373587019543, -1e-20, 0, 1e-20, 0.881373587019543, 1.4436354751788103, 710.4758600739439},
		},
		{
			name: "acosh", fn: builtInACosh,
			input: []float64{-1, 0, math.Nextafter(1, 0), 1, math.Nextafter(1, 2), 2, math.MaxFloat64},
			want:  []float64{0, 0, 0, 0, 2.1073424255447017e-8, 1.3169578969248166, 710.4758600739439},
			nulls: []bool{true, true, true, false, false, false, false},
		},
		{
			name: "atanh", fn: builtInATanh,
			input: []float64{-2, -1, math.Nextafter(-1, 0), -0.5, -1e-20, 0, 1e-20, 0.5, math.Nextafter(1, 0), 1, 2},
			want:  []float64{0, 0, -18.714973875118524, -0.5493061443340548, -1e-20, 0, 1e-20, 0.5493061443340548, 18.714973875118524, 0, 0},
			nulls: []bool{true, true, false, false, false, false, false, false, false, true, true},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			fc := NewFunctionTestCase(proc,
				[]FunctionTestInput{NewFunctionTestInput(types.T_float64.ToType(), tc.input, nil)},
				NewFunctionTestResult(types.T_float64.ToType(), false, tc.want, tc.nulls), tc.fn)
			defer fc.Free()
			result, err := fc.DebugRun()
			require.NoError(t, err)
			assertHyperbolicRows(t, result, tc.want, tc.nulls)
		})
	}
}

func TestHyperbolicConstantsNullsAndEmpty(t *testing.T) {
	for _, tc := range hyperbolicTestFunctions() {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			fc := NewFunctionTestCase(proc,
				[]FunctionTestInput{NewFunctionTestConstInput(types.T_float64.ToType(), []float64{tc.input}, nil)},
				NewFunctionTestResult(types.T_float64.ToType(), false, nil, nil), tc.fn)
			defer fc.Free()
			fc.fnLength = 3
			result, err := fc.DebugRun()
			require.NoError(t, err)
			assertHyperbolicRows(t, result, []float64{tc.want, tc.want, tc.want}, nil)

			fc.parameters[0].GetNulls().Add(0)
			result, err = fc.DebugRun()
			require.NoError(t, err)
			assertHyperbolicRows(t, result, []float64{0, 0, 0}, []bool{true, true, true})

			fc.fnLength = 0
			result, err = fc.DebugRun()
			require.NoError(t, err)
			assertHyperbolicRows(t, result, []float64{}, nil)

			fc.parameters[0].GetNulls().Reset()
			fc.fnLength = 3
			result, err = fc.DebugRun()
			require.NoError(t, err)
			assertHyperbolicRows(t, result, []float64{tc.want, tc.want, tc.want}, nil)
		})
	}
}

func TestHyperbolicSelectionAndReuse(t *testing.T) {
	for _, tc := range hyperbolicTestFunctions() {
		for _, constant := range []bool{false, true} {
			name := tc.name + "/vector"
			if constant {
				name = tc.name + "/constant"
			}
			t.Run(name, func(t *testing.T) {
				proc := testutil.NewProcess(t)
				defer proc.Free()
				input := NewFunctionTestInput(types.T_float64.ToType(), []float64{tc.input, 711, 711}, []bool{false, false, true})
				if constant {
					input = NewFunctionTestConstInput(types.T_float64.ToType(), []float64{tc.input}, nil)
				}
				fc := NewFunctionTestCase(proc, []FunctionTestInput{input},
					NewFunctionTestResult(types.T_float64.ToType(), false, nil, nil), tc.fn).
					WithSelectList(&FunctionSelectList{AnyNull: true, SelectList: []bool{true, false, false}})
				defer fc.Free()
				fc.fnLength = 3
				result, err := fc.DebugRun()
				require.NoError(t, err)
				assertHyperbolicRows(t, result, []float64{tc.want, 0, 0}, []bool{false, true, true})

				fc = fc.WithSelectList(&FunctionSelectList{AllNull: true})
				result, err = fc.DebugRun()
				require.NoError(t, err)
				assertHyperbolicRows(t, result, []float64{0, 0, 0}, []bool{true, true, true})

				fc = fc.WithSelectList(nil)
				if !constant {
					vector.MustFixedColNoTypeCheck[float64](fc.parameters[0])[1] = tc.input
				}
				result, err = fc.DebugRun()
				require.NoError(t, err)
				nulls := []bool{false, false, !constant}
				assertHyperbolicRows(t, result, []float64{tc.want, tc.want, tc.want}, nulls)
			})
		}
	}
}

func TestHyperbolicSpecialValues(t *testing.T) {
	negativeZero := math.Copysign(0, -1)
	for _, tc := range []struct {
		name  string
		fn    executeLogicOfOverload
		input []float64
		want  []float64
		nulls []bool
	}{
		{"tanh", builtInTanh, []float64{math.NaN(), negativeZero, math.Inf(-1), math.Inf(1)}, []float64{math.NaN(), negativeZero, -1, 1}, nil},
		{"cosh", builtInCosh, []float64{math.NaN(), negativeZero}, []float64{math.NaN(), 1}, nil},
		{"asinh", builtInASinh, []float64{math.NaN(), negativeZero, math.Inf(-1), math.Inf(1)}, []float64{math.NaN(), negativeZero, math.Inf(-1), math.Inf(1)}, nil},
		{"acosh", builtInACosh, []float64{math.NaN(), math.Inf(-1), math.Inf(1)}, []float64{math.NaN(), 0, math.Inf(1)}, []bool{false, true, false}},
		{"atanh", builtInATanh, []float64{math.NaN(), negativeZero, math.Inf(-1), math.Inf(1)}, []float64{math.NaN(), negativeZero, 0, 0}, []bool{false, false, true, true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			fc := NewFunctionTestCase(proc,
				[]FunctionTestInput{NewFunctionTestInput(types.T_float64.ToType(), tc.input, nil)},
				NewFunctionTestResult(types.T_float64.ToType(), false, nil, nil), tc.fn)
			defer fc.Free()
			result, err := fc.DebugRun()
			require.NoError(t, err)
			assertHyperbolicRows(t, result, tc.want, tc.nulls)
		})
	}
}

func TestHyperbolicCoshOverflow(t *testing.T) {
	for _, value := range []float64{-711, 711, -math.MaxFloat64, math.MaxFloat64, math.Inf(-1), math.Inf(1)} {
		for _, constant := range []bool{false, true} {
			proc := testutil.NewProcess(t)
			func() {
				defer proc.Free()
				input := NewFunctionTestInput(types.T_float64.ToType(), []float64{0, value}, nil)
				if constant {
					input = NewFunctionTestConstInput(types.T_float64.ToType(), []float64{value}, nil)
				}
				fc := NewFunctionTestCase(proc, []FunctionTestInput{input},
					NewFunctionTestResult(types.T_float64.ToType(), true, nil, nil), builtInCosh)
				defer fc.Free()
				_, err := fc.DebugRun()
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange), "value=%v constant=%v err=%v", value, constant, err)
			}()
		}
	}
}

func TestHyperbolicResolutionAndEncodedExecution(t *testing.T) {
	for _, tc := range hyperbolicTestFunctions() {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			floatType := types.T_float64.ToType()
			for _, inputType := range []types.T{types.T_float64, types.T_float32, types.T_int64, types.T_uint64, types.T_decimal64, types.T_decimal128, types.T_bool, types.T_varchar, types.T_any} {
				resolved, err := GetFunctionByName(proc.Ctx, tc.name, []types.Type{inputType.ToType()})
				require.NoError(t, err, inputType)
				require.Equal(t, floatType, resolved.GetReturnType())
				require.Equal(t, EncodeOverloadID(tc.id, 0), resolved.GetEncodedOverloadID())
				require.Equal(t, inputType != types.T_float64, resolved.needCast)
				if resolved.needCast {
					targetType := floatType
					if inputType == types.T_varchar || inputType == types.T_any {
						targetType.Scale = -1
					}
					require.Equal(t, []types.Type{targetType}, resolved.targetTypes)
				}
			}
			for _, args := range [][]types.Type{nil, {floatType, floatType}, {types.T_date.ToType()}} {
				_, err := GetFunctionByName(proc.Ctx, tc.name, args)
				require.Error(t, err, args)
			}
			input := newVectorByType(proc.Mp(), floatType, []float64{tc.input, 711}, nil)
			defer input.Free(proc.Mp())
			input.GetNulls().Add(1)
			result, err := RunFunctionDirectly(proc, EncodeOverloadID(tc.id, 0), []*vector.Vector{input}, 2)
			require.NoError(t, err)
			defer result.Free(proc.Mp())
			assertHyperbolicRows(t, result, []float64{tc.want, 0}, []bool{false, true})
		})
	}
}

func hyperbolicTestFunctions() []struct {
	name  string
	id    int32
	fn    executeLogicOfOverload
	input float64
	want  float64
} {
	return []struct {
		name  string
		id    int32
		fn    executeLogicOfOverload
		input float64
		want  float64
	}{
		{"tanh", TANH, builtInTanh, 1, 0.7615941559557649},
		{"cosh", COSH, builtInCosh, 1, 1.5430806348152437},
		{"asinh", ASINH, builtInASinh, 1, 0.881373587019543},
		{"acosh", ACOSH, builtInACosh, 1, 0},
		{"atanh", ATANH, builtInATanh, 0.5, 0.5493061443340548},
	}
}

func assertHyperbolicRows(t *testing.T, result *vector.Vector, want []float64, wantNulls []bool) {
	t.Helper()
	require.Equal(t, types.T_float64.ToType(), *result.GetType())
	require.Equal(t, len(want), result.Length())
	for i, expected := range want {
		isNull := len(wantNulls) != 0 && wantNulls[i]
		require.Equal(t, isNull, result.GetNulls().Contains(uint64(i)), "row %d", i)
		if isNull {
			continue
		}
		actual := vector.GetFixedAtNoTypeCheck[float64](result, i)
		switch {
		case math.IsNaN(expected):
			require.True(t, math.IsNaN(actual), "row %d", i)
		case expected == 0:
			require.Equal(t, expected, actual, "row %d", i)
			require.Equal(t, math.Signbit(expected), math.Signbit(actual), "row %d", i)
		case math.IsInf(expected, 0):
			require.Equal(t, expected, actual, "row %d", i)
		default:
			require.InEpsilon(t, expected, actual, 2e-15, "row %d", i)
		}
	}
}
