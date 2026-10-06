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

package aggexec

import (
	"bytes"
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

// Variance inputs keep their existing memory domain and are owned before any
// append can fail. Child scenarios share only immutable inputs.
func varianceInput[T any](t *testing.T, mp *mpool.MPool, typ types.Type, values []T, nulls []bool) *vector.Vector {
	t.Helper()
	input := vector.NewVec(typ)
	t.Cleanup(func() { input.Free(mp) })
	require.NoError(t, vector.AppendFixedList(input, values, nulls, mp))
	return input
}

func varianceExec(t *testing.T, mp *mpool.MPool, id int64, distinct bool, typ types.Type) AggFuncExec {
	t.Helper()
	exec, err := MakeAgg(mp, id, distinct, typ)
	require.NoError(t, err)
	t.Cleanup(exec.Free)
	args, _ := exec.TypesInfo()
	require.Equal(t, []types.Type{typ}, args)
	return exec
}

func varianceResult(t *testing.T, mp *mpool.MPool, exec AggFuncExec, typ types.Type, nulls []bool) *vector.Vector {
	t.Helper()
	results, err := exec.Flush()
	for _, result := range results {
		if result != nil {
			t.Cleanup(func() { result.Free(mp) })
		}
	}
	require.NoError(t, err)
	require.Len(t, results, 1)
	result := results[0]
	require.NotNil(t, result)
	require.Equal(t, typ, *result.GetType())
	require.Equal(t, len(nulls), result.Length())
	for row, want := range nulls {
		require.Equal(t, want, result.IsNull(uint64(row)), "row %d NULL", row)
	}
	return result
}

func TestVarianceStateLargeOffset(t *testing.T) {
	mean, variance, varianceExponent, count := 0.0, 0.0, int64(0), int64(0)
	for i := 0; i < 7; i++ {
		var err error
		mean, variance, varianceExponent, count, err = updateVarianceState(
			mean, variance, varianceExponent, count, 1_000_000_000_000+float64(i))
		require.NoError(t, err)
	}

	require.Equal(t, int64(7), count)
	require.InEpsilon(t, 1_000_000_000_003.0, mean, 1e-15)
	require.InEpsilon(t, 4.0, variance, 1e-15)
	require.Zero(t, varianceExponent)

	varPop := &varStdDevExec[float64, float64]{isVar: true, isPop: true, f2t: float64ToResult}
	stddevPop := &varStdDevExec[float64, float64]{isVar: false, isPop: true, f2t: float64ToResult}
	result, err := varPop.getResult(variance, varianceExponent, count)
	require.NoError(t, err)
	stddev, err := stddevPop.getResult(variance, varianceExponent, count)
	require.NoError(t, err)
	require.InEpsilon(t, 4.0, result, 1e-15)
	require.InEpsilon(t, 2.0, stddev, 1e-15)
}

func TestDecimalVarianceResultUsesShortestRoundTripRepresentation(t *testing.T) {
	tests := []struct {
		name  string
		value float64
		scale int32
		want  string
	}{
		{name: "temporal variance scale 6", value: 680202686800.6666, scale: 6, want: "680202686800.666600"},
		{name: "adjacent temporal variance below", value: math.Nextafter(680202686800.6666, math.Inf(-1)), scale: 6, want: "680202686800.666500"},
		{name: "adjacent temporal variance above", value: math.Nextafter(680202686800.6666, math.Inf(1)), scale: 6, want: "680202686800.666700"},
		{name: "temporal variance scale 12", value: 680202686800.6666, scale: 12, want: "680202686800.666600000000"},
		{name: "small decimal", value: 0.01, scale: 12, want: "0.010000000000"},
		{name: "below half quantum", value: 1.23456749, scale: 6, want: "1.234567"},
		{name: "half quantum rounds", value: 1.2345675, scale: 6, want: "1.234568"},
		{name: "above half quantum", value: 1.23456751, scale: 6, want: "1.234568"},
		{name: "negative", value: -0.01, scale: 12, want: "-0.010000000000"},
		{name: "positive zero", value: 0, scale: 6, want: "0.000000"},
		{name: "negative zero", value: math.Copysign(0, -1), scale: 6, want: "0.000000"},
		{name: "scientific notation", value: 1e-20, scale: 38, want: "0.00000000000000000001000000000000000000"},
		{name: "smallest float underflows at decimal scale", value: math.SmallestNonzeroFloat64, scale: 38, want: "0.00000000000000000000000000000000000000"},
		{name: "large shortest integer", value: 1e23, scale: 12, want: "100000000000000000000000.000000000000"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := fToDec128(tc.value, tc.scale)
			require.NoError(t, err)
			require.Equal(t, tc.want, got.Format(tc.scale))
		})
	}
}

func TestVarianceDecimalResults(t *testing.T) {
	for _, param := range []types.Type{types.New(types.T_decimal64, 18, 0), types.New(types.T_decimal128, 38, 0)} {
		t.Run(param.Oid.String(), func(t *testing.T) {
			mp := newAggExecTestPool(t)
			makeInput := func(values ...string) *vector.Vector {
				input := vector.NewVec(param)
				t.Cleanup(func() { input.Free(mp) })
				for _, value := range values {
					if param.Oid == types.T_decimal64 {
						parsed, err := types.ParseDecimal64(value, param.Width, param.Scale)
						require.NoError(t, err)
						require.NoError(t, vector.AppendFixed(input, parsed, false, mp))
					} else {
						parsed, err := types.ParseDecimal128(value, param.Width, param.Scale)
						require.NoError(t, err)
						require.NoError(t, vector.AppendFixed(input, parsed, false, mp))
					}
				}
				return input
			}
			input := makeInput("20240101010203", "20240102020304", "20240103030405")
			duplicateInput := makeInput("20240101010203", "20240101010203", "20240102020304", "20240103030405")
			cases := []struct {
				name string
				id   int64
				want string
			}{
				{"var_pop", AggIdOfVarPop, "680202686800.666600"},
				{"var_samp", AggIdOfVarSample, "1020304030201.000000"},
				{"stddev_pop", AggIdOfStdDevPop, "824744.012892"},
				{"stddev_samp", AggIdOfStdDevSample, "1010101.000000"},
			}
			for _, tc := range cases {
				for _, distinct := range []bool{false, true} {
					mode := "resident"
					if distinct {
						mode = "distinct"
					}
					t.Run(tc.name+"/"+mode, func(t *testing.T) {
						exec := varianceExec(t, mp, tc.id, distinct, param)
						require.NoError(t, exec.GroupGrow(1))
						selected := input
						if distinct {
							selected = duplicateInput
						}
						require.NoError(t, exec.BulkFill(0, []*vector.Vector{selected}))
						result := varianceResult(t, mp, exec, types.New(types.T_decimal128, 38, 6), []bool{false})
						require.Equal(t, tc.want, vector.MustFixedColWithTypeCheck[types.Decimal128](result)[0].Format(6))
					})
				}
			}
			if param.Oid == types.T_decimal128 {
				leftInput := vector.NewVec(param)
				t.Cleanup(func() { leftInput.Free(mp) })
				require.NoError(t, input.CloneWindowTo(leftInput, 0, 2, mp))
				rightInput := vector.NewVec(param)
				t.Cleanup(func() { rightInput.Free(mp) })
				require.NoError(t, input.CloneWindowTo(rightInput, 2, 3, mp))
				t.Run("wire-merge", func(t *testing.T) {
					left := varianceExec(t, mp, AggIdOfVarPop, false, param)
					right := varianceExec(t, mp, AggIdOfVarPop, false, param)
					require.NoError(t, left.GroupGrow(1))
					require.NoError(t, right.GroupGrow(1))
					require.NoError(t, left.BulkFill(0, []*vector.Vector{leftInput}))
					require.NoError(t, right.BulkFill(0, []*vector.Vector{rightInput}))
					var wire bytes.Buffer
					require.NoError(t, left.SaveIntermediateResult(1, [][]byte{{1}}, &wire))
					restored := varianceExec(t, mp, AggIdOfVarPop, false, param)
					require.NoError(t, restored.UnmarshalFromReader(bytes.NewReader(wire.Bytes()), mp))
					require.NoError(t, restored.Merge(right, 0, 0))
					result := varianceResult(t, mp, restored, types.New(types.T_decimal128, 38, 6), []bool{false})
					require.Equal(t, "680202686800.666600", vector.MustFixedColWithTypeCheck[types.Decimal128](result)[0].Format(6))
				})
			}
		})
	}
}

func TestDecimalVarianceResultRejectsInvalidValuesAndBounds(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value float64
		scale int32
	}{
		{name: "nan", value: math.NaN(), scale: 12},
		{name: "positive infinity", value: math.Inf(1), scale: 12},
		{name: "negative infinity", value: math.Inf(-1), scale: 12},
		{name: "negative scale", value: 1, scale: -1},
		{name: "scale above precision", value: 1, scale: 39},
		{name: "positive precision overflow", value: 1, scale: 38},
		{name: "negative precision overflow", value: -1, scale: 38},
		{name: "finite magnitude overflow", value: math.MaxFloat64, scale: 12},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := fToDec128(tc.value, tc.scale)
			require.Error(t, err)
		})
	}
}

func BenchmarkDecimalVarianceResultConversion(b *testing.B) {
	for _, value := range []float64{0.01, 680202686800.6666, 1e-20} {
		b.Run(strconv.FormatFloat(value, 'g', -1, 64), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_, _ = fToDec128(value, 12)
			}
		})
	}
}

func TestMergeVarianceStateLargeOffset(t *testing.T) {
	leftMean, leftVariance, leftExponent, leftCount := 0.0, 0.0, int64(0), int64(0)
	for i := 0; i < 3; i++ {
		var err error
		leftMean, leftVariance, leftExponent, leftCount, err = updateVarianceState(
			leftMean, leftVariance, leftExponent, leftCount, 1_000_000_000_000+float64(i))
		require.NoError(t, err)
	}
	rightMean, rightVariance, rightExponent, rightCount := 0.0, 0.0, int64(0), int64(0)
	for i := 3; i < 7; i++ {
		var err error
		rightMean, rightVariance, rightExponent, rightCount, err = updateVarianceState(
			rightMean, rightVariance, rightExponent, rightCount, 1_000_000_000_000+float64(i))
		require.NoError(t, err)
	}

	mean, variance, varianceExponent, count, err := mergeVarianceState(
		leftMean, leftVariance, leftExponent, leftCount,
		rightMean, rightVariance, rightExponent, rightCount)
	require.NoError(t, err)
	require.Equal(t, int64(7), count)
	require.InEpsilon(t, 1_000_000_000_003.0, mean, 1e-15)
	require.InEpsilon(t, 4.0, variance, 1e-15)
	require.Zero(t, varianceExponent)
}

func TestMergeVarianceStateAvoidsFiniteIntermediateOverflow(t *testing.T) {
	_, variance, varianceExponent, count, err := mergeVarianceState(
		0, 0, 0, 2, 1.5e154, 0, 0, 2)
	require.NoError(t, err)
	require.Equal(t, int64(4), count)
	require.InEpsilon(t, 5.625e307,
		scaledVarianceFloat64(scaledVariance{value: variance, exponent: varianceExponent}), 1e-15)

	mean, residentVariance, residentExponent, residentCount := 0.0, 0.0, int64(0), int64(0)
	for _, value := range []float64{0, 0, 1.5e154, 1.5e154} {
		mean, residentVariance, residentExponent, residentCount, err = updateVarianceState(
			mean, residentVariance, residentExponent, residentCount, value)
		require.NoError(t, err)
	}
	require.Equal(t, int64(4), residentCount)
	require.InEpsilon(t, 5.625e307,
		scaledVarianceFloat64(scaledVariance{value: residentVariance, exponent: residentExponent}), 1e-15)
}

func TestVarianceLargeOffset(t *testing.T) {
	for _, tc := range []struct {
		name   string
		typ    types.Type
		origin int64
	}{
		{"float64", types.T_float64.ToType(), 1_000_000_000_000},
		{"decimal128", types.New(types.T_decimal128, 30, 6), 1_000_000_000_000},
		{"decimal64", types.New(types.T_decimal64, 18, 6), 100_000_000_000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := newAggExecTestPool(t)
			input := vector.NewVec(tc.typ)
			t.Cleanup(func() { input.Free(mp) })
			for i := int64(0); i < 7; i++ {
				literal := strconv.FormatInt(tc.origin+i, 10)
				switch tc.typ.Oid {
				case types.T_float64:
					require.NoError(t, vector.AppendFixed(input, float64(tc.origin+i), false, mp))
				case types.T_decimal64:
					value, err := types.ParseDecimal64(literal, tc.typ.Width, tc.typ.Scale)
					require.NoError(t, err)
					require.NoError(t, vector.AppendFixed(input, value, false, mp))
				case types.T_decimal128:
					value, err := types.ParseDecimal128(literal, tc.typ.Width, tc.typ.Scale)
					require.NoError(t, err)
					require.NoError(t, vector.AppendFixed(input, value, false, mp))
				}
			}
			// Only Decimal128's original bulk fixture included a trailing NULL.
			if tc.typ.Oid == types.T_decimal128 {
				require.NoError(t, vector.AppendFixed(input, types.Decimal128{}, true, mp))
			}
			type offsetCase struct {
				name            string
				id              int64
				distinct, merge bool
				want            float64
			}
			cases := []offsetCase{
				{"var-resident", AggIdOfVarPop, false, false, 4},
				{"var-distinct", AggIdOfVarPop, true, false, 4},
			}
			var leftInput, rightInput *vector.Vector
			if tc.typ.Oid != types.T_decimal64 {
				cases = append(cases, offsetCase{"var-merge", AggIdOfVarPop, false, true, 4})
				leftInput = vector.NewVec(tc.typ)
				t.Cleanup(func() { leftInput.Free(mp) })
				require.NoError(t, input.CloneWindowTo(leftInput, 0, 3, mp))
				rightInput = vector.NewVec(tc.typ)
				t.Cleanup(func() { rightInput.Free(mp) })
				require.NoError(t, input.CloneWindowTo(rightInput, 3, 7, mp))
			}
			if tc.typ.Oid == types.T_float64 {
				cases = append(cases, offsetCase{"stddev-resident", AggIdOfStdDevPop, false, false, 2}, offsetCase{"stddev-distinct", AggIdOfStdDevPop, true, false, 2})
			}
			for _, cell := range cases {
				t.Run(cell.name, func(t *testing.T) {
					exec := varianceExec(t, mp, cell.id, cell.distinct, tc.typ)
					require.NoError(t, exec.GroupGrow(1))
					if cell.merge {
						right := varianceExec(t, mp, cell.id, false, tc.typ)
						require.NoError(t, right.GroupGrow(1))
						require.NoError(t, exec.BulkFill(0, []*vector.Vector{leftInput}))
						require.NoError(t, right.BulkFill(0, []*vector.Vector{rightInput}))
						require.NoError(t, exec.Merge(right, 0, 0))
						// The merged result must survive donor destruction.
						right.Free()
					} else {
						require.NoError(t, exec.BulkFill(0, []*vector.Vector{input}))
					}
					resultType := types.T_float64.ToType()
					if tc.typ.Oid != types.T_float64 {
						resultType = types.New(types.T_decimal128, 38, 12)
					}
					result := varianceResult(t, mp, exec, resultType, []bool{false})
					if tc.typ.Oid == types.T_float64 {
						require.InEpsilon(t, cell.want, vector.MustFixedColWithTypeCheck[float64](result)[0], 1e-15)
					} else {
						require.Equal(t, "4.000000000000", vector.MustFixedColWithTypeCheck[types.Decimal128](result)[0].Format(12))
					}
				})
			}
		})
	}
}

func TestVarPopExecAvoidsFiniteIntermediateOverflow(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	input := vector.NewVec(types.T_float64.ToType())
	defer input.Free(mp)
	for _, value := range []float64{0, 0, 1.5e154, 1.5e154} {
		require.NoError(t, vector.AppendFixed(input, value, false, mp))
	}

	exec := makeVarPopExec(mp, AggIdOfVarPop, false, *input.GetType())
	defer exec.Free()
	require.NoError(t, exec.GroupGrow(1))
	require.NoError(t, exec.BulkFill(0, []*vector.Vector{input}))
	vecs, err := exec.Flush()
	require.NoError(t, err)
	defer vecs[0].Free(mp)
	result := vector.MustFixedColNoTypeCheck[float64](vecs[0])[0]
	require.False(t, math.IsInf(result, 0))
	require.InEpsilon(t, 5.625e307, result, 1e-15)
}

func TestVarPopExecRescalesExistingVarianceBeforeMultiply(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	for _, tc := range []struct {
		name   string
		values []float64
		want   float64
	}{
		{name: "reviewer-zero-two-one", values: []float64{0, 2e154, 1e154}, want: 6.666666666666667e307},
		{name: "reviewer-symmetric", values: []float64{1.3e154, -1.3e154, 0}, want: 1.1266666666666666e308},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := vector.NewVec(types.T_float64.ToType())
			defer input.Free(mp)
			for _, value := range tc.values {
				require.NoError(t, vector.AppendFixed(input, value, false, mp))
			}

			exec := makeVarPopExec(mp, AggIdOfVarPop, false, *input.GetType())
			defer exec.Free()
			require.NoError(t, exec.GroupGrow(1))
			require.NoError(t, exec.BulkFill(0, []*vector.Vector{input}))
			vecs, err := exec.Flush()
			require.NoError(t, err)
			defer vecs[0].Free(mp)
			got := vector.MustFixedColNoTypeCheck[float64](vecs[0])[0]
			require.False(t, math.IsInf(got, 0))
			require.InEpsilon(t, tc.want, got, 1e-15)
		})
	}
}

func TestStdDevPopExecRetainsFiniteResultWhenVarianceOverflows(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	param := types.T_float64.ToType()

	makeInput := func(values ...float64) *vector.Vector {
		input := vector.NewVec(param)
		for _, value := range values {
			require.NoError(t, vector.AppendFixed(input, value, false, mp))
		}
		return input
	}
	check := func(t *testing.T, exec AggFuncExec, want float64) {
		t.Helper()
		vecs, err := exec.Flush()
		require.NoError(t, err)
		defer vecs[0].Free(mp)
		got := vector.MustFixedColNoTypeCheck[float64](vecs[0])[0]
		require.False(t, math.IsInf(got, 0))
		require.InEpsilon(t, want, got, 1e-15)
	}

	for _, distinct := range []bool{false, true} {
		t.Run(map[bool]string{false: "resident", true: "distinct"}[distinct], func(t *testing.T) {
			input := makeInput(1e200, -1e200)
			defer input.Free(mp)
			exec := makeStdDevPopExec(mp, AggIdOfStdDevPop, distinct, param)
			defer exec.Free()
			require.NoError(t, exec.GroupGrow(1))
			require.NoError(t, exec.BulkFill(0, []*vector.Vector{input}))
			check(t, exec, 1e200)
		})
	}

	t.Run("sample", func(t *testing.T) {
		input := makeInput(1e200, -1e200)
		defer input.Free(mp)
		exec := makeStdDevSampleExec(mp, AggIdOfStdDevSample, false, param)
		defer exec.Free()
		require.NoError(t, exec.GroupGrow(1))
		require.NoError(t, exec.BulkFill(0, []*vector.Vector{input}))
		check(t, exec, math.Sqrt2*1e200)
	})

	t.Run("merge", func(t *testing.T) {
		leftInput, rightInput := makeInput(1e200), makeInput(-1e200)
		defer leftInput.Free(mp)
		defer rightInput.Free(mp)
		left := makeStdDevPopExec(mp, AggIdOfStdDevPop, false, param)
		right := makeStdDevPopExec(mp, AggIdOfStdDevPop, false, param)
		defer left.Free()
		defer right.Free()
		require.NoError(t, left.GroupGrow(1))
		require.NoError(t, right.GroupGrow(1))
		require.NoError(t, left.BulkFill(0, []*vector.Vector{leftInput}))
		require.NoError(t, right.BulkFill(0, []*vector.Vector{rightInput}))
		require.NoError(t, left.Merge(right, 0, 0))
		check(t, left, 1e200)
	})

	for _, tc := range []struct {
		name      string
		magnitude float64
	}{
		{name: "variance-underflows", magnitude: 1e-200},
		{name: "difference-overflows", magnitude: math.MaxFloat64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := makeInput(tc.magnitude, -tc.magnitude)
			defer input.Free(mp)
			exec := makeStdDevPopExec(mp, AggIdOfStdDevPop, false, param)
			defer exec.Free()
			require.NoError(t, exec.GroupGrow(1))
			require.NoError(t, exec.BulkFill(0, []*vector.Vector{input}))
			check(t, exec, tc.magnitude)
		})
	}
}

func TestVarianceExactOrigins(t *testing.T) {
	mp := newAggExecTestPool(t)

	maxInt64 := int64(^uint64(0) >> 1)
	maxUint64 := ^uint64(0)
	decimal256Base, err := types.ParseDecimal256("1"+strings.Repeat("0", 60), 65, 0)
	require.NoError(t, err)
	inputs := []struct {
		name   string
		param  types.Type
		append func(*testing.T, *vector.Vector, int, int)
	}{
		{
			name:  "int64",
			param: types.T_int64.ToType(),
			append: func(t *testing.T, input *vector.Vector, from, to int) {
				for i := from; i < to; i++ {
					require.NoError(t, vector.AppendFixed(
						input, maxInt64-3+int64(i), false, mp))
				}
			},
		},
		{
			name:  "int64-min",
			param: types.T_int64.ToType(),
			append: func(t *testing.T, input *vector.Vector, from, to int) {
				minInt64 := -maxInt64 - 1
				for i := from; i < to; i++ {
					require.NoError(t, vector.AppendFixed(
						input, minInt64+int64(i), false, mp))
				}
			},
		},
		{
			name:  "uint64",
			param: types.T_uint64.ToType(),
			append: func(t *testing.T, input *vector.Vector, from, to int) {
				for i := from; i < to; i++ {
					require.NoError(t, vector.AppendFixed(
						input, maxUint64-3+uint64(i), false, mp))
				}
			},
		},
		{
			name:  "bit",
			param: types.T_bit.ToType(),
			append: func(t *testing.T, input *vector.Vector, from, to int) {
				for i := from; i < to; i++ {
					require.NoError(t, vector.AppendFixed(
						input, maxUint64-3+uint64(i), false, mp))
				}
			},
		},
		{
			name:  "decimal256",
			param: types.New(types.T_decimal256, 65, 0),
			append: func(t *testing.T, input *vector.Vector, from, to int) {
				for i := from; i < to; i++ {
					value, _, err := decimal256Base.Add(
						types.Decimal256FromInt64(int64(i)), 0, 0)
					require.NoError(t, err)
					require.NoError(t, vector.AppendFixed(input, value, false, mp))
				}
			},
		},
	}
	aggregates := []struct {
		name string
		id   int64
		want float64
	}{
		{"var-pop", AggIdOfVarPop, 1.25},
		{"var-sample", AggIdOfVarSample, 5.0 / 3.0},
		{"stddev-pop", AggIdOfStdDevPop, math.Sqrt(1.25)},
		{"stddev-sample", AggIdOfStdDevSample, math.Sqrt(5.0 / 3.0)},
	}
	for _, inputCase := range inputs {
		t.Run(inputCase.name, func(t *testing.T) {
			// Native values are built once per type. Executors never own these inputs.
			input := vector.NewVec(inputCase.param)
			t.Cleanup(func() { input.Free(mp) })
			inputCase.append(t, input, 0, 4)
			leftInput := vector.NewVec(inputCase.param)
			t.Cleanup(func() { leftInput.Free(mp) })
			require.NoError(t, input.CloneWindowTo(leftInput, 0, 2, mp))
			rightInput := vector.NewVec(inputCase.param)
			t.Cleanup(func() { rightInput.Free(mp) })
			require.NoError(t, input.CloneWindowTo(rightInput, 2, 4, mp))
			check := func(t *testing.T, exec AggFuncExec, want float64) {
				t.Helper()
				result := varianceResult(t, mp, exec, types.T_float64.ToType(), []bool{false})
				require.InDelta(t, want, vector.MustFixedColWithTypeCheck[float64](result)[0], 1e-14)
			}
			for _, aggregate := range aggregates {
				for _, mode := range []string{"resident", "distinct", "merge"} {
					t.Run(aggregate.name+"/"+mode, func(t *testing.T) {
						exec := varianceExec(t, mp, aggregate.id, mode == "distinct", inputCase.param)
						require.NoError(t, exec.GroupGrow(1))
						if mode == "merge" {
							right := varianceExec(t, mp, aggregate.id, false, inputCase.param)
							require.NoError(t, right.GroupGrow(1))
							require.NoError(t, exec.BulkFill(0, []*vector.Vector{leftInput}))
							require.NoError(t, right.BulkFill(0, []*vector.Vector{rightInput}))
							require.NoError(t, exec.Merge(right, 0, 0))
						} else {
							require.NoError(t, exec.BulkFill(0, []*vector.Vector{input}))
						}
						check(t, exec, aggregate.want)
					})
				}
			}
			// The signed-minimum fixture has no former wire continuation contract.
			if inputCase.name == "int64-min" {
				return
			}
			for _, continuation := range []string{"fill", "merge"} {
				t.Run("wire/"+continuation, func(t *testing.T) {
					source := varianceExec(t, mp, AggIdOfVarPop, false, inputCase.param)
					require.NoError(t, source.GroupGrow(1))
					require.NoError(t, source.BulkFill(0, []*vector.Vector{leftInput}))
					var wire bytes.Buffer
					require.NoError(t, source.SaveIntermediateResult(1, [][]uint8{{1}}, &wire))
					restored := varianceExec(t, mp, AggIdOfVarPop, false, inputCase.param)
					require.NoError(t, restored.UnmarshalFromReader(bytes.NewReader(wire.Bytes()), mp))
					if continuation == "fill" {
						require.NoError(t, restored.BulkFill(0, []*vector.Vector{rightInput}))
					} else {
						right := varianceExec(t, mp, AggIdOfVarPop, false, inputCase.param)
						require.NoError(t, right.GroupGrow(1))
						require.NoError(t, right.BulkFill(0, []*vector.Vector{rightInput}))
						require.NoError(t, restored.Merge(right, 0, 0))
					}
					check(t, restored, 1.25)
				})
			}
		})
	}
}

func TestExactIntegerStdDevAcrossFullRange(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	maxInt64 := int64(^uint64(0) >> 1)
	minInt64 := -maxInt64 - 1
	maxUint64 := ^uint64(0)
	tests := []struct {
		name   string
		param  types.Type
		append func(*testing.T, *vector.Vector)
		want   float64
	}{
		{
			name:  "int64",
			param: types.T_int64.ToType(),
			append: func(t *testing.T, input *vector.Vector) {
				require.NoError(t, vector.AppendFixed(input, minInt64, false, mp))
				require.NoError(t, vector.AppendFixed(input, maxInt64, false, mp))
			},
			want: float64(maxUint64) / 2,
		},
		{
			name:  "uint64",
			param: types.T_uint64.ToType(),
			append: func(t *testing.T, input *vector.Vector) {
				require.NoError(t, vector.AppendFixed(input, uint64(0), false, mp))
				require.NoError(t, vector.AppendFixed(input, maxUint64, false, mp))
			},
			want: float64(maxUint64) / 2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			input := vector.NewVec(tc.param)
			defer input.Free(mp)
			tc.append(t, input)
			exec := makeStdDevPopExec(mp, AggIdOfStdDevPop, false, tc.param)
			defer exec.Free()
			require.NoError(t, exec.GroupGrow(1))
			require.NoError(t, exec.BulkFill(0, []*vector.Vector{input}))
			results, err := exec.Flush()
			require.NoError(t, err)
			defer results[0].Free(mp)
			require.InEpsilon(t, tc.want,
				vector.MustFixedColNoTypeCheck[float64](results[0])[0], 1e-15)
		})
	}
}

func TestExactIntegerVarianceDeviations(t *testing.T) {
	maxInt64 := int64(^uint64(0) >> 1)
	minInt64 := -maxInt64 - 1
	maxUint64 := ^uint64(0)

	got, err := exactVarianceDeviationToFloat64(
		maxInt64, maxInt64-3, types.T_int64, 0)
	require.NoError(t, err)
	require.Equal(t, 3.0, got)

	got, err = exactVarianceDeviationToFloat64(
		minInt64, minInt64+3, types.T_int64, 0)
	require.NoError(t, err)
	require.Equal(t, -3.0, got)

	got, err = exactVarianceDeviationToFloat64(
		maxInt64, minInt64, types.T_int64, 0)
	require.NoError(t, err)
	require.Equal(t, float64(maxUint64), got)

	unsignedGot, err := exactVarianceDeviationToFloat64(
		maxUint64-3, maxUint64, types.T_uint64, 0)
	require.NoError(t, err)
	require.Equal(t, -3.0, unsignedGot)

	_, err = exactVarianceDeviationToFloat64(
		float64(1), float64(0), types.T_float64, 0)
	require.ErrorContains(t, err, "unsupported exact variance type")
}

func BenchmarkUpdateVarianceStateNormalRange(b *testing.B) {
	mean, variance, varianceExponent, count := 0.0, 0.0, int64(0), int64(0)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		mean, variance, varianceExponent, count, _ = updateVarianceState(
			mean, variance, varianceExponent, count, float64(i&1023))
		if count == 1<<20 {
			mean, variance, varianceExponent, count = 0, 0, 0, 0
		}
	}
}

func TestLegacyVarianceStateKeepsPreV35WireLayout(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	param := types.New(types.T_decimal128, 38, 20)

	legacy := makeVarPopExec(mp, AggIdOfVarPop, false, param, true).(*varStdDevExec[types.Decimal128, types.Decimal128])
	defer legacy.Free()
	require.True(t, legacy.legacyState)
	require.Len(t, legacy.aggInfo.stateTypes, 3)

	stable := makeVarPopExec(mp, AggIdOfVarPop, false, param).(*varStdDevExec[types.Decimal128, types.Decimal128])
	defer stable.Free()
	require.False(t, stable.legacyState)
	require.Len(t, stable.aggInfo.stateTypes, 5)

	stableInt := makeVarPopExec(mp, AggIdOfVarPop, false, types.T_int64.ToType()).(*varStdDevExec[float64, int64])
	defer stableInt.Free()
	require.Len(t, stableInt.aggInfo.stateTypes, 5)
	require.Equal(t, types.T_int64, stableInt.aggInfo.stateTypes[4].Oid)

	stableUint := makeVarPopExec(mp, AggIdOfVarPop, false, types.T_uint64.ToType()).(*varStdDevExec[float64, uint64])
	defer stableUint.Free()
	require.Len(t, stableUint.aggInfo.stateTypes, 5)
	require.Equal(t, types.T_uint64, stableUint.aggInfo.stateTypes[4].Oid)

	stableFloat := makeVarPopExec(mp, AggIdOfVarPop, false, types.T_float64.ToType()).(*varStdDevExec[float64, float64])
	defer stableFloat.Free()
	require.Len(t, stableFloat.aggInfo.stateTypes, 4)

	stableInt32 := makeVarPopExec(mp, AggIdOfVarPop, false, types.T_int32.ToType()).(*varStdDevExec[float64, int32])
	defer stableInt32.Free()
	require.Len(t, stableInt32.aggInfo.stateTypes, 4)

	stableBit := makeVarPopExec(mp, AggIdOfVarPop, false, types.T_bit.ToType()).(*varStdDevExec[float64, uint64])
	defer stableBit.Free()
	require.Len(t, stableBit.aggInfo.stateTypes, 5)
	require.Equal(t, types.T_bit, stableBit.aggInfo.stateTypes[4].Oid)

	legacyDecimal256 := makeVarPopExec(mp, AggIdOfVarPop, false,
		types.New(types.T_decimal256, 65, 0), true).(*varStdDevExec[float64, types.Decimal256])
	defer legacyDecimal256.Free()
	require.Len(t, legacyDecimal256.aggInfo.stateTypes, 3)

	stableDecimal256 := makeVarPopExec(mp, AggIdOfVarPop, false,
		types.New(types.T_decimal256, 65, 0)).(*varStdDevExec[float64, types.Decimal256])
	defer stableDecimal256.Free()
	require.Len(t, stableDecimal256.aggInfo.stateTypes, 5)
	require.Equal(t, types.T_decimal256, stableDecimal256.aggInfo.stateTypes[4].Oid)
}

func TestVarianceIntermediateStateWireLayouts(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	tests := []struct {
		name   string
		param  types.Type
		append func(*testing.T, *vector.Vector)
		read   func(*vector.Vector) float64
	}{
		{
			name:  "float64",
			param: types.T_float64.ToType(),
			append: func(t *testing.T, input *vector.Vector) {
				for _, value := range []float64{2, 4, 6, 8} {
					require.NoError(t, vector.AppendFixed(input, value, false, mp))
				}
			},
			read: func(result *vector.Vector) float64 {
				return vector.MustFixedColNoTypeCheck[float64](result)[0]
			},
		},
		{
			name:  "int64",
			param: types.T_int64.ToType(),
			append: func(t *testing.T, input *vector.Vector) {
				for _, value := range []int64{2, 4, 6, 8} {
					require.NoError(t, vector.AppendFixed(input, value, false, mp))
				}
			},
			read: func(result *vector.Vector) float64 {
				return vector.MustFixedColNoTypeCheck[float64](result)[0]
			},
		},
		{
			name:  "uint64",
			param: types.T_uint64.ToType(),
			append: func(t *testing.T, input *vector.Vector) {
				for _, value := range []uint64{2, 4, 6, 8} {
					require.NoError(t, vector.AppendFixed(input, value, false, mp))
				}
			},
			read: func(result *vector.Vector) float64 {
				return vector.MustFixedColNoTypeCheck[float64](result)[0]
			},
		},
		{
			name:  "bit",
			param: types.T_bit.ToType(),
			append: func(t *testing.T, input *vector.Vector) {
				for _, value := range []uint64{2, 4, 6, 8} {
					require.NoError(t, vector.AppendFixed(input, value, false, mp))
				}
			},
			read: func(result *vector.Vector) float64 {
				return vector.MustFixedColNoTypeCheck[float64](result)[0]
			},
		},
		{
			name:  "decimal128",
			param: types.New(types.T_decimal128, 30, 6),
			append: func(t *testing.T, input *vector.Vector) {
				for _, value := range []float64{2, 4, 6, 8} {
					decimal, err := types.Decimal128FromFloat64(value, 30, 6)
					require.NoError(t, err)
					require.NoError(t, vector.AppendFixed(input, decimal, false, mp))
				}
			},
			read: func(result *vector.Vector) float64 {
				value := vector.MustFixedColNoTypeCheck[types.Decimal128](result)[0]
				return types.Decimal128ToFloat64(value, result.GetType().Scale)
			},
		},
		{
			name:  "decimal256",
			param: types.New(types.T_decimal256, 65, 0),
			append: func(t *testing.T, input *vector.Vector) {
				for _, value := range []int64{2, 4, 6, 8} {
					require.NoError(t, vector.AppendFixed(
						input, types.Decimal256FromInt64(value), false, mp))
				}
			},
			read: func(result *vector.Vector) float64 {
				return vector.MustFixedColNoTypeCheck[float64](result)[0]
			},
		},
	}

	for _, tc := range tests {
		for _, legacy := range []bool{true, false} {
			layout := "v35"
			if legacy {
				layout = "legacy"
			}
			t.Run(tc.name+"/"+layout, func(t *testing.T) {
				input := vector.NewVec(tc.param)
				defer input.Free(mp)
				tc.append(t, input)

				source := makeVarPopExec(mp, AggIdOfVarPop, false, tc.param, legacy)
				defer source.Free()
				require.NoError(t, source.GroupGrow(1))
				require.NoError(t, source.BulkFill(0, []*vector.Vector{input}))

				var wire bytes.Buffer
				require.NoError(t, source.SaveIntermediateResult(
					1, [][]uint8{{1}}, &wire))

				restored := makeVarPopExec(mp, AggIdOfVarPop, false, tc.param, legacy)
				defer restored.Free()
				require.NoError(t, restored.UnmarshalFromReader(
					bytes.NewReader(wire.Bytes()), mp))
				results, err := restored.Flush()
				require.NoError(t, err)
				defer results[0].Free(mp)
				require.InEpsilon(t, 5.0, tc.read(results[0]), 1e-12)

				mismatched := makeVarPopExec(mp, AggIdOfVarPop, false, tc.param, !legacy)
				defer mismatched.Free()
				require.Error(t, mismatched.UnmarshalFromReader(
					bytes.NewReader(wire.Bytes()), mp),
					"the protocol gate must prevent unlike state layouts from decoding")
			})
		}
	}
}

func TestVarianceMergeRejectsDifferentWireLayouts(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	param := types.T_float64.ToType()

	stable := makeVarPopExec(mp, AggIdOfVarPop, false, param)
	legacy := makeVarPopExec(mp, AggIdOfVarPop, false, param, true)
	defer stable.Free()
	defer legacy.Free()
	require.NoError(t, stable.GroupGrow(1))
	require.NoError(t, legacy.GroupGrow(1))
	require.ErrorContains(t, stable.Merge(legacy, 0, 0), "different wire layouts")
	require.ErrorContains(t, legacy.Merge(stable, 0, 0), "different wire layouts")
}

func TestScaledStdDevIntermediateStateWireRoundTrip(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	param := types.T_float64.ToType()

	input := vector.NewVec(param)
	defer input.Free(mp)
	for _, value := range []float64{1e200, -1e200} {
		require.NoError(t, vector.AppendFixed(input, value, false, mp))
	}

	source := makeStdDevPopExec(mp, AggIdOfStdDevPop, false, param)
	defer source.Free()
	require.NoError(t, source.GroupGrow(1))
	require.NoError(t, source.BulkFill(0, []*vector.Vector{input}))

	state := source.(*varStdDevExec[float64, float64])
	exponent := vector.MustFixedColNoTypeCheck[int64](state.state[0].vecs[3])[0]
	require.NotZero(t, exponent, "the test must exercise the v35 exponent sidecar")

	var wire bytes.Buffer
	require.NoError(t, source.SaveIntermediateResult(1, [][]uint8{{1}}, &wire))
	restored := makeStdDevPopExec(mp, AggIdOfStdDevPop, false, param)
	defer restored.Free()
	require.NoError(t, restored.UnmarshalFromReader(bytes.NewReader(wire.Bytes()), mp))

	results, err := restored.Flush()
	require.NoError(t, err)
	defer results[0].Free(mp)
	got := vector.MustFixedColNoTypeCheck[float64](results[0])[0]
	require.False(t, math.IsInf(got, 0))
	require.InEpsilon(t, 1e200, got, 1e-15)
}

func TestLegacyVarianceExecFillMergeAndFlush(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	param := types.T_float64.ToType()

	makeInput := func(values ...float64) *vector.Vector {
		input := vector.NewVec(param)
		for _, value := range values {
			require.NoError(t, vector.AppendFixed(input, value, false, mp))
		}
		return input
	}
	flushFloat := func(t *testing.T, exec AggFuncExec) *vector.Vector {
		t.Helper()
		vecs, err := exec.Flush()
		require.NoError(t, err)
		require.Len(t, vecs, 1)
		return vecs[0]
	}

	t.Run("merge-population", func(t *testing.T) {
		leftInput := makeInput(2, 4)
		rightInput := makeInput(6, 8)
		defer leftInput.Free(mp)
		defer rightInput.Free(mp)

		left := makeVarPopExec(mp, AggIdOfVarPop, false, param, true)
		right := makeVarPopExec(mp, AggIdOfVarPop, false, param, true)
		defer left.Free()
		defer right.Free()
		require.NoError(t, left.GroupGrow(1))
		require.NoError(t, right.GroupGrow(1))
		require.NoError(t, left.BulkFill(0, []*vector.Vector{leftInput}))
		require.NoError(t, right.BatchFill(0, []uint64{1, 1}, []*vector.Vector{rightInput}))
		require.NoError(t, left.Merge(right, 0, 0))

		result := flushFloat(t, left)
		defer result.Free(mp)
		require.InEpsilon(t, 5.0, vector.MustFixedColNoTypeCheck[float64](result)[0], 1e-15)
	})

	t.Run("distinct-sample", func(t *testing.T) {
		input := makeInput(2, 2, 4)
		defer input.Free(mp)
		exec := makeVarSampleExec(mp, AggIdOfVarSample, true, param, true)
		defer exec.Free()
		require.NoError(t, exec.GroupGrow(1))
		require.NoError(t, exec.BulkFill(0, []*vector.Vector{input}))

		result := flushFloat(t, exec)
		defer result.Free(mp)
		require.InEpsilon(t, 2.0, vector.MustFixedColNoTypeCheck[float64](result)[0], 1e-15)
	})

	t.Run("stddev-sample", func(t *testing.T) {
		input := makeInput(2, 4)
		defer input.Free(mp)
		exec := makeStdDevSampleExec(mp, AggIdOfStdDevSample, false, param, true)
		defer exec.Free()
		require.NoError(t, exec.GroupGrow(1))
		require.NoError(t, exec.BulkFill(0, []*vector.Vector{input}))

		result := flushFloat(t, exec)
		defer result.Free(mp)
		require.InEpsilon(t, math.Sqrt(2), vector.MustFixedColNoTypeCheck[float64](result)[0], 1e-15)
	})

	t.Run("empty-and-singleton", func(t *testing.T) {
		input := makeInput(7)
		defer input.Free(mp)
		exec := makeVarPopExec(mp, AggIdOfVarPop, false, param, true)
		defer exec.Free()
		require.NoError(t, exec.GroupGrow(2))
		require.NoError(t, exec.Fill(1, 0, []*vector.Vector{input}))

		result := flushFloat(t, exec)
		defer result.Free(mp)
		require.True(t, result.IsNull(0))
		require.Equal(t, 0.0, vector.MustFixedColNoTypeCheck[float64](result)[1])
	})
}

func TestDecimalDeviationToFloat64Branches(t *testing.T) {
	value64, err := types.Decimal64FromFloat64(12.5, 18, 2)
	require.NoError(t, err)
	origin64, err := types.Decimal64FromFloat64(10.0, 18, 2)
	require.NoError(t, err)
	delta64, err := decimalDeviationToFloat64(value64, origin64, types.T_decimal64, 2)
	require.NoError(t, err)
	require.InEpsilon(t, 2.5, delta64, 1e-15)

	positive, err := types.ParseDecimal128("900000000000000000.00000000000000000000", 38, 20)
	require.NoError(t, err)
	negative, err := types.ParseDecimal128("-900000000000000000.00000000000000000000", 38, 20)
	require.NoError(t, err)
	delta128, err := decimalDeviationToFloat64(positive, negative, types.T_decimal128, 20)
	require.NoError(t, err)
	require.InEpsilon(t, 1.8e18, delta128, 1e-15)

	base, err := types.ParseDecimal256("1"+strings.Repeat("0", 60), 65, 0)
	require.NoError(t, err)
	value256, _, err := base.Add(types.Decimal256FromInt64(2), 0, 0)
	require.NoError(t, err)
	delta256, err := decimalDeviationToFloat64(value256, base, types.T_decimal256, 0)
	require.NoError(t, err)
	require.Equal(t, 2.0, delta256)

	_, err = decimalDeviationToFloat64(int64(1), int64(0), types.T_int64, 0)
	require.Error(t, err)
}

func TestVarianceWideDecimalResults(t *testing.T) {
	mp := newAggExecTestPool(t)
	param := types.New(types.T_decimal128, 38, 20)
	positive, err := types.ParseDecimal128("900000000000000000.00000000000000000000", 38, 20)
	require.NoError(t, err)
	negative, err := types.ParseDecimal128("-900000000000000000.00000000000000000000", 38, 20)
	require.NoError(t, err)
	input := varianceInput(t, mp, param, []types.Decimal128{positive, negative}, nil)
	leftInput := varianceInput(t, mp, param, []types.Decimal128{positive}, nil)
	rightInput := varianceInput(t, mp, param, []types.Decimal128{negative}, nil)
	// Preserve the existing 1e-15 relative bound (900 at magnitude9e17)
	// without converting the materialized decimal back to float64.
	lower, err := types.ParseDecimal128("899999999999999100.00000000000000000000", 38, 20)
	require.NoError(t, err)
	upper, err := types.ParseDecimal128("900000000000000900.00000000000000000000", 38, 20)
	require.NoError(t, err)
	for _, mode := range []string{"resident", "distinct", "merge"} {
		t.Run("stddev/"+mode, func(t *testing.T) {
			exec := varianceExec(t, mp, AggIdOfStdDevPop, mode == "distinct", param)
			require.NoError(t, exec.GroupGrow(1))
			if mode == "merge" {
				right := varianceExec(t, mp, AggIdOfStdDevPop, false, param)
				require.NoError(t, right.GroupGrow(1))
				require.NoError(t, exec.BulkFill(0, []*vector.Vector{leftInput}))
				require.NoError(t, right.BulkFill(0, []*vector.Vector{rightInput}))
				require.NoError(t, exec.Merge(right, 0, 0))
			} else {
				require.NoError(t, exec.BulkFill(0, []*vector.Vector{input}))
			}
			result := varianceResult(t, mp, exec, types.New(types.T_decimal128, 38, 20), []bool{false})
			got := vector.MustFixedColWithTypeCheck[types.Decimal128](result)[0]
			require.GreaterOrEqual(t, got.Compare(lower), 0)
			require.LessOrEqual(t, got.Compare(upper), 0)
		})
	}
	// The standard deviation fits, but variance 8.1e35 exceeds the return
	// precision. Empty and singleton rows are constructed before that error.
	for _, mode := range []string{"resident", "distinct", "legacy"} {
		t.Run("variance-overflow/"+mode, func(t *testing.T) {
			var exec AggFuncExec
			if mode == "legacy" {
				exec = makeVarPopExec(mp, AggIdOfVarPop, false, param, true)
				t.Cleanup(exec.Free)
			} else {
				exec = varianceExec(t, mp, AggIdOfVarPop, mode == "distinct", param)
			}
			require.NoError(t, exec.GroupGrow(3))
			require.NoError(t, exec.BulkFill(1, []*vector.Vector{leftInput}))
			require.NoError(t, exec.BulkFill(2, []*vector.Vector{input}))
			beforeNB := mp.CurrNB()
			beforeBytes, beforeObjects := mp.OnHeapOutstanding()
			results, err := exec.Flush()
			for _, result := range results {
				if result != nil {
					t.Cleanup(func() { result.Free(mp) })
				}
			}
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), "error: %v", err)
			require.Nil(t, results)
			// Check while the executor is still alive: its cleanup cannot mask a leak.
			require.Equal(t, beforeNB, mp.CurrNB())
			afterBytes, afterObjects := mp.OnHeapOutstanding()
			require.Equal(t, beforeBytes, afterBytes)
			require.Equal(t, beforeObjects, afterObjects)
		})
	}
}

func TestVarianceCardinality(t *testing.T) {
	t.Run("decimal256", func(t *testing.T) {
		mp := newAggExecTestPool(t)
		typ := types.New(types.T_decimal256, 65, 0)
		value, err := types.ParseDecimal256("1"+strings.Repeat("0", 60), 65, 0)
		require.NoError(t, err)
		input := varianceInput(t, mp, typ, []types.Decimal256{value, value, value}, []bool{false, false, true})
		singleton := varianceInput(t, mp, typ, []types.Decimal256{value, value}, []bool{false, true})
		cases := []struct {
			name string
			id   int64
			pop  bool
		}{
			{"var_pop", AggIdOfVarPop, true}, {"var_sample", AggIdOfVarSample, false},
			{"stddev_pop", AggIdOfStdDevPop, true}, {"stddev_sample", AggIdOfStdDevSample, false},
		}
		for _, tc := range cases {
			for _, distinct := range []bool{false, true} {
				mode := "resident"
				if distinct {
					mode = "distinct"
				}
				t.Run(tc.name+"/"+mode, func(t *testing.T) {
					exec := varianceExec(t, mp, tc.id, distinct, typ)
					require.NoError(t, exec.GroupGrow(4))
					// Groups: empty, NULL-only, singleton with ignored NULL, duplicate pair.
					require.NoError(t, exec.Fill(1, 2, []*vector.Vector{input}))
					require.NoError(t, exec.BulkFill(2, []*vector.Vector{singleton}))
					require.NoError(t, exec.BatchFill(0, []uint64{4, 4}, []*vector.Vector{input}))
					wantNull := []bool{true, true, !tc.pop, !tc.pop && distinct}
					result := varianceResult(t, mp, exec, types.T_float64.ToType(), wantNull)
					values := vector.MustFixedColWithTypeCheck[float64](result)
					for row, isNull := range wantNull {
						if !isNull {
							require.Equal(t, 0.0, values[row], "row %d", row)
						}
					}
				})
			}
		}
	})
	t.Run("int32-fill", func(t *testing.T) {
		mp := newAggExecTestPool(t)
		typ := types.T_int32.ToType()
		input := varianceInput(t, mp, typ, []int32{4}, nil)
		for _, tc := range []struct {
			name             string
			id               int64
			distinct, isNull bool
		}{
			{"sample-resident", AggIdOfVarSample, false, true},
			{"sample-distinct", AggIdOfVarSample, true, true},
			{"population", AggIdOfVarPop, false, false},
		} {
			t.Run(tc.name, func(t *testing.T) {
				exec := varianceExec(t, mp, tc.id, tc.distinct, typ)
				require.NoError(t, exec.GroupGrow(1))
				require.NoError(t, exec.Fill(0, 0, []*vector.Vector{input}))
				result := varianceResult(t, mp, exec, types.T_float64.ToType(), []bool{tc.isNull})
				if !tc.isNull {
					require.Equal(t, 0.0, vector.MustFixedColWithTypeCheck[float64](result)[0])
				}
			})
		}
	})
	t.Run("bigint-batch", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			typ  types.Type
			id   int64
		}{
			{"var_pop_int64", types.T_int64.ToTypeWithScale(-1), AggIdOfVarPop},
			{"stddev_pop_uint64", types.T_uint64.ToTypeWithScale(-1), AggIdOfStdDevPop},
		} {
			t.Run(tc.name, func(t *testing.T) {
				mp := newAggExecTestPool(t)
				var input *vector.Vector
				if tc.typ.Oid == types.T_int64 {
					input = varianceInput(t, mp, tc.typ, []int64{99, 1, 1, 7, 3, 5, 99}, []bool{false, false, false, false, false, false, true})
				} else {
					input = varianceInput(t, mp, tc.typ, []uint64{99, 1, 1, 7, 3, 5, 99}, []bool{false, false, false, false, false, false, true})
				}
				exec := varianceExec(t, mp, tc.id, false, tc.typ)
				require.NoError(t, exec.GroupGrow(2))
				// The prefix and group 0 sentinel are filtered; NULL 99 must not count.
				require.NoError(t, exec.BatchFill(1, []uint64{1, 1, 0, 2, 2, 2}, []*vector.Vector{input}))
				result := varianceResult(t, mp, exec, types.T_float64.ToType(), []bool{false, false})
				require.Equal(t, []float64{0, 1}, vector.MustFixedColWithTypeCheck[float64](result))
			})
		}
	})
}

func TestVarianceDecimal256Conversion(t *testing.T) {
	mp := newAggExecTestPool(t)
	param := types.New(types.T_decimal256, 65, 0)
	zero := types.Decimal256{}
	a := types.Decimal256{B64_127: 1 << 63}
	b := types.Decimal256{B64_127: 3 << 62}
	input := varianceInput(t, mp, param, []types.Decimal256{zero, a, b}, nil)
	leftInput := varianceInput(t, mp, param, []types.Decimal256{zero, a}, nil)
	rightInput := varianceInput(t, mp, param, []types.Decimal256{b}, nil)
	floatA := math.Ldexp(1, 127)
	wantVarPop := 7.0 / 18.0 * floatA * floatA
	cases := []struct {
		name string
		id   int64
		want float64
	}{
		{"var_pop", AggIdOfVarPop, wantVarPop},
		{"var_sample", AggIdOfVarSample, wantVarPop * 3 / 2},
		{"stddev_pop", AggIdOfStdDevPop, math.Sqrt(wantVarPop)},
		{"stddev_sample", AggIdOfStdDevSample, math.Sqrt(wantVarPop * 3 / 2)},
	}
	for _, tc := range cases {
		for _, mode := range []string{"resident", "distinct", "merge"} {
			t.Run(tc.name+"/"+mode, func(t *testing.T) {
				exec := varianceExec(t, mp, tc.id, mode == "distinct", param)
				require.NoError(t, exec.GroupGrow(1))
				if mode == "merge" {
					right := varianceExec(t, mp, tc.id, false, param)
					require.NoError(t, right.GroupGrow(1))
					require.NoError(t, exec.BulkFill(0, []*vector.Vector{leftInput}))
					require.NoError(t, right.BulkFill(0, []*vector.Vector{rightInput}))
					require.NoError(t, exec.Merge(right, 0, 0))
				} else {
					require.NoError(t, exec.BulkFill(0, []*vector.Vector{input}))
				}
				result := varianceResult(t, mp, exec, types.T_float64.ToType(), []bool{false})
				require.InEpsilon(t, tc.want, vector.MustFixedColWithTypeCheck[float64](result)[0], 1e-14)
			})
		}
	}
}
