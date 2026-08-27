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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

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

func TestVarStdDevExecLargeOffset(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	input := vector.NewVec(types.T_float64.ToType())
	for i := 0; i < 7; i++ {
		require.NoError(t, vector.AppendFixed(input, 1_000_000_000_000+float64(i), false, mp))
	}
	defer input.Free(mp)

	tests := []struct {
		name string
		make func(bool) AggFuncExec
		want float64
	}{
		{
			name: "var_pop",
			make: func(distinct bool) AggFuncExec {
				return makeVarPopExec(mp, AggIdOfVarPop, distinct, *input.GetType())
			},
			want: 4,
		},
		{
			name: "stddev_pop",
			make: func(distinct bool) AggFuncExec {
				return makeStdDevPopExec(mp, AggIdOfStdDevPop, distinct, *input.GetType())
			},
			want: 2,
		},
	}
	for _, tc := range tests {
		for _, distinct := range []bool{false, true} {
			t.Run(tc.name, func(t *testing.T) {
				exec := tc.make(distinct)
				require.NoError(t, exec.GroupGrow(1))
				require.NoError(t, exec.BulkFill(0, []*vector.Vector{input}))
				vecs, err := exec.Flush()
				require.NoError(t, err)
				require.InEpsilon(t, tc.want, vector.MustFixedColNoTypeCheck[float64](vecs[0])[0], 1e-15)
				vecs[0].Free(mp)
				exec.Free()
			})
		}
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

func TestExactIntegerVarianceAtTypeLimits(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	maxInt64 := int64(^uint64(0) >> 1)
	maxUint64 := ^uint64(0)
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
	}
	aggregates := []struct {
		name string
		make func(bool, types.Type) AggFuncExec
		want float64
	}{
		{
			name: "var-pop",
			make: func(distinct bool, param types.Type) AggFuncExec {
				return makeVarPopExec(mp, AggIdOfVarPop, distinct, param)
			},
			want: 1.25,
		},
		{
			name: "var-sample",
			make: func(distinct bool, param types.Type) AggFuncExec {
				return makeVarSampleExec(mp, AggIdOfVarSample, distinct, param)
			},
			want: 5.0 / 3.0,
		},
		{
			name: "stddev-pop",
			make: func(distinct bool, param types.Type) AggFuncExec {
				return makeStdDevPopExec(mp, AggIdOfStdDevPop, distinct, param)
			},
			want: math.Sqrt(1.25),
		},
		{
			name: "stddev-sample",
			make: func(distinct bool, param types.Type) AggFuncExec {
				return makeStdDevSampleExec(mp, AggIdOfStdDevSample, distinct, param)
			},
			want: math.Sqrt(5.0 / 3.0),
		},
	}

	flush := func(t *testing.T, exec AggFuncExec, want float64) {
		t.Helper()
		results, err := exec.Flush()
		require.NoError(t, err)
		defer results[0].Free(mp)
		require.InDelta(t, want,
			vector.MustFixedColNoTypeCheck[float64](results[0])[0], 1e-14)
	}

	for _, inputCase := range inputs {
		for _, aggregate := range aggregates {
			for _, mode := range []string{"resident", "distinct", "merge"} {
				t.Run(inputCase.name+"/"+aggregate.name+"/"+mode, func(t *testing.T) {
					if mode == "merge" {
						leftInput := vector.NewVec(inputCase.param)
						rightInput := vector.NewVec(inputCase.param)
						defer leftInput.Free(mp)
						defer rightInput.Free(mp)
						inputCase.append(t, leftInput, 0, 2)
						inputCase.append(t, rightInput, 2, 4)

						left := aggregate.make(false, inputCase.param)
						right := aggregate.make(false, inputCase.param)
						defer left.Free()
						defer right.Free()
						require.NoError(t, left.GroupGrow(1))
						require.NoError(t, right.GroupGrow(1))
						require.NoError(t, left.BulkFill(0, []*vector.Vector{leftInput}))
						require.NoError(t, right.BulkFill(0, []*vector.Vector{rightInput}))
						require.NoError(t, left.Merge(right, 0, 0))
						flush(t, left, aggregate.want)
						return
					}

					input := vector.NewVec(inputCase.param)
					defer input.Free(mp)
					inputCase.append(t, input, 0, 4)
					exec := aggregate.make(mode == "distinct", inputCase.param)
					defer exec.Free()
					require.NoError(t, exec.GroupGrow(1))
					require.NoError(t, exec.BulkFill(0, []*vector.Vector{input}))
					flush(t, exec, aggregate.want)
				})
			}
		}
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

func TestLegacyVarianceStateKeepsPreV32WireLayout(t *testing.T) {
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
	}

	for _, tc := range tests {
		for _, legacy := range []bool{true, false} {
			layout := "v32"
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

func TestExactIntegerVarianceOriginWireRoundTrip(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	maxInt64 := int64(^uint64(0) >> 1)
	maxUint64 := ^uint64(0)
	tests := []struct {
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
	}

	for _, tc := range tests {
		for _, continuation := range []string{"fill", "merge"} {
			t.Run(tc.name+"/"+continuation, func(t *testing.T) {
				leftInput := vector.NewVec(tc.param)
				defer leftInput.Free(mp)
				tc.append(t, leftInput, 0, 2)

				source := makeVarPopExec(mp, AggIdOfVarPop, false, tc.param)
				defer source.Free()
				require.NoError(t, source.GroupGrow(1))
				require.NoError(t, source.BulkFill(0, []*vector.Vector{leftInput}))

				var wire bytes.Buffer
				require.NoError(t, source.SaveIntermediateResult(
					1, [][]uint8{{1}}, &wire))
				restored := makeVarPopExec(mp, AggIdOfVarPop, false, tc.param)
				defer restored.Free()
				require.NoError(t, restored.UnmarshalFromReader(
					bytes.NewReader(wire.Bytes()), mp))

				rightInput := vector.NewVec(tc.param)
				defer rightInput.Free(mp)
				tc.append(t, rightInput, 2, 4)
				if continuation == "fill" {
					require.NoError(t, restored.BulkFill(
						0, []*vector.Vector{rightInput}))
				} else {
					right := makeVarPopExec(mp, AggIdOfVarPop, false, tc.param)
					defer right.Free()
					require.NoError(t, right.GroupGrow(1))
					require.NoError(t, right.BulkFill(
						0, []*vector.Vector{rightInput}))
					require.NoError(t, restored.Merge(right, 0, 0))
				}

				results, err := restored.Flush()
				require.NoError(t, err)
				defer results[0].Free(mp)
				require.InDelta(t, 1.25,
					vector.MustFixedColNoTypeCheck[float64](results[0])[0], 1e-14)
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
	require.NotZero(t, exponent, "the test must exercise the v32 exponent sidecar")

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

	_, err = decimalDeviationToFloat64(int64(1), int64(0), types.T_int64, 0)
	require.Error(t, err)
}

func TestVarPopExecMergeLargeOffset(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	makeInput := func(from, to int) *vector.Vector {
		v := vector.NewVec(types.T_float64.ToType())
		for i := from; i < to; i++ {
			require.NoError(t, vector.AppendFixed(v, 1_000_000_000_000+float64(i), false, mp))
		}
		return v
	}
	leftInput, rightInput := makeInput(0, 3), makeInput(3, 7)
	defer leftInput.Free(mp)
	defer rightInput.Free(mp)

	left := makeVarPopExec(mp, AggIdOfVarPop, false, *leftInput.GetType())
	right := makeVarPopExec(mp, AggIdOfVarPop, false, *rightInput.GetType())
	require.NoError(t, left.GroupGrow(1))
	require.NoError(t, right.GroupGrow(1))
	require.NoError(t, left.BulkFill(0, []*vector.Vector{leftInput}))
	require.NoError(t, right.BulkFill(0, []*vector.Vector{rightInput}))
	require.NoError(t, left.Merge(right, 0, 0))
	right.Free()
	vecs, err := left.Flush()
	require.NoError(t, err)
	require.InEpsilon(t, 4.0, vector.MustFixedColNoTypeCheck[float64](vecs[0])[0], 1e-15)
	vecs[0].Free(mp)
	left.Free()
}

func TestVarPopDecimalLargeOffsetWithNull(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	param := types.New(types.T_decimal128, 30, 6)
	input := vector.NewVec(param)
	defer input.Free(mp)
	for i := 0; i < 7; i++ {
		value, err := types.Decimal128FromFloat64(1_000_000_000_000+float64(i), 30, 6)
		require.NoError(t, err)
		require.NoError(t, vector.AppendFixed(input, value, false, mp))
	}
	zero, err := types.Decimal128FromFloat64(0, 30, 6)
	require.NoError(t, err)
	require.NoError(t, vector.AppendFixed(input, zero, true, mp))

	exec := makeVarPopExec(mp, AggIdOfVarPop, false, param)
	defer exec.Free()
	require.NoError(t, exec.GroupGrow(1))
	require.NoError(t, exec.BulkFill(0, []*vector.Vector{input}))
	vecs, err := exec.Flush()
	require.NoError(t, err)
	defer vecs[0].Free(mp)
	got := vector.MustFixedColNoTypeCheck[types.Decimal128](vecs[0])[0]
	// The DECIMAL result is materialized through a float64 aggregate state; this
	// bound is far below the input scale while ensuring we catch the previous
	// cancellation-to-zero failure.
	require.InEpsilon(t, 4.0, types.Decimal128ToFloat64(got, vecs[0].GetType().Scale), 1e-8)
}

func TestVarPopDecimalMergeLargeOffset(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	param := types.New(types.T_decimal128, 30, 6)
	makeInput := func(from, to int) *vector.Vector {
		v := vector.NewVec(param)
		for i := from; i < to; i++ {
			value, err := types.Decimal128FromFloat64(1_000_000_000_000+float64(i), 30, 6)
			require.NoError(t, err)
			require.NoError(t, vector.AppendFixed(v, value, false, mp))
		}
		return v
	}
	leftInput, rightInput := makeInput(0, 3), makeInput(3, 7)
	defer leftInput.Free(mp)
	defer rightInput.Free(mp)

	left := makeVarPopExec(mp, AggIdOfVarPop, false, param)
	right := makeVarPopExec(mp, AggIdOfVarPop, false, param)
	defer left.Free()
	defer right.Free()
	require.NoError(t, left.GroupGrow(1))
	require.NoError(t, right.GroupGrow(1))
	require.NoError(t, left.BulkFill(0, []*vector.Vector{leftInput}))
	require.NoError(t, right.BulkFill(0, []*vector.Vector{rightInput}))
	require.NoError(t, left.Merge(right, 0, 0))
	vecs, err := left.Flush()
	require.NoError(t, err)
	defer vecs[0].Free(mp)
	got := vector.MustFixedColNoTypeCheck[types.Decimal128](vecs[0])[0]
	require.InEpsilon(t, 4.0, types.Decimal128ToFloat64(got, vecs[0].GetType().Scale), 1e-8)
}

func TestStdDevPopDecimal128WideDeviation(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	param := types.New(types.T_decimal128, 38, 20)
	positive, err := types.ParseDecimal128("900000000000000000.00000000000000000000", 38, 20)
	require.NoError(t, err)
	negative, err := types.ParseDecimal128("-900000000000000000.00000000000000000000", 38, 20)
	require.NoError(t, err)

	makeInput := func(values ...types.Decimal128) *vector.Vector {
		input := vector.NewVec(param)
		for _, value := range values {
			require.NoError(t, vector.AppendFixed(input, value, false, mp))
		}
		return input
	}
	checkResult := func(t *testing.T, exec AggFuncExec) {
		t.Helper()
		vecs, err := exec.Flush()
		require.NoError(t, err)
		defer vecs[0].Free(mp)
		got := vector.MustFixedColNoTypeCheck[types.Decimal128](vecs[0])[0]
		require.InEpsilon(t, 9e17, types.Decimal128ToFloat64(got, vecs[0].GetType().Scale), 1e-15)
	}

	t.Run("bulk", func(t *testing.T) {
		input := makeInput(positive, negative)
		defer input.Free(mp)
		exec := makeStdDevPopExec(mp, AggIdOfStdDevPop, false, param)
		defer exec.Free()
		require.NoError(t, exec.GroupGrow(1))
		require.NoError(t, exec.BulkFill(0, []*vector.Vector{input}))
		checkResult(t, exec)
	})
	t.Run("distinct", func(t *testing.T) {
		input := makeInput(positive, negative)
		defer input.Free(mp)
		exec := makeStdDevPopExec(mp, AggIdOfStdDevPop, true, param)
		defer exec.Free()
		require.NoError(t, exec.GroupGrow(1))
		require.NoError(t, exec.BulkFill(0, []*vector.Vector{input}))
		checkResult(t, exec)
	})
	t.Run("merge", func(t *testing.T) {
		leftInput, rightInput := makeInput(positive), makeInput(negative)
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
		checkResult(t, left)
	})
}

func TestVarPopDecimal64LargeOffset(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	param := types.New(types.T_decimal64, 18, 6)
	input := vector.NewVec(param)
	defer input.Free(mp)
	for i := 0; i < 7; i++ {
		value, err := types.Decimal64FromFloat64(100_000_000_000+float64(i), 18, 6)
		require.NoError(t, err)
		require.NoError(t, vector.AppendFixed(input, value, false, mp))
	}

	for _, distinct := range []bool{false, true} {
		exec := makeVarPopExec(mp, AggIdOfVarPop, distinct, param)
		require.NoError(t, exec.GroupGrow(1))
		require.NoError(t, exec.BulkFill(0, []*vector.Vector{input}))
		vecs, err := exec.Flush()
		require.NoError(t, err)
		got := vector.MustFixedColNoTypeCheck[types.Decimal128](vecs[0])[0]
		require.InEpsilon(t, 4.0, types.Decimal128ToFloat64(got, vecs[0].GetType().Scale), 1e-8)
		vecs[0].Free(mp)
		exec.Free()
	}
}

func TestVarStdDevBigIntReturnsFloat64(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	cases := []struct {
		name string
		typ  types.Type
		make func(types.Type) AggFuncExec
	}{
		{
			name: "var_pop_int64",
			typ:  types.T_int64.ToTypeWithScale(-1),
			make: func(typ types.Type) AggFuncExec {
				return makeVarPopExec(mp, 0, false, typ)
			},
		},
		{
			name: "stddev_pop_uint64",
			typ:  types.T_uint64.ToTypeWithScale(-1),
			make: func(typ types.Type) AggFuncExec {
				return makeStdDevPopExec(mp, 0, false, typ)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			curNB := mp.CurrNB()
			exec := tc.make(tc.typ)
			vec := vector.NewVec(tc.typ)
			require.NoError(t, exec.GroupGrow(1))
			switch tc.typ.Oid {
			case types.T_int64:
				require.NoError(t, vector.AppendFixed(vec, int64(1), false, mp))
				require.NoError(t, vector.AppendFixed(vec, int64(1), false, mp))
			case types.T_uint64:
				require.NoError(t, vector.AppendFixed(vec, uint64(1), false, mp))
				require.NoError(t, vector.AppendFixed(vec, uint64(1), false, mp))
			}
			require.NoError(t, exec.BatchFill(0, []uint64{1, 1}, []*vector.Vector{vec}))

			vecs, err := exec.Flush()
			require.NoError(t, err)
			require.Len(t, vecs, 1)
			require.Equal(t, types.T_float64, vecs[0].GetType().Oid)
			require.Equal(t, 0.0, vector.MustFixedColNoTypeCheck[float64](vecs[0])[0])

			for _, vec := range vecs {
				vec.Free(mp)
			}
			vec.Free(mp)
			exec.Free()
			require.Equal(t, curNB, mp.CurrNB())
		})
	}
}

func TestVarSampleSingleNonNullValueReturnsNull(t *testing.T) {
	tests := []struct {
		name       string
		isDistinct bool
	}{
		{
			name: "non-distinct",
		},
		{
			name:       "distinct",
			isDistinct: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			param := types.T_int32.ToType()
			exec := makeVarSampleExec(mp, 0, tc.isDistinct, param)
			require.NoError(t, exec.GroupGrow(1))

			v := vector.NewVec(param)
			require.NoError(t, vector.AppendFixed(v, int32(4), false, mp))
			require.NoError(t, exec.Fill(0, 0, []*vector.Vector{v}))
			v.Free(mp)

			vecs, err := exec.Flush()
			require.NoError(t, err)
			require.Len(t, vecs, 1)
			require.True(t, vecs[0].IsNull(0))

			for _, vec := range vecs {
				vec.Free(mp)
			}
			exec.Free()
		})
	}
}

func TestNumericToFloat64ViaVarExec(t *testing.T) {
	mp := mpool.MustNewZero()

	param := types.T_int32.ToType()
	exec := makeVarStdDevExec(mp, true, true, 0, false, param)
	require.NoError(t, exec.GroupGrow(1))

	v := vector.NewVec(param)
	require.NoError(t, vector.AppendFixed(v, int32(4), false, mp))
	require.NoError(t, exec.Fill(0, 0, []*vector.Vector{v}))
	v.Free(mp)

	vecs, err := exec.Flush()
	require.NoError(t, err)
	for _, vec := range vecs {
		vec.Free(mp)
	}
	exec.Free()
}
