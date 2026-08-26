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
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestVarianceStateLargeOffset(t *testing.T) {
	mean, variance, count := 0.0, 0.0, int64(0)
	for i := 0; i < 7; i++ {
		var err error
		mean, variance, count, err = updateVarianceState(mean, variance, count, 1_000_000_000_000+float64(i))
		require.NoError(t, err)
	}

	require.Equal(t, int64(7), count)
	require.InEpsilon(t, 1_000_000_000_003.0, mean, 1e-15)
	require.InEpsilon(t, 4.0, variance, 1e-15)

	varPop := &varStdDevExec[float64, float64]{isVar: true, isPop: true, f2t: float64ToResult}
	stddevPop := &varStdDevExec[float64, float64]{isVar: false, isPop: true, f2t: float64ToResult}
	result, err := varPop.getResult(variance, count)
	require.NoError(t, err)
	stddev, err := stddevPop.getResult(variance, count)
	require.NoError(t, err)
	require.InEpsilon(t, 4.0, result, 1e-15)
	require.InEpsilon(t, 2.0, stddev, 1e-15)
}

func TestMergeVarianceStateLargeOffset(t *testing.T) {
	leftMean, leftVariance, leftCount := 0.0, 0.0, int64(0)
	for i := 0; i < 3; i++ {
		var err error
		leftMean, leftVariance, leftCount, err = updateVarianceState(leftMean, leftVariance, leftCount, 1_000_000_000_000+float64(i))
		require.NoError(t, err)
	}
	rightMean, rightVariance, rightCount := 0.0, 0.0, int64(0)
	for i := 3; i < 7; i++ {
		var err error
		rightMean, rightVariance, rightCount, err = updateVarianceState(rightMean, rightVariance, rightCount, 1_000_000_000_000+float64(i))
		require.NoError(t, err)
	}

	mean, variance, count, err := mergeVarianceState(leftMean, leftVariance, leftCount, rightMean, rightVariance, rightCount)
	require.NoError(t, err)
	require.Equal(t, int64(7), count)
	require.InEpsilon(t, 1_000_000_000_003.0, mean, 1e-15)
	require.InEpsilon(t, 4.0, variance, 1e-15)
}

func TestMergeVarianceStateAvoidsFiniteIntermediateOverflow(t *testing.T) {
	_, variance, count, err := mergeVarianceState(0, 0, 2, 1.5e154, 0, 2)
	require.NoError(t, err)
	require.Equal(t, int64(4), count)
	require.False(t, math.IsInf(variance, 0))
	require.InEpsilon(t, 5.625e307, variance, 1e-15)

	mean, residentVariance, residentCount := 0.0, 0.0, int64(0)
	for _, value := range []float64{0, 0, 1.5e154, 1.5e154} {
		mean, residentVariance, residentCount, err = updateVarianceState(mean, residentVariance, residentCount, value)
		require.NoError(t, err)
	}
	require.Equal(t, int64(4), residentCount)
	require.False(t, math.IsInf(residentVariance, 0))
	require.InEpsilon(t, 5.625e307, residentVariance, 1e-15)
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

func TestLegacyVarianceStateKeepsPreV29WireLayout(t *testing.T) {
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
	require.Len(t, stable.aggInfo.stateTypes, 4)
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
