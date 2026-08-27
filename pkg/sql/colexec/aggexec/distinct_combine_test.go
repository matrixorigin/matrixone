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

package aggexec

import (
	"bytes"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestInternalSumCombinePreservesTypeNullAndOverflow(t *testing.T) {
	mp := mpool.MustNewZero()
	typ := types.T_int64.ToType()
	input := testutil.NewInt64Vector(
		3, typ, mp, false, []bool{false, false, true}, []int64{10, 20, 0})
	defer input.Free(mp)

	exec, err := MakeAgg(mp, AggIdOfInternalSumCombine, false, typ)
	require.NoError(t, err)
	require.NoError(t, exec.GroupGrow(2))
	require.NoError(t, exec.BatchFill(
		0, []uint64{1, 1, 2}, []*vector.Vector{input}))
	result, err := exec.Flush()
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Equal(t, typ, *result[0].GetType())
	require.Equal(t, int64(30), vector.GetFixedAtNoTypeCheck[int64](result[0], 0))
	require.False(t, result[0].IsNull(0))
	require.True(t, result[0].IsNull(1))
	result[0].Free(mp)
	exec.Free()

	overflow := testutil.NewInt64Vector(
		2, typ, mp, false, nil, []int64{math.MaxInt64, 1})
	defer overflow.Free(mp)
	exec, err = MakeAgg(mp, AggIdOfInternalSumCombine, false, typ)
	require.NoError(t, err)
	require.NoError(t, exec.GroupGrow(1))
	require.Error(t, exec.BulkFill(0, []*vector.Vector{overflow}))
	exec.Free()
	require.Zero(t, mp.CurrNB())
}

func TestInternalCountCombineEmptyAndMerge(t *testing.T) {
	mp := mpool.MustNewZero()
	typ := types.T_int64.ToType()
	leftInput := testutil.NewInt64Vector(1, typ, mp, false, nil, []int64{2})
	rightInput := testutil.NewInt64Vector(1, typ, mp, false, nil, []int64{3})
	defer leftInput.Free(mp)
	defer rightInput.Free(mp)

	left, err := MakeAgg(mp, AggIdOfInternalCountCombine, false, typ)
	require.NoError(t, err)
	right, err := MakeAgg(mp, AggIdOfInternalCountCombine, false, typ)
	require.NoError(t, err)
	require.NoError(t, left.GroupGrow(2))
	require.NoError(t, right.GroupGrow(1))
	require.NoError(t, left.Fill(0, 0, []*vector.Vector{leftInput}))
	require.NoError(t, right.Fill(0, 0, []*vector.Vector{rightInput}))
	require.NoError(t, left.Merge(right, 0, 0))
	result, err := left.Flush()
	require.NoError(t, err)
	require.Equal(t, int64(5), vector.GetFixedAtNoTypeCheck[int64](result[0], 0))
	require.False(t, result[0].IsNull(0))
	require.Equal(t, int64(0), vector.GetFixedAtNoTypeCheck[int64](result[0], 1))
	require.False(t, result[0].IsNull(1), "COUNT combine must return zero for an empty group")
	result[0].Free(mp)
	left.Free()
	right.Free()
	require.Zero(t, mp.CurrNB())
}

func TestInternalAvgCombineWeightedAndDecimal(t *testing.T) {
	t.Run("numeric weighted average", func(t *testing.T) {
		mp := mpool.MustNewZero()
		sumType := types.T_int64.ToType()
		resultType := types.T_float64.ToType()
		sums := testutil.NewInt64Vector(2, sumType, mp, false, nil, []int64{10, 20})
		counts := testutil.NewInt64Vector(2, types.T_int64.ToType(), mp, false, nil, []int64{2, 3})
		witness := vector.NewConstNull(resultType, 2, mp)
		defer sums.Free(mp)
		defer counts.Free(mp)
		defer witness.Free(mp)

		exec, err := MakeAgg(
			mp, AggIdOfInternalAvgCombine, false,
			sumType, types.T_int64.ToType(), resultType)
		require.NoError(t, err)
		require.NoError(t, exec.GroupGrow(2))
		require.NoError(t, exec.BulkFill(
			0, []*vector.Vector{sums, counts, witness}))
		result, err := exec.Flush()
		require.NoError(t, err)
		require.InDelta(t, 6.0,
			vector.GetFixedAtNoTypeCheck[float64](result[0], 0), 1e-12)
		require.True(t, result[0].IsNull(1))
		result[0].Free(mp)
		exec.Free()
		require.Zero(t, mp.CurrNB())
	})

	t.Run("decimal scale comes from result witness", func(t *testing.T) {
		mp := mpool.MustNewZero()
		sumType := types.New(types.T_decimal128, 38, 2)
		resultType := types.New(types.T_decimal128, 38, 8)
		one, err := types.ParseDecimal128("1.00", 38, 2)
		require.NoError(t, err)
		two, err := types.ParseDecimal128("2.00", 38, 2)
		require.NoError(t, err)
		sums := testutil.NewDecimal128Vector(
			2, sumType, mp, false, nil, []types.Decimal128{one, two})
		counts := testutil.NewInt64Vector(
			2, types.T_int64.ToType(), mp, false, nil, []int64{1, 2})
		witness := vector.NewConstNull(resultType, 2, mp)
		defer sums.Free(mp)
		defer counts.Free(mp)
		defer witness.Free(mp)

		exec, err := MakeAgg(
			mp, AggIdOfInternalAvgCombine, false,
			sumType, types.T_int64.ToType(), resultType)
		require.NoError(t, err)
		require.NoError(t, exec.GroupGrow(1))
		require.NoError(t, exec.BatchFill(
			0, []uint64{1, 1}, []*vector.Vector{sums, counts, witness}))
		result, err := exec.Flush()
		require.NoError(t, err)
		got := vector.GetFixedAtNoTypeCheck[types.Decimal128](result[0], 0)
		require.Equal(t, "1.00000000", got.Format(resultType.Scale))
		result[0].Free(mp)
		exec.Free()
		require.Zero(t, mp.CurrNB())
	})
}

func TestInternalAvgCombineAdditionalValueTypes(t *testing.T) {
	t.Run("uint64", func(t *testing.T) {
		mp := mpool.MustNewZero()
		sumType := types.T_uint64.ToType()
		resultType := types.T_float64.ToType()
		sums := testutil.NewUInt64Vector(2, sumType, mp, false, nil, []uint64{10, 20})
		counts := testutil.NewInt64Vector(
			2, types.T_int64.ToType(), mp, false, nil, []int64{2, 3})
		witness := vector.NewConstNull(resultType, 2, mp)
		defer sums.Free(mp)
		defer counts.Free(mp)
		defer witness.Free(mp)
		exec, err := MakeAgg(
			mp, AggIdOfInternalAvgCombine, false,
			sumType, types.T_int64.ToType(), resultType)
		require.NoError(t, err)
		require.NoError(t, exec.GroupGrow(1))
		require.NoError(t, exec.BulkFill(0, []*vector.Vector{sums, counts, witness}))
		result, err := exec.Flush()
		require.NoError(t, err)
		require.Equal(t, float64(6), vector.GetFixedAtNoTypeCheck[float64](result[0], 0))
		result[0].Free(mp)
		exec.Free()
		require.Zero(t, mp.CurrNB())
	})

	t.Run("float64", func(t *testing.T) {
		mp := mpool.MustNewZero()
		sumType := types.T_float64.ToType()
		sums := testutil.NewFloat64Vector(2, sumType, mp, false, nil, []float64{1.5, 2.5})
		counts := testutil.NewInt64Vector(
			2, types.T_int64.ToType(), mp, false, nil, []int64{1, 1})
		witness := vector.NewConstNull(sumType, 2, mp)
		defer sums.Free(mp)
		defer counts.Free(mp)
		defer witness.Free(mp)
		exec, err := MakeAgg(
			mp, AggIdOfInternalAvgCombine, false,
			sumType, types.T_int64.ToType(), sumType)
		require.NoError(t, err)
		require.NoError(t, exec.GroupGrow(1))
		require.NoError(t, exec.BulkFill(0, []*vector.Vector{sums, counts, witness}))
		result, err := exec.Flush()
		require.NoError(t, err)
		require.Equal(t, float64(2), vector.GetFixedAtNoTypeCheck[float64](result[0], 0))
		result[0].Free(mp)
		exec.Free()
		require.Zero(t, mp.CurrNB())
	})

	t.Run("decimal256", func(t *testing.T) {
		mp := mpool.MustNewZero()
		sumType := types.New(types.T_decimal256, 65, 2)
		resultType := types.New(types.T_decimal256, 65, 8)
		one, err := types.ParseDecimal256("1.00", 65, 2)
		require.NoError(t, err)
		two, err := types.ParseDecimal256("2.00", 65, 2)
		require.NoError(t, err)
		sums := vector.NewVec(sumType)
		require.NoError(t, vector.AppendFixed(sums, one, false, mp))
		require.NoError(t, vector.AppendFixed(sums, two, false, mp))
		counts := testutil.NewInt64Vector(
			2, types.T_int64.ToType(), mp, false, nil, []int64{1, 2})
		witness := vector.NewConstNull(resultType, 2, mp)
		defer sums.Free(mp)
		defer counts.Free(mp)
		defer witness.Free(mp)
		exec, err := MakeAgg(
			mp, AggIdOfInternalAvgCombine, false,
			sumType, types.T_int64.ToType(), resultType)
		require.NoError(t, err)
		require.NoError(t, exec.GroupGrow(1))
		require.NoError(t, exec.BatchFill(
			0, []uint64{1, 1}, []*vector.Vector{sums, counts, witness}))
		result, err := exec.Flush()
		require.NoError(t, err)
		got := vector.GetFixedAtNoTypeCheck[types.Decimal256](result[0], 0)
		require.Equal(t, "1.00000000", got.Format(resultType.Scale))
		result[0].Free(mp)
		exec.Free()
		require.Zero(t, mp.CurrNB())
	})
}

func TestInternalAvgCombineRejectsInconsistentPartials(t *testing.T) {
	mp := mpool.MustNewZero()
	sumType := types.T_int64.ToType()
	resultType := types.T_float64.ToType()
	nullSum := vector.NewConstNull(sumType, 1, mp)
	nonNullSum := testutil.NewInt64Vector(1, sumType, mp, false, nil, []int64{1})
	zeroCount := testutil.NewInt64Vector(1, types.T_int64.ToType(), mp, false, nil, []int64{0})
	oneCount := testutil.NewInt64Vector(1, types.T_int64.ToType(), mp, false, nil, []int64{1})
	witness := vector.NewConstNull(resultType, 1, mp)
	defer nullSum.Free(mp)
	defer nonNullSum.Free(mp)
	defer zeroCount.Free(mp)
	defer oneCount.Free(mp)
	defer witness.Free(mp)

	for _, vectors := range [][]*vector.Vector{
		{nonNullSum, zeroCount, witness},
		{nullSum, oneCount, witness},
	} {
		exec, err := MakeAgg(
			mp, AggIdOfInternalAvgCombine, false,
			sumType, types.T_int64.ToType(), resultType)
		require.NoError(t, err)
		require.NoError(t, exec.GroupGrow(1))
		require.Error(t, exec.Fill(0, 0, vectors))
		exec.Free()
	}
	require.Zero(t, mp.CurrNB())
}

func TestInternalAvgCombineIntermediateRoundTrip(t *testing.T) {
	mp := mpool.MustNewZero()
	sumType := types.T_int64.ToType()
	resultType := types.T_float64.ToType()
	sums := testutil.NewInt64Vector(2, sumType, mp, false, nil, []int64{10, 20})
	counts := testutil.NewInt64Vector(
		2, types.T_int64.ToType(), mp, false, nil, []int64{2, 3})
	witness := vector.NewConstNull(resultType, 2, mp)
	defer sums.Free(mp)
	defer counts.Free(mp)
	defer witness.Free(mp)

	source, err := MakeAgg(
		mp, AggIdOfInternalAvgCombine, false,
		sumType, types.T_int64.ToType(), resultType)
	require.NoError(t, err)
	require.NoError(t, source.GroupGrow(1))
	require.NoError(t, source.BatchFill(
		0, []uint64{1, 1}, []*vector.Vector{sums, counts, witness}))
	var encoded bytes.Buffer
	require.NoError(t, source.SaveIntermediateResultOfChunk(0, &encoded))

	restored, err := MakeAgg(
		mp, AggIdOfInternalAvgCombine, false,
		sumType, types.T_int64.ToType(), resultType)
	require.NoError(t, err)
	require.NoError(t, restored.UnmarshalFromReader(bytes.NewReader(encoded.Bytes()), mp))
	result, err := restored.Flush()
	require.NoError(t, err)
	require.InDelta(t, 6.0,
		vector.GetFixedAtNoTypeCheck[float64](result[0], 0), 1e-12)
	result[0].Free(mp)
	source.Free()
	restored.Free()
	require.Zero(t, mp.CurrNB())
}

func TestInternalAvgCombineMerge(t *testing.T) {
	mp := mpool.MustNewZero()
	sumType := types.T_int64.ToType()
	resultType := types.T_float64.ToType()
	sums := testutil.NewInt64Vector(2, sumType, mp, false, nil, []int64{10, 20})
	counts := testutil.NewInt64Vector(
		2, types.T_int64.ToType(), mp, false, nil, []int64{2, 3})
	witness := vector.NewConstNull(resultType, 2, mp)
	defer sums.Free(mp)
	defer counts.Free(mp)
	defer witness.Free(mp)

	makeExec := func() AggFuncExec {
		exec, err := MakeAgg(
			mp, AggIdOfInternalAvgCombine, false,
			sumType, types.T_int64.ToType(), resultType)
		require.NoError(t, err)
		require.NoError(t, exec.GroupGrow(1))
		return exec
	}
	left := makeExec()
	right := makeExec()
	require.NoError(t, left.Fill(0, 0, []*vector.Vector{sums, counts, witness}))
	require.NoError(t, right.Fill(0, 1, []*vector.Vector{sums, counts, witness}))
	require.NoError(t, left.Merge(right, 0, 0))
	result, err := left.Flush()
	require.NoError(t, err)
	require.InDelta(t, 6.0,
		vector.GetFixedAtNoTypeCheck[float64](result[0], 0), 1e-12)
	result[0].Free(mp)
	left.Free()
	right.Free()
	require.Zero(t, mp.CurrNB())
}

func TestInternalCombineRejectsInvalidSignatures(t *testing.T) {
	mp := mpool.MustNewZero()
	_, err := MakeAgg(mp, AggIdOfInternalSumCombine, true, types.T_int64.ToType())
	require.Error(t, err)
	_, err = MakeAgg(mp, AggIdOfInternalSumCombine, false, types.T_varchar.ToType())
	require.Error(t, err)
	_, err = MakeAgg(mp, AggIdOfInternalCountCombine, false, types.T_uint64.ToType())
	require.Error(t, err)
	_, err = MakeAgg(mp, AggIdOfInternalAvgCombine, false, types.T_int64.ToType())
	require.Error(t, err)
	_, err = MakeAgg(
		mp, AggIdOfInternalAvgCombine, false,
		types.T_int64.ToType(), types.T_int64.ToType(), types.T_decimal128.ToType())
	require.Error(t, err)
	require.Zero(t, mp.CurrNB())
}
