// Copyright 2024 Matrix Origin
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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func buildDecimal256Vector(t *testing.T, mp *mpool.MPool, typ types.Type, nulls []bool, values []types.Decimal256) *vector.Vector {
	vec := vector.NewVec(typ)
	for i, value := range values {
		isNull := len(nulls) > 0 && nulls[i]
		require.NoError(t, vector.AppendFixed(vec, value, isNull, mp))
	}
	return vec
}

func buildNumericTestDataVecs(t *testing.T, mp *mpool.MPool) ([]types.Type, []*vector.Vector, []*vector.Vector) {
	nulls := []bool{false, false, false, false, true, false, false, false, false, true}
	int8s := []int8{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	int32s := []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	int64s := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	float32s := []float32{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0}
	float64s := []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0}
	d64s := []types.Decimal64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	d128s := []types.Decimal128{
		{B0_63: 1, B64_127: 0},
		{B0_63: 2, B64_127: 0},
		{B0_63: 3, B64_127: 0},
		{B0_63: 4, B64_127: 0},
		{B0_63: 5, B64_127: 0},
		{B0_63: 6, B64_127: 0},
		{B0_63: 7, B64_127: 0},
		{B0_63: 8, B64_127: 0},
		{B0_63: 9, B64_127: 0},
		{B0_63: 10, B64_127: 0},
		{B0_63: 11, B64_127: 0},
		{B0_63: 12, B64_127: 0}}
	d256s := []types.Decimal256{
		types.Decimal256FromInt64(1),
		types.Decimal256FromInt64(2),
		types.Decimal256FromInt64(3),
		types.Decimal256FromInt64(4),
		types.Decimal256FromInt64(5),
		types.Decimal256FromInt64(6),
		types.Decimal256FromInt64(7),
		types.Decimal256FromInt64(8),
		types.Decimal256FromInt64(9),
		types.Decimal256FromInt64(10),
		types.Decimal256FromInt64(11),
		types.Decimal256FromInt64(12),
	}

	typs := []types.Type{
		types.T_int8.ToType(),
		types.T_int32.ToType(),
		types.T_int64.ToType(),
		types.T_float32.ToType(),
		types.T_float64.ToType(),
		types.T_decimal64.ToType(),
		types.T_decimal128.ToType(),
		types.T_decimal256.ToType(),
	}

	for i := range typs {
		typs[i].Scale = 0
	}

	vecs := make([]*vector.Vector, 8)
	nvecs := make([]*vector.Vector, 8)
	vecs[0] = testutil.NewInt8Vector(10, typs[0], mp, false, nil, int8s[:10])
	nvecs[0] = testutil.NewInt8Vector(10, typs[0], mp, false, nulls, int8s[2:])
	vecs[1] = testutil.NewInt32Vector(10, typs[1], mp, false, nil, int32s[:10])
	nvecs[1] = testutil.NewInt32Vector(10, typs[1], mp, false, nulls, int32s[2:])
	vecs[2] = testutil.NewInt64Vector(10, typs[2], mp, false, nil, int64s[:10])
	nvecs[2] = testutil.NewInt64Vector(10, typs[2], mp, false, nulls, int64s[2:])
	vecs[3] = testutil.NewFloat32Vector(10, typs[3], mp, false, nil, float32s[:10])
	nvecs[3] = testutil.NewFloat32Vector(10, typs[3], mp, false, nulls, float32s[2:])
	vecs[4] = testutil.NewFloat64Vector(10, typs[4], mp, false, nil, float64s[:10])
	nvecs[4] = testutil.NewFloat64Vector(10, typs[4], mp, false, nulls, float64s[2:])
	vecs[5] = testutil.NewDecimal64Vector(10, typs[5], mp, false, nil, d64s[:10])
	nvecs[5] = testutil.NewDecimal64Vector(10, typs[5], mp, false, nulls, d64s[2:])
	vecs[6] = testutil.NewDecimal128Vector(10, typs[6], mp, false, nil, d128s[:10])
	nvecs[6] = testutil.NewDecimal128Vector(10, typs[6], mp, false, nulls, d128s[2:])
	vecs[7] = buildDecimal256Vector(t, mp, typs[7], nil, d256s[:10])
	nvecs[7] = buildDecimal256Vector(t, mp, typs[7], nulls, d256s[2:])
	return typs, vecs, nvecs
}

type expectedResult struct {
	expected float64
}

func (e *expectedResult) check(val any, scale int32) error {
	switch val := val.(type) {
	case int64:
		if math.Abs(e.expected-float64(val)) > 1e-6 {
			return moerr.NewInternalErrorNoCtxf("expected %f, got %d", e.expected, val)
		}
	case float64:
		if math.Abs(e.expected-val) > 1e-6 {
			return moerr.NewInternalErrorNoCtxf("expected %f, got %f", e.expected, val)
		}
	case types.Decimal128:
		resultFloat := types.Decimal128ToFloat64(val, scale)
		tolerance := 1e-6
		if scale > 0 {
			tolerance = math.Max(tolerance, math.Pow10(-int(scale)))
		}
		if math.Abs(e.expected-resultFloat) > tolerance {
			return moerr.NewInternalErrorNoCtxf("expected %f, got %f", e.expected, resultFloat)
		}
	case types.Decimal256:
		resultFloat := types.Decimal256ToFloat64(val, scale)
		if math.Abs(e.expected-resultFloat) > 1e-6 {
			return moerr.NewInternalErrorNoCtxf("expected %f, got %f", e.expected, resultFloat)
		}
	default:
		return moerr.NewInternalErrorNoCtxf("unsupported type %T", val)
	}
	return nil
}

func (e *expectedResult) checkVecAt(vec *vector.Vector, idx int) error {
	typ := vec.GetType()
	switch typ.Oid {
	case types.T_int64:
		return e.check(vector.MustFixedColNoTypeCheck[int64](vec)[idx], typ.Scale)
	case types.T_float64:
		return e.check(vector.MustFixedColNoTypeCheck[float64](vec)[idx], typ.Scale)
	case types.T_decimal128:
		return e.check(vector.MustFixedColNoTypeCheck[types.Decimal128](vec)[idx], typ.Scale)
	case types.T_decimal256:
		return e.check(vector.MustFixedColNoTypeCheck[types.Decimal256](vec)[idx], typ.Scale)
	}
	return moerr.NewInternalErrorNoCtxf("unsupported type %s", typ.Oid)
}

func checkVecAll(vec *vector.Vector, expected []expectedResult) error {
	for i, expected := range expected {
		if err := expected.checkVecAt(vec, i); err != nil {
			return err
		}
	}
	return nil
}

type expectedSumAvg struct {
	expected    expectedResult
	b2          [2]expectedResult
	expected20k expectedResult
}

func newExpectedSumAvg(exp1, b2a, b2b, exp20k float64) *expectedSumAvg {
	return &expectedSumAvg{
		expected: expectedResult{expected: exp1},
		b2: [2]expectedResult{
			{expected: b2a},
			{expected: b2b},
		},
		expected20k: expectedResult{expected: exp20k},
	}
}

func TestExpectedSumAvg(t *testing.T) {
	e1 := expectedResult{expected: 100}
	e2 := expectedResult{expected: 200.1230000001}
	e3 := expectedResult{expected: 200.1234}
	require.NoError(t, e1.check(int64(100), 3))
	require.NoError(t, e2.check(float64(200.1230000001), 3))
	require.Error(t, e3.check(float64(200.123456), 3))
}

func makeSumExec(t *testing.T, mp *mpool.MPool, typ types.Type) AggFuncExec {
	agg := makeSumAvgExec(mp, true, AggIdOfSum, false, typ)
	return agg
}

func makeSumDistinctExec(t *testing.T, mp *mpool.MPool, typ types.Type) AggFuncExec {
	agg := makeSumAvgExec(mp, true, AggIdOfSum, true, typ)
	return agg
}

func makeAvgExec(t *testing.T, mp *mpool.MPool, typ types.Type) AggFuncExec {
	agg := makeSumAvgExec(mp, false, AggIdOfAvg, false, typ)
	return agg
}

func makeAvgDistinctExec(t *testing.T, mp *mpool.MPool, typ types.Type) AggFuncExec {
	agg := makeSumAvgExec(mp, false, AggIdOfSum, true, typ)
	return agg
}

func TestSum(t *testing.T) {
	testSumAvg(t, makeSumExec, newExpectedSumAvg(111, 53, 58, 222000))
}

func TestSumDistinct(t *testing.T) {
	testSumAvg(t, makeSumDistinctExec, newExpectedSumAvg(66, 36, 30, 222000))
}

func TestAvg(t *testing.T) {
	testSumAvg(t, makeAvgExec, newExpectedSumAvg(6.1666666666, 5.88888888, 6.4444444444, 126000))
}

func TestAvgDistinct(t *testing.T) {
	testSumAvg(t, makeAvgDistinctExec, newExpectedSumAvg(6, 6, 6, 126000))
}

func TestSumAvgBigIntOverflowUsesDecimal128State(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	cases := []struct {
		name       string
		isSum      bool
		typ        types.Type
		vec        *vector.Vector
		want       string
		wantScale  int32
		wantRetTyp types.T
	}{
		{
			name:       "sum_int64_positive_over_int64",
			isSum:      true,
			typ:        types.T_int64.ToType(),
			vec:        testutil.NewInt64Vector(3, types.T_int64.ToType(), mp, false, nil, []int64{math.MaxInt64, 1, -2}),
			want:       "9223372036854775806",
			wantScale:  0,
			wantRetTyp: types.T_decimal128,
		},
		{
			name:       "sum_int64_negative_under_int64",
			isSum:      true,
			typ:        types.T_int64.ToType(),
			vec:        testutil.NewInt64Vector(3, types.T_int64.ToType(), mp, false, nil, []int64{math.MinInt64, -1, 2}),
			want:       "-9223372036854775807",
			wantScale:  0,
			wantRetTyp: types.T_decimal128,
		},
		{
			name:       "sum_uint64_over_uint64",
			isSum:      true,
			typ:        types.T_uint64.ToType(),
			vec:        testutil.NewUInt64Vector(3, types.T_uint64.ToType(), mp, false, nil, []uint64{1, math.MaxUint64, 3}),
			want:       "18446744073709551619",
			wantScale:  0,
			wantRetTyp: types.T_decimal128,
		},
		{
			name:       "avg_int64_positive_over_int64",
			isSum:      false,
			typ:        types.T_int64.ToType(),
			vec:        testutil.NewInt64Vector(3, types.T_int64.ToType(), mp, false, nil, []int64{math.MaxInt64, 1, -2}),
			want:       "3074457345618258602.0000",
			wantScale:  4,
			wantRetTyp: types.T_decimal128,
		},
		{
			name:       "avg_int64_negative_under_int64",
			isSum:      false,
			typ:        types.T_int64.ToType(),
			vec:        testutil.NewInt64Vector(3, types.T_int64.ToType(), mp, false, nil, []int64{math.MinInt64, -1, 2}),
			want:       "-3074457345618258602.3333",
			wantScale:  4,
			wantRetTyp: types.T_decimal128,
		},
		{
			name:       "avg_uint64_over_uint64",
			isSum:      false,
			typ:        types.T_uint64.ToType(),
			vec:        testutil.NewUInt64Vector(3, types.T_uint64.ToType(), mp, false, nil, []uint64{1, math.MaxUint64, 3}),
			want:       "6148914691236517206.3333",
			wantScale:  4,
			wantRetTyp: types.T_decimal128,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			curNB := mp.CurrNB()
			aggID := AggIdOfSum
			if !tc.isSum {
				aggID = AggIdOfAvg
			}
			exec := makeSumAvgExec(mp, tc.isSum, aggID, false, tc.typ)
			require.NoError(t, exec.GroupGrow(1))
			require.NoError(t, exec.BatchFill(0, []uint64{1, 1, 1}, []*vector.Vector{tc.vec}))

			results, err := exec.Flush()
			require.NoError(t, err)
			require.Len(t, results, 1)
			require.Equal(t, tc.wantRetTyp, results[0].GetType().Oid)
			require.Equal(t, tc.wantScale, results[0].GetType().Scale)

			got := vector.MustFixedColNoTypeCheck[types.Decimal128](results[0])[0]
			want, err := types.ParseDecimal128(tc.want, 38, tc.wantScale)
			require.NoError(t, err)
			require.Equal(t, want, got)

			exec.Free()
			for _, result := range results {
				result.Free(mp)
			}
			tc.vec.Free(mp)
			require.Equal(t, curNB, mp.CurrNB())
		})
	}
}

func TestSumAvgDistinctBigIntOverflowUsesDecimal128State(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	cases := []struct {
		name       string
		isSum      bool
		typ        types.Type
		vec        *vector.Vector
		want       string
		wantScale  int32
		wantRetTyp types.T
	}{
		{
			name:       "sum_distinct_int64_positive_over_int64",
			isSum:      true,
			typ:        types.T_int64.ToType(),
			vec:        testutil.NewInt64Vector(4, types.T_int64.ToType(), mp, false, nil, []int64{math.MaxInt64, 1, 1, -2}),
			want:       "9223372036854775806",
			wantScale:  0,
			wantRetTyp: types.T_decimal128,
		},
		{
			name:       "sum_distinct_uint64_over_uint64",
			isSum:      true,
			typ:        types.T_uint64.ToType(),
			vec:        testutil.NewUInt64Vector(4, types.T_uint64.ToType(), mp, false, nil, []uint64{1, math.MaxUint64, 1, 3}),
			want:       "18446744073709551619",
			wantScale:  0,
			wantRetTyp: types.T_decimal128,
		},
		{
			name:       "avg_distinct_int64_negative_under_int64",
			isSum:      false,
			typ:        types.T_int64.ToType(),
			vec:        testutil.NewInt64Vector(4, types.T_int64.ToType(), mp, false, nil, []int64{math.MinInt64, -1, -1, 2}),
			want:       "-3074457345618258602.3333",
			wantScale:  4,
			wantRetTyp: types.T_decimal128,
		},
		{
			name:       "avg_distinct_uint64_over_uint64",
			isSum:      false,
			typ:        types.T_uint64.ToType(),
			vec:        testutil.NewUInt64Vector(4, types.T_uint64.ToType(), mp, false, nil, []uint64{1, math.MaxUint64, 1, 3}),
			want:       "6148914691236517206.3333",
			wantScale:  4,
			wantRetTyp: types.T_decimal128,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			curNB := mp.CurrNB()
			aggID := AggIdOfSum
			if !tc.isSum {
				aggID = AggIdOfAvg
			}
			exec := makeSumAvgExec(mp, tc.isSum, aggID, true, tc.typ)
			require.NoError(t, exec.GroupGrow(1))
			require.NoError(t, exec.BatchFill(0, []uint64{1, 1, 1, 1}, []*vector.Vector{tc.vec}))

			results, err := exec.Flush()
			require.NoError(t, err)
			require.Len(t, results, 1)
			require.Equal(t, tc.wantRetTyp, results[0].GetType().Oid)
			require.Equal(t, tc.wantScale, results[0].GetType().Scale)

			got := vector.MustFixedColNoTypeCheck[types.Decimal128](results[0])[0]
			want, err := types.ParseDecimal128(tc.want, 38, tc.wantScale)
			require.NoError(t, err)
			require.Equal(t, want, got)

			exec.Free()
			for _, result := range results {
				result.Free(mp)
			}
			tc.vec.Free(mp)
			require.Equal(t, curNB, mp.CurrNB())
		})
	}
}

func TestAvgBigIntColumnScaleMinusOneUsesDecimalScaleZero(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	cases := []struct {
		name string
		typ  types.Type
		vec  *vector.Vector
		want string
	}{
		{
			name: "int64",
			typ:  types.T_int64.ToTypeWithScale(-1),
			vec:  testutil.NewInt64Vector(2, types.T_int64.ToTypeWithScale(-1), mp, false, nil, []int64{2, 2}),
			want: "2.0000",
		},
		{
			name: "uint64",
			typ:  types.T_uint64.ToTypeWithScale(-1),
			vec:  testutil.NewUInt64Vector(2, types.T_uint64.ToTypeWithScale(-1), mp, false, nil, []uint64{3, 4}),
			want: "3.5000",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			curNB := mp.CurrNB()
			exec := makeSumAvgExec(mp, false, AggIdOfAvg, false, tc.typ)
			require.NoError(t, exec.GroupGrow(1))
			require.NoError(t, exec.BatchFill(0, []uint64{1, 1}, []*vector.Vector{tc.vec}))

			results, err := exec.Flush()
			require.NoError(t, err)
			require.Len(t, results, 1)
			require.Equal(t, types.T_decimal128, results[0].GetType().Oid)
			require.Equal(t, int32(4), results[0].GetType().Scale)

			got := vector.MustFixedColNoTypeCheck[types.Decimal128](results[0])[0]
			want, err := types.ParseDecimal128(tc.want, 38, 4)
			require.NoError(t, err)
			require.Equal(t, want, got)

			exec.Free()
			for _, result := range results {
				result.Free(mp)
			}
			tc.vec.Free(mp)
			require.Equal(t, curNB, mp.CurrNB())
		})
	}
}

func TestSumBigIntMergeOverflowUsesDecimal128State(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	cases := []struct {
		name string
		typ  types.Type
		vec1 *vector.Vector
		vec2 *vector.Vector
		want string
	}{
		{
			name: "int64_merge_positive_over_int64",
			typ:  types.T_int64.ToType(),
			vec1: testutil.NewInt64Vector(1, types.T_int64.ToType(), mp, false, nil, []int64{math.MaxInt64}),
			vec2: testutil.NewInt64Vector(1, types.T_int64.ToType(), mp, false, nil, []int64{1}),
			want: "9223372036854775808",
		},
		{
			name: "uint64_merge_over_uint64",
			typ:  types.T_uint64.ToType(),
			vec1: testutil.NewUInt64Vector(1, types.T_uint64.ToType(), mp, false, nil, []uint64{math.MaxUint64}),
			vec2: testutil.NewUInt64Vector(1, types.T_uint64.ToType(), mp, false, nil, []uint64{1}),
			want: "18446744073709551616",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			curNB := mp.CurrNB()
			left := makeSumAvgExec(mp, true, AggIdOfSum, false, tc.typ)
			right := makeSumAvgExec(mp, true, AggIdOfSum, false, tc.typ)
			require.NoError(t, left.GroupGrow(1))
			require.NoError(t, right.GroupGrow(1))
			require.NoError(t, left.BatchFill(0, []uint64{1}, []*vector.Vector{tc.vec1}))
			require.NoError(t, right.BatchFill(0, []uint64{1}, []*vector.Vector{tc.vec2}))
			require.NoError(t, left.BatchMerge(right, 0, []uint64{1}))

			results, err := left.Flush()
			require.NoError(t, err)
			require.Len(t, results, 1)
			require.Equal(t, types.T_decimal128, results[0].GetType().Oid)
			require.Equal(t, int32(0), results[0].GetType().Scale)

			got := vector.MustFixedColNoTypeCheck[types.Decimal128](results[0])[0]
			want, err := types.ParseDecimal128(tc.want, 38, 0)
			require.NoError(t, err)
			require.Equal(t, want, got)

			left.Free()
			right.Free()
			for _, result := range results {
				result.Free(mp)
			}
			tc.vec1.Free(mp)
			tc.vec2.Free(mp)
			require.Equal(t, curNB, mp.CurrNB())
		})
	}
}

func testSumAvg(t *testing.T,
	makeSumAvgExec func(t *testing.T, mp *mpool.MPool, typ types.Type) AggFuncExec,
	expected *expectedSumAvg) {

	mp := mpool.MustNewZero()
	typs, vecs, nvecs := buildNumericTestDataVecs(t, mp)

	t.Run("BulkFill", func(t *testing.T) {
		for i, typ := range typs {
			curNB := mp.CurrNB()
			exec := makeSumAvgExec(t, mp, typ)
			exec.GetOptResult().modifyChunkSize(1)
			require.NoError(t, exec.GroupGrow(1))
			require.NoError(t, exec.BulkFill(0, vecs[i:i+1]))
			require.NoError(t, exec.BulkFill(0, nvecs[i:i+1]))
			results, err := exec.Flush()
			require.NoError(t, err)
			require.Len(t, results, 1)

			require.NoError(t, expected.expected.checkVecAt(results[0], 0))

			exec.Free()
			for _, result := range results {
				result.Free(mp)
			}
			require.Equal(t, curNB, mp.CurrNB())
		}
	})

	t.Run("BatchFill1", func(t *testing.T) {
		for i, typ := range typs {
			curNB := mp.CurrNB()
			exec := makeSumAvgExec(t, mp, typ)
			require.NoError(t, exec.GroupGrow(1))

			require.NoError(t, exec.BatchFill(0, []uint64{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, vecs[i:i+1]))
			require.NoError(t, exec.BatchFill(0, []uint64{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, nvecs[i:i+1]))

			results, err := exec.Flush()
			require.NoError(t, err)
			require.Len(t, results, 1)

			require.NoError(t, expected.expected.checkVecAt(results[0], 0))
			exec.Free()
			for _, result := range results {
				result.Free(mp)
			}
			require.Equal(t, curNB, mp.CurrNB())
		}
	})

	t.Run("BatchFill2", func(t *testing.T) {
		for i, typ := range typs {
			curNB := mp.CurrNB()
			exec := makeSumAvgExec(t, mp, typ)
			require.NoError(t, exec.GroupGrow(2))

			require.NoError(t, exec.BatchFill(0, []uint64{1, 2, 1, 2, 1, 2, 1, 2, 1, 2}, vecs[i:i+1]))
			require.NoError(t, exec.BatchFill(0, []uint64{1, 2, 1, 2, 1, 2, 1, 2, 1, 2}, nvecs[i:i+1]))

			results, err := exec.Flush()
			require.NoError(t, err)
			require.Len(t, results, 1)

			require.NoError(t, checkVecAll(results[0], expected.b2[:]))

			exec.Free()
			for _, result := range results {
				result.Free(mp)
			}
			require.Equal(t, curNB, mp.CurrNB())
		}
	})

	t.Run("BatchFill20000", func(t *testing.T) {
		for i, typ := range typs {
			curNB := mp.CurrNB()
			exec := makeSumAvgExec(t, mp, typ)
			// grow twice, so we have 20000 groups.
			require.NoError(t, exec.GroupGrow(10000))
			require.NoError(t, exec.GroupGrow(10000))

			for j := 0; j < 2000; j++ {
				groups := make([]uint64, 10)
				for k := range groups {
					groups[k] = uint64(j*10 + k + 1)
				}

				require.NoError(t, exec.BatchFill(0, groups[:5], vecs[i:i+1]))
				require.NoError(t, exec.BatchFill(5, groups[5:], vecs[i:i+1]))
				require.NoError(t, exec.BatchFill(0, groups[:5], nvecs[i:i+1]))
				require.NoError(t, exec.BatchFill(5, groups[5:], nvecs[i:i+1]))
			}

			results, err := exec.Flush()
			require.NoError(t, err)
			require.Equal(t, 3, len(results))
			require.Equal(t, 8192, results[0].Length())
			require.Equal(t, 8192, results[1].Length())
			require.Equal(t, 3616, results[2].Length())
			require.NoError(t, expected.expected20k.checkVecSum(results))

			for _, result := range results {
				result.Free(mp)
			}
			exec.Free()
			require.Equal(t, curNB, mp.CurrNB())
		}
	})
}

func (e *expectedResult) checkVecSum(vecs []*vector.Vector) error {
	var fsum float64

	for _, vec := range vecs {
		typ := vec.GetType()
		switch typ.Oid {
		case types.T_int64:
			vals := vector.MustFixedColNoTypeCheck[int64](vec)
			var sum int64 = 0
			for _, val := range vals {
				sum += val
			}
			fsum += float64(sum)
		case types.T_float64:
			vals := vector.MustFixedColNoTypeCheck[float64](vec)
			sum := 0.0
			for _, val := range vals {
				sum += val
			}
			fsum += sum
		case types.T_decimal128:
			vals := vector.MustFixedColNoTypeCheck[types.Decimal128](vec)
			sum := types.Decimal128{B0_63: 0, B64_127: 0}
			var err error
			for _, val := range vals {
				sum, err = sum.Add128(val)
				if err != nil {
					return err
				}
			}
			fsum += types.Decimal128ToFloat64(sum, typ.Scale)
		case types.T_decimal256:
			vals := vector.MustFixedColNoTypeCheck[types.Decimal256](vec)
			sum := types.Decimal256{}
			var err error
			for _, val := range vals {
				sum, err = sum.Add256(val)
				if err != nil {
					return err
				}
			}
			fsum += types.Decimal256ToFloat64(sum, typ.Scale)
		default:
			return moerr.NewInternalErrorNoCtxf("unsupported type %s", typ.Oid)
		}
	}

	if math.Abs(e.expected-fsum) > 1e-6 {
		return moerr.NewInternalErrorNoCtxf("expected %f, got %f", e.expected, fsum)
	}
	return nil
}
