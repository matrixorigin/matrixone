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
	"context"
	"encoding/binary"
	"math"
	"math/big"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"
	"github.com/stretchr/testify/require"
)

// The issue #29944 fixture: values NULL, 2, 4, 4, NULL, -1 with weights
// 1, 2, 1, 3, 1, 2. Under the lower weighted-CDF threshold
// k = ceil(0.5 * 8) = 4 the weighted P50 is 2, while the supported unweighted
// PERCENTILE_CONT(0.5) interpolates the surviving values to 3.
var issueFixtureValues = []float64{0, 2, 4, 4, 0, -1}
var issueFixtureValueNulls = []bool{true, false, false, false, true, false}
var issueFixtureWeights = []float64{1, 2, 1, 3, 1, 2}

func buildWeightedFixtureVecs(
	t *testing.T, mp *mpool.MPool,
) (*vector.Vector, *vector.Vector) {
	t.Helper()
	values := vector.NewVec(types.T_float64.ToType())
	for i := range issueFixtureValues {
		require.NoError(t, vector.AppendFixed(
			values, issueFixtureValues[i], issueFixtureValueNulls[i], mp))
	}
	weights := vector.NewVec(types.T_float64.ToType())
	for _, w := range issueFixtureWeights {
		require.NoError(t, vector.AppendFixed(weights, w, false, mp))
	}
	return values, weights
}

func makeWeightedExecForTest(
	t *testing.T, mp *mpool.MPool, distinct bool,
	valueType, weightType types.Type, level string,
) *quantileExactWeightedExec {
	t.Helper()
	exec, err := makeQuantileExactWeightedExec(mp,
		AggIdOfQuantileExactWeighted, distinct,
		[]types.Type{valueType, weightType})
	require.NoError(t, err)
	require.NoError(t, exec.SetExtraInformation(
		EncodeQuantileExactWeightedConfig([]byte(level)), 0))
	return exec.(*quantileExactWeightedExec)
}

// TestQuantileExactWeightedIssueFixture proves the issue's exact weighted
// percentile (2) beside the documented unweighted control (3).
func TestQuantileExactWeightedIssueFixture(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

	values, weights := buildWeightedFixtureVecs(t, mp)
	defer values.Free(mp)
	defer weights.Free(mp)

	exec := makeWeightedExecForTest(t, mp, false,
		types.T_float64.ToType(), types.T_float64.ToType(), "0.5")
	require.NoError(t, exec.GroupGrow(1))
	require.NoError(t, exec.BulkFill(0, []*vector.Vector{values, weights}))
	results, err := exec.Flush()
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.False(t, results[0].IsNull(0))
	require.Equal(t, 2.0, vector.GetFixedAtNoTypeCheck[float64](results[0], 0))
	results[0].Free(mp)
	exec.Free()

	// The supported unweighted control on the same values interpolates to 3,
	// so substituting it for the weighted aggregate cannot pass either check.
	controlVec := vector.NewVec(types.T_float64.ToType())
	defer controlVec.Free(mp)
	for i := range issueFixtureValues {
		require.NoError(t, vector.AppendFixed(
			controlVec, issueFixtureValues[i], issueFixtureValueNulls[i], mp))
	}
	control, err := makeOrderedPercentileExec(mp, AggIdOfPercentileCont,
		false, types.T_float64.ToType(), orderedPercentileContinuous)
	require.NoError(t, err)
	require.NoError(t, control.SetExtraInformation(
		EncodeOrderedPercentileConfig([]byte("0.5"), false), 0))
	require.NoError(t, control.GroupGrow(1))
	require.NoError(t, control.BulkFill(0, []*vector.Vector{controlVec}))
	controlResults, err := control.Flush()
	require.NoError(t, err)
	require.Equal(t, 3.0, vector.GetFixedAtNoTypeCheck[float64](controlResults[0], 0))
	controlResults[0].Free(mp)
	control.Free()
}

// TestQuantileExactWeightedEmptyAndSkippedGroups returns NULL for an empty
// group and for a group whose every row was skipped by the paired-NULL rule.
func TestQuantileExactWeightedEmptyAndSkippedGroups(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

	exec := makeWeightedExecForTest(t, mp, false,
		types.T_float64.ToType(), types.T_float64.ToType(), "0.5")
	require.NoError(t, exec.GroupGrow(2))

	allNullValues := vector.NewVec(types.T_float64.ToType())
	defer allNullValues.Free(mp)
	for i := 0; i < 3; i++ {
		require.NoError(t, vector.AppendFixed(allNullValues, 0, true, mp))
	}
	weights := vector.NewVec(types.T_float64.ToType())
	defer weights.Free(mp)
	for i := 0; i < 3; i++ {
		require.NoError(t, vector.AppendFixed(weights, 1, false, mp))
	}
	groups := []uint64{1, 2, 2}
	require.NoError(t, exec.BatchFill(0, groups,
		[]*vector.Vector{allNullValues, weights}))

	results, err := exec.Flush()
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.True(t, results[0].IsNull(0), "empty group must return NULL")
	require.True(t, results[0].IsNull(1), "all-skipped group must return NULL")
	results[0].Free(mp)
	exec.Free()
}

// TestQuantileExactWeightedEndpoints wires every zero-weight and level
// endpoint witness required by design decision D6.
func TestQuantileExactWeightedEndpoints(t *testing.T) {
	cases := []struct {
		name    string
		level   string
		values  []float64
		weights []uint64
		want    float64
	}{
		{
			name:    "level 1 never selects a trailing zero-weight value",
			level:   "1",
			values:  []float64{1, 9},
			weights: []uint64{1, 0},
			want:    1,
		},
		{
			name:    "level 0 selects the leading massless minimum",
			level:   "0",
			values:  []float64{0, 5},
			weights: []uint64{0, 1},
			want:    0,
		},
		{
			name:    "any level with ceil(level*W) >= 1 never selects massless values",
			level:   "0.5",
			values:  []float64{0, 5},
			weights: []uint64{0, 1},
			want:    5,
		},
		{
			name:    "an all-zero-weight group returns the minimum present value",
			level:   "1",
			values:  []float64{3, 7},
			weights: []uint64{0, 0},
			want:    3,
		},
		{
			name:    "ties collapse to summed weight",
			level:   "0.5",
			values:  []float64{5, 5, 9},
			weights: []uint64{2, 3, 1},
			want:    5,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

			values := vector.NewVec(types.T_float64.ToType())
			defer values.Free(mp)
			for _, v := range tc.values {
				require.NoError(t, vector.AppendFixed(values, v, false, mp))
			}
			weights := vector.NewVec(types.T_uint64.ToType())
			defer weights.Free(mp)
			for _, w := range tc.weights {
				require.NoError(t, vector.AppendFixed(weights, w, false, mp))
			}

			exec := makeWeightedExecForTest(t, mp, false,
				types.T_float64.ToType(), types.T_uint64.ToType(), tc.level)
			require.NoError(t, exec.GroupGrow(1))
			require.NoError(t, exec.BulkFill(0, []*vector.Vector{values, weights}))
			results, err := exec.Flush()
			require.NoError(t, err)
			require.Equal(t, tc.want,
				vector.GetFixedAtNoTypeCheck[float64](results[0], 0))
			results[0].Free(mp)
			exec.Free()
		})
	}
}

// TestQuantileExactWeightedThresholdArithmetic proves the k = ceil(level * W)
// threshold is computed exactly where a reduced fixed-width or float64
// product would select a different value.
func TestQuantileExactWeightedThresholdArithmetic(t *testing.T) {
	cases := []struct {
		name    string
		level   string
		values  []float64
		weights []uint64
		want    float64
	}{
		{
			name:    "level 1e-100 has k=1 and selects the first positive-mass value",
			level:   "1e-100",
			values:  []float64{1, 2},
			weights: []uint64{1, 1},
			want:    1,
		},
		{
			// exact k = ceil(0.5000000000000000000001 * 1e19) = 5e18 + 1;
			// a float64-rounded level gives k = 5e18 and selects 10.
			name:    "22-digit decimal level above 0.5",
			level:   "0.5000000000000000000001",
			values:  []float64{10, 90},
			weights: []uint64{5000000000000000000, 5000000000000000000},
			want:    90,
		},
		{
			name:    "level exactly 1 selects the largest positive-weight value",
			level:   "1",
			values:  []float64{-3, 4},
			weights: []uint64{7, 2},
			want:    4,
		},
		{
			name:    "level exactly 0 selects the minimum present value",
			level:   "0",
			values:  []float64{-3, 4},
			weights: []uint64{7, 2},
			want:    -3,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

			values := vector.NewVec(types.T_float64.ToType())
			defer values.Free(mp)
			for _, v := range tc.values {
				require.NoError(t, vector.AppendFixed(values, v, false, mp))
			}
			weights := vector.NewVec(types.T_uint64.ToType())
			defer weights.Free(mp)
			for _, w := range tc.weights {
				require.NoError(t, vector.AppendFixed(weights, w, false, mp))
			}

			exec := makeWeightedExecForTest(t, mp, false,
				types.T_float64.ToType(), types.T_uint64.ToType(), tc.level)
			require.NoError(t, exec.GroupGrow(1))
			require.NoError(t, exec.BulkFill(0, []*vector.Vector{values, weights}))
			results, err := exec.Flush()
			require.NoError(t, err)
			require.Equal(t, tc.want,
				vector.GetFixedAtNoTypeCheck[float64](results[0], 0),
				"level %q", tc.level)
			results[0].Free(mp)
			exec.Free()
		})
	}
}

// TestQuantileExactWeightedTypedWeights exercises typed weight validation in
// the weight's own type, including the two witnesses a FLOAT64 transport would
// have accepted incorrectly (UINT64 2^53+1 and a fractional high-scale
// DECIMAL).
func TestQuantileExactWeightedTypedWeights(t *testing.T) {
	t.Run("uint64 2^53+1 survives exactly, selecting the float-transport counterexample", func(t *testing.T) {
		mp := mpool.MustNewZero()
		defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

		values := vector.NewVec(types.T_int64.ToType())
		defer values.Free(mp)
		require.NoError(t, vector.AppendFixed[int64](values, 1, false, mp))
		require.NoError(t, vector.AppendFixed[int64](values, 2, false, mp))
		weights := vector.NewVec(types.T_uint64.ToType())
		defer weights.Free(mp)
		require.NoError(t, vector.AppendFixed[uint64](
			weights, 9007199254740993, false, mp))
		require.NoError(t, vector.AppendFixed[uint64](
			weights, 9007199254740992, false, mp))

		exec := makeWeightedExecForTest(t, mp, false,
			types.T_int64.ToType(), types.T_uint64.ToType(), "0.5")
		require.NoError(t, exec.GroupGrow(1))
		require.NoError(t, exec.BulkFill(0, []*vector.Vector{values, weights}))
		// k = ceil(18014398509481985 / 2) = 9007199254740993 selects value 1;
		// rounding 2^53+1 to 2^53 would shift the selection to 2.
		results, err := exec.Flush()
		require.NoError(t, err)
		require.Equal(t, int64(1),
			vector.GetFixedAtNoTypeCheck[int64](results[0], 0))
		results[0].Free(mp)
		exec.Free()
	})

	t.Run("uint64 max is accepted", func(t *testing.T) {
		mp := mpool.MustNewZero()
		defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

		values := vector.NewVec(types.T_int64.ToType())
		defer values.Free(mp)
		require.NoError(t, vector.AppendFixed[int64](values, 7, false, mp))
		weights := vector.NewVec(types.T_uint64.ToType())
		defer weights.Free(mp)
		require.NoError(t, vector.AppendFixed[uint64](
			weights, math.MaxUint64, false, mp))

		exec := makeWeightedExecForTest(t, mp, false,
			types.T_int64.ToType(), types.T_uint64.ToType(), "1")
		require.NoError(t, exec.GroupGrow(1))
		require.NoError(t, exec.BulkFill(0, []*vector.Vector{values, weights}))
		results, err := exec.Flush()
		require.NoError(t, err)
		require.Equal(t, int64(7),
			vector.GetFixedAtNoTypeCheck[int64](results[0], 0))
		results[0].Free(mp)
		exec.Free()
	})

	t.Run("float32 integral weight accepted", func(t *testing.T) {
		mp := mpool.MustNewZero()
		defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

		values := vector.NewVec(types.T_float64.ToType())
		defer values.Free(mp)
		require.NoError(t, vector.AppendFixed(values, 2.0, false, mp))
		require.NoError(t, vector.AppendFixed(values, 5.0, false, mp))
		weights := vector.NewVec(types.T_float32.ToType())
		defer weights.Free(mp)
		require.NoError(t, vector.AppendFixed[float32](weights, 3, false, mp))
		require.NoError(t, vector.AppendFixed[float32](weights, 1, false, mp))

		exec := makeWeightedExecForTest(t, mp, false,
			types.T_float64.ToType(), types.T_float32.ToType(), "0.6")
		require.NoError(t, exec.GroupGrow(1))
		require.NoError(t, exec.BulkFill(0, []*vector.Vector{values, weights}))
		// W=4, k=ceil(2.4)=3 selects 2.0.
		results, err := exec.Flush()
		require.NoError(t, err)
		require.Equal(t, 2.0, vector.GetFixedAtNoTypeCheck[float64](results[0], 0))
		results[0].Free(mp)
		exec.Free()
	})

	t.Run("decimal weights validate exactly through the stored scale", func(t *testing.T) {
		mp := mpool.MustNewZero()
		defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

		weightType := types.New(types.T_decimal128, 38, 18)
		values := vector.NewVec(types.T_float64.ToType())
		defer values.Free(mp)
		require.NoError(t, vector.AppendFixed(values, 1.5, false, mp))
		require.NoError(t, vector.AppendFixed(values, 9.5, false, mp))

		weights := vector.NewVec(weightType)
		defer weights.Free(mp)
		// 1.000000000000000001 is fractional in its own scale and must be
		// rejected before any conversion; a FLOAT64 cast would read it as 1.
		fractional, err := types.ParseDecimal128(
			"1.000000000000000001", 38, 18)
		require.NoError(t, err)
		require.NoError(t, vector.AppendFixed(weights, fractional, false, mp))

		exec := makeWeightedExecForTest(t, mp, false,
			types.T_float64.ToType(), weightType, "0.5")
		require.NoError(t, exec.GroupGrow(1))
		err = exec.BulkFill(0, []*vector.Vector{values, weights})
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
		exec.Free()

		// The integral spelling is valid and finalizes exactly.
		goodWeights := vector.NewVec(weightType)
		defer goodWeights.Free(mp)
		goodOne, parseErr := types.ParseDecimal128("1.000000000000000000", 38, 18)
		require.NoError(t, parseErr)
		goodTwo, parseErr := types.ParseDecimal128("2.000000000000000000", 38, 18)
		require.NoError(t, parseErr)
		require.NoError(t, vector.AppendFixed(goodWeights, goodOne, false, mp))
		require.NoError(t, vector.AppendFixed(goodWeights, goodTwo, false, mp))

		exec = makeWeightedExecForTest(t, mp, false,
			types.T_float64.ToType(), weightType, "0.5")
		require.NoError(t, exec.GroupGrow(1))
		require.NoError(t, exec.BulkFill(0, []*vector.Vector{values, goodWeights}))
		// W=3, k=2 selects 9.5 (F(1.5) = 1 < 2, F(9.5) = 3 >= 2).
		results, err := exec.Flush()
		require.NoError(t, err)
		require.Equal(t, 9.5, vector.GetFixedAtNoTypeCheck[float64](results[0], 0))
		results[0].Free(mp)
		exec.Free()
	})

	t.Run("invalid double weights abort at fill time", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			set  func(v *vector.Vector, mp *mpool.MPool)
		}{
			{name: "fractional", set: func(v *vector.Vector, mp *mpool.MPool) {
				require.NoError(t, vector.AppendFixed(v, 2.5, false, mp))
			}},
			{name: "negative", set: func(v *vector.Vector, mp *mpool.MPool) {
				require.NoError(t, vector.AppendFixed(v, -1.0, false, mp))
			}},
			{name: "nan", set: func(v *vector.Vector, mp *mpool.MPool) {
				require.NoError(t, vector.AppendFixed(v, math.NaN(), false, mp))
			}},
			{name: "inf", set: func(v *vector.Vector, mp *mpool.MPool) {
				require.NoError(t, vector.AppendFixed(v, math.Inf(1), false, mp))
			}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				mp := mpool.MustNewZero()
				defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

				values := vector.NewVec(types.T_float64.ToType())
				defer values.Free(mp)
				require.NoError(t, vector.AppendFixed(values, 1.0, false, mp))
				weights := vector.NewVec(types.T_float64.ToType())
				defer weights.Free(mp)
				tc.set(weights, mp)

				exec := makeWeightedExecForTest(t, mp, false,
					types.T_float64.ToType(), types.T_float64.ToType(), "0.5")
				require.NoError(t, exec.GroupGrow(1))
				err := exec.BulkFill(0, []*vector.Vector{values, weights})
				require.Error(t, err)
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
				exec.Free()
			})
		}
	})

	t.Run("negative integer weight aborts at fill time", func(t *testing.T) {
		mp := mpool.MustNewZero()
		defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

		values := vector.NewVec(types.T_int64.ToType())
		defer values.Free(mp)
		require.NoError(t, vector.AppendFixed[int64](values, 1, false, mp))
		weights := vector.NewVec(types.T_int64.ToType())
		defer weights.Free(mp)
		require.NoError(t, vector.AppendFixed[int64](weights, -2, false, mp))

		exec := makeWeightedExecForTest(t, mp, false,
			types.T_int64.ToType(), types.T_int64.ToType(), "0.5")
		require.NoError(t, exec.GroupGrow(1))
		err := exec.BulkFill(0, []*vector.Vector{values, weights})
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
		exec.Free()
	})
}

// TestQuantileExactWeightedResultTypeIsValueType checks the member-selection
// return type across every supported value domain.
func TestQuantileExactWeightedResultTypeIsValueType(t *testing.T) {
	for _, valueType := range []types.Type{
		types.T_int8.ToType(), types.T_int16.ToType(), types.T_int32.ToType(),
		types.T_int64.ToType(), types.T_uint8.ToType(), types.T_uint16.ToType(),
		types.T_uint32.ToType(), types.T_uint64.ToType(),
		types.T_float32.ToType(), types.T_float64.ToType(),
		types.New(types.T_decimal64, 10, 2),
		types.New(types.T_decimal128, 20, 2),
		types.T_bit.ToType(),
	} {
		t.Run(valueType.String(), func(t *testing.T) {
			mp := mpool.MustNewZero()
			defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

			exec := makeWeightedExecForTest(t, mp, false,
				valueType, types.T_uint64.ToType(), "0.5")
			require.NoError(t, exec.GroupGrow(1))
			results, err := exec.Flush()
			require.NoError(t, err)
			require.Len(t, results, 1)
			require.True(t, results[0].GetType().Eq(valueType))
			require.True(t, results[0].IsNull(0))
			results[0].Free(mp)
			exec.Free()
		})
	}
}

// TestQuantileExactWeightedTotalOverflow asserts the uint64 total-overflow
// abort, that it fires after partial results were already built for earlier
// groups, and that a fresh execution defines the expected recovery.
func TestQuantileExactWeightedTotalOverflow(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

	// Row 0 builds a normal group; rows 1 and 2 share a group whose total
	// weight crosses the uint64 boundary.
	values := vector.NewVec(types.T_int64.ToType())
	defer values.Free(mp)
	for _, v := range []int64{9, 1, 2} {
		require.NoError(t, vector.AppendFixed[int64](values, v, false, mp))
	}
	weights := vector.NewVec(types.T_uint64.ToType())
	defer weights.Free(mp)
	require.NoError(t, vector.AppendFixed[uint64](weights, 1, false, mp))
	require.NoError(t, vector.AppendFixed[uint64](
		weights, math.MaxUint64, false, mp))
	require.NoError(t, vector.AppendFixed[uint64](weights, 1, false, mp))

	exec := makeWeightedExecForTest(t, mp, false,
		types.T_int64.ToType(), types.T_uint64.ToType(), "0.5")
	require.NoError(t, exec.GroupGrow(2))
	require.NoError(t, exec.BatchFill(0, []uint64{1, 2, 2},
		[]*vector.Vector{values, weights}))
	results, err := exec.Flush()
	require.Error(t, err)
	require.ErrorContains(t, err, "total weight exceeds")
	require.Nil(t, results,
		"the error return must release the partially built group result")
	exec.Free()
	require.Equal(t, int64(0), mp.CurrNB(),
		"the abort must release exactly its partially built results")

	// A fresh execution on a fresh executor succeeds and returns the
	// expected value.
	freshValues := singleRowVec(t, mp, types.T_int64.ToType(), int64(1))
	defer freshValues.Free(mp)
	freshWeights := singleRowVec(t, mp, types.T_uint64.ToType(), uint64(3))
	defer freshWeights.Free(mp)
	fresh := makeWeightedExecForTest(t, mp, false,
		types.T_int64.ToType(), types.T_uint64.ToType(), "1")
	require.NoError(t, fresh.GroupGrow(1))
	require.NoError(t, fresh.BulkFill(
		0, []*vector.Vector{freshValues, freshWeights}))
	freshResults, err := fresh.Flush()
	require.NoError(t, err)
	require.Equal(t, int64(1),
		vector.GetFixedAtNoTypeCheck[int64](freshResults[0], 0))
	freshResults[0].Free(mp)
	fresh.Free()
}

func singleRowVec[T types.FixedSizeTExceptStrType](
	t *testing.T, mp *mpool.MPool, typ types.Type, value T,
) *vector.Vector {
	t.Helper()
	v := vector.NewVec(typ)
	require.NoError(t, vector.AppendFixed(v, value, false, mp))
	return v
}

// TestQuantileExactWeightedMergeEquivalence distributes one group's rows
// across partial states, merges them in every order, and requires the exact
// single-state result every time (order independence).
func TestQuantileExactWeightedMergeEquivalence(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

	partialRows := [][][2]int64{
		{{2, 2}, {4, 1}},
		{{4, 3}, {-1, 2}},
	}
	singleRows := [][][2]int64{{{2, 2}, {4, 1}, {4, 3}, {-1, 2}}}[0]

	buildVectors := func(rows [][2]int64) (values, weights *vector.Vector) {
		values = vector.NewVec(types.T_int64.ToType())
		weights = vector.NewVec(types.T_int64.ToType())
		for _, row := range rows {
			require.NoError(t, vector.AppendFixed[int64](values, row[0], false, mp))
			require.NoError(t, vector.AppendFixed[int64](weights, row[1], false, mp))
		}
		return values, weights
	}

	single := makeWeightedExecForTest(t, mp, false,
		types.T_int64.ToType(), types.T_int64.ToType(), "0.5")
	require.NoError(t, single.GroupGrow(1))
	singleValues, singleWeights := buildVectors(singleRows)
	defer singleValues.Free(mp)
	defer singleWeights.Free(mp)
	require.NoError(t, single.BulkFill(0, []*vector.Vector{singleValues, singleWeights}))
	want, err := single.Flush()
	require.NoError(t, err)
	defer want[0].Free(mp)
	single.Free()

	for _, order := range []struct {
		name   string
		first  int
		second int
	}{
		{name: "left-right", first: 0, second: 1},
		{name: "right-left", first: 1, second: 0},
	} {
		t.Run(order.name, func(t *testing.T) {
			merged := makeWeightedExecForTest(t, mp, false,
				types.T_int64.ToType(), types.T_int64.ToType(), "0.5")
			require.NoError(t, merged.GroupGrow(1))
			for _, which := range []int{order.first, order.second} {
				partial := makeWeightedExecForTest(t, mp, false,
					types.T_int64.ToType(), types.T_int64.ToType(), "0.5")
				require.NoError(t, partial.GroupGrow(1))
				values, weights := buildVectors(partialRows[which])
				require.NoError(t, partial.BulkFill(0, []*vector.Vector{values, weights}))
				require.NoError(t, merged.BatchMerge(partial, 0, []uint64{1}))
				values.Free(mp)
				weights.Free(mp)
				partial.Free()
			}
			got, err := merged.Flush()
			require.NoError(t, err)
			require.Equal(t,
				vector.GetFixedAtNoTypeCheck[int64](want[0], 0),
				vector.GetFixedAtNoTypeCheck[int64](got[0], 0))
			got[0].Free(mp)
			merged.Free()
		})
	}
}

// TestQuantileExactWeightedIntermediateWireRoundTrip saves the typed pair rows
// through the saved-argument interim codec and flushes identical output from
// the reconstructed state (decision D11 keeps the payload typed).
func TestQuantileExactWeightedIntermediateWireRoundTrip(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

	values, weights := buildWeightedFixtureVecs(t, mp)
	defer values.Free(mp)
	defer weights.Free(mp)

	source := makeWeightedExecForTest(t, mp, false,
		types.T_float64.ToType(), types.T_float64.ToType(), "0.5")
	require.NoError(t, source.GroupGrow(1))
	require.NoError(t, source.BulkFill(0, []*vector.Vector{values, weights}))
	var encoded bytes.Buffer
	require.NoError(t, source.SaveIntermediateResultOfChunk(0, &encoded))

	target := makeWeightedExecForTest(t, mp, false,
		types.T_float64.ToType(), types.T_float64.ToType(), "0.5")
	require.NoError(t, target.GroupGrow(1))
	require.NoError(t, target.UnmarshalFromReader(
		bytes.NewReader(encoded.Bytes()), mp))
	results, err := target.Flush()
	require.NoError(t, err)
	require.Equal(t, 2.0, vector.GetFixedAtNoTypeCheck[float64](results[0], 0),
		"the wired typed pair state must reproduce the fixture result")
	results[0].Free(mp)
	source.Free()
	target.Free()
}

// TestQuantileExactWeightedDistinctTypedPairs deduplicates equal (value,
// weight) pairs under DISTINCT and round-trips the distinct typed states,
// including FLOAT32 and DECIMAL128 weights, through the spill codec that
// validates each member against the declared argument types (decision D11).
func TestQuantileExactWeightedDistinctTypedPairs(t *testing.T) {
	t.Run("float32 weights dedupe and spill round-trip", func(t *testing.T) {
		mp := mpool.MustNewZero()
		defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

		values := vector.NewVec(types.T_float64.ToType())
		defer values.Free(mp)
		for _, v := range []float64{2, 2, 4} {
			require.NoError(t, vector.AppendFixed(values, v, false, mp))
		}
		weights := vector.NewVec(types.T_float32.ToType())
		defer weights.Free(mp)
		for _, w := range []float32{2, 2, 1} {
			require.NoError(t, vector.AppendFixed[float32](weights, w, false, mp))
		}

		source := makeWeightedExecForTest(t, mp, true,
			types.T_float64.ToType(), types.T_float32.ToType(), "0.5")
		require.NoError(t, source.GroupGrow(1))
		require.NoError(t, source.BulkFill(0, []*vector.Vector{values, weights}))
		// (2, 2) twice collapses once; W = 2 + 1 = 3, k = 2 selects 2.
		var spill bytes.Buffer
		require.NoError(t, source.SaveSpillIntermediateRows(0, []int32{0}, &spill))

		restored := makeWeightedExecForTest(t, mp, true,
			types.T_float64.ToType(), types.T_float32.ToType(), "0.5")
		require.NoError(t, restored.GroupGrow(1))
		require.NoError(t, restored.UnmarshalSpillFromReader(
			bytes.NewReader(spill.Bytes()), mp))
		results, err := restored.Flush()
		require.NoError(t, err)
		require.Equal(t, 2.0, vector.GetFixedAtNoTypeCheck[float64](results[0], 0))
		results[0].Free(mp)
		source.Free()
		restored.Free()
	})

	t.Run("decimal128 weights dedupe exactly", func(t *testing.T) {
		mp := mpool.MustNewZero()
		defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

		weightType := types.New(types.T_decimal128, 38, 18)
		values := vector.NewVec(types.T_float64.ToType())
		defer values.Free(mp)
		for _, v := range []float64{5, 5, 9} {
			require.NoError(t, vector.AppendFixed(values, v, false, mp))
		}
		weights := vector.NewVec(weightType)
		defer weights.Free(mp)
		decimalOne, err := types.ParseDecimal128("1.000000000000000000", 38, 18)
		require.NoError(t, err)
		for i := 0; i < 3; i++ {
			require.NoError(t, vector.AppendFixed(weights, decimalOne, false, mp))
		}

		exec := makeWeightedExecForTest(t, mp, true,
			types.T_float64.ToType(), weightType, "0.5")
		require.NoError(t, exec.GroupGrow(1))
		require.NoError(t, exec.BulkFill(0, []*vector.Vector{values, weights}))
		// (5, 1) deduped leaves W = 1 + 1 = 2, k = 1 selects 5.
		results, err := exec.Flush()
		require.NoError(t, err)
		require.Equal(t, 5.0, vector.GetFixedAtNoTypeCheck[float64](results[0], 0))
		results[0].Free(mp)
		exec.Free()
	})
}

// causeAtNthCheck deterministically turns into a cancelled context once
// finalization performs its n-th context check. context.Cause falls back to
// Err for non-canceler contexts, so every executor check flows through Err
// and the flip point pins exactly where in finalization the abort lands.
type causeAtNthCheck struct {
	context.Context
	n      int32
	cause  error
	checks int32
	flips  int32
}

func (c *causeAtNthCheck) Done() <-chan struct{}       { return nil }
func (c *causeAtNthCheck) Deadline() (time.Time, bool) { return time.Time{}, false }
func (c *causeAtNthCheck) Value(key any) any           { return c.Context.Value(key) }

func (c *causeAtNthCheck) Err() error {
	c.checks++
	if c.checks >= c.n {
		if c.flips == 0 {
			c.flips = c.checks
		}
		return c.cause
	}
	return c.Context.Err()
}

// fillWeightedThreeGroups fills three single-pair groups, so
// FlushWithContext's synchronous check order is deterministic: chunk entry;
// then per group: row entry, retained-args restore, post-sort, mid-scan.
func fillWeightedThreeGroups(
	t *testing.T, mp *mpool.MPool, exec *quantileExactWeightedExec,
) {
	t.Helper()
	groupValues := [][]float64{{2}, {4}, {1}}
	groupWeights := [][]float64{{2}, {3}, {1}}
	for g := range groupValues {
		values := vector.NewVec(types.T_float64.ToType())
		weights := vector.NewVec(types.T_float64.ToType())
		require.NoError(t, vector.AppendFixed(values, groupValues[g][0], false, mp))
		require.NoError(t, vector.AppendFixed(weights, groupWeights[g][0], false, mp))
		require.NoError(t, exec.BulkFill(g, []*vector.Vector{values, weights}))
		values.Free(mp)
		weights.Free(mp)
	}
}

// TestQuantileExactWeightedCancellation witnesses decision D12's cancellation
// points at every documented stage of finalization, not only at entry: the
// cancelled cause surfaces from the exact flip point, the error return frees
// all scratch and every partially built result, the allocation account is
// back to the baseline after executor Free, and a subsequent fresh execution
// still produces the correct weighted result.
func TestQuantileExactWeightedCancellation(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

	cancelCause := moerr.NewInvalidInputNoCtx("quantile test deadline")

	// Deterministic check order for the three-group input: 1 = chunk entry;
	// 2/3/4/5 = group 0's row entry, restore, post-sort, mid-scan; 6 = group
	// 1's row entry (group 0's answer is already appended to the result).
	for _, tc := range []struct {
		name    string
		flipAt  int32
		details string
	}{
		{"before finalization starts", 1,
			"the chunk-entry check aborts before any allocation"},
		{"mid finalization after the sort", 4,
			"the chunk result vector, the restored pair and the selector scratch exist"},
		{"mid scan while accumulating weights", 5,
			"the selector and converted-weight scratch exist"},
		{"between groups after partial output", 6,
			"group 0's selected value is already appended to the result"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, int64(0), mp.CurrNB())
			exec := makeWeightedExecForTest(t, mp, false,
				types.T_float64.ToType(), types.T_float64.ToType(), "0.5")
			require.NoError(t, exec.GroupGrow(3))
			fillWeightedThreeGroups(t, mp, exec)

			ctx := &causeAtNthCheck{
				Context: context.Background(), n: tc.flipAt, cause: cancelCause}
			results, err := exec.FlushWithContext(ctx)
			require.Error(t, err, tc.details)
			require.ErrorIs(t, err, cancelCause)
			require.Nil(t, results, "the error return must free all built results")
			require.EqualValues(t, tc.flipAt, ctx.checks, tc.details+
				"; finalization must abort on its first cancelled check")
			exec.Free()
			require.Equal(t, int64(0), mp.CurrNB(),
				"a cancelled finalization must not leak scratch or result memory")

			fresh := makeWeightedExecForTest(t, mp, false,
				types.T_float64.ToType(), types.T_float64.ToType(), "0.5")
			require.NoError(t, fresh.GroupGrow(1))
			fv, fw := buildWeightedFixtureVecs(t, mp)
			defer fv.Free(mp)
			defer fw.Free(mp)
			require.NoError(t, fresh.BulkFill(0, []*vector.Vector{fv, fw}))
			freshResults, err := fresh.Flush()
			require.NoError(t, err)
			require.Equal(t, 2.0, vector.GetFixedAtNoTypeCheck[float64](freshResults[0], 0))
			freshResults[0].Free(mp)
			fresh.Free()
		})
	}
}

// TestQuantileExactWeightedFinalizationReportsNoErrorsOnValidPayloads is the
// pre-PR review witness for the retained-pair error contract: moerr
// construction reports through errutil immediately, so a valid finalization
// must construct no error anywhere, while malformed retained payloads must
// construct exactly one reported internal error and fail recovery.
func TestQuantileExactWeightedFinalizationReportsNoErrorsOnValidPayloads(t *testing.T) {
	var reported int32
	errutil.SetErrorReporter(func(context.Context, error, int) {
		atomic.AddInt32(&reported, 1)
	})
	defer errutil.SetErrorReporter(func(context.Context, error, int) {})

	t.Run("valid DISTINCT finalization reports no errors", func(t *testing.T) {
		mp := mpool.MustNewZero()
		defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

		values, weights := buildWeightedFixtureVecs(t, mp)
		defer values.Free(mp)
		defer weights.Free(mp)

		exec := makeWeightedExecForTest(t, mp, true,
			types.T_float64.ToType(), types.T_float64.ToType(), "0.5")
		require.NoError(t, exec.GroupGrow(1))
		require.NoError(t, exec.BulkFill(0, []*vector.Vector{values, weights}))
		atomic.StoreInt32(&reported, 0)
		results, err := exec.Flush()
		require.NoError(t, err)
		require.Equal(t, 2.0, vector.GetFixedAtNoTypeCheck[float64](results[0], 0))
		require.Zero(t, atomic.LoadInt32(&reported),
			"a finalization over valid retained pairs must not construct or report any error")
		results[0].Free(mp)
		exec.Free()
	})

	t.Run("well-formed pair bytes parse silently", func(t *testing.T) {
		payload := make([]byte, 24)
		binary.BigEndian.PutUint32(payload[:4], 8)
		copy(payload[4:12], "12345678")
		binary.BigEndian.PutUint32(payload[12:16], 8)
		copy(payload[16:24], "87654321")
		atomic.StoreInt32(&reported, 0)
		valueBytes, weightBytes, err := parseQuantileExactWeightedPair(payload, 8, 8)
		require.NoError(t, err)
		require.Equal(t, []byte("12345678"), valueBytes)
		require.Equal(t, []byte("87654321"), weightBytes)
		require.Zero(t, atomic.LoadInt32(&reported))
	})

	t.Run("malformed retained payloads fail and report once", func(t *testing.T) {
		full := make([]byte, 24)
		binary.BigEndian.PutUint32(full[:4], 8)
		binary.BigEndian.PutUint32(full[12:16], 8)

		malformed := map[string][]byte{
			"truncated payload": full[:6],
			"value width mismatch": func() []byte {
				bad := append([]byte(nil), full...)
				binary.BigEndian.PutUint32(bad[:4], 4)
				return bad
			}(),
			"weight region truncated": full[:20],
		}
		for name, payload := range malformed {
			before := atomic.LoadInt32(&reported)
			_, _, err := parseQuantileExactWeightedPair(payload, 8, 8)
			require.Error(t, err, name)
			require.ErrorContains(t, err, "quantileExactWeighted has invalid retained arguments")
			require.Equal(t, before+1, atomic.LoadInt32(&reported),
				"%s: exactly the failing branch may construct the error", name)
		}
	})
}

// TestQuantileExactWeightedFlushOutputOwnership is the output-ownership
// witness from the approved design and its P3 review action: a successful
// finalize transfers the result vectors to the caller, executor destruction
// happens BEFORE the retained-result reads (the window consumer ordering at
// window.go:739/760-769), repeated reads stay valid, and caller-side release
// returns the allocation balance to zero with no double release.
func TestQuantileExactWeightedFlushOutputOwnership(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

	values := vector.NewVec(types.T_float64.ToType())
	defer values.Free(mp)
	for _, v := range []float64{2, 4, -1} {
		require.NoError(t, vector.AppendFixed(values, v, false, mp))
	}
	weights := vector.NewVec(types.T_float64.ToType())
	defer weights.Free(mp)
	for _, w := range []float64{2, 4, 2} {
		require.NoError(t, vector.AppendFixed(weights, w, false, mp))
	}

	exec := makeWeightedExecForTest(t, mp, false,
		types.T_float64.ToType(), types.T_float64.ToType(), "0.5")
	require.NoError(t, exec.GroupGrow(2))
	require.NoError(t, exec.BatchFill(0, []uint64{1, 1, 1},
		[]*vector.Vector{values, weights}))
	require.NoError(t, exec.BatchFill(0, []uint64{2, 2, 2},
		[]*vector.Vector{values, weights}))

	results, err := exec.Flush()
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, 2, results[0].Length())

	// First read of the contracted output: k = ceil(0.5*8) = 4 selects 2 for
	// both groups, including the NULL bits staying clear.
	require.False(t, results[0].IsNull(0))
	require.Equal(t, 2.0, vector.GetFixedAtNoTypeCheck[float64](results[0], 0))
	require.False(t, results[0].IsNull(1))
	require.Equal(t, 2.0, vector.GetFixedAtNoTypeCheck[float64](results[0], 1))

	// The window consumer destroys the aggregate executor before the retained
	// results are re-read by later output chunks; executor destruction must
	// leave the transferred output untouched.
	exec.Free()

	// Repeated reads after executor destruction stay valid for every
	// contracted row, values and NULL bits included.
	for read := 0; read < 2; read++ {
		require.False(t, results[0].IsNull(0))
		require.Equal(t, 2.0, vector.GetFixedAtNoTypeCheck[float64](results[0], 0))
		require.False(t, results[0].IsNull(1))
		require.Equal(t, 2.0, vector.GetFixedAtNoTypeCheck[float64](results[0], 1))
	}

	// Caller-side release is the single release of the transferred output.
	results[0].Free(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

// TestQuantileExactWeightedDispatchAndConfigGuards checks the constructor
// boundary: the flat two-data-argument dispatch contract, the typed value and
// weight admission, and the versioned level config codec's guards.
func TestQuantileExactWeightedDispatchAndConfigGuards(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

	_, err := makeQuantileExactWeightedExec(mp, AggIdOfQuantileExactWeighted,
		false, []types.Type{types.T_int64.ToType()})
	require.Error(t, err)

	_, err = makeQuantileExactWeightedExec(mp, AggIdOfQuantileExactWeighted,
		false, []types.Type{types.T_varchar.ToType(), types.T_int64.ToType()})
	require.Error(t, err)

	_, err = makeQuantileExactWeightedExec(mp, AggIdOfQuantileExactWeighted,
		false, []types.Type{types.T_int64.ToType(),
			types.New(types.T_decimal256, 40, 0)})
	require.Error(t, err)

	// The MakeAgg dispatch reaches the special executor with the plan-derived
	// two-data-argument contract.
	exec, err := MakeAgg(mp, AggIdOfQuantileExactWeighted, false,
		types.T_float64.ToType(), types.T_uint64.ToType())
	require.NoError(t, err)
	require.Equal(t, EmptyResultNull, GetEmptyResultKind(AggIdOfQuantileExactWeighted))
	require.Equal(t, []types.Type{
		types.T_float64.ToType(), types.T_uint64.ToType(),
	}, nilCheckTypes(exec))
	exec.Free()

	for _, tc := range []struct {
		name string
		raw  any
	}{
		{name: "empty", raw: []byte(nil)},
		{name: "wrong type", raw: "0.5"},
		{name: "out of range", raw: []byte("1.5")},
		{name: "negative", raw: []byte("-0.5")},
		{name: "not numeric", raw: []byte("abc")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := decodeQuantileExactWeightedConfig(tc.raw)
			require.Error(t, err)
		})
	}

	// The versioned codec round-trips the exact rational text.
	config := EncodeQuantileExactWeightedConfig([]byte("1e-100"))
	require.Equal(t, byte(quantileExactWeightedConfigVersion), config[0])
	level, err := decodeQuantileExactWeightedConfig(config)
	require.NoError(t, err)
	expected, ok := new(big.Rat).SetString("1e-100")
	require.True(t, ok)
	require.Equal(t, 0, level.Cmp(expected))
}

func nilCheckTypes(exec AggFuncExec) []types.Type {
	args, _ := exec.TypesInfo()
	return args
}
