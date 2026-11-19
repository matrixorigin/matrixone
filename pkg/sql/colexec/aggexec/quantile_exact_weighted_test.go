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
	"math/rand"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mosort "github.com/matrixorigin/matrixone/pkg/sort"
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
		// Untyped int literals would bind vector.AppendFixed's generic
		// parameter to int; the race build's vector type check rejects the
		// int cast over float64 storage, and a plain build would write int
		// bit patterns into the weights. Keep the literals float64.
		require.NoError(t, vector.AppendFixed(allNullValues, 0.0, true, mp))
	}
	weights := vector.NewVec(types.T_float64.ToType())
	defer weights.Free(mp)
	for i := 0; i < 3; i++ {
		require.NoError(t, vector.AppendFixed(weights, 1.0, false, mp))
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

// fillWeightedLargeGroupInputs builds the shared inputs of the large-group
// cancellation witness: 4096 descending values of weight 1, so a group's
// finalization performs a real multi-row restore, sort and cumulative scan
// (the descending order forces the sort's element work).
func fillWeightedLargeGroupInputs(
	t *testing.T, mp *mpool.MPool,
) (*vector.Vector, *vector.Vector) {
	t.Helper()
	values := vector.NewVec(types.T_float64.ToType())
	weights := vector.NewVec(types.T_float64.ToType())
	for i := 0; i < 4096; i++ {
		require.NoError(t, vector.AppendFixed(values, float64(4096-i), false, mp))
		require.NoError(t, vector.AppendFixed(weights, 1.0, false, mp))
	}
	return values, weights
}

// fillWeightedEqualGroupInputs builds the equal-key variant of the
// large-group cancellation witness: 4096 identical values of weight 1. A
// group's finalization still performs the real multi-row restore, sort and
// cumulative scan, but every sift-down comparison compares without swapping,
// so only a checkpoint cadence that counts no-swap work fires inside it.
func fillWeightedEqualGroupInputs(
	t *testing.T, mp *mpool.MPool,
) (*vector.Vector, *vector.Vector) {
	t.Helper()
	values := vector.NewVec(types.T_float64.ToType())
	weights := vector.NewVec(types.T_float64.ToType())
	for i := 0; i < 4096; i++ {
		require.NoError(t, vector.AppendFixed(values, 42.0, false, mp))
		require.NoError(t, vector.AppendFixed(weights, 1.0, false, mp))
	}
	return values, weights
}

// fillWeightedFollowUpGroup adds a one-row second group (value 4, weight 3)
// so a later case can cancel between groups after the large group's row is
// already appended to the result.
func fillWeightedFollowUpGroup(
	t *testing.T, mp *mpool.MPool, exec *quantileExactWeightedExec,
) {
	t.Helper()
	require.NoError(t, exec.GroupGrow(1))
	smallValues := singleRowVec(t, mp, types.T_float64.ToType(), 4.0)
	defer smallValues.Free(mp)
	smallWeights := singleRowVec(t, mp, types.T_float64.ToType(), 3.0)
	defer smallWeights.Free(mp)
	require.NoError(t, exec.BulkFill(1, []*vector.Vector{smallValues, smallWeights}))
}

// TestQuantileExactWeightedCancellation witnesses decision D12's
// cancellation points inside actual multi-row finalization work (the review
// regression for cancelling while a large group or window partition is being
// finalized): every case cancels the context at the n-th finalization check
// of a 4096-row group — mid-restore, at the sort's entry, deep inside the
// sort's element work, or mid cumulative scan — the cancelled cause must
// surface at exactly that check, and all scratch plus partially built
// results must be released. The check sequence of the 4096-row group is
// deterministic (fixed data drives a fixed sequence of elementary heap
// steps, including no-swap comparisons), and the
// warmup run learns the total T: checks 1-2 are the chunk and group-row
// entries, 3-6 the restore cadence at rows 0/1024/2048/3072, 7 the sort's
// entry, 8..T-4 the sort's bounded work checkpoints, and T-3..T the scan's
// four checkpoints (level 0.99 crosses at the 4056th cumulative weight).
func TestQuantileExactWeightedCancellation(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

	cancelCause := moerr.NewInvalidInputNoCtx("quantile test deadline")
	values, weights := fillWeightedLargeGroupInputs(t, mp)
	defer values.Free(mp)
	defer weights.Free(mp)
	baseline := mp.CurrNB()

	// countFinalizationChecks runs the successful finalization under a
	// counting context and returns how many checks it performed.
	countFinalizationChecks := func(withFollowUpGroup bool) int32 {
		exec := makeWeightedExecForTest(t, mp, false,
			types.T_float64.ToType(), types.T_float64.ToType(), "0.99")
		require.NoError(t, exec.GroupGrow(1))
		require.NoError(t, exec.BulkFill(0, []*vector.Vector{values, weights}))
		if withFollowUpGroup {
			fillWeightedFollowUpGroup(t, mp, exec)
		}
		counting := &causeAtNthCheck{
			Context: context.Background(), n: int32(math.MaxInt32)}
		results, err := exec.FlushWithContext(counting)
		require.NoError(t, err)
		// 4096 unit weights give W=4096 and k=ceil(0.99*4096)=4056, so the
		// ascending values select 4056; the second group's W=3 selects 4.
		require.Equal(t, 4056.0,
			vector.GetFixedAtNoTypeCheck[float64](results[0], 0))
		if withFollowUpGroup {
			require.Equal(t, 4.0,
				vector.GetFixedAtNoTypeCheck[float64](results[0], 1))
		}
		for _, result := range results {
			result.Free(mp)
		}
		exec.Free()
		require.Equal(t, baseline, mp.CurrNB())
		return counting.checks
	}

	totalChecks := countFinalizationChecks(false)
	require.Greater(t, totalChecks, int32(10),
		"the large group's sort must run bounded checkpoints past its entry")
	// The last four checks (T-3..T) belong to the cumulative scan, so T-5 is
	// the sort's final in-work checkpoint.
	deepSortFlip := totalChecks - 5
	require.Greater(t, deepSortFlip, int32(7),
		"the flip point must be inside the sort's element work")

	for _, tc := range []struct {
		name    string
		flipAt  int32
		details string
	}{
		{"before finalization starts", 1,
			"the chunk-entry check aborts before any allocation"},
		{"mid restore after 2048 retained rows", 5,
			"the restored value rows and converted-weight scratch exist"},
		{"at the sort's entry", 7,
			"the large group's sort has not performed element work yet"},
		{"deep inside the sort's element work", deepSortFlip,
			"the selector scratch exists mid-sort with swaps already applied"},
		{"mid scan accumulating sorted weights", totalChecks - 1,
			"the sort finished; the scan holds converted weights"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, baseline, mp.CurrNB())
			exec := makeWeightedExecForTest(t, mp, false,
				types.T_float64.ToType(), types.T_float64.ToType(), "0.99")
			require.NoError(t, exec.GroupGrow(1))
			require.NoError(t, exec.BulkFill(0, []*vector.Vector{values, weights}))

			ctx := &causeAtNthCheck{
				Context: context.Background(), n: tc.flipAt, cause: cancelCause}
			results, err := exec.FlushWithContext(ctx)
			require.Error(t, err, tc.details)
			require.ErrorIs(t, err, cancelCause)
			require.Nil(t, results, "the error return must free all built results")
			require.EqualValues(t, tc.flipAt, ctx.checks, tc.details+
				"; finalization must abort on its first cancelled check")
			exec.Free()
			require.Equal(t, baseline, mp.CurrNB(),
				"a cancelled finalization must not leak scratch or result memory")
		})
	}

	// The same witness across the group boundary: cancelling at the second
	// group's row entry aborts after the large group's selected value was
	// already appended to the chunk result.
	t.Run("between groups after partial output", func(t *testing.T) {
		require.Equal(t, baseline, mp.CurrNB())
		twoGroupChecks := countFinalizationChecks(true)
		afterFirstGroup := totalChecks + 1
		require.Greater(t, int32(afterFirstGroup), totalChecks)
		require.LessOrEqual(t, int32(afterFirstGroup), twoGroupChecks,
			"the flip must stay inside the two-group finalization")
		exec := makeWeightedExecForTest(t, mp, false,
			types.T_float64.ToType(), types.T_float64.ToType(), "0.99")
		require.NoError(t, exec.GroupGrow(1))
		require.NoError(t, exec.BulkFill(0, []*vector.Vector{values, weights}))
		fillWeightedFollowUpGroup(t, mp, exec)

		ctx := &causeAtNthCheck{
			Context: context.Background(), n: afterFirstGroup, cause: cancelCause}
		results, err := exec.FlushWithContext(ctx)
		require.Error(t, err,
			"group 0's selected value is already appended to the result")
		require.ErrorIs(t, err, cancelCause)
		require.Nil(t, results, "the error return must free all built results")
		require.EqualValues(t, afterFirstGroup, ctx.checks)
		exec.Free()
		require.Equal(t, baseline, mp.CurrNB(),
			"a cancelled finalization must not leak scratch or result memory")
	})

	// A subsequent fresh execution on a fresh executor observes no residue.
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
}

// TestQuantileExactWeightedEqualValuedCancellation is the executor-level
// equal-key cancellation regression of the review probe ("cancel the
// finalization of a large equal-valued aggregate or window partition"): a
// 4096-row group of identical values makes the finalization sort compare
// without swapping at every sift-down, and window partitions finalize
// through this same FlushWithContext path. The warmup gate proves the sort's
// no-swap work produces real checkpoints; cancelling at the sort's first
// in-work checkpoint — the exact spot where the old swap-counted cadence fell
// silent and ran the sort to completion — and deep inside its element work
// must surface the cancelled cause at the first flipped check and free
// every scratch buffer and partially built result.
func TestQuantileExactWeightedEqualValuedCancellation(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

	cancelCause := moerr.NewInvalidInputNoCtx("equal-valued test deadline")
	values, weights := fillWeightedEqualGroupInputs(t, mp)
	defer values.Free(mp)
	defer weights.Free(mp)
	baseline := mp.CurrNB()

	// The counting warmup learns the total T of the successful equal-key
	// finalization and proves the group selects its shared value.
	exec := makeWeightedExecForTest(t, mp, false,
		types.T_float64.ToType(), types.T_float64.ToType(), "0.99")
	require.NoError(t, exec.GroupGrow(1))
	require.NoError(t, exec.BulkFill(0, []*vector.Vector{values, weights}))
	counting := &causeAtNthCheck{
		Context: context.Background(), n: int32(math.MaxInt32)}
	results, err := exec.FlushWithContext(counting)
	require.NoError(t, err)
	require.Equal(t, 42.0,
		vector.GetFixedAtNoTypeCheck[float64](results[0], 0),
		"4096 equal values select that value at any weighted threshold")
	for _, result := range results {
		result.Free(mp)
	}
	exec.Free()
	require.Equal(t, baseline, mp.CurrNB())
	totalChecks := counting.checks
	// Checks 1-2 are the chunk/group-row entries, 3-6 the restore cadence
	// and 7 the sort's entry — the sequence any 4096-row group shares — and
	// the scan contributes the final four, so this floor forces real in-sort
	// checkpoints between check 7 and the scan (the old swap-counted cadence
	// produced none for equal keys).
	require.Greater(t, totalChecks, int32(15),
		"equal-valued element work must run bounded in-sort checkpoints")

	// Check 8 is the sort's first in-work checkpoint; T-5 sits deep inside
	// the sort's element work, before the scan's final four checkpoints.
	for _, tc := range []struct {
		name   string
		flipAt int32
	}{
		{"at the sort's first in-work checkpoint", 8},
		{"deep inside the sort's element work", totalChecks - 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, baseline, mp.CurrNB())
			exec := makeWeightedExecForTest(t, mp, false,
				types.T_float64.ToType(), types.T_float64.ToType(), "0.99")
			require.NoError(t, exec.GroupGrow(1))
			require.NoError(t, exec.BulkFill(0, []*vector.Vector{values, weights}))

			ctx := &causeAtNthCheck{
				Context: context.Background(), n: tc.flipAt, cause: cancelCause}
			results, err := exec.FlushWithContext(ctx)
			require.Error(t, err,
				"equal-valued element work must honor cancellation checkpoints")
			require.ErrorIs(t, err, cancelCause)
			require.Nil(t, results, "the error return must free all built results")
			require.EqualValues(t, tc.flipAt, ctx.checks,
				"finalization must abort on its first cancelled check")
			exec.Free()
			require.Equal(t, baseline, mp.CurrNB(),
				"a cancelled finalization must not leak scratch or result memory")
		})
	}

	// A fresh executor over the same equal-valued group observes no residue.
	fresh := makeWeightedExecForTest(t, mp, false,
		types.T_float64.ToType(), types.T_float64.ToType(), "0.5")
	require.NoError(t, fresh.GroupGrow(1))
	require.NoError(t, fresh.BulkFill(0, []*vector.Vector{values, weights}))
	freshResults, err := fresh.Flush()
	require.NoError(t, err)
	require.Equal(t, 42.0, vector.GetFixedAtNoTypeCheck[float64](freshResults[0], 0))
	freshResults[0].Free(mp)
	fresh.Free()
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

// TestQuantileSortSelectorsInterruptsInsideElementWork pins decision D12's
// in-sort cancellation on the executor's sort helper: an already-cancelled
// context stops the sort before its first element move (a plain SortByVectors
// would run the whole sort first), and the bounded swap checkpoint stops a
// real sort mid-work with genuine element swaps already applied. Ordering
// correctness under a live context is covered by
// TestQuantileSortSelectorsMatchesSortByVectors.
func TestQuantileSortSelectorsInterruptsInsideElementWork(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

	const rows = 1 << 16
	values := vector.NewVec(types.T_int64.ToType())
	defer values.Free(mp)
	for i := 0; i < rows; i++ {
		require.NoError(t, vector.AppendFixed(values, int64(rows-i), false, mp))
	}
	identity := func() []int64 {
		selectors := make([]int64, rows)
		for i := range selectors {
			selectors[i] = int64(i)
		}
		return selectors
	}
	// Descending values mean a completed ascending sort is exactly the
	// reversed selector sequence.
	reversed := make([]int64, rows)
	for i := range reversed {
		reversed[i] = int64(rows - 1 - i)
	}

	// A pre-cancelled context surfaces at the sort's entry checkpoint
	// before any selector moves.
	cancelCause := moerr.NewInvalidInputNoCtx("sort deadline")
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(cancelCause)
	selectors := identity()
	err := quantileSortSelectors(ctx, values, selectors)
	require.ErrorIs(t, err, cancelCause)
	require.Equal(t, identity(), selectors,
		"a cancelled context must stop the sort before its first element move")

	// The checkpoint cadence counts every elementary heap step, so with
	// descending values the first in-sort checkpoints fire during the heap
	// build, which compares without swapping. A counting warmup learns the
	// total T of the completed sort, and flipping at T/2+1 aborts deep
	// inside the extraction phase: real element moves have happened yet the
	// completed order was never reached.
	counting := &causeAtNthCheck{
		Context: context.Background(), n: int32(math.MaxInt32)}
	selectors = identity()
	require.NoError(t, quantileSortSelectors(counting, values, selectors))
	require.Equal(t, reversed, selectors,
		"a completed sort over descending values selects the reversed order")
	totalChecks := counting.checks
	require.Greater(t, totalChecks, int32(4),
		"descending element work must produce bounded in-sort checkpoints")
	deepFlip := totalChecks/2 + 1
	require.Less(t, deepFlip, totalChecks)

	deep := &causeAtNthCheck{
		Context: context.Background(), n: deepFlip, cause: cancelCause}
	selectors = identity()
	err = quantileSortSelectors(deep, values, selectors)
	require.ErrorIs(t, err, cancelCause)
	require.EqualValues(t, deepFlip, deep.checks,
		"the sort must abort on its first cancelled check")
	require.NotEqual(t, identity(), selectors,
		"the mid-sort checkpoint must fire after real element swaps")
	require.NotEqual(t, reversed, selectors,
		"the aborted sort must not read like a completed sort")
}

// TestQuantileSortSelectorsInterruptsEqualValuedWork is the equal-key
// cancellation regression of the review probe: with 65,536 identical values
// every sift-down comparison returns without a swap and root-extraction
// swaps were uncounted as well, so the old checkpoint cadence counted only
// element swaps and never fired — the sort performed all 65,536 root
// extractions and returned nil while the context was already cancelled. The
// cadence now counts every elementary heap iteration: an already-cancelled
// context surfaces at the first in-sort checkpoint, before any element
// move, and a later flip aborts the extraction phase mid-work instead of
// completing it.
func TestQuantileSortSelectorsInterruptsEqualValuedWork(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

	const rows = 1 << 16
	values := vector.NewVec(types.T_int64.ToType())
	defer values.Free(mp)
	for i := 0; i < rows; i++ {
		require.NoError(t, vector.AppendFixed(values, int64(29944), false, mp))
	}
	identity := func() []int64 {
		selectors := make([]int64, rows)
		for i := range selectors {
			selectors[i] = int64(i)
		}
		return selectors
	}
	cancelCause := moerr.NewInvalidInputNoCtx("equal-valued sort deadline")

	// The counting warmup completes the sort with a live context, learning
	// both the total checkpoint count of the equal-key work and the
	// completed permutation of the deterministic heap sequence.
	counting := &causeAtNthCheck{
		Context: context.Background(), n: int32(math.MaxInt32)}
	finished := identity()
	require.NoError(t, quantileSortSelectors(counting, values, finished))
	totalChecks := counting.checks
	require.Greater(t, totalChecks, int32(10),
		"equal-valued element work must produce bounded in-sort checkpoints")

	// Flipping at the first in-sort checkpoint aborts during the heap build,
	// which for equal keys compares without swapping — the exact spot where
	// the swap-counted cadence fell silent and ran the full 65,536-move sort
	// to completion under an already-cancelled context.
	early := &causeAtNthCheck{
		Context: context.Background(), n: 2, cause: cancelCause}
	selectors := identity()
	err := quantileSortSelectors(early, values, selectors)
	require.ErrorIs(t, err, cancelCause,
		"an already-cancelled context must surface at the first in-sort checkpoint")
	require.EqualValues(t, 2, early.checks,
		"the sort must abort on its first cancelled check")
	require.Equal(t, identity(), selectors,
		"equal-valued heap builds compare without swapping, so the abort precedes every element move")

	// Flipping deep inside the extraction phase aborts mid-work: real
	// root-extraction moves have happened, yet the permutation is neither
	// the identity nor the completed order.
	deepFlip := totalChecks/2 + 1
	require.Less(t, deepFlip, totalChecks)
	deep := &causeAtNthCheck{
		Context: context.Background(), n: deepFlip, cause: cancelCause}
	selectors = identity()
	err = quantileSortSelectors(deep, values, selectors)
	require.ErrorIs(t, err, cancelCause)
	require.EqualValues(t, deepFlip, deep.checks,
		"the sort must abort on its first cancelled check")
	require.NotEqual(t, identity(), selectors,
		"the flip point sits inside the extraction phase, after real element moves")
	require.NotEqual(t, finished, selectors,
		"the aborted sort must not read like a completed sort")
}

// TestQuantileSortSelectorsMatchesSortByVectors proves the context-aware
// sort produces the identical row ordering the repository's selector-sort
// authority (SortByVectors over one vector) produces for every supported
// value type, including edge values (NaN and ±Inf payloads, -0.0/+0.0,
// width extremes and duplicates), so the cancellation-aware finalization
// never changes which value a finalized group selects.
func TestQuantileSortSelectorsMatchesSortByVectors(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

	rng := rand.New(rand.NewSource(29944))

	// Each case returns a value vector whose rows mix typed extremes with
	// seeded duplicates.
	decimal128Type := types.New(types.T_decimal128, 37, 4)
	builders := []struct {
		name string
		env  func(t *testing.T) *vector.Vector
	}{
		{"int8", func(t *testing.T) *vector.Vector {
			vec := vector.NewVec(types.T_int8.ToType())
			for i := 0; i < 517; i++ {
				value := int8(rng.Intn(251) - 125)
				if i == 0 {
					value = math.MinInt8
				} else if i == 1 {
					value = math.MaxInt8
				}
				require.NoError(t, vector.AppendFixed(vec, value, false, mp))
			}
			return vec
		}},
		{"int16", func(t *testing.T) *vector.Vector {
			vec := vector.NewVec(types.T_int16.ToType())
			for i := 0; i < 517; i++ {
				var value int16
				switch {
				case i == 0:
					value = math.MinInt16
				case i == 1:
					value = math.MaxInt16
				default:
					value = int16(rng.Intn(65536) - 32768)
				}
				require.NoError(t, vector.AppendFixed(vec, value, false, mp))
			}
			return vec
		}},
		{"int32", func(t *testing.T) *vector.Vector {
			vec := vector.NewVec(types.T_int32.ToType())
			for i := 0; i < 517; i++ {
				value := int32(rng.Int31())
				if i&7 == 0 {
					value = int32(rng.Int31n(97) - 48)
				}
				require.NoError(t, vector.AppendFixed(vec, value, false, mp))
			}
			return vec
		}},
		{"int64", func(t *testing.T) *vector.Vector {
			vec := vector.NewVec(types.T_int64.ToType())
			for i := 0; i < 517; i++ {
				value := rng.Int63()
				if i == 0 {
					value = math.MinInt64
				} else if i == 1 {
					value = math.MaxInt64
				} else if i&7 == 0 {
					value = rng.Int63n(97) - 48
				}
				require.NoError(t, vector.AppendFixed(vec, value, false, mp))
			}
			return vec
		}},
		{"uint8", func(t *testing.T) *vector.Vector {
			vec := vector.NewVec(types.T_uint8.ToType())
			for i := 0; i < 517; i++ {
				value := uint8(rng.Intn(256))
				if i == 0 {
					value = math.MaxUint8
				}
				require.NoError(t, vector.AppendFixed(vec, value, false, mp))
			}
			return vec
		}},
		{"uint16", func(t *testing.T) *vector.Vector {
			vec := vector.NewVec(types.T_uint16.ToType())
			for i := 0; i < 517; i++ {
				value := uint16(rng.Intn(65536))
				if i == 0 {
					value = math.MaxUint16
				}
				require.NoError(t, vector.AppendFixed(vec, value, false, mp))
			}
			return vec
		}},
		{"uint32", func(t *testing.T) *vector.Vector {
			vec := vector.NewVec(types.T_uint32.ToType())
			for i := 0; i < 517; i++ {
				value := uint32(rng.Int31())
				if i == 0 {
					value = math.MaxUint32
				}
				require.NoError(t, vector.AppendFixed(vec, value, false, mp))
			}
			return vec
		}},
		{"uint64 and bit rows", func(t *testing.T) *vector.Vector {
			vec := vector.NewVec(types.T_uint64.ToType())
			for i := 0; i < 517; i++ {
				value := rng.Uint64()
				if i == 0 {
					value = math.MaxUint64
				} else if i&15 == 0 {
					value = rng.Uint64() & 0xff
				}
				require.NoError(t, vector.AppendFixed(vec, value, false, mp))
			}
			return vec
		}},
		{"float32", func(t *testing.T) *vector.Vector {
			vec := vector.NewVec(types.T_float32.ToType())
			for _, value := range []float32{
				float32(math.NaN()), float32(math.Inf(-1)),
				float32(math.Inf(1)), -0.0, 0.0,
				float32(math.NaN()), -0.0,
			} {
				require.NoError(t, vector.AppendFixed(vec, value, false, mp))
			}
			for i := 0; i < 510; i++ {
				require.NoError(t, vector.AppendFixed(vec,
					float32(rng.NormFloat64())*1e6, false, mp))
			}
			return vec
		}},
		{"float64", func(t *testing.T) *vector.Vector {
			vec := vector.NewVec(types.T_float64.ToType())
			for _, value := range []float64{
				math.NaN(), math.Inf(-1), math.Inf(1),
				math.Float64frombits(0xfff8000000000001), // NaN with a high payload
				-0.0, 0.0, -0.0, math.NaN(),
			} {
				require.NoError(t, vector.AppendFixed(vec, value, false, mp))
			}
			for i := 0; i < 509; i++ {
				require.NoError(t, vector.AppendFixed(vec,
					rng.NormFloat64()*1e20, false, mp))
			}
			return vec
		}},
		{"decimal64", func(t *testing.T) *vector.Vector {
			typ := types.New(types.T_decimal64, 10, 2)
			vec := vector.NewVec(typ)
			for _, text := range []string{
				"-99999999.99", "-1.25", "0.00", "0.00", "0.01", "1.25",
				"1.25", "99999999.99",
			} {
				value, err := types.ParseDecimal64(text, 10, 2)
				require.NoError(t, err)
				require.NoError(t, vector.AppendFixed(vec, value, false, mp))
			}
			for i := 0; i < 508; i++ {
				value, err := types.ParseDecimal64(
					strconv.FormatInt(rng.Int63n(10000000)-5000000, 10)+".99",
					10, 2)
				require.NoError(t, err)
				require.NoError(t, vector.AppendFixed(vec, value, false, mp))
			}
			return vec
		}},
		{"decimal128", func(t *testing.T) *vector.Vector {
			vec := vector.NewVec(decimal128Type)
			for _, text := range []string{
				"-999999999999999999999999999999.9999",
				"-1.5", "0", "0.0000", "0", "-1.5",
				"999999999999999999999999999999.9999",
			} {
				value, err := types.ParseDecimal128(text, 37, 4)
				require.NoError(t, err)
				require.NoError(t, vector.AppendFixed(vec, value, false, mp))
			}
			for i := 0; i < 510; i++ {
				value := types.Decimal128FromInt64(rng.Int63n(1<<40) - 1<<39)
				require.NoError(t, vector.AppendFixed(vec, value, false, mp))
			}
			return vec
		}},
	}

	for _, builder := range builders {
		t.Run(builder.name, func(t *testing.T) {
			values := builder.env(t)
			defer values.Free(mp)
			rows := values.Length()
			mine := make([]int64, rows)
			repo := make([]int64, rows)
			for i := range mine {
				mine[i], repo[i] = int64(i), int64(i)
			}
			require.NoError(t,
				quantileSortSelectors(context.Background(), values, mine))
			mosort.SortByVectors(repo, []*vector.Vector{values},
				[]bool{false}, []bool{false})
			less := quantileValueLessFunc(values)
			require.NotNil(t, less, "the type must be sortable")
			for k := 1; k < rows; k++ {
				require.False(t, less(mine[k], mine[k-1]),
					"the executor sort must be ascending")
				require.False(t, less(repo[k], repo[k-1]),
					"the reference sort must be ascending")
			}
			// Both sequences order the same multiset, so corresponding
			// positions hold the same value up to equal-comparing ties
			// (NaN payloads, -0.0/+0.0, or duplicate rows).
			for k := 0; k < rows; k++ {
				require.True(t, !less(mine[k], repo[k]) && !less(repo[k], mine[k]),
					"row %d disagrees with the repository sort", k)
			}
		})
	}
}

// peakScratchController observes every physical capacity acquisition the
// aggregate makes through its allocation account. Once armed it measures
// the temporary footprint finalization adds above the armed baseline (the
// retained state was already accounted before arming), recording the
// peak; with enforce enabled it denies any acquisition that would exceed
// the armed budget, turning the measured budget into an admission witness
// for large groups.
type peakScratchController struct {
	armed   bool
	budget  uint64
	enforce bool
	denied  bool
	window  uint64
	peak    uint64
}

func (c *peakScratchController) arm(budget uint64, enforce bool) {
	c.armed = true
	c.budget = budget
	c.enforce = enforce
	c.denied = false
	c.window = 0
	c.peak = 0
}

func (c *peakScratchController) disarm() { c.armed = false }

func (c *peakScratchController) AcquireAllocationCapacity(size uint64) error {
	if !c.armed {
		return nil
	}
	if c.enforce && c.window+size > c.budget {
		c.denied = true
		return mpool.ErrAllocationAccountCapacity
	}
	c.window += size
	if c.peak < c.window {
		c.peak = c.window
	}
	return nil
}

func (c *peakScratchController) ReleaseAllocationCapacity(size uint64) {
	if !c.armed {
		return
	}
	if c.window < size {
		c.window = 0
		return
	}
	c.window -= size
}

// TestQuantileExactWeightedFinalizeScratchHonorsDesignBudget proves decision
// D12's resource model on large real groups with the review's witness
// arithmetic: per-group finalization holds at most the restored value rows
// plus the 8-byte-per-row sort selectors and converted collapse weights —
// (valueWidth + 16) × N bytes plus the result-vector and bitmap tail — and
// never a second pair-payload copy (which would add weightWidth × N). A
// measured run bounds the peak and proves the three buffers really are
// simultaneous; an admission run then finalizes the same large group under
// an account that denies any byte beyond that budget, and the finalization
// still succeeds. Window partitions finalize through the same executor
// path, so the same budget governs them.
func TestQuantileExactWeightedFinalizeScratchHonorsDesignBudget(t *testing.T) {
	decimalValues := types.New(types.T_decimal128, 37, 0)
	decimalWeights := types.New(types.T_decimal128, 38, 0)
	for _, tc := range []struct {
		name       string
		valueType  types.Type
		weightType types.Type
	}{
		{"int64 values, uint64 weights",
			types.T_int64.ToType(), types.T_uint64.ToType()},
		{"int64 values, decimal128 weights",
			types.T_int64.ToType(), decimalWeights},
		{"decimal128 values, decimal128 weights",
			decimalValues, decimalWeights},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const rows = 8192
			// Structural tail: the restored rows' null bitmap, the per-chunk
			// result vector and allocation headers.
			budget := uint64((tc.valueType.TypeSize()+16)*rows) + 8<<10

			run := func(enforce bool) (*peakScratchController, error) {
				mp := mpool.MustNewZero()
				defer mpool.DeleteMPool(mp)
				controller := &peakScratchController{}
				registry, err := mpool.NewAllocationAccountRegistry(1, 512)
				require.NoError(t, err)
				account, err := registry.OpenWithController(128<<20, controller)
				require.NoError(t, err)
				allocation, err := NewAllocationAccount(account,
					mpool.AllocationOwnerGroup, AllocationAccountSites{
						VectorData: 1, VectorArea: 2, VectorNulls: 3,
						VectorGrouping: 4, ArgumentCount: 5, ArgumentArena: 6})
				require.NoError(t, err)
				exec, err := MakeGroupAgg(mp, AggIdOfQuantileExactWeighted, false,
					allocation, EncodeQuantileExactWeightedConfig([]byte("0.5")),
					tc.valueType, tc.weightType)
				require.NoError(t, err)
				SyncAggregatorsToChunkSize([]AggFuncExec{exec}, AggBatchSize)
				require.NoError(t, exec.GroupGrow(1))

				// values 1..rows in descending input order, weight 1 each.
				valueVec := vector.NewVec(tc.valueType)
				weightVec := vector.NewVec(tc.weightType)
				switch tc.valueType.Oid {
				case types.T_int64:
					vals := make([]int64, rows)
					for i := range vals {
						vals[i] = int64(rows - i)
					}
					require.NoError(t, vector.AppendFixedList(
						valueVec, vals, nil, mp))
				case types.T_decimal128:
					vals := make([]types.Decimal128, rows)
					for i := range vals {
						vals[i] = types.Decimal128FromInt64(int64(rows - i))
					}
					require.NoError(t, vector.AppendFixedList(
						valueVec, vals, nil, mp))
				}
				switch tc.weightType.Oid {
				case types.T_uint64:
					weights := make([]uint64, rows)
					for i := range weights {
						weights[i] = 1
					}
					require.NoError(t, vector.AppendFixedList(
						weightVec, weights, nil, mp))
				case types.T_decimal128:
					weights := make([]types.Decimal128, rows)
					for i := range weights {
						weights[i] = types.Decimal128FromInt64(1)
					}
					require.NoError(t, vector.AppendFixedList(
						weightVec, weights, nil, mp))
				}
				require.NoError(t, exec.BulkFill(
					0, []*vector.Vector{valueVec, weightVec}))
				valueVec.Free(mp)
				weightVec.Free(mp)

				controller.arm(budget, enforce)
				results, flushErr := exec.Flush()
				controller.disarm()
				if flushErr == nil {
					// W=rows with weight 1 each and k=rows/2 select the
					// 4096th smallest of the values 1..rows.
					switch tc.valueType.Oid {
					case types.T_int64:
						require.Equal(t, int64(rows>>1),
							vector.GetFixedAtNoTypeCheck[int64](results[0], 0))
					case types.T_decimal128:
						selected := vector.GetFixedAtNoTypeCheck[types.Decimal128](results[0], 0)
						require.Equal(t, uint64(rows>>1), selected.B0_63)
						require.Zero(t, selected.B64_127)
					}
				}
				for _, result := range results {
					result.Free(mp)
				}
				exec.Free()
				require.NoError(t, exec.ClearAllocationAccount(allocation))
				require.Zero(t, account.Snapshot().Used)
				account.Seal()
				_, err = registry.Finalize(account)
				require.NoError(t, err)
				require.Zero(t, mp.CurrNB())
				return controller, flushErr
			}

			measured, err := run(false)
			require.NoError(t, err)
			require.False(t, measured.denied)
			require.GreaterOrEqual(t, measured.peak,
				uint64((tc.valueType.TypeSize()+16)*rows))
			require.LessOrEqual(t, measured.peak, budget,
				"finalization must not hold a second pair-payload copy")

			admitted, err := run(true)
			require.NoError(t, err,
				"the design budget must admit a large group's finalization")
			require.False(t, admitted.denied,
				"the enforced budget witnessed a request beyond the design model")
		})
	}
}
