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
	"math/big"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestOrderedPercentileExecNumericAndDirection(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

	valueVec := buildFixedVec(t, mp, types.T_int64.ToType(), []int64{1, 2, 3, 4, 5})
	defer valueVec.Free(mp)

	cont, err := makeOrderedPercentileExec(mp, AggIdOfPercentileCont, false,
		types.T_int64.ToType(), orderedPercentileContinuous)
	require.NoError(t, err)
	require.NoError(t, cont.GroupGrow(1))
	require.NoError(t, cont.SetExtraInformation(EncodeOrderedPercentileConfig([]byte("0.25"), false), 0))
	require.NoError(t, cont.BulkFill(0, []*vector.Vector{valueVec}))
	result, err := cont.Flush()
	require.NoError(t, err)
	require.Equal(t, 2.0, vector.GetFixedAtNoTypeCheck[float64](result[0], 0))
	result[0].Free(mp)
	cont.Free()

	disc, err := makeOrderedPercentileExec(mp, AggIdOfPercentileDisc, false,
		types.T_int64.ToType(), orderedPercentileDiscrete)
	require.NoError(t, err)
	require.NoError(t, disc.GroupGrow(1))
	require.NoError(t, disc.SetExtraInformation(EncodeOrderedPercentileConfig([]byte("0.5"), true), 0))
	require.NoError(t, disc.BulkFill(0, []*vector.Vector{valueVec}))
	result, err = disc.Flush()
	require.NoError(t, err)
	// DESC makes p=0.5 select the third value from the high end.
	require.Equal(t, int64(3), vector.GetFixedAtNoTypeCheck[int64](result[0], 0))
	result[0].Free(mp)
	disc.Free()
}

func TestOrderedPercentileUsesNativeUint64Order(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

	valueVec := buildFixedVec(t, mp, types.T_uint64.ToType(), []uint64{
		9007199254740993,
		9007199254740992,
	})
	defer valueVec.Free(mp)

	for _, tc := range []struct {
		name string
		desc bool
		want uint64
	}{
		{name: "ascending", want: 9007199254740992},
		{name: "descending", desc: true, want: 9007199254740993},
	} {
		t.Run(tc.name, func(t *testing.T) {
			disc, err := makeOrderedPercentileExec(mp, AggIdOfPercentileDisc, false,
				types.T_uint64.ToType(), orderedPercentileDiscrete)
			require.NoError(t, err)
			require.NoError(t, disc.GroupGrow(1))
			require.NoError(t, disc.SetExtraInformation(
				EncodeOrderedPercentileConfig([]byte("0"), tc.desc), 0))
			require.NoError(t, disc.BulkFill(0, []*vector.Vector{valueVec}))
			result, err := disc.Flush()
			require.NoError(t, err)
			require.Equal(t, tc.want, vector.GetFixedAtNoTypeCheck[uint64](result[0], 0))
			result[0].Free(mp)
			disc.Free()
		})
	}
}

func TestOrderedPercentileExecGroupsNullsAndMerge(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

	left, err := makeOrderedPercentileExec(mp, AggIdOfPercentileCont, false,
		types.T_int64.ToType(), orderedPercentileContinuous)
	require.NoError(t, err)
	right, err := makeOrderedPercentileExec(mp, AggIdOfPercentileCont, false,
		types.T_int64.ToType(), orderedPercentileContinuous)
	require.NoError(t, err)
	require.NoError(t, left.GroupGrow(2))
	require.NoError(t, right.GroupGrow(2))
	config := EncodeOrderedPercentileConfig([]byte("0.5"), false)
	require.NoError(t, left.SetExtraInformation(config, 0))
	require.NoError(t, right.SetExtraInformation(config, 0))

	leftVec := buildFixedVec(t, mp, types.T_int64.ToType(), []int64{1, 0, 10, 0})
	leftVec.SetNull(1)
	leftVec.SetNull(3)
	rightVec := buildFixedVec(t, mp, types.T_int64.ToType(), []int64{3, 5, 30})
	defer leftVec.Free(mp)
	defer rightVec.Free(mp)
	require.NoError(t, left.BatchFill(0, []uint64{1, 1, 2, GroupNotMatched}, []*vector.Vector{leftVec}))
	require.NoError(t, right.BatchFill(0, []uint64{1, 2, 2}, []*vector.Vector{rightVec}))
	require.NoError(t, left.BatchMerge(right, 0, []uint64{1, 2}))

	result, err := left.Flush()
	require.NoError(t, err)
	// Group 1 has [1,3], group 2 has [5,10,30] after NULLs are ignored.
	require.Equal(t, 2.0, vector.GetFixedAtNoTypeCheck[float64](result[0], 0))
	require.Equal(t, 10.0, vector.GetFixedAtNoTypeCheck[float64](result[0], 1))
	result[0].Free(mp)
	left.Free()
	right.Free()
}

func TestOrderedPercentileConfigValidationAndMerge(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

	newExec := func() *orderedPercentileExec[int64, float64] {
		exec, err := makeOrderedPercentileExec(mp, AggIdOfPercentileCont, false,
			types.T_int64.ToType(), orderedPercentileContinuous)
		require.NoError(t, err)
		return exec.(*orderedPercentileExec[int64, float64])
	}

	t.Run("legacy and versioned configs", func(t *testing.T) {
		legacy := newExec()
		require.NoError(t, legacy.SetExtraInformation([]byte("0.25"), 0))
		require.False(t, legacy.descending)
		require.Equal(t, "1/4", legacy.percentile.RatString())
		legacy.Free()

		versioned := newExec()
		require.NoError(t, versioned.SetExtraInformation(
			EncodeOrderedPercentileConfig([]byte("0.75"), true), 0))
		require.True(t, versioned.descending)
		require.Equal(t, "3/4", versioned.percentile.RatString())
		versioned.Free()
	})

	for _, tc := range []struct {
		name   string
		config any
	}{
		{name: "wrong config type", config: "0.5"},
		{name: "empty", config: EncodeOrderedPercentileConfig(nil, false)},
		{name: "invalid direction", config: []byte{orderedPercentileConfigVersion, 2, '0'}},
		{name: "below range", config: EncodeOrderedPercentileConfig([]byte("-0.1"), false)},
		{name: "above range", config: EncodeOrderedPercentileConfig([]byte("1.1"), false)},
		{name: "not a float", config: EncodeOrderedPercentileConfig([]byte("1/2"), false)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exec := newExec()
			require.Error(t, exec.SetExtraInformation(tc.config, 0))
			exec.Free()
		})
	}

	t.Run("flush requires config", func(t *testing.T) {
		exec := newExec()
		require.NoError(t, exec.GroupGrow(1))
		_, err := exec.Flush()
		require.Error(t, err)
		exec.Free()
	})

	t.Run("merge rejects different settings", func(t *testing.T) {
		left, right := newExec(), newExec()
		require.NoError(t, left.SetExtraInformation(
			EncodeOrderedPercentileConfig([]byte("0.5"), false), 0))
		require.NoError(t, right.SetExtraInformation(
			EncodeOrderedPercentileConfig([]byte("0.75"), false), 0))
		require.Error(t, left.Merge(right, 0, 0))
		require.Error(t, left.BatchMerge(right, 0, nil))
		left.Free()
		right.Free()

		left, right = newExec(), newExec()
		require.NoError(t, left.SetExtraInformation(
			EncodeOrderedPercentileConfig([]byte("0.5"), false), 0))
		require.NoError(t, right.SetExtraInformation(
			EncodeOrderedPercentileConfig([]byte("0.5"), true), 0))
		require.Error(t, left.Merge(right, 0, 0))
		require.Error(t, left.BatchMerge(right, 0, nil))
		left.Free()
		right.Free()
	})
}

func TestOrderedPercentileTypeDispatchAndMath(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

	for _, oid := range []types.T{
		types.T_bit, types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64, types.T_decimal64, types.T_decimal128,
	} {
		for _, mode := range []orderedPercentileMode{orderedPercentileContinuous, orderedPercentileDiscrete} {
			exec, err := makeOrderedPercentileExec(mp, AggIdOfPercentileCont, false,
				oid.ToType(), mode)
			require.NoError(t, err, "oid=%s mode=%d", oid, mode)
			exec.Free()
		}
	}
	_, err := makeOrderedPercentileExec(mp, AggIdOfPercentileCont, true,
		types.T_int64.ToType(), orderedPercentileContinuous)
	require.Error(t, err)
	_, err = makeOrderedPercentileExec(mp, AggIdOfPercentileCont, false,
		types.T_varchar.ToType(), orderedPercentileContinuous)
	require.Error(t, err)

	require.Equal(t, types.T_float64, PercentileContReturnType(nil).Oid)
	require.Equal(t, types.T_decimal128,
		PercentileContReturnType([]types.Type{types.New(types.T_decimal64, 10, 2)}).Oid)
	require.Equal(t, int32(3), PercentileContReturnType(
		[]types.Type{types.New(types.T_decimal64, 10, 2)}).Scale)
	require.Equal(t, types.T_int64,
		PercentileDiscReturnType([]types.Type{types.T_int64.ToType()}).Oid)

	frac := big.NewRat(1, 2)
	require.Equal(t, 1.5, interpolateOrderedNumericValue(int8(1), int8(2), frac))
	require.Equal(t, 1.5, interpolateOrderedNumericValue(int16(1), int16(2), frac))
	require.Equal(t, 1.5, interpolateOrderedNumericValue(int32(1), int32(2), frac))
	require.Equal(t, 1.5, interpolateOrderedNumericValue(int64(1), int64(2), frac))
	require.Equal(t, 1.5, interpolateOrderedNumericValue(uint8(1), uint8(2), frac))
	require.Equal(t, 1.5, interpolateOrderedNumericValue(uint16(1), uint16(2), frac))
	require.Equal(t, 1.5, interpolateOrderedNumericValue(uint32(1), uint32(2), frac))
	require.Equal(t, 1.5, interpolateOrderedNumericValue(uint64(1), uint64(2), frac))
	require.Equal(t, 1.5, interpolateOrderedNumericValue(float32(1), float32(2), frac))
	require.Equal(t, 1.5, interpolateOrderedNumericValue(float64(1), float64(2), frac))

	lo, hi, gotFrac := orderedPercentileRanks(5, big.NewRat(1, 4), orderedPercentileContinuous)
	require.Equal(t, uint64(1), lo)
	require.Equal(t, uint64(2), hi)
	require.Zero(t, gotFrac.Sign())
	lo, hi, gotFrac = orderedPercentileRanks(5, big.NewRat(1, 2), orderedPercentileDiscrete)
	require.Equal(t, uint64(2), lo)
	require.Equal(t, lo, hi)
	require.Zero(t, gotFrac.Sign())
	lo, hi, _ = orderedPercentileRanks(5, new(big.Rat), orderedPercentileDiscrete)
	require.Equal(t, uint64(0), lo)
	require.Equal(t, lo, hi)
	lo, hi, gotFrac = orderedPercentileRanks(1, big.NewRat(1, 2), orderedPercentileContinuous)
	require.Equal(t, uint64(0), lo)
	require.Equal(t, lo, hi)
	require.Zero(t, gotFrac.Sign())

	ascending, err := sortOrderedPercentileValues(mp, types.T_int64.ToType(), []int64{3, 1, 2}, false)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2, 0}, ascending)
	descending, err := sortOrderedPercentileValues(mp, types.T_int64.ToType(), []int64{3, 1, 2}, true)
	require.NoError(t, err)
	require.Equal(t, []int64{0, 2, 1}, descending)
	single, err := sortOrderedPercentileValues(mp, types.T_int64.ToType(), []int64{7}, false)
	require.NoError(t, err)
	require.Equal(t, []int64{0}, single)
}

func TestOrderedPercentileDecimalExecution(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

	t.Run("decimal64 continuous", func(t *testing.T) {
		typ := types.New(types.T_decimal64, 10, 2)
		one, err := types.ParseDecimal64("1.00", typ.Width, typ.Scale)
		require.NoError(t, err)
		three, err := types.ParseDecimal64("3.00", typ.Width, typ.Scale)
		require.NoError(t, err)
		vec := buildFixedVec(t, mp, typ, []types.Decimal64{one, three})
		defer vec.Free(mp)
		exec, err := makeOrderedPercentileExec(mp, AggIdOfPercentileCont, false, typ, orderedPercentileContinuous)
		require.NoError(t, err)
		require.NoError(t, exec.GroupGrow(1))
		require.NoError(t, exec.SetExtraInformation(EncodeOrderedPercentileConfig([]byte("0.5"), false), 0))
		require.NoError(t, exec.BulkFill(0, []*vector.Vector{vec}))
		result, err := exec.Flush()
		require.NoError(t, err)
		require.Equal(t, "2.000", vector.GetFixedAtNoTypeCheck[types.Decimal128](result[0], 0).Format(3))
		result[0].Free(mp)
		exec.Free()
	})

	t.Run("decimal128 discrete", func(t *testing.T) {
		typ := types.New(types.T_decimal128, 38, 2)
		one, err := types.ParseDecimal128("1.00", typ.Width, typ.Scale)
		require.NoError(t, err)
		three, err := types.ParseDecimal128("3.00", typ.Width, typ.Scale)
		require.NoError(t, err)
		vec := buildFixedVec(t, mp, typ, []types.Decimal128{one, three})
		defer vec.Free(mp)
		exec, err := makeOrderedPercentileExec(mp, AggIdOfPercentileDisc, false, typ, orderedPercentileDiscrete)
		require.NoError(t, err)
		require.NoError(t, exec.GroupGrow(1))
		require.NoError(t, exec.SetExtraInformation(EncodeOrderedPercentileConfig([]byte("0.5"), false), 0))
		require.NoError(t, exec.BulkFill(0, []*vector.Vector{vec}))
		result, err := exec.Flush()
		require.NoError(t, err)
		require.Equal(t, "1.00", vector.GetFixedAtNoTypeCheck[types.Decimal128](result[0], 0).Format(2))
		result[0].Free(mp)
		exec.Free()
	})

	t.Run("decimal128 continuous", func(t *testing.T) {
		typ := types.New(types.T_decimal128, 38, 2)
		one, err := types.ParseDecimal128("1.00", typ.Width, typ.Scale)
		require.NoError(t, err)
		three, err := types.ParseDecimal128("3.00", typ.Width, typ.Scale)
		require.NoError(t, err)
		vec := buildFixedVec(t, mp, typ, []types.Decimal128{one, three})
		defer vec.Free(mp)
		exec, err := makeOrderedPercentileExec(mp, AggIdOfPercentileCont, false, typ, orderedPercentileContinuous)
		require.NoError(t, err)
		require.NoError(t, exec.GroupGrow(1))
		require.NoError(t, exec.SetExtraInformation(EncodeOrderedPercentileConfig([]byte("0.5"), false), 0))
		require.NoError(t, exec.BulkFill(0, []*vector.Vector{vec}))
		result, err := exec.Flush()
		require.NoError(t, err)
		require.Equal(t, "2.000", vector.GetFixedAtNoTypeCheck[types.Decimal128](result[0], 0).Format(3))
		result[0].Free(mp)
		exec.Free()
	})
}

func TestMakeSpecialOrderedPercentileExec(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

	for _, id := range []int64{AggIdOfPercentileCont, AggIdOfPercentileDisc} {
		exec, ok, err := makeSpecialAggExec(mp, id, false, types.T_int64.ToType())
		require.True(t, ok)
		require.NoError(t, err)
		require.NotNil(t, exec)
		exec.Free()

		_, ok, err = makeSpecialAggExec(mp, id, false)
		require.True(t, ok)
		require.Error(t, err)
		_, ok, err = makeSpecialAggExec(mp, id, false, types.T_int64.ToType(), types.T_int64.ToType())
		require.True(t, ok)
		require.Error(t, err)
	}
}
