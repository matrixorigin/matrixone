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

package keycodec

import (
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestFloat32CodecContract(t *testing.T) {
	scaled := NewFloat32Codec(2)
	unscaled := NewFloat32Codec(0)

	require.Equal(t, scaled.CanonicalBits(float32(1.234)), scaled.CanonicalBits(float32(1.23)))
	require.Equal(t, scaled.CanonicalBits(float32(-1.234)), scaled.CanonicalBits(float32(-1.23)))
	require.Equal(t, scaled.CanonicalBits(float32(1.236)), scaled.CanonicalBits(float32(1.24)))
	require.NotEqual(t, scaled.CanonicalBits(float32(1.234)), scaled.CanonicalBits(float32(1.236)))
	require.NotEqual(t, unscaled.CanonicalBits(float32(1.234)), unscaled.CanonicalBits(float32(1.23)))

	negativeZero := float32(math.Copysign(0, -1))
	require.Equal(t, uint32(0), unscaled.CanonicalBits(negativeZero))
	require.Equal(t, scaled.CanonicalBits(float32(0)), scaled.CanonicalBits(negativeZero))
}

func TestSupportsExactRawRuntimeFilter(t *testing.T) {
	supported := []types.T{
		types.T_bool,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_decimal64, types.T_decimal128, types.T_decimal256,
		types.T_char, types.T_varchar, types.T_blob, types.T_text,
		types.T_binary, types.T_varbinary,
		types.T_date, types.T_time, types.T_datetime, types.T_timestamp,
		types.T_uuid, types.T_year, types.T_enum,
	}
	for _, oid := range supported {
		require.True(t, SupportsExactRawRuntimeFilter(oid), oid.String())
	}

	unsupported := []types.T{
		types.T_any,
		types.T_float32, types.T_float64,
		types.T_json,
		types.T_array_float32, types.T_array_float64,
	}
	for _, oid := range unsupported {
		require.False(t, SupportsExactRawRuntimeFilter(oid), oid.String())
	}
	for _, oid := range supported {
		want := oid != types.T_decimal64 &&
			oid != types.T_decimal128 &&
			oid != types.T_decimal256 &&
			oid != types.T_enum
		require.Equal(t, want,
			LegacyExactRawProducerSafe(oid), oid.String())
	}

	varchar10 := types.New(types.T_varchar, 10, 0)
	varchar20 := types.New(types.T_varchar, 20, 0)
	require.True(t, SupportsExactRawRuntimeFilterPair(varchar10, varchar20))

	decimal10Scale2 := types.New(types.T_decimal64, 10, 2)
	decimal18Scale2 := types.New(types.T_decimal64, 18, 2)
	decimal18Scale3 := types.New(types.T_decimal64, 18, 3)
	require.True(t, SupportsExactRawRuntimeFilterPair(decimal10Scale2, decimal18Scale2))
	require.False(t, SupportsExactRawRuntimeFilterPair(decimal10Scale2, decimal18Scale3))
	require.False(t, SupportsExactRawRuntimeFilterPair(types.T_int32.ToType(), types.T_int64.ToType()))
	require.False(t, SupportsExactRawRuntimeFilterPair(types.T_float32.ToType(), types.T_float32.ToType()))

	require.Equal(t, ExactRuntimeFilterFloatZeroClosed,
		ExactRuntimeFilterEncodingForPair(types.T_float64.ToType(), types.T_float64.ToType()))
	require.Equal(t, ExactRuntimeFilterFloatZeroClosed,
		ExactRuntimeFilterEncodingForPair(types.T_float32.ToType(), types.T_float32.ToType()))

	unscaledNegative := types.T_float32.ToType()
	unscaledNegative.Scale = -1
	require.Equal(t, ExactRuntimeFilterFloatZeroClosed,
		ExactRuntimeFilterEncodingForPair(unscaledNegative, types.T_float32.ToType()))

	scaledFloat32 := types.T_float32.ToType()
	scaledFloat32.Scale = 2
	require.Equal(t, ExactRuntimeFilterUnsupported,
		ExactRuntimeFilterEncodingForPair(scaledFloat32, types.T_float32.ToType()))
	require.Equal(t, ExactRuntimeFilterUnsupported,
		ExactRuntimeFilterEncodingForPair(types.T_float32.ToType(), scaledFloat32))
}

func TestComputeXXHashScaledFloat32Contract(t *testing.T) {
	m := mpool.MustNewZero()
	floatType := types.T_float32.ToType()
	floatType.Scale = 2
	vec := vector.NewVec(floatType)
	nullable := vector.NewVec(floatType)
	constVec, err := vector.NewConstFixed(floatType, float32(1.23), 3, m)
	require.NoError(t, err)
	constNull := vector.NewConstNull(floatType, 3, m)
	short := vector.NewVec(floatType)
	defer func() {
		vec.Free(m)
		nullable.Free(m)
		constVec.Free(m)
		constNull.Free(m)
		short.Free(m)
		require.Zero(t, m.CurrNB())
	}()

	for _, value := range []float32{1.234, 1.23, -1.234, -1.23, 1.236, 1.24} {
		require.NoError(t, vector.AppendFixed(vec, value, false, m))
	}
	hashes := make([]uint64, vec.Length())
	ComputeXXHash([]*vector.Vector{vec}, hashes, 17)
	require.Equal(t, hashes[0], hashes[1])
	require.Equal(t, hashes[2], hashes[3])
	require.Equal(t, hashes[4], hashes[5])
	require.NotEqual(t, hashes[0], hashes[4])

	require.NoError(t, vector.AppendFixed(nullable, float32(1.234), false, m))
	require.NoError(t, vector.AppendFixed(nullable, float32(99), true, m))
	require.NoError(t, vector.AppendFixed(nullable, float32(1.23), false, m))
	nullableHashes := make([]uint64, nullable.Length())
	ComputeXXHash([]*vector.Vector{nullable}, nullableHashes, 17)
	require.Equal(t, nullableHashes[0], nullableHashes[2])
	require.Equal(t, HashCombine(17, 0), nullableHashes[1])

	constHashes := make([]uint64, 3)
	ComputeXXHash([]*vector.Vector{constVec}, constHashes, 17)
	require.Equal(t, []uint64{hashes[1], hashes[1], hashes[1]}, constHashes)
	nullHashes := make([]uint64, 3)
	ComputeXXHash([]*vector.Vector{constNull}, nullHashes, 17)
	require.Equal(t, []uint64{HashCombine(17, 0), HashCombine(17, 0), HashCombine(17, 0)}, nullHashes)

	require.NoError(t, vector.AppendFixed(short, float32(1.234), false, m))
	shortHashes := make([]uint64, 2)
	ComputeXXHash([]*vector.Vector{short}, shortHashes, 17)
	require.Equal(t, hashes[0], shortHashes[0])
	require.Equal(t, uint64(17), shortHashes[1])
}

func TestComputeXXHashCompositeScaledFloat32Contract(t *testing.T) {
	m := mpool.MustNewZero()
	floatType := types.T_float32.ToType()
	floatType.Scale = 2
	discriminators := vector.NewVec(types.T_int64.ToType())
	floats := vector.NewVec(floatType)
	defer func() {
		discriminators.Free(m)
		floats.Free(m)
		require.Zero(t, m.CurrNB())
	}()

	for _, value := range []int64{7, 7, 8, 7} {
		require.NoError(t, vector.AppendFixed(discriminators, value, false, m))
	}
	for _, value := range []float32{1.234, 1.23, 1.234, 1.236} {
		require.NoError(t, vector.AppendFixed(floats, value, false, m))
	}

	hashes := make([]uint64, floats.Length())
	ComputeXXHash([]*vector.Vector{discriminators, floats}, hashes, 17)
	require.Equal(t, hashes[0], hashes[1])
	require.NotEqual(t, hashes[0], hashes[2], "the FLOAT32 codec must preserve prior column hash state")
	require.NotEqual(t, hashes[0], hashes[3], "a distinct canonical FLOAT32 value must change the composite hash")
}

func TestComputeXXHashCanonicalizesGroupingRows(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()

	for _, test := range []struct {
		name   string
		left   any
		right  any
		newVec func(any) *vector.Vector
	}{
		{
			name:  "fixed",
			left:  []int64{11, 22, 33},
			right: []int64{101, 22, 303},
			newVec: func(values any) *vector.Vector {
				vec := vector.NewVec(types.T_int64.ToType())
				require.NoError(t, vector.AppendFixedList(
					vec, values.([]int64), nil, mp,
				))
				return vec
			},
		},
		{
			name:  "float64",
			left:  []float64{11, 22, 33},
			right: []float64{101, 22, 303},
			newVec: func(values any) *vector.Vector {
				vec := vector.NewVec(types.T_float64.ToType())
				require.NoError(t, vector.AppendFixedList(
					vec, values.([]float64), nil, mp,
				))
				return vec
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			left := test.newVec(test.left)
			right := test.newVec(test.right)
			ordinary := test.newVec(test.left)
			defer left.Free(mp)
			defer right.Free(mp)
			defer ordinary.Free(mp)
			left.GetGrouping().AddRange(0, 3)
			right.GetGrouping().AddRange(0, 3)

			leftHashes := make([]uint64, 3)
			rightHashes := make([]uint64, 3)
			ComputeXXHash([]*vector.Vector{left}, leftHashes, 17)
			ComputeXXHash([]*vector.Vector{right}, rightHashes, 17)

			require.Equal(t, leftHashes, rightHashes)

			left.GetGrouping().Reset()
			left.GetGrouping().Add(0)
			ComputeXXHash([]*vector.Vector{left}, leftHashes, 17)
			ComputeXXHash([]*vector.Vector{ordinary}, rightHashes, 17)
			require.NotEqual(t, rightHashes[0], leftHashes[0])
			require.Equal(t, rightHashes[1:], leftHashes[1:])
		})
	}
}

func TestComputeXXHashDoesNotTreatStaleGroupingAsFull(t *testing.T) {
	mp := mpool.MustNewZero()
	left := vector.NewVec(types.T_int64.ToType())
	right := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(left, int64(11), false, mp))
	require.NoError(t, vector.AppendFixed(right, int64(22), false, mp))
	left.GetGrouping().Add(5)
	right.GetGrouping().Add(5)
	leftHash := []uint64{0}
	rightHash := []uint64{0}
	ComputeXXHash([]*vector.Vector{left}, leftHash, 17)
	ComputeXXHash([]*vector.Vector{right}, rightHash, 17)
	require.NotEqual(t, leftHash, rightHash)
	left.Free(mp)
	right.Free(mp)
	require.Zero(t, mp.CurrNB())
}
