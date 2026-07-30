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
