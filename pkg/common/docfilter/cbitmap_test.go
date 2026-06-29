// Copyright 2022 Matrix Origin
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

package docfilter

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestCbitmapFilter(t *testing.T) {
	mp := mpool.MustNewZero()
	present := []int64{1, 2, 100, 4096, 7}
	v := buildIntVec(t, mp, types.T_int64.ToType(), present, nil)
	defer v.Free(mp)

	payload, ok, err := BuildCbitmapBytes(v)
	require.NoError(t, err)
	require.True(t, ok)
	require.NotEmpty(t, payload)

	f, err := NewCbitmapFilter(payload)
	require.NoError(t, err)
	require.True(t, f.Valid())

	for i := range present {
		require.True(t, f.Test(v.GetRawBytesAt(i)), "value %d present", present[i])
	}
	absent := buildIntVec(t, mp, types.T_int64.ToType(), []int64{3, 4, 999, 1 << 20}, nil)
	defer absent.Free(mp)
	for i := 0; i < absent.Length(); i++ {
		require.False(t, f.Test(absent.GetRawBytesAt(i)))
	}

	probe := buildIntVec(t, mp, types.T_int64.ToType(), []int64{1, 3, 100, 0}, map[int]bool{3: true})
	defer probe.Free(mp)
	require.Equal(t, []uint8{1, 0, 1, 0}, f.TestVector(probe, nil))

	// share independence
	a := f.Share()
	a.Free()
	require.True(t, f.Valid())
	require.True(t, f.Test(v.GetRawBytesAt(0)))

	// over-budget id SPAN -> ok=false (caller falls back to CRoaring). With the
	// base offset on (default), feasibility is span-based, so a lone huge value
	// (span 0) is feasible — it's a set whose SPAN exceeds the cap that falls
	// back. {0, MaxCbitmapBits+10} spans past the cap either way (base 0).
	big := buildIntVec(t, mp, types.T_int64.ToType(), []int64{0, int64(MaxCbitmapBits) + 10}, nil)
	defer big.Free(mp)
	_, ok, err = BuildCbitmapBytes(big)
	require.NoError(t, err)
	require.False(t, ok)
}

// TestCbitmapNegativeInt32 verifies negative int32 values — which zero-extend to
// large uint64 (int32 -1 -> 0xFFFFFFFF = 4294967295, NOT the int64 -1 pattern) —
// are handled consistently across build (C decode), Test (Go rawIntToUint64),
// and TestVector (C decode). It also shows the offset layout makes a clustered
// all-negative set feasible, where the value-indexed layout cannot (max ~4.3B).
func TestCbitmapNegativeInt32(t *testing.T) {
	mp := mpool.MustNewZero()
	orig := CbitmapUseOffset
	defer func() { CbitmapUseOffset = orig }()

	// All-negative, clustered: uint64(int32(-1)) ~= 4.29B (max), uint64(int32(
	// -1000)) (min) -> span ~999.
	present := []int32{-1000, -100, -50, -1}
	v := buildIntVec(t, mp, types.T_int32.ToType(), present, nil)
	defer v.Free(mp)

	// Without offset: max ~4.29B >> MaxCbitmapBits -> infeasible -> CRoaring.
	CbitmapUseOffset = false
	_, ok, err := BuildCbitmapBytes(v)
	require.NoError(t, err)
	require.False(t, ok, "negative int32 max (~4.3B) exceeds the cap without offset")

	// With offset: span ~999 -> feasible, membership exact.
	CbitmapUseOffset = true
	payload, ok, err := BuildCbitmapBytes(v)
	require.NoError(t, err)
	require.True(t, ok, "offset makes the narrow negative span feasible")

	f, err := NewCbitmapFilter(payload)
	require.NoError(t, err)
	defer f.Free()

	for i := range present {
		require.True(t, f.Test(v.GetRawBytesAt(i)), "value %d present", present[i])
	}
	// absent: negatives inside the span but unset, plus positives (below base).
	absent := buildIntVec(t, mp, types.T_int32.ToType(), []int32{-2, -999, -101, 0, 5}, nil)
	defer absent.Free(mp)
	for i := 0; i < absent.Length(); i++ {
		require.False(t, f.Test(absent.GetRawBytesAt(i)), "row %d should be absent", i)
	}
	// TestVector (C decode) must agree with Test (Go decode) on the same data.
	require.Equal(t, []uint8{1, 1, 1, 1}, f.TestVector(v, nil))
}
