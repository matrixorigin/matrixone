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

func TestCbitmapDocFilter(t *testing.T) {
	mp := mpool.MustNewZero()
	present := []int64{1, 2, 100, 4096, 7}
	v := buildIntVec(t, mp, types.T_int64.ToType(), present, nil)
	defer v.Free(mp)

	payload, ok, err := BuildCbitmapBytes(v)
	require.NoError(t, err)
	require.True(t, ok)
	require.NotEmpty(t, payload)

	f, err := NewCbitmapDocFilter(payload)
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

	// over-budget id range -> ok=false (caller falls back to CRoaring)
	big := buildIntVec(t, mp, types.T_int64.ToType(), []int64{int64(MaxCbitmapBits) + 10}, nil)
	defer big.Free(mp)
	_, ok, err = BuildCbitmapBytes(big)
	require.NoError(t, err)
	require.False(t, ok)
}
