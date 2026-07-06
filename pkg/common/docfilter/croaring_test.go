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

func TestCRoaringFilter(t *testing.T) {
	mp := mpool.MustNewZero()
	present := []int64{1, 2, 100, 1 << 40, 7}
	v := buildIntVec(t, mp, types.T_int64.ToType(), present, nil)
	defer v.Free(mp)

	payload, err := BuildCRoaringBytes(v)
	require.NoError(t, err)
	require.NotEmpty(t, payload)

	f, err := NewCRoaringFilter(payload)
	require.NoError(t, err)
	require.True(t, f.Valid())
	defer f.Free()

	for i := range present {
		require.True(t, f.Test(v.GetRawBytesAt(i)), "value %d present", present[i])
	}
	absent := buildIntVec(t, mp, types.T_int64.ToType(), []int64{3, 4, 999, 1 << 41}, nil)
	defer absent.Free(mp)
	for i := 0; i < absent.Length(); i++ {
		require.False(t, f.Test(absent.GetRawBytesAt(i)))
	}

	probe := buildIntVec(t, mp, types.T_int64.ToType(), []int64{1, 3, 100, 0}, map[int]bool{3: true})
	defer probe.Free(mp)
	require.Equal(t, []uint8{1, 0, 1, 0}, f.TestVector(probe, nil))

	// uint32 width
	v32 := buildIntVec(t, mp, types.T_uint32.ToType(), []uint32{5, 6, 7}, nil)
	defer v32.Free(mp)
	f32, err := NewCRoaringFilter(must(BuildCRoaringBytes(v32)))
	require.NoError(t, err)
	defer f32.Free()
	require.True(t, f32.Test(v32.GetRawBytesAt(0)))
	require.True(t, f32.Test(v32.GetRawBytesAt(2)))

	// share + independent free
	a := f.SharePointer()
	a.Free()
	require.True(t, f.Valid())
	require.True(t, f.Test(v.GetRawBytesAt(0)))
}
