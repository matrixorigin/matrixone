// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package containers

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestCloneConstNullWithReusedPoolVector(t *testing.T) {
	for _, typ := range []types.Type{types.T_int64.ToType(), types.T_varchar.ToType()} {
		t.Run(typ.Oid.String(), func(t *testing.T) {
			mp := mpool.MustNewZero()
			pool := NewVectorPool(t.Name(), 2, WithMPool(mp))
			t.Cleanup(pool.Destory)
			seed := pool.GetVector(&typ)
			t.Cleanup(func() { ReleaseVector(seed) })
			require.NoError(t, seed.PreExtend(2, seed.MPool()))
			allocated := mp.CurrNB()
			require.Positive(t, allocated)
			ReleaseVector(seed)
			src := vector.NewConstNull(typ, 2, mp)
			t.Cleanup(func() { ReleaseVector(src) })
			cloned := pool.GetVector(src.GetType())
			require.NoError(t, src.CloneWindowTo(cloned, 0, 2, cloned.MPool()))
			t.Cleanup(func() { ReleaseVector(cloned) })
			require.True(t, cloned.IsConstNull())
			require.Equal(t, 2, cloned.Length())
			ReleaseVector(cloned)
			require.Equal(t, int(allocated), pool.Allocated())
			reused := pool.GetVector(&typ)
			t.Cleanup(func() { ReleaseVector(reused) })
			if typ.IsVarlen() {
				reused.MustAppendValue([]byte("next"), false)
			} else {
				reused.MustAppendValue(int64(7), false)
			}
			require.False(t, reused.IsNull(0))
			require.Equal(t, allocated, mp.CurrNB())
			ReleaseVector(reused)
			used, _ := pool.Used(false)
			require.Zero(t, used)
			pool.Destory()
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestCloneConstNullPoolAllocationLimit(t *testing.T) {
	mp := mpool.MustNewZero()
	pool := NewVectorPool(t.Name(), 2, WithMPool(mp), WithAllocationLimit(1))
	t.Cleanup(pool.Destory)
	typ := types.T_int64.ToType()
	seed := pool.GetVector(&typ)
	t.Cleanup(func() { ReleaseVector(seed) })
	// Keep this checkout alive so its storage can be used directly as the clone destination.
	require.NoError(t, seed.PreExtend(1, seed.MPool()))
	src := vector.NewConstNull(typ, 1, mp)
	t.Cleanup(func() { ReleaseVector(src) })
	require.NoError(t, src.CloneWindowTo(seed, 0, 1, mp))
	ReleaseVector(seed)
	used, _ := pool.Used(false)
	require.Zero(t, used)
	require.Zero(t, pool.Allocated())
	require.Zero(t, mp.CurrNB())
}

func TestVectorPoolCloneWindow(t *testing.T) {
	mp := mpool.MustNewZero()
	pool := NewVectorPool(t.Name(), 2, WithMPool(mp))
	t.Cleanup(pool.Destory)
	src := MakeVector(types.T_int32.ToType(), mp)
	defer ReleaseVector(src)
	for i := range 5 {
		src.MustAppendValue(int32(i), false)
	}
	cloned, err := pool.CloneWindow(src, 1, 3)
	require.NoError(t, err)
	require.True(t, isLent(cloned))
	require.Equal(t, 3, cloned.Length())
	require.Equal(t, int32(1), cloned.GetValue(0))
	require.Equal(t, int32(3), cloned.GetValue(2))
	ReleaseVector(cloned)
	used, _ := pool.Used(false)
	require.Zero(t, used)
}
