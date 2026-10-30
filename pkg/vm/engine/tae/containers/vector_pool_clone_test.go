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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCloneConstNullWithReusedPoolVector(t *testing.T) {
	for _, typ := range []types.Type{types.T_int64.ToType(), types.T_varchar.ToType()} {
		t.Run(typ.Oid.String(), func(t *testing.T) {
			mp := mpool.MustNewZero()
			pool := NewVectorPool(t.Name(), 2, WithMPool(mp))
			t.Cleanup(pool.Destory)
			seed := pool.GetVector(&typ)
			t.Cleanup(seed.Close)
			require.NoError(t, seed.PreExtend(2))
			allocated := mp.CurrNB()
			require.Positive(t, allocated)
			seed.Close()
			src := NewConstNullVector(typ, 2, mp)
			t.Cleanup(src.Close)
			cloned := src.CloneWindowWithPool(0, 2, pool)
			t.Cleanup(cloned.Close)
			require.True(t, cloned.IsConstNull())
			require.Equal(t, 2, cloned.Length())
			cloned.Close()
			require.Equal(t, int(allocated), pool.Allocated())
			reused := pool.GetVector(&typ)
			t.Cleanup(reused.Close)
			if typ.IsVarlen() {
				reused.Append([]byte("next"), false)
			} else {
				reused.Append(int64(7), false)
			}
			require.False(t, reused.IsNull(0))
			require.Equal(t, allocated, mp.CurrNB())
			reused.Close()
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
	t.Cleanup(seed.Close)
	// Keep this checkout alive so its storage can be used directly as the clone destination.
	require.NoError(t, seed.PreExtend(1))
	src := NewConstNullVector(typ, 1, mp)
	t.Cleanup(src.Close)
	require.NoError(t, src.GetDownstreamVector().CloneWindowTo(seed.GetDownstreamVector(), 0, 1, mp))
	seed.Close()
	used, _ := pool.Used(false)
	require.Zero(t, used)
	require.Zero(t, pool.Allocated())
	require.Zero(t, mp.CurrNB())
}
