// Copyright 2024 Matrix Origin
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

package process

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestWhatVectorCouldBeAcceptedForPool(t *testing.T) {
	mp := mpool.MustNewZero()
	// accepted.
	{
		// small vector.
		v1 := vector.NewVec(types.T_bool.ToType())
		_ = v1.PreExtend(500, mp)

		require.False(t, vectorCannotPut(v1))
	}

	// not accepted.
	{
		// const
		v1, _ := vector.NewConstBytes(types.T_varchar.ToType(), []byte("abc"), 10, mp)
		v2, _ := vector.NewConstFixed[bool](types.T_bool.ToType(), true, 10, mp)
		v3 := vector.NewConstNull(types.T_bool.ToType(), 5, mp)
		// cantFree
		v4 := vector.NewVec(types.T_bool.ToType())
		_ = v4.PreExtend(10, mp)
		v4.SetLength(10)
		v4, _ = v4.Window(1, 3)
		// big vector.
		v5 := vector.NewVec(types.T_bool.ToType())
		_ = v5.PreExtend(defaultMaxVectorItemSize+1, mp)

		require.True(t, vectorCannotPut(v1))
		require.True(t, vectorCannotPut(v2))
		require.True(t, vectorCannotPut(v3))
		require.True(t, vectorCannotPut(v4))
		require.True(t, vectorCannotPut(v5))
	}
}

func TestVectorPool(t *testing.T) {
	mp := mpool.MustNewZero()
	vp := initCachedVectorPool()
	vp.modifyCapacity(1, mp)

	// now, this vp can only store `1 without-area-field vectors` and `1 with-area-field vectors`.
	{
		v1 := vector.NewVec(types.T_bool.ToType())
		_ = v1.PreExtend(10, mp)
		v2 := vector.NewVec(types.T_bool.ToType())
		_ = v2.PreExtend(10, mp)

		v3 := vector.NewVec(types.T_varchar.ToType())
		_ = v3.PreExtendWithArea(10, 100, mp)
		v4 := vector.NewVec(types.T_varchar.ToType())
		_ = v4.PreExtendWithArea(10, 100, mp)

		// can only put 1 without-area vector.
		vp.putVectorIntoPool(v1)
		s := vp.memorySize()
		require.Equal(t, int64(v1.Allocated()), s)
		vp.putVectorIntoPool(v2)
		require.Equal(t, s, vp.memorySize())

		// can only put 1 with-area vector.
		vp.putVectorIntoPool(v3)
		s = vp.memorySize()
		require.Equal(t, int64(v1.Allocated()+v3.Allocated()), s)
		vp.putVectorIntoPool(v4)
		require.Equal(t, s, vp.memorySize())

		// get v1 and v3.
		nv3 := vp.getVectorFromPool(types.T_varchar.ToType())
		require.True(t, nv3 == v3)
		nv1 := vp.getVectorFromPool(types.T_bool.ToType())
		require.True(t, nv1 == v1)
		require.Equal(t, int64(0), vp.memorySize())
		require.Equal(t, 0, len(vp.pool[poolWithArea]))
		require.Equal(t, 0, len(vp.pool[poolWithoutArea]))
	}
}
