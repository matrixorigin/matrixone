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

package vector

import (
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestVarlenaAreaIsDisjointIsComputedFromDescriptors(t *testing.T) {
	mp := mpool.MustNewZero()
	long := func(c string) []byte { return []byte(strings.Repeat(c, 40)) }

	build := func(t *testing.T) *Vector {
		v := NewVec(types.T_varchar.ToType())
		for _, c := range []string{"a", "b", "c"} {
			require.NoError(t, AppendBytes(v, long(c), false, mp))
		}
		require.NoError(t, AppendBytes(v, []byte("inline"), false, mp))
		return v
	}

	t.Run("append built", func(t *testing.T) {
		v := build(t)
		defer v.Free(mp)
		require.True(t, v.VarlenaAreaIsDisjoint())
	})

	t.Run("shuffled stays disjoint", func(t *testing.T) {
		v := build(t)
		defer v.Free(mp)
		require.NoError(t, v.Shuffle([]int64{2, 0, 3, 1}, mp))
		require.True(t, v.VarlenaAreaIsDisjoint())
		require.Equal(t, long("c"), v.GetBytesAt(0))
	})

	t.Run("aliased descriptors", func(t *testing.T) {
		v := build(t)
		defer v.Free(mp)
		values := MustFixedColNoTypeCheck[types.Varlena](v)
		values[2] = values[0]
		require.False(t, v.VarlenaAreaIsDisjoint())
		// A NULL row's stale descriptor is not live.
		v.SetNull(2)
		require.True(t, v.VarlenaAreaIsDisjoint())
	})

	t.Run("out of area", func(t *testing.T) {
		v := build(t)
		defer v.Free(mp)
		v.area = v.area[:len(v.area)-1]
		require.False(t, v.VarlenaAreaIsDisjoint())
	})

	t.Run("const and fixed never report", func(t *testing.T) {
		c, err := NewConstBytes(types.T_varchar.ToType(), long("z"), 3, mp)
		require.NoError(t, err)
		defer c.Free(mp)
		require.False(t, c.VarlenaAreaIsDisjoint())
		f := NewVec(types.T_int64.ToType())
		require.False(t, f.VarlenaAreaIsDisjoint())
	})
}
