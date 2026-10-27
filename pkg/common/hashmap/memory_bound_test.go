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

package hashmap

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestStringIteratorReusesSmallTailAndPrunesWideHistory(t *testing.T) {
	pool := mpool.MustNewZero()
	table, err := NewStrHashMap(false, pool)
	require.NoError(t, err)
	defer table.Free()
	vec := vector.NewVec(types.T_varchar.ToType())
	defer vec.Free(pool)
	for range UnitLimit + 1 {
		require.NoError(t, vector.AppendBytes(vec, bytes.Repeat([]byte{'x'}, 32), false, pool))
	}
	vecs := []*vector.Vector{vec}
	itr := table.NewIterator().(*strHashmapIterator)
	_, _, err = itr.Insert(0, UnitLimit, vecs)
	require.NoError(t, err)
	capacity := StrIteratorCapacity(itr)
	tail := &itr.keys[UnitLimit-1][:cap(itr.keys[UnitLimit-1])][0]
	for _, count := range []int{1, UnitLimit, 1} {
		values, _ := itr.Find(0, count, vecs)
		require.Equal(t, uint64(1), values[0])
		require.Equal(t, capacity, StrIteratorCapacity(itr))
		require.Equal(t, capacity, itr.keyCapacity)
		require.Same(t, tail, &itr.keys[UnitLimit-1][:cap(itr.keys[UnitLimit-1])][0])
	}
	duplicate, err := itr.DetectDup(vecs, UnitLimit)
	require.NoError(t, err)
	require.False(t, duplicate)
	require.Equal(t, capacity, itr.keyCapacity)
	wide := vector.NewVec(types.T_varchar.ToType())
	defer wide.Free(pool)
	require.NoError(t, vector.AppendBytes(wide, bytes.Repeat([]byte{'w'}, 128<<10), false, pool))
	itr.Find(0, 1, []*vector.Vector{wide})
	require.Equal(t, StrIteratorCapacity(itr), itr.keyCapacity)
	itr.Find(0, 1, vecs)
	require.LessOrEqual(t, StrIteratorCapacity(itr), MaxStrIteratorCapacity)
	require.Equal(t, StrIteratorCapacity(itr), itr.keyCapacity)
}

func TestStringIteratorDoesNotKeepRotatingWidePeaks(t *testing.T) {
	pool := mpool.MustNewZero()
	table, err := NewStrHashMap(false, pool)
	require.NoError(t, err)
	defer table.Free()
	iterator := table.NewIterator()
	wide := bytes.Repeat([]byte{'x'}, 128<<10)
	for position := 0; position < 4; position++ {
		vec := vector.NewVec(types.T_varchar.ToType())
		for row := 0; row < 4; row++ {
			value := []byte("s")
			if row == position {
				value = wide
			}
			require.NoError(t, vector.AppendBytes(vec, value, false, pool))
		}
		_, _, err := iterator.Insert(0, 4, []*vector.Vector{vec})
		require.NoError(t, err)
		vec.Free(pool)
	}
	require.Less(t, StrIteratorCapacity(iterator), len(wide)*3, "key buffers follow current window, not every historical row-position peak")
}
