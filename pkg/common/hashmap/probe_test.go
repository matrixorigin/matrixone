// Copyright 2026 Matrix Origin
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

package hashmap

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestFindWithMPool(t *testing.T) {
	for _, hasNull := range []bool{false, true} {
		t.Run(map[bool]string{false: "skip null", true: "encode null"}[hasNull], func(t *testing.T) {
			pool := mpool.MustNewZero()
			table, err := NewStrHashMap(hasNull, pool)
			require.NoError(t, err)
			var vectors []*vector.Vector
			t.Cleanup(func() {
				for _, v := range vectors {
					v.Free(pool)
				}
				table.Free()
				require.Zero(t, pool.CurrNB())
			})
			s := vector.NewVec(types.T_varchar.ToType())
			vectors = append(vectors, s)
			for _, v := range []string{"a", "bb", ""} {
				require.NoError(t, vector.AppendBytes(s, []byte(v), false, pool))
			}
			fixed := vector.NewVec(types.T_int32.ToType())
			vectors = append(vectors, fixed)
			require.NoError(t, vector.AppendFixedList(fixed, []int32{1, 2, 3}, nil, pool))
			nulls.Add(fixed.GetNulls(), 1)
			constant, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("constant"), 3, pool)
			require.NoError(t, err)
			vectors = append(vectors, constant)
			constantNull := vector.NewConstNull(types.T_varchar.ToType(), 3, pool)
			vectors = append(vectors, constantNull)
			for _, cols := range [][]*vector.Vector{{s}, {s, fixed}, {constant}, {constantNull}} {
				_, _, err = table.NewIterator().Insert(0, 3, cols)
				require.NoError(t, err)
				iter := table.NewIterator()
				reference := table.NewIterator()
				for _, start := range []int{0, 1} {
					before := pool.CurrNB()
					beforeAllocated := pool.AllocatedBytes()
					allocs, frees := pool.Stats().NumAlloc.Load(), pool.Stats().NumFree.Load()
					want, wantNull := reference.Find(start, 3-start, cols)
					got, gotNull, err := FindWithMPool(iter, start, 3-start, cols, pool)
					require.NoError(t, err)
					require.Equal(t, want, got)
					require.Equal(t, wantNull, gotNull)
					require.Zero(t, StrIteratorCapacity(iter))
					require.Equal(t, before, pool.CurrNB())
					require.Equal(t, beforeAllocated, pool.AllocatedBytes())
					require.Equal(t, allocs+1, pool.Stats().NumAlloc.Load(), "scratch must use cap-enforced off-heap accounting")
					require.Equal(t, frees+1, pool.Stats().NumFree.Load())
				}
			}
			// An allocator rejection leaves neither borrowed arena references nor a
			// damaged index, and the next lookup can succeed with sufficient headroom.
			limited, err := mpool.NewMPool("probe allocation failure", 1<<20, mpool.NoFixed)
			require.NoError(t, err)
			held, err := limited.Alloc(1<<20, true)
			require.NoError(t, err)
			t.Cleanup(func() { limited.Free(held); require.Zero(t, limited.CurrNB()) })
			iter := table.NewIterator()
			iter.Find(0, 3, []*vector.Vector{s})
			require.Positive(t, StrIteratorCapacity(iter))
			_, _, err = FindWithMPool(iter, 0, 3, []*vector.Vector{s}, limited)
			require.Error(t, err)
			require.Equal(t, int64(1<<20), limited.CurrNB())
			require.Zero(t, StrIteratorCapacity(iter))
			got, _, err := FindWithMPool(iter, 0, 3, []*vector.Vector{s}, pool)
			require.NoError(t, err)
			require.NotZero(t, got[0])
			got, _, err = FindWithMPool(iter, 0, 0, []*vector.Vector{s}, pool)
			require.NoError(t, err)
			require.Empty(t, got)
			require.Zero(t, StrIteratorCapacity(iter))
		})
	}
}

func TestFindWithMPoolFixedIterator(t *testing.T) {
	pool := mpool.MustNewZero()
	table, err := NewIntHashMap(false, pool)
	require.NoError(t, err)
	vec := vector.NewVec(types.T_int32.ToType())
	t.Cleanup(func() { vec.Free(pool); table.Free(); require.Zero(t, pool.CurrNB()) })
	require.NoError(t, vector.AppendFixedList(vec, []int32{7, 8}, nil, pool))
	_, _, err = table.NewIterator().Insert(0, 1, []*vector.Vector{vec})
	require.NoError(t, err)
	got, nonnull, err := FindWithMPool(table.NewIterator(), 0, 2, []*vector.Vector{vec}, pool)
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 0}, got)
	require.Equal(t, []int64{1, 1}, nonnull)
}
