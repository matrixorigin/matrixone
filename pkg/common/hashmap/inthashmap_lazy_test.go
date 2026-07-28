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

package hashmap

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestIntHashMapIteratorLazyBuffers(t *testing.T) {
	for _, hasNull := range []bool{false, true} {
		t.Run(fmt.Sprintf("has-null-%t", hasNull), func(t *testing.T) {
			for _, typ := range []types.Type{types.T_int32.ToType(), types.T_int64.ToType()} {
				t.Run(typ.String(), func(t *testing.T) {
					for _, count := range []int{0, 1, 255, 256} {
						t.Run(fmt.Sprintf("rows-%d", count), func(t *testing.T) {
							m := mpool.MustNewZero()
							mp, err := NewIntHashMap(hasNull, m)
							require.NoError(t, err)
							defer mp.Free()

							vecs := newVectorsWithNull([]types.Type{typ}, false, max(count, 1), m)
							defer func() {
								for _, vec := range vecs {
									vec.Free(m)
								}
							}()

							itr := mp.NewIterator()
							assertIntIteratorCapacity(t, itr, 0, hasNull)
							vs, zvs, err := itr.Insert(0, count, vecs)
							require.NoError(t, err)
							require.Len(t, vs, count)
							require.Len(t, zvs, count)

							insertedVs := append([]uint64(nil), vs...)
							insertedZvs := append([]int64(nil), zvs...)
							foundVs, foundZvs := itr.Find(0, count, vecs)
							if count > 0 {
								require.Equal(t, insertedVs, foundVs)
								require.Equal(t, insertedZvs, foundZvs)
							}
							assertIntIteratorCapacity(t, itr, count, hasNull)
						})
					}
				})
			}
		})
	}

	t.Run("grow-and-shrink", func(t *testing.T) {
		itr := &intHashMapIterator{}
		for _, tc := range []struct {
			count   int
			wantCap int
		}{
			{count: 0, wantCap: 0},
			{count: 1, wantCap: 1},
			{count: 255, wantCap: 255},
			{count: 1, wantCap: 255},
			{count: 256, wantCap: 256},
			{count: 0, wantCap: 256},
		} {
			itr.ensureCapacity(tc.count)
			require.Len(t, itr.keys, tc.count)
			require.Len(t, itr.keyOffs, tc.count)
			require.Len(t, itr.values, tc.count)
			require.Len(t, itr.zValues, tc.count)
			require.Len(t, itr.hashes, tc.count)
			assertIntIteratorCapacity(t, itr, tc.wantCap, false)
		}
		require.Panics(t, func() {
			itr.ensureCapacity(UnitLimit + 1)
		})
	})

	t.Run("owner-change-adds-null-guard", func(t *testing.T) {
		m := mpool.MustNewZero()
		nonNullable, err := NewIntHashMap(false, m)
		require.NoError(t, err)
		defer nonNullable.Free()
		nullable, err := NewIntHashMap(true, m)
		require.NoError(t, err)
		defer nullable.Free()

		nonNullVecs := newVectors([]types.Type{types.T_int64.ToType()}, false, UnitLimit, m)
		nullVecs := newVectorsWithNull([]types.Type{types.T_int64.ToType()}, false, UnitLimit, m)
		defer func() {
			for _, vec := range append(nonNullVecs, nullVecs...) {
				vec.Free(m)
			}
		}()

		itr := nonNullable.NewIterator()
		_, _, err = itr.Insert(0, UnitLimit, nonNullVecs)
		require.NoError(t, err)
		require.Equal(t, UnitLimit, cap(itr.keys))

		IteratorChangeOwner(itr, nullable)
		_, _, err = itr.Insert(0, UnitLimit, nullVecs)
		require.NoError(t, err)
		assertIntIteratorCapacity(t, itr, UnitLimit, true)
	})
}

func assertIntIteratorCapacity(t *testing.T, itr *intHashMapIterator, want int, hasNull bool) {
	t.Helper()
	if want == 0 {
		require.Zero(t, cap(itr.keys))
	} else {
		wantKeyCap := want
		if hasNull {
			wantKeyCap++
		}
		require.Equal(t, wantKeyCap, cap(itr.keys))
	}
	require.Equal(t, want, cap(itr.keyOffs))
	require.Equal(t, want, cap(itr.values))
	require.Equal(t, want, cap(itr.zValues))
	require.Equal(t, want, cap(itr.hashes))
}

var (
	benchmarkIntIterator *intHashMapIterator
	benchmarkIntValues   []uint64
)

func BenchmarkIntHashMapIteratorFirstInsert(b *testing.B) {
	for _, count := range []int{1, 255, 256} {
		b.Run(fmt.Sprintf("rows-%d", count), func(b *testing.B) {
			m := mpool.MustNewZero()
			mp, err := NewIntHashMap(false, m)
			if err != nil {
				b.Fatal(err)
			}
			defer mp.Free()

			vec := newVector(count, types.T_int64.ToType(), m, false, nil)
			defer vec.Free(m)
			vecs := []*vector.Vector{vec}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				itr := mp.NewIterator()
				vs, _, err := itr.Insert(0, count, vecs)
				if err != nil {
					b.Fatal(err)
				}
				benchmarkIntIterator = itr
				benchmarkIntValues = vs
			}
		})
	}
}
