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
							foundVs, foundZvs, err := itr.Find(0, count, vecs)
							require.NoError(t, err)
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

func TestIntHashMapFloat32ZeroCountWithNonZeroStart(t *testing.T) {
	for _, scale := range []int32{0, 2} {
		t.Run(fmt.Sprintf("scale-%d", scale), func(t *testing.T) {
			m := mpool.MustNewZero()
			typ := types.T_float32.ToType()
			typ.Scale = scale
			vec := vector.NewVec(typ)
			defer vec.Free(m)
			vecs := []*vector.Vector{vec}

			intMap, err := NewIntHashMap(false, m)
			require.NoError(t, err)
			defer intMap.Free()
			intItr := intMap.NewIterator()
			require.NotPanics(t, func() {
				intItr.encodeHashKeys(vecs, 1, 0)
			})
		})
	}
}

func TestIntHashMapIteratorReadWriteTransitions(t *testing.T) {
	for _, tc := range []struct {
		name    string
		types   []types.Type
		hasNull bool
		count   int
	}{
		{"int32", []types.Type{types.T_int32.ToType()}, false, 2},
		{"int64", []types.Type{types.T_int64.ToType()}, false, 2},
		{"composite", []types.Type{types.T_int32.ToType(), types.T_int32.ToType()}, false, 2},
		{"nullable", []types.Type{types.T_int32.ToType()}, true, 2},
		{"full-batch", []types.Type{types.T_int64.ToType()}, false, UnitLimit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, m.CurrNB()) })
			makeKeys := func(base int) []*vector.Vector {
				vecs := make([]*vector.Vector, len(tc.types))
				for j, typ := range tc.types {
					vec := vector.NewVec(typ)
					vecs[j] = vec
					t.Cleanup(func() { vec.Free(m) })
					for i := 0; i < tc.count; i++ {
						value := base + i + j*1000
						if typ.Oid == types.T_int32 {
							require.NoError(t, vector.AppendFixed(vec, int32(value), tc.hasNull && i == 1 && base == 10, m))
						} else {
							require.NoError(t, vector.AppendFixed(vec, int64(value), false, m))
						}
					}
				}
				return vecs
			}
			keys, missing := makeKeys(10), makeKeys(10000)
			first, err := NewIntHashMap(tc.hasNull, m)
			require.NoError(t, err)
			defer first.Free()
			second, err := NewIntHashMap(tc.hasNull, m)
			require.NoError(t, err)
			defer second.Free()
			itr := first.NewIterator()
			for _, owner := range []*IntHashMap{first, second} {
				if owner != first {
					IteratorClearOwner(itr)
					IteratorChangeOwner(itr, owner)
				}
				// Grow, shrink, and regrow without allocating a new iterator.
				for _, count := range []int{tc.count, 1, tc.count} {
					_, _, err = itr.Find(0, count, missing)
					require.NoError(t, err)
					inserted, _, err := itr.Insert(0, count, keys)
					require.NoError(t, err)
					for i, group := range inserted {
						require.Equal(t, uint64(i+1), group)
					}
					found, _, err := owner.NewIterator().Find(0, count, keys)
					require.NoError(t, err)
					require.Equal(t, inserted, found)
					require.Equal(t, uint64(tc.count), owner.GroupCount())
				}
				absent, _, err := itr.Find(0, tc.count, missing)
				require.NoError(t, err)
				for _, group := range absent {
					require.Zero(t, group)
				}
				isNew, err := itr.DetectDup(keys, 0)
				require.NoError(t, err)
				require.False(t, isNew)
				_, _, err = itr.Find(0, tc.count, keys)
				require.NoError(t, err)
			}
		})
	}
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
	benchmarkIntZValues  []int64
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

func BenchmarkIntHashMapFindFloat32(b *testing.B) {
	const count = UnitLimit

	for _, scale := range []int32{0, 2} {
		b.Run(fmt.Sprintf("scale-%d/rows-%d", scale, count), func(b *testing.B) {
			m := mpool.MustNewZero()
			mp, err := NewIntHashMap(false, m)
			if err != nil {
				b.Fatal(err)
			}
			defer mp.Free()

			typ := types.T_float32.ToType()
			typ.Scale = scale
			vec := vector.NewVec(typ)
			defer vec.Free(m)
			for i := 0; i < count; i++ {
				if err := vector.AppendFixed(vec, float32(i)+0.125, false, m); err != nil {
					b.Fatal(err)
				}
			}
			vecs := []*vector.Vector{vec}
			itr := mp.NewIterator()
			if _, _, err := itr.Insert(0, count, vecs); err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()
			b.SetBytes(int64(count * types.T_float32.TypeLen()))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				benchmarkIntValues, benchmarkIntZValues, err = itr.Find(0, count, vecs)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
