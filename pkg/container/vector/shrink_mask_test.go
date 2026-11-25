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

package vector

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// shrinkMask returns a mask over maskLen rows with density p, plus full and
// empty words so both fast paths run.
func shrinkMask(t testing.TB, rng *rand.Rand, maskLen int, p float64, mp *mpool.MPool) (*bitmap.Bitmap, []int64) {
	var m bitmap.Bitmap
	require.NoError(t, m.InitWithSize(int64(maskLen), mp))
	var rows []int64
	for r := 0; r < maskLen; r++ {
		word := r / 64
		set := rng.Float64() < p
		switch word % 5 {
		case 1:
			set = true
		case 3:
			set = false
		}
		if set {
			m.Add(uint64(r))
			rows = append(rows, int64(r))
		}
	}
	return &m, rows
}

// TestShrinkByMaskMatchesShrink checks ShrinkByMask against ShrinkByRows with the
// same rows as a list (their complement for remove), with and without an offset.
func TestShrinkByMaskMatchesShrink(t *testing.T) {
	mp := mpool.MustNewZero()
	rng := rand.New(rand.NewSource(7))
	typs := []types.Type{
		types.T_int8.ToType(), types.T_int32.ToType(), types.T_int64.ToType(),
		types.T_decimal128.ToType(), types.T_varchar.ToType(), types.T_Rowid.ToType(),
	}
	for _, typ := range typs {
		for _, n := range []int{1, 63, 64, 65, 200, 1000} {
			for _, p := range []float64{0, 0.02, 0.5, 0.98, 1} {
				for _, offset := range []int{0, 1, 37} {
					for _, negate := range []bool{false, true} {
						if offset >= n {
							continue
						}
						name := fmt.Sprintf("%s/n%d/p%v/off%d/neg%v", typ, n, p, offset, negate)
						src := unionMaskSource(t, rng, typ, n, 0.2, mp)
						mask, rows := shrinkMask(t, rng, n-offset, p, mp)
						// Mask row b names vector row b+offset.
						for i := range rows {
							rows[i] += int64(offset)
						}
						want, err := src.Dup(mp)
						require.NoError(t, err)
						got, err := src.Dup(mp)
						require.NoError(t, err)
						keep := rows
						if negate {
							keep = complementRows(n, rows)
						}
						require.NoError(t, want.ShrinkByRows(keep, mp), name)
						require.NoError(t, got.ShrinkByMask(mask, negate, uint64(offset), mp), name)
						requireSameRows(t, want, got)
						want.Free(mp)
						got.Free(mp)
						src.Free(mp)
						mask.Free()
					}
				}
			}
		}
	}
	require.Zero(t, mp.CurrNB())
}

func BenchmarkShrinkDeletesByMask(b *testing.B)   { benchShrinkDeletes(b, 0.01) }
func BenchmarkShrinkDeletes30ByMask(b *testing.B) { benchShrinkDeletes(b, 0.3) }

// benchShrinkDeletes removes a fraction p of 8192 int64 rows, as the reader
// does with a block's delete mask. Refilling the vector is not timed.
func benchShrinkDeletes(b *testing.B, p float64) {
	const n = 8192
	mp := mpool.MustNewZero()
	rng := rand.New(rand.NewSource(1))
	var mask bitmap.Bitmap
	require.NoError(b, mask.InitWithSize(n, mp))
	for r := 0; r < n; r++ {
		if rng.Float64() < p {
			mask.Add(uint64(r))
		}
	}
	src := NewVec(types.T_int64.ToType(), mp)
	for i := 0; i < n; i++ {
		require.NoError(b, AppendFixed(src, int64(i), i%17 == 0, mp))
	}
	dst := NewVec(types.T_int64.ToType(), mp)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dst.CleanOnlyData()
		require.NoError(b, dst.UnionBatch(src, 0, n, nil, mp))
		b.StartTimer()
		require.NoError(b, dst.ShrinkByMask(&mask, true, 0, mp))
	}
}

// BenchmarkUnionBatchNullable appends 8192 int64 rows, one in 17 NULL, as the
// reader copies a nullable column out of the cache.
func BenchmarkUnionBatchNullable(b *testing.B) {
	const n = 8192
	mp := mpool.MustNewZero()
	src := NewVec(types.T_int64.ToType(), mp)
	for i := 0; i < n; i++ {
		require.NoError(b, AppendFixed(src, int64(i), i%17 == 0, mp))
	}
	dst := NewVec(types.T_int64.ToType(), mp)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst.CleanOnlyData()
		require.NoError(b, dst.UnionBatch(src, 0, n, nil, mp))
	}
}

// UnionBatch keeps NULLs at unaligned source and destination offsets.
func TestUnionBatchNullsAtOffsets(t *testing.T) {
	mp := mpool.MustNewZero()
	rng := rand.New(rand.NewSource(3))
	src := unionMaskSource(t, rng, types.T_int32.ToType(), 500, 0.3, mp)
	for _, pre := range []int{0, 1, 63, 64, 100} {
		for _, off := range []int{0, 1, 37, 64, 200} {
			for _, cnt := range []int{1, 63, 64, 65, 300} {
				dst := unionMaskSource(t, rng, types.T_int32.ToType(), pre, 0.3, mp)
				want, err := dst.Dup(mp)
				require.NoError(t, err)
				for k := 0; k < cnt; k++ {
					require.NoError(t, want.UnionOne(src, int64(off+k), mp))
				}
				require.NoError(t, dst.UnionBatch(src, int64(off), cnt, nil, mp))
				requireSameRows(t, want, dst)
				dst.Free(mp)
				want.Free(mp)
			}
		}
	}
	src.Free(mp)
	require.Zero(t, mp.CurrNB())
}

// A read-only, shifted view as the mask goes through Word instead of the
// owned words; the result matches an owned mask with the same rows.
func TestMaskGatherWithViewMask(t *testing.T) {
	mp := mpool.MustNewZero()
	rng := rand.New(rand.NewSource(5))
	const n, shift = 300, 13
	var parent bitmap.Bitmap
	require.NoError(t, parent.InitWithSize(n+shift+40, mp))
	for r := 0; r < n+shift+40; r++ {
		if rng.Intn(3) == 0 {
			parent.Add(uint64(r))
		}
	}
	var view bitmap.Bitmap
	view.InitView(&parent, shift, shift+n)
	require.True(t, view.IsReadOnly())
	require.Nil(t, view.OwnedWords())
	var owned bitmap.Bitmap
	require.NoError(t, owned.InitWithSize(n, mp))
	for r := 0; r < n; r++ {
		if view.Contains(uint64(r)) {
			owned.Add(uint64(r))
		}
	}
	require.NotNil(t, owned.OwnedWords())

	src := unionMaskSource(t, rng, types.T_int64.ToType(), n, 0.2, mp)
	byView := NewVec(types.T_int64.ToType(), mp)
	byOwned := NewVec(types.T_int64.ToType(), mp)
	require.NoError(t, byView.UnionByMask(src, &view, mp))
	require.NoError(t, byOwned.UnionByMask(src, &owned, mp))
	requireSameRows(t, byOwned, byView)

	for _, negate := range []bool{false, true} {
		a, err := src.Dup(mp)
		require.NoError(t, err)
		b, err := src.Dup(mp)
		require.NoError(t, err)
		require.NoError(t, a.ShrinkByMask(&view, negate, 0, mp))
		require.NoError(t, b.ShrinkByMask(&owned, negate, 0, mp))
		requireSameRows(t, b, a)
		a.Free(mp)
		b.Free(mp)
	}
	byView.Free(mp)
	byOwned.Free(mp)
	src.Free(mp)
	owned.Free()
	parent.Free()
	require.Zero(t, mp.CurrNB())
}

// complementRows returns the rows of [0, n) not in rows, which is ascending.
func complementRows(n int, rows []int64) []int64 {
	out := make([]int64, 0, n-len(rows))
	j := 0
	for r := 0; r < n; r++ {
		if j < len(rows) && rows[j] == int64(r) {
			j++
			continue
		}
		out = append(out, int64(r))
	}
	return out
}
