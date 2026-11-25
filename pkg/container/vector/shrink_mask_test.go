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

// TestShrinkByMaskMatchesShrink checks ShrinkByMask against Shrink with the
// same rows as a list, for keep and remove, with and without an offset.
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
						require.NoError(t, want.Shrink(rows, negate, mp), name)
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

func BenchmarkShrinkDeletesByList(b *testing.B)   { benchShrinkDeletes(b, false, 0.01) }
func BenchmarkShrinkDeletesByMask(b *testing.B)   { benchShrinkDeletes(b, true, 0.01) }
func BenchmarkShrinkDeletes30ByList(b *testing.B) { benchShrinkDeletes(b, false, 0.3) }
func BenchmarkShrinkDeletes30ByMask(b *testing.B) { benchShrinkDeletes(b, true, 0.3) }

// benchShrinkDeletes removes a fraction p of 8192 int64 rows, as the reader
// does with a block's delete mask. The list path includes ToI64Array, which
// the reader ran before every Shrink. Refilling the vector is not timed.
func benchShrinkDeletes(b *testing.B, byMask bool, p float64) {
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
	var list []int64
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dst.CleanOnlyData()
		require.NoError(b, dst.UnionBatch(src, 0, n, nil, mp))
		b.StartTimer()
		if byMask {
			require.NoError(b, dst.ShrinkByMask(&mask, true, 0, mp))
		} else {
			list = mask.ToI64Array(&list)
			require.NoError(b, dst.Shrink(list, true, mp))
		}
	}
}
