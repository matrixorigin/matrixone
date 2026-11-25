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

// TestScatterByMaskInvertsUnionByMask scatters the gathered rows of a source
// back to their positions: selected rows equal the source, the others are NULL.
func TestScatterByMaskInvertsUnionByMask(t *testing.T) {
	rng := rand.New(rand.NewSource(11))
	mp := mpool.MustNewZero()
	typs := []types.Type{
		types.T_bool.ToType(), types.T_int16.ToType(), types.T_int32.ToType(),
		types.T_int64.ToType(), types.T_decimal128.ToType(), types.T_decimal256.ToType(),
		types.T_Rowid.ToType(), types.T_varchar.ToType(),
	}
	for _, typ := range typs {
		for _, n := range []int{1, 63, 64, 65, 200, 8192} {
			for _, p := range []float64{0, 0.02, 0.5, 1} {
				for _, nullRate := range []float64{0, 0.3} {
					name := fmt.Sprintf("%s/n=%d/p=%v/null=%v", typ.String(), n, p, nullRate)
					src := unionMaskSource(t, rng, typ, n, nullRate, mp)
					// The mask sometimes covers more rows than are scattered.
					mask := new(bitmap.Bitmap)
					require.NoError(t, mask.InitWithSize(int64(n+rng.Intn(3)*40), mp))
					for r := 0; r < int(mask.Len()); r++ {
						if rng.Float64() < p {
							mask.Add(uint64(r))
						}
					}
					compact := NewVec(typ, mp)
					require.NoError(t, compact.UnionByMask(src, mask, mp), name)

					want := NewVec(typ, mp)
					for r := 0; r < n; r++ {
						if mask.Contains(uint64(r)) {
							require.NoError(t, want.UnionOne(src, int64(r), mp))
						} else if typ.IsVarlen() {
							require.NoError(t, AppendBytes(want, nil, true, mp))
						} else {
							require.NoError(t, appendRawFixed(want, make([]byte, typ.TypeSize()), true, mp))
						}
					}
					got := NewVec(typ, mp)
					require.NoError(t, got.ScatterByMask(compact, mask, n, mp), name)
					requireSameRows(t, want, got)

					for _, v := range []*Vector{src, compact, want, got} {
						v.Free(mp)
					}
					mask.Free()
				}
			}
		}
	}
	require.Zero(t, mp.CurrNB())
}

func TestScatterByMaskConst(t *testing.T) {
	mp := mpool.MustNewZero()
	mask := new(bitmap.Bitmap)
	require.NoError(t, mask.InitWithSize(130, mp))
	for _, r := range []uint64{0, 3, 64, 65, 129} {
		mask.Add(r)
	}
	check := func(w *Vector, value func(r int) (int64, bool)) {
		got := NewVec(types.T_int64.ToType(), mp)
		require.NoError(t, got.ScatterByMask(w, mask, 130, mp))
		require.Equal(t, 130, got.Length())
		for r := 0; r < 130; r++ {
			want, null := value(r)
			require.Equal(t, null, got.IsNull(uint64(r)), "row %d", r)
			if !null {
				require.Equal(t, want, GetFixedAtNoTypeCheck[int64](got, r), "row %d", r)
			}
		}
		got.Free(mp)
	}

	seven, err := NewConstFixed(types.T_int64.ToType(), int64(7), 5, mp)
	require.NoError(t, err)
	check(seven, func(r int) (int64, bool) { return 7, !mask.Contains(uint64(r)) })
	seven.Free(mp)

	null := NewConstNull(types.T_int64.ToType(), 5, mp)
	check(null, func(int) (int64, bool) { return 0, true })
	null.Free(mp)

	str := NewConstNull(types.T_varchar.ToType(), 5, mp)
	got := NewVec(types.T_varchar.ToType(), mp)
	require.NoError(t, got.ScatterByMask(str, mask, 130, mp))
	require.Equal(t, 130, got.Length())
	for r := 0; r < 130; r++ {
		require.True(t, got.IsNull(uint64(r)))
	}
	got.Free(mp)
	str.Free(mp)

	// Too few source rows for the selection.
	short := NewVec(types.T_int64.ToType(), mp)
	require.NoError(t, AppendFixed(short, int64(1), false, mp))
	got = NewVec(types.T_int64.ToType(), mp)
	require.Error(t, got.ScatterByMask(short, mask, 130, mp))
	got.Free(mp)
	short.Free(mp)

	mask.Free()
	require.Zero(t, mp.CurrNB())
}
