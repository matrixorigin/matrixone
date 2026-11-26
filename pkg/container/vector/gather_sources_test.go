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
	"math/rand"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// randomSourceRows picks n (source, row) pairs over the given sources.
func randomSourceRows(rng *rand.Rand, n int, sources []*Vector) ([]*Vector, []int64) {
	srcs := make([]*Vector, n)
	rows := make([]int64, n)
	for i := range srcs {
		w := sources[rng.Intn(len(sources))]
		srcs[i], rows[i] = w, int64(rng.Intn(w.Length()))
	}
	return srcs, rows
}

// UnionRowsFrom matches UnionOne row by row: for every width, NULL rate,
// destination prefix (unaligned NULL words) and list length, with rows drawn
// from several sources.
func TestUnionRowsFromMatchesUnionOne(t *testing.T) {
	mp := mpool.MustNewZero()
	rng := rand.New(rand.NewSource(11))
	for _, typ := range gatherTestTypes {
		for _, rate := range []float64{0, 0.3, 1} {
			sources := make([]*Vector, 4)
			for i := range sources {
				sources[i] = unionMaskSource(t, rng, typ, 50+i*40, rate, mp)
			}
			for _, pre := range []int{0, 1, 63, 64, 100} {
				for _, n := range []int{1, 63, 64, 65, 300} {
					srcs, rows := randomSourceRows(rng, n, sources)
					dst := unionMaskSource(t, rng, typ, pre, 0.3, mp)
					want, err := dst.Dup(mp)
					require.NoError(t, err)
					for i := range srcs {
						require.NoError(t, want.UnionOne(srcs[i], rows[i], mp))
					}
					require.NoError(t, dst.UnionRowsFrom(srcs, rows, mp))
					requireSameRows(t, want, dst)
					dst.Free(mp)
					want.Free(mp)
				}
			}
			for _, w := range sources {
				w.Free(mp)
			}
		}
	}
	require.Zero(t, mp.CurrNB())
}

// Windows (read-only NULL views), constants, a source that is the
// destination, and a different type all give the UnionOne result.
func TestUnionRowsFromUnusualSources(t *testing.T) {
	mp := mpool.MustNewZero()
	rng := rand.New(rand.NewSource(12))
	for _, typ := range []types.Type{types.T_int64.ToType(), types.T_varchar.ToType()} {
		base := unionMaskSource(t, rng, typ, 300, 0.3, mp)
		window, err := base.Window(17, 217)
		require.NoError(t, err)
		other := unionMaskSource(t, rng, typ, 80, 0.3, mp)
		constNull := NewConstNull(typ, 10, mp)
		for _, sources := range [][]*Vector{{window, other}, {window, constNull, other}} {
			srcs, rows := randomSourceRows(rng, 200, sources)
			dst := unionMaskSource(t, rng, typ, 5, 0.3, mp)
			want, err := dst.Dup(mp)
			require.NoError(t, err)
			for i := range srcs {
				require.NoError(t, want.UnionOne(srcs[i], rows[i], mp))
			}
			require.NoError(t, dst.UnionRowsFrom(srcs, rows, mp))
			requireSameRows(t, want, dst)

			// The destination as one of its own sources.
			self := []*Vector{dst, other, dst}
			selfRows := []int64{0, 1, int64(dst.Length() - 1)}
			want2, err := dst.Dup(mp)
			require.NoError(t, err)
			for i := range self {
				src := self[i]
				if src == dst {
					src = want2
				}
				require.NoError(t, want2.UnionOne(src, selfRows[i], mp))
			}
			require.NoError(t, dst.UnionRowsFrom(self, selfRows, mp))
			requireSameRows(t, want2, dst)
			want2.Free(mp)
			dst.Free(mp)
			want.Free(mp)
		}
		constNull.Free(mp)
		other.Free(mp)
		window.Free(mp)
		base.Free(mp)
	}
	require.Zero(t, mp.CurrNB())
}

func TestUnionRowsFromErrors(t *testing.T) {
	mp := mpool.MustNewZero()
	other := mpool.MustNewZero()
	src := NewVec(types.T_int64.ToType(), mp)
	require.NoError(t, AppendFixedList(src, []int64{1, 2}, nil, mp))
	dst := NewVec(types.T_int64.ToType(), mp)
	require.NoError(t, dst.UnionRowsFrom(nil, nil, mp))
	require.Error(t, dst.UnionRowsFrom([]*Vector{src}, nil, mp))
	require.Error(t, dst.UnionRowsFrom([]*Vector{src, src}, []int64{0, 1}, other))
	require.Error(t, dst.UnionRowsFrom([]*Vector{nil}, []int64{0}, mp))
	require.Equal(t, 0, dst.Length())
	dst.Free(mp)
	src.Free(mp)
	require.Zero(t, mp.CurrNB())
}
