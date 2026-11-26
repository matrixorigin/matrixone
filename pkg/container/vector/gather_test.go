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

// gatherTestTypes cover every payload width the value loop switches on (1, 2,
// 4, 8 and the wide 12, 16, 20, 24, 32 bytes) and the varlena path.
var gatherTestTypes = []types.Type{
	types.T_bool.ToType(), types.T_int16.ToType(), types.T_int32.ToType(),
	types.T_int64.ToType(), types.T_TS.ToType(), types.T_decimal128.ToType(),
	types.T_Blockid.ToType(), types.T_Rowid.ToType(), types.T_decimal256.ToType(),
	types.T_uuid.ToType(), types.T_varchar.ToType(),
}

// randomGatherRows returns n source rows below src, in any order and with
// repeats.
func randomGatherRows(rng *rand.Rand, n, src int) []int64 {
	rows := make([]int64, n)
	for i := range rows {
		rows[i] = int64(rng.Intn(src))
	}
	return rows
}

// Union appends the rows of a row list, NULLs with them, and matches
// UnionOne row by row: for every width, NULL rate, destination prefix
// (unaligned NULL words) and list length.
func TestUnionRowsMatchesUnionOne(t *testing.T) {
	mp := mpool.MustNewZero()
	rng := rand.New(rand.NewSource(7))
	for _, typ := range gatherTestTypes {
		for _, rate := range []float64{0, 0.3, 1} {
			src := unionMaskSource(t, rng, typ, 300, rate, mp)
			for _, pre := range []int{0, 1, 63, 64, 100} {
				for _, n := range []int{1, 63, 64, 65, 500} {
					rows := randomGatherRows(rng, n, src.Length())
					dst := unionMaskSource(t, rng, typ, pre, 0.3, mp)
					want, err := dst.Dup(mp)
					require.NoError(t, err)
					for _, r := range rows {
						require.NoError(t, want.UnionOne(src, r, mp))
					}
					require.NoError(t, dst.Union(src, rows, mp))
					requireSameRows(t, want, dst)
					dst.Free(mp)
					want.Free(mp)
				}
			}
			src.Free(mp)
		}
	}
	require.Zero(t, mp.CurrNB())
}

// A source whose NULL bitmap is a read-only, shifted view (a window) is read
// through Word.
func TestUnionRowsFromWindow(t *testing.T) {
	mp := mpool.MustNewZero()
	rng := rand.New(rand.NewSource(8))
	for _, typ := range []types.Type{types.T_int64.ToType(), types.T_varchar.ToType()} {
		base := unionMaskSource(t, rng, typ, 400, 0.3, mp)
		src, err := base.Window(37, 337)
		require.NoError(t, err)
		rows := randomGatherRows(rng, 500, src.Length())
		dst := NewVec(typ, mp)
		want := NewVec(typ, mp)
		for _, r := range rows {
			require.NoError(t, want.UnionOne(src, r, mp))
		}
		require.NoError(t, dst.Union(src, rows, mp))
		requireSameRows(t, want, dst)
		dst.Free(mp)
		want.Free(mp)
		src.Free(mp)
		base.Free(mp)
	}
	require.Zero(t, mp.CurrNB())
}

// Constant sources broadcast, and a constant NULL makes every gathered row
// NULL.
func TestUnionRowsConstSource(t *testing.T) {
	mp := mpool.MustNewZero()
	rows := []int64{0, 0, 0}

	cn := NewConstNull(types.T_int64.ToType(), 5, mp)
	dst := NewVec(types.T_int64.ToType(), mp)
	require.NoError(t, AppendFixed(dst, int64(9), false, mp))
	require.NoError(t, dst.Union(cn, rows, mp))
	require.Equal(t, 4, dst.Length())
	require.False(t, dst.IsNull(0))
	for i := 1; i < 4; i++ {
		require.True(t, dst.IsNull(uint64(i)))
	}
	dst.Free(mp)
	cn.Free(mp)

	cv, err := NewConstBytes(types.T_varchar.ToType(), []byte("a long value that is not inline"), 5, mp)
	require.NoError(t, err)
	dst = NewVec(types.T_varchar.ToType(), mp)
	require.NoError(t, dst.Union(cv, rows, mp))
	for i := 0; i < 3; i++ {
		require.Equal(t, "a long value that is not inline", dst.GetStringAt(i))
	}
	dst.Free(mp)
	cv.Free(mp)
	require.Zero(t, mp.CurrNB())
}

// Gathering a vector into itself reads the rows as they were before the
// call.
func TestUnionRowsIntoItself(t *testing.T) {
	mp := mpool.MustNewZero()
	rng := rand.New(rand.NewSource(9))
	for _, typ := range []types.Type{types.T_int32.ToType(), types.T_varchar.ToType()} {
		v := unionMaskSource(t, rng, typ, 100, 0.3, mp)
		rows := randomGatherRows(rng, 150, v.Length())
		want, err := v.Dup(mp)
		require.NoError(t, err)
		for _, r := range rows {
			require.NoError(t, want.UnionOne(v, r, mp))
		}
		require.NoError(t, v.Union(v, rows, mp))
		requireSameRows(t, want, v)
		v.Free(mp)
		want.Free(mp)
	}
	require.Zero(t, mp.CurrNB())
}

// An empty row list changes nothing; a destination from another pool is
// rejected.
func TestUnionRowsEmptyAndPoolMismatch(t *testing.T) {
	mp := mpool.MustNewZero()
	other := mpool.MustNewZero()
	src := NewVec(types.T_int64.ToType(), mp)
	require.NoError(t, AppendFixed(src, int64(1), true, mp))
	dst := NewVec(types.T_int64.ToType(), mp)
	require.NoError(t, dst.Union(src, nil, mp))
	require.Equal(t, 0, dst.Length())
	require.Error(t, dst.Union(src, []int64{0}, other))
	dst.Free(mp)
	src.Free(mp)
	require.Zero(t, mp.CurrNB())
}
