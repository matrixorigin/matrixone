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
	"bytes"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// unionMaskSource builds a flat vector of n rows of typ with some NULLs.
func unionMaskSource(t *testing.T, rng *rand.Rand, typ types.Type, n int, nullRate float64, mp *mpool.MPool) *Vector {
	v := NewVec(typ, mp)
	for i := 0; i < n; i++ {
		isNull := rng.Float64() < nullRate
		var err error
		if typ.IsVarlen() {
			// Mix inline (short) and out-of-line (long) values.
			s := fmt.Sprintf("v%d", i)
			if i%3 == 0 {
				s = strings.Repeat(s, 10)
			}
			err = AppendBytes(v, []byte(s), isNull, mp)
		} else {
			raw := make([]byte, typ.TypeSize())
			rng.Read(raw)
			if typ.Oid == types.T_bool {
				raw[0] &= 1
			}
			err = appendRawFixed(v, raw, isNull, mp)
		}
		require.NoError(t, err)
	}
	return v
}

// appendRawFixed appends one fixed-width value given by its bytes.
func appendRawFixed(v *Vector, raw []byte, isNull bool, mp *mpool.MPool) error {
	if err := v.PreExtend(1, mp); err != nil {
		return err
	}
	n := v.length
	tlen := v.typ.TypeSize()
	v.setLengthAfterExtend(n + 1)
	copy(v.data[n*tlen:(n+1)*tlen], raw)
	if isNull {
		v.nsp.Add(uint64(n))
	}
	return nil
}

func requireSameRows(t *testing.T, want, got *Vector) {
	t.Helper()
	require.Equal(t, want.Length(), got.Length())
	for i := 0; i < want.Length(); i++ {
		require.Equal(t, want.IsNull(uint64(i)), got.IsNull(uint64(i)), "null at %d", i)
		if want.IsNull(uint64(i)) {
			continue
		}
		if want.GetType().IsVarlen() {
			require.True(t, bytes.Equal(want.GetBytesAt(i), got.GetBytesAt(i)), "row %d", i)
		} else {
			tlen := want.GetType().TypeSize()
			require.Equal(t, want.data[i*tlen:(i+1)*tlen], got.data[i*tlen:(i+1)*tlen], "row %d", i)
		}
	}
}

func TestUnionByMaskMatchesUnion(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
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
					// The mask sometimes covers more rows than the source.
					maskLen := n + rng.Intn(3)*40
					mask := new(bitmap.Bitmap)
					require.NoError(t, mask.InitWithSize(int64(maskLen), mp))
					var sels []int64
					for r := 0; r < maskLen; r++ {
						if rng.Float64() < p {
							mask.Add(uint64(r))
							if r < n {
								sels = append(sels, int64(r))
							}
						}
					}
					// Both start from a non-empty prefix.
					want := unionMaskSource(t, rng, typ, 3, 0, mp)
					got, err := want.Dup(mp)
					require.NoError(t, err)
					require.NoError(t, want.Union(src, sels, mp), name)
					require.NoError(t, got.UnionByMask(src, mask, mp), name)
					requireSameRows(t, want, got)

					want.Free(mp)
					got.Free(mp)
					src.Free(mp)
					mask.Free()
				}
			}
		}
	}
	require.Zero(t, mp.CurrNB())
}

func TestUnionByMaskConstAndSelf(t *testing.T) {
	mp := mpool.MustNewZero()
	mask := new(bitmap.Bitmap)
	require.NoError(t, mask.InitWithSize(10, mp))
	for _, r := range []uint64{1, 4, 9} {
		mask.Add(r)
	}

	// A const source appends one copy per selected row.
	c, err := NewConstFixed(types.T_int64.ToType(), int64(42), 10, mp)
	require.NoError(t, err)
	dst := NewVec(types.T_int64.ToType(), mp)
	require.NoError(t, dst.UnionByMask(c, mask, mp))
	require.Equal(t, 3, dst.Length())
	for i := 0; i < 3; i++ {
		require.Equal(t, int64(42), GetFixedAtNoTypeCheck[int64](dst, i))
	}
	nc := NewConstNull(types.T_int64.ToType(), 10, mp)
	require.NoError(t, dst.UnionByMask(nc, mask, mp))
	require.Equal(t, 6, dst.Length())
	require.True(t, dst.IsNull(4))

	// The source may be the destination itself.
	self := NewVec(types.T_varchar.ToType(), mp)
	for i := 0; i < 10; i++ {
		require.NoError(t, AppendBytes(self, []byte(strings.Repeat(fmt.Sprint(i), 40)), i == 4, mp))
	}
	require.NoError(t, self.UnionByMask(self, mask, mp))
	require.Equal(t, 13, self.Length())
	require.Equal(t, strings.Repeat("1", 40), self.GetStringAt(10))
	require.True(t, self.IsNull(11))
	require.Equal(t, strings.Repeat("9", 40), self.GetStringAt(12))

	// An empty selection appends nothing.
	empty := new(bitmap.Bitmap)
	require.NoError(t, empty.InitWithSize(10, mp))
	require.NoError(t, self.UnionByMask(self, empty, mp))
	require.Equal(t, 13, self.Length())

	for _, v := range []*Vector{c, dst, nc, self} {
		v.Free(mp)
	}
	mask.Free()
	empty.Free()
	require.Zero(t, mp.CurrNB())
}

func BenchmarkUnionByMask(b *testing.B) {
	benchUnionSelection(b, true)
}

// BenchmarkUnionBySels is the path UnionByMask replaces: the selection is
// converted to a row list, then Union gathers it.
func BenchmarkUnionBySels(b *testing.B) {
	benchUnionSelection(b, false)
}

func BenchmarkUnionByMaskDense(b *testing.B) {
	benchUnionSelectionAt(b, true, 0.95)
}

func BenchmarkUnionBySelsDense(b *testing.B) {
	benchUnionSelectionAt(b, false, 0.95)
}

func benchUnionSelection(b *testing.B, byMask bool) {
	benchUnionSelectionAt(b, byMask, 0.5)
}

func benchUnionSelectionAt(b *testing.B, byMask bool, p float64) {
	rng := rand.New(rand.NewSource(8))
	mp := mpool.MustNewZero()
	src := NewVec(types.T_int64.ToType(), mp)
	for i := 0; i < 8192; i++ {
		_ = AppendFixed(src, int64(i), false, mp)
	}
	mask := new(bitmap.Bitmap)
	_ = mask.InitWithSize(8192, mp)
	for r := 0; r < 8192; r++ {
		if rng.Float64() < p {
			mask.Add(uint64(r))
		}
	}
	dst := NewVec(types.T_int64.ToType(), mp)
	sels := make([]int64, 0, 8192)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst.CleanOnlyData()
		if byMask {
			_ = dst.UnionByMask(src, mask, mp)
		} else {
			sels = mask.ToI64Array(&sels)
			_ = dst.Union(src, sels, mp)
		}
	}
}

func TestShrinkAssertsAscendingSelsUnderRace(t *testing.T) {
	if !raceBuild {
		t.Skip("the ascending-sels check runs only in race builds")
	}
	mp := mpool.MustNewZero()
	v := NewVec(types.T_int64.ToType(), mp)
	for i := 0; i < 4; i++ {
		require.NoError(t, AppendFixed(v, int64(i), false, mp))
	}
	require.Panics(t, func() { _ = v.Shrink([]int64{2, 1}, false, mp) })
	require.Panics(t, func() { _ = v.Shrink([]int64{1, 1}, true, mp) })
	require.NoError(t, v.Shrink([]int64{0, 2}, false, mp))
	v.Free(mp)
}
