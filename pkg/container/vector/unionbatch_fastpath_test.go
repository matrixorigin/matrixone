// Copyright 2022 Matrix Origin
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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// TestUnionBatchVarlenFastPath exercises the full-append fast path: mixed inline
// (short) + non-inline (long) values, unioned into a non-empty target so the
// offset-rebase (baseOff != 0) runs, and cross-checked value-by-value.
func TestUnionBatchVarlenFastPath(t *testing.T) {
	mp := mpool.MustNewZero()
	const n = 500
	src := func() []string {
		out := make([]string, n)
		for i := 0; i < n; i++ {
			if i%3 == 0 {
				out[i] = fmt.Sprintf("s%d", i) // inline (<=23 bytes)
			} else {
				out[i] = fmt.Sprintf("long-%d-", i) + string(make([]byte, 800)) // non-inline
			}
		}
		return out
	}()

	w := NewVec(types.T_varchar.ToType())
	for _, s := range src {
		require.NoError(t, AppendBytes(w, []byte(s), false, mp))
	}

	// seed target with a few rows so baseOff != 0 on the batched union.
	v := NewVec(types.T_varchar.ToType())
	seed := []string{"seed-a", "seed-" + string(make([]byte, 900))}
	for _, s := range seed {
		require.NoError(t, AppendBytes(v, []byte(s), false, mp))
	}

	// full-append fast path: offset=0, cnt=w.length, flags=nil, no nulls/grouping.
	require.NoError(t, v.UnionBatch(w, 0, w.Length(), nil, mp))

	require.Equal(t, len(seed)+n, v.Length())
	for i, s := range seed {
		require.Equalf(t, s, string(v.GetBytesAt(i)), "seed row %d", i)
	}
	for i, s := range src {
		require.Equalf(t, s, string(v.GetBytesAt(len(seed)+i)), "src row %d", i)
	}

	// equivalence oracle: same union built via UnionOne (per-row, general path).
	ref := NewVec(types.T_varchar.ToType())
	for _, s := range seed {
		require.NoError(t, AppendBytes(ref, []byte(s), false, mp))
	}
	for i := 0; i < w.Length(); i++ {
		require.NoError(t, ref.UnionOne(w, int64(i), mp))
	}
	require.Equal(t, ref.Length(), v.Length())
	for i := 0; i < v.Length(); i++ {
		require.Equalf(t, string(ref.GetBytesAt(i)), string(v.GetBytesAt(i)), "row %d vs oracle", i)
	}

	w.Free(mp)
	v.Free(mp)
	ref.Free(mp)
}

// TestUnionBroadcastAndPregrow covers the const-broadcast doubling fill and the
// sels-gather area pre-grow (UnionMulti / Union / UnionBatch / AppendMultiFixed),
// with mixed inline+non-inline values and nulls, cross-checked against per-row
// UnionOne (the known-correct path) and direct expectations.
func TestUnionBroadcastAndPregrow(t *testing.T) {
	mp := mpool.MustNewZero()
	const n = 400
	vals := make([]string, n)
	isNull := make([]bool, n)
	for i := 0; i < n; i++ {
		switch i % 4 {
		case 0:
			vals[i] = fmt.Sprintf("s%d", i) // inline
		case 1:
			vals[i] = "L" + fmt.Sprintf("%d", i) + string(make([]byte, 700)) // non-inline
		case 2:
			isNull[i] = true
		default:
			vals[i] = fmt.Sprintf("m%d-%s", i, string(make([]byte, 40))) // non-inline, mid
		}
	}
	src := NewVec(types.T_varchar.ToType())
	for i := 0; i < n; i++ {
		require.NoError(t, AppendBytes(src, []byte(vals[i]), isNull[i], mp))
	}
	get := func(v *Vector, i int) (string, bool) {
		if v.GetNulls().Contains(uint64(i)) {
			return "", true
		}
		return string(v.GetBytesAt(i)), false
	}

	// 1) Union (unionT sels-gather, pre-grow path) into a seeded target, reverse order.
	{
		v := NewVec(types.T_varchar.ToType())
		require.NoError(t, AppendBytes(v, []byte("seed"+string(make([]byte, 900))), false, mp))
		sels := make([]int64, n)
		for i := range sels {
			sels[i] = int64(n - 1 - i)
		}
		require.NoError(t, v.Union(src, sels, mp))
		require.Equal(t, 1+n, v.Length())
		s, nu := get(v, 0)
		require.False(t, nu)
		require.Equal(t, "seed"+string(make([]byte, 900)), s)
		for i, sel := range sels {
			gs, gn := get(v, 1+i)
			require.Equalf(t, isNull[sel], gn, "Union null row %d", i)
			if !gn {
				require.Equalf(t, vals[sel], gs, "Union row %d", i)
			}
		}
	}

	// 2) UnionMulti broadcast (varlen) of one non-inline row, large cnt.
	{
		v := NewVec(types.T_varchar.ToType())
		require.NoError(t, v.UnionMulti(src, 1, 333, mp)) // src[1] is non-inline
		require.Equal(t, 333, v.Length())
		for i := 0; i < 333; i++ {
			require.Equal(t, vals[1], string(v.GetBytesAt(i)))
		}
	}

	// 3) UnionBatch with nulls (general null branch + pre-grow), offset 0.
	{
		v := NewVec(types.T_varchar.ToType())
		require.NoError(t, v.UnionBatch(src, 0, n, nil, mp))
		require.Equal(t, n, v.Length())
		for i := 0; i < n; i++ {
			gs, gn := get(v, i)
			require.Equalf(t, isNull[i], gn, "UnionBatch null row %d", i)
			if !gn {
				require.Equalf(t, vals[i], gs, "UnionBatch row %d", i)
			}
		}
	}

	// 4) AppendMultiFixed broadcast (fillSlice on a fixed type), large cnt.
	{
		v := NewVec(types.T_int64.ToType())
		require.NoError(t, AppendMultiFixed(v, int64(0x1122334455667788), false, 1000, mp))
		require.Equal(t, 1000, v.Length())
		col := MustFixedColNoTypeCheck[int64](v)
		for i := 0; i < 1000; i++ {
			require.Equalf(t, int64(0x1122334455667788), col[i], "AppendMultiFixed row %d", i)
		}
	}
	src.Free(mp)
}

func BenchmarkConstBroadcastFill(b *testing.B) {
	const cnt = 8192
	var va types.Varlena
	va.SetOffsetLen(12345, 678)
	dst := make([]types.Varlena, cnt)
	b.Run("scalar", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for j := 0; j < cnt; j++ {
				dst[j] = va
			}
		}
	})
	b.Run("doubling", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			fillSlice(dst, 0, cnt, va)
		}
	})
}

// TestUnionBatchNullFastPath exercises the UnionBatch full-append fast path with
// nulls and grouping bits, appended into a non-empty target (baseOff != 0 so the
// offset-rebase and null-header-clear interact), cross-checked against per-row
// UnionOne (which handles nulls/grouping correctly).
func TestUnionBatchNullFastPath(t *testing.T) {
	mp := mpool.MustNewZero()
	const n = 300
	build := func() *Vector {
		w := NewVec(types.T_varchar.ToType())
		for i := 0; i < n; i++ {
			var b []byte
			null := false
			switch i % 5 {
			case 0:
				b = []byte(fmt.Sprintf("s%d", i)) // inline
			case 1:
				b = append([]byte(fmt.Sprintf("L%d-", i)), make([]byte, 600)...) // non-inline
			case 2:
				null = true // null
			default:
				b = []byte(fmt.Sprintf("m%d", i))
			}
			require.NoError(t, AppendBytes(w, b, null, mp))
		}
		// set a few grouping bits (independent of nulls)
		nulls.Add(&w.gsp, 3, 7, 100, 299)
		return w
	}
	w := build()

	// fast path: append all of w into a seeded (non-empty) target.
	v := NewVec(types.T_varchar.ToType())
	require.NoError(t, AppendBytes(v, append([]byte("seed-"), make([]byte, 500)...), false, mp))
	require.NoError(t, v.UnionBatch(w, 0, w.Length(), nil, mp))

	// reference via per-row UnionOne.
	ref := NewVec(types.T_varchar.ToType())
	require.NoError(t, AppendBytes(ref, append([]byte("seed-"), make([]byte, 500)...), false, mp))
	for i := 0; i < w.Length(); i++ {
		require.NoError(t, ref.UnionOne(w, int64(i), mp))
	}

	require.Equal(t, ref.Length(), v.Length())
	for i := 0; i < v.Length(); i++ {
		rn := ref.GetNulls().Contains(uint64(i))
		vn := v.GetNulls().Contains(uint64(i))
		require.Equalf(t, rn, vn, "nsp row %d", i)
		require.Equalf(t, ref.GetGrouping().Contains(uint64(i)), v.GetGrouping().Contains(uint64(i)), "gsp row %d", i)
		if !rn {
			require.Equalf(t, string(ref.GetBytesAt(i)), string(v.GetBytesAt(i)), "value row %d", i)
		}
	}

	// edge: all-null source.
	{
		aw := NewVec(types.T_varchar.ToType())
		for i := 0; i < 50; i++ {
			require.NoError(t, AppendBytes(aw, nil, true, mp))
		}
		av := NewVec(types.T_varchar.ToType())
		require.NoError(t, av.UnionBatch(aw, 0, aw.Length(), nil, mp))
		require.Equal(t, 50, av.Length())
		for i := 0; i < 50; i++ {
			require.True(t, av.GetNulls().Contains(uint64(i)))
		}
		aw.Free(mp)
		av.Free(mp)
	}
	w.Free(mp)
	v.Free(mp)
	ref.Free(mp)
}
