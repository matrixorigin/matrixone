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

// TestUnionBatchVarlenContiguousRangeFastPath covers the partial-range shape
// used when a wide loop-join result is split into bounded output batches. The
// source payload is contiguous, but offset != 0 and cnt < source length, so it
// cannot use the whole-vector fast path.
func TestUnionBatchVarlenContiguousRangeFastPath(t *testing.T) {
	mp := mpool.MustNewZero()
	const (
		rows  = 300
		dims  = 768
		start = 41
		count = 173
	)

	src := NewVec(types.T_array_float32.ToType())
	for row := 0; row < rows; row++ {
		isNull := row%29 == 0
		value := make([]float32, dims)
		if !isNull {
			for col := range value {
				value[col] = float32(row*1000 + col)
			}
		}
		require.NoError(t, AppendArray(src, value, isNull, mp))
	}
	nulls.Add(&src.gsp, uint64(start+3), uint64(start+count-1))

	dst := NewVec(types.T_array_float32.ToType())
	require.NoError(t, AppendArray(dst, []float32{1, 2, 3}, false, mp))
	require.NoError(t, dst.UnionBatch(src, start, count, nil, mp))

	ref := NewVec(types.T_array_float32.ToType())
	require.NoError(t, AppendArray(ref, []float32{1, 2, 3}, false, mp))
	for row := start; row < start+count; row++ {
		require.NoError(t, ref.UnionOne(src, int64(row), mp))
	}

	require.Equal(t, ref.Length(), dst.Length())
	for row := 0; row < dst.Length(); row++ {
		isNull := ref.IsNull(uint64(row))
		require.Equalf(t, isNull, dst.IsNull(uint64(row)), "null row %d", row)
		require.Equalf(t,
			ref.GetGrouping().Contains(uint64(row)),
			dst.GetGrouping().Contains(uint64(row)),
			"grouping row %d", row)
		if !isNull {
			require.Equalf(t,
				GetArrayAt[float32](ref, row),
				GetArrayAt[float32](dst, row),
				"value row %d", row)
		}
	}

	src.Free(mp)
	dst.Free(mp)
	ref.Free(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

// TestUnionBatchVarlenWindowCopiesOnlyLiveArea is the regression guard for
// borrowed windows retaining their parent's complete area. UnionBatch receives
// the window as a whole vector (offset 0, cnt == length), but must copy only the
// payload referenced by the window's live descriptors.
func TestUnionBatchVarlenWindowCopiesOnlyLiveArea(t *testing.T) {
	mp := mpool.MustNewZero()
	const (
		rows  = 300
		dims  = 64
		start = 101
		count = 37
	)
	src := NewVec(types.T_array_float32.ToType())
	value := make([]float32, dims)
	for row := 0; row < rows; row++ {
		value[0] = float32(row)
		require.NoError(t, AppendArray(src, value, false, mp))
	}
	window, err := src.Window(start, start+count)
	require.NoError(t, err)
	require.Len(t, window.GetArea(), rows*dims*4)

	dst := NewVec(types.T_array_float32.ToType())
	require.NoError(t, dst.UnionBatch(window, 0, window.Length(), nil, mp))
	require.Len(t, dst.GetArea(), count*dims*4)
	for row := 0; row < count; row++ {
		require.Equal(t,
			GetArrayAt[float32](src, start+row),
			GetArrayAt[float32](dst, row))
	}

	window.Free(mp)
	src.Free(mp)
	dst.Free(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

// TestUnionBatchVarlenRangeFallsBackForReorderedArea verifies that a valid
// vector whose row headers do not follow area order still uses the general
// per-row path and preserves its logical values.
func TestUnionBatchVarlenRangeFallsBackForReorderedArea(t *testing.T) {
	mp := mpool.MustNewZero()
	src := NewVec(types.T_varchar.ToType())
	for i := 0; i < 8; i++ {
		require.NoError(t, AppendBytes(src,
			[]byte(fmt.Sprintf("row-%d-", i)+string(make([]byte, 128))),
			false, mp))
	}
	var headers []types.Varlena
	ToSliceNoTypeCheck(src, &headers)
	headers[3], headers[4] = headers[4], headers[3]

	dst := NewVec(types.T_varchar.ToType())
	require.NoError(t, dst.UnionBatch(src, 2, 4, nil, mp))
	require.Equal(t, 4, dst.Length())
	for i := 0; i < 4; i++ {
		require.Equal(t, src.GetBytesAt(2+i), dst.GetBytesAt(i))
	}

	src.Free(mp)
	dst.Free(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestUnionBatchVarlenPartialSelfAppendForcedGrowth(t *testing.T) {
	mp := mpool.MustNewZero()
	vec, values, isNull := newForcedGrowthAliasVector(t, mp)
	const offset = 1
	cnt := vec.Length() - offset
	requireUnionBatchAliasGrowth(t, vec, values, isNull, offset, cnt)

	oldLength := vec.Length()
	require.NoError(t, vec.UnionBatch(vec, offset, cnt, nil, mp))
	assertUnionBatchAliasResult(t, vec, values, isNull, oldLength, offset, cnt)

	vec.Free(mp)
	require.Zero(t, mp.CurrNB())
}

func TestUnionBatchVarlenWindowAliasForcedGrowth(t *testing.T) {
	mp := mpool.MustNewZero()
	vec, values, isNull := newForcedGrowthAliasVector(t, mp)
	const offset = 1
	cnt := vec.Length() - offset
	requireUnionBatchAliasGrowth(t, vec, values, isNull, offset, cnt)

	window, err := vec.Window(offset, offset+cnt)
	require.NoError(t, err)
	oldLength := vec.Length()
	require.NoError(t, vec.UnionBatch(window, 0, window.Length(), nil, mp))
	assertUnionBatchAliasResult(t, vec, values, isNull, oldLength, offset, cnt)

	window.Free(mp)
	vec.Free(mp)
	require.Zero(t, mp.CurrNB())
}

func newForcedGrowthAliasVector(
	t *testing.T,
	mp *mpool.MPool,
) (*Vector, []string, []bool) {
	t.Helper()
	vec := NewVec(types.T_varchar.ToType())
	values := make([]string, 0, 128)
	isNull := make([]bool, 0, 128)
	for row := 0; row < 128; row++ {
		null := row%7 == 3
		value := fmt.Sprintf("row-%03d-", row) + string(make([]byte, 256+row%13))
		require.NoError(t, AppendBytes(vec, []byte(value), null, mp))
		values = append(values, value)
		isNull = append(isNull, null)
		if row < 7 {
			continue
		}
		payloadBytes := 0
		for i := 1; i < len(values); i++ {
			if !isNull[i] {
				payloadBytes += len(values[i])
			}
		}
		selectedRows := len(values) - 1
		if len(vec.area)+payloadBytes > cap(vec.area) &&
			len(vec.data)+selectedRows*vec.typ.TypeSize() > cap(vec.data) {
			nulls.Add(&vec.gsp, 2)
			return vec, values, isNull
		}
	}
	vec.Free(mp)
	t.Fatal("failed to construct a vector that forces data and area growth")
	return nil, nil, nil
}

func requireUnionBatchAliasGrowth(
	t *testing.T,
	vec *Vector,
	values []string,
	isNull []bool,
	offset int,
	cnt int,
) {
	t.Helper()
	payloadBytes := 0
	for row := offset; row < offset+cnt; row++ {
		if !isNull[row] {
			payloadBytes += len(values[row])
		}
	}
	require.Greater(t, len(vec.area)+payloadBytes, cap(vec.area))
	require.Greater(t, len(vec.data)+cnt*vec.typ.TypeSize(), cap(vec.data))
}

func assertUnionBatchAliasResult(
	t *testing.T,
	vec *Vector,
	values []string,
	isNull []bool,
	oldLength int,
	offset int,
	cnt int,
) {
	t.Helper()
	require.Equal(t, oldLength+cnt, vec.Length())
	for i := 0; i < cnt; i++ {
		sourceRow := offset + i
		targetRow := oldLength + i
		require.Equalf(t, isNull[sourceRow], vec.IsNull(uint64(targetRow)),
			"null row %d", targetRow)
		require.Equalf(t,
			vec.GetGrouping().Contains(uint64(sourceRow)),
			vec.GetGrouping().Contains(uint64(targetRow)),
			"grouping row %d", targetRow)
		if !isNull[sourceRow] {
			require.Equalf(t, values[sourceRow], string(vec.GetBytesAt(targetRow)),
				"value row %d", targetRow)
		}
	}
}

func BenchmarkUnionBatchVarlenWindow768(b *testing.B) {
	mp := mpool.MustNewZero()
	const (
		rows  = 16384
		dims  = 768
		start = 4096
		count = 8192
	)
	src := NewVec(types.T_array_float32.ToType())
	value := make([]float32, dims)
	for row := 0; row < rows; row++ {
		value[0] = float32(row)
		if err := AppendArray(src, value, false, mp); err != nil {
			b.Fatal(err)
		}
	}
	window, err := src.Window(start, start+count)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(count * dims * 4)
	dst := NewVec(types.T_array_float32.ToType())
	if err := dst.UnionBatch(window, 0, count, nil, mp); err != nil {
		b.Fatal(err)
	}
	dst.CleanOnlyData()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := dst.UnionBatch(window, 0, count, nil, mp); err != nil {
			b.Fatal(err)
		}
		dst.CleanOnlyData()
	}
	b.StopTimer()
	dst.Free(mp)
	window.Free(mp)
	src.Free(mp)
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

// TestUnionBatchFastPathStaleBitmapBits is the regression guard for the fast-path
// bug where w carried nsp/gsp bits at index >= w.length — a normal reused state,
// since SetLength shrinks length without clearing the bitmaps. The buggy code
// propagated bits via Foreach (which walks every set bit), so a stale nsp bit
// panicked on the header clear (vCol[oldLen+i] out of range) and a stale gsp bit
// leaked a phantom grouping bit. The fix bounds propagation to [0,cnt), matching
// the per-row UnionOne path which only consults [0,cnt). Pre-fix this test panics
// at UnionBatch; post-fix it matches UnionOne with no phantom bits.
func TestUnionBatchFastPathStaleBitmapBits(t *testing.T) {
	mp := mpool.MustNewZero()
	build := func() *Vector {
		w := NewVec(types.T_varchar.ToType())
		for i := 0; i < 20; i++ {
			require.NoError(t, AppendBytes(w, []byte(fmt.Sprintf("r%d", i)), false, mp))
		}
		// in-range bits (must propagate) plus stale bits >= the shrunk length
		// (must be ignored, exactly as the per-row path ignores them).
		nulls.Add(&w.nsp, 3, 15, 18)
		nulls.Add(&w.gsp, 5, 12, 17)
		w.SetLength(10) // length 10; bits 12,15,17,18 are now stale (>= length)
		return w
	}
	w := build()
	require.Equal(t, 10, w.Length())

	// fast path into a non-empty target (baseOff != 0 so the rebase runs too).
	v := NewVec(types.T_varchar.ToType())
	require.NoError(t, AppendBytes(v, []byte("seed"), false, mp))
	require.NoError(t, v.UnionBatch(w, 0, w.Length(), nil, mp)) // panics pre-fix

	// reference: per-row UnionOne, which only consults [0,cnt).
	ref := NewVec(types.T_varchar.ToType())
	require.NoError(t, AppendBytes(ref, []byte("seed"), false, mp))
	for i := 0; i < w.Length(); i++ {
		require.NoError(t, ref.UnionOne(w, int64(i), mp))
	}

	require.Equal(t, ref.Length(), v.Length())
	for i := 0; i < v.Length(); i++ {
		rn := ref.GetNulls().Contains(uint64(i))
		require.Equalf(t, rn, v.GetNulls().Contains(uint64(i)), "nsp row %d", i)
		require.Equalf(t, ref.GetGrouping().Contains(uint64(i)), v.GetGrouping().Contains(uint64(i)), "gsp row %d", i)
		if !rn {
			require.Equalf(t, string(ref.GetBytesAt(i)), string(v.GetBytesAt(i)), "value row %d", i)
		}
	}

	// stale source bits must not leak as phantom bits past the appended range
	// (oldLen == 1 for the single seed row).
	for _, stale := range []uint64{12, 15, 17, 18} {
		require.Falsef(t, v.GetNulls().Contains(1+stale), "phantom nsp bit at %d", 1+stale)
		require.Falsef(t, v.GetGrouping().Contains(1+stale), "phantom gsp bit at %d", 1+stale)
	}

	w.Free(mp)
	v.Free(mp)
	ref.Free(mp)
}

// TestUnionPregrowSkipsNullRows guards the varlena area pre-grow against counting
// null rows. A reused varlen vector can retain a stale non-inline header in a null
// slot (a null append does not overwrite the slot). The union skips null rows, so
// the pre-grow must too — otherwise it reserves area for dead payload, triggering a
// large needless mp.Grow (or an alloc failure) on null-heavy inputs the union path
// would mostly skip. Output is identical with/without the fix, so this asserts on
// the reservation: the only copied (non-null) rows are inline, so v.area must stay
// tiny rather than be grown to fit the stale null-slot headers.
func TestUnionPregrowSkipsNullRows(t *testing.T) {
	mp := mpool.MustNewZero()
	const n = 32
	w := NewVec(types.T_varchar.ToType())
	for i := 0; i < n; i++ {
		require.NoError(t, AppendBytes(w, []byte("x"), false, mp)) // inline: uses no area
	}
	// Plant a stale BIG header claiming a large length in each null slot (as a
	// reused vector would), then mark the row null without clearing the header.
	var wCol []types.Varlena
	ToSliceNoTypeCheck(w, &wCol)
	const staleLen = 1 << 20 // 1 MiB of dead payload per null row
	for i := 1; i < n; i += 2 {
		wCol[i].SetOffsetLen(0, staleLen)
		nulls.Add(&w.nsp, uint64(i))
	}

	// Union all rows (sels-gather -> the pre-grow path). Pre-fix this reserves
	// ~(n/2)*staleLen of area for the dead null-slot payload.
	v := NewVec(types.T_varchar.ToType())
	sels := make([]int64, n)
	for i := range sels {
		sels[i] = int64(i)
	}
	require.NoError(t, v.Union(w, sels, mp))

	require.Lessf(t, cap(v.area), staleLen,
		"pre-grow over-reserved area (cap=%d) from stale null-row headers", cap(v.area))

	// correctness is unchanged: odd rows null, even rows carry "x".
	require.Equal(t, n, v.Length())
	for i := 0; i < n; i++ {
		if i%2 == 1 {
			require.Truef(t, v.GetNulls().Contains(uint64(i)), "row %d should be null", i)
		} else {
			require.Falsef(t, v.GetNulls().Contains(uint64(i)), "row %d should not be null", i)
			require.Equalf(t, "x", string(v.GetBytesAt(i)), "row %d value", i)
		}
	}
	w.Free(mp)
	v.Free(mp)
}
