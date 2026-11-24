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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// A vector allocates only from the pool it was created with: every call that
// passes a pool must pass exactly that one.
func TestVectorRejectsForeignPool(t *testing.T) {
	mp := mpool.MustNewZero()
	other := mpool.MustNewZero()
	vec := NewVec(types.T_int64.ToType(), mp)
	require.Same(t, mp, vec.MPool())
	require.False(t, vec.IsReadOnly())

	require.Error(t, AppendFixed(vec, int64(1), false, other))
	require.Panics(t, func() { _ = AppendFixed(vec, int64(1), false, nil) })
	require.Zero(t, other.CurrNB())
	require.NoError(t, AppendFixed(vec, int64(1), false, mp))
	require.Panics(t, func() { vec.Free(other) })

	vec.Free(mp)
	require.Zero(t, mp.CurrNB())
}

// A nil pool makes a read-only reference vector: it never allocates, never
// mutates, and Free only drops its references.
func TestReadOnlyVectorIsImmutable(t *testing.T) {
	mp := mpool.MustNewZero()
	owned := NewVec(types.T_int64.ToType(), mp)
	require.NoError(t, AppendFixedList(owned, []int64{1, 2, 3}, nil, mp))
	encoded, err := owned.MarshalBinary()
	require.NoError(t, err)

	ro := NewVec(types.T_any.ToType(), nil)
	require.True(t, ro.IsReadOnly())
	require.NoError(t, ro.UnmarshalBinary(encoded))
	require.Equal(t, []int64{1, 2, 3}, MustFixedColNoTypeCheck[int64](ro))
	// No-copy: the read-only vector references the encoding.
	require.True(t, byteSlicesOverlap(ro.GetData(), encoded))

	require.Error(t, AppendFixed(ro, int64(4), false, mp))
	require.Error(t, ro.Shrink([]int64{0}, false, mp))
	require.Equal(t, 3, ro.Length())

	// The window of a read-only vector is another read-only reference.
	w, err := ro.Window(1, 3)
	require.NoError(t, err)
	require.True(t, w.IsReadOnly())
	require.Equal(t, []int64{2, 3}, MustFixedColNoTypeCheck[int64](w))
	w.Free(mp)

	// A copy is owned by the pool it is made in.
	dup, err := ro.Dup(mp)
	require.NoError(t, err)
	require.Same(t, mp, dup.MPool())
	require.NoError(t, AppendFixed(dup, int64(4), false, mp))
	dup.Free(mp)

	ro.Free(mp)
	owned.Free(mp)
	require.Zero(t, mp.CurrNB())
}

// A window is a read-only reference to its source's rows, owned by no pool;
// CloneWindow and decodes into a vector with a pool copy into that pool.
func TestOwnedWindowAndDecodeCopy(t *testing.T) {
	mp := mpool.MustNewZero()
	src := NewVec(types.T_varchar.ToType(), mp)
	require.NoError(t, AppendBytesList(src, [][]byte{
		[]byte("a"), []byte("a value longer than the inline varlena size"), []byte("c"),
	}, nil, mp))
	allocated := mp.CurrNB()

	w, err := src.Window(1, 3)
	require.NoError(t, err)
	require.True(t, w.IsReadOnly())
	require.Equal(t, allocated, mp.CurrNB())
	require.Equal(t, "a value longer than the inline varlena size", w.GetStringAt(0))
	require.True(t, byteSlicesOverlap(w.GetData(), src.GetData()))
	require.Error(t, AppendBytes(w, []byte("x"), false, mp))
	w.Free(nil)

	c, err := src.CloneWindow(1, 3, mp)
	require.NoError(t, err)
	require.Same(t, mp, c.MPool())
	require.Equal(t, "a value longer than the inline varlena size", c.GetStringAt(0))
	require.False(t, byteSlicesOverlap(c.GetData(), src.GetData()))
	require.False(t, byteSlicesOverlap(c.GetArea(), src.GetArea()))
	c.Free(mp)

	encoded, err := src.MarshalBinary()
	require.NoError(t, err)
	decoded := NewVec(types.T_any.ToType(), mp)
	require.NoError(t, decoded.UnmarshalBinary(encoded))
	require.False(t, byteSlicesOverlap(decoded.GetArea(), encoded))
	for i := range encoded {
		encoded[i] = 0
	}
	require.Equal(t, "a value longer than the inline varlena size", decoded.GetStringAt(1))
	decoded.Free(mp)

	src.Free(mp)
	require.Zero(t, mp.CurrNB())
}

// A vector's null bitmap comes from the vector's pool and goes back in Free.
func TestVectorNullsUseVectorPool(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := NewVec(types.T_int64.ToType(), mp)
	require.Same(t, mp, vec.GetNulls().MPool())
	for i := range 1000 {
		require.NoError(t, AppendFixed(vec, int64(i), i%7 == 0, mp))
	}
	withNulls := mp.CurrNB()
	dup, err := vec.Dup(mp)
	require.NoError(t, err)
	require.Same(t, mp, dup.GetNulls().MPool())
	require.Equal(t, vec.GetNulls().ToArray(), dup.GetNulls().ToArray())
	dup.Free(mp)
	require.Equal(t, withNulls, mp.CurrNB())
	vec.Free(mp)
	require.Zero(t, mp.CurrNB(), "Free returns the null bitmap's words")
}

// A window's nulls are a read-only view of the source's bitmap at any start:
// nothing is allocated or copied, and writing them panics.
func TestWindowNullsAreAView(t *testing.T) {
	mp := mpool.MustNewZero()
	src := NewVec(types.T_int32.ToType(), mp)
	defer src.Free(mp)
	for i := range 300 {
		require.NoError(t, AppendFixed(src, int32(i), i%5 == 0 || i == 299, mp))
	}
	allocated := mp.CurrNB()
	for _, rng := range [][2]int{{0, 300}, {1, 64}, {63, 65}, {64, 128}, {100, 300}, {150, 150}} {
		w, err := src.Window(rng[0], rng[1])
		require.NoError(t, err)
		require.Equal(t, allocated, mp.CurrNB(), "a window allocates nothing")
		require.True(t, w.GetNulls().IsReadOnly())
		require.Equal(t, src.GetNulls().CountRange(uint64(rng[0]), uint64(rng[1])), w.GetNulls().Count())
		for row := range rng[1] - rng[0] {
			require.Equal(t, src.IsNull(uint64(rng[0]+row)), w.IsNull(uint64(row)))
		}
		if !w.GetNulls().IsEmpty() {
			require.Panics(t, func() { w.GetNulls().Del(w.GetNulls().ToArray()[0]) })
		}
		// A copy of the window is owned and exact.
		c, err := w.CloneWindow(0, w.Length(), mp)
		require.NoError(t, err)
		require.Equal(t, w.GetNulls().ToArray(), c.GetNulls().ToArray())
		c.Free(mp)
		w.Free(nil)
	}
	require.Equal(t, allocated, mp.CurrNB())
}

// A read-only decode references the encoded null bitmap instead of copying
// it, and the read-only bitmap refuses writes.
func TestReadOnlyDecodeSharesNullBitmap(t *testing.T) {
	mp := mpool.MustNewZero()
	src := NewVec(types.T_int64.ToType(), mp)
	defer src.Free(mp)
	require.NoError(t, AppendFixedList(src, []int64{1, 2, 3}, []bool{false, true, false}, mp))
	encoded, err := src.MarshalBinary()
	require.NoError(t, err)

	ro := NewVec(types.T_any.ToType(), nil)
	require.NoError(t, ro.UnmarshalBinary(encoded))
	defer ro.Free(nil)
	require.True(t, ro.IsNull(1))
	require.True(t, ro.GetNulls().IsReadOnly())
	require.Panics(t, func() { ro.GetNulls().Add(0) })
	require.Panics(t, func() { ro.GetNulls().Del(1) })

	// Clearing the encoded words is visible: the bitmap is not a copy.
	clear(encoded[len(encoded)-1-8 : len(encoded)-1])
	require.False(t, ro.IsNull(1))
}
