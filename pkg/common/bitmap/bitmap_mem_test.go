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

package bitmap

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/stretchr/testify/require"
)

func newTestPool(t *testing.T) *mpool.MPool {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB(), "bitmap words leaked") })
	return mp
}

// Words come from the bitmap's pool, survive growth, and go back in Free
// and Reset.
func TestBitmapOwnsPoolWords(t *testing.T) {
	mp := newTestPool(t)
	var n Bitmap
	require.NoError(t, n.InitWithSize(100, mp))
	require.Same(t, mp, n.MPool())
	require.False(t, n.IsReadOnly())
	require.Positive(t, mp.CurrNB())
	n.Add(3)
	n.Add(99)

	require.NoError(t, n.TryExpandWithSize(10000))
	require.True(t, n.Contains(3) && n.Contains(99))
	require.False(t, n.Contains(9999))
	require.Equal(t, 2, n.Count())
	require.NoError(t, n.Reserve(20000))
	require.Equal(t, int64(10000), n.Len())

	n.Reset()
	require.Zero(t, mp.CurrNB())
	require.Same(t, mp, n.MPool(), "Reset keeps the pool")
	require.NoError(t, n.InitWithSize(64, mp))
	n.Free()
	n.Free() // idempotent
	require.Zero(t, mp.CurrNB())

	// InitWithSize reuses a large enough block and clears it.
	require.NoError(t, n.InitWithSize(1000, mp))
	n.AddRange(0, 1000)
	before := mp.CurrNB()
	require.NoError(t, n.InitWithSize(500, mp))
	require.Equal(t, before, mp.CurrNB())
	require.Zero(t, n.Count())
	n.Free()

	other := mpool.MustNewZero()
	require.NoError(t, n.InitWithSize(10, mp))
	require.Error(t, n.InitWithSize(10, other), "an owned bitmap keeps its pool")
	require.Error(t, n.InitWithSize(10, nil))
	n.Free()
}

// A bitmap without a pool cannot be written or grown; reads still work.
func TestBitmapWithoutPoolIsReadOnly(t *testing.T) {
	mp := newTestPool(t)
	var zero Bitmap
	require.True(t, zero.IsReadOnly())
	require.PanicsWithValue(t, ErrReadOnly, func() { zero.AddRange(0, 1) })
	require.Error(t, zero.TryExpandWithSize(1))
	require.Error(t, zero.Reserve(1))
	require.NoError(t, zero.TryExpandWithSize(0), "no growth, no allocation")

	var owned Bitmap
	require.NoError(t, owned.InitWithSize(130, mp))
	owned.Add(1)
	owned.Add(129)
	encoded := owned.Marshal()

	var ro Bitmap
	ro.UnmarshalNoCopy(encoded)
	require.True(t, ro.IsReadOnly())
	require.True(t, ro.Contains(129))
	require.Equal(t, 2, ro.Count())
	for name, write := range map[string]func(){
		"Add":         func() { ro.Add(0) },
		"Remove":      func() { ro.Remove(1) },
		"AddRange":    func() { ro.AddRange(0, 2) },
		"RemoveRange": func() { ro.RemoveRange(0, 2) },
		"Negate":      func() { ro.Negate() },
		"Clear":       func() { ro.Clear() },
		"Or":          func() { ro.Or(&owned) },
		"And":         func() { ro.And(&owned) },
		"Remap":       func() { ro.RemapOrdered([]int64{1}, false) },
	} {
		require.PanicsWithValue(t, ErrReadOnly, write, name)
	}
	require.Equal(t, 2, ro.Count(), "a refused write changes nothing")
	ro.Remove(500) // past Len: nothing to write

	// A copy of a read-only bitmap is owned and writable.
	cl, err := ro.Clone(mp)
	require.NoError(t, err)
	cl.Add(0)
	require.Equal(t, []uint64{0, 1, 129}, cl.ToArray())
	require.Equal(t, []uint64{1, 129}, ro.ToArray())

	// Unmarshal references when there is no pool and copies when there is.
	var ref, cp Bitmap
	require.NoError(t, ref.Unmarshal(encoded))
	require.True(t, ref.IsReadOnly())
	cp.Init(mp)
	require.NoError(t, cp.Unmarshal(encoded))
	require.False(t, cp.IsReadOnly())
	clear(encoded[MarshalHeaderSize:])
	require.False(t, ref.Contains(129), "a read-only bitmap shares the bytes")
	require.True(t, cp.Contains(129), "a pooled bitmap owns a copy")

	cl.Free()
	cp.Free()
	owned.Free()
}

// Swap moves the words together with the pool that owns them.
func TestBitmapSwapMovesOwnership(t *testing.T) {
	mp := newTestPool(t)
	var owned, ro Bitmap
	require.NoError(t, owned.InitWithSize(64, mp))
	owned.Add(5)
	ro.UnmarshalNoCopy(owned.Marshal())
	owned.Swap(&ro)
	require.True(t, owned.IsReadOnly())
	require.Same(t, mp, ro.MPool())
	ro.Add(6)
	owned.Free() // drops a reference, frees nothing
	require.Positive(t, mp.CurrNB())
	ro.Free()
}

// Every read of a view, at any shift and length, matches an owned copy of
// the same rows, and creating it allocates nothing.
func TestBitmapViewMatchesCopy(t *testing.T) {
	mp := newTestPool(t)
	r := rand.New(rand.NewSource(7))
	for _, srcLen := range []int64{1, 63, 64, 65, 200, 1000} {
		for _, density := range []float64{0, 0.02, 0.5, 1} {
			var src Bitmap
			require.NoError(t, src.InitWithSize(srcLen, mp))
			for row := int64(0); row < srcLen; row++ {
				if r.Float64() < density {
					src.Add(uint64(row))
				}
			}
			for trial := 0; trial < 20; trial++ {
				start := r.Int63n(srcLen + 1)
				end := start + r.Int63n(srcLen-start+1)
				t.Run(fmt.Sprintf("%d/%.2f/[%d,%d)", srcLen, density, start, end), func(t *testing.T) {
					checkView(t, mp, &src, start, end)
				})
			}
			src.Free()
		}
	}
}

func checkView(t *testing.T, mp *mpool.MPool, src *Bitmap, start, end int64) {
	allocated := mp.CurrNB()
	var view Bitmap
	view.InitView(src, start, end)
	require.Equal(t, allocated, mp.CurrNB(), "a view allocates nothing")
	require.True(t, view.IsReadOnly())

	var want Bitmap
	require.NoError(t, want.InitWithSize(end-start, mp))
	for row := start; row < end; row++ {
		if src.Contains(uint64(row)) {
			want.Add(uint64(row - start))
		}
	}

	require.Equal(t, want.Len(), view.Len())
	require.Equal(t, want.Count(), view.Count())
	require.Equal(t, want.IsEmpty(), view.IsEmpty())
	require.Equal(t, want.ToArray(), view.ToArray())
	for row := uint64(0); row < uint64(end-start)+2; row++ {
		require.Equal(t, want.Contains(row), view.Contains(row), "row %d", row)
	}
	var rows []uint64
	for it := view.IteratorValue(); it.HasNext(); {
		rows = append(rows, it.Next())
	}
	require.Equal(t, want.ToArray(), append([]uint64{}, rows...))
	n := uint64(end - start)
	for _, rng := range [][2]uint64{{0, n}, {n / 3, n - n/3}, {1, n + 5}} {
		require.Equal(t, want.CountRange(rng[0], rng[1]), view.CountRange(rng[0], rng[1]))
		require.Equal(t, want.AnySetNotIn(nil, rng[0], rng[1]), view.AnySetNotIn(nil, rng[0], rng[1]))
		require.Equal(t, want.AnySetNotIn(&want, rng[0], rng[1]), view.AnySetNotIn(&view, rng[0], rng[1]))
	}
	require.True(t, view.IsSame(&want))
	require.True(t, want.IsSame(&view))
	require.Equal(t, want.ExpandBools(nil, int(n)), view.ExpandBools(nil, int(n)))
	if words := view.WordsCovering(int(n) + 64); words == nil {
		require.Zero(t, want.Count(), "nil words mean no rows are set")
	} else {
		require.Equal(t, want.WordsCovering(int(n)+64), words)
	}
	require.True(t, bytes.Equal(want.Marshal(), view.Marshal()), "a view marshals clean words")
	require.Equal(t, want.MarshalSize(), view.MarshalSize())

	// Owned results built from a view hold clean words.
	cl, err := view.Clone(mp)
	require.NoError(t, err)
	require.True(t, cl.IsSame(&want))
	var or Bitmap
	require.NoError(t, or.InitWithSize(0, mp))
	or.Or(&view)
	require.Equal(t, want.ToArray(), or.ToArray())
	var and Bitmap
	require.NoError(t, and.InitWithSize(int64(n)+70, mp))
	and.AddRange(0, n+70)
	and.And(&view)
	require.Equal(t, want.ToArray(), and.ToArray(), "rows past the view's end read clear")
	cl.Free()
	or.Free()
	and.Free()

	// A view of a view composes the offsets.
	if n > 2 {
		var inner Bitmap
		inner.InitView(&view, 1, int64(n)-1)
		for row := uint64(0); row < n-2; row++ {
			require.Equal(t, want.Contains(row+1), inner.Contains(row))
		}
		require.Equal(t, want.CountRange(1, n-1), inner.Count())
	}
	view.Free()
	want.Free()
	require.Equal(t, allocated, mp.CurrNB())
}
