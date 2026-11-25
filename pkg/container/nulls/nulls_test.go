// Copyright 2021 Matrix Origin
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

package nulls

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testMP backs the bitmaps of tests that do not check pool accounting.
var testMP = mpool.MustNewZero()

func mustNulls(n *Nulls, err error) *Nulls {
	mustOK(err)
	return n
}

func TestOr(t *testing.T) {
	t.Run("or test", func(t *testing.T) {
		var m, m1, n, result Nulls
		m.Init(testMP)
		m1.Init(testMP)
		n.Init(testMP)
		result.Init(testMP)
		mustOK(n.InitWithSize(4000, testMP))
		for k := uint64(0); k < 4000; k++ {
			n.Add(k)
		}

		mustOK(m.InitWithSize(8000, testMP))
		for k := uint64(4000); k < 8000; k++ {
			m.Add(k)
		}

		Or(&n, &m, &result)
		assert.Equal(t, n.Count()+m.Count(), result.Count())
		result.Reset()
		Or(&n, &m1, &result)
		assert.Equal(t, n.Count(), result.Count())
		result.Reset()
		Or(&m1, &n, &result)
		assert.Equal(t, n.Count(), result.Count())
	})
}

func TestReset(t *testing.T) {
	t.Run("reset test", func(t *testing.T) {
		var n Nulls
		n.Init(testMP)
		mustOK(n.InitWithSize(2000, testMP))
		for i := uint64(0); i < 2000; i += 7 {
			n.Add(i)
		}
		n.Reset()
		assert.EqualValues(t, 0, n.Count())
	})
}

func TestAny(t *testing.T) {
	t.Run("Any test", func(t *testing.T) {
		var n Nulls
		n.Init(testMP)
		assert.EqualValues(t, false, n.Any())
		mustOK(n.InitWithSize(2000, testMP))
		for i := uint64(0); i < 2000; i += 7 {
			n.Add(i)
		}
		assert.EqualValues(t, true, n.Any())
	})
}

func TestMarshalTo(t *testing.T) {
	var n Nulls
	n.Init(testMP)
	var empty bytes.Buffer
	require.NoError(t, n.MarshalTo(&empty))
	require.Zero(t, n.MarshalSize())
	require.Zero(t, empty.Len())

	mustOK(n.InitWithSize(128, testMP))
	n.Add(3)
	n.Add(65)
	encoded, err := n.Show()
	require.NoError(t, err)
	var streamed bytes.Buffer
	require.NoError(t, n.MarshalTo(&streamed))
	require.Equal(t, encoded, streamed.Bytes())
	require.Equal(t, len(encoded), n.MarshalSize())
}

func TestSize(t *testing.T) {
	t.Run("Size test", func(t *testing.T) {
		var n Nulls
		n.Init(testMP)
		assert.EqualValues(t, 0, Size(&n))
		mustOK(n.InitWithSize(16, testMP))
		for i := uint64(0); i < 16; i++ {
			n.Add(i)
		}
		assert.EqualValues(t, 8, Size(&n))
	})
}

func TestLength(t *testing.T) {
	t.Run("Length test", func(t *testing.T) {
		var n Nulls
		n.Init(testMP)
		assert.EqualValues(t, 0, n.Count())
		mustOK(n.InitWithSize(16, testMP))
		for i := uint64(0); i < 16; i += 2 {
			n.Add(i)
		}
		assert.EqualValues(t, 8, n.Count())
	})
}

func TestString(t *testing.T) {
	t.Run("String test", func(t *testing.T) {
		var n Nulls
		n.Init(testMP)
		assert.EqualValues(t, "[]", String(&n))
		mustOK(n.InitWithSize(16, testMP))
		for i := uint64(0); i < 16; i += 2 {
			n.Add(i)
		}
		assert.EqualValues(t, "[0 2 4 6 8 10 12 14]", String(&n))
	})
}

func TestContains(t *testing.T) {
	t.Run("Contains test", func(t *testing.T) {
		var n Nulls
		n.Init(testMP)
		assert.EqualValues(t, false, n.Contains(2))
		mustOK(n.InitWithSize(16, testMP))
		for i := uint64(0); i < 16; i += 2 {
			n.Add(i)
		}
		assert.EqualValues(t, true, n.Contains(2))
	})
}

func TestAdd(t *testing.T) {
	t.Run("Contains test", func(t *testing.T) {
		n := Nulls{}
		n.Init(testMP)
		assert.EqualValues(t, false, n.Contains(2))
		n.Add(2)
		assert.EqualValues(t, true, n.Contains(2))
	})
}

func TestAddRangeUsesExclusiveEndForExternalStorage(t *testing.T) {
	var n Nulls
	n.Init(testMP)
	mustOK(n.InitWithSize(128, testMP))
	require.NotPanics(t, func() {
		n.AddRange(0, 128)
	})
	require.Equal(t, 128, n.Count())
}

func TestRangeUsesExclusiveEndForExternalStorage(t *testing.T) {
	var source, destination Nulls
	source.Init(testMP)
	destination.Init(testMP)
	source.Add(7, 63)
	mustOK(destination.InitWithSize(64, testMP))
	require.NotPanics(t, func() {
		mustOK(Range(&source, 0, 64, 0, &destination, testMP))
	})
	require.True(t, destination.Contains(7))
	require.True(t, destination.Contains(63))
}

func TestDel(t *testing.T) {
	t.Run("Contains test", func(t *testing.T) {
		n := &Nulls{}
		n.Init(testMP)
		n.Add(2)
		assert.EqualValues(t, true, n.Contains(2))
		n.Del(2)
		assert.EqualValues(t, false, n.Contains(2))
	})
}

func TestSet(t *testing.T) {
	t.Run("set test", func(t *testing.T) {
		n := &Nulls{}
		n.Init(testMP)
		n.Add(2)
		m := &Nulls{}
		m.Init(testMP)
		m.Add(5)
		n.Or(m)
		assert.Equal(t, 2, n.Count())
	})
}

func TestFilterCount(t *testing.T) {
	t.Run("FilterCount test", func(t *testing.T) {
		var n Nulls
		n.Init(testMP)
		n.Add(2, 3, 5, 8)
		assert.Equal(t, 2, FilterCount(&n, []int64{2, 3}))
	})
}

func TestRemoveRange(t *testing.T) {
	t.Run("set test", func(t *testing.T) {
		var n Nulls
		n.Init(testMP)
		mustOK(n.InitWithSize(16, testMP))
		for i := uint64(0); i < 16; i++ {
			n.Add(i)
		}
		assert.Equal(t, 16, n.Count())
		RemoveRange(&n, 10, 16)
		assert.Equal(t, 10, n.Count())
	})
}

func TestRange(t *testing.T) {
	t.Run("set test", func(t *testing.T) {
		var m, n, correctM Nulls
		m.Init(testMP)
		n.Init(testMP)
		correctM.Init(testMP)
		mustOK(Range(&n, 0, 16, 0, &m, testMP))
		mustOK(n.InitWithSize(16, testMP))
		for i := uint64(0); i < 16; i++ {
			n.Add(i)
		}
		mustOK(Range(&n, 0, 16, 0, &m, testMP))
		assert.Equal(t, n.Count(), m.Count())
		mustOK(Range(&n, 10, 16, 0, &m, testMP))
		mustOK(correctM.InitWithSize(16, testMP))
		for i := uint64(10); i < 16; i++ {
			correctM.Add(i)
		}
		assert.Equal(t, correctM.Count(), m.Count())
	})
}

func TestFilter(t *testing.T) {
	t.Run("set test", func(t *testing.T) {
		var n Nulls
		n.Init(testMP)
		for i := uint64(0); i < 16; i++ {
			n.Add(i)
		}
		assert.Equal(t, 16, n.Count())
		sels := []int64{1, 3, 5}
		mustOK(Filter(&n, sels, false))
		assert.Equal(t, 3, n.Count())
	})

	t.Run("set test", func(t *testing.T) {
		var n Nulls
		n.Init(testMP)
		for i := uint64(0); i < 16; i++ {
			n.Add(i)
		}
		assert.Equal(t, 16, n.Count())
		sels := []int64{1, 3, 5}
		mustOK(Filter(&n, sels, true))
		assert.Equal(t, 13, n.Count())
	})
}

func TestFilterInPlaceOrderedMatchesFilter(t *testing.T) {
	source := mustNulls(Build(130, testMP, 0, 2, 63, 64, 65, 128, 129))
	for _, test := range []struct {
		name   string
		sels   []int64
		negate bool
	}{
		{"select-across-words", []int64{0, 2, 64, 65, 129}, false},
		{"select-trailing-clear", []int64{1, 63, 130, 131}, false},
		{"remove-across-words", []int64{1, 63, 128}, true},
		{"remove-after-bitmap", []int64{130, 131}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			legacy := mustNulls(source.Clone(testMP))
			mustOK(Filter(legacy, test.sels, test.negate))
			var inPlace Nulls
			inPlace.Init(testMP)
			mustOK(inPlace.InitWith(source, testMP))
			FilterInPlaceOrdered(&inPlace, test.sels, test.negate)

			require.True(t, inPlace.GetBitmap().IsSame(legacy.GetBitmap()))
			require.Equal(t, legacy.GetBitmap().Len(), inPlace.GetBitmap().Len())
			require.Equal(t, legacy.Count(), inPlace.Count())
		})
	}
}

func TestOrderedFilterSparseTail(t *testing.T) {
	for _, mode := range []string{"slice", "mask", "offset"} {
		for _, negate := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/negate=%t", mode, negate), func(t *testing.T) {
				source := mustNulls(Build(3, testMP, 0, 2))
				legacy := mustNulls(source.Clone(testMP))
				inPlace := mustNulls(source.Clone(testMP))
				var offset uint64
				if mode == "offset" {
					offset = 1
				}
				sels := make([]int64, 65)
				var mask bitmap.Bitmap
				mask.Init(testMP)
				mustOK(mask.InitWithSize(66, testMP))
				for i := range sels {
					sels[i] = int64(i) + int64(offset)
					mask.Add(uint64(i))
				}
				mustOK(Filter(legacy, sels, negate))
				if mode == "slice" {
					FilterInPlaceOrdered(inPlace, sels, negate)
				} else {
					FilterByMaskInPlaceWithOffset(inPlace, &mask, negate, offset)
				}
				require.Equal(t, legacy.ToArray(), inPlace.ToArray())
				require.Equal(t, legacy.Count(), inPlace.Count())
				for row := uint64(0); row < 67; row++ {
					require.Equal(t, legacy.Contains(row), inPlace.Contains(row), "row %d", row)
				}
				require.NoError(t, inPlace.GetBitmap().Validate())
			})
		}
	}
}

func TestFilterByMaskInPlaceMatchesFilter(t *testing.T) {
	source := mustNulls(Build(130, testMP, 0, 2, 63, 64, 65, 128, 129))
	for _, negate := range []bool{false, true} {
		t.Run(fmt.Sprintf("negate=%t", negate), func(t *testing.T) {
			var selection bitmap.Bitmap
			selection.Init(testMP)
			mustOK(selection.InitWithSize(132, testMP))
			selection.AddMany([]uint64{1, 63, 128, 130, 131})

			legacy := mustNulls(source.Clone(testMP))
			FilterByMask(legacy, &selection, negate)

			var inPlace Nulls

			inPlace.Init(testMP)
			mustOK(inPlace.InitWith(source, testMP))
			FilterByMaskInPlace(&inPlace, &selection, negate)

			require.True(t, inPlace.GetBitmap().IsSame(legacy.GetBitmap()))
			require.Equal(t, legacy.GetBitmap().Len(), inPlace.GetBitmap().Len())
			require.Equal(t, legacy.Count(), inPlace.Count())
		})
	}
}

func TestFilterByMaskInPlaceWithOffsetMatchesAllocatingPath(t *testing.T) {
	source := mustNulls(Build(130, testMP, 1, 63, 64, 127, 129))
	var selection bitmap.Bitmap
	selection.Init(testMP)
	mustOK(selection.InitWithSize(129, testMP))
	selection.AddMany([]uint64{0, 62, 128})

	for _, negate := range []bool{false, true} {
		for _, external := range []bool{false, true} {
			t.Run(fmt.Sprintf("negate=%t/external=%t", negate, external), func(t *testing.T) {
				legacy := mustNulls(source.Clone(testMP))
				FilterByMaskWithOffset(legacy, &selection, negate, 1)

				var inPlace Nulls

				inPlace.Init(testMP)
				if external {
				}
				mustOK(inPlace.InitWith(source, testMP))
				FilterByMaskInPlaceWithOffset(&inPlace, &selection, negate, 1)

				require.True(t, inPlace.GetBitmap().IsSame(legacy.GetBitmap()))
				require.Equal(t, legacy.GetBitmap().Len(), inPlace.GetBitmap().Len())
				require.Equal(t, legacy.Count(), inPlace.Count())
				if negate {
					require.Equal(t, []uint64{62, 125}, inPlace.ToArray())
					require.Equal(t, int64(130), inPlace.GetBitmap().Len())
				} else {
					require.Equal(t, []uint64{0, 1, 2}, inPlace.ToArray())
					require.Equal(t, int64(3), inPlace.GetBitmap().Len())
				}
			})
		}
	}

	var empty Nulls

	empty.Init(testMP)
	FilterByMaskInPlaceWithOffset(&empty, &selection, true, 1)
	require.True(t, empty.IsEmpty())
}

func TestMerge(t *testing.T) {
	t.Run("merge test", func(t *testing.T) {
		var n, m Nulls
		n.Init(testMP)
		m.Init(testMP)
		for i := uint64(0); i < 16; i++ {
			n.Add(i)
		}
		for i := uint64(8); i < 24; i++ {
			m.Add(i)
		}
		assert.Equal(t, 16, n.Count())
		assert.Equal(t, 16, m.Count())
		n.Or(&m)
		assert.Equal(t, 24, n.Count())
	})
}

func TestIsSame(t *testing.T) {
	t.Run("IsSame test", func(t *testing.T) {
		var n Nulls
		n.Init(testMP)
		for i := uint64(0); i < 16; i++ {
			n.Add(i)
		}
		assert.Equal(t, true, n.IsSame(&n))
	})
	t.Run("IsSame test", func(t *testing.T) {
		var n Nulls
		n.Init(testMP)
		for i := uint64(0); i < 16; i++ {
			n.Add(i)
		}
		assert.Equal(t, false, n.IsSame(nil))
	})
	t.Run("IsSame test", func(t *testing.T) {
		var n, m Nulls
		n.Init(testMP)
		m.Init(testMP)
		for i := uint64(0); i < 16; i++ {
			n.Add(i)
		}
		for i := uint64(0); i < 16; i++ {
			m.Add(i)
		}
		assert.Equal(t, true, n.IsSame(&m))
	})
}

// TestAddUnsortedRows: Add used to grow the bitmap only to cover the last
// row, so a larger earlier row landed beyond the logical length.
func TestAddUnsortedRows(t *testing.T) {
	var n Nulls
	n.Init(testMP)
	mustOK(n.InitWithSize(0, testMP))
	n.Add(5, 2)
	require.True(t, n.Contains(5))
	require.True(t, n.Contains(2))
	require.Equal(t, 2, n.Count())
	require.GreaterOrEqual(t, n.Len(), int64(6))
	require.NoError(t, n.GetBitmap().Validate())

	m := mustNulls(Build(0, testMP, 200, 3, 70))
	require.Equal(t, []uint64{3, 70, 200}, m.ToArray())
	require.NoError(t, m.GetBitmap().Validate())
}

// TestFilterNegateEmptySels: removing no rows used to index sels[0].
func TestFilterNegateEmptySels(t *testing.T) {
	n := mustNulls(Build(10, testMP, 1, 9))
	mustOK(Filter(n, nil, true))
	require.Equal(t, []uint64{1, 9}, n.ToArray())
	require.Equal(t, int64(10), n.Len())
}

// refFilter is the original row-by-row Filter, kept as a reference model.
func refFilter(nsp *Nulls, sels []int64, negate bool) *Nulls {
	if nsp.IsEmpty() {
		return mustNulls(nsp.Clone(testMP))
	}
	var out Nulls
	out.Init(testMP)
	if negate {
		oldLen := nsp.Len()
		mustOK(out.InitWithSize(int(oldLen), testMP))
		removed := make(map[int64]bool, len(sels))
		for _, s := range sels {
			removed[s] = true
		}
		newIdx := uint64(0)
		for old := int64(0); old < oldLen; old++ {
			if removed[old] {
				continue
			}
			if nsp.Contains(uint64(old)) {
				out.GetBitmap().Add(newIdx)
			}
			newIdx++
		}
		return &out
	}
	mustOK(out.InitWithSize(len(sels), testMP))
	for i, s := range sels {
		if s >= 0 && nsp.Contains(uint64(s)) {
			out.GetBitmap().Add(uint64(i))
		}
	}
	return &out
}

func randomNulls(r *rand.Rand, size int) *Nulls {
	n := mustNulls(NewWithSize(size, testMP))
	for i := 0; i < size; i++ {
		if r.Intn(3) == 0 {
			n.Set(uint64(i))
		}
	}
	return n
}

func randomSortedSels(r *rand.Rand, limit int) []int64 {
	var sels []int64
	for i := 0; i < limit; i++ {
		if r.Intn(2) == 0 {
			sels = append(sels, int64(i))
		}
	}
	return sels
}

func requireSameNulls(t *testing.T, want, got *Nulls, msg string) {
	t.Helper()
	// Rows past Len read as clear, so only the set rows must match; an
	// in-place filter does not grow the bitmap to cover clear rows.
	require.LessOrEqual(t, got.Len(), want.Len(), msg)
	require.Equal(t, want.ToArray(), got.ToArray(), msg)
	require.Equal(t, want.Count(), got.Count(), msg)
	require.NoError(t, got.GetBitmap().Validate(), msg)
}

// TestFilterVariantsMatchReference checks Filter, FilterInPlaceOrdered and
// FilterByMaskWithOffset, which now delegate to the bitmap's in-place remaps,
// against the row-by-row reference.
func TestFilterVariantsMatchReference(t *testing.T) {
	r := rand.New(rand.NewSource(42))
	for iter := 0; iter < 2000; iter++ {
		size := r.Intn(200)
		src := randomNulls(r, size)
		// Selections may reach past the bitmap, as with a short null map.
		sels := randomSortedSels(r, size+r.Intn(70))
		offset := uint64(r.Intn(3))
		for _, negate := range []bool{false, true} {
			msg := fmt.Sprintf("iter=%d size=%d negate=%t offset=%d", iter, size, negate, offset)
			want := refFilter(src, sels, negate)

			got := mustNulls(src.Clone(testMP))
			mustOK(Filter(got, sels, negate))
			requireSameNulls(t, want, got, "Filter "+msg)

			got = mustNulls(src.Clone(testMP))
			FilterInPlaceOrdered(got, sels, negate)
			requireSameNulls(t, want, got, "FilterInPlaceOrdered "+msg)

			var mask bitmap.Bitmap

			mask.Init(testMP)
			mustOK(mask.InitWithSize(int64(size+70), testMP))
			var shifted []int64
			for _, s := range sels {
				if uint64(s) >= offset {
					mask.Add(uint64(s) - offset)
					shifted = append(shifted, s)
				}
			}
			want = refFilter(src, shifted, negate)
			got = mustNulls(src.Clone(testMP))
			FilterByMaskWithOffset(got, &mask, negate, offset)
			requireSameNulls(t, want, got, "FilterByMaskWithOffset "+msg)
		}

		// Unordered, duplicated and negative selections for Filter.
		perm := make([]int64, r.Intn(size+5))
		for i := range perm {
			perm[i] = int64(r.Intn(size+10)) - 2
		}
		want := refFilter(src, perm, false)
		got := mustNulls(src.Clone(testMP))
		mustOK(Filter(got, perm, false))
		requireSameNulls(t, want, got, fmt.Sprintf("Filter unordered iter=%d", iter))
	}
}

// Or of two empty inputs clears the result in place: a result that reserved
// its validity keeps that storage, so later in-place Adds do not allocate.
func TestOrOfEmptyKeepsReservedStorage(t *testing.T) {
	mp := mpool.MustNewZero()
	var r Nulls
	r.Init(mp)
	require.NoError(t, r.GetBitmap().Reserve(130))
	r.Add(3)
	reserved := mp.CurrNB()
	require.Positive(t, reserved)

	Or(nil, &Nulls{}, &r)
	require.True(t, r.IsEmpty())
	require.False(t, r.Contains(3))
	require.Equal(t, reserved, mp.CurrNB(), "the reservation is kept")

	r.Add(0)
	r.Add(129)
	require.Equal(t, reserved, mp.CurrNB(), "Adds within the reservation do not allocate")
	require.Equal(t, []uint64{0, 129}, r.ToArray())
	r.Free()
	require.Zero(t, mp.CurrNB())
}
