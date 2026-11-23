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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOr(t *testing.T) {
	t.Run("or test", func(t *testing.T) {
		var m, m1, n, result Nulls
		n.InitWithSize(4000)
		for k := uint64(0); k < 4000; k++ {
			n.Add(k)
		}

		m.InitWithSize(8000)
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
		n.InitWithSize(2000)
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
		assert.EqualValues(t, false, n.Any())
		n.InitWithSize(2000)
		for i := uint64(0); i < 2000; i += 7 {
			n.Add(i)
		}
		assert.EqualValues(t, true, n.Any())
	})
}

func TestMarshalTo(t *testing.T) {
	var n Nulls
	var empty bytes.Buffer
	require.NoError(t, n.MarshalTo(&empty))
	require.Zero(t, n.MarshalSize())
	require.Zero(t, empty.Len())

	n.InitWithSize(128)
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
		assert.EqualValues(t, 0, Size(&n))
		n.InitWithSize(16)
		for i := uint64(0); i < 16; i++ {
			n.Add(i)
		}
		assert.EqualValues(t, 8, Size(&n))
	})
}

func TestLength(t *testing.T) {
	t.Run("Length test", func(t *testing.T) {
		var n Nulls
		assert.EqualValues(t, 0, n.Count())
		n.InitWithSize(16)
		for i := uint64(0); i < 16; i += 2 {
			n.Add(i)
		}
		assert.EqualValues(t, 8, n.Count())
	})
}

func TestString(t *testing.T) {
	t.Run("String test", func(t *testing.T) {
		var n Nulls
		assert.EqualValues(t, "[]", String(&n))
		n.InitWithSize(16)
		for i := uint64(0); i < 16; i += 2 {
			n.Add(i)
		}
		assert.EqualValues(t, "[0 2 4 6 8 10 12 14]", String(&n))
	})
}

func TestContains(t *testing.T) {
	t.Run("Contains test", func(t *testing.T) {
		var n Nulls
		assert.EqualValues(t, false, n.Contains(2))
		n.InitWithSize(16)
		for i := uint64(0); i < 16; i += 2 {
			n.Add(i)
		}
		assert.EqualValues(t, true, n.Contains(2))
	})
}

func TestAdd(t *testing.T) {
	t.Run("Contains test", func(t *testing.T) {
		n := Nulls{}
		assert.EqualValues(t, false, n.Contains(2))
		n.Add(2)
		assert.EqualValues(t, true, n.Contains(2))
	})
}

func TestAddRangeUsesExclusiveEndForExternalStorage(t *testing.T) {
	var n Nulls
	n.InitWithSize(128)
	require.NotPanics(t, func() {
		n.AddRange(0, 128)
	})
	require.Equal(t, 128, n.Count())
}

func TestRangeUsesExclusiveEndForExternalStorage(t *testing.T) {
	var source, destination Nulls
	source.Add(7, 63)
	destination.InitWithSize(64)
	require.NotPanics(t, func() {
		Range(&source, 0, 64, 0, &destination)
	})
	require.True(t, destination.Contains(7))
	require.True(t, destination.Contains(63))
}

func TestDel(t *testing.T) {
	t.Run("Contains test", func(t *testing.T) {
		n := &Nulls{}
		n.Add(2)
		assert.EqualValues(t, true, n.Contains(2))
		n.Del(2)
		assert.EqualValues(t, false, n.Contains(2))
	})
}

func TestSet(t *testing.T) {
	t.Run("set test", func(t *testing.T) {
		n := &Nulls{}
		n.Add(2)
		m := &Nulls{}
		m.Add(5)
		n.Or(m)
		assert.Equal(t, 2, n.Count())
	})
}

func TestFilterCount(t *testing.T) {
	t.Run("FilterCount test", func(t *testing.T) {
		var n Nulls
		n.Add(2, 3, 5, 8)
		assert.Equal(t, 2, FilterCount(&n, []int64{2, 3}))
	})
}

func TestRemoveRange(t *testing.T) {
	t.Run("set test", func(t *testing.T) {
		var n Nulls
		n.InitWithSize(16)
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
		Range(&n, 0, 16, 0, &m)
		n.InitWithSize(16)
		for i := uint64(0); i < 16; i++ {
			n.Add(i)
		}
		Range(&n, 0, 16, 0, &m)
		assert.Equal(t, n.Count(), m.Count())
		Range(&n, 10, 16, 0, &m)
		correctM.InitWithSize(16)
		for i := uint64(10); i < 16; i++ {
			correctM.Add(i)
		}
		assert.Equal(t, correctM.Count(), m.Count())
	})
}

func TestFilter(t *testing.T) {
	t.Run("set test", func(t *testing.T) {
		var n Nulls
		for i := uint64(0); i < 16; i++ {
			n.Add(i)
		}
		assert.Equal(t, 16, n.Count())
		sels := []int64{1, 3, 5}
		Filter(&n, sels, false)
		assert.Equal(t, 3, n.Count())
	})

	t.Run("set test", func(t *testing.T) {
		var n Nulls
		for i := uint64(0); i < 16; i++ {
			n.Add(i)
		}
		assert.Equal(t, 16, n.Count())
		sels := []int64{1, 3, 5}
		Filter(&n, sels, true)
		assert.Equal(t, 13, n.Count())
	})
}

func TestFilterInPlaceOrderedMatchesFilter(t *testing.T) {
	source := Build(130, 0, 2, 63, 64, 65, 128, 129)
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
			legacy := source.Clone()
			Filter(legacy, test.sels, test.negate)

			var inPlace Nulls
			inPlace.InitWith(source)
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
				source := Build(3, 0, 2)
				legacy := source.Clone()
				inPlace := source.Clone()
				var offset uint64
				if mode == "offset" {
					offset = 1
				}
				sels := make([]int64, 65)
				var mask bitmap.Bitmap
				mask.InitWithSize(66)
				for i := range sels {
					sels[i] = int64(i) + int64(offset)
					mask.Add(uint64(i))
				}
				Filter(legacy, sels, negate)
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
	source := Build(130, 0, 2, 63, 64, 65, 128, 129)
	for _, negate := range []bool{false, true} {
		t.Run(fmt.Sprintf("negate=%t", negate), func(t *testing.T) {
			var selection bitmap.Bitmap
			selection.InitWithSize(132)
			selection.AddMany([]uint64{1, 63, 128, 130, 131})

			legacy := source.Clone()
			FilterByMask(legacy, &selection, negate)

			var inPlace Nulls
			inPlace.InitWith(source)
			FilterByMaskInPlace(&inPlace, &selection, negate)

			require.True(t, inPlace.GetBitmap().IsSame(legacy.GetBitmap()))
			require.Equal(t, legacy.GetBitmap().Len(), inPlace.GetBitmap().Len())
			require.Equal(t, legacy.Count(), inPlace.Count())
		})
	}
}

func TestFilterByMaskInPlaceWithOffsetMatchesAllocatingPath(t *testing.T) {
	source := Build(130, 1, 63, 64, 127, 129)
	var selection bitmap.Bitmap
	selection.InitWithSize(129)
	selection.AddMany([]uint64{0, 62, 128})

	for _, negate := range []bool{false, true} {
		for _, external := range []bool{false, true} {
			t.Run(fmt.Sprintf("negate=%t/external=%t", negate, external), func(t *testing.T) {
				legacy := source.Clone()
				FilterByMaskWithOffset(legacy, &selection, negate, 1)

				var inPlace Nulls
				if external {
				}
				inPlace.InitWith(source)
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
	FilterByMaskInPlaceWithOffset(&empty, &selection, true, 1)
	require.True(t, empty.IsEmpty())
}

func TestMerge(t *testing.T) {
	t.Run("merge test", func(t *testing.T) {
		var n, m Nulls
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
		for i := uint64(0); i < 16; i++ {
			n.Add(i)
		}
		assert.Equal(t, true, n.IsSame(&n))
	})
	t.Run("IsSame test", func(t *testing.T) {
		var n Nulls
		for i := uint64(0); i < 16; i++ {
			n.Add(i)
		}
		assert.Equal(t, false, n.IsSame(nil))
	})
	t.Run("IsSame test", func(t *testing.T) {
		var n, m Nulls
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
	n.InitWithSize(0)
	n.Add(5, 2)
	require.True(t, n.Contains(5))
	require.True(t, n.Contains(2))
	require.Equal(t, 2, n.Count())
	require.GreaterOrEqual(t, n.Len(), int64(6))
	require.NoError(t, n.GetBitmap().Validate())

	m := Build(0, 200, 3, 70)
	require.Equal(t, []uint64{3, 70, 200}, m.ToArray())
	require.NoError(t, m.GetBitmap().Validate())
}

// TestFilterNegateEmptySels: removing no rows used to index sels[0].
func TestFilterNegateEmptySels(t *testing.T) {
	n := Build(10, 1, 9)
	Filter(n, nil, true)
	require.Equal(t, []uint64{1, 9}, n.ToArray())
	require.Equal(t, int64(10), n.Len())
}

// refFilter is the original row-by-row Filter, kept as a reference model.
func refFilter(nsp *Nulls, sels []int64, negate bool) *Nulls {
	if nsp.IsEmpty() {
		return nsp.Clone()
	}
	var out Nulls
	if negate {
		oldLen := nsp.Len()
		out.InitWithSize(int(oldLen))
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
	out.InitWithSize(len(sels))
	for i, s := range sels {
		if s >= 0 && nsp.Contains(uint64(s)) {
			out.GetBitmap().Add(uint64(i))
		}
	}
	return &out
}

func randomNulls(r *rand.Rand, size int) *Nulls {
	n := NewWithSize(size)
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
	require.Equal(t, want.Len(), got.Len(), msg)
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

			got := src.Clone()
			Filter(got, sels, negate)
			requireSameNulls(t, want, got, "Filter "+msg)

			got = src.Clone()
			FilterInPlaceOrdered(got, sels, negate)
			requireSameNulls(t, want, got, "FilterInPlaceOrdered "+msg)

			var mask bitmap.Bitmap
			mask.InitWithSize(int64(size + 70))
			var shifted []int64
			for _, s := range sels {
				if uint64(s) >= offset {
					mask.Add(uint64(s) - offset)
					shifted = append(shifted, s)
				}
			}
			want = refFilter(src, shifted, negate)
			got = src.Clone()
			FilterByMaskWithOffset(got, &mask, negate, offset)
			requireSameNulls(t, want, got, "FilterByMaskWithOffset "+msg)
		}

		// Unordered, duplicated and negative selections for Filter.
		perm := make([]int64, r.Intn(size+5))
		for i := range perm {
			perm[i] = int64(r.Intn(size+10)) - 2
		}
		want := refFilter(src, perm, false)
		got := src.Clone()
		Filter(got, perm, false)
		requireSameNulls(t, want, got, fmt.Sprintf("Filter unordered iter=%d", iter))
	}
}
