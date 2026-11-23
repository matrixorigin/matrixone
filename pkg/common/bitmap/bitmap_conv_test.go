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
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

// The iterator-based conversions the word-at-a-time ones replace; kept as the
// reference and the benchmark baseline.
func iterRows(n *Bitmap, dst []int64) []int64 {
	dst = dst[:0]
	itr := n.IteratorValue()
	for itr.HasNext() {
		dst = append(dst, int64(itr.Next()))
	}
	return dst
}

func iterBools(n *Bitmap, mask []bool, length int) []bool {
	if cap(mask) < length {
		mask = make([]bool, length)
	} else {
		mask = mask[:length]
		clear(mask)
	}
	itr := n.IteratorValue()
	for itr.HasNext() {
		if r := itr.Next(); r < uint64(length) {
			mask[r] = true
		}
	}
	return mask
}

func addRows(n *Bitmap, length int64, rows []int64) {
	n.InitWithSize(length)
	for _, r := range rows {
		n.Add(uint64(r))
	}
}

func randomRows(r *rand.Rand, length int, density float64) []int64 {
	var rows []int64
	for i := 0; i < length; i++ {
		if r.Float64() < density {
			rows = append(rows, int64(i))
		}
	}
	return rows
}

func TestBitmapRowConversions(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	for _, length := range []int{0, 1, 63, 64, 65, 127, 128, 8191, 8192, 10000} {
		for _, density := range []float64{0, 0.01, 0.5, 0.99, 1} {
			name := fmt.Sprintf("len=%d/density=%v", length, density)
			rows := randomRows(r, length, density)
			var want Bitmap
			addRows(&want, int64(length), rows)

			var got Bitmap
			got.InitWithRows(int64(length), append(rows, rows...)) // duplicates allowed
			require.Equal(t, want.Count(), got.Count(), name)
			require.Equal(t, want.Len(), got.Len(), name)
			require.True(t, slices.Equal(iterRows(&want, nil), iterRows(&got, nil)), name)

			require.True(t, slices.Equal(iterRows(&want, nil), AppendRows(&want, []int64(nil))), name)
			require.True(t, slices.Equal(iterRows(&want, nil), want.ToI64Array(nil)), name)
			var u32 []uint32
			ToArray(&want, &u32)
			require.Len(t, u32, len(rows), name)

			// A longer mask reads the bits past the bitmap as unset; a reused
			// mask is fully rewritten.
			for _, maskLen := range []int{length, length + 70} {
				dirty := make([]bool, maskLen+5)
				for i := range dirty {
					dirty[i] = true
				}
				require.True(t, slices.Equal(iterBools(&want, nil, maskLen), want.ExpandBools(dirty, maskLen)), name)
				require.True(t, slices.Equal(iterBools(&want, nil, maskLen), want.ExpandBools(nil, maskLen)), name)
			}

			// Reusing a larger bitmap's storage clears its old bits.
			var reused Bitmap
			reused.InitWithRows(20000, randomRows(r, 20000, 0.5))
			reused.InitWithRows(int64(length), rows)
			require.True(t, slices.Equal(iterRows(&want, nil), AppendRows(&reused, []int64(nil))), name)
		}
	}
}

// Words missing from data read as zero.
func TestBitmapRowConversionsShortData(t *testing.T) {
	var n Bitmap
	n.InitWithRows(64, []int64{1, 63})
	n.numBits = 200 // data covers only the first word
	require.Equal(t, []int64{1, 63}, AppendRows(&n, []int64(nil)))
	require.True(t, slices.Equal(iterBools(&n, nil, 200), n.ExpandBools(nil, 200)))
}

const benchRows = 8192

func benchBitmap(density float64) (*Bitmap, []int64) {
	rows := randomRows(rand.New(rand.NewPCG(3, 4)), benchRows, density)
	var n Bitmap
	n.InitWithRows(benchRows, rows)
	return &n, rows
}

var benchDensities = []float64{0.01, 0.5, 0.99, 1}

func BenchmarkBitmapToRows(b *testing.B) {
	for _, density := range benchDensities {
		n, _ := benchBitmap(density)
		dst := make([]int64, 0, benchRows)
		b.Run(fmt.Sprintf("density=%v/iterator", density), func(b *testing.B) {
			for b.Loop() {
				dst = iterRows(n, dst)
			}
		})
		b.Run(fmt.Sprintf("density=%v/iterator-nil-dst", density), func(b *testing.B) {
			for b.Loop() {
				_ = iterRows(n, nil)
			}
		})
		b.Run(fmt.Sprintf("density=%v/words", density), func(b *testing.B) {
			for b.Loop() {
				dst = AppendRows(n, dst[:0])
			}
		})
	}
}

func BenchmarkBitmapToBools(b *testing.B) {
	for _, density := range benchDensities {
		n, _ := benchBitmap(density)
		mask := make([]bool, benchRows)
		b.Run(fmt.Sprintf("density=%v/iterator", density), func(b *testing.B) {
			for b.Loop() {
				mask = iterBools(n, mask, benchRows)
			}
		})
		b.Run(fmt.Sprintf("density=%v/words", density), func(b *testing.B) {
			for b.Loop() {
				mask = n.ExpandBools(mask, benchRows)
			}
		})
	}
}

func BenchmarkBitmapFromRows(b *testing.B) {
	for _, density := range benchDensities {
		_, rows := benchBitmap(density)
		var n Bitmap
		b.Run(fmt.Sprintf("density=%v/init-add", density), func(b *testing.B) {
			for b.Loop() {
				addRows(&n, benchRows, rows)
			}
		})
		b.Run(fmt.Sprintf("density=%v/init-with-rows", density), func(b *testing.B) {
			for b.Loop() {
				n.InitWithRows(benchRows, rows)
			}
		})
	}
}
