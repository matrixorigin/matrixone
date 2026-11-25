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
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

// Row-by-row reference models for the ordered remaps.

func refRemap(src *Bitmap, selected []uint64, negate bool) *Bitmap {
	var out Bitmap
	oldLen := src.Len()
	if !negate {
		l := oldLen
		if int64(len(selected)) < l {
			l = int64(len(selected))
		}
		mustOK(out.InitWithSize(l, testMP))
		for i := int64(0); i < l; i++ {
			if src.Contains(selected[i]) {
				out.Add(uint64(i))
			}
		}
		return &out
	}
	removed := make(map[uint64]bool, len(selected))
	for _, r := range selected {
		removed[r] = true
	}
	mustOK(out.InitWithSize(oldLen, testMP))
	next := uint64(0)
	for r := uint64(0); r < uint64(oldLen); r++ {
		if removed[r] {
			continue
		}
		if src.Contains(r) {
			out.Add(next)
		}
		next++
	}
	return &out
}

type remapCase struct {
	src    Bitmap
	sels   []int64
	mask   Bitmap
	offset uint64
}

func randomRemapCase(r *rand.Rand) *remapCase {
	var c remapCase
	size := r.Intn(700)
	nullP := []float64{0, 0.001, 0.01, 0.33, 0.9, 1}[r.Intn(6)]
	selP := []float64{0, 0.01, 0.5, 0.99, 1}[r.Intn(5)]
	run := []int{1, 1, 7, 64, 256}[r.Intn(5)]
	mustOK(c.src.InitWithSize(int64(size), testMP))
	for i := 0; i < size; i++ {
		if r.Float64() < nullP {
			c.src.Add(uint64(i))
		}
	}
	limit := size + r.Intn(200)
	mustOK(c.mask.InitWithSize(int64(limit), testMP))
	in := false
	for i := 0; i < limit; i++ {
		if i%run == 0 {
			in = r.Float64() < selP
		}
		if in {
			c.sels = append(c.sels, int64(i))
			c.mask.Add(uint64(i))
		}
	}
	c.offset = uint64(r.Intn(200))
	return &c
}

func requireSameBitmap(t *testing.T, want, got *Bitmap, msg string) {
	t.Helper()
	require.Equal(t, want.Len(), got.Len(), msg)
	require.Equal(t, want.ToArray(), got.ToArray(), msg)
	require.Equal(t, want.Count(), got.Count(), msg)
	require.NoError(t, got.Validate(), msg)
}

func TestRemapOrderedMatchesReference(t *testing.T) {
	r := rand.New(rand.NewSource(20261119))
	for iter := 0; iter < 5000; iter++ {
		c := randomRemapCase(r)
		selected := make([]uint64, len(c.sels))
		for i, s := range c.sels {
			selected[i] = uint64(s)
		}
		for _, negate := range []bool{false, true} {
			msg := fmt.Sprintf("iter=%d len=%d sels=%d negate=%t", iter, c.src.Len(), len(c.sels), negate)
			var got Bitmap
			mustOK(got.InitWith(&c.src, testMP))
			got.RemapOrdered(c.sels, negate)
			requireSameBitmap(t, refRemap(&c.src, selected, negate), &got, msg)
		}
	}
}

func TestRemapMaskOrderedMatchesReference(t *testing.T) {
	r := rand.New(rand.NewSource(20261120))
	for iter := 0; iter < 5000; iter++ {
		c := randomRemapCase(r)
		selected := make([]uint64, 0, len(c.sels))
		for _, s := range c.sels {
			selected = append(selected, uint64(s)+c.offset)
		}
		for _, negate := range []bool{false, true} {
			msg := fmt.Sprintf("iter=%d len=%d sels=%d offset=%d negate=%t", iter, c.src.Len(), len(c.sels), c.offset, negate)
			var got Bitmap
			mustOK(got.InitWith(&c.src, testMP))
			got.RemapMaskOrderedWithOffset(&c.mask, negate, c.offset)
			requireSameBitmap(t, refRemap(&c.src, selected, negate), &got, msg)
		}
	}
}

func TestRemapOrderedRejectsUnsorted(t *testing.T) {
	for _, sels := range [][]int64{{3, 3}, {5, 2}, {-1}, {0, 70, 65}} {
		var n Bitmap
		mustOK(n.InitWithSize(200, testMP))
		require.Panics(t, func() { n.RemapOrdered(sels, true) }, "%v", sels)
	}
}
