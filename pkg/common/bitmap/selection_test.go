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
	"math/rand"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/stretchr/testify/require"
)

// selectionLengths covers empty, sub-word, word-boundary and multi-word
// bitmaps up to a full 8192-row batch.
var selectionLengths = []int{0, 1, 5, 63, 64, 65, 127, 128, 129, 200, 1000, 8192}

// randomBools sets each row with probability p (p < 0 picks all or none
// patterns that exercise the whole-word fast paths).
func randomBools(rng *rand.Rand, n int, p float64) []bool {
	rows := make([]bool, n)
	for i := range rows {
		switch {
		case p < -1:
			rows[i] = false
		case p < 0:
			rows[i] = true
		default:
			rows[i] = rng.Float64() < p
		}
	}
	return rows
}

var selectionDensities = []float64{-2, -1, 0.01, 0.3, 0.5, 0.9}

// selOwnedFrom builds an owned bitmap holding rows.
func selOwnedFrom(t *testing.T, rows []bool, mp *mpool.MPool) *Bitmap {
	bm := new(Bitmap)
	require.NoError(t, bm.InitWithSize(int64(len(rows)), mp))
	for r, set := range rows {
		if set {
			bm.Add(uint64(r))
		}
	}
	return bm
}

// selMaybeView returns bm itself, or a read-only view of the same rows taken
// at a non-zero bit shift, so the primitives see both kinds of input.
func selMaybeView(t *testing.T, rng *rand.Rand, rows []bool, mp *mpool.MPool) (*Bitmap, func()) {
	if rng.Intn(2) == 0 {
		bm := selOwnedFrom(t, rows, mp)
		return bm, bm.Free
	}
	shift := 1 + rng.Intn(70)
	padded := make([]bool, shift+len(rows)+rng.Intn(70))
	for i := range padded {
		padded[i] = rng.Intn(2) == 0 // garbage outside the view
	}
	copy(padded[shift:], rows)
	src := selOwnedFrom(t, padded, mp)
	view := new(Bitmap)
	view.InitView(src, int64(shift), int64(shift+len(rows)))
	require.True(t, view.IsReadOnly())
	return view, src.Free
}

func requireSelRows(t *testing.T, want []bool, got *Bitmap) {
	t.Helper()
	require.Equal(t, int64(len(want)), got.Len())
	count := 0
	for r, set := range want {
		require.Equal(t, set, got.Contains(uint64(r)), "row %d", r)
		if set {
			count++
		}
	}
	require.Equal(t, count, got.Count())
}

func TestInitWithBools(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	mp := mpool.MustNewZero()
	var bm Bitmap // reused across cases: words are recycled
	for _, n := range selectionLengths {
		for _, pv := range selectionDensities {
			for _, pn := range []float64{-2, 0.1, 0.5} {
				values := randomBools(rng, n, pv)
				nullRows := randomBools(rng, n, pn)
				var nulls *Bitmap
				release := func() {}
				if rng.Intn(4) != 0 {
					nulls, release = selMaybeView(t, rng, nullRows, mp)
				} else {
					nullRows = make([]bool, n)
				}
				require.NoError(t, bm.InitWithBools(values, nulls, mp))
				want := make([]bool, n)
				for r := range want {
					want[r] = values[r] && !nullRows[r]
				}
				requireSelRows(t, want, &bm)
				release()
			}
		}
	}
	bm.Free()
	require.Zero(t, mp.CurrNB())
}

func TestInitDeposit(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	mp := mpool.MustNewZero()
	var bm Bitmap
	for _, n := range selectionLengths {
		for _, pb := range selectionDensities {
			for _, ps := range selectionDensities {
				baseRows := randomBools(rng, n, pb)
				live := 0
				for _, set := range baseRows {
					if set {
						live++
					}
				}
				// sel covers the live rows, sometimes fewer, sometimes more.
				selLen := live
				switch rng.Intn(3) {
				case 1:
					selLen = rng.Intn(live + 1)
				case 2:
					selLen = live + rng.Intn(100)
				}
				selRows := randomBools(rng, selLen, ps)
				base, releaseBase := selMaybeView(t, rng, baseRows, mp)
				sel, releaseSel := selMaybeView(t, rng, selRows, mp)
				require.NoError(t, bm.InitDeposit(base, sel, mp))

				want := make([]bool, n)
				rank := 0
				for r, set := range baseRows {
					if set {
						want[r] = rank < selLen && selRows[rank]
						rank++
					}
				}
				requireSelRows(t, want, &bm)
				releaseBase()
				releaseSel()
			}
		}
	}
	bm.Free()
	require.Zero(t, mp.CurrNB())
}

func TestKeepFirstN(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	mp := mpool.MustNewZero()
	for _, n := range selectionLengths {
		for _, p := range selectionDensities {
			rows := randomBools(rng, n, p)
			set := 0
			for _, s := range rows {
				if s {
					set++
				}
			}
			for _, k := range []int{0, 1, set / 2, set - 1, set, set + 1} {
				if k < 0 {
					continue
				}
				bm := selOwnedFrom(t, rows, mp)
				bm.KeepFirstN(k)
				want := make([]bool, n)
				kept := 0
				for r, s := range rows {
					if s && kept < k {
						want[r] = true
						kept++
					}
				}
				requireSelRows(t, want, bm)
				bm.Free()
			}
		}
	}
	require.Zero(t, mp.CurrNB())
	require.Panics(t, func() { new(Bitmap).KeepFirstN(-1) })
}

func TestDepositBits(t *testing.T) {
	rng := rand.New(rand.NewSource(4))
	for i := 0; i < 10000; i++ {
		w, s := rng.Uint64(), rng.Uint64()
		if i%7 == 0 {
			w = 0
		}
		var want uint64
		j := 0
		for b := 0; b < 64; b++ {
			if w&(1<<uint(b)) != 0 {
				if s&(1<<uint(j)) != 0 {
					want |= 1 << uint(b)
				}
				j++
			}
		}
		require.Equal(t, want, depositBits(w, s), "w=%x s=%x", w, s)
	}
}

func BenchmarkInitWithBools(b *testing.B) {
	rng := rand.New(rand.NewSource(5))
	values := randomBools(rng, 8192, 0.5)
	mp := mpool.MustNewZero()
	var bm Bitmap
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := bm.InitWithBools(values, nil, mp); err != nil {
			b.Fatal(err)
		}
	}
	bm.Free()
}

// BenchmarkBoolsToSelsToBitmap is the conversion InitWithBools replaces:
// a bool predicate compacted to sels, then published with InitWithRows.
func BenchmarkBoolsToSelsToBitmap(b *testing.B) {
	rng := rand.New(rand.NewSource(5))
	values := randomBools(rng, 8192, 0.5)
	mp := mpool.MustNewZero()
	var bm Bitmap
	sels := make([]int64, 0, 8192)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		sels = sels[:0]
		for r, v := range values {
			if v {
				sels = append(sels, int64(r))
			}
		}
		if err := bm.InitWithRows(8192, sels, mp); err != nil {
			b.Fatal(err)
		}
	}
	bm.Free()
}

func BenchmarkInitDeposit(b *testing.B) {
	rng := rand.New(rand.NewSource(6))
	mp := mpool.MustNewZero()
	var base, sel, bm Bitmap
	baseRows := randomBools(rng, 8192, 0.9)
	_ = base.InitWithBools(baseRows, nil, mp)
	_ = sel.InitWithBools(randomBools(rng, base.Count(), 0.3), nil, mp)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := bm.InitDeposit(&base, &sel, mp); err != nil {
			b.Fatal(err)
		}
	}
	bm.Free()
	base.Free()
	sel.Free()
}
