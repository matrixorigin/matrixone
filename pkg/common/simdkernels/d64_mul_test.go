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

//go:build goexperiment.simd && amd64

package simdkernels

import (
	"math"
	"math/bits"
	"math/rand/v2"
	"testing"
)

// referenceD64Mul is the canonical scalar reference. Independent of the
// dispatcher under test (scalarD64MulNoBroadcast or any future SIMD swap-in)
// so the SIMD impls are checked against the same source-of-truth used by
// the main repo (d64MulInline + d128DivPow10Once semantics).
func referenceD64Mul(a, b []uint64, scaleAdj int32) []uint64 {
	n := len(a)
	r := make([]uint64, 2*n)
	for i := 0; i < n; i++ {
		xi, yi := int64(a[i]), int64(b[i])
		mx, my := xi>>63, yi>>63
		ax, ay := uint64((xi^mx)-mx), uint64((yi^my)-my)
		hi, lo := bits.Mul64(ax, ay)
		nm := uint64((xi ^ yi) >> 63)
		lo ^= nm
		hi ^= nm
		var c uint64
		lo, c = bits.Add64(lo, 0, nm&1)
		hi, _ = bits.Add64(hi, 0, c)
		if scaleAdj != 0 {
			d := pow10Table[-scaleAdj]
			half := (d + 1) >> 1
			// abs
			s := uint64(int64(hi) >> 63)
			lo ^= s
			hi ^= s
			lo, c = bits.Add64(lo, 0, s&1)
			hi, _ = bits.Add64(hi, 0, c)
			// divide
			var rem uint64
			hi, rem = bits.Div64(0, hi, d)
			lo, rem = bits.Div64(rem, lo, d)
			_, borrow := bits.Sub64(rem, half, 0)
			round := 1 - borrow
			lo, c = bits.Add64(lo, round, 0)
			hi += c
			// re-sign
			lo ^= s
			hi ^= s
			lo, c = bits.Add64(lo, 0, s&1)
			hi, _ = bits.Add64(hi, 0, c)
		}
		r[2*i] = lo
		r[2*i+1] = hi
	}
	return r
}

func d64MulSizes() []int {
	return []int{0, 1, 3, 4, 7, 8, 15, 16, 17, 31, 32, 33, 35, 63, 64, 127, 128, 255, 256, 1023, 4096}
}

type d64MulImpl struct {
	name string
	fn   func(a, b, r []uint64, scaleAdj int32)
}

func d64MulImpls() []d64MulImpl {
	out := []d64MulImpl{{"scalar", scalarD64MulNoBroadcast}}
	if D64MulNoBroadcast != nil {
		out = append(out, d64MulImpl{"dispatch", D64MulNoBroadcast})
	}
	return out
}

func makeRandInt64Slice(n int, seed uint64, maxAbs uint64) []uint64 {
	rng := rand.New(rand.NewPCG(seed, seed^0xD1B54A32D192ED03))
	out := make([]uint64, n)
	for i := range out {
		v := rng.Uint64()
		if maxAbs > 0 {
			v %= 2 * maxAbs
			out[i] = uint64(int64(v) - int64(maxAbs))
		} else {
			out[i] = v
		}
	}
	return out
}

// u64 wraps a runtime int64→uint64 conversion. Used in test fixtures to
// avoid Go's compile-time check that rejects `uint64(int64(-N))` as a
// constant expression.
func u64(x int64) uint64 { return uint64(x) }

func d64MulEdgeInputs() (a, b []uint64) {
	a = []uint64{
		0,
		1,
		u64(-1),
		u64(math.MaxInt64),
		u64(math.MinInt64),
		u64(math.MaxInt32),
		u64(math.MinInt32),
		1 << 32,
		1<<32 - 1,
		u64(-(1 << 32)),
		u64(1e15),
		u64(-1e15),
		u64(1e9),
		u64(-1e9),
		u64(7),
		u64(-7),
	}
	b = []uint64{
		0,
		u64(math.MaxInt64),
		u64(math.MinInt64),
		1,
		u64(-1),
		1 << 31,
		u64(-(1 << 31)),
		u64(1e9),
		u64(-1e9),
		1 << 30,
		u64(1e3),
		u64(-1e3),
		u64(1e9),
		u64(-1e9),
		u64(13),
		u64(-13),
	}
	return a, b
}

func TestD64MulCorrectness(t *testing.T) {
	scaleAdjs := []int32{0, -1, -2, -4, -8, -12, -18}
	impls := d64MulImpls()

	for _, sa := range scaleAdjs {
		t.Run("scaleAdj="+itoa(int(sa)), func(t *testing.T) {
			for _, n := range d64MulSizes() {
				a := makeRandInt64Slice(n, 0xC0FFEE^uint64(n), 1<<40)
				b := makeRandInt64Slice(n, 0xBADBEEF^uint64(n), 1<<40)
				want := referenceD64Mul(a, b, sa)
				for _, im := range impls {
					got := make([]uint64, 2*n)
					im.fn(a, b, got, sa)
					for i := 0; i < 2*n; i++ {
						if got[i] != want[i] {
							t.Fatalf("%s n=%d sa=%d idx=%d: got 0x%x want 0x%x (a=%d b=%d)",
								im.name, n, sa, i, got[i], want[i],
								int64(a[i/2]), int64(b[i/2]))
							return
						}
					}
				}
			}
		})
	}
}

func TestD64MulEdges(t *testing.T) {
	a, b := d64MulEdgeInputs()
	scaleAdjs := []int32{0, -1, -8, -18}
	impls := d64MulImpls()
	for _, sa := range scaleAdjs {
		want := referenceD64Mul(a, b, sa)
		for _, im := range impls {
			got := make([]uint64, 2*len(a))
			im.fn(a, b, got, sa)
			for i := 0; i < 2*len(a); i++ {
				if got[i] != want[i] {
					t.Fatalf("%s sa=%d idx=%d: got 0x%x want 0x%x (a=%d b=%d)",
						im.name, sa, i, got[i], want[i],
						int64(a[i/2]), int64(b[i/2]))
				}
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

func benchD64Mul(b *testing.B, fn func(a, b, r []uint64, scaleAdj int32), n int, scaleAdj int32) {
	a := makeRandInt64Slice(n, 0x1234, 1<<40)
	bv := makeRandInt64Slice(n, 0x5678, 1<<40)
	r := make([]uint64, 2*n)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fn(a, bv, r, scaleAdj)
	}
}

func BenchmarkD64Mul(b *testing.B) {
	sizes := []int{16, 64, 256, 1024, 4096}
	scaleAdjs := []int32{0, -8}
	for _, sa := range scaleAdjs {
		for _, n := range sizes {
			b.Run("scalar/sa="+itoa(int(sa))+"/n="+itoa(n), func(b *testing.B) {
				benchD64Mul(b, scalarD64MulNoBroadcast, n, sa)
			})
			b.Run("dispatch/sa="+itoa(int(sa))+"/n="+itoa(n), func(b *testing.B) {
				benchD64Mul(b, D64MulNoBroadcast, n, sa)
			})
		}
	}
}
