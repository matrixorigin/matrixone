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
	"testing"
)

type d128UnaryImpl struct {
	name string
	fn   func(src, dst []uint64)
}

func d128NegateImpls() []d128UnaryImpl {
	out := []d128UnaryImpl{{name: "scalar", fn: scalarD128Negate}}
	if D128Negate != nil {
		out = append(out, d128UnaryImpl{name: "dispatch", fn: D128Negate})
	}
	return out
}

func d128AbsImpls() []d128UnaryImpl {
	out := []d128UnaryImpl{{name: "scalar", fn: scalarD128Abs}}
	if D128Abs != nil {
		out = append(out, d128UnaryImpl{name: "dispatch", fn: D128Abs})
	}
	return out
}

func d128NegAbsEdges() []uint64 {
	// pairs of (lo, hi) covering boundary cases
	pairs := [][2]uint64{
		{0, 0},
		{1, 0},
		{math.MaxUint64, 0},
		{0, 1},
		{0, math.MaxUint64},
		{math.MaxUint64, math.MaxUint64}, // -1
		{1, 0x8000000000000000},          // negative with lo == 1
		{0, 0x8000000000000000},          // MinInt128 (negation wraps)
		{math.MaxUint64, 0x7FFFFFFFFFFFFFFF},
		{0, 0x7FFFFFFFFFFFFFFF},
	}
	out := make([]uint64, 0, 2*len(pairs))
	for _, p := range pairs {
		out = append(out, p[0], p[1])
	}
	return out
}

func TestD128NegateCorrectness(t *testing.T) {
	impls := d128NegateImpls()
	for _, n := range d128Sizes() {
		src := makeRandD128(n, 0xDEC128^uint64(n))
		want := make([]uint64, 2*n)
		scalarD128Negate(src, want)
		for _, im := range impls {
			got := make([]uint64, 2*n)
			im.fn(src, got)
			for i := 0; i < 2*n; i++ {
				if got[i] != want[i] {
					t.Fatalf("%s n=%d idx=%d: got 0x%x want 0x%x src(lo=0x%x hi=0x%x)",
						im.name, n, i, got[i], want[i], src[(i/2)*2], src[(i/2)*2+1])
				}
			}
		}
	}
}

func TestD128NegateEdges(t *testing.T) {
	src := d128NegAbsEdges()
	n := len(src) / 2
	want := make([]uint64, 2*n)
	scalarD128Negate(src, want)
	for _, im := range d128NegateImpls() {
		got := make([]uint64, 2*n)
		im.fn(src, got)
		for i := 0; i < 2*n; i++ {
			if got[i] != want[i] {
				t.Fatalf("%s idx=%d: got 0x%x want 0x%x src(lo=0x%x hi=0x%x)",
					im.name, i, got[i], want[i], src[(i/2)*2], src[(i/2)*2+1])
			}
		}
	}
}

func TestD128AbsCorrectness(t *testing.T) {
	impls := d128AbsImpls()
	for _, n := range d128Sizes() {
		src := makeRandD128(n, 0xABA127^uint64(n))
		want := make([]uint64, 2*n)
		scalarD128Abs(src, want)
		for _, im := range impls {
			got := make([]uint64, 2*n)
			im.fn(src, got)
			for i := 0; i < 2*n; i++ {
				if got[i] != want[i] {
					t.Fatalf("%s n=%d idx=%d: got 0x%x want 0x%x src(lo=0x%x hi=0x%x)",
						im.name, n, i, got[i], want[i], src[(i/2)*2], src[(i/2)*2+1])
				}
			}
		}
	}
}

func TestD128AbsEdges(t *testing.T) {
	src := d128NegAbsEdges()
	n := len(src) / 2
	want := make([]uint64, 2*n)
	scalarD128Abs(src, want)
	for _, im := range d128AbsImpls() {
		got := make([]uint64, 2*n)
		im.fn(src, got)
		for i := 0; i < 2*n; i++ {
			if got[i] != want[i] {
				t.Fatalf("%s idx=%d: got 0x%x want 0x%x src(lo=0x%x hi=0x%x)",
					im.name, i, got[i], want[i], src[(i/2)*2], src[(i/2)*2+1])
			}
		}
	}
}

func benchmarkD128Unary(b *testing.B, name string, fn func(src, dst []uint64), n int) {
	src := makeRandD128(n, 0xBEEF^uint64(n))
	dst := make([]uint64, 2*n)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fn(src, dst)
	}
}

func BenchmarkD128Negate(b *testing.B) {
	for _, n := range []int{16, 64, 256, 1024, 4096} {
		for _, im := range d128NegateImpls() {
			b.Run(im.name+"/n="+itoa(n), func(b *testing.B) {
				benchmarkD128Unary(b, im.name, im.fn, n)
			})
		}
	}
}

func BenchmarkD128Abs(b *testing.B) {
	for _, n := range []int{16, 64, 256, 1024, 4096} {
		for _, im := range d128AbsImpls() {
			b.Run(im.name+"/n="+itoa(n), func(b *testing.B) {
				benchmarkD128Unary(b, im.name, im.fn, n)
			})
		}
	}
}
