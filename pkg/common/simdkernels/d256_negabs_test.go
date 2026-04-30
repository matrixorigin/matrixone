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

type d256UnaryImpl struct {
	name string
	fn   func(src, dst []uint64)
}

func d256NegateImpls() []d256UnaryImpl {
	out := []d256UnaryImpl{{name: "scalar", fn: scalarD256Negate}}
	if D256Negate != nil {
		out = append(out, d256UnaryImpl{name: "dispatch", fn: D256Negate})
	}
	return out
}

func d256AbsImpls() []d256UnaryImpl {
	out := []d256UnaryImpl{{name: "scalar", fn: scalarD256Abs}}
	if D256Abs != nil {
		out = append(out, d256UnaryImpl{name: "dispatch", fn: D256Abs})
	}
	return out
}

func d256NegAbsEdges() []uint64 {
	// each row = 4 q-words (lo..hi) of one D256.
	rows := [][4]uint64{
		{0, 0, 0, 0},
		{1, 0, 0, 0},
		{0, 0, 0, 1},
		{0, 0, 0, 0x8000000000000000}, // MinInt256
		{math.MaxUint64, math.MaxUint64, math.MaxUint64, math.MaxUint64}, // -1
		{0, 0, 0, 0x7FFFFFFFFFFFFFFF},                                    // MaxInt256
		{1, 0, 0, 0x8000000000000000},                                    // negative, lo!=0
		{math.MaxUint64, 0, 0, 0x8000000000000000},                       // tests carry over middle words
		{0, math.MaxUint64, math.MaxUint64, 0x8000000000000000},
		{math.MaxUint64, math.MaxUint64, math.MaxUint64, 0x8000000000000000},
	}
	out := make([]uint64, 0, 4*len(rows))
	for _, r := range rows {
		out = append(out, r[0], r[1], r[2], r[3])
	}
	return out
}

func TestD256NegateCorrectness(t *testing.T) {
	for _, n := range d256Sizes() {
		src := makeRandD256(n, 0xD256^uint64(n))
		want := make([]uint64, 4*n)
		scalarD256Negate(src, want)
		for _, im := range d256NegateImpls() {
			got := make([]uint64, 4*n)
			im.fn(src, got)
			for i := 0; i < 4*n; i++ {
				if got[i] != want[i] {
					t.Fatalf("%s n=%d idx=%d: got 0x%x want 0x%x", im.name, n, i, got[i], want[i])
				}
			}
		}
	}
}

func TestD256NegateEdges(t *testing.T) {
	src := d256NegAbsEdges()
	n := len(src) / 4
	want := make([]uint64, 4*n)
	scalarD256Negate(src, want)
	for _, im := range d256NegateImpls() {
		got := make([]uint64, 4*n)
		im.fn(src, got)
		for i := 0; i < 4*n; i++ {
			if got[i] != want[i] {
				t.Fatalf("%s idx=%d: got 0x%x want 0x%x", im.name, i, got[i], want[i])
			}
		}
	}
}

func TestD256AbsCorrectness(t *testing.T) {
	for _, n := range d256Sizes() {
		src := makeRandD256(n, 0xABA256^uint64(n))
		want := make([]uint64, 4*n)
		scalarD256Abs(src, want)
		for _, im := range d256AbsImpls() {
			got := make([]uint64, 4*n)
			im.fn(src, got)
			for i := 0; i < 4*n; i++ {
				if got[i] != want[i] {
					t.Fatalf("%s n=%d idx=%d: got 0x%x want 0x%x", im.name, n, i, got[i], want[i])
				}
			}
		}
	}
}

func TestD256AbsEdges(t *testing.T) {
	src := d256NegAbsEdges()
	n := len(src) / 4
	want := make([]uint64, 4*n)
	scalarD256Abs(src, want)
	for _, im := range d256AbsImpls() {
		got := make([]uint64, 4*n)
		im.fn(src, got)
		for i := 0; i < 4*n; i++ {
			if got[i] != want[i] {
				t.Fatalf("%s idx=%d: got 0x%x want 0x%x", im.name, i, got[i], want[i])
			}
		}
	}
}

func benchmarkD256Unary(b *testing.B, fn func(src, dst []uint64), n int) {
	src := makeRandD256(n, 0xBEEF^uint64(n))
	dst := make([]uint64, 4*n)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fn(src, dst)
	}
}

func BenchmarkD256Negate(b *testing.B) {
	for _, n := range []int{16, 64, 256, 1024, 4096} {
		for _, im := range d256NegateImpls() {
			b.Run(im.name+"/n="+itoa(n), func(b *testing.B) {
				benchmarkD256Unary(b, im.fn, n)
			})
		}
	}
}

func BenchmarkD256Abs(b *testing.B) {
	for _, n := range []int{16, 64, 256, 1024, 4096} {
		for _, im := range d256AbsImpls() {
			b.Run(im.name+"/n="+itoa(n), func(b *testing.B) {
				benchmarkD256Unary(b, im.fn, n)
			})
		}
	}
}
