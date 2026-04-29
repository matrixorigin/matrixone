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
	"math/rand/v2"
	"testing"
)

// pow10 mirrors types.Pow10 up to 10^18; sufficient for D64 scale factors.
var pow10 = [...]uint64{
	1,
	10,
	100,
	1000,
	10000,
	100000,
	1000000,
	10000000,
	100000000,
	1000000000,
	10000000000,
	100000000000,
	1000000000000,
	10000000000000,
	100000000000000,
	1000000000000000,
	10000000000000000,
	100000000000000000,
	1000000000000000000,
}

func d64ScaleSizes() []int {
	return []int{0, 1, 3, 4, 5, 7, 8, 15, 16, 17, 31, 32, 33, 63, 64, 127, 1024, 4096}
}

type d64ScaleImpl struct {
	name string
	fn   func(vec, rs []uint64, f uint64)
}

func d64ScaleImpls() []d64ScaleImpl {
	out := []d64ScaleImpl{{name: "scalar", fn: scalarD64ScaleUnchecked}}
	if D64ScaleUnchecked != nil {
		out = append(out, d64ScaleImpl{name: "dispatch", fn: D64ScaleUnchecked})
	}
	return out
}

// makeRandSafeD64 returns n int64 lanes, each guaranteed to satisfy
// |v| ≤ MaxInt64 / f (the unchecked-path precondition).
func makeRandSafeD64(n int, f uint64, seed uint64) []uint64 {
	rng := rand.New(rand.NewPCG(seed, seed^0xDEADBEEF))
	maxAbs := uint64(math.MaxInt64) / f
	out := make([]uint64, n)
	for i := range out {
		v := int64(rng.Uint64N(maxAbs + 1))
		if rng.Uint64()&1 == 1 {
			v = -v
		}
		out[i] = uint64(v)
	}
	return out
}

func TestD64ScaleUncheckedCorrectness(t *testing.T) {
	for _, scale := range []int{1, 2, 6, 12, 18} {
		f := pow10[scale]
		for _, n := range d64ScaleSizes() {
			vec := makeRandSafeD64(n, f, uint64(scale*1000+n))
			want := make([]uint64, n)
			scalarD64ScaleUnchecked(vec, want, f)
			for _, im := range d64ScaleImpls() {
				got := make([]uint64, n)
				im.fn(vec, got, f)
				for i := 0; i < n; i++ {
					if got[i] != want[i] {
						t.Fatalf("%s scale=%d n=%d idx=%d: got 0x%x want 0x%x (vec=%d)",
							im.name, scale, n, i, got[i], want[i], int64(vec[i]))
					}
				}
			}
		}
	}
}

func TestD64ScaleUncheckedEdges(t *testing.T) {
	cases := []struct {
		name string
		vec  []int64
		f    uint64
	}{
		{"zeros", []int64{0, 0, 0, 0, 0}, 1000},
		{"ones", []int64{1, -1, 1, -1, 1, -1, 1, -1, 1}, pow10[6]},
		{"max_safe", []int64{
			int64(math.MaxInt64 / pow10[3]), -int64(math.MaxInt64 / pow10[3]),
			int64(math.MaxInt64 / pow10[3]), -int64(math.MaxInt64 / pow10[3]),
		}, pow10[3]},
		{"alt_signs", []int64{1, -1000, 12345, -987654321, 0, 99, -42, 100000}, pow10[2]},
	}
	for _, tc := range cases {
		vec := make([]uint64, len(tc.vec))
		for i, v := range tc.vec {
			vec[i] = uint64(v)
		}
		want := make([]uint64, len(vec))
		scalarD64ScaleUnchecked(vec, want, tc.f)
		for _, im := range d64ScaleImpls() {
			got := make([]uint64, len(vec))
			im.fn(vec, got, tc.f)
			for i := range vec {
				if got[i] != want[i] {
					t.Fatalf("%s/%s idx=%d: got %d want %d", tc.name, im.name, i,
						int64(got[i]), int64(want[i]))
				}
			}
		}
	}
}

func benchmarkD64Scale(b *testing.B, fn func(vec, rs []uint64, f uint64), n int, f uint64) {
	vec := makeRandSafeD64(n, f, 0xC0FFEE^uint64(n))
	rs := make([]uint64, n)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fn(vec, rs, f)
	}
}

func BenchmarkD64ScaleUnchecked(b *testing.B) {
	f := pow10[6]
	for _, n := range []int{16, 64, 256, 1024, 4096} {
		for _, im := range d64ScaleImpls() {
			b.Run(im.name+"/n="+itoa(n), func(b *testing.B) {
				benchmarkD64Scale(b, im.fn, n, f)
			})
		}
	}
}
