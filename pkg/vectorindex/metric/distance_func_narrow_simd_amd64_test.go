//go:build amd64 && go1.27 && goexperiment.simd

// Copyright 2023 Matrix Origin
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

package metric

import (
	"math/rand"
	"testing"
)

// scalar oracle
func l1Uint8Oracle(a, b []uint8) float64 {
	var s int64
	for i := range a {
		d := int32(a[i]) - int32(b[i])
		if d < 0 {
			d = -d
		}
		s += int64(d)
	}
	return float64(s)
}

// Exact equality is the right bar: every value is an integer well inside
// float64's exact range, so any difference is a real defect, not rounding.
func TestL1DistanceUint8SIMD(t *testing.T) {
	if !hasAVX512 {
		t.Skip("AVX-512 not available")
	}
	// Lengths chosen around the 64-byte block: below it (pure tail), exactly
	// on it, one over, and several blocks plus a ragged tail.
	for _, n := range []int{0, 1, 7, 63, 64, 65, 127, 128, 1023, 1024} {
		a := make([]uint8, n)
		b := make([]uint8, n)
		for i := range a {
			a[i] = uint8(rand.Intn(256))
			b[i] = uint8(rand.Intn(256))
		}
		want := l1Uint8Oracle(a, b)

		got, err := l1DistanceUint8SIMD(a, b)
		if err != nil {
			t.Fatalf("n=%d: %v", n, err)
		}
		if got != want {
			t.Errorf("n=%d: SAD kernel = %v, oracle = %v", n, got, want)
		}

	}

	// Extremes: max possible per-element difference in both directions.
	n := 1024
	hi, lo := make([]uint8, n), make([]uint8, n)
	for i := range hi {
		hi[i], lo[i] = 255, 0
	}
	got, _ := l1DistanceUint8SIMD(hi, lo)
	if want := float64(255 * n); got != want {
		t.Errorf("all-max: got %v want %v", got, want)
	}
	got, _ = l1DistanceUint8SIMD(lo, hi)
	if want := float64(255 * n); got != want {
		t.Errorf("all-max reversed: got %v want %v", got, want)
	}

	if _, err := l1DistanceUint8SIMD(make([]uint8, 4), make([]uint8, 5)); err == nil {
		t.Error("mismatched lengths must error")
	}
}

func BenchmarkL1DistanceUint8SIMD(b *testing.B) {
	if !hasAVX512 {
		b.Skip("AVX-512 not available")
	}
	const dim, pool = 1024, 256
	xs, ys := make([][]uint8, pool), make([][]uint8, pool)
	for i := range xs {
		xs[i], ys[i] = make([]uint8, dim), make([]uint8, dim)
		for j := 0; j < dim; j++ {
			xs[i][j] = uint8(rand.Intn(256))
			ys[i][j] = uint8(rand.Intn(256))
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = l1DistanceUint8SIMD(xs[i%pool], ys[i%pool])
	}
}

// TestNarrowSIMDMatchesScalar checks every AVX-512 narrow kernel against the
// scalar implementation in distance_func_narrow.go. The scalar version is the
// definition of the score, so any divergence is a bug in the vector kernel.
//
// Dimensions sweep 0..200 plus block boundaries, which covers every tail
// remainder for both the 32-element (VPMADDWD) and 64-element (VPSADBW) loops.
func TestNarrowSIMDMatchesScalar(t *testing.T) {
	if !hasAVX512 {
		t.Skip("AVX-512 not available")
	}
	rnd := rand.New(rand.NewSource(20260821))

	i8 := []struct {
		name         string
		simd, scalar func(a, b []int8) (float64, error)
	}{
		{"l2sqInt8", l2sqInt8SIMD, l2sqInt8},
		{"innerProductInt8", innerProductInt8SIMD, innerProductInt8},
		{"l1DistanceInt8", l1DistanceInt8SIMD, l1DistanceInt8},
		{"cosineDistanceInt8", cosineDistanceInt8SIMD, cosineDistanceInt8},
	}
	u8 := []struct {
		name         string
		simd, scalar func(a, b []uint8) (float64, error)
	}{
		{"l2sqUint8", l2sqUint8SIMD, l2sqUint8},
		{"innerProductUint8", innerProductUint8SIMD, innerProductUint8},
		{"l1DistanceUint8", l1DistanceUint8SIMD, l1DistanceUint8},
		{"cosineDistanceUint8", cosineDistanceUint8SIMD, cosineDistanceUint8},
	}

	dims := make([]int, 0, 210)
	for d := 0; d <= 200; d++ {
		dims = append(dims, d)
	}
	dims = append(dims, 255, 256, 257, 511, 512, 513, 1023, 1024, 1025)

	for _, dim := range dims {
		for rep := 0; rep < 6; rep++ {
			ai := make([]int8, dim)
			bi := make([]int8, dim)
			au := make([]uint8, dim)
			bu := make([]uint8, dim)
			for k := 0; k < dim; k++ {
				switch rep {
				case 0: // extreme magnitudes
					ai[k], bi[k] = -128, 127
					au[k], bu[k] = 255, 0
				case 1:
					ai[k], bi[k] = 127, -128
					au[k], bu[k] = 0, 255
				case 2: // identical inputs
					ai[k] = int8(rnd.Intn(256) - 128)
					bi[k] = ai[k]
					au[k] = uint8(rnd.Intn(256))
					bu[k] = au[k]
				case 3: // all zero: exercises the cosine zero-denominator branch
				default:
					ai[k], bi[k] = int8(rnd.Intn(256)-128), int8(rnd.Intn(256)-128)
					au[k], bu[k] = uint8(rnd.Intn(256)), uint8(rnd.Intn(256))
				}
			}
			for _, c := range i8 {
				got, err1 := c.simd(ai, bi)
				want, err2 := c.scalar(ai, bi)
				if (err1 == nil) != (err2 == nil) {
					t.Fatalf("%s dim=%d rep=%d: err %v vs scalar %v", c.name, dim, rep, err1, err2)
				}
				if got != want {
					t.Fatalf("%s dim=%d rep=%d: simd=%v scalar=%v", c.name, dim, rep, got, want)
				}
			}
			for _, c := range u8 {
				got, err1 := c.simd(au, bu)
				want, err2 := c.scalar(au, bu)
				if (err1 == nil) != (err2 == nil) {
					t.Fatalf("%s dim=%d rep=%d: err %v vs scalar %v", c.name, dim, rep, err1, err2)
				}
				if got != want {
					t.Fatalf("%s dim=%d rep=%d: simd=%v scalar=%v", c.name, dim, rep, got, want)
				}
			}
		}
	}

	// mismatched lengths must error, not compute a partial score
	for _, c := range i8 {
		if _, err := c.simd(make([]int8, 8), make([]int8, 9)); err == nil {
			t.Errorf("%s: expected dimension-mismatch error", c.name)
		}
	}
	for _, c := range u8 {
		if _, err := c.simd(make([]uint8, 8), make([]uint8, 9)); err == nil {
			t.Errorf("%s: expected dimension-mismatch error", c.name)
		}
	}
}
