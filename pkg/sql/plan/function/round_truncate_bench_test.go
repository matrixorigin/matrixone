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

package function

import (
	"math"
	"math/rand"
	"testing"
)

// PR review 5668724880 (item 2) repro magnitude: 1024 rows at the ~1e20
// scale. The grid variant is the exact identity case reported; the off-grid
// variant exercises the plain double lane, which must also stay allocation
// free (only ULP-boundary rows may delegate to the exact decimal path).
var benchRoundTruncateHighGrid1024 = func() []float64 {
	v := make([]float64, 1024)
	r := rand.New(rand.NewSource(7))
	for i := range v {
		// multiples of 10 in the 1e17..1e21 range: on-grid lane.
		v[i] = float64(int64(r.Float64()*9e4)+1) * 1e17
		if i%2 == 1 {
			v[i] = -v[i]
		}
	}
	return v
}()

var benchRoundTruncateHighOffGrid1024 = func() []float64 {
	v := make([]float64, 1024)
	r := rand.New(rand.NewSource(11))
	for i := range v {
		// magnitudes ~1e18..1e21, off the -1/-3 grids.
		v[i] = (r.Float64() - 0.5) * 2 * math.Pow(10, float64(18+i%4))
	}
	return v
}()

// Benchmarks for issue 31154: the exact decimal path must only pay its cost
// at the boundary scales; the common small negative digits (-1/-2) on
// ordinary magnitudes keep the plain float64 fast path. Input size is 1024
// rows per benchmark op.
var benchRoundTruncateNormal1024 = func() []float64 {
	v := make([]float64, 1024)
	r := rand.New(rand.NewSource(42))
	for i := range v {
		// mixed signs, magnitudes from ~2^-1 to ~2^39: all inside the
		// fast-path guard (|x| <= 2^52*10^k) for k in {1,2}.
		v[i] = (r.Float64() - 0.5) * 2 * float64(uint64(1)<<uint(i%40))
	}
	return v
}()

// Extreme magnitudes forced through the exact decimal path. Inputs stay
// inside the finite DOUBLE answer range at -307 and -308 so the benchmark
// does not hit the intended ER_DATA_OUT_OF_RANGE (issue 31154) error path.
var benchRoundTruncateExtreme = []float64{
	1e308, -1e308, 1e200, -1e200, 5e307, -5e307, 9.9e307, -9.9e307,
}

var benchRoundTruncateSink float64

func benchRoundTruncate(b *testing.B, values []float64, digits int64, truncate bool) {
	b.ReportAllocs()
	sink := 0.0
	for i := 0; i < b.N; i++ {
		for _, x := range values {
			if truncate {
				if v, err := truncateFloat64(x, digits); err == nil {
					sink += v
				}
			} else {
				if v, err := roundFloat64(x, digits); err == nil {
					sink += v
				}
			}
		}
	}
	benchRoundTruncateSink = sink
}

func BenchmarkRoundFloat64DigitsNeg1Normal1024(b *testing.B) {
	benchRoundTruncate(b, benchRoundTruncateNormal1024, -1, false)
}

func BenchmarkRoundFloat64DigitsNeg2Normal1024(b *testing.B) {
	benchRoundTruncate(b, benchRoundTruncateNormal1024, -2, false)
}

func BenchmarkTruncateFloat64DigitsNeg1Normal1024(b *testing.B) {
	benchRoundTruncate(b, benchRoundTruncateNormal1024, -1, true)
}

func BenchmarkTruncateFloat64DigitsNeg2Normal1024(b *testing.B) {
	benchRoundTruncate(b, benchRoundTruncateNormal1024, -2, true)
}

func BenchmarkRoundFloat64DigitsNeg307Extreme(b *testing.B) {
	benchRoundTruncate(b, benchRoundTruncateExtreme, -307, false)
}

func BenchmarkRoundFloat64DigitsNeg308Extreme(b *testing.B) {
	benchRoundTruncate(b, benchRoundTruncateExtreme, -308, false)
}

func BenchmarkRoundFloat64DigitsNeg1HighGrid1024(b *testing.B) {
	benchRoundTruncate(b, benchRoundTruncateHighGrid1024, -1, false)
}

func BenchmarkTruncateFloat64DigitsNeg1HighGrid1024(b *testing.B) {
	benchRoundTruncate(b, benchRoundTruncateHighGrid1024, -1, true)
}

func BenchmarkRoundFloat64DigitsNeg1HighOffGrid1024(b *testing.B) {
	benchRoundTruncate(b, benchRoundTruncateHighOffGrid1024, -1, false)
}

func BenchmarkTruncateFloat64DigitsNeg3HighOffGrid1024(b *testing.B) {
	benchRoundTruncate(b, benchRoundTruncateHighOffGrid1024, -3, true)
}

// PR review 5673058412 snap lane: all 1024 rows are the exact-fallback
// representation fl(1.5e26) == RN(15·10^25) with mixed signs, so each row
// walks the near-grid window and the exact-grid recognizer at digits -25.
// This documents the cost class of the boundary snap; ordinary and extreme
// lanes are covered above.
var benchRoundTruncateExactGridSnap1024 = func() []float64 {
	v := make([]float64, 1024)
	for i := range v {
		v[i] = 1.5e26
		if i%2 == 1 {
			v[i] = -v[i]
		}
	}
	return v
}()

func BenchmarkRoundFloat64DigitsNeg25ExactGridSnap1024(b *testing.B) {
	benchRoundTruncate(b, benchRoundTruncateExactGridSnap1024, -25, false)
}

func BenchmarkTruncateFloat64DigitsNeg25ExactGridSnap1024(b *testing.B) {
	benchRoundTruncate(b, benchRoundTruncateExactGridSnap1024, -25, true)
}
