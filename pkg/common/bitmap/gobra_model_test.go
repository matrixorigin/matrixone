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
	"math/bits"
	"math/rand/v2"
	"testing"
)

// TestGobraBitModel checks every trusted fact of the Gobra bit model in
// gobra/stubs/math/bits/bits.gobra against the real Go operators. The proof
// of bitmap.go is only as sound as these facts.
func TestGobraBitModel(t *testing.T) {
	bit := func(w uint64, k int) bool { return w>>uint(k)&1 == 1 }
	rng := rand.New(rand.NewPCG(1, 2))
	words := []uint64{0, 1, ^uint64(0), 1 << 63, 0x5555555555555555, 0xaaaaaaaaaaaaaaaa}
	for k := 0; k < 64; k++ {
		words = append(words, uint64(1)<<k, ^(uint64(1) << k), ^uint64(0)<<k, ^uint64(0)>>k)
	}
	for range 2000 {
		words = append(words, rng.Uint64(), rng.Uint64()&rng.Uint64(), rng.Uint64()|rng.Uint64())
	}
	pairs := func(f func(a, b uint64)) {
		for i, a := range words {
			f(a, words[(i*7+3)%len(words)])
			f(a, words[(i*13+1)%len(words)])
		}
	}

	for k := 0; k < 64; k++ {
		if bit(0, k) {
			t.Fatalf("zero: bit %d set", k)
		}
	}
	pairs(func(a, b uint64) {
		for k := 0; k < 64; k++ {
			if bit(a&b, k) != (bit(a, k) && bit(b, k)) ||
				bit(a|b, k) != (bit(a, k) || bit(b, k)) ||
				bit(a&^b, k) != (bit(a, k) && !bit(b, k)) ||
				bit(a^b, k) != (bit(a, k) != bit(b, k)) {
				t.Fatalf("binary op axiom fails for %#x, %#x at bit %d", a, b, k)
			}
		}
	})
	for _, a := range words {
		pc, tz, low := 0, -1, false
		for k := 0; k < 64; k++ {
			if bit(^a, k) != !bit(a, k) {
				t.Fatalf("not axiom fails for %#x at bit %d", a, k)
			}
			if bit(a, k) {
				pc++
				if tz < 0 {
					tz = k
				}
				low = true
			}
		}
		if bits.OnesCount64(a) != pc {
			t.Fatalf("OnesCount64(%#x) = %d, model %d", a, bits.OnesCount64(a), pc)
		}
		if a != 0 && (bits.TrailingZeros64(a) != tz || !low) {
			t.Fatalf("TrailingZeros64(%#x) = %d, model %d", a, bits.TrailingZeros64(a), tz)
		}
		for s := uint64(0); s < 64; s++ {
			if (a&(uint64(1)<<s) != 0) != bit(a, int(s)) {
				t.Fatalf("single-bit test fails for %#x at %d", a, s)
			}
		}
	}
	for s := uint64(0); s < 64; s++ {
		for k := 0; k < 64; k++ {
			if bit(uint64(1)<<s, k) != (k == int(s)) ||
				bit(^uint64(0)<<s, k) != (k >= int(s)) ||
				bit(^uint64(0)>>s, k) != (k < 64-int(s)) {
				t.Fatalf("shift axiom fails for shift %d at bit %d", s, k)
			}
		}
	}
}
