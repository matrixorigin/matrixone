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
	"unsafe"

	"github.com/stretchr/testify/require"
)

// TestPackBools checks packBools, which the Gobra proof trusts, against its
// contract: bit k is values[base+k] for k < end-base, the rest are clear.
func TestPackBools(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	for _, n := range []int{0, 1, 7, 8, 9, 63, 64, 65, 130} {
		values := make([]bool, n)
		// Bools backed by arbitrary bytes, as a vector read from storage could
		// hold; any non-zero byte is true.
		raw := unsafe.Slice((*byte)(unsafe.Pointer(unsafe.SliceData(values))), n)
		for i := range raw {
			switch rng.Intn(3) {
			case 0:
				raw[i] = 0
			case 1:
				raw[i] = 1
			default:
				raw[i] = byte(rng.Intn(256))
			}
		}
		for base := 0; base <= n; base++ {
			for end := base; end <= n && end-base <= 64; end++ {
				got := packBools(values, base, end)
				for k := 0; k < 64; k++ {
					want := k < end-base && raw[base+k] != 0
					require.Equal(t, want, got&(1<<uint(k)) != 0, "n=%d base=%d end=%d k=%d", n, base, end, k)
				}
			}
		}
	}
}
