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

package types

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// Decoded-value contract (PR #29554 review): an encoder-produced NVFP4 cell whose element's
// fully-scaled value is a representable float32 subnormal must decode NONZERO, and At and
// Dequantize must agree. Before the fix, element*(global*blockScale) underflowed the inner float32
// product to zero, so the whole vector decoded to zero -- disagreeing with the GPU, which keeps the
// element nonzero. Matrix: dims {1,16,17} x {zero, smallest-positive-float32, ordinary}; the
// smallest cases are the ones that were wrong, zero/ordinary are controls.
func TestBlockScaledSubnormalDecodesNonzero29554(t *testing.T) {
	for _, dim := range []int{1, 16, 17} {
		for _, tc := range []struct {
			name     string
			fill     float32
			wantZero bool
		}{
			{"zero", 0, true},
			{"smallest", math.SmallestNonzeroFloat32, false},
			{"ordinary", 1.5, false},
		} {
			v := make([]float32, dim)
			for i := range v {
				v[i] = tc.fill
			}
			cell, err := AppendBlockScaled(nil, BlockScaledNVFP4, v)
			require.NoError(t, err, "dim=%d %s", dim, tc.name)
			c, err := ParseBlockScaledCell(cell)
			require.NoError(t, err, "dim=%d %s", dim, tc.name)

			deq := make([]float32, dim)
			c.Dequantize(deq)
			anyNonzero := false
			for i := 0; i < dim; i++ {
				require.Equalf(t, c.At(i), deq[i], "dim=%d %s: At and Dequantize must agree at %d", dim, tc.name, i)
				if c.At(i) != 0 {
					anyNonzero = true
				}
			}
			if tc.wantZero {
				require.Falsef(t, anyNonzero, "dim=%d %s: must decode to the zero vector", dim, tc.name)
			} else {
				require.Truef(t, anyNonzero, "dim=%d %s: a representable vector must not decode to zero", dim, tc.name)
			}
		}
	}
}
