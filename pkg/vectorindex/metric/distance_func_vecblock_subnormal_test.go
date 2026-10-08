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

package metric

import (
	"fmt"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// The decoded-value / zero-vector contract must hold on CPU and GPU alike, independent of executor
// eligibility (PR #29554 review): an NVFP4 cell whose element decodes to a representable float32
// subnormal is a NONZERO vector, so its self cosine distance is 0. The CPU returned 1 when
// element*(global*blockScale) underflowed the inner float32 product to zero (decoding the whole
// vector as the zero vector); the GPU keeps the element nonzero (global applied in double) and
// returns 0, so a 1 here is a CPU/GPU divergence. Matrix: dims {1,16,17} x {zero -> distance 1,
// smallest-positive-float32 -> 0, ordinary nonzero -> 0}. The smallest cases are the ones that were
// wrong; zero and ordinary are controls. Covers the scalar path (dim 1), the unit kernel (dim 16),
// and kernel + tail (dim 17).
func TestVecBlockSubnormalSelfCosine29554(t *testing.T) {
	for _, dim := range []int{1, 16, 17} {
		for _, tc := range []struct {
			name string
			fill float32
			want float64
		}{
			{"zero", 0, 1},
			{"smallest", math.SmallestNonzeroFloat32, 0},
			{"ordinary", 1.5, 0},
		} {
			t.Run(fmt.Sprintf("dim%d/%s", dim, tc.name), func(t *testing.T) {
				v := make([]float32, dim)
				for i := range v {
					v[i] = tc.fill
				}
				op, _ := vecBlockTestOperand(t, types.BlockScaledNVFP4, v)
				d, err := VecBlockCosineDistance(op, op)
				require.NoError(t, err)
				require.Equalf(t, tc.want, d, "dim=%d fill=%v self cosine distance", dim, tc.fill)
			})
		}
	}
}
