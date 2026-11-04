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

// Every public block-scaled metric decodes a cell element as BlockScaledCell.At does, in the
// full-unit kernels and in the tail alike. The oracle is the float64 sum of the At-decoded terms;
// a result may differ from it only by float32 accumulation, bounded relative to the sum of the
// absolute terms. A cell holding one 2^-149 element at the first, unit-boundary or last position,
// against a query of 1e30, has a single representable term: a kernel that decodes it to zero
// returns 0 instead of ±1.4e-15.
func TestVecBlockMetricsUseAtDecode(t *testing.T) {
	const amp = float32(1e30)
	encode := func(t *testing.T, f types.BlockScaledFormat, v []float32) *VecBlockOperand {
		cell, err := types.AppendBlockScaled(nil, f, v)
		require.NoError(t, err)
		c, err := types.ParseBlockScaledCell(cell)
		require.NoError(t, err)
		return &VecBlockOperand{Cell: c}
	}
	at := func(o *VecBlockOperand, i int) float64 {
		if o.F32 != nil {
			return float64(o.F32[i])
		}
		return float64(o.Cell.At(i))
	}
	check := func(t *testing.T, name string, got, want, absSum float64) {
		require.Falsef(t, math.Signbit(got) != math.Signbit(want) && want != 0,
			"%s: got %v, want %v", name, got, want)
		require.LessOrEqualf(t, math.Abs(got-want), 1e-5*absSum,
			"%s: got %v, want %v (sum of |terms| %v)", name, got, want, absSum)
	}
	assertMetrics := func(t *testing.T, x, ampY, zeroY *VecBlockOperand) {
		n := x.Dim()
		var dot, dotAbs, l1 float64
		for i := 0; i < n; i++ {
			term := at(x, i) * at(ampY, i)
			dot += term
			dotAbs += math.Abs(term)
			l1 += math.Abs(at(x, i) - at(zeroY, i))
		}
		got, err := VecBlockDot(x, ampY)
		require.NoError(t, err)
		check(t, "dot", got, dot, dotAbs)
		gotDot, _, _, err := VecBlockCosineParts(x, ampY)
		require.NoError(t, err)
		check(t, "cosine dot", gotDot, dot, dotAbs)
		got, err = VecBlockL1Distance(x, zeroY)
		require.NoError(t, err)
		check(t, "l1", got, l1, l1)
	}

	dims := []int{1, 15, 16, 17, 31, 32, 33}
	for _, f := range []types.BlockScaledFormat{types.BlockScaledNVFP4, types.BlockScaledMXFP8} {
		for _, dim := range dims {
			var cells [][]float32
			for _, pos := range []int{0, 16, dim - 1} {
				if pos >= dim {
					continue
				}
				for _, sign := range []float32{1, -1} {
					v := make([]float32, dim)
					v[pos] = sign * math.SmallestNonzeroFloat32
					cells = append(cells, v)
				}
			}
			for _, fill := range vecBlockBoundaryFills {
				if fill > 1e8 {
					continue // keeps fill*amp finite in float32
				}
				v := make([]float32, dim)
				for i := range v {
					v[i] = fill
				}
				cells = append(cells, v)
			}
			ampF32 := make([]float32, dim)
			for i := range ampF32 {
				ampF32[i] = amp
			}
			zeroF32 := make([]float32, dim)
			for ci, v := range cells {
				t.Run(fmt.Sprintf("%s/dim%d/cell%d", f, dim, ci), func(t *testing.T) {
					x := encode(t, f, v)
					// F8F32 / F4F32
					assertMetrics(t, x, &VecBlockOperand{F32: ampF32}, &VecBlockOperand{F32: zeroF32})
					// F8F8 / F4F4
					assertMetrics(t, x, encode(t, f, ampF32), encode(t, f, zeroF32))
					// F8F4, with the cell under test on either side
					other := types.BlockScaledMXFP8
					if f == types.BlockScaledMXFP8 {
						other = types.BlockScaledNVFP4
					}
					assertMetrics(t, x, encode(t, other, ampF32), encode(t, other, zeroF32))
				})
			}
		}
	}
}
