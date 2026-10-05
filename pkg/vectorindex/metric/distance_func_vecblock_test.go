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
	"math/rand"
	"strconv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// vecBlockKinds names the operand encodings: 0 vecf32, else a block-scaled format.
var vecBlockKinds = []types.BlockScaledFormat{0, types.BlockScaledMXFP8, types.BlockScaledNVFP4}

func vecBlockTestOperand(t testing.TB, f types.BlockScaledFormat, v []float32) (*VecBlockOperand, []float64) {
	if f == 0 {
		ref := make([]float64, len(v))
		for i, x := range v {
			ref[i] = float64(x)
		}
		return &VecBlockOperand{F32: v}, ref
	}
	cell, err := types.AppendBlockScaled(nil, f, v)
	require.NoError(t, err)
	c, err := types.ParseBlockScaledCell(cell)
	require.NoError(t, err)
	deq := make([]float32, c.Dim)
	c.Dequantize(deq)
	ref := make([]float64, len(deq))
	for i, x := range deq {
		ref[i] = float64(x)
	}
	return &VecBlockOperand{Cell: c}, ref
}

func vecBlockRandom(r *rand.Rand, dim int, scale float64) []float32 {
	v := make([]float32, dim)
	for i := range v {
		v[i] = float32(r.NormFloat64() * scale)
	}
	return v
}

func requireRel(t *testing.T, want, got float64, msg string) {
	t.Helper()
	require.InDelta(t, want, got, 1e-5*math.Max(1, math.Abs(want)), msg)
}

func TestVecBlockKernelsMatchReference(t *testing.T) {
	r := rand.New(rand.NewSource(7))
	dims := []int{1, 2, 3, 15, 16, 17, 31, 32, 33, 47, 48, 64, 100, 768, 1000}
	for _, fx := range vecBlockKinds {
		for _, fy := range vecBlockKinds {
			for _, dim := range dims {
				for _, scale := range []float64{1e-3, 1, 1e4} {
					x, rx := vecBlockTestOperand(t, fx, vecBlockRandom(r, dim, scale))
					y, ry := vecBlockTestOperand(t, fy, vecBlockRandom(r, dim, scale))
					var dot, l2, l1, nx, ny float64
					for i := range rx {
						dot += rx[i] * ry[i]
						l2 += (rx[i] - ry[i]) * (rx[i] - ry[i])
						l1 += math.Abs(rx[i] - ry[i])
						nx += rx[i] * rx[i]
						ny += ry[i] * ry[i]
					}
					msg := func(m string) string {
						return m + " " + fx.String() + "x" + fy.String() + " dim " + strconv.Itoa(dim)
					}
					got, err := VecBlockDot(x, y)
					require.NoError(t, err)
					requireRel(t, dot, got, msg("dot"))
					got, err = VecBlockL2DistanceSq(x, y)
					require.NoError(t, err)
					requireRel(t, l2, got, msg("l2sq"))
					got, err = VecBlockL1Distance(x, y)
					require.NoError(t, err)
					requireRel(t, l1, got, msg("l1"))
					gd, gx, gy, err := VecBlockCosineParts(x, y)
					require.NoError(t, err)
					requireRel(t, dot, gd, msg("cos dot"))
					requireRel(t, nx, gx, msg("cos nx"))
					requireRel(t, ny, gy, msg("cos ny"))
					cos := max(-1, min(1, dot/(math.Sqrt(nx)*math.Sqrt(ny))))
					got, err = VecBlockCosineSimilarity(x, y)
					require.NoError(t, err)
					require.InDelta(t, cos, got, 1e-6, msg("cosine similarity"))
					got, err = VecBlockCosineDistance(x, y)
					require.NoError(t, err)
					require.InDelta(t, 1-cos, got, 1e-6, msg("cosine distance"))
				}
			}
		}
	}
}

func TestVecBlockKernelsSymmetric(t *testing.T) {
	r := rand.New(rand.NewSource(11))
	for _, fx := range vecBlockKinds {
		for _, fy := range vecBlockKinds {
			x, _ := vecBlockTestOperand(t, fx, vecBlockRandom(r, 70, 1))
			y, _ := vecBlockTestOperand(t, fy, vecBlockRandom(r, 70, 1))
			for _, fn := range []func(x, y *VecBlockOperand) (float64, error){
				VecBlockDot, VecBlockL2DistanceSq, VecBlockL1Distance, VecBlockCosineDistance, VecBlockCosineSimilarity,
			} {
				a, err := fn(x, y)
				require.NoError(t, err)
				b, err := fn(y, x)
				require.NoError(t, err)
				require.Equal(t, a, b)
			}
			d1, nx1, ny1, err := VecBlockCosineParts(x, y)
			require.NoError(t, err)
			d2, nx2, ny2, err := VecBlockCosineParts(y, x)
			require.NoError(t, err)
			require.Equal(t, []float64{d1, nx1, ny1}, []float64{d2, ny2, nx2})
		}
	}
}

func TestVecBlockKernelsExact(t *testing.T) {
	// E2M1 values with every 16-element block peaking at 6 are exact in both formats.
	v := []float32{1, -2, 0.5, 3, 0, -1, 2, 1.5, 4, -3, 1, 0.5, 2, -0.5, 1, 6, 6, 2}
	w := []float32{2, 1, -1, 0.5, 1, 1, -2, 0, 1, 1, 3, 2, -1, 4, 0.5, -6, -3, -6}
	var dot, l2, l1 float64
	for i := range v {
		dot += float64(v[i] * w[i])
		l2 += float64((v[i] - w[i]) * (v[i] - w[i]))
		l1 += math.Abs(float64(v[i] - w[i]))
	}
	for _, fx := range vecBlockKinds {
		for _, fy := range vecBlockKinds {
			x, rx := vecBlockTestOperand(t, fx, v)
			y, ry := vecBlockTestOperand(t, fy, w)
			for i := range v {
				require.Equal(t, float64(v[i]), rx[i], "%s elem %d", fx, i)
				require.Equal(t, float64(w[i]), ry[i], "%s elem %d", fy, i)
			}
			got, err := VecBlockDot(x, y)
			require.NoError(t, err)
			require.Equal(t, dot, got)
			got, err = VecBlockL2DistanceSq(x, y)
			require.NoError(t, err)
			require.Equal(t, l2, got)
			got, err = VecBlockL1Distance(x, y)
			require.NoError(t, err)
			require.Equal(t, l1, got)
		}
	}
}

func TestVecBlockKernelsEdgeCases(t *testing.T) {
	for _, f := range vecBlockKinds[1:] {
		x, _ := vecBlockTestOperand(t, f, []float32{1, 2, 3})
		short, _ := vecBlockTestOperand(t, f, []float32{1, 2})
		for _, fn := range []func(x, y *VecBlockOperand) (float64, error){
			VecBlockDot, VecBlockL2DistanceSq, VecBlockL1Distance, VecBlockCosineDistance, VecBlockCosineSimilarity,
		} {
			_, err := fn(x, short)
			require.Error(t, err)
		}
		_, _, _, err := VecBlockCosineParts(x, short)
		require.Error(t, err)

		zero, _ := vecBlockTestOperand(t, f, []float32{0, 0, 0})
		d, err := VecBlockCosineDistance(x, zero)
		require.NoError(t, err)
		require.Equal(t, 1.0, d)
		_, err = VecBlockCosineSimilarity(zero, x)
		require.Error(t, err)

		d, err = VecBlockCosineSimilarity(x, x)
		require.NoError(t, err)
		require.InDelta(t, 1.0, d, 1e-12)
		neg, _ := vecBlockTestOperand(t, f, []float32{-1, -2, -3})
		d, err = VecBlockCosineDistance(x, neg)
		require.NoError(t, err)
		require.InDelta(t, 2.0, d, 1e-12)
	}
	// Two vecf32 operands take the per-element path.
	x := &VecBlockOperand{F32: []float32{1, 2, 3, 4}}
	y := &VecBlockOperand{F32: []float32{4, 3, 2, 1}}
	d, err := VecBlockDot(x, y)
	require.NoError(t, err)
	require.Equal(t, 20.0, d)
	d, err = VecBlockL2DistanceSq(x, y)
	require.NoError(t, err)
	require.Equal(t, 20.0, d)
	d, err = VecBlockL1Distance(x, y)
	require.NoError(t, err)
	require.Equal(t, 8.0, d)
	dot, nx, ny, err := VecBlockCosineParts(x, y)
	require.NoError(t, err)
	require.Equal(t, []float64{20, 30, 30}, []float64{dot, nx, ny})
}

func BenchmarkVecBlockKernels(b *testing.B) {
	r := rand.New(rand.NewSource(1))
	const dim = 768
	v1, v2 := vecBlockRandom(r, dim, 1), vecBlockRandom(r, dim, 1)
	for _, fx := range vecBlockKinds[1:] {
		for _, fy := range vecBlockKinds {
			x, _ := vecBlockTestOperand(b, fx, v1)
			y, _ := vecBlockTestOperand(b, fy, v2)
			name := fx.String() + "x"
			if fy == 0 {
				name += "vecf32"
			} else {
				name += fy.String()
			}
			for _, m := range []struct {
				name string
				fn   func(x, y *VecBlockOperand) (float64, error)
			}{
				{"dot", VecBlockDot}, {"l2sq", VecBlockL2DistanceSq}, {"l1", VecBlockL1Distance}, {"cosine", VecBlockCosineDistance},
			} {
				b.Run(name+"/"+m.name, func(b *testing.B) {
					for i := 0; i < b.N; i++ {
						_, _ = m.fn(x, y)
					}
				})
			}
		}
	}
}

// vecBlockOverflowPair returns x = [M, M, ...] and y = [M, -M, M, -M, ...] with M near the
// float32 maximum: each product overflows float32, lane 0 sums to +Inf and lane 1 to -Inf.
func vecBlockOverflowPair(dim int) ([]float32, []float32) {
	const m = 3e38
	x, y := make([]float32, dim), make([]float32, dim)
	for i := range x {
		x[i] = m
		y[i] = m
		if i%2 == 1 {
			y[i] = -m
		}
	}
	return x, y
}

func TestVecBlockOverflowNaNMapsToPosInf(t *testing.T) {
	xv, yv := vecBlockOverflowPair(32)
	for _, fx := range vecBlockKinds[1:] {
		for _, fy := range vecBlockKinds {
			x, _ := vecBlockTestOperand(t, fx, xv)
			y, _ := vecBlockTestOperand(t, fy, yv)
			msg := fx.String() + " x " + fy.String()
			dot, err := VecBlockDot(x, y)
			require.NoError(t, err)
			require.True(t, math.IsNaN(dot), msg)

			d, err := VecBlockInnerProduct(x, y)
			require.NoError(t, err)
			require.True(t, math.IsInf(d, 1), msg)
			// cosine recomputes in float64
			d, err = VecBlockCosineDistance(x, y)
			require.NoError(t, err)
			require.True(t, d >= 0 && d <= 2, msg)

			// L2 and L1 are sums of non-negative terms: never NaN
			for _, fn := range []func(x, y *VecBlockOperand) (float64, error){VecBlockL2DistanceSq, VecBlockL1Distance} {
				d, err := fn(x, y)
				require.NoError(t, err)
				require.False(t, math.IsNaN(d), msg)
			}
		}
	}
	// finite results are unchanged
	x, _ := vecBlockTestOperand(t, types.BlockScaledMXFP8, []float32{1, 2, 3, 4})
	y, _ := vecBlockTestOperand(t, types.BlockScaledMXFP8, []float32{1, 0, 0, 1})
	d, err := VecBlockInnerProduct(x, y)
	require.NoError(t, err)
	require.Equal(t, -5.0, d)
	require.Equal(t, 1.0, vecBlockNaNToPosInf(1))
	require.True(t, math.IsInf(vecBlockNaNToPosInf(math.Inf(-1)), -1))
}

// TestVecBlockSelfDistance checks that a stored vector is at distance 0 from itself, with
// decoded values near the float32 limit and at several magnitudes: each decoded element is
// rounded once, as stored, on targets that fuse multiply-add (arm64) as on the others.
func TestVecBlockSelfDistance(t *testing.T) {
	r := rand.New(rand.NewSource(29554))
	vectors := [][]float32{
		{9.9999994e29, 9.9999994e29, 9.9999994e29, 9.9999994e29, 9.9999994e29, 9.9999994e29, 9.9999994e29, 9.9999994e29,
			9.9999994e29, 9.9999994e29, 9.9999994e29, 9.9999994e29, 9.9999994e29, 9.9999994e29, 9.9999994e29, 9.9999994e29},
	}
	for _, scale := range []float64{1e-30, 1e-3, 1, 1e3, 1e20} {
		for _, dim := range []int{16, 17, 70} {
			vectors = append(vectors, vecBlockRandom(r, dim, scale))
		}
	}
	for _, f := range []types.BlockScaledFormat{types.BlockScaledMXFP8, types.BlockScaledNVFP4} {
		for i, v := range vectors {
			x, _ := vecBlockTestOperand(t, f, v)
			y, _ := vecBlockTestOperand(t, f, v)
			l2, err := VecBlockL2DistanceSq(x, y)
			require.NoError(t, err)
			require.Equal(t, float64(0), l2, "%s vector %d", f, i)
			l1, err := VecBlockL1Distance(x, y)
			require.NoError(t, err)
			require.Equal(t, float64(0), l1, "%s vector %d", f, i)
		}
	}
}

// TestVecBlockCosineFloat32Range checks cosine and L2 when a unit's float32 sums leave the
// float32 range: the result does not depend on whether elements fall in a unit or the tail.
func TestVecBlockCosineFloat32Range(t *testing.T) {
	fill := func(dim int, lead, rest float32) []float32 {
		v := make([]float32, dim)
		for i := range v {
			v[i] = rest
		}
		v[0] = lead
		return v
	}
	for _, dim := range []int{15, 16, 33} {
		for _, f := range vecBlockKinds[1:] {
			for _, c := range []struct {
				name string
				x, y []float32
			}{
				{"overflow", fill(dim, 2e19, 0), fill(dim, 1, 0)},
				{"underflow", fill(dim, 1e-25, 1e-25), fill(dim, 2e-25, 2e-25)},
			} {
				msg := fmt.Sprintf("%s %s dim %d", c.name, f, dim)
				x, _ := vecBlockTestOperand(t, f, c.x)
				y, _ := vecBlockTestOperand(t, f, c.y)
				d, err := VecBlockCosineDistance(x, y)
				require.NoError(t, err, msg)
				require.InDelta(t, 0, d, 1e-6, msg)
				s, err := VecBlockCosineSimilarity(x, y)
				require.NoError(t, err, msg)
				require.InDelta(t, 1, s, 1e-6, msg)
			}
			x, _ := vecBlockTestOperand(t, f, fill(dim, 2e19, 0))
			y, _ := vecBlockTestOperand(t, f, fill(dim, 0, 0))
			sq, err := VecBlockL2DistanceSq(x, y)
			require.NoError(t, err)
			require.True(t, math.IsInf(sq, 1), "l2sq %s dim %d", f, dim)
		}
	}
}
