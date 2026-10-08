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
	"math"
	"math/rand"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// A CPU-side model of the GPU (cuVS) block-scaled arithmetic, ported from the kernel source
// cgo/cuvs/blockscaled_matmul.hpp (bsmm_elem / bsmm_row_stats_kernel / bsmm_fixup_kernel). It runs
// with no GPU: the design doc claims a cell "decodes identically on the CPU and the tensor cores",
// and this oracle makes that claim executable so the CPU decode can be differential-tested against
// the GPU contract on ordinary CI -- the review method that caught the subnormal NVFP4 miss (#29554 /
// #20567). CUDA hardware is not run here; the oracle is derived from the same source the kernel is
// built from. A separate GPU-box check that this oracle matches the real kernel at runtime is a
// follow-up; this closes the gap of asserting the claim with no oracle behind it.

// gpuElemPreGlobal is bsmm_elem: element k of a cell BEFORE the row's global scale -- element * block
// scale in float64 for NVFP4/MXFP8. The block scale is never folded with the global in float32, so it
// cannot underflow a representable element to zero (the bug the CPU had).
func gpuElemPreGlobal(f8, e8 *[256]float32, f4 *[256][2]float32, c *types.BlockScaledCell, k int) float64 {
	if c.Format == types.BlockScaledMXFP8 {
		return float64(f8[c.Elems[k]]) * float64(e8[c.Scales[k/32]])
	}
	return float64(f4[c.Elems[k/2]][k%2]) * float64(f8[c.Scales[k/16]])
}

// gpuRawDot is the matmul output d[i]: sum over k of the pre-global products, in float64.
func gpuRawDot(x, y *types.BlockScaledCell) float64 {
	f8, e8, f4 := types.BlockScaledTables()
	var acc float64
	for k := 0; k < x.Dim; k++ {
		acc += gpuElemPreGlobal(f8, e8, f4, x, k) * gpuElemPreGlobal(f8, e8, f4, y, k)
	}
	return acc
}

// gpuNorm is bsmm_row_stats_kernel: norm[r] = (sum of pre-global squares) * global^2, in float64.
func gpuNorm(c *types.BlockScaledCell) float64 {
	g := float64(c.Global)
	return gpuRawDot(c, c) * g * g
}

// gpuCosineDistance is bsmm_fixup_kernel for the cosine metric: dot = rawdot * g_x * g_y;
// den = sqrt(norm_x * norm_y); distance = 1 - clamp(dot/den, -1, 1), and 1 when den is not positive
// (a zero vector). So the vector is zero IFF its norm is zero -- the global or every pre-global
// element is zero -- never because element*blockScale underflowed in float32.
func gpuCosineDistance(x, y *types.BlockScaledCell) float64 {
	den := math.Sqrt(gpuNorm(x)) * math.Sqrt(gpuNorm(y))
	if !(den > 0) {
		return 1
	}
	dot := gpuRawDot(x, y) * float64(x.Global) * float64(y.Global)
	return 1 - math.Max(-1, math.Min(1, dot/den))
}

// gpuZeroVector reports whether the GPU treats the cell as the zero vector (norm == 0).
func gpuZeroVector(c *types.BlockScaledCell) bool {
	return !(gpuNorm(c) > 0)
}

// vecBlockBoundaryFills are per-component magnitudes that drive the encoder's global and block
// scales across the whole representable range -- in particular the subnormal region, where a
// float32 global*blockScale underflows. These are the scale-space boundaries, not just element
// values: the NVFP4 miss lived at a subnormal global, which no ordinary fixture reached.
var vecBlockBoundaryFills = []float32{
	0,
	math.SmallestNonzeroFloat32, // 2^-149, smallest subnormal -- the #29554 witness
	0x1p-140,
	0x1p-126, // smallest normal
	0x1p-120,
	0x1p-60,
	1,
	1.5,
	0x1p60,
	0x1p120,
	math.MaxFloat32,
}

// TestVecBlockCPUMatchesGPUOracle29554 makes the "decodes identically on CPU and tensor cores" claim
// executable: over the scale-space boundary matrix (dims x fill magnitudes x same/mixed patterns x
// NVFP4/MXFP8), the CPU self cosine distance must agree with the GPU oracle's zero/nonzero
// classification (1 for a zero vector, 0 for a nonzero one), and every decoded element must be
// finite. The subnormal-fill NVFP4 rows are exactly the ones that returned 1 on the CPU while the
// GPU returned 0 before the fix.
func TestVecBlockCPUMatchesGPUOracle29554(t *testing.T) {
	dims := []int{1, 15, 16, 17, 31, 32, 33}
	formats := []types.BlockScaledFormat{types.BlockScaledNVFP4, types.BlockScaledMXFP8}
	r := rand.New(rand.NewSource(20567))

	build := func(t *testing.T, f types.BlockScaledFormat, v []float32) (*VecBlockOperand, bool) {
		cell, err := types.AppendBlockScaled(nil, f, v)
		if err != nil {
			// Some extreme inputs (e.g. a value whose block decodes to +Inf) are rejected at encode;
			// that is a separate validated contract, not what this test covers.
			t.Logf("skip %s %v: %v", f, v[:min(len(v), 3)], err)
			return nil, false
		}
		c, err := types.ParseBlockScaledCell(cell)
		require.NoError(t, err)
		return &VecBlockOperand{Cell: c}, true
	}

	assertSelf := func(t *testing.T, op *VecBlockOperand) {
		for i := 0; i < op.Cell.Dim; i++ {
			require.Falsef(t, math.IsInf(float64(op.Cell.At(i)), 0) || math.IsNaN(float64(op.Cell.At(i))),
				"At(%d)=%v must be finite", i, op.Cell.At(i))
		}
		d, err := VecBlockCosineDistance(op, op)
		require.NoError(t, err)
		// The zero/nonzero classification is the hard contract: the zero-vector path returns exactly
		// 1, and a nonzero self cosine is 1 - dot/sqrt(nx*ny) ~ 0 (never 1), so d==1 iff the CPU
		// treats the cell as zero. That must match the GPU oracle -- a nonzero-per-GPU cell returning
		// 1 here is the #29554 miss. The nonzero distance is ~0 but not exactly 0 (sqrt rounding).
		oracleZero := gpuZeroVector(&op.Cell)
		require.Equalf(t, oracleZero, d == 1, "zero-vector classification must match the GPU oracle (CPU self distance=%v)", d)
		if !oracleZero {
			require.InDeltaf(t, 0, d, 1e-5, "nonzero vector self cosine distance must be ~0, got %v", d)
		}
	}

	for _, f := range formats {
		for _, dim := range dims {
			// Uniform fills: each boundary magnitude across the whole vector.
			for _, fill := range vecBlockBoundaryFills {
				v := make([]float32, dim)
				for i := range v {
					v[i] = fill
				}
				if op, ok := build(t, f, v); ok {
					assertSelf(t, op)
				}
			}
			// Mixed fills: one large component with the rest subnormal, and random wide-exponent
			// vectors -- exercises per-block scale variation and partial underflow.
			mixed := make([]float32, dim)
			for i := range mixed {
				mixed[i] = math.SmallestNonzeroFloat32
			}
			mixed[0] = 1
			if op, ok := build(t, f, mixed); ok {
				assertSelf(t, op)
			}
			for trial := 0; trial < 8; trial++ {
				v := make([]float32, dim)
				for i := range v {
					// exponents from deep subnormal to large, random sign.
					e := r.Intn(210) - 149
					m := r.Float64()*2 - 1
					v[i] = float32(math.Ldexp(m, e))
				}
				if op, ok := build(t, f, v); ok {
					assertSelf(t, op)
				}
			}
		}
	}
}

// TestVecBlockCPUMatchesGPUOracleCrossPair29554 checks the general (x != y) cosine distance against
// the oracle within float32 tolerance -- the CPU kernels accumulate in float32 with a float64
// fallback, the oracle in float64, so ordinary pairs must agree to a few ULPs of the [0,2] range,
// and the zero/nonzero classification must be exact.
func TestVecBlockCPUMatchesGPUOracleCrossPair29554(t *testing.T) {
	r := rand.New(rand.NewSource(27453))
	for _, f := range []types.BlockScaledFormat{types.BlockScaledNVFP4, types.BlockScaledMXFP8} {
		for _, dim := range []int{1, 16, 17, 48} {
			for trial := 0; trial < 64; trial++ {
				mk := func() (*VecBlockOperand, bool) {
					v := make([]float32, dim)
					for i := range v {
						v[i] = float32(math.Ldexp(r.Float64()*2-1, r.Intn(60)-30))
					}
					cell, err := types.AppendBlockScaled(nil, f, v)
					if err != nil {
						return nil, false
					}
					c, err := types.ParseBlockScaledCell(cell)
					require.NoError(t, err)
					return &VecBlockOperand{Cell: c}, true
				}
				x, ok1 := mk()
				y, ok2 := mk()
				if !ok1 || !ok2 {
					continue
				}
				got, err := VecBlockCosineDistance(x, y)
				require.NoError(t, err)
				want := gpuCosineDistance(&x.Cell, &y.Cell)
				require.Equalf(t, want == 1, got == 1, "zero/nonzero classification must match the oracle (got=%v want=%v)", got, want)
				if want != 1 {
					require.InDeltaf(t, want, got, 1e-5, "cosine distance must match the GPU oracle within float32 tolerance")
				}
			}
		}
	}
}
