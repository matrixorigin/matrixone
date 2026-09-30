//go:build (amd64 || arm64) && go1.27 && goexperiment.simd

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

// Fixtures shared by the per-architecture narrow-kernel equivalence tests
// (distance_func_narrow_amd64_test.go, distance_func_narrow_arm64_test.go).
//
// Tagged to match its only consumers — the amd64/arm64 SIMD test files, both
// //go:build ...goexperiment.simd — so the shared dim list and generators are not
// duplicated across them, yet are excluded from non-SIMD builds where neither
// consumer compiles (otherwise these fixtures read as unused).

package metric

import (
	"math"
	"math/rand"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// narrowSIMDDims exercises each architecture's main loop plus every tail
// remainder, including odd final elements.
//
// The per-iteration element counts differ by arch — amd64 consumes 32 bf16/f16
// and 64 int8 per iteration at 16 lanes, arm64 consumes 8 and 16 at 4 lanes — so
// the list has to straddle the boundaries of both: powers of two from 2 to 64
// with n-1 / n / n+1 around each, plus a large non-multiple tail.
var narrowSIMDDims = []int{1, 2, 3, 4, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 1000, 1024, 1025}

func randF32(dim int, r *rand.Rand) []float32 {
	f := make([]float32, dim)
	for i := range f {
		f[i] = float32(r.Float64()*16 - 8) // [-8, 8)
	}
	return f
}
func randBF16(dim int, r *rand.Rand) []types.BF16 { return types.Float32ToBF16Slice(randF32(dim, r)) }
func randF16(dim int, r *rand.Rand) []types.Float16 {
	return types.Float32ToFloat16Slice(randF32(dim, r))
}
func randI8(dim int, r *rand.Rand) []int8 {
	v := make([]int8, dim)
	for i := range v {
		v[i] = int8(r.Intn(255) - 127)
	}
	return v
}
func randU8(dim int, r *rand.Rand) []uint8 {
	v := make([]uint8, dim)
	for i := range v {
		v[i] = uint8(r.Intn(256))
	}
	return v
}

// checkPair asserts a SIMD kernel matches its scalar oracle. exact=true requires
// bit-equality (integer int8/uint8 L2sq/IP/L1); otherwise a magnitude-scaled
// tolerance (float reductions reorder).
func checkPair(t *testing.T, name string, dim int, got, want float64, exact bool) {
	t.Helper()
	if exact {
		require.Equal(t, want, got, "%s dim=%d", name, dim)
		return
	}
	require.InDelta(t, want, got, 1e-4*(1+math.Abs(want)), "%s dim=%d", name, dim)
}
