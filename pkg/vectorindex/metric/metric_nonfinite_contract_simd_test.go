//go:build (amd64 || arm64) && go1.27 && goexperiment.simd

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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// CONTRACT (SIMD specific): the SIMD kernels accumulate signed products in several
// float lanes and reduce at the end, so an input whose products are each finite but
// whose per-lane partial sums overflow to +/-Inf before the reduction (here
// a[i]*b[i] is +/-mag^2 with adjacent pairs cancelling) yields NaN -- returned with
// a nil error. The sequential scalar build cancels in source order and never hits
// this, which is why it is SIMD-tagged.
//
// Returning the NaN is correct, not a bug to paper over in the kernel: the lanes are
// what make the kernel fast, the input cannot occur with stored data (an indexed
// vector never holds such magnitudes, #28688), and NaN faithfully reports that the
// accumulation overflowed. The caller screens it ONCE at the consumer boundary
// (index Search via CheckFiniteDists; moarray and the SQL array-distance builtins
// via CheckFiniteDist) -- never with a per-distance branch inside the search loop
// (#29496). See metric_nonfinite_contract_test.go for the full rationale.
func TestMetricReturnsNaNOnLaneCancellation29496(t *testing.T) {
	const n = 128
	t.Run("f32", func(t *testing.T) {
		a, b := make([]float32, n), make([]float32, n)
		mag := float32(1 << 63)
		for i := range a {
			a[i] = mag
			if i%2 == 0 {
				b[i] = mag
			} else {
				b[i] = -mag
			}
		}
		d, err := InnerProduct[float32](a, b)
		require.NoError(t, err, "metric must not reject a NaN result")
		require.Truef(t, math.IsNaN(float64(d)), "lane cancellation must yield NaN, got %v", d)
	})
	t.Run("bf16_via_resolve", func(t *testing.T) {
		a, b := make([]types.BF16, n), make([]types.BF16, n)
		mag := float32(1 << 63)
		for i := range a {
			a[i] = types.BF16FromFloat32(mag)
			if i%2 == 0 {
				b[i] = types.BF16FromFloat32(mag)
			} else {
				b[i] = types.BF16FromFloat32(-mag)
			}
		}
		fn, err := ResolveDistanceFn[types.BF16, float64](Metric_InnerProduct)
		require.NoError(t, err)
		d, err := fn(a, b)
		require.NoError(t, err, "ResolveDistanceFn must not reject a NaN result")
		require.Truef(t, math.IsNaN(d), "lane cancellation must yield NaN, got %v", d)
	})
}
