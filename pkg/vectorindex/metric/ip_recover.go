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

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// Exceptional recompute for inner product / spherical, mirroring cosineRecomputeF64: the
// ordinary fast path is a SIMD kernel; only when its result is NON-FINITE do we recompute the
// dot in float64 in SOURCE ORDER. A parallel SIMD lane sum diverges from the in-order reference
// only when a strided lane overflows to +/-Inf before the cross-lane reduction cancels it, so the
// in-order sum recovers the reference's finite answer. When even the in-order f64 result leaves
// the element domain the overflow is GENUINE (not a lane-ordering artifact): we fast-fail to +Inf
// -- a well-ordered distance the serve boundary rejects -- exactly like cosineRecomputeF64's
// ok=false path. The fast path pays only one finiteness test; the recompute runs only on the rare
// non-finite result, which finite stored vectors never produce.

// isFinite reports whether v is neither NaN nor +/-Inf. x-x is 0 for every finite x and NaN
// otherwise (the CheckFiniteDist idiom).
func isFinite[T types.RealNumbers](v T) bool { return v-v == 0 }

// recoverInnerProduct returns the SIMD inner-product distance when finite, else the in-order
// float64 reference cast to T, else +Inf (genuine overflow).
func recoverInnerProduct[T types.RealNumbers](res T, p, q []T) T {
	if isFinite(res) {
		return res
	}
	var dot float64
	for i := range p {
		dot += float64(p[i]) * float64(q[i])
	}
	if r := T(-dot); isFinite(r) {
		return r
	}
	return T(math.Inf(1))
}

// recoverInnerProductBF16 is recoverInnerProduct for the bf16 narrow kernels (result in float64,
// decoded via BF16.ToFloat32 exactly as the kernels accumulate).
func recoverInnerProductBF16(res float64, a, b []types.BF16) float64 {
	if isFinite(res) {
		return res
	}
	var dot float64
	for i := range a {
		dot += float64(a[i].ToFloat32()) * float64(b[i].ToFloat32())
	}
	if r := -dot; isFinite(r) {
		return r
	}
	return math.Inf(1)
}

// recoverInnerProductF16 is recoverInnerProduct for the f16 narrow kernels (decoded via f16fast,
// the same magic-multiply the kernels use).
func recoverInnerProductF16(res float64, a, b []types.Float16) float64 {
	if isFinite(res) {
		return res
	}
	var dot float64
	for i := range a {
		dot += float64(f16fast(a[i])) * float64(f16fast(b[i]))
	}
	if r := -dot; isFinite(r) {
		return r
	}
	return math.Inf(1)
}

// recoverSpherical returns the SIMD spherical distance when finite, else recomputes the dot in
// float64 in source order and re-derives acos(clamp(dot))/pi; a non-finite in-order dot is genuine
// overflow and fast-fails to +Inf.
func recoverSpherical[T types.RealNumbers](res T, p, q []T) T {
	if isFinite(res) {
		return res
	}
	var dot float64
	for i := range p {
		dot += float64(p[i]) * float64(q[i])
	}
	if !isFinite(dot) {
		return T(math.Inf(1))
	}
	if dot > 1.0 {
		dot = 1.0
	} else if dot < -1.0 {
		dot = -1.0
	}
	return T(math.Acos(dot) / math.Pi)
}
