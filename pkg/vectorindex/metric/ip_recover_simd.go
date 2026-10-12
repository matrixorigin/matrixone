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

import "github.com/matrixorigin/matrixone/pkg/container/types"

// Exceptional recompute for the SIMD inner-product / spherical kernels, mirroring cosineRecomputeF64:
// the ordinary fast path is the SIMD kernel; only when its result is NON-FINITE do we recompute via
// the source-order reference (InnerProductUnrolled / SphericalDistanceUnrolled, which sum in T's own
// type, and the narrow innerProductBF16 / innerProductF16). A parallel lane sum diverges from that
// reference only when a strided lane overflows to +/-Inf before the cross-lane reduction cancels it,
// so the reference recovers the finite answer (2^63 products are 2^126, fit float32, cancel to 0). A
// GENUINE overflow -- 1e20 products are 1e40, which overflow float32 in the lanes AND in the reference
// -- stays non-finite. Each reference applies nanToPosInf itself (so its own 8-wide block cancellation
// never leaks a NaN either), keeping the result well-ordered for the serve boundary to reject. The fast
// path pays only one finiteness test; the recompute runs only on the rare non-finite result. This file
// is SIMD-tagged because only the SIMD kernels need recovery.

// isFinite reports whether v is neither NaN nor +/-Inf (x-x is 0 iff x is finite).
func isFinite[T types.RealNumbers](v T) bool { return v-v == 0 }

func recoverInnerProduct[T types.RealNumbers](res T, p, q []T) (T, error) {
	if isFinite(res) {
		return res, nil
	}
	return InnerProductUnrolled(p, q)
}

func recoverInnerProductBF16(res float64, a, b []types.BF16) (float64, error) {
	if isFinite(res) {
		return res, nil
	}
	return innerProductBF16(a, b)
}

func recoverInnerProductF16(res float64, a, b []types.Float16) (float64, error) {
	if isFinite(res) {
		return res, nil
	}
	return innerProductF16(a, b)
}

// recoverCosineBF16/F16 recover narrow cosine the same way: the SIMD lanes sum dot/norms in separated
// float32 accumulators, so a bf16 run whose products are each finite (e.g. +/-2^127) can overflow a
// lane to +/-Inf before the reduction cancels, mapping to +Inf via cosineDistClamped. On a non-finite
// result, recompute via the scalar reference (in source order, with its own f64 norm recompute), which
// recovers the finite cosine. f16 cannot overflow float32 so its recover is a no-op guard.
func recoverCosineBF16(res float64, a, b []types.BF16) (float64, error) {
	if isFinite(res) {
		return res, nil
	}
	return cosineDistanceBF16(a, b)
}

func recoverCosineF16(res float64, a, b []types.Float16) (float64, error) {
	if isFinite(res) {
		return res, nil
	}
	return cosineDistanceF16(a, b)
}

func recoverSpherical[T types.RealNumbers](res T, p, q []T) (T, error) {
	if isFinite(res) {
		return res, nil
	}
	return SphericalDistanceUnrolled(p, q)
}
