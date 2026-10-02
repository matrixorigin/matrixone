//go:build arm64 && go1.27 && goexperiment.simd

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

// ARM64 NEON counterpart of distance_func_amd64.go. Same exported surface,
// same semantics (including the error strings, the zero-denominator returns
// and the clamps) — only the vector width differs.
//
// NEON is 128-bit, so a Float32x4 holds 4 lanes against AVX-512's 16. To keep
// the FMLA dependency chains from serializing we run 4 independent
// accumulators, giving a 16-element block for float32 and an 8-element block
// for float64. Everything below the block size falls through to the same
// unrolled scalar tails the amd64 file uses.

package metric

import (
	"math"
	"os"
	"simd/archsimd"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// hasNeon gates the SIMD kernels. NEON is mandatory in ARMv8, so unlike the
// amd64 AVX2/AVX-512 tiers there is no CPU probe to make here. The var exists
// solely as a TESTING-ONLY override: set MO_METRIC_NO_NEON=1 to force the
// scalar tails, so they get real coverage on this hardware. Package-level var
// initializers run before every init() selector, so the override is seen by
// all kernel-selection init() funcs in this package.
var hasNeon = os.Getenv("MO_METRIC_NO_NEON") == ""

// Reduction helpers — store and tree sum, mirroring sumF32x16/sumF64x8.
func sumF32x4(v archsimd.Float32x4) float32 {
	var a [4]float32
	v.StoreArray(&a)
	return (a[0] + a[1]) + (a[2] + a[3])
}

func sumF64x2(v archsimd.Float64x2) float64 {
	var a [2]float64
	v.StoreArray(&a)
	return a[0] + a[1]
}

// L2 Distance Squared kernels
func L2DistanceSqFloat32(a, b []float32) (float32, error) {
	n := len(a)
	if n != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}

	var sum float32
	i := 0

	if hasNeon && n >= 32 {
		// 8 independent FMLA chains (32 f32/iter). Measured ~+25% over 4 accumulators
		// on Apple M2 Max at dim 768/1536 (55-57 vs ~44 GFLOP/s): wide NEON cores
		// (4 FP pipes) need more in-flight accumulators than the AVX-512 (4-acc) shape
		// to hide FMLA latency. 8 saturates; 16 spills and regresses.
		acc0, acc1, acc2, acc3 := archsimd.Float32x4{}, archsimd.Float32x4{}, archsimd.Float32x4{}, archsimd.Float32x4{}
		acc4, acc5, acc6, acc7 := archsimd.Float32x4{}, archsimd.Float32x4{}, archsimd.Float32x4{}, archsimd.Float32x4{}
		for i <= n-32 {
			as, bs := a[i:i+32:i+32], b[i:i+32:i+32]
			d0 := archsimd.LoadFloat32x4(as[0:4]).Sub(archsimd.LoadFloat32x4(bs[0:4]))
			d1 := archsimd.LoadFloat32x4(as[4:8]).Sub(archsimd.LoadFloat32x4(bs[4:8]))
			d2 := archsimd.LoadFloat32x4(as[8:12]).Sub(archsimd.LoadFloat32x4(bs[8:12]))
			d3 := archsimd.LoadFloat32x4(as[12:16]).Sub(archsimd.LoadFloat32x4(bs[12:16]))
			d4 := archsimd.LoadFloat32x4(as[16:20]).Sub(archsimd.LoadFloat32x4(bs[16:20]))
			d5 := archsimd.LoadFloat32x4(as[20:24]).Sub(archsimd.LoadFloat32x4(bs[20:24]))
			d6 := archsimd.LoadFloat32x4(as[24:28]).Sub(archsimd.LoadFloat32x4(bs[24:28]))
			d7 := archsimd.LoadFloat32x4(as[28:32]).Sub(archsimd.LoadFloat32x4(bs[28:32]))
			acc0 = d0.MulAdd(d0, acc0)
			acc1 = d1.MulAdd(d1, acc1)
			acc2 = d2.MulAdd(d2, acc2)
			acc3 = d3.MulAdd(d3, acc3)
			acc4 = d4.MulAdd(d4, acc4)
			acc5 = d5.MulAdd(d5, acc5)
			acc6 = d6.MulAdd(d6, acc6)
			acc7 = d7.MulAdd(d7, acc7)
			i += 32
		}
		sum += sumF32x4(acc0.Add(acc1).Add(acc2.Add(acc3)).Add(acc4.Add(acc5).Add(acc6.Add(acc7))))
	}

	// 16-block SIMD cleanup: keeps SIMD for the [16,32) remainder and for 16<=n<32.
	if hasNeon && i <= n-16 {
		acc0, acc1, acc2, acc3 := archsimd.Float32x4{}, archsimd.Float32x4{}, archsimd.Float32x4{}, archsimd.Float32x4{}
		for i <= n-16 {
			as, bs := a[i:i+16:i+16], b[i:i+16:i+16]
			d0 := archsimd.LoadFloat32x4(as[0:4]).Sub(archsimd.LoadFloat32x4(bs[0:4]))
			d1 := archsimd.LoadFloat32x4(as[4:8]).Sub(archsimd.LoadFloat32x4(bs[4:8]))
			d2 := archsimd.LoadFloat32x4(as[8:12]).Sub(archsimd.LoadFloat32x4(bs[8:12]))
			d3 := archsimd.LoadFloat32x4(as[12:16]).Sub(archsimd.LoadFloat32x4(bs[12:16]))
			acc0 = d0.MulAdd(d0, acc0)
			acc1 = d1.MulAdd(d1, acc1)
			acc2 = d2.MulAdd(d2, acc2)
			acc3 = d3.MulAdd(d3, acc3)
			i += 16
		}
		sum += sumF32x4(acc0.Add(acc1).Add(acc2.Add(acc3)))
	}

	for i <= n-8 {
		// BCE Hint
		as := a[i : i+8 : i+8]
		bs := b[i : i+8 : i+8]
		d0 := as[0] - bs[0]
		d1 := as[1] - bs[1]
		d2 := as[2] - bs[2]
		d3 := as[3] - bs[3]
		d4 := as[4] - bs[4]
		d5 := as[5] - bs[5]
		d6 := as[6] - bs[6]
		d7 := as[7] - bs[7]
		sum += (d0*d0 + d1*d1) + (d2*d2 + d3*d3) + (d4*d4 + d5*d5) + (d6*d6 + d7*d7)
		i += 8
	}

	for ; i < n; i++ {
		diff := a[i] - b[i]
		sum += diff * diff
	}
	return sum, nil
}

func InnerProductFloat32(a, b []float32) (float32, error) {
	n := len(a)
	if n != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}

	var total float32
	i := 0

	if hasNeon && n >= 32 {
		// 8 FMLA chains (32 f32/iter): ~+25% over 4 accumulators on wide NEON cores.
		acc0, acc1, acc2, acc3 := archsimd.Float32x4{}, archsimd.Float32x4{}, archsimd.Float32x4{}, archsimd.Float32x4{}
		acc4, acc5, acc6, acc7 := archsimd.Float32x4{}, archsimd.Float32x4{}, archsimd.Float32x4{}, archsimd.Float32x4{}
		for i <= n-32 {
			as, bs := a[i:i+32:i+32], b[i:i+32:i+32]
			acc0 = archsimd.LoadFloat32x4(as[0:4]).MulAdd(archsimd.LoadFloat32x4(bs[0:4]), acc0)
			acc1 = archsimd.LoadFloat32x4(as[4:8]).MulAdd(archsimd.LoadFloat32x4(bs[4:8]), acc1)
			acc2 = archsimd.LoadFloat32x4(as[8:12]).MulAdd(archsimd.LoadFloat32x4(bs[8:12]), acc2)
			acc3 = archsimd.LoadFloat32x4(as[12:16]).MulAdd(archsimd.LoadFloat32x4(bs[12:16]), acc3)
			acc4 = archsimd.LoadFloat32x4(as[16:20]).MulAdd(archsimd.LoadFloat32x4(bs[16:20]), acc4)
			acc5 = archsimd.LoadFloat32x4(as[20:24]).MulAdd(archsimd.LoadFloat32x4(bs[20:24]), acc5)
			acc6 = archsimd.LoadFloat32x4(as[24:28]).MulAdd(archsimd.LoadFloat32x4(bs[24:28]), acc6)
			acc7 = archsimd.LoadFloat32x4(as[28:32]).MulAdd(archsimd.LoadFloat32x4(bs[28:32]), acc7)
			i += 32
		}
		total += sumF32x4(acc0.Add(acc1).Add(acc2.Add(acc3)).Add(acc4.Add(acc5).Add(acc6.Add(acc7))))
	}
	if hasNeon && i <= n-16 {
		acc0, acc1, acc2, acc3 := archsimd.Float32x4{}, archsimd.Float32x4{}, archsimd.Float32x4{}, archsimd.Float32x4{}
		for i <= n-16 {
			as, bs := a[i:i+16:i+16], b[i:i+16:i+16]
			acc0 = archsimd.LoadFloat32x4(as[0:4]).MulAdd(archsimd.LoadFloat32x4(bs[0:4]), acc0)
			acc1 = archsimd.LoadFloat32x4(as[4:8]).MulAdd(archsimd.LoadFloat32x4(bs[4:8]), acc1)
			acc2 = archsimd.LoadFloat32x4(as[8:12]).MulAdd(archsimd.LoadFloat32x4(bs[8:12]), acc2)
			acc3 = archsimd.LoadFloat32x4(as[12:16]).MulAdd(archsimd.LoadFloat32x4(bs[12:16]), acc3)
			i += 16
		}
		total += sumF32x4(acc0.Add(acc1).Add(acc2.Add(acc3)))
	}

	for i <= n-8 {
		// BCE Hint
		as := a[i : i+8 : i+8]
		bs := b[i : i+8 : i+8]
		total += as[0]*bs[0] + as[1]*bs[1] + as[2]*bs[2] + as[3]*bs[3] +
			as[4]*bs[4] + as[5]*bs[5] + as[6]*bs[6] + as[7]*bs[7]
		i += 8
	}

	for ; i < n; i++ {
		total += a[i] * b[i]
	}
	return -total, nil
}

func L2Distance[T types.RealNumbers](v1, v2 []T) (T, error) {
	if pf32, ok := any(v1).([]float32); ok {
		sq, err := L2DistanceSqFloat32(pf32, any(v2).([]float32))
		if err != nil {
			return 0, err
		}
		d, err := L2FromSquared(sq)
		return T(d), err
	}
	if pf64, ok := any(v1).([]float64); ok {
		sq, err := L2DistanceSqFloat64(pf64, any(v2).([]float64))
		if err != nil {
			return 0, err
		}
		d, err := L2FromSquared(sq)
		return T(d), err
	}
	return 0, moerr.NewInternalErrorNoCtx("vector type not supported")
}

func L2DistanceSqFloat64(a, b []float64) (float64, error) {
	n := len(a)
	if n != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	var sum float64
	i := 0
	// Float64x2 is only 2 lanes, so the f64 kernels gain far less than the f32
	// ones: measured ~1.05x over the scalar tail at dim 1024, against 1.5-2.2x
	// for float32. Raising this to 8 accumulators (16 elems/iter) was measured
	// and made no difference, so the shape stays simple — at this width the
	// kernel is bandwidth-bound, not ILP-bound. It is kept because it is never
	// slower and keeps every element type on one code path.
	if hasNeon && n >= 8 {
		acc0, acc1, acc2, acc3 := archsimd.Float64x2{}, archsimd.Float64x2{}, archsimd.Float64x2{}, archsimd.Float64x2{}
		for i <= n-8 {
			as, bs := a[i:i+8:i+8], b[i:i+8:i+8]
			d0 := archsimd.LoadFloat64x2(as[0:2]).Sub(archsimd.LoadFloat64x2(bs[0:2]))
			d1 := archsimd.LoadFloat64x2(as[2:4]).Sub(archsimd.LoadFloat64x2(bs[2:4]))
			d2 := archsimd.LoadFloat64x2(as[4:6]).Sub(archsimd.LoadFloat64x2(bs[4:6]))
			d3 := archsimd.LoadFloat64x2(as[6:8]).Sub(archsimd.LoadFloat64x2(bs[6:8]))
			acc0 = d0.MulAdd(d0, acc0)
			acc1 = d1.MulAdd(d1, acc1)
			acc2 = d2.MulAdd(d2, acc2)
			acc3 = d3.MulAdd(d3, acc3)
			i += 8
		}
		sum += sumF64x2(acc0.Add(acc1).Add(acc2.Add(acc3)))
	}

	for i <= n-8 {
		// BCE Hint
		as := a[i : i+8 : i+8]
		bs := b[i : i+8 : i+8]
		d0 := as[0] - bs[0]
		d1 := as[1] - bs[1]
		d2 := as[2] - bs[2]
		d3 := as[3] - bs[3]
		d4 := as[4] - bs[4]
		d5 := as[5] - bs[5]
		d6 := as[6] - bs[6]
		d7 := as[7] - bs[7]
		sum += (d0*d0 + d1*d1) + (d2*d2 + d3*d3) + (d4*d4 + d5*d5) + (d6*d6 + d7*d7)
		i += 8
	}

	for ; i < n; i++ {
		diff := a[i] - b[i]
		sum += diff * diff
	}
	return sum, nil
}

func InnerProductFloat64(a, b []float64) (float64, error) {
	n := len(a)
	if n != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	var total float64
	i := 0
	// See L2DistanceSqFloat64 for why the f64 block stays at 4 accumulators.
	if hasNeon && n >= 8 {
		acc0, acc1, acc2, acc3 := archsimd.Float64x2{}, archsimd.Float64x2{}, archsimd.Float64x2{}, archsimd.Float64x2{}
		for i <= n-8 {
			as, bs := a[i:i+8:i+8], b[i:i+8:i+8]
			acc0 = archsimd.LoadFloat64x2(as[0:2]).MulAdd(archsimd.LoadFloat64x2(bs[0:2]), acc0)
			acc1 = archsimd.LoadFloat64x2(as[2:4]).MulAdd(archsimd.LoadFloat64x2(bs[2:4]), acc1)
			acc2 = archsimd.LoadFloat64x2(as[4:6]).MulAdd(archsimd.LoadFloat64x2(bs[4:6]), acc2)
			acc3 = archsimd.LoadFloat64x2(as[6:8]).MulAdd(archsimd.LoadFloat64x2(bs[6:8]), acc3)
			i += 8
		}
		total += sumF64x2(acc0.Add(acc1).Add(acc2.Add(acc3)))
	}

	for i <= n-8 {
		// BCE Hint
		as := a[i : i+8 : i+8]
		bs := b[i : i+8 : i+8]
		total += as[0]*bs[0] + as[1]*bs[1] + as[2]*bs[2] + as[3]*bs[3] +
			as[4]*bs[4] + as[5]*bs[5] + as[6]*bs[6] + as[7]*bs[7]
		i += 8
	}

	for ; i < n; i++ {
		total += a[i] * b[i]
	}
	return -total, nil
}

func L2DistanceSq[T types.RealNumbers](p, q []T) (T, error) {
	if pf32, ok := any(p).([]float32); ok {
		res, err := L2DistanceSqFloat32(pf32, any(q).([]float32))
		return T(res), err
	}
	if pf64, ok := any(p).([]float64); ok {
		res, err := L2DistanceSqFloat64(pf64, any(q).([]float64))
		return T(res), err
	}
	return 0, moerr.NewInternalErrorNoCtx("vector type not supported")
}

func InnerProduct[T types.RealNumbers](p, q []T) (T, error) {
	if pf32, ok := any(p).([]float32); ok {
		res, err := InnerProductFloat32(pf32, any(q).([]float32))
		if err != nil {
			return 0, err
		}
		return recoverInnerProduct(T(res), p, q), nil
	}
	if pf64, ok := any(p).([]float64); ok {
		res, err := InnerProductFloat64(pf64, any(q).([]float64))
		if err != nil {
			return 0, err
		}
		return recoverInnerProduct(T(res), p, q), nil
	}
	return 0, moerr.NewInternalErrorNoCtx("vector type not supported")
}

func L1DistanceFloat32(a, b []float32) (float32, error) {
	n := len(a)
	if n != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension mismatch")
	}
	var sum float32
	i := 0
	if hasNeon && n >= 32 {
		// NEON has a native FABS, so unlike the amd64 kernel there is no need
		// for the max(a-b, b-a) trick. 8 accumulators (32 f32/iter) to feed the
		// wide NEON pipes; ~+25% over 4 on Apple M-class cores.
		acc0, acc1, acc2, acc3 := archsimd.Float32x4{}, archsimd.Float32x4{}, archsimd.Float32x4{}, archsimd.Float32x4{}
		acc4, acc5, acc6, acc7 := archsimd.Float32x4{}, archsimd.Float32x4{}, archsimd.Float32x4{}, archsimd.Float32x4{}
		for i <= n-32 {
			as, bs := a[i:i+32:i+32], b[i:i+32:i+32]
			acc0 = acc0.Add(archsimd.LoadFloat32x4(as[0:4]).Sub(archsimd.LoadFloat32x4(bs[0:4])).Abs())
			acc1 = acc1.Add(archsimd.LoadFloat32x4(as[4:8]).Sub(archsimd.LoadFloat32x4(bs[4:8])).Abs())
			acc2 = acc2.Add(archsimd.LoadFloat32x4(as[8:12]).Sub(archsimd.LoadFloat32x4(bs[8:12])).Abs())
			acc3 = acc3.Add(archsimd.LoadFloat32x4(as[12:16]).Sub(archsimd.LoadFloat32x4(bs[12:16])).Abs())
			acc4 = acc4.Add(archsimd.LoadFloat32x4(as[16:20]).Sub(archsimd.LoadFloat32x4(bs[16:20])).Abs())
			acc5 = acc5.Add(archsimd.LoadFloat32x4(as[20:24]).Sub(archsimd.LoadFloat32x4(bs[20:24])).Abs())
			acc6 = acc6.Add(archsimd.LoadFloat32x4(as[24:28]).Sub(archsimd.LoadFloat32x4(bs[24:28])).Abs())
			acc7 = acc7.Add(archsimd.LoadFloat32x4(as[28:32]).Sub(archsimd.LoadFloat32x4(bs[28:32])).Abs())
			i += 32
		}
		sum += sumF32x4(acc0.Add(acc1).Add(acc2.Add(acc3)).Add(acc4.Add(acc5).Add(acc6.Add(acc7))))
	}
	if hasNeon && i <= n-16 {
		acc0, acc1, acc2, acc3 := archsimd.Float32x4{}, archsimd.Float32x4{}, archsimd.Float32x4{}, archsimd.Float32x4{}
		for i <= n-16 {
			as, bs := a[i:i+16:i+16], b[i:i+16:i+16]
			acc0 = acc0.Add(archsimd.LoadFloat32x4(as[0:4]).Sub(archsimd.LoadFloat32x4(bs[0:4])).Abs())
			acc1 = acc1.Add(archsimd.LoadFloat32x4(as[4:8]).Sub(archsimd.LoadFloat32x4(bs[4:8])).Abs())
			acc2 = acc2.Add(archsimd.LoadFloat32x4(as[8:12]).Sub(archsimd.LoadFloat32x4(bs[8:12])).Abs())
			acc3 = acc3.Add(archsimd.LoadFloat32x4(as[12:16]).Sub(archsimd.LoadFloat32x4(bs[12:16])).Abs())
			i += 16
		}
		sum += sumF32x4(acc0.Add(acc1).Add(acc2.Add(acc3)))
	}

	abs := func(x float32) float32 {
		return math.Float32frombits(math.Float32bits(x) &^ (1 << 31))
	}
	for i <= n-8 {
		// BCE Hint
		as := a[i : i+8 : i+8]
		bs := b[i : i+8 : i+8]
		sum += abs(as[0]-bs[0]) + abs(as[1]-bs[1]) + abs(as[2]-bs[2]) + abs(as[3]-bs[3]) +
			abs(as[4]-bs[4]) + abs(as[5]-bs[5]) + abs(as[6]-bs[6]) + abs(as[7]-bs[7])
		i += 8
	}

	for ; i < n; i++ {
		sum += abs(a[i] - b[i])
	}
	return sum, nil
}

func L1DistanceFloat64(a, b []float64) (float64, error) {
	n := len(a)
	if n != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension mismatch")
	}
	var sum float64
	i := 0
	if hasNeon && n >= 8 {
		acc0, acc1, acc2, acc3 := archsimd.Float64x2{}, archsimd.Float64x2{}, archsimd.Float64x2{}, archsimd.Float64x2{}
		for i <= n-8 {
			as, bs := a[i:i+8:i+8], b[i:i+8:i+8]
			acc0 = acc0.Add(archsimd.LoadFloat64x2(as[0:2]).Sub(archsimd.LoadFloat64x2(bs[0:2])).Abs())
			acc1 = acc1.Add(archsimd.LoadFloat64x2(as[2:4]).Sub(archsimd.LoadFloat64x2(bs[2:4])).Abs())
			acc2 = acc2.Add(archsimd.LoadFloat64x2(as[4:6]).Sub(archsimd.LoadFloat64x2(bs[4:6])).Abs())
			acc3 = acc3.Add(archsimd.LoadFloat64x2(as[6:8]).Sub(archsimd.LoadFloat64x2(bs[6:8])).Abs())
			i += 8
		}
		sum += sumF64x2(acc0.Add(acc1).Add(acc2.Add(acc3)))
	}

	abs := func(x float64) float64 {
		return math.Abs(x)
	}
	for i <= n-8 {
		// BCE Hint
		as := a[i : i+8 : i+8]
		bs := b[i : i+8 : i+8]
		sum += abs(as[0]-bs[0]) + abs(as[1]-bs[1]) + abs(as[2]-bs[2]) + abs(as[3]-bs[3]) +
			abs(as[4]-bs[4]) + abs(as[5]-bs[5]) + abs(as[6]-bs[6]) + abs(as[7]-bs[7])
		i += 8
	}

	for ; i < n; i++ {
		sum += abs(a[i] - b[i])
	}
	return sum, nil
}

func L1Distance[T types.RealNumbers](p, q []T) (T, error) {
	if pf32, ok := any(p).([]float32); ok {
		res, err := L1DistanceFloat32(pf32, any(q).([]float32))
		return T(res), err
	}
	if pf64, ok := any(p).([]float64); ok {
		res, err := L1DistanceFloat64(pf64, any(q).([]float64))
		return T(res), err
	}
	return 0, moerr.NewInternalErrorNoCtx("vector type not supported")
}

func CosineDistanceF32(a, b []float32) (float32, error) {
	n := len(a)
	if n != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension mismatch")
	}
	var dot, normA, normB float32
	i := 0
	if n >= 8 && hasNeon {
		// Three accumulator chains already give decent ILP, so a 2x unroll
		// (8 elements) is enough to keep the FMLA pipeline fed.
		accD0, accD1 := archsimd.Float32x4{}, archsimd.Float32x4{}
		accA0, accA1 := archsimd.Float32x4{}, archsimd.Float32x4{}
		accB0, accB1 := archsimd.Float32x4{}, archsimd.Float32x4{}
		for i <= n-8 {
			va0, vb0 := archsimd.LoadFloat32x4(a[i:i+4]), archsimd.LoadFloat32x4(b[i:i+4])
			va1, vb1 := archsimd.LoadFloat32x4(a[i+4:i+8]), archsimd.LoadFloat32x4(b[i+4:i+8])
			accD0 = va0.MulAdd(vb0, accD0)
			accA0 = va0.MulAdd(va0, accA0)
			accB0 = vb0.MulAdd(vb0, accB0)
			accD1 = va1.MulAdd(vb1, accD1)
			accA1 = va1.MulAdd(va1, accA1)
			accB1 = vb1.MulAdd(vb1, accB1)
			i += 8
		}
		dot, normA, normB = sumF32x4(accD0.Add(accD1)), sumF32x4(accA0.Add(accA1)), sumF32x4(accB0.Add(accB1))
	}

	for i <= n-4 {
		// BCE Hint
		va := a[i : i+4 : i+4]
		vb := b[i : i+4 : i+4]
		dot += va[0]*vb[0] + va[1]*vb[1] + va[2]*vb[2] + va[3]*vb[3]
		normA += va[0]*va[0] + va[1]*va[1] + va[2]*va[2] + va[3]*va[3]
		normB += vb[0]*vb[0] + vb[1]*vb[1] + vb[2]*vb[2] + vb[3]*vb[3]
		i += 4
	}

	for ; i < n; i++ {
		dot, normA, normB = dot+a[i]*b[i], normA+a[i]*a[i], normB+b[i]*b[i]
	}
	d := float64(dot)
	den := math.Sqrt(float64(normA)) * math.Sqrt(float64(normB))
	if !cosineNormsOK(float64(normA), float64(normB), smallestNormalFloat32) {
		var nP, nQ float64
		var ok bool
		if d, nP, nQ, ok = cosineRecomputeF64(a, b); !ok {
			return 0, moerr.NewInternalErrorNoCtx("cosine distance: vector magnitude overflows the float64 domain")
		}
		den = math.Sqrt(nP) * math.Sqrt(nQ)
	}
	if den == 0 {
		if anyNonZero(a) && anyNonZero(b) {
			return 0, moerr.NewInternalErrorNoCtx("cosine distance: vector magnitude underflows the element domain")
		}
		return 1.0, nil
	}
	return float32(cosineDistClamped(d, den)), nil
}

func CosineDistanceF64(a, b []float64) (float64, error) {
	n := len(a)
	if n != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension mismatch")
	}
	var dot, normA, normB float64
	i := 0
	if n >= 4 && hasNeon {
		accD0, accD1 := archsimd.Float64x2{}, archsimd.Float64x2{}
		accA0, accA1 := archsimd.Float64x2{}, archsimd.Float64x2{}
		accB0, accB1 := archsimd.Float64x2{}, archsimd.Float64x2{}
		for i <= n-4 {
			va0, vb0 := archsimd.LoadFloat64x2(a[i:i+2]), archsimd.LoadFloat64x2(b[i:i+2])
			va1, vb1 := archsimd.LoadFloat64x2(a[i+2:i+4]), archsimd.LoadFloat64x2(b[i+2:i+4])
			accD0 = va0.MulAdd(vb0, accD0)
			accA0 = va0.MulAdd(va0, accA0)
			accB0 = vb0.MulAdd(vb0, accB0)
			accD1 = va1.MulAdd(vb1, accD1)
			accA1 = va1.MulAdd(va1, accA1)
			accB1 = vb1.MulAdd(vb1, accB1)
			i += 4
		}
		dot, normA, normB = sumF64x2(accD0.Add(accD1)), sumF64x2(accA0.Add(accA1)), sumF64x2(accB0.Add(accB1))
	}

	for i <= n-4 {
		// BCE Hint
		va := a[i : i+4 : i+4]
		vb := b[i : i+4 : i+4]
		dot += va[0]*vb[0] + va[1]*vb[1] + va[2]*vb[2] + va[3]*vb[3]
		normA += va[0]*va[0] + va[1]*va[1] + va[2]*va[2] + va[3]*va[3]
		normB += vb[0]*vb[0] + vb[1]*vb[1] + vb[2]*vb[2] + vb[3]*vb[3]
		i += 4
	}

	for ; i < n; i++ {
		dot, normA, normB = dot+a[i]*b[i], normA+a[i]*a[i], normB+b[i]*b[i]
	}
	d := float64(dot)
	den := math.Sqrt(float64(normA)) * math.Sqrt(float64(normB))
	if !cosineNormsOK(float64(normA), float64(normB), smallestNormalFloat64) {
		var nP, nQ float64
		var ok bool
		if d, nP, nQ, ok = cosineRecomputeF64(a, b); !ok {
			return 0, moerr.NewInternalErrorNoCtx("cosine distance: vector magnitude overflows the float64 domain")
		}
		den = math.Sqrt(nP) * math.Sqrt(nQ)
	}
	if den == 0 {
		if anyNonZero(a) && anyNonZero(b) {
			return 0, moerr.NewInternalErrorNoCtx("cosine distance: vector magnitude underflows the element domain")
		}
		return 1.0, nil
	}
	return cosineDistClamped(d, den), nil
}

func CosineDistance[T types.RealNumbers](p, q []T) (T, error) {
	if pf32, ok := any(p).([]float32); ok {
		res, err := CosineDistanceF32(pf32, any(q).([]float32))
		return T(res), err
	}
	if pf64, ok := any(p).([]float64); ok {
		res, err := CosineDistanceF64(pf64, any(q).([]float64))
		return T(res), err
	}
	return 0, moerr.NewInternalErrorNoCtx("vector type not supported")
}

func CosineSimilarityF32(a, b []float32) (float32, error) {
	n := len(a)
	if n == 0 {
		return 0, nil
	}
	if n != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension mismatch")
	}
	var dot, normA, normB float32
	i := 0
	if n >= 8 && hasNeon {
		accD0, accD1 := archsimd.Float32x4{}, archsimd.Float32x4{}
		accA0, accA1 := archsimd.Float32x4{}, archsimd.Float32x4{}
		accB0, accB1 := archsimd.Float32x4{}, archsimd.Float32x4{}
		for i <= n-8 {
			va0, vb0 := archsimd.LoadFloat32x4(a[i:i+4]), archsimd.LoadFloat32x4(b[i:i+4])
			va1, vb1 := archsimd.LoadFloat32x4(a[i+4:i+8]), archsimd.LoadFloat32x4(b[i+4:i+8])
			accD0 = va0.MulAdd(vb0, accD0)
			accA0 = va0.MulAdd(va0, accA0)
			accB0 = vb0.MulAdd(vb0, accB0)
			accD1 = va1.MulAdd(vb1, accD1)
			accA1 = va1.MulAdd(va1, accA1)
			accB1 = vb1.MulAdd(vb1, accB1)
			i += 8
		}
		dot, normA, normB = sumF32x4(accD0.Add(accD1)), sumF32x4(accA0.Add(accA1)), sumF32x4(accB0.Add(accB1))
	}

	for i <= n-4 {
		// BCE Hint
		va := a[i : i+4 : i+4]
		vb := b[i : i+4 : i+4]
		dot += va[0]*vb[0] + va[1]*vb[1] + va[2]*vb[2] + va[3]*vb[3]
		normA += va[0]*va[0] + va[1]*va[1] + va[2]*va[2] + va[3]*va[3]
		normB += vb[0]*vb[0] + vb[1]*vb[1] + vb[2]*vb[2] + vb[3]*vb[3]
		i += 4
	}

	for ; i < n; i++ {
		dot, normA, normB = dot+a[i]*b[i], normA+a[i]*a[i], normB+b[i]*b[i]
	}
	d := float64(dot)
	den := math.Sqrt(float64(normA)) * math.Sqrt(float64(normB))
	if !cosineNormsOK(float64(normA), float64(normB), smallestNormalFloat32) {
		var nP, nQ float64
		var ok bool
		if d, nP, nQ, ok = cosineRecomputeF64(a, b); !ok {
			return 0, moerr.NewInternalErrorNoCtx("cosine similarity: vector magnitude overflows the float64 domain")
		}
		den = math.Sqrt(nP) * math.Sqrt(nQ)
	}
	if den == 0 {
		if anyNonZero(a) && anyNonZero(b) {
			return 0, moerr.NewInternalErrorNoCtx("cosine similarity: vector magnitude underflows the element domain")
		}
		return 0, moerr.NewInternalErrorNoCtx("cosine similarity: one of the vector is zero")
	}
	// Clamp to [-1,1]: float32 accumulation can push the quotient a hair
	// outside (e.g. 1.000000119) and mirror the scalar CosineSimilarity.
	sim := d / den
	if sim > 1 {
		sim = 1
	} else if sim < -1 {
		sim = -1
	}
	return float32(sim), nil
}

func CosineSimilarityF64(a, b []float64) (float64, error) {
	n := len(a)
	if n == 0 {
		return 0, nil
	}
	if n != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension mismatch")
	}
	var dot, normA, normB float64
	i := 0
	if n >= 4 && hasNeon {
		accD0, accD1 := archsimd.Float64x2{}, archsimd.Float64x2{}
		accA0, accA1 := archsimd.Float64x2{}, archsimd.Float64x2{}
		accB0, accB1 := archsimd.Float64x2{}, archsimd.Float64x2{}
		for i <= n-4 {
			va0, vb0 := archsimd.LoadFloat64x2(a[i:i+2]), archsimd.LoadFloat64x2(b[i:i+2])
			va1, vb1 := archsimd.LoadFloat64x2(a[i+2:i+4]), archsimd.LoadFloat64x2(b[i+2:i+4])
			accD0 = va0.MulAdd(vb0, accD0)
			accA0 = va0.MulAdd(va0, accA0)
			accB0 = vb0.MulAdd(vb0, accB0)
			accD1 = va1.MulAdd(vb1, accD1)
			accA1 = va1.MulAdd(va1, accA1)
			accB1 = vb1.MulAdd(vb1, accB1)
			i += 4
		}
		dot, normA, normB = sumF64x2(accD0.Add(accD1)), sumF64x2(accA0.Add(accA1)), sumF64x2(accB0.Add(accB1))
	}

	for i <= n-4 {
		// BCE Hint
		va := a[i : i+4 : i+4]
		vb := b[i : i+4 : i+4]
		dot += va[0]*vb[0] + va[1]*vb[1] + va[2]*vb[2] + va[3]*vb[3]
		normA += va[0]*va[0] + va[1]*va[1] + va[2]*va[2] + va[3]*va[3]
		normB += vb[0]*vb[0] + vb[1]*vb[1] + vb[2]*vb[2] + vb[3]*vb[3]
		i += 4
	}

	for ; i < n; i++ {
		dot, normA, normB = dot+a[i]*b[i], normA+a[i]*a[i], normB+b[i]*b[i]
	}
	d := float64(dot)
	den := math.Sqrt(float64(normA)) * math.Sqrt(float64(normB))
	if !cosineNormsOK(float64(normA), float64(normB), smallestNormalFloat64) {
		var nP, nQ float64
		var ok bool
		if d, nP, nQ, ok = cosineRecomputeF64(a, b); !ok {
			return 0, moerr.NewInternalErrorNoCtx("cosine similarity: vector magnitude overflows the float64 domain")
		}
		den = math.Sqrt(nP) * math.Sqrt(nQ)
	}
	if den == 0 {
		if anyNonZero(a) && anyNonZero(b) {
			return 0, moerr.NewInternalErrorNoCtx("cosine similarity: vector magnitude underflows the element domain")
		}
		return 0, moerr.NewInternalErrorNoCtx("cosine similarity: one of the vector is zero")
	}
	// Clamp to [-1,1]: float accumulation can push the quotient a hair
	// outside and mirror the scalar CosineSimilarity.
	sim := d / den
	if sim > 1 {
		sim = 1
	} else if sim < -1 {
		sim = -1
	}
	return sim, nil
}

func CosineSimilarity[T types.RealNumbers](p, q []T) (T, error) {
	if pf32, ok := any(p).([]float32); ok {
		res, err := CosineSimilarityF32(pf32, any(q).([]float32))
		return T(res), err
	}
	if pf64, ok := any(p).([]float64); ok {
		res, err := CosineSimilarityF64(pf64, any(q).([]float64))
		return T(res), err
	}
	return 0, moerr.NewInternalErrorNoCtx("vector type not supported")
}

func SphericalDistanceFloat32(a, b []float32) (float32, error) {
	n := len(a)
	if n != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	var total float32
	i := 0
	if hasNeon && n >= 32 {
		// 8 FMLA chains (32 f32/iter): ~+25% over 4 accumulators on wide NEON cores.
		acc0, acc1, acc2, acc3 := archsimd.Float32x4{}, archsimd.Float32x4{}, archsimd.Float32x4{}, archsimd.Float32x4{}
		acc4, acc5, acc6, acc7 := archsimd.Float32x4{}, archsimd.Float32x4{}, archsimd.Float32x4{}, archsimd.Float32x4{}
		for i <= n-32 {
			as, bs := a[i:i+32:i+32], b[i:i+32:i+32]
			acc0 = archsimd.LoadFloat32x4(as[0:4]).MulAdd(archsimd.LoadFloat32x4(bs[0:4]), acc0)
			acc1 = archsimd.LoadFloat32x4(as[4:8]).MulAdd(archsimd.LoadFloat32x4(bs[4:8]), acc1)
			acc2 = archsimd.LoadFloat32x4(as[8:12]).MulAdd(archsimd.LoadFloat32x4(bs[8:12]), acc2)
			acc3 = archsimd.LoadFloat32x4(as[12:16]).MulAdd(archsimd.LoadFloat32x4(bs[12:16]), acc3)
			acc4 = archsimd.LoadFloat32x4(as[16:20]).MulAdd(archsimd.LoadFloat32x4(bs[16:20]), acc4)
			acc5 = archsimd.LoadFloat32x4(as[20:24]).MulAdd(archsimd.LoadFloat32x4(bs[20:24]), acc5)
			acc6 = archsimd.LoadFloat32x4(as[24:28]).MulAdd(archsimd.LoadFloat32x4(bs[24:28]), acc6)
			acc7 = archsimd.LoadFloat32x4(as[28:32]).MulAdd(archsimd.LoadFloat32x4(bs[28:32]), acc7)
			i += 32
		}
		total += sumF32x4(acc0.Add(acc1).Add(acc2.Add(acc3)).Add(acc4.Add(acc5).Add(acc6.Add(acc7))))
	}
	if hasNeon && i <= n-16 {
		acc0, acc1, acc2, acc3 := archsimd.Float32x4{}, archsimd.Float32x4{}, archsimd.Float32x4{}, archsimd.Float32x4{}
		for i <= n-16 {
			as, bs := a[i:i+16:i+16], b[i:i+16:i+16]
			acc0 = archsimd.LoadFloat32x4(as[0:4]).MulAdd(archsimd.LoadFloat32x4(bs[0:4]), acc0)
			acc1 = archsimd.LoadFloat32x4(as[4:8]).MulAdd(archsimd.LoadFloat32x4(bs[4:8]), acc1)
			acc2 = archsimd.LoadFloat32x4(as[8:12]).MulAdd(archsimd.LoadFloat32x4(bs[8:12]), acc2)
			acc3 = archsimd.LoadFloat32x4(as[12:16]).MulAdd(archsimd.LoadFloat32x4(bs[12:16]), acc3)
			i += 16
		}
		total += sumF32x4(acc0.Add(acc1).Add(acc2.Add(acc3)))
	}

	for i <= n-8 {
		// BCE Hint
		as := a[i : i+8 : i+8]
		bs := b[i : i+8 : i+8]
		total += as[0]*bs[0] + as[1]*bs[1] + as[2]*bs[2] + as[3]*bs[3] +
			as[4]*bs[4] + as[5]*bs[5] + as[6]*bs[6] + as[7]*bs[7]
		i += 8
	}

	for ; i < n; i++ {
		total += a[i] * b[i]
	}
	if total > 1.0 {
		total = 1.0
	} else if total < -1.0 {
		total = -1.0
	}
	// A NaN total (float lane overflow cancelling signs) is not caught by the clamp
	// (NaN compares false) and acos(NaN) is NaN; recover the in-order reference, fast-
	// failing genuine overflow to +Inf so ranking stays well-ordered (#29496).
	return recoverSpherical(float32(math.Acos(float64(total))/math.Pi), a, b), nil
}

func SphericalDistanceFloat64(a, b []float64) (float64, error) {
	n := len(a)
	if n != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	var total float64
	i := 0
	if hasNeon && n >= 8 {
		acc0, acc1, acc2, acc3 := archsimd.Float64x2{}, archsimd.Float64x2{}, archsimd.Float64x2{}, archsimd.Float64x2{}
		for i <= n-8 {
			as, bs := a[i:i+8:i+8], b[i:i+8:i+8]
			acc0 = archsimd.LoadFloat64x2(as[0:2]).MulAdd(archsimd.LoadFloat64x2(bs[0:2]), acc0)
			acc1 = archsimd.LoadFloat64x2(as[2:4]).MulAdd(archsimd.LoadFloat64x2(bs[2:4]), acc1)
			acc2 = archsimd.LoadFloat64x2(as[4:6]).MulAdd(archsimd.LoadFloat64x2(bs[4:6]), acc2)
			acc3 = archsimd.LoadFloat64x2(as[6:8]).MulAdd(archsimd.LoadFloat64x2(bs[6:8]), acc3)
			i += 8
		}
		total += sumF64x2(acc0.Add(acc1).Add(acc2.Add(acc3)))
	}

	for i <= n-8 {
		// BCE Hint
		as := a[i : i+8 : i+8]
		bs := b[i : i+8 : i+8]
		total += as[0]*bs[0] + as[1]*bs[1] + as[2]*bs[2] + as[3]*bs[3] +
			as[4]*bs[4] + as[5]*bs[5] + as[6]*bs[6] + as[7]*bs[7]
		i += 8
	}

	for ; i < n; i++ {
		total += a[i] * b[i]
	}
	if total > 1.0 {
		total = 1.0
	} else if total < -1.0 {
		total = -1.0
	}
	return recoverSpherical(math.Acos(total)/math.Pi, a, b), nil
}

func SphericalDistance[T types.RealNumbers](p, q []T) (T, error) {
	if pf32, ok := any(p).([]float32); ok {
		res, err := SphericalDistanceFloat32(pf32, any(q).([]float32))
		return T(res), err
	}
	if pf64, ok := any(p).([]float64); ok {
		res, err := SphericalDistanceFloat64(pf64, any(q).([]float64))
		return T(res), err
	}
	return 0, moerr.NewInternalErrorNoCtx("vector type not supported")
}

func ScaleInPlace[T types.RealNumbers](v []T, scale T) {
	for i := range v {
		v[i] *= scale
	}
}
