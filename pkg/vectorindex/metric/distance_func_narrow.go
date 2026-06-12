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

// Distance kernels for the narrow vector element types: vecbf16 (types.BF16),
// vecf16 (types.Float16), vecint8 (int8). Pure Go, loop-unrolled. Untagged so
// it compiles in both the scalar and SIMD builds (a narrow SIMD variant can be
// split out later as distance_func_narrow_amd64.go, with this as the fallback +
// equivalence oracle).
//
// Semantics MATCH ResolveDistanceFn (the float32 path):
//   - Metric_L2Distance / Metric_L2sqDistance -> squared L2 (caller sqrts L2)
//   - Metric_InnerProduct                     -> -dot
//   - Metric_CosineDistance                   -> 1 - similarity
//   - Metric_L1Distance                       -> sum|a-b|
//
// bf16/f16 decode to float32 and reuse the float32 kernels (Go has no native
// fp16 arithmetic). int8 uses INTEGER (int64-accumulated) kernels — no float
// upcast in the inner loop — with only the cosine denominator going through
// float for the sqrt/divide.

package metric

import (
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// NarrowDistanceFn computes a distance between two narrow-typed vectors given
// their raw stored bytes.
type NarrowDistanceFn func(a, b []byte) (float64, error)

// ResolveNarrowDistanceFn returns the distance function for a narrow vector
// element type (bf16/f16/int8), or an error for any other oid.
func ResolveNarrowDistanceFn(oid types.T, metric MetricType) (NarrowDistanceFn, error) {
	switch oid {
	case types.T_array_bf16:
		kern, err := resolveBF16Kernel(metric)
		if err != nil {
			return nil, err
		}
		return func(a, b []byte) (float64, error) {
			// BytesToArray is a zero-copy reinterpret; the kernel decodes each
			// element to float32 inline (no []float32 slice materialized).
			return kern(types.BytesToArray[types.BF16](a), types.BytesToArray[types.BF16](b))
		}, nil
	case types.T_array_float16:
		kern, err := resolveF16Kernel(metric)
		if err != nil {
			return nil, err
		}
		return func(a, b []byte) (float64, error) {
			return kern(types.BytesToArray[types.Float16](a), types.BytesToArray[types.Float16](b))
		}, nil
	case types.T_array_int8:
		kern, err := resolveInt8Kernel(metric)
		if err != nil {
			return nil, err
		}
		return func(a, b []byte) (float64, error) {
			return kern(types.BytesToArray[int8](a), types.BytesToArray[int8](b))
		}, nil
	default:
		return nil, moerr.NewInternalErrorNoCtx("ResolveNarrowDistanceFn: not a narrow vector type")
	}
}

// ----------------------------------------------------------------------------
// bf16 / f16 CONCRETE fused kernels (unroll-8). NOT generic: a generic
// A generic [T] would share the uint16 gcshape, so .ToFloat32() would become a
// dictionary (virtual) call per element and never inlines. Concrete types let
// .ToFloat32() inline (bf16 = one shift). Decode inline, accumulate in float32,
// no slice materialized -> zero alloc. The multiply/add cannot be done without a
// float (bf16/f16 are floating-point; Go has no 16-bit-float ALU).
// ----------------------------------------------------------------------------

// bf16 kernel selection. The SIMD build (distance_func_narrow_amd64.go) swaps
// these to its archsimd implementations in init() when AVX-512 is available;
// otherwise they stay the pure-Go fallbacks defined below (which also remain the
// equivalence oracle the SIMD tests compare against).
var (
	bf16L2sqFn   = l2sqBF16
	bf16IPFn     = innerProductBF16
	bf16CosineFn = cosineDistanceBF16
	bf16L1Fn     = l1DistanceBF16
)

func resolveBF16Kernel(metric MetricType) (func(a, b []types.BF16) (float64, error), error) {
	switch metric {
	case Metric_L2Distance, Metric_L2sqDistance:
		return bf16L2sqFn, nil
	case Metric_InnerProduct:
		return bf16IPFn, nil
	case Metric_CosineDistance:
		return bf16CosineFn, nil
	case Metric_L1Distance:
		return bf16L1Fn, nil
	default:
		return nil, moerr.NewInternalErrorNoCtx("invalid distance type")
	}
}

func l2sqBF16(a, b []types.BF16) (float64, error) {
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	var sum float32
	n := len(a)
	i := 0
	for ; i <= n-8; i += 8 {
		aa := a[i : i+8 : i+8]
		bb := b[i : i+8 : i+8]
		d0 := aa[0].ToFloat32() - bb[0].ToFloat32()
		d1 := aa[1].ToFloat32() - bb[1].ToFloat32()
		d2 := aa[2].ToFloat32() - bb[2].ToFloat32()
		d3 := aa[3].ToFloat32() - bb[3].ToFloat32()
		d4 := aa[4].ToFloat32() - bb[4].ToFloat32()
		d5 := aa[5].ToFloat32() - bb[5].ToFloat32()
		d6 := aa[6].ToFloat32() - bb[6].ToFloat32()
		d7 := aa[7].ToFloat32() - bb[7].ToFloat32()
		sum += (d0*d0 + d1*d1) + (d2*d2 + d3*d3) + (d4*d4 + d5*d5) + (d6*d6 + d7*d7)
	}
	for ; i < n; i++ {
		d := a[i].ToFloat32() - b[i].ToFloat32()
		sum += d * d
	}
	return float64(sum), nil
}

func innerProductBF16(a, b []types.BF16) (float64, error) {
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	var sum float32
	n := len(a)
	i := 0
	for ; i <= n-8; i += 8 {
		aa := a[i : i+8 : i+8]
		bb := b[i : i+8 : i+8]
		sum += (aa[0].ToFloat32()*bb[0].ToFloat32() + aa[1].ToFloat32()*bb[1].ToFloat32()) +
			(aa[2].ToFloat32()*bb[2].ToFloat32() + aa[3].ToFloat32()*bb[3].ToFloat32()) +
			(aa[4].ToFloat32()*bb[4].ToFloat32() + aa[5].ToFloat32()*bb[5].ToFloat32()) +
			(aa[6].ToFloat32()*bb[6].ToFloat32() + aa[7].ToFloat32()*bb[7].ToFloat32())
	}
	for ; i < n; i++ {
		sum += a[i].ToFloat32() * b[i].ToFloat32()
	}
	return float64(-sum), nil
}

func l1DistanceBF16(a, b []types.BF16) (float64, error) {
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	var sum float32
	for i := range a {
		d := a[i].ToFloat32() - b[i].ToFloat32()
		if d < 0 {
			d = -d
		}
		sum += d
	}
	return float64(sum), nil
}

// cosineDistClamped mirrors metric.CosineDistance: clamp the cosine similarity to
// [-1,1] (float32 accumulation / fp16 decode can push it a hair outside) before
// distance = 1 - sim, so a near-parallel pair never yields a tiny negative
// distance that would mis-sort in a top-k scan.
func cosineDistClamped(dot, denom float64) float64 {
	sim := dot / denom
	if sim > 1 {
		sim = 1
	} else if sim < -1 {
		sim = -1
	}
	return 1.0 - sim
}

func cosineDistanceBF16(a, b []types.BF16) (float64, error) {
	if len(a) == 0 {
		return 0, nil
	}
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	var dot, na2, nb2 float32
	for i := range a {
		ai := a[i].ToFloat32()
		bi := b[i].ToFloat32()
		dot += ai * bi
		na2 += ai * ai
		nb2 += bi * bi
	}
	denom := math.Sqrt(float64(na2)) * math.Sqrt(float64(nb2))
	if denom == 0 {
		return 1.0, nil
	}
	return cosineDistClamped(float64(dot), denom), nil
}

// magic-multiply branchless half->float (Fabian Giesen / rygorous,
// https://gist.github.com/rygorous/2156668). No loop and no fallback call, so it
// inlines into the kernels — unlike types.Float16.ToFloat32, whose subnormal
// normalization loop blocks inlining. The magic multiply rescales the exponent
// and turns half subnormals into the right normal floats in one step; the single
// branch only fixes up Inf/NaN. Verified EXHAUSTIVELY against ToFloat32 over all
// 65536 inputs (TestF16FastExhaustive).
var (
	f16Magic     = math.Float32frombits(uint32(254-15) << 23)
	f16WasInfNan = math.Float32frombits(uint32(127+16) << 23)
)

func f16fast(h types.Float16) float32 {
	o := uint32(h&0x7fff) << 13              // exponent/mantissa bits, into f32 position
	of := math.Float32frombits(o) * f16Magic // rescale exponent; subnormals -> normals
	ou := math.Float32bits(of)
	if of >= f16WasInfNan { // Inf/NaN -> max exponent
		ou |= 255 << 23
	}
	ou |= uint32(h&0x8000) << 16 // sign
	return math.Float32frombits(ou)
}

// f16 kernel selection — swapped to archsimd impls by distance_func_narrow_f16_amd64.go.
var (
	f16L2sqFn   = l2sqF16
	f16IPFn     = innerProductF16
	f16CosineFn = cosineDistanceF16
	f16L1Fn     = l1DistanceF16
)

func resolveF16Kernel(metric MetricType) (func(a, b []types.Float16) (float64, error), error) {
	switch metric {
	case Metric_L2Distance, Metric_L2sqDistance:
		return f16L2sqFn, nil
	case Metric_InnerProduct:
		return f16IPFn, nil
	case Metric_CosineDistance:
		return f16CosineFn, nil
	case Metric_L1Distance:
		return f16L1Fn, nil
	default:
		return nil, moerr.NewInternalErrorNoCtx("invalid distance type")
	}
}

func l2sqF16(a, b []types.Float16) (float64, error) {
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	var sum float32
	n := len(a)
	i := 0
	for ; i <= n-8; i += 8 {
		aa := a[i : i+8 : i+8]
		bb := b[i : i+8 : i+8]
		d0 := f16fast(aa[0]) - f16fast(bb[0])
		d1 := f16fast(aa[1]) - f16fast(bb[1])
		d2 := f16fast(aa[2]) - f16fast(bb[2])
		d3 := f16fast(aa[3]) - f16fast(bb[3])
		d4 := f16fast(aa[4]) - f16fast(bb[4])
		d5 := f16fast(aa[5]) - f16fast(bb[5])
		d6 := f16fast(aa[6]) - f16fast(bb[6])
		d7 := f16fast(aa[7]) - f16fast(bb[7])
		sum += (d0*d0 + d1*d1) + (d2*d2 + d3*d3) + (d4*d4 + d5*d5) + (d6*d6 + d7*d7)
	}
	for ; i < n; i++ {
		d := f16fast(a[i]) - f16fast(b[i])
		sum += d * d
	}
	return float64(sum), nil
}

func innerProductF16(a, b []types.Float16) (float64, error) {
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	var sum float32
	n := len(a)
	i := 0
	for ; i <= n-8; i += 8 {
		aa := a[i : i+8 : i+8]
		bb := b[i : i+8 : i+8]
		sum += (f16fast(aa[0])*f16fast(bb[0]) + f16fast(aa[1])*f16fast(bb[1])) +
			(f16fast(aa[2])*f16fast(bb[2]) + f16fast(aa[3])*f16fast(bb[3])) +
			(f16fast(aa[4])*f16fast(bb[4]) + f16fast(aa[5])*f16fast(bb[5])) +
			(f16fast(aa[6])*f16fast(bb[6]) + f16fast(aa[7])*f16fast(bb[7]))
	}
	for ; i < n; i++ {
		sum += f16fast(a[i]) * f16fast(b[i])
	}
	return float64(-sum), nil
}

func l1DistanceF16(a, b []types.Float16) (float64, error) {
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	var sum float32
	for i := range a {
		d := f16fast(a[i]) - f16fast(b[i])
		if d < 0 {
			d = -d
		}
		sum += d
	}
	return float64(sum), nil
}

func cosineDistanceF16(a, b []types.Float16) (float64, error) {
	if len(a) == 0 {
		return 0, nil
	}
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	var dot, na2, nb2 float32
	for i := range a {
		ai := f16fast(a[i])
		bi := f16fast(b[i])
		dot += ai * bi
		na2 += ai * ai
		nb2 += bi * bi
	}
	denom := math.Sqrt(float64(na2)) * math.Sqrt(float64(nb2))
	if denom == 0 {
		return 1.0, nil
	}
	return cosineDistClamped(float64(dot), denom), nil
}

// int8 kernel selection — swapped to archsimd impls by distance_func_narrow_int8_amd64.go.
var (
	int8L2sqFn   = l2sqInt8
	int8IPFn     = innerProductInt8
	int8CosineFn = cosineDistanceInt8
	int8L1Fn     = l1DistanceInt8
)

func resolveInt8Kernel(metric MetricType) (func(a, b []int8) (float64, error), error) {
	switch metric {
	case Metric_L2Distance, Metric_L2sqDistance:
		return int8L2sqFn, nil
	case Metric_InnerProduct:
		return int8IPFn, nil
	case Metric_CosineDistance:
		return int8CosineFn, nil
	case Metric_L1Distance:
		return int8L1Fn, nil
	default:
		return nil, moerr.NewInternalErrorNoCtx("invalid distance type")
	}
}

// ----------------------------------------------------------------------------
// int8 integer kernels (unroll-8). Accumulate in int64: for int8 inputs the
// per-element term is bounded (|d|<=255 -> d*d<=65025, a*b in [-16384,16129]),
// and MaxArrayDimension is 65535, so int32 could overflow — int64 cannot.
// ----------------------------------------------------------------------------

func l2sqInt8(a, b []int8) (float64, error) {
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	var sum int64
	n := len(a)
	i := 0
	for ; i <= n-8; i += 8 {
		aa := a[i : i+8 : i+8]
		bb := b[i : i+8 : i+8]
		d0 := int32(aa[0]) - int32(bb[0])
		d1 := int32(aa[1]) - int32(bb[1])
		d2 := int32(aa[2]) - int32(bb[2])
		d3 := int32(aa[3]) - int32(bb[3])
		d4 := int32(aa[4]) - int32(bb[4])
		d5 := int32(aa[5]) - int32(bb[5])
		d6 := int32(aa[6]) - int32(bb[6])
		d7 := int32(aa[7]) - int32(bb[7])
		sum += int64(d0*d0+d1*d1) + int64(d2*d2+d3*d3) + int64(d4*d4+d5*d5) + int64(d6*d6+d7*d7)
	}
	for ; i < n; i++ {
		d := int32(a[i]) - int32(b[i])
		sum += int64(d * d)
	}
	return float64(sum), nil
}

func innerProductInt8(a, b []int8) (float64, error) {
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	var sum int64
	n := len(a)
	i := 0
	for ; i <= n-8; i += 8 {
		aa := a[i : i+8 : i+8]
		bb := b[i : i+8 : i+8]
		sum += int64(int32(aa[0])*int32(bb[0])+int32(aa[1])*int32(bb[1])) +
			int64(int32(aa[2])*int32(bb[2])+int32(aa[3])*int32(bb[3])) +
			int64(int32(aa[4])*int32(bb[4])+int32(aa[5])*int32(bb[5])) +
			int64(int32(aa[6])*int32(bb[6])+int32(aa[7])*int32(bb[7]))
	}
	for ; i < n; i++ {
		sum += int64(int32(a[i]) * int32(b[i]))
	}
	// matches metric.InnerProduct: returns -dot
	return float64(-sum), nil
}

func l1DistanceInt8(a, b []int8) (float64, error) {
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	var sum int64
	n := len(a)
	i := 0
	abs := func(x int32) int32 {
		if x < 0 {
			return -x
		}
		return x
	}
	for ; i <= n-8; i += 8 {
		aa := a[i : i+8 : i+8]
		bb := b[i : i+8 : i+8]
		sum += int64(abs(int32(aa[0])-int32(bb[0]))+abs(int32(aa[1])-int32(bb[1]))) +
			int64(abs(int32(aa[2])-int32(bb[2]))+abs(int32(aa[3])-int32(bb[3]))) +
			int64(abs(int32(aa[4])-int32(bb[4]))+abs(int32(aa[5])-int32(bb[5]))) +
			int64(abs(int32(aa[6])-int32(bb[6]))+abs(int32(aa[7])-int32(bb[7])))
	}
	for ; i < n; i++ {
		sum += int64(abs(int32(a[i]) - int32(b[i])))
	}
	return float64(sum), nil
}

func cosineDistanceInt8(a, b []int8) (float64, error) {
	if len(a) == 0 {
		return 0, nil
	}
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	var dot, na2, nb2 int64
	n := len(a)
	i := 0
	for ; i <= n-8; i += 8 {
		aa := a[i : i+8 : i+8]
		bb := b[i : i+8 : i+8]
		for k := 0; k < 8; k++ {
			ai := int64(aa[k])
			bi := int64(bb[k])
			dot += ai * bi
			na2 += ai * ai
			nb2 += bi * bi
		}
	}
	for ; i < n; i++ {
		ai := int64(a[i])
		bi := int64(b[i])
		dot += ai * bi
		na2 += ai * ai
		nb2 += bi * bi
	}
	// matches metric.CosineDistance: denominator 0 -> distance 1.0
	denom := math.Sqrt(float64(na2)) * math.Sqrt(float64(nb2))
	if denom == 0 {
		return 1.0, nil
	}
	return cosineDistClamped(float64(dot), denom), nil
}
