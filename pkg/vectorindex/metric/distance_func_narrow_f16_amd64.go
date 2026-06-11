//go:build amd64 && go1.26 && goexperiment.simd

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

// AVX-512 SIMD distance kernels for vecf16 (types.Float16).
//
// IEEE half->float32 is NOT a plain shift (exponent rebias + subnormals), and Go
// archsimd exposes no f16 type (this CPU also lacks avx512_fp16, and F16C is not
// surfaced). So we vectorize the same magic-multiply f16fast() the scalar path
// uses (Fabian Giesen / rygorous): rescale the exponent via a float multiply and
// fix up Inf/NaN with a masked Merge. Inputs are loaded as Uint32x16 (32 f16 per
// load), even/odd 16-bit halves split out, decoded to Float32x16, then fed to the
// existing AVX-512 float32 reduction (sumF32x16). Matches f16fast bit-for-bit, so
// it agrees with the scalar oracle (which also uses f16fast).

package metric

import (
	"math"
	"unsafe"

	"simd/archsimd"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// NOTE: the f16 SIMD kernels are intentionally NOT swapped in. Unlike bf16
// (decode = one shift) and int8 (decode = sign-extend shifts), the IEEE
// half->float32 decode is a multi-op magic-multiply with no hardware support on
// this stack (archsimd has no f16 type; the CPU lacks avx512_fp16; F16C is not
// surfaced). Benchmarked at dim=1024 the vectorized decode only reaches ~1.3x
// over the already-cheap scalar f16fast — and cosine is slower — so production
// keeps the scalar path (f16*Fn defaults in distance_func_narrow.go). The
// kernels below remain compiled as the head-to-head benchmark / equivalence
// reference (Benchmark_Narrow_SIMDvsScalar, TestF16SIMDMatchesScalar); flip the
// four assignments here back on if a future archsimd exposes VCVTPH2PS.
func init() {}

// f16consts holds the broadcast constant vectors for the magic-multiply decode,
// built once per kernel call (cheap broadcasts) and passed into f16fastVec so it
// inlines without recomputing them.
type f16consts struct {
	mask7fff, mask8000, maskLo, infBits archsimd.Uint32x16
	magic, infNan                       archsimd.Float32x16
}

func newF16Consts() f16consts {
	return f16consts{
		mask7fff: archsimd.BroadcastUint32x16(0x7fff),
		mask8000: archsimd.BroadcastUint32x16(0x8000),
		maskLo:   archsimd.BroadcastUint32x16(0xffff),
		infBits:  archsimd.BroadcastUint32x16(255 << 23),
		magic:    archsimd.BroadcastFloat32x16(f16Magic),
		infNan:   archsimd.BroadcastFloat32x16(f16WasInfNan),
	}
}

// f16fastVec decodes 16 half-floats (each in the low 16 bits of a uint32 lane) to
// float32 — the SIMD form of f16fast(), Inf/NaN fixup included.
func f16fastVec(h archsimd.Uint32x16, c f16consts) archsimd.Float32x16 {
	o := h.And(c.mask7fff).ShiftAllLeft(13)
	of := o.AsFloat32x16().Mul(c.magic)
	ou := of.AsUint32x16()
	ouInf := ou.Or(c.infBits)
	ou = ouInf.Merge(ou, of.GreaterEqual(c.infNan)) // keep ouInf where >=infNan, else ou
	ou = ou.Or(h.And(c.mask8000).ShiftAllLeft(16))  // sign
	return ou.AsFloat32x16()
}

func f16AsU32(s []types.Float16) []uint32 {
	if len(s) < 2 {
		return nil
	}
	return unsafe.Slice((*uint32)(unsafe.Pointer(unsafe.SliceData(s))), len(s)/2)
}

func l2sqF16SIMD(a, b []types.Float16) (float64, error) {
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	n := len(a)
	au, bu := f16AsU32(a), f16AsU32(b)
	c := newF16Consts()
	acc0, acc1 := archsimd.Float32x16{}, archsimd.Float32x16{}
	np, j := len(au), 0
	for ; j <= np-16; j += 16 {
		ua := archsimd.LoadUint32x16Slice(au[j : j+16])
		ub := archsimd.LoadUint32x16Slice(bu[j : j+16])
		dE := f16fastVec(ua.And(c.maskLo), c).Sub(f16fastVec(ub.And(c.maskLo), c))
		dO := f16fastVec(ua.ShiftAllRight(16), c).Sub(f16fastVec(ub.ShiftAllRight(16), c))
		acc0 = dE.MulAdd(dE, acc0)
		acc1 = dO.MulAdd(dO, acc1)
	}
	sum := sumF32x16(acc0.Add(acc1))
	for i := j * 2; i < n; i++ {
		d := f16fast(a[i]) - f16fast(b[i])
		sum += d * d
	}
	return float64(sum), nil
}

func innerProductF16SIMD(a, b []types.Float16) (float64, error) {
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	n := len(a)
	au, bu := f16AsU32(a), f16AsU32(b)
	c := newF16Consts()
	acc0, acc1 := archsimd.Float32x16{}, archsimd.Float32x16{}
	np, j := len(au), 0
	for ; j <= np-16; j += 16 {
		ua := archsimd.LoadUint32x16Slice(au[j : j+16])
		ub := archsimd.LoadUint32x16Slice(bu[j : j+16])
		acc0 = f16fastVec(ua.And(c.maskLo), c).MulAdd(f16fastVec(ub.And(c.maskLo), c), acc0)
		acc1 = f16fastVec(ua.ShiftAllRight(16), c).MulAdd(f16fastVec(ub.ShiftAllRight(16), c), acc1)
	}
	sum := sumF32x16(acc0.Add(acc1))
	for i := j * 2; i < n; i++ {
		sum += f16fast(a[i]) * f16fast(b[i])
	}
	return float64(-sum), nil
}

func l1DistanceF16SIMD(a, b []types.Float16) (float64, error) {
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	n := len(a)
	au, bu := f16AsU32(a), f16AsU32(b)
	c := newF16Consts()
	absMask := archsimd.BroadcastUint32x16(0x7fffffff)
	acc0, acc1 := archsimd.Float32x16{}, archsimd.Float32x16{}
	np, j := len(au), 0
	for ; j <= np-16; j += 16 {
		ua := archsimd.LoadUint32x16Slice(au[j : j+16])
		ub := archsimd.LoadUint32x16Slice(bu[j : j+16])
		dE := f16fastVec(ua.And(c.maskLo), c).Sub(f16fastVec(ub.And(c.maskLo), c))
		dO := f16fastVec(ua.ShiftAllRight(16), c).Sub(f16fastVec(ub.ShiftAllRight(16), c))
		acc0 = acc0.Add(dE.AsUint32x16().And(absMask).AsFloat32x16())
		acc1 = acc1.Add(dO.AsUint32x16().And(absMask).AsFloat32x16())
	}
	sum := sumF32x16(acc0.Add(acc1))
	for i := j * 2; i < n; i++ {
		d := f16fast(a[i]) - f16fast(b[i])
		if d < 0 {
			d = -d
		}
		sum += d
	}
	return float64(sum), nil
}

func cosineDistanceF16SIMD(a, b []types.Float16) (float64, error) {
	if len(a) == 0 {
		return 0, nil
	}
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	n := len(a)
	au, bu := f16AsU32(a), f16AsU32(b)
	c := newF16Consts()
	dot0, dot1 := archsimd.Float32x16{}, archsimd.Float32x16{}
	na0, na1 := archsimd.Float32x16{}, archsimd.Float32x16{}
	nb0, nb1 := archsimd.Float32x16{}, archsimd.Float32x16{}
	np, j := len(au), 0
	for ; j <= np-16; j += 16 {
		ua := archsimd.LoadUint32x16Slice(au[j : j+16])
		ub := archsimd.LoadUint32x16Slice(bu[j : j+16])
		aE := f16fastVec(ua.And(c.maskLo), c)
		aO := f16fastVec(ua.ShiftAllRight(16), c)
		bE := f16fastVec(ub.And(c.maskLo), c)
		bO := f16fastVec(ub.ShiftAllRight(16), c)
		dot0 = aE.MulAdd(bE, dot0)
		dot1 = aO.MulAdd(bO, dot1)
		na0 = aE.MulAdd(aE, na0)
		na1 = aO.MulAdd(aO, na1)
		nb0 = bE.MulAdd(bE, nb0)
		nb1 = bO.MulAdd(bO, nb1)
	}
	dot := sumF32x16(dot0.Add(dot1))
	na2 := sumF32x16(na0.Add(na1))
	nb2 := sumF32x16(nb0.Add(nb1))
	for i := j * 2; i < n; i++ {
		ai, bi := f16fast(a[i]), f16fast(b[i])
		dot += ai * bi
		na2 += ai * ai
		nb2 += bi * bi
	}
	denom := math.Sqrt(float64(na2)) * math.Sqrt(float64(nb2))
	if denom == 0 {
		return 1.0, nil
	}
	return 1.0 - float64(dot)/denom, nil
}
