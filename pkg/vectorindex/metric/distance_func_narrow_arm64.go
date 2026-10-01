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

// ARM64 NEON distance kernels for vecbf16 (types.BF16) and vecf16
// (types.Float16), mirroring distance_func_narrow_amd64.go and
// distance_func_narrow_f16_amd64.go at 128-bit width.
//
// bf16 is the high 16 bits of an IEEE float32, so bf16->f32 is a pure bit op:
// value<<16. As on amd64 there is no bf16 vector type, and none is needed: load
// the raw bf16 bytes as Uint32x4 (8 bf16 per load), split the even and odd
// 16-bit halves into two Float32x4 vectors with one shift / one and-mask plus a
// BitsToFloat32 bitcast, then reduce with sumF32x4 from distance_func_arm64.go
// (same package + build tag).
//
// f16 is NOT a plain shift (exponent rebias + subnormals) and NEON's hardware
// FCVT for half-precision is not surfaced by archsimd either, so f16 uses the
// same vectorized magic-multiply decode as amd64 (Fabian Giesen / rygorous),
// matching the scalar f16fast() bit-for-bit.
//
// The pure-Go kernels in distance_func_narrow.go stay the fallback and the
// equivalence oracle; init() only swaps the selection vars.

package metric

import (
	"math"
	"unsafe"

	"simd/archsimd"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func init() {
	// NEON is ARMv8 baseline, so unlike amd64 there is no tier ladder here —
	// either the SIMD kernels run, or MO_METRIC_NO_NEON forces the scalar
	// oracle back in for coverage.
	if hasNeon {
		bf16L2sqFn = l2sqBF16SIMD
		bf16IPFn = innerProductBF16SIMD
		bf16CosineFn = cosineDistanceBF16SIMD
		bf16L1Fn = l1DistanceBF16SIMD

		f16L2sqFn = l2sqF16SIMD
		f16IPFn = innerProductF16SIMD
		f16CosineFn = cosineDistanceF16SIMD
		f16L1Fn = l1DistanceF16SIMD
	}
}

// bf16AsU32 reinterprets a []types.BF16 (uint16-backed) as []uint32 viewing its
// first len/2 even-aligned pairs. ARMv8 permits unaligned loads on normal
// memory; the stored bf16 bytes originate from an 8-aligned []byte
// (BytesToArray), so in practice the start is 4-aligned anyway.
func bf16AsU32(s []types.BF16) []uint32 {
	if len(s) < 2 {
		return nil
	}
	return unsafe.Slice((*uint32)(unsafe.Pointer(unsafe.SliceData(s))), len(s)/2)
}

// ---- bf16 ----

func l2sqBF16SIMD(a, b []types.BF16) (float64, error) {
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	n := len(a)
	au, bu := bf16AsU32(a), bf16AsU32(b)
	hi := archsimd.BroadcastUint32x4(0xFFFF0000)
	acc0, acc1 := archsimd.Float32x4{}, archsimd.Float32x4{}
	np, j := len(au), 0
	for ; j <= np-4; j += 4 {
		ua := archsimd.LoadUint32x4(au[j : j+4])
		ub := archsimd.LoadUint32x4(bu[j : j+4])
		dE := ua.ShiftAllLeft(16).BitsToFloat32().Sub(ub.ShiftAllLeft(16).BitsToFloat32())
		dO := ua.And(hi).BitsToFloat32().Sub(ub.And(hi).BitsToFloat32())
		acc0 = dE.MulAdd(dE, acc0)
		acc1 = dO.MulAdd(dO, acc1)
	}
	sum := sumF32x4(acc0.Add(acc1))
	for i := j * 2; i < n; i++ {
		d := a[i].ToFloat32() - b[i].ToFloat32()
		sum += d * d
	}
	return float64(sum), nil
}

func innerProductBF16SIMD(a, b []types.BF16) (float64, error) {
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	n := len(a)
	au, bu := bf16AsU32(a), bf16AsU32(b)
	hi := archsimd.BroadcastUint32x4(0xFFFF0000)
	acc0, acc1 := archsimd.Float32x4{}, archsimd.Float32x4{}
	np, j := len(au), 0
	for ; j <= np-4; j += 4 {
		ua := archsimd.LoadUint32x4(au[j : j+4])
		ub := archsimd.LoadUint32x4(bu[j : j+4])
		acc0 = ua.ShiftAllLeft(16).BitsToFloat32().MulAdd(ub.ShiftAllLeft(16).BitsToFloat32(), acc0)
		acc1 = ua.And(hi).BitsToFloat32().MulAdd(ub.And(hi).BitsToFloat32(), acc1)
	}
	sum := sumF32x4(acc0.Add(acc1))
	for i := j * 2; i < n; i++ {
		sum += a[i].ToFloat32() * b[i].ToFloat32()
	}
	return nanToPosInf(float64(-sum)), nil
}

func l1DistanceBF16SIMD(a, b []types.BF16) (float64, error) {
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	n := len(a)
	au, bu := bf16AsU32(a), bf16AsU32(b)
	hi := archsimd.BroadcastUint32x4(0xFFFF0000)
	acc0, acc1 := archsimd.Float32x4{}, archsimd.Float32x4{}
	np, j := len(au), 0
	for ; j <= np-4; j += 4 {
		ua := archsimd.LoadUint32x4(au[j : j+4])
		ub := archsimd.LoadUint32x4(bu[j : j+4])
		// NEON has a native FABS, so no and-with-0x7FFFFFFF mask is needed.
		dE := ua.ShiftAllLeft(16).BitsToFloat32().Sub(ub.ShiftAllLeft(16).BitsToFloat32()).Abs()
		dO := ua.And(hi).BitsToFloat32().Sub(ub.And(hi).BitsToFloat32()).Abs()
		acc0 = acc0.Add(dE)
		acc1 = acc1.Add(dO)
	}
	sum := sumF32x4(acc0.Add(acc1))
	for i := j * 2; i < n; i++ {
		d := a[i].ToFloat32() - b[i].ToFloat32()
		if d < 0 {
			d = -d
		}
		sum += d
	}
	return float64(sum), nil
}

func cosineDistanceBF16SIMD(a, b []types.BF16) (float64, error) {
	if len(a) == 0 {
		return 0, nil
	}
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	n := len(a)
	au, bu := bf16AsU32(a), bf16AsU32(b)
	hi := archsimd.BroadcastUint32x4(0xFFFF0000)
	dot0, dot1 := archsimd.Float32x4{}, archsimd.Float32x4{}
	na0, na1 := archsimd.Float32x4{}, archsimd.Float32x4{}
	nb0, nb1 := archsimd.Float32x4{}, archsimd.Float32x4{}
	np, j := len(au), 0
	for ; j <= np-4; j += 4 {
		ua := archsimd.LoadUint32x4(au[j : j+4])
		ub := archsimd.LoadUint32x4(bu[j : j+4])
		aE := ua.ShiftAllLeft(16).BitsToFloat32()
		aO := ua.And(hi).BitsToFloat32()
		bE := ub.ShiftAllLeft(16).BitsToFloat32()
		bO := ub.And(hi).BitsToFloat32()
		dot0 = aE.MulAdd(bE, dot0)
		dot1 = aO.MulAdd(bO, dot1)
		na0 = aE.MulAdd(aE, na0)
		na1 = aO.MulAdd(aO, na1)
		nb0 = bE.MulAdd(bE, nb0)
		nb1 = bO.MulAdd(bO, nb1)
	}
	dot := sumF32x4(dot0.Add(dot1))
	na2 := sumF32x4(na0.Add(na1))
	nb2 := sumF32x4(nb0.Add(nb1))
	for i := j * 2; i < n; i++ {
		ai, bi := a[i].ToFloat32(), b[i].ToFloat32()
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

// ---- f16 ----

func f16AsU32(s []types.Float16) []uint32 {
	if len(s) < 2 {
		return nil
	}
	return unsafe.Slice((*uint32)(unsafe.Pointer(unsafe.SliceData(s))), len(s)/2)
}

// f16dec decodes 4 half-floats (each in the low 16 bits of a uint32 lane) to
// float32 — the SIMD form of f16fast(), Inf/NaN fixup included. As on amd64 the
// constants are individual args rather than a struct: a by-value struct of
// vectors spills to the stack and is reloaded on every field access.
func f16dec(h, m7fff, m8000, mInf archsimd.Uint32x4, magic, infNan archsimd.Float32x4) archsimd.Float32x4 {
	o := h.And(m7fff).ShiftAllLeft(13)
	of := o.BitsToFloat32().Mul(magic)
	ou := of.ToBits()
	// ou |= inf where of >= infNan. amd64 spells this Merge(ou, mask), which
	// go1.27 deprecates in favour of the equivalent IfElse(mask, ou).
	ou = ou.Or(mInf).IfElse(of.GreaterEqual(infNan), ou)
	return ou.Or(h.And(m8000).ShiftAllLeft(16)).BitsToFloat32()
}

// f16DecodeConsts builds the decode constants once per kernel as locals.
func f16DecodeConsts() (m7fff, m8000, mLo, mInf archsimd.Uint32x4, magic, infNan archsimd.Float32x4) {
	m7fff = archsimd.BroadcastUint32x4(0x7fff)
	m8000 = archsimd.BroadcastUint32x4(0x8000)
	mLo = archsimd.BroadcastUint32x4(0xffff)
	mInf = archsimd.BroadcastUint32x4(255 << 23)
	magic = archsimd.BroadcastFloat32x4(f16Magic)
	infNan = archsimd.BroadcastFloat32x4(f16WasInfNan)
	return
}

func l2sqF16SIMD(a, b []types.Float16) (float64, error) {
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	n := len(a)
	au, bu := f16AsU32(a), f16AsU32(b)
	m7fff, m8000, mLo, mInf, magic, infNan := f16DecodeConsts()
	acc0, acc1 := archsimd.Float32x4{}, archsimd.Float32x4{}
	np, j := len(au), 0
	for ; j <= np-4; j += 4 {
		ua := archsimd.LoadUint32x4(au[j : j+4])
		ub := archsimd.LoadUint32x4(bu[j : j+4])
		dE := f16dec(ua.And(mLo), m7fff, m8000, mInf, magic, infNan).Sub(f16dec(ub.And(mLo), m7fff, m8000, mInf, magic, infNan))
		dO := f16dec(ua.ShiftAllRight(16), m7fff, m8000, mInf, magic, infNan).Sub(f16dec(ub.ShiftAllRight(16), m7fff, m8000, mInf, magic, infNan))
		acc0 = dE.MulAdd(dE, acc0)
		acc1 = dO.MulAdd(dO, acc1)
	}
	sum := sumF32x4(acc0.Add(acc1))
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
	m7fff, m8000, mLo, mInf, magic, infNan := f16DecodeConsts()
	acc0, acc1 := archsimd.Float32x4{}, archsimd.Float32x4{}
	np, j := len(au), 0
	for ; j <= np-4; j += 4 {
		ua := archsimd.LoadUint32x4(au[j : j+4])
		ub := archsimd.LoadUint32x4(bu[j : j+4])
		acc0 = f16dec(ua.And(mLo), m7fff, m8000, mInf, magic, infNan).
			MulAdd(f16dec(ub.And(mLo), m7fff, m8000, mInf, magic, infNan), acc0)
		acc1 = f16dec(ua.ShiftAllRight(16), m7fff, m8000, mInf, magic, infNan).
			MulAdd(f16dec(ub.ShiftAllRight(16), m7fff, m8000, mInf, magic, infNan), acc1)
	}
	sum := sumF32x4(acc0.Add(acc1))
	for i := j * 2; i < n; i++ {
		sum += f16fast(a[i]) * f16fast(b[i])
	}
	return nanToPosInf(float64(-sum)), nil
}

func l1DistanceF16SIMD(a, b []types.Float16) (float64, error) {
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	n := len(a)
	au, bu := f16AsU32(a), f16AsU32(b)
	m7fff, m8000, mLo, mInf, magic, infNan := f16DecodeConsts()
	acc0, acc1 := archsimd.Float32x4{}, archsimd.Float32x4{}
	np, j := len(au), 0
	for ; j <= np-4; j += 4 {
		ua := archsimd.LoadUint32x4(au[j : j+4])
		ub := archsimd.LoadUint32x4(bu[j : j+4])
		dE := f16dec(ua.And(mLo), m7fff, m8000, mInf, magic, infNan).Sub(f16dec(ub.And(mLo), m7fff, m8000, mInf, magic, infNan)).Abs()
		dO := f16dec(ua.ShiftAllRight(16), m7fff, m8000, mInf, magic, infNan).Sub(f16dec(ub.ShiftAllRight(16), m7fff, m8000, mInf, magic, infNan)).Abs()
		acc0 = acc0.Add(dE)
		acc1 = acc1.Add(dO)
	}
	sum := sumF32x4(acc0.Add(acc1))
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
	m7fff, m8000, mLo, mInf, magic, infNan := f16DecodeConsts()
	dot0, dot1 := archsimd.Float32x4{}, archsimd.Float32x4{}
	na0, na1 := archsimd.Float32x4{}, archsimd.Float32x4{}
	nb0, nb1 := archsimd.Float32x4{}, archsimd.Float32x4{}
	np, j := len(au), 0
	for ; j <= np-4; j += 4 {
		ua := archsimd.LoadUint32x4(au[j : j+4])
		ub := archsimd.LoadUint32x4(bu[j : j+4])
		aE := f16dec(ua.And(mLo), m7fff, m8000, mInf, magic, infNan)
		aO := f16dec(ua.ShiftAllRight(16), m7fff, m8000, mInf, magic, infNan)
		bE := f16dec(ub.And(mLo), m7fff, m8000, mInf, magic, infNan)
		bO := f16dec(ub.ShiftAllRight(16), m7fff, m8000, mInf, magic, infNan)
		dot0 = aE.MulAdd(bE, dot0)
		dot1 = aO.MulAdd(bO, dot1)
		na0 = aE.MulAdd(aE, na0)
		na1 = aO.MulAdd(aO, na1)
		nb0 = bE.MulAdd(bE, nb0)
		nb1 = bO.MulAdd(bO, nb1)
	}
	dot := sumF32x4(dot0.Add(dot1))
	na2 := sumF32x4(na0.Add(na1))
	nb2 := sumF32x4(nb0.Add(nb1))
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
	return cosineDistClamped(float64(dot), denom), nil
}
