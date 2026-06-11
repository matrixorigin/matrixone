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
// fix up Inf/NaN with a masked Merge. Inputs load as Uint32x16 (32 f16/load),
// even/odd 16-bit halves split out, decoded, then fed to the existing AVX-512
// float32 reduction (sumF32x16). Matches f16fast bit-for-bit so it agrees with
// the scalar oracle.
//
// PERF: the six decode constants are passed to f16dec as individual vector args,
// NOT bundled in a struct. A by-value struct of vectors gets spilled to the stack
// and reloaded on every field access (77 MOVUPS in the inner loop -> ~7x slower);
// individual args stay in zmm registers. With that, f16 SIMD is ~8x over scalar
// (was ~1.3x with the struct) — the difference between "not worth it" and "worth
// it", so the swap below is ON.

package metric

import (
	"math"
	"unsafe"

	"simd/archsimd"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func init() {
	if hasAVX512 {
		f16L2sqFn = l2sqF16SIMD
		f16IPFn = innerProductF16SIMD
		f16CosineFn = cosineDistanceF16SIMD
		f16L1Fn = l1DistanceF16SIMD
	}
}

func f16AsU32(s []types.Float16) []uint32 {
	if len(s) < 2 {
		return nil
	}
	return unsafe.Slice((*uint32)(unsafe.Pointer(unsafe.SliceData(s))), len(s)/2)
}

// f16dec decodes 16 half-floats (each in the low 16 bits of a uint32 lane) to
// float32 — the SIMD form of f16fast(), Inf/NaN fixup included. Constants are
// individual args (see file header: a struct spills; args stay in registers).
func f16dec(h, m7fff, m8000, mInf archsimd.Uint32x16, magic, infNan archsimd.Float32x16) archsimd.Float32x16 {
	o := h.And(m7fff).ShiftAllLeft(13)
	of := o.AsFloat32x16().Mul(magic)
	ou := of.AsUint32x16()
	ou = ou.Or(mInf).Merge(ou, of.GreaterEqual(infNan)) // ou|=inf where >=infNan
	return ou.Or(h.And(m8000).ShiftAllLeft(16)).AsFloat32x16()
}

// f16Decode constants, built once per kernel as locals.
func f16DecodeConsts() (m7fff, m8000, mLo, mInf archsimd.Uint32x16, magic, infNan archsimd.Float32x16) {
	m7fff = archsimd.BroadcastUint32x16(0x7fff)
	m8000 = archsimd.BroadcastUint32x16(0x8000)
	mLo = archsimd.BroadcastUint32x16(0xffff)
	mInf = archsimd.BroadcastUint32x16(255 << 23)
	magic = archsimd.BroadcastFloat32x16(f16Magic)
	infNan = archsimd.BroadcastFloat32x16(f16WasInfNan)
	return
}

func l2sqF16SIMD(a, b []types.Float16) (float64, error) {
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	n := len(a)
	au, bu := f16AsU32(a), f16AsU32(b)
	m7fff, m8000, mLo, mInf, magic, infNan := f16DecodeConsts()
	acc0, acc1 := archsimd.Float32x16{}, archsimd.Float32x16{}
	np, j := len(au), 0
	for ; j <= np-16; j += 16 {
		ua := archsimd.LoadUint32x16Slice(au[j : j+16])
		ub := archsimd.LoadUint32x16Slice(bu[j : j+16])
		dE := f16dec(ua.And(mLo), m7fff, m8000, mInf, magic, infNan).Sub(f16dec(ub.And(mLo), m7fff, m8000, mInf, magic, infNan))
		dO := f16dec(ua.ShiftAllRight(16), m7fff, m8000, mInf, magic, infNan).Sub(f16dec(ub.ShiftAllRight(16), m7fff, m8000, mInf, magic, infNan))
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
	m7fff, m8000, mLo, mInf, magic, infNan := f16DecodeConsts()
	acc0, acc1 := archsimd.Float32x16{}, archsimd.Float32x16{}
	np, j := len(au), 0
	for ; j <= np-16; j += 16 {
		ua := archsimd.LoadUint32x16Slice(au[j : j+16])
		ub := archsimd.LoadUint32x16Slice(bu[j : j+16])
		acc0 = f16dec(ua.And(mLo), m7fff, m8000, mInf, magic, infNan).MulAdd(f16dec(ub.And(mLo), m7fff, m8000, mInf, magic, infNan), acc0)
		acc1 = f16dec(ua.ShiftAllRight(16), m7fff, m8000, mInf, magic, infNan).MulAdd(f16dec(ub.ShiftAllRight(16), m7fff, m8000, mInf, magic, infNan), acc1)
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
	m7fff, m8000, mLo, mInf, magic, infNan := f16DecodeConsts()
	absMask := archsimd.BroadcastUint32x16(0x7fffffff)
	acc0, acc1 := archsimd.Float32x16{}, archsimd.Float32x16{}
	np, j := len(au), 0
	for ; j <= np-16; j += 16 {
		ua := archsimd.LoadUint32x16Slice(au[j : j+16])
		ub := archsimd.LoadUint32x16Slice(bu[j : j+16])
		dE := f16dec(ua.And(mLo), m7fff, m8000, mInf, magic, infNan).Sub(f16dec(ub.And(mLo), m7fff, m8000, mInf, magic, infNan))
		dO := f16dec(ua.ShiftAllRight(16), m7fff, m8000, mInf, magic, infNan).Sub(f16dec(ub.ShiftAllRight(16), m7fff, m8000, mInf, magic, infNan))
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
	m7fff, m8000, mLo, mInf, magic, infNan := f16DecodeConsts()
	dot0, dot1 := archsimd.Float32x16{}, archsimd.Float32x16{}
	na0, na1 := archsimd.Float32x16{}, archsimd.Float32x16{}
	nb0, nb1 := archsimd.Float32x16{}, archsimd.Float32x16{}
	np, j := len(au), 0
	for ; j <= np-16; j += 16 {
		ua := archsimd.LoadUint32x16Slice(au[j : j+16])
		ub := archsimd.LoadUint32x16Slice(bu[j : j+16])
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
