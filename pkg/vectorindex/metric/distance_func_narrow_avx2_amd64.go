//go:build amd64 && go1.27 && goexperiment.simd

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

// AVX2 (256-bit, 8-lane) narrow distance kernels — the middle fallback tier for
// CPUs that have AVX2 but not AVX-512. Mirrors the AVX-512 (x16) kernels exactly
// with Float32x8 / Int32x8 / Uint32x8 ops. Each type's init() (in its x16 file)
// selects AVX-512 -> AVX2 -> scalar.
//
// The narrow types benefit from AVX2 (unlike f32, where AVX2 ~= scalar): their
// decode (bf16 shift / int8 sign-extend / f16 magic-multiply) is pure scalar
// overhead that AVX2 vectorizes. Measured ~7-9x over scalar at dim=1024; AVX-512
// adds another ~1.3-1.5x for bf16/f16, and ~nothing for int8 (byte-unpack bound).

package metric

import (
	"math"
	"os"

	"simd/archsimd"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// hasAVX2 gates the middle tier. AVX512 implies AVX2, so init() checks hasAVX512
// first; this only decides AVX2-vs-scalar on non-AVX512 CPUs.
// TESTING-ONLY override: set MO_METRIC_NO_AVX2=1 (typically with
// MO_METRIC_NO_AVX512=1) to force the pure-Go scalar fallback for coverage.
var hasAVX2 = archsimd.X86.AVX2() && os.Getenv("MO_METRIC_NO_AVX2") == ""

func sumF32x8(v archsimd.Float32x8) float32 {
	var a [8]float32
	v.StoreArray(&a)
	return ((a[0] + a[1]) + (a[2] + a[3])) + ((a[4] + a[5]) + (a[6] + a[7]))
}

func sumI32x8(v archsimd.Int32x8) int64 {
	var a [8]int32
	v.StoreArray(&a)
	var s int64
	for _, x := range a {
		s += int64(x)
	}
	return s
}

// ---- bf16 (AVX2) ----

func l2sqBF16AVX2(a, b []types.BF16) (float64, error) {
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	n := len(a)
	au, bu := bf16AsU32(a), bf16AsU32(b)
	hi := archsimd.BroadcastUint32x8(0xFFFF0000)
	acc0, acc1 := archsimd.Float32x8{}, archsimd.Float32x8{}
	np, j := len(au), 0
	for ; j <= np-8; j += 8 {
		ua := archsimd.LoadUint32x8(au[j : j+8])
		ub := archsimd.LoadUint32x8(bu[j : j+8])
		dE := ua.ShiftAllLeft(16).AsFloat32x8().Sub(ub.ShiftAllLeft(16).AsFloat32x8())
		dO := ua.And(hi).AsFloat32x8().Sub(ub.And(hi).AsFloat32x8())
		acc0 = dE.MulAdd(dE, acc0)
		acc1 = dO.MulAdd(dO, acc1)
	}
	sum := sumF32x8(acc0.Add(acc1))
	for i := j * 2; i < n; i++ {
		d := a[i].ToFloat32() - b[i].ToFloat32()
		sum += d * d
	}
	return float64(sum), nil
}

func innerProductBF16AVX2(a, b []types.BF16) (float64, error) {
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	n := len(a)
	au, bu := bf16AsU32(a), bf16AsU32(b)
	hi := archsimd.BroadcastUint32x8(0xFFFF0000)
	acc0, acc1 := archsimd.Float32x8{}, archsimd.Float32x8{}
	np, j := len(au), 0
	for ; j <= np-8; j += 8 {
		ua := archsimd.LoadUint32x8(au[j : j+8])
		ub := archsimd.LoadUint32x8(bu[j : j+8])
		acc0 = ua.ShiftAllLeft(16).AsFloat32x8().MulAdd(ub.ShiftAllLeft(16).AsFloat32x8(), acc0)
		acc1 = ua.And(hi).AsFloat32x8().MulAdd(ub.And(hi).AsFloat32x8(), acc1)
	}
	sum := sumF32x8(acc0.Add(acc1))
	for i := j * 2; i < n; i++ {
		sum += a[i].ToFloat32() * b[i].ToFloat32()
	}
	return recoverInnerProductBF16(float64(-sum), a, b), nil
}

func l1DistanceBF16AVX2(a, b []types.BF16) (float64, error) {
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	n := len(a)
	au, bu := bf16AsU32(a), bf16AsU32(b)
	hi := archsimd.BroadcastUint32x8(0xFFFF0000)
	absMask := archsimd.BroadcastUint32x8(0x7FFFFFFF)
	acc0, acc1 := archsimd.Float32x8{}, archsimd.Float32x8{}
	np, j := len(au), 0
	for ; j <= np-8; j += 8 {
		ua := archsimd.LoadUint32x8(au[j : j+8])
		ub := archsimd.LoadUint32x8(bu[j : j+8])
		dE := ua.ShiftAllLeft(16).AsFloat32x8().Sub(ub.ShiftAllLeft(16).AsFloat32x8())
		dO := ua.And(hi).AsFloat32x8().Sub(ub.And(hi).AsFloat32x8())
		acc0 = acc0.Add(dE.AsUint32x8().And(absMask).AsFloat32x8())
		acc1 = acc1.Add(dO.AsUint32x8().And(absMask).AsFloat32x8())
	}
	sum := sumF32x8(acc0.Add(acc1))
	for i := j * 2; i < n; i++ {
		d := a[i].ToFloat32() - b[i].ToFloat32()
		if d < 0 {
			d = -d
		}
		sum += d
	}
	return float64(sum), nil
}

func cosineDistanceBF16AVX2(a, b []types.BF16) (float64, error) {
	if len(a) == 0 {
		return 0, nil
	}
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	n := len(a)
	au, bu := bf16AsU32(a), bf16AsU32(b)
	hi := archsimd.BroadcastUint32x8(0xFFFF0000)
	dot0, dot1 := archsimd.Float32x8{}, archsimd.Float32x8{}
	na0, na1 := archsimd.Float32x8{}, archsimd.Float32x8{}
	nb0, nb1 := archsimd.Float32x8{}, archsimd.Float32x8{}
	np, j := len(au), 0
	for ; j <= np-8; j += 8 {
		ua := archsimd.LoadUint32x8(au[j : j+8])
		ub := archsimd.LoadUint32x8(bu[j : j+8])
		aE := ua.ShiftAllLeft(16).AsFloat32x8()
		aO := ua.And(hi).AsFloat32x8()
		bE := ub.ShiftAllLeft(16).AsFloat32x8()
		bO := ub.And(hi).AsFloat32x8()
		dot0 = aE.MulAdd(bE, dot0)
		dot1 = aO.MulAdd(bO, dot1)
		na0 = aE.MulAdd(aE, na0)
		na1 = aO.MulAdd(aO, na1)
		nb0 = bE.MulAdd(bE, nb0)
		nb1 = bO.MulAdd(bO, nb1)
	}
	dot := sumF32x8(dot0.Add(dot1))
	na2 := sumF32x8(na0.Add(na1))
	nb2 := sumF32x8(nb0.Add(nb1))
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

// ---- int8 (AVX2), integer-exact ----

// l2sqInt8AVX2 is the 256-bit twin of l2sqInt8SIMD. One 32-byte load per
// operand is split into low/high halves and widened to int16, so a single
// VPMADDWD covers each half -- replacing the 4-way unpack to Int32x8 plus
// 32-bit VPMULLD the previous version used.
func l2sqInt8AVX2(a, b []int8) (float64, error) {
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	n := len(a)
	acc := archsimd.Int32x8{}
	i := 0
	for ; i <= n-32; i += 32 {
		va := archsimd.LoadInt8x32(a[i : i+32])
		vb := archsimd.LoadInt8x32(b[i : i+32])
		dlo := va.GetLo().ExtendToInt16().Sub(vb.GetLo().ExtendToInt16())
		dhi := va.GetHi().ExtendToInt16().Sub(vb.GetHi().ExtendToInt16())
		acc = acc.Add(dlo.DotProductPairs(dlo)).Add(dhi.DotProductPairs(dhi))
	}
	sum := sumI32x8(acc)
	for ; i < n; i++ {
		d := int32(a[i]) - int32(b[i])
		sum += int64(d * d)
	}
	return float64(sum), nil
}

// innerProductInt8AVX2 accumulates the dot product with 256-bit VPMADDWD,
// 32 elements per iteration.
func innerProductInt8AVX2(a, b []int8) (float64, error) {
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	n := len(a)
	acc := archsimd.Int32x8{}
	i := 0
	for ; i <= n-32; i += 32 {
		va := archsimd.LoadInt8x32(a[i : i+32])
		vb := archsimd.LoadInt8x32(b[i : i+32])
		lo := va.GetLo().ExtendToInt16().DotProductPairs(vb.GetLo().ExtendToInt16())
		hi := va.GetHi().ExtendToInt16().DotProductPairs(vb.GetHi().ExtendToInt16())
		acc = acc.Add(lo).Add(hi)
	}
	sum := sumI32x8(acc)
	for ; i < n; i++ {
		sum += int64(int32(a[i]) * int32(b[i]))
	}
	return float64(-sum), nil
}

// l1DistanceInt8AVX2 uses 256-bit VPSADBW with the same XOR 0x80 bias as the
// AVX-512 kernel: (a+128)-(b+128) == a-b, so absolute differences are exact.
func l1DistanceInt8AVX2(a, b []int8) (float64, error) {
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	n := len(a)
	bias := archsimd.BroadcastUint8x32(0x80)
	var acc archsimd.Uint64x4
	i := 0
	for ; i <= n-32; i += 32 {
		va := archsimd.LoadInt8x32(a[i : i+32]).AsUint8x32().Xor(bias)
		vb := archsimd.LoadInt8x32(b[i : i+32]).AsUint8x32().Xor(bias)
		acc = acc.Add(va.SumOf8AbsDiff(vb))
	}
	var lanes [4]uint64
	acc.StoreArray(&lanes)
	var usum uint64
	for _, v := range lanes {
		usum += v
	}
	sum := int64(usum)
	for ; i < n; i++ {
		d := int32(a[i]) - int32(b[i])
		if d < 0 {
			d = -d
		}
		sum += int64(d)
	}
	return float64(sum), nil
}

// cosineDistanceInt8AVX2 accumulates dot, |a|^2 and |b|^2 with 256-bit VPMADDWD.
func cosineDistanceInt8AVX2(a, b []int8) (float64, error) {
	if len(a) == 0 {
		return 0, nil
	}
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	n := len(a)
	dotA, naA, nbA := archsimd.Int32x8{}, archsimd.Int32x8{}, archsimd.Int32x8{}
	i := 0
	for ; i <= n-32; i += 32 {
		va := archsimd.LoadInt8x32(a[i : i+32])
		vb := archsimd.LoadInt8x32(b[i : i+32])
		alo, ahi := va.GetLo().ExtendToInt16(), va.GetHi().ExtendToInt16()
		blo, bhi := vb.GetLo().ExtendToInt16(), vb.GetHi().ExtendToInt16()
		dotA = dotA.Add(alo.DotProductPairs(blo)).Add(ahi.DotProductPairs(bhi))
		naA = naA.Add(alo.DotProductPairs(alo)).Add(ahi.DotProductPairs(ahi))
		nbA = nbA.Add(blo.DotProductPairs(blo)).Add(bhi.DotProductPairs(bhi))
	}
	dot, na2, nb2 := sumI32x8(dotA), sumI32x8(naA), sumI32x8(nbA)
	for ; i < n; i++ {
		ai8, bi8 := int64(a[i]), int64(b[i])
		dot += ai8 * bi8
		na2 += ai8 * ai8
		nb2 += bi8 * bi8
	}
	denom := math.Sqrt(float64(na2)) * math.Sqrt(float64(nb2))
	if denom == 0 {
		return 1.0, nil
	}
	return cosineDistClamped(float64(dot), denom), nil
}

// ---- f16 (AVX2) ----

func f16decX8(h, m7fff, m8000, mInf archsimd.Uint32x8, magic, infNan archsimd.Float32x8) archsimd.Float32x8 {
	o := h.And(m7fff).ShiftAllLeft(13)
	of := o.AsFloat32x8().Mul(magic)
	ou := of.AsUint32x8()
	ou = ou.Or(mInf).Merge(ou, of.GreaterEqual(infNan))
	return ou.Or(h.And(m8000).ShiftAllLeft(16)).AsFloat32x8()
}

func f16DecodeConstsX8() (m7fff, m8000, mLo, mInf archsimd.Uint32x8, magic, infNan archsimd.Float32x8) {
	m7fff = archsimd.BroadcastUint32x8(0x7fff)
	m8000 = archsimd.BroadcastUint32x8(0x8000)
	mLo = archsimd.BroadcastUint32x8(0xffff)
	mInf = archsimd.BroadcastUint32x8(255 << 23)
	magic = archsimd.BroadcastFloat32x8(f16Magic)
	infNan = archsimd.BroadcastFloat32x8(f16WasInfNan)
	return
}

func l2sqF16AVX2(a, b []types.Float16) (float64, error) {
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	n := len(a)
	au, bu := f16AsU32(a), f16AsU32(b)
	m7fff, m8000, mLo, mInf, magic, infNan := f16DecodeConstsX8()
	acc0, acc1 := archsimd.Float32x8{}, archsimd.Float32x8{}
	np, j := len(au), 0
	for ; j <= np-8; j += 8 {
		ua := archsimd.LoadUint32x8(au[j : j+8])
		ub := archsimd.LoadUint32x8(bu[j : j+8])
		dE := f16decX8(ua.And(mLo), m7fff, m8000, mInf, magic, infNan).Sub(f16decX8(ub.And(mLo), m7fff, m8000, mInf, magic, infNan))
		dO := f16decX8(ua.ShiftAllRight(16), m7fff, m8000, mInf, magic, infNan).Sub(f16decX8(ub.ShiftAllRight(16), m7fff, m8000, mInf, magic, infNan))
		acc0 = dE.MulAdd(dE, acc0)
		acc1 = dO.MulAdd(dO, acc1)
	}
	sum := sumF32x8(acc0.Add(acc1))
	for i := j * 2; i < n; i++ {
		d := f16fast(a[i]) - f16fast(b[i])
		sum += d * d
	}
	return float64(sum), nil
}

func innerProductF16AVX2(a, b []types.Float16) (float64, error) {
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	n := len(a)
	au, bu := f16AsU32(a), f16AsU32(b)
	m7fff, m8000, mLo, mInf, magic, infNan := f16DecodeConstsX8()
	acc0, acc1 := archsimd.Float32x8{}, archsimd.Float32x8{}
	np, j := len(au), 0
	for ; j <= np-8; j += 8 {
		ua := archsimd.LoadUint32x8(au[j : j+8])
		ub := archsimd.LoadUint32x8(bu[j : j+8])
		acc0 = f16decX8(ua.And(mLo), m7fff, m8000, mInf, magic, infNan).MulAdd(f16decX8(ub.And(mLo), m7fff, m8000, mInf, magic, infNan), acc0)
		acc1 = f16decX8(ua.ShiftAllRight(16), m7fff, m8000, mInf, magic, infNan).MulAdd(f16decX8(ub.ShiftAllRight(16), m7fff, m8000, mInf, magic, infNan), acc1)
	}
	sum := sumF32x8(acc0.Add(acc1))
	for i := j * 2; i < n; i++ {
		sum += f16fast(a[i]) * f16fast(b[i])
	}
	return recoverInnerProductF16(float64(-sum), a, b), nil
}

func l1DistanceF16AVX2(a, b []types.Float16) (float64, error) {
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	n := len(a)
	au, bu := f16AsU32(a), f16AsU32(b)
	m7fff, m8000, mLo, mInf, magic, infNan := f16DecodeConstsX8()
	absMask := archsimd.BroadcastUint32x8(0x7fffffff)
	acc0, acc1 := archsimd.Float32x8{}, archsimd.Float32x8{}
	np, j := len(au), 0
	for ; j <= np-8; j += 8 {
		ua := archsimd.LoadUint32x8(au[j : j+8])
		ub := archsimd.LoadUint32x8(bu[j : j+8])
		dE := f16decX8(ua.And(mLo), m7fff, m8000, mInf, magic, infNan).Sub(f16decX8(ub.And(mLo), m7fff, m8000, mInf, magic, infNan))
		dO := f16decX8(ua.ShiftAllRight(16), m7fff, m8000, mInf, magic, infNan).Sub(f16decX8(ub.ShiftAllRight(16), m7fff, m8000, mInf, magic, infNan))
		acc0 = acc0.Add(dE.AsUint32x8().And(absMask).AsFloat32x8())
		acc1 = acc1.Add(dO.AsUint32x8().And(absMask).AsFloat32x8())
	}
	sum := sumF32x8(acc0.Add(acc1))
	for i := j * 2; i < n; i++ {
		d := f16fast(a[i]) - f16fast(b[i])
		if d < 0 {
			d = -d
		}
		sum += d
	}
	return float64(sum), nil
}

func cosineDistanceF16AVX2(a, b []types.Float16) (float64, error) {
	if len(a) == 0 {
		return 0, nil
	}
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	n := len(a)
	au, bu := f16AsU32(a), f16AsU32(b)
	m7fff, m8000, mLo, mInf, magic, infNan := f16DecodeConstsX8()
	dot0, dot1 := archsimd.Float32x8{}, archsimd.Float32x8{}
	na0, na1 := archsimd.Float32x8{}, archsimd.Float32x8{}
	nb0, nb1 := archsimd.Float32x8{}, archsimd.Float32x8{}
	np, j := len(au), 0
	for ; j <= np-8; j += 8 {
		ua := archsimd.LoadUint32x8(au[j : j+8])
		ub := archsimd.LoadUint32x8(bu[j : j+8])
		aE := f16decX8(ua.And(mLo), m7fff, m8000, mInf, magic, infNan)
		aO := f16decX8(ua.ShiftAllRight(16), m7fff, m8000, mInf, magic, infNan)
		bE := f16decX8(ub.And(mLo), m7fff, m8000, mInf, magic, infNan)
		bO := f16decX8(ub.ShiftAllRight(16), m7fff, m8000, mInf, magic, infNan)
		dot0 = aE.MulAdd(bE, dot0)
		dot1 = aO.MulAdd(bO, dot1)
		na0 = aE.MulAdd(aE, na0)
		na1 = aO.MulAdd(aO, na1)
		nb0 = bE.MulAdd(bE, nb0)
		nb1 = bO.MulAdd(bO, nb1)
	}
	dot := sumF32x8(dot0.Add(dot1))
	na2 := sumF32x8(na0.Add(na1))
	nb2 := sumF32x8(nb0.Add(nb1))
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
