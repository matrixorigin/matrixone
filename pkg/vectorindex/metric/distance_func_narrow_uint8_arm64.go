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

// ARM64 NEON distance kernels for vecuint8 ([]uint8), INTEGER-EXACT (bit-for-bit
// identical to the int64-accumulating pure-Go oracle).
//
// Like the int8 kernels we load a 16-byte vector straight from the slice and
// widen it to four Int32x4. The ONLY difference from int8 is the unpack: uint8
// ZERO-extends (UXTL/UXTL2) where int8 sign-extends (SXTL/SXTL2). The values
// land in [0,255] so reinterpreting the widened Uint32 lanes as Int32
// (BitsToInt32) is exact, and all subsequent arithmetic (Sub/Mul/Add/Max) stays
// in signed int32 lanes — d=a-b is in [-255,255], d*d <= 65025, a*b in
// [0,65025]. For the max dimension (65535) a lane accumulates < 16384 terms each
// <= 65025, i.e. ~1.1e9, under 2^31; the final horizontal reduction is in int64.
//
// The pure-Go kernels in distance_func_narrow_uint8.go stay the fallback and the
// equivalence oracle. sumI32x4 is shared with the int8 kernels (same package +
// build tag).

package metric

import (
	"math"

	"simd/archsimd"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

func init() {
	if hasNeon {
		uint8L2sqFn = l2sqUint8SIMD
		uint8IPFn = innerProductUint8SIMD
		uint8CosineFn = cosineDistanceUint8SIMD
		uint8L1Fn = l1DistanceUint8SIMD
	}
}

// unpackU8 zero-extends a Uint8x16 into four Int32x4 vectors via NEON's native
// widening UXTL, vJ holding elements [4J..4J+3] as values in [0,255].
//
// See unpackI8 in distance_func_narrow_int8_arm64.go: the amd64 shift/mask form
// makes archsimd emit a VDUP per ShiftAll* inside the loop, and the widening
// form avoids that entirely. Lane order differs from amd64 but every kernel here
// is an order-independent sum over identically-unpacked a and b.
func unpackU8(v archsimd.Uint8x16) (v0, v1, v2, v3 archsimd.Int32x4) {
	lo16 := v.ExtendLo8ToUint16()
	hi16 := v.HiToLo().ExtendLo8ToUint16()
	v0 = lo16.ExtendLo4ToUint32().BitsToInt32()
	v1 = lo16.HiToLo().ExtendLo4ToUint32().BitsToInt32()
	v2 = hi16.ExtendLo4ToUint32().BitsToInt32()
	v3 = hi16.HiToLo().ExtendLo4ToUint32().BitsToInt32()
	return
}

func l2sqUint8SIMD(a, b []uint8) (float64, error) {
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	n := len(a)
	acc := archsimd.Int32x4{}
	j := 0
	for ; j <= n-16; j += 16 {
		a0, a1, a2, a3 := unpackU8(archsimd.LoadUint8x16(a[j : j+16]))
		b0, b1, b2, b3 := unpackU8(archsimd.LoadUint8x16(b[j : j+16]))
		d0, d1, d2, d3 := a0.Sub(b0), a1.Sub(b1), a2.Sub(b2), a3.Sub(b3)
		acc = acc.Add(d0.Mul(d0).Add(d1.Mul(d1)).Add(d2.Mul(d2).Add(d3.Mul(d3))))
	}
	sum := sumI32x4(acc)
	for i := j; i < n; i++ {
		d := int32(a[i]) - int32(b[i])
		sum += int64(d * d)
	}
	return float64(sum), nil
}

func innerProductUint8SIMD(a, b []uint8) (float64, error) {
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	n := len(a)
	acc := archsimd.Int32x4{}
	j := 0
	for ; j <= n-16; j += 16 {
		a0, a1, a2, a3 := unpackU8(archsimd.LoadUint8x16(a[j : j+16]))
		b0, b1, b2, b3 := unpackU8(archsimd.LoadUint8x16(b[j : j+16]))
		acc = acc.Add(a0.Mul(b0).Add(a1.Mul(b1)).Add(a2.Mul(b2).Add(a3.Mul(b3))))
	}
	sum := sumI32x4(acc)
	for i := j; i < n; i++ {
		sum += int64(int32(a[i]) * int32(b[i]))
	}
	return float64(-sum), nil
}

func l1DistanceUint8SIMD(a, b []uint8) (float64, error) {
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	n := len(a)
	zero := archsimd.Int32x4{}
	acc := archsimd.Int32x4{}
	abs := func(d archsimd.Int32x4) archsimd.Int32x4 { return d.Max(zero.Sub(d)) }
	j := 0
	for ; j <= n-16; j += 16 {
		a0, a1, a2, a3 := unpackU8(archsimd.LoadUint8x16(a[j : j+16]))
		b0, b1, b2, b3 := unpackU8(archsimd.LoadUint8x16(b[j : j+16]))
		acc = acc.Add(abs(a0.Sub(b0)).Add(abs(a1.Sub(b1))).Add(abs(a2.Sub(b2)).Add(abs(a3.Sub(b3)))))
	}
	sum := sumI32x4(acc)
	for i := j; i < n; i++ {
		d := int32(a[i]) - int32(b[i])
		if d < 0 {
			d = -d
		}
		sum += int64(d)
	}
	return float64(sum), nil
}

func cosineDistanceUint8SIMD(a, b []uint8) (float64, error) {
	if len(a) == 0 {
		return 0, nil
	}
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	n := len(a)
	dotA, naA, nbA := archsimd.Int32x4{}, archsimd.Int32x4{}, archsimd.Int32x4{}
	j := 0
	for ; j <= n-16; j += 16 {
		a0, a1, a2, a3 := unpackU8(archsimd.LoadUint8x16(a[j : j+16]))
		b0, b1, b2, b3 := unpackU8(archsimd.LoadUint8x16(b[j : j+16]))
		dotA = dotA.Add(a0.Mul(b0).Add(a1.Mul(b1)).Add(a2.Mul(b2).Add(a3.Mul(b3))))
		naA = naA.Add(a0.Mul(a0).Add(a1.Mul(a1)).Add(a2.Mul(a2).Add(a3.Mul(a3))))
		nbA = nbA.Add(b0.Mul(b0).Add(b1.Mul(b1)).Add(b2.Mul(b2).Add(b3.Mul(b3))))
	}
	dot, na2, nb2 := sumI32x4(dotA), sumI32x4(naA), sumI32x4(nbA)
	for i := j; i < n; i++ {
		au8, bu8 := int64(a[i]), int64(b[i])
		dot += au8 * bu8
		na2 += au8 * au8
		nb2 += bu8 * bu8
	}
	denom := math.Sqrt(float64(na2)) * math.Sqrt(float64(nb2))
	if denom == 0 {
		return 1.0, nil
	}
	return cosineDistClamped(float64(dot), denom), nil
}
