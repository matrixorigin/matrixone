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

// ARM64 NEON distance kernels for vecint8 ([]int8), INTEGER-EXACT (bit-for-bit
// identical to the int64-accumulating pure-Go oracle).
//
// Same shape as distance_func_narrow_int8_amd64.go, but deliberately NOT the
// same unpack. amd64 reinterprets the bytes as 32-bit lanes and peels them apart
// with shifts; ported literally that was measurably slower than not vectorizing
// at all (see unpackI8 below). Here we load an Int8x16 straight from the []int8
// and widen with NEON's native SXTL/SXTL2, which also drops the unsafe
// reinterpret the amd64 file needs.
//
// All arithmetic stays in int32 lanes — exact, since for the max dimension
// (65535) a lane accumulates < 16384 terms each <= 255^2, still under 2^31 —
// and the final horizontal reduction is in int64. No float, so results equal
// the oracle exactly (the int8 equivalence test asserts ==, not approx).
//
// The pure-Go kernels in distance_func_narrow.go stay the fallback and the
// equivalence oracle.

package metric

import (
	"math"

	"simd/archsimd"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

func init() {
	if hasNeon {
		int8L2sqFn = l2sqInt8SIMD
		int8IPFn = innerProductInt8SIMD
		int8CosineFn = cosineDistanceInt8SIMD
		int8L1Fn = l1DistanceInt8SIMD
	}
}

// sumI32x4 horizontally adds the 4 int32 lanes into an int64. Int32x4 does have
// a native ReduceSum, but it returns int32; lane values stay well under 2^31
// while their total need not, so the reduction is done in int64 instead.
func sumI32x4(v archsimd.Int32x4) int64 {
	var a [4]int32
	v.StoreArray(&a)
	return int64(a[0]) + int64(a[1]) + int64(a[2]) + int64(a[3])
}

// unpackI8 sign-extends an Int8x16 into four Int32x4 vectors via NEON's native
// widening SXTL, vJ holding elements [4J..4J+3].
//
// The amd64 kernel instead reinterprets the bytes as 32-bit lanes and peels them
// apart with (u << 24-8J) >>arith 24. That shape is a trap here: archsimd lowers
// each ShiftAll* to VDUP+VSSHL, and the VDUP of the shift amount is NOT hoisted
// out of the loop, so the shift-based unpack cost 14 VDUP + 14 VSSHL per
// iteration and measured ~19% SLOWER than the pure-Go scalar kernel. The
// widening form has no immediate to broadcast and wins comfortably.
//
// Note the lane order differs from the amd64 unpack (consecutive runs rather
// than a stride-4 interleave). That is immaterial: every kernel here is an
// order-independent sum, and a and b are unpacked identically, so element
// correspondence is preserved.
func unpackI8(v archsimd.Int8x16) (v0, v1, v2, v3 archsimd.Int32x4) {
	lo16 := v.ExtendLo8ToInt16()
	hi16 := v.HiToLo().ExtendLo8ToInt16()
	v0 = lo16.ExtendLo4ToInt32()
	v1 = lo16.HiToLo().ExtendLo4ToInt32()
	v2 = hi16.ExtendLo4ToInt32()
	v3 = hi16.HiToLo().ExtendLo4ToInt32()
	return
}

func l2sqInt8SIMD(a, b []int8) (float64, error) {
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	n := len(a)
	acc := archsimd.Int32x4{}
	j := 0
	for ; j <= n-16; j += 16 {
		a0, a1, a2, a3 := unpackI8(archsimd.LoadInt8x16(a[j : j+16]))
		b0, b1, b2, b3 := unpackI8(archsimd.LoadInt8x16(b[j : j+16]))
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

func innerProductInt8SIMD(a, b []int8) (float64, error) {
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	n := len(a)
	acc := archsimd.Int32x4{}
	j := 0
	for ; j <= n-16; j += 16 {
		a0, a1, a2, a3 := unpackI8(archsimd.LoadInt8x16(a[j : j+16]))
		b0, b1, b2, b3 := unpackI8(archsimd.LoadInt8x16(b[j : j+16]))
		acc = acc.Add(a0.Mul(b0).Add(a1.Mul(b1)).Add(a2.Mul(b2).Add(a3.Mul(b3))))
	}
	sum := sumI32x4(acc)
	for i := j; i < n; i++ {
		sum += int64(int32(a[i]) * int32(b[i]))
	}
	return float64(-sum), nil
}

func l1DistanceInt8SIMD(a, b []int8) (float64, error) {
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	n := len(a)
	zero := archsimd.Int32x4{}
	acc := archsimd.Int32x4{}
	abs := func(d archsimd.Int32x4) archsimd.Int32x4 { return d.Max(zero.Sub(d)) }
	j := 0
	for ; j <= n-16; j += 16 {
		a0, a1, a2, a3 := unpackI8(archsimd.LoadInt8x16(a[j : j+16]))
		b0, b1, b2, b3 := unpackI8(archsimd.LoadInt8x16(b[j : j+16]))
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

func cosineDistanceInt8SIMD(a, b []int8) (float64, error) {
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
		a0, a1, a2, a3 := unpackI8(archsimd.LoadInt8x16(a[j : j+16]))
		b0, b1, b2, b3 := unpackI8(archsimd.LoadInt8x16(b[j : j+16]))
		dotA = dotA.Add(a0.Mul(b0).Add(a1.Mul(b1)).Add(a2.Mul(b2).Add(a3.Mul(b3))))
		naA = naA.Add(a0.Mul(a0).Add(a1.Mul(a1)).Add(a2.Mul(a2).Add(a3.Mul(a3))))
		nbA = nbA.Add(b0.Mul(b0).Add(b1.Mul(b1)).Add(b2.Mul(b2).Add(b3.Mul(b3))))
	}
	dot, na2, nb2 := sumI32x4(dotA), sumI32x4(naA), sumI32x4(nbA)
	for i := j; i < n; i++ {
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
