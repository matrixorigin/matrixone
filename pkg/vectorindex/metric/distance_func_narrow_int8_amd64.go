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

// AVX-512 SIMD distance kernels for vecint8 ([]int8), INTEGER-EXACT (bit-for-bit
// identical to the int64-accumulating pure-Go oracle).
//
// archsimd has no int8->int32 widening op, so we load the raw bytes as Int32x16
// (64 int8 per load) and sign-extend the four byte lanes with shifts:
// byteJ = (u << (24-8J)) >>arith 24 (Int32x16.ShiftAllRight is arithmetic). All
// arithmetic then stays in int32 lanes — exact, since for the max dimension
// (65535) a lane accumulates < 1024 terms each <= 255^2, far under 2^31 — and the
// final horizontal reduction is in int64. No float, so results equal the oracle
// exactly (the int8 equivalence test asserts ==, not approx).

package metric

import (
	"math"
	"unsafe"

	"simd/archsimd"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

func init() {
	switch {
	case hasAVX512:
		int8L2sqFn = l2sqInt8SIMD
		int8IPFn = innerProductInt8SIMD
		int8CosineFn = cosineDistanceInt8SIMD
		int8L1Fn = l1DistanceInt8SIMD
	case hasAVX2:
		int8L2sqFn = l2sqInt8AVX2
		int8IPFn = innerProductInt8AVX2
		int8CosineFn = cosineDistanceInt8AVX2
		int8L1Fn = l1DistanceInt8AVX2
	}
}

// int8AsI32 reinterprets a []int8 as []int32 viewing its first len/4 dwords.
func int8AsI32(s []int8) []int32 {
	if len(s) < 4 {
		return nil
	}
	return unsafe.Slice((*int32)(unsafe.Pointer(unsafe.SliceData(s))), len(s)/4)
}

// sumI32x16 horizontally adds the 16 int32 lanes into an int64 (lane values are
// bounded well under 2^31, but the 16-lane total can exceed it).
func sumI32x16(v archsimd.Int32x16) int64 {
	var a [16]int32
	v.Store(&a)
	var s int64
	for _, x := range a {
		s += int64(x)
	}
	return s
}

// unpackI8 sign-extends the four byte lanes of an Int32x16 (64 packed int8) into
// four Int32x16 vectors. Lane k of vJ holds int8[4k+J].
func unpackI8(u archsimd.Int32x16) (v0, v1, v2, v3 archsimd.Int32x16) {
	v0 = u.ShiftAllLeft(24).ShiftAllRight(24)
	v1 = u.ShiftAllLeft(16).ShiftAllRight(24)
	v2 = u.ShiftAllLeft(8).ShiftAllRight(24)
	v3 = u.ShiftAllRight(24)
	return
}

func l2sqInt8SIMD(a, b []int8) (float64, error) {
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	n := len(a)
	ai, bi := int8AsI32(a), int8AsI32(b)
	acc := archsimd.Int32x16{}
	nq, j := len(ai), 0
	for ; j <= nq-16; j += 16 {
		a0, a1, a2, a3 := unpackI8(archsimd.LoadInt32x16Slice(ai[j : j+16]))
		b0, b1, b2, b3 := unpackI8(archsimd.LoadInt32x16Slice(bi[j : j+16]))
		d0, d1, d2, d3 := a0.Sub(b0), a1.Sub(b1), a2.Sub(b2), a3.Sub(b3)
		acc = acc.Add(d0.Mul(d0).Add(d1.Mul(d1)).Add(d2.Mul(d2).Add(d3.Mul(d3))))
	}
	sum := sumI32x16(acc)
	for i := j * 4; i < n; i++ {
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
	ai, bi := int8AsI32(a), int8AsI32(b)
	acc := archsimd.Int32x16{}
	nq, j := len(ai), 0
	for ; j <= nq-16; j += 16 {
		a0, a1, a2, a3 := unpackI8(archsimd.LoadInt32x16Slice(ai[j : j+16]))
		b0, b1, b2, b3 := unpackI8(archsimd.LoadInt32x16Slice(bi[j : j+16]))
		acc = acc.Add(a0.Mul(b0).Add(a1.Mul(b1)).Add(a2.Mul(b2).Add(a3.Mul(b3))))
	}
	sum := sumI32x16(acc)
	for i := j * 4; i < n; i++ {
		sum += int64(int32(a[i]) * int32(b[i]))
	}
	return float64(-sum), nil
}

func l1DistanceInt8SIMD(a, b []int8) (float64, error) {
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	n := len(a)
	ai, bi := int8AsI32(a), int8AsI32(b)
	zero := archsimd.Int32x16{}
	acc := archsimd.Int32x16{}
	abs := func(d archsimd.Int32x16) archsimd.Int32x16 { return d.Max(zero.Sub(d)) }
	nq, j := len(ai), 0
	for ; j <= nq-16; j += 16 {
		a0, a1, a2, a3 := unpackI8(archsimd.LoadInt32x16Slice(ai[j : j+16]))
		b0, b1, b2, b3 := unpackI8(archsimd.LoadInt32x16Slice(bi[j : j+16]))
		acc = acc.Add(abs(a0.Sub(b0)).Add(abs(a1.Sub(b1))).Add(abs(a2.Sub(b2)).Add(abs(a3.Sub(b3)))))
	}
	sum := sumI32x16(acc)
	for i := j * 4; i < n; i++ {
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
	ai, bi := int8AsI32(a), int8AsI32(b)
	dotA, naA, nbA := archsimd.Int32x16{}, archsimd.Int32x16{}, archsimd.Int32x16{}
	nq, j := len(ai), 0
	for ; j <= nq-16; j += 16 {
		a0, a1, a2, a3 := unpackI8(archsimd.LoadInt32x16Slice(ai[j : j+16]))
		b0, b1, b2, b3 := unpackI8(archsimd.LoadInt32x16Slice(bi[j : j+16]))
		dotA = dotA.Add(a0.Mul(b0).Add(a1.Mul(b1)).Add(a2.Mul(b2).Add(a3.Mul(b3))))
		naA = naA.Add(a0.Mul(a0).Add(a1.Mul(a1)).Add(a2.Mul(a2).Add(a3.Mul(a3))))
		nbA = nbA.Add(b0.Mul(b0).Add(b1.Mul(b1)).Add(b2.Mul(b2).Add(b3.Mul(b3))))
	}
	dot, na2, nb2 := sumI32x16(dotA), sumI32x16(naA), sumI32x16(nbA)
	for i := j * 4; i < n; i++ {
		ai8, bi8 := int64(a[i]), int64(b[i])
		dot += ai8 * bi8
		na2 += ai8 * ai8
		nb2 += bi8 * bi8
	}
	denom := math.Sqrt(float64(na2)) * math.Sqrt(float64(nb2))
	if denom == 0 {
		return 1.0, nil
	}
	return 1.0 - float64(dot)/denom, nil
}
