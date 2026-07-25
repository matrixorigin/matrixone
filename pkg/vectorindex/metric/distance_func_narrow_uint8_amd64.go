//go:build amd64 && go1.26 && goexperiment.simd

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

// AVX-512 / AVX2 SIMD distance kernels for vecuint8 ([]uint8), INTEGER-EXACT
// (bit-for-bit identical to the int64-accumulating pure-Go oracle).
//
// Like the int8 kernels we load the raw bytes as a 32-bit-lane vector (4 uint8
// per lane) and split the four byte lanes. The ONLY difference from int8 is the
// unpack: uint8 is ZERO-extended (mask each byte with 0xFF / logical shift),
// whereas int8 sign-extends with arithmetic shifts. The values land in [0,255]
// so reinterpreting the masked Uint32 lanes as Int32 (AsInt32x16) is exact, and
// all subsequent arithmetic (Sub/Mul/Add/Max) stays in signed int32 lanes —
// d=a-b is in [-255,255], d*d <= 65025, a*b in [0,65025]. For the max dimension
// (65535) a lane accumulates < 1100 terms each <= 65025, i.e. < 2^28, far under
// 2^31; the final horizontal reduction is in int64. No float, so results equal
// the oracle exactly (the equivalence test asserts == for L2sq/IP/L1).
//
// The pure-Go kernels in distance_func_narrow_uint8.go stay the fallback
// (non-AVX2 CPUs) and the equivalence oracle; init() only swaps the selection
// vars when AVX-512 / AVX2 is present. hasAVX512 / hasAVX2 / sumI32x16 /
// sumI32x8 are shared with the int8/bf16/f16 kernels (same package + build tag).

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
		uint8L2sqFn = l2sqUint8SIMD
		uint8IPFn = innerProductUint8SIMD
		uint8CosineFn = cosineDistanceUint8SIMD
		uint8L1Fn = l1DistanceUint8SIMD
	case hasAVX2:
		uint8L2sqFn = l2sqUint8AVX2
		uint8IPFn = innerProductUint8AVX2
		uint8CosineFn = cosineDistanceUint8AVX2
		uint8L1Fn = l1DistanceUint8AVX2
	}
}

// uint8AsU32 reinterprets a []uint8 as []uint32 viewing its first len/4 dwords
// (4 packed uint8 per lane).
func uint8AsU32(s []uint8) []uint32 {
	if len(s) < 4 {
		return nil
	}
	return unsafe.Slice((*uint32)(unsafe.Pointer(unsafe.SliceData(s))), len(s)/4)
}

// ---- uint8 (AVX-512), integer-exact ----

// unpackU8 zero-extends the four byte lanes of a Uint32x16 (64 packed uint8)
// into four Int32x16 vectors. Lane k of vJ holds uint8[4k+J] as a value in
// [0,255]. mask is BroadcastUint32x16(0xFF); the top byte (>>24) needs no mask.
func unpackU8(u, mask archsimd.Uint32x16) (v0, v1, v2, v3 archsimd.Int32x16) {
	v0 = u.And(mask).AsInt32x16()
	v1 = u.ShiftAllRight(8).And(mask).AsInt32x16()
	v2 = u.ShiftAllRight(16).And(mask).AsInt32x16()
	v3 = u.ShiftAllRight(24).AsInt32x16()
	return
}

func l2sqUint8SIMD(a, b []uint8) (float64, error) {
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	n := len(a)
	au, bu := uint8AsU32(a), uint8AsU32(b)
	mask := archsimd.BroadcastUint32x16(0xFF)
	acc := archsimd.Int32x16{}
	nq, j := len(au), 0
	for ; j <= nq-16; j += 16 {
		a0, a1, a2, a3 := unpackU8(archsimd.LoadUint32x16Slice(au[j:j+16]), mask)
		b0, b1, b2, b3 := unpackU8(archsimd.LoadUint32x16Slice(bu[j:j+16]), mask)
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

func innerProductUint8SIMD(a, b []uint8) (float64, error) {
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	n := len(a)
	au, bu := uint8AsU32(a), uint8AsU32(b)
	mask := archsimd.BroadcastUint32x16(0xFF)
	acc := archsimd.Int32x16{}
	nq, j := len(au), 0
	for ; j <= nq-16; j += 16 {
		a0, a1, a2, a3 := unpackU8(archsimd.LoadUint32x16Slice(au[j:j+16]), mask)
		b0, b1, b2, b3 := unpackU8(archsimd.LoadUint32x16Slice(bu[j:j+16]), mask)
		acc = acc.Add(a0.Mul(b0).Add(a1.Mul(b1)).Add(a2.Mul(b2).Add(a3.Mul(b3))))
	}
	sum := sumI32x16(acc)
	for i := j * 4; i < n; i++ {
		sum += int64(int32(a[i]) * int32(b[i]))
	}
	return float64(-sum), nil
}

func l1DistanceUint8SIMD(a, b []uint8) (float64, error) {
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	n := len(a)
	au, bu := uint8AsU32(a), uint8AsU32(b)
	mask := archsimd.BroadcastUint32x16(0xFF)
	zero := archsimd.Int32x16{}
	acc := archsimd.Int32x16{}
	abs := func(d archsimd.Int32x16) archsimd.Int32x16 { return d.Max(zero.Sub(d)) }
	nq, j := len(au), 0
	for ; j <= nq-16; j += 16 {
		a0, a1, a2, a3 := unpackU8(archsimd.LoadUint32x16Slice(au[j:j+16]), mask)
		b0, b1, b2, b3 := unpackU8(archsimd.LoadUint32x16Slice(bu[j:j+16]), mask)
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

func cosineDistanceUint8SIMD(a, b []uint8) (float64, error) {
	if len(a) == 0 {
		return 0, nil
	}
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	n := len(a)
	au, bu := uint8AsU32(a), uint8AsU32(b)
	mask := archsimd.BroadcastUint32x16(0xFF)
	dotA, naA, nbA := archsimd.Int32x16{}, archsimd.Int32x16{}, archsimd.Int32x16{}
	nq, j := len(au), 0
	for ; j <= nq-16; j += 16 {
		a0, a1, a2, a3 := unpackU8(archsimd.LoadUint32x16Slice(au[j:j+16]), mask)
		b0, b1, b2, b3 := unpackU8(archsimd.LoadUint32x16Slice(bu[j:j+16]), mask)
		dotA = dotA.Add(a0.Mul(b0).Add(a1.Mul(b1)).Add(a2.Mul(b2).Add(a3.Mul(b3))))
		naA = naA.Add(a0.Mul(a0).Add(a1.Mul(a1)).Add(a2.Mul(a2).Add(a3.Mul(a3))))
		nbA = nbA.Add(b0.Mul(b0).Add(b1.Mul(b1)).Add(b2.Mul(b2).Add(b3.Mul(b3))))
	}
	dot, na2, nb2 := sumI32x16(dotA), sumI32x16(naA), sumI32x16(nbA)
	for i := j * 4; i < n; i++ {
		ai, bi := int64(a[i]), int64(b[i])
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

// ---- uint8 (AVX2), integer-exact ----

// unpackU8x8 is the 256-bit (8-lane) twin of unpackU8.
func unpackU8x8(u, mask archsimd.Uint32x8) (v0, v1, v2, v3 archsimd.Int32x8) {
	v0 = u.And(mask).AsInt32x8()
	v1 = u.ShiftAllRight(8).And(mask).AsInt32x8()
	v2 = u.ShiftAllRight(16).And(mask).AsInt32x8()
	v3 = u.ShiftAllRight(24).AsInt32x8()
	return
}

func l2sqUint8AVX2(a, b []uint8) (float64, error) {
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	n := len(a)
	au, bu := uint8AsU32(a), uint8AsU32(b)
	mask := archsimd.BroadcastUint32x8(0xFF)
	acc := archsimd.Int32x8{}
	nq, j := len(au), 0
	for ; j <= nq-8; j += 8 {
		a0, a1, a2, a3 := unpackU8x8(archsimd.LoadUint32x8Slice(au[j:j+8]), mask)
		b0, b1, b2, b3 := unpackU8x8(archsimd.LoadUint32x8Slice(bu[j:j+8]), mask)
		d0, d1, d2, d3 := a0.Sub(b0), a1.Sub(b1), a2.Sub(b2), a3.Sub(b3)
		acc = acc.Add(d0.Mul(d0).Add(d1.Mul(d1)).Add(d2.Mul(d2).Add(d3.Mul(d3))))
	}
	sum := sumI32x8(acc)
	for i := j * 4; i < n; i++ {
		d := int32(a[i]) - int32(b[i])
		sum += int64(d * d)
	}
	return float64(sum), nil
}

func innerProductUint8AVX2(a, b []uint8) (float64, error) {
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	n := len(a)
	au, bu := uint8AsU32(a), uint8AsU32(b)
	mask := archsimd.BroadcastUint32x8(0xFF)
	acc := archsimd.Int32x8{}
	nq, j := len(au), 0
	for ; j <= nq-8; j += 8 {
		a0, a1, a2, a3 := unpackU8x8(archsimd.LoadUint32x8Slice(au[j:j+8]), mask)
		b0, b1, b2, b3 := unpackU8x8(archsimd.LoadUint32x8Slice(bu[j:j+8]), mask)
		acc = acc.Add(a0.Mul(b0).Add(a1.Mul(b1)).Add(a2.Mul(b2).Add(a3.Mul(b3))))
	}
	sum := sumI32x8(acc)
	for i := j * 4; i < n; i++ {
		sum += int64(int32(a[i]) * int32(b[i]))
	}
	return float64(-sum), nil
}

func l1DistanceUint8AVX2(a, b []uint8) (float64, error) {
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	n := len(a)
	au, bu := uint8AsU32(a), uint8AsU32(b)
	mask := archsimd.BroadcastUint32x8(0xFF)
	zero := archsimd.Int32x8{}
	acc := archsimd.Int32x8{}
	abs := func(d archsimd.Int32x8) archsimd.Int32x8 { return d.Max(zero.Sub(d)) }
	nq, j := len(au), 0
	for ; j <= nq-8; j += 8 {
		a0, a1, a2, a3 := unpackU8x8(archsimd.LoadUint32x8Slice(au[j:j+8]), mask)
		b0, b1, b2, b3 := unpackU8x8(archsimd.LoadUint32x8Slice(bu[j:j+8]), mask)
		acc = acc.Add(abs(a0.Sub(b0)).Add(abs(a1.Sub(b1))).Add(abs(a2.Sub(b2)).Add(abs(a3.Sub(b3)))))
	}
	sum := sumI32x8(acc)
	for i := j * 4; i < n; i++ {
		d := int32(a[i]) - int32(b[i])
		if d < 0 {
			d = -d
		}
		sum += int64(d)
	}
	return float64(sum), nil
}

func cosineDistanceUint8AVX2(a, b []uint8) (float64, error) {
	if len(a) == 0 {
		return 0, nil
	}
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	n := len(a)
	au, bu := uint8AsU32(a), uint8AsU32(b)
	mask := archsimd.BroadcastUint32x8(0xFF)
	dotA, naA, nbA := archsimd.Int32x8{}, archsimd.Int32x8{}, archsimd.Int32x8{}
	nq, j := len(au), 0
	for ; j <= nq-8; j += 8 {
		a0, a1, a2, a3 := unpackU8x8(archsimd.LoadUint32x8Slice(au[j:j+8]), mask)
		b0, b1, b2, b3 := unpackU8x8(archsimd.LoadUint32x8Slice(bu[j:j+8]), mask)
		dotA = dotA.Add(a0.Mul(b0).Add(a1.Mul(b1)).Add(a2.Mul(b2).Add(a3.Mul(b3))))
		naA = naA.Add(a0.Mul(a0).Add(a1.Mul(a1)).Add(a2.Mul(a2).Add(a3.Mul(a3))))
		nbA = nbA.Add(b0.Mul(b0).Add(b1.Mul(b1)).Add(b2.Mul(b2).Add(b3.Mul(b3))))
	}
	dot, na2, nb2 := sumI32x8(dotA), sumI32x8(naA), sumI32x8(nbA)
	for i := j * 4; i < n; i++ {
		ai, bi := int64(a[i]), int64(b[i])
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
