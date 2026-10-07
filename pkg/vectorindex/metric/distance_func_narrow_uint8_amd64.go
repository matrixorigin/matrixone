//go:build amd64 && go1.27 && goexperiment.simd

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
// uint8 zero-extends to int16 (VPMOVZXBW) losslessly, so the signed VPMADDWD
// (Int16x32.DotProductPairs) used for L2sq/IP/cosine is exact: d=a-b is in
// [-255,255] and products are <= 65025. L1 uses VPSADBW
// (Uint8x64.SumOf8AbsDiff), which needs no bias for unsigned input.
//
// The pure-Go kernels in distance_func_narrow_uint8.go stay the fallback
// (non-AVX2 CPUs) and the equivalence oracle; init() only swaps the selection
// vars when AVX-512 / AVX2 is present. hasAVX512 / hasAVX2 / sumI32x16 /
// sumI32x8 are shared with the int8/bf16/f16 kernels (same package + build tag).
package metric

import (
	"math"

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

// ---- uint8 (AVX-512), integer-exact ----

// l2sqUint8SIMD computes sum (a-b)^2 with VPMADDWD. uint8 widens to int16
// losslessly (0..255), so the signed multiply is exact. See l2sqInt8SIMD for
// why the loop stride is 32 elements rather than 64.
func l2sqUint8SIMD(a, b []uint8) (float64, error) {
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	n := len(a)
	acc := archsimd.Int32x16{}
	i := 0
	for ; i <= n-32; i += 32 {
		va := archsimd.LoadUint8x32(a[i : i+32]).ExtendToUint16().AsInt16x32()
		vb := archsimd.LoadUint8x32(b[i : i+32]).ExtendToUint16().AsInt16x32()
		d := va.Sub(vb)
		acc = acc.Add(d.DotProductPairs(d))
	}
	sum := sumI32x16(acc)
	for ; i < n; i++ {
		d := int32(a[i]) - int32(b[i])
		sum += int64(d * d)
	}
	return float64(sum), nil
}

// innerProductUint8SIMD accumulates the dot product with VPMADDWD.
func innerProductUint8SIMD(a, b []uint8) (float64, error) {
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	n := len(a)
	acc := archsimd.Int32x16{}
	i := 0
	for ; i <= n-32; i += 32 {
		va := archsimd.LoadUint8x32(a[i : i+32]).ExtendToUint16().AsInt16x32()
		vb := archsimd.LoadUint8x32(b[i : i+32]).ExtendToUint16().AsInt16x32()
		acc = acc.Add(va.DotProductPairs(vb))
	}
	sum := sumI32x16(acc)
	for ; i < n; i++ {
		sum += int64(int32(a[i]) * int32(b[i]))
	}
	return float64(-sum), nil
}

// l1DistanceUint8SIMD computes sum|a-b| with VPSADBW, exposed as
// Uint8x64.SumOf8AbsDiff. That single instruction does the absolute difference
// AND the horizontal sum of each 8-byte group, so the loop is load/load/SAD/add
// -- about 4 SIMD ops per 64 elements.
//
// The previous implementation unpacked each 64-byte load into 4x Int32x16, then
// did 4 subtracts, 4 absolute values (each a Sub plus a Max) and a tree-add:
// roughly 20 ops for the same 64 elements. Measured on Zen5 at dim=1024, this
// version is 3.5x faster (51.0ns -> 14.5ns) and byte-identical on every input.
//
// Overflow is safe by construction rather than by luck: each SAD lane holds at
// most 8*255 = 2040, accumulating into Uint64x8, so a lane cannot overflow
// before ~9e15 iterations. No periodic flush to scalar is required.
func l1DistanceUint8SIMD(a, b []uint8) (float64, error) {
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	n := len(a)
	var acc archsimd.Uint64x8
	i := 0
	for ; i <= n-64; i += 64 {
		va := archsimd.LoadUint8x64(a[i : i+64])
		vb := archsimd.LoadUint8x64(b[i : i+64])
		acc = acc.Add(va.SumOf8AbsDiff(vb))
	}
	var lanes [8]uint64
	acc.StoreArray(&lanes)
	var sum uint64
	for _, v := range lanes {
		sum += v
	}
	for ; i < n; i++ {
		d := int32(a[i]) - int32(b[i])
		if d < 0 {
			d = -d
		}
		sum += uint64(d)
	}
	return float64(sum), nil
}

// cosineDistanceUint8SIMD accumulates dot, |a|^2 and |b|^2 with VPMADDWD.
// See cosineDistanceInt8SIMD for why the AVX2 twin currently measures faster.
func cosineDistanceUint8SIMD(a, b []uint8) (float64, error) {
	if len(a) == 0 {
		return 0, nil
	}
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	n := len(a)
	dotA, naA, nbA := archsimd.Int32x16{}, archsimd.Int32x16{}, archsimd.Int32x16{}
	i := 0
	for ; i <= n-32; i += 32 {
		va := archsimd.LoadUint8x32(a[i : i+32]).ExtendToUint16().AsInt16x32()
		vb := archsimd.LoadUint8x32(b[i : i+32]).ExtendToUint16().AsInt16x32()
		dotA = dotA.Add(va.DotProductPairs(vb))
		naA = naA.Add(va.DotProductPairs(va))
		nbA = nbA.Add(vb.DotProductPairs(vb))
	}
	dot, na2, nb2 := sumI32x16(dotA), sumI32x16(naA), sumI32x16(nbA)
	for ; i < n; i++ {
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

// l2sqUint8AVX2 is the 256-bit twin of l2sqUint8SIMD, 32 elements per iteration.
func l2sqUint8AVX2(a, b []uint8) (float64, error) {
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	n := len(a)
	acc := archsimd.Int32x8{}
	i := 0
	for ; i <= n-32; i += 32 {
		va := archsimd.LoadUint8x32(a[i : i+32])
		vb := archsimd.LoadUint8x32(b[i : i+32])
		dlo := va.GetLo().ExtendToUint16().AsInt16x16().Sub(vb.GetLo().ExtendToUint16().AsInt16x16())
		dhi := va.GetHi().ExtendToUint16().AsInt16x16().Sub(vb.GetHi().ExtendToUint16().AsInt16x16())
		acc = acc.Add(dlo.DotProductPairs(dlo)).Add(dhi.DotProductPairs(dhi))
	}
	sum := sumI32x8(acc)
	for ; i < n; i++ {
		d := int32(a[i]) - int32(b[i])
		sum += int64(d * d)
	}
	return float64(sum), nil
}

// innerProductUint8AVX2 accumulates the dot product with 256-bit VPMADDWD.
func innerProductUint8AVX2(a, b []uint8) (float64, error) {
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	n := len(a)
	acc := archsimd.Int32x8{}
	i := 0
	for ; i <= n-32; i += 32 {
		va := archsimd.LoadUint8x32(a[i : i+32])
		vb := archsimd.LoadUint8x32(b[i : i+32])
		lo := va.GetLo().ExtendToUint16().AsInt16x16().DotProductPairs(vb.GetLo().ExtendToUint16().AsInt16x16())
		hi := va.GetHi().ExtendToUint16().AsInt16x16().DotProductPairs(vb.GetHi().ExtendToUint16().AsInt16x16())
		acc = acc.Add(lo).Add(hi)
	}
	sum := sumI32x8(acc)
	for ; i < n; i++ {
		sum += int64(int32(a[i]) * int32(b[i]))
	}
	return float64(-sum), nil
}

// l1DistanceUint8AVX2 uses 256-bit VPSADBW.
func l1DistanceUint8AVX2(a, b []uint8) (float64, error) {
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	n := len(a)
	var acc archsimd.Uint64x4
	i := 0
	for ; i <= n-32; i += 32 {
		va := archsimd.LoadUint8x32(a[i : i+32])
		vb := archsimd.LoadUint8x32(b[i : i+32])
		acc = acc.Add(va.SumOf8AbsDiff(vb))
	}
	var lanes [4]uint64
	acc.StoreArray(&lanes)
	var sum uint64
	for _, v := range lanes {
		sum += v
	}
	for ; i < n; i++ {
		d := int32(a[i]) - int32(b[i])
		if d < 0 {
			d = -d
		}
		sum += uint64(d)
	}
	return float64(sum), nil
}

// cosineDistanceUint8AVX2 accumulates dot, |a|^2 and |b|^2 with 256-bit VPMADDWD.
func cosineDistanceUint8AVX2(a, b []uint8) (float64, error) {
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
		va := archsimd.LoadUint8x32(a[i : i+32])
		vb := archsimd.LoadUint8x32(b[i : i+32])
		alo := va.GetLo().ExtendToUint16().AsInt16x16()
		ahi := va.GetHi().ExtendToUint16().AsInt16x16()
		blo := vb.GetLo().ExtendToUint16().AsInt16x16()
		bhi := vb.GetHi().ExtendToUint16().AsInt16x16()
		dotA = dotA.Add(alo.DotProductPairs(blo)).Add(ahi.DotProductPairs(bhi))
		naA = naA.Add(alo.DotProductPairs(alo)).Add(ahi.DotProductPairs(ahi))
		nbA = nbA.Add(blo.DotProductPairs(blo)).Add(bhi.DotProductPairs(bhi))
	}
	dot, na2, nb2 := sumI32x8(dotA), sumI32x8(naA), sumI32x8(nbA)
	for ; i < n; i++ {
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
