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

// AVX-512 SIMD distance kernels for vecint8 ([]int8), INTEGER-EXACT (bit-for-bit
// identical to the int64-accumulating pure-Go oracle).
//
// The kernels widen int8 to int16 (VPMOVSXBW) and use VPMADDWD
// (Int16x32.DotProductPairs), which multiplies and pairwise-adds in one
// instruction, accumulating into int32 lanes. L1 instead uses VPSADBW
// (Uint8x64.SumOf8AbsDiff) over XOR 0x80 biased bytes.
//
// Everything stays in integer lanes, so results equal the oracle exactly (the
// equivalence tests assert ==, not approx). For the max dimension (65535) a
// lane accumulates well under 2^31, and the final horizontal reduction widens
// to int64.
package metric

import (
	"math"

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

// sumI32x16 horizontally adds the 16 int32 lanes into an int64 (lane values are
// bounded well under 2^31, but the 16-lane total can exceed it).
func sumI32x16(v archsimd.Int32x16) int64 {
	var a [16]int32
	v.StoreArray(&a)
	var s int64
	for _, x := range a {
		s += int64(x)
	}
	return s
}

// l2sqInt8SIMD computes sum (a-b)^2 with VPMADDWD (Int16x32.DotProductPairs).
// Widening to int16 keeps the difference exact (|a-b| <= 255) and lets one
// instruction do the multiply AND the pairwise add, replacing the 4-way unpack
// to Int32x16 plus 32-bit VPMULLD that the previous version needed.
//
// Measured: widening this loop to 64 elements/iteration (one 64-byte load split
// with GetLo/GetHi) was tried and was SLOWER -- VEXTRACTI64X4 on a zmm costs
// more than the extra 32-byte load it saves. The same applies to the IP and
// cosine kernels below, which is why all three load 32 elements at a time.
func l2sqInt8SIMD(a, b []int8) (float64, error) {
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	n := len(a)
	acc := archsimd.Int32x16{}
	i := 0
	for ; i <= n-32; i += 32 {
		va := archsimd.LoadInt8x32(a[i : i+32]).ExtendToInt16()
		vb := archsimd.LoadInt8x32(b[i : i+32]).ExtendToInt16()
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

// innerProductInt8SIMD accumulates the dot product with VPMADDWD; each
// instruction handles a multiply plus a pairwise add on int16 lanes.
func innerProductInt8SIMD(a, b []int8) (float64, error) {
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	n := len(a)
	acc := archsimd.Int32x16{}
	i := 0
	for ; i <= n-32; i += 32 {
		va := archsimd.LoadInt8x32(a[i : i+32]).ExtendToInt16()
		vb := archsimd.LoadInt8x32(b[i : i+32]).ExtendToInt16()
		acc = acc.Add(va.DotProductPairs(vb))
	}
	sum := sumI32x16(acc)
	for ; i < n; i++ {
		sum += int64(int32(a[i]) * int32(b[i]))
	}
	return float64(-sum), nil
}

// l1DistanceInt8SIMD computes sum|a-b| with VPSADBW, the same instruction the
// uint8 kernel uses. VPSADBW is unsigned-only, so each input is first biased by
// XOR 0x80, which reinterprets int8 as uint8 while preserving differences:
// (a+128) - (b+128) == a-b, so the absolute differences are unchanged. The bias
// is exact for every int8 value -- it is a relabelling, not an approximation.
func l1DistanceInt8SIMD(a, b []int8) (float64, error) {
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	n := len(a)
	bias := archsimd.BroadcastUint8x64(0x80)
	var acc archsimd.Uint64x8
	i := 0
	for ; i <= n-64; i += 64 {
		va := archsimd.LoadInt8x64(a[i : i+64]).AsUint8x64().Xor(bias)
		vb := archsimd.LoadInt8x64(b[i : i+64]).AsUint8x64().Xor(bias)
		acc = acc.Add(va.SumOf8AbsDiff(vb))
	}
	var lanes [8]uint64
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

// cosineDistanceInt8SIMD accumulates dot, |a|^2 and |b|^2 in one pass. The old
// form needed twelve 32-bit VPMULLD per 64 elements; this needs three VPMADDWD
// per 32 elements, the same multiply count at much lower latency.
//
// Measured on Zen5: the AVX2 twin of this kernel is FASTER than this one at
// every dimension tested (three sumI32x16 reductions per call cost more than
// the 512-bit width saves). init() still prefers AVX-512 -- re-measure on the
// target CPU before changing that, since the balance is microarchitecture
// specific.
func cosineDistanceInt8SIMD(a, b []int8) (float64, error) {
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
		va := archsimd.LoadInt8x32(a[i : i+32]).ExtendToInt16()
		vb := archsimd.LoadInt8x32(b[i : i+32]).ExtendToInt16()
		dotA = dotA.Add(va.DotProductPairs(vb))
		naA = naA.Add(va.DotProductPairs(va))
		nbA = nbA.Add(vb.DotProductPairs(vb))
	}
	dot, na2, nb2 := sumI32x16(dotA), sumI32x16(naA), sumI32x16(nbA)
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
