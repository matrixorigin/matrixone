//go:build amd64 && go1.27 && goexperiment.simd

// Copyright 2021 Matrix Origin
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

package function

import (
	"simd/archsimd"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// A rejected block is untouched. The owner resumes its existing exact scalar
// arithmetic at the returned row count, without replaying the completed prefix.
func decimalMulSubSIMDPrefix(x, y []types.Decimal64, c types.Decimal64, out []types.Decimal128) int {
	n := len(out)
	if n < decimalSIMDFusedMinRows || !archsimd.X86.AVX2() {
		return 0
	}
	xd := types.DecodeSlice[uint64](types.EncodeSlice(x))
	yd := types.DecodeSlice[uint64](types.EncodeSlice(y))
	dst := types.DecodeSlice[uint64](types.EncodeSlice(out))
	cv := archsimd.BroadcastUint64x4(uint64(c))
	zero := archsimd.BroadcastUint64x4(0)
	bias := archsimd.BroadcastUint64x4(1 << 31)
	upper := archsimd.BroadcastUint64x4(0xffffffff00000000)
	sb := archsimd.BroadcastUint64x4(1 << 63)
	i := 0
	for full := n &^ 3; i < full; i += 4 {
		a := archsimd.LoadUint64x4(xd[i : i+4])
		b := archsimd.LoadUint64x4(yd[i : i+4])
		delta := cv.Sub(b)
		// A wrapped 65-bit difference can look signed32; reject overflow first.
		if !cv.Xor(b).And(cv.Xor(delta)).And(sb).IsZero() || !decimalSIMDFitsLane32(a, bias, upper) || !decimalSIMDFitsLane32(delta, bias, upper) {
			break
		}
		lo := a.ReshapeToUint32s().BitsToInt32().MulWidenEven(delta.ReshapeToUint32s().BitsToInt32()).ToBits()
		hi := decimalSIMDSign64(lo, zero)
		l, h := lo.InterleaveLoGrouped(hi), lo.InterleaveHiGrouped(hi)
		at := i * 2
		l.ConcatPermute128Scalars(0, 2, h).Store(dst[at : at+4])
		l.ConcatPermute128Scalars(1, 3, h).Store(dst[at+4 : at+8])
	}
	return i
}

func decimal64MulSIMDPrefix(a, b []types.Decimal64, out []types.Decimal128, n int) int {
	if n < decimalSIMDMultiplyMinRows || !archsimd.X86.AVX2() {
		return 0
	}
	if len(a) == 1 && len(b) == 1 {
		return 0
	}
	if len(b) == 1 {
		a, b = b, a
	}
	if len(a) == 1 {
		c := int64(a[0])
		if c != int64(int32(c)) {
			return 0
		}
		return decimal64MulConstPrefix(a[0], b, out, n)
	}
	return decimal64MulFlatPrefix(a, b, out, n)
}
func decimal64MulFlatPrefix(a, b []types.Decimal64, out []types.Decimal128, n int) int {
	ad := types.DecodeSlice[uint64](types.EncodeSlice(a))
	bd := types.DecodeSlice[uint64](types.EncodeSlice(b))
	dst := types.DecodeSlice[uint64](types.EncodeSlice(out))
	zero := archsimd.BroadcastUint64x4(0)
	bias := archsimd.BroadcastUint64x4(1 << 31)
	upper := archsimd.BroadcastUint64x4(0xffffffff00000000)
	i := 0
	for full := n &^ 3; i < full; i += 4 {
		av := archsimd.LoadUint64x4(ad[i : i+4])
		bv := archsimd.LoadUint64x4(bd[i : i+4])
		if !decimalSIMDFitsLane32(av, bias, upper) || !decimalSIMDFitsLane32(bv, bias, upper) {
			break
		}
		lo := av.ReshapeToUint32s().BitsToInt32().MulWidenEven(bv.ReshapeToUint32s().BitsToInt32()).ToBits()
		hi := decimalSIMDSign64(lo, zero)
		l, h := lo.InterleaveLoGrouped(hi), lo.InterleaveHiGrouped(hi)
		at := i * 2
		l.ConcatPermute128Scalars(0, 2, h).Store(dst[at : at+4])
		l.ConcatPermute128Scalars(1, 3, h).Store(dst[at+4 : at+8])
	}
	return i
}
func decimal64MulConstPrefix(c types.Decimal64, b []types.Decimal64, out []types.Decimal128, n int) int {
	bd := types.DecodeSlice[uint64](types.EncodeSlice(b))
	dst := types.DecodeSlice[uint64](types.EncodeSlice(out))
	a := archsimd.BroadcastUint64x4(uint64(c))
	zero := archsimd.BroadcastUint64x4(0)
	bias := archsimd.BroadcastUint64x4(1 << 31)
	upper := archsimd.BroadcastUint64x4(0xffffffff00000000)
	i := 0
	for full := n &^ 3; i < full; i += 4 {
		bv := archsimd.LoadUint64x4(bd[i : i+4])
		if !decimalSIMDFitsLane32(bv, bias, upper) {
			break
		}
		lo := a.ReshapeToUint32s().BitsToInt32().MulWidenEven(bv.ReshapeToUint32s().BitsToInt32()).ToBits()
		hi := decimalSIMDSign64(lo, zero)
		l, h := lo.InterleaveLoGrouped(hi), lo.InterleaveHiGrouped(hi)
		at := i * 2
		l.ConcatPermute128Scalars(0, 2, h).Store(dst[at : at+4])
		l.ConcatPermute128Scalars(1, 3, h).Store(dst[at+4 : at+8])
	}
	return i
}

func decimal64WidenSIMDPrefix(src []types.Decimal64, out []types.Decimal128, n int) int {
	if n < decimalSIMDCastMinRows || !archsimd.X86.AVX2() {
		return 0
	}
	data := types.DecodeSlice[uint64](types.EncodeSlice(src))
	dst := types.DecodeSlice[uint64](types.EncodeSlice(out))
	zero := archsimd.BroadcastUint64x4(0)
	i := 0
	for full := n &^ 3; i < full; i += 4 {
		lo := archsimd.LoadUint64x4(data[i : i+4])
		hi := decimalSIMDSign64(lo, zero)
		l, h := lo.InterleaveLoGrouped(hi), lo.InterleaveHiGrouped(hi)
		at := i * 2
		l.ConcatPermute128Scalars(0, 2, h).Store(dst[at : at+4])
		l.ConcatPermute128Scalars(1, 3, h).Store(dst[at+4 : at+8])
	}
	return i
}
