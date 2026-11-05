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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"simd/archsimd"
)

// Separate concrete loops select operand width, output width and broadcast
// orientation before processing rows. No per-block type or opcode dispatch.
func decimalMul64To128Left(x, y, out *vector.Vector, xi, yi, oi, n int) bool {
	xd, yd, od := types.DecodeSlice[uint64](x.GetData()), types.DecodeSlice[uint64](y.GetData()), types.DecodeSlice[uint64](out.GetData())
	a := archsimd.BroadcastUint64x4(xd[0])
	yd = yd[yi : yi+n]
	od = od[oi*2 : (oi+n)*2]
	zero := archsimd.BroadcastUint64x4(0)
	bias := archsimd.BroadcastUint64x4(1 << 31)
	upperMask := archsimd.BroadcastUint64x4(0xffffffff00000000)
	i := 0
	for full := n &^ 3; i < full; i += 4 {
		b := archsimd.LoadUint64x4(yd[i : i+4])
		if !decimalSIMDFitsLane32(b, bias, upperMask) {
			if i == 0 {
				return false
			}
			decimalSIMDScalarSpan(MULTI, x, y, out, xi+i, yi+i, oi+i, n-i)
			return true
		}
		lo := a.ReshapeToUint32s().BitsToInt32().MulWidenEven(b.ReshapeToUint32s().BitsToInt32()).ToBits()
		hi := decimalSIMDSign64(lo, zero)
		atOut := i * 2
		low, high := lo.InterleaveLoGrouped(hi), lo.InterleaveHiGrouped(hi)
		low.ConcatPermute128Scalars(0, 2, high).Store(od[atOut : atOut+4])
		low.ConcatPermute128Scalars(1, 3, high).Store(od[atOut+4 : atOut+8])
	}
	if i < n {
		decimalSIMDScalarSpan(MULTI, x, y, out, xi+i, yi+i, oi+i, n-i)
	}
	return true
}
func decimalMul64To128Right(x, y, out *vector.Vector, xi, yi, oi, n int) bool {
	xd, yd, od := types.DecodeSlice[uint64](x.GetData()), types.DecodeSlice[uint64](y.GetData()), types.DecodeSlice[uint64](out.GetData())
	b := archsimd.BroadcastUint64x4(yd[0])
	xd = xd[xi*1 : (xi+n)*1]
	od = od[oi*2 : (oi+n)*2]
	zero := archsimd.BroadcastUint64x4(0)
	bias := archsimd.BroadcastUint64x4(1 << 31)
	upperMask := archsimd.BroadcastUint64x4(0xffffffff00000000)
	i := 0
	for full := n &^ 3; i < full; i += 4 {
		a := archsimd.LoadUint64x4(xd[i : i+4])
		if !decimalSIMDFitsLane32(a, bias, upperMask) {
			if i == 0 {
				return false
			}
			decimalSIMDScalarSpan(MULTI, x, y, out, xi+i, yi+i, oi+i, n-i)
			return true
		}
		lo := a.ReshapeToUint32s().BitsToInt32().MulWidenEven(b.ReshapeToUint32s().BitsToInt32()).ToBits()
		hi := decimalSIMDSign64(lo, zero)
		atOut := i * 2
		low, high := lo.InterleaveLoGrouped(hi), lo.InterleaveHiGrouped(hi)
		low.ConcatPermute128Scalars(0, 2, high).Store(od[atOut : atOut+4])
		low.ConcatPermute128Scalars(1, 3, high).Store(od[atOut+4 : atOut+8])
	}
	if i < n {
		decimalSIMDScalarSpan(MULTI, x, y, out, xi+i, yi+i, oi+i, n-i)
	}
	return true
}
func decimalMul64To128Flat(x, y, out *vector.Vector, xi, yi, oi, n int) bool {
	xd, yd, od := types.DecodeSlice[uint64](x.GetData()), types.DecodeSlice[uint64](y.GetData()), types.DecodeSlice[uint64](out.GetData())
	xd = xd[xi*1 : (xi+n)*1]
	yd = yd[yi : yi+n]
	od = od[oi*2 : (oi+n)*2]
	zero := archsimd.BroadcastUint64x4(0)
	bias := archsimd.BroadcastUint64x4(1 << 31)
	upperMask := archsimd.BroadcastUint64x4(0xffffffff00000000)
	i := 0
	for full := n &^ 3; i < full; i += 4 {
		a := archsimd.LoadUint64x4(xd[i : i+4])
		b := archsimd.LoadUint64x4(yd[i : i+4])
		if !decimalSIMDFitsLane32(a, bias, upperMask) || !decimalSIMDFitsLane32(b, bias, upperMask) {
			if i == 0 {
				return false
			}
			decimalSIMDScalarSpan(MULTI, x, y, out, xi+i, yi+i, oi+i, n-i)
			return true
		}
		lo := a.ReshapeToUint32s().BitsToInt32().MulWidenEven(b.ReshapeToUint32s().BitsToInt32()).ToBits()
		hi := decimalSIMDSign64(lo, zero)
		atOut := i * 2
		low, high := lo.InterleaveLoGrouped(hi), lo.InterleaveHiGrouped(hi)
		low.ConcatPermute128Scalars(0, 2, high).Store(od[atOut : atOut+4])
		low.ConcatPermute128Scalars(1, 3, high).Store(od[atOut+4 : atOut+8])
	}
	if i < n {
		decimalSIMDScalarSpan(MULTI, x, y, out, xi+i, yi+i, oi+i, n-i)
	}
	return true
}
func decimalMul64To256Left(x, y, out *vector.Vector, xi, yi, oi, n int) bool {
	xd, yd, od := types.DecodeSlice[uint64](x.GetData()), types.DecodeSlice[uint64](y.GetData()), types.DecodeSlice[uint64](out.GetData())
	a := archsimd.BroadcastUint64x4(xd[0])
	yd = yd[yi : yi+n]
	od = od[oi*4 : (oi+n)*4]
	zero := archsimd.BroadcastUint64x4(0)
	bias := archsimd.BroadcastUint64x4(1 << 31)
	upperMask := archsimd.BroadcastUint64x4(0xffffffff00000000)
	i := 0
	for full := n &^ 3; i < full; i += 4 {
		b := archsimd.LoadUint64x4(yd[i : i+4])
		if !decimalSIMDFitsLane32(b, bias, upperMask) {
			if i == 0 {
				return false
			}
			decimalSIMDScalarSpan(MULTI, x, y, out, xi+i, yi+i, oi+i, n-i)
			return true
		}
		lo := a.ReshapeToUint32s().BitsToInt32().MulWidenEven(b.ReshapeToUint32s().BitsToInt32()).ToBits()
		hi := decimalSIMDSign64(lo, zero)
		atOut := i * 4
		low, high := lo.InterleaveLoGrouped(hi), lo.InterleaveHiGrouped(hi)
		upperLow, upperHigh := hi.InterleaveLoGrouped(hi), hi.InterleaveHiGrouped(hi)
		low.ConcatPermute128Scalars(0, 2, upperLow).Store(od[atOut : atOut+4])
		high.ConcatPermute128Scalars(0, 2, upperHigh).Store(od[atOut+4 : atOut+8])
		low.ConcatPermute128Scalars(1, 3, upperLow).Store(od[atOut+8 : atOut+12])
		high.ConcatPermute128Scalars(1, 3, upperHigh).Store(od[atOut+12 : atOut+16])
	}
	if i < n {
		decimalSIMDScalarSpan(MULTI, x, y, out, xi+i, yi+i, oi+i, n-i)
	}
	return true
}
func decimalMul64To256Right(x, y, out *vector.Vector, xi, yi, oi, n int) bool {
	xd, yd, od := types.DecodeSlice[uint64](x.GetData()), types.DecodeSlice[uint64](y.GetData()), types.DecodeSlice[uint64](out.GetData())
	b := archsimd.BroadcastUint64x4(yd[0])
	xd = xd[xi*1 : (xi+n)*1]
	od = od[oi*4 : (oi+n)*4]
	zero := archsimd.BroadcastUint64x4(0)
	bias := archsimd.BroadcastUint64x4(1 << 31)
	upperMask := archsimd.BroadcastUint64x4(0xffffffff00000000)
	i := 0
	for full := n &^ 3; i < full; i += 4 {
		a := archsimd.LoadUint64x4(xd[i : i+4])
		if !decimalSIMDFitsLane32(a, bias, upperMask) {
			if i == 0 {
				return false
			}
			decimalSIMDScalarSpan(MULTI, x, y, out, xi+i, yi+i, oi+i, n-i)
			return true
		}
		lo := a.ReshapeToUint32s().BitsToInt32().MulWidenEven(b.ReshapeToUint32s().BitsToInt32()).ToBits()
		hi := decimalSIMDSign64(lo, zero)
		atOut := i * 4
		low, high := lo.InterleaveLoGrouped(hi), lo.InterleaveHiGrouped(hi)
		upperLow, upperHigh := hi.InterleaveLoGrouped(hi), hi.InterleaveHiGrouped(hi)
		low.ConcatPermute128Scalars(0, 2, upperLow).Store(od[atOut : atOut+4])
		high.ConcatPermute128Scalars(0, 2, upperHigh).Store(od[atOut+4 : atOut+8])
		low.ConcatPermute128Scalars(1, 3, upperLow).Store(od[atOut+8 : atOut+12])
		high.ConcatPermute128Scalars(1, 3, upperHigh).Store(od[atOut+12 : atOut+16])
	}
	if i < n {
		decimalSIMDScalarSpan(MULTI, x, y, out, xi+i, yi+i, oi+i, n-i)
	}
	return true
}
func decimalMul64To256Flat(x, y, out *vector.Vector, xi, yi, oi, n int) bool {
	xd, yd, od := types.DecodeSlice[uint64](x.GetData()), types.DecodeSlice[uint64](y.GetData()), types.DecodeSlice[uint64](out.GetData())
	xd = xd[xi*1 : (xi+n)*1]
	yd = yd[yi : yi+n]
	od = od[oi*4 : (oi+n)*4]
	zero := archsimd.BroadcastUint64x4(0)
	bias := archsimd.BroadcastUint64x4(1 << 31)
	upperMask := archsimd.BroadcastUint64x4(0xffffffff00000000)
	i := 0
	for full := n &^ 3; i < full; i += 4 {
		a := archsimd.LoadUint64x4(xd[i : i+4])
		b := archsimd.LoadUint64x4(yd[i : i+4])
		if !decimalSIMDFitsLane32(a, bias, upperMask) || !decimalSIMDFitsLane32(b, bias, upperMask) {
			if i == 0 {
				return false
			}
			decimalSIMDScalarSpan(MULTI, x, y, out, xi+i, yi+i, oi+i, n-i)
			return true
		}
		lo := a.ReshapeToUint32s().BitsToInt32().MulWidenEven(b.ReshapeToUint32s().BitsToInt32()).ToBits()
		hi := decimalSIMDSign64(lo, zero)
		atOut := i * 4
		low, high := lo.InterleaveLoGrouped(hi), lo.InterleaveHiGrouped(hi)
		upperLow, upperHigh := hi.InterleaveLoGrouped(hi), hi.InterleaveHiGrouped(hi)
		low.ConcatPermute128Scalars(0, 2, upperLow).Store(od[atOut : atOut+4])
		high.ConcatPermute128Scalars(0, 2, upperHigh).Store(od[atOut+4 : atOut+8])
		low.ConcatPermute128Scalars(1, 3, upperLow).Store(od[atOut+8 : atOut+12])
		high.ConcatPermute128Scalars(1, 3, upperHigh).Store(od[atOut+12 : atOut+16])
	}
	if i < n {
		decimalSIMDScalarSpan(MULTI, x, y, out, xi+i, yi+i, oi+i, n-i)
	}
	return true
}
func decimalMul128To128Left(x, y, out *vector.Vector, xi, yi, oi, n int) bool {
	xd, yd, od := types.DecodeSlice[uint64](x.GetData()), types.DecodeSlice[uint64](y.GetData()), types.DecodeSlice[uint64](out.GetData())
	a := archsimd.BroadcastUint64x4(xd[0])
	yd = yd[yi : yi+n]
	od = od[oi*2 : (oi+n)*2]
	zero := archsimd.BroadcastUint64x4(0)
	bias := archsimd.BroadcastUint64x4(1 << 31)
	upperMask := archsimd.BroadcastUint64x4(0xffffffff00000000)
	i := 0
	for full := n &^ 3; i < full; i += 4 {
		b := archsimd.LoadUint64x4(yd[i : i+4])
		if !decimalSIMDFitsLane32(b, bias, upperMask) {
			if i == 0 {
				return false
			}
			decimalSIMDScalarSpan(MULTI, x, y, out, xi+i, yi+i, oi+i, n-i)
			return true
		}
		lo := a.ReshapeToUint32s().BitsToInt32().MulWidenEven(b.ReshapeToUint32s().BitsToInt32()).ToBits()
		hi := decimalSIMDSign64(lo, zero)
		atOut := i * 2
		low, high := lo.InterleaveLoGrouped(hi), lo.InterleaveHiGrouped(hi)
		low.ConcatPermute128Scalars(0, 2, high).Store(od[atOut : atOut+4])
		low.ConcatPermute128Scalars(1, 3, high).Store(od[atOut+4 : atOut+8])
	}
	if i < n {
		decimalSIMDScalarSpan(MULTI, x, y, out, xi+i, yi+i, oi+i, n-i)
	}
	return true
}
func decimalMul128To128Right(x, y, out *vector.Vector, xi, yi, oi, n int) bool {
	xd, yd, od := types.DecodeSlice[uint64](x.GetData()), types.DecodeSlice[uint64](y.GetData()), types.DecodeSlice[uint64](out.GetData())
	b := archsimd.BroadcastUint64x4(yd[0])
	xd = xd[xi*2 : (xi+n)*2]
	od = od[oi*2 : (oi+n)*2]
	zero := archsimd.BroadcastUint64x4(0)
	bias := archsimd.BroadcastUint64x4(1 << 31)
	upperMask := archsimd.BroadcastUint64x4(0xffffffff00000000)
	i := 0
	for full := n &^ 3; i < full; i += 4 {
		at := i * 2
		l, h := archsimd.LoadUint64x4(xd[at:at+4]), archsimd.LoadUint64x4(xd[at+4:at+8])
		even, odd := l.ConcatPermute128Scalars(0, 2, h), l.ConcatPermute128Scalars(1, 3, h)
		a, ah := even.InterleaveLoGrouped(odd), even.InterleaveHiGrouped(odd)
		if !decimalSIMDFitsLane32(a, bias, upperMask) || !decimalSIMDSign64(a, zero).Xor(ah).IsZero() {
			if i == 0 {
				return false
			}
			decimalSIMDScalarSpan(MULTI, x, y, out, xi+i, yi+i, oi+i, n-i)
			return true
		}
		lo := a.ReshapeToUint32s().BitsToInt32().MulWidenEven(b.ReshapeToUint32s().BitsToInt32()).ToBits()
		hi := decimalSIMDSign64(lo, zero)
		atOut := i * 2
		low, high := lo.InterleaveLoGrouped(hi), lo.InterleaveHiGrouped(hi)
		low.ConcatPermute128Scalars(0, 2, high).Store(od[atOut : atOut+4])
		low.ConcatPermute128Scalars(1, 3, high).Store(od[atOut+4 : atOut+8])
	}
	if i < n {
		decimalSIMDScalarSpan(MULTI, x, y, out, xi+i, yi+i, oi+i, n-i)
	}
	return true
}
func decimalMul128To128Flat(x, y, out *vector.Vector, xi, yi, oi, n int) bool {
	xd, yd, od := types.DecodeSlice[uint64](x.GetData()), types.DecodeSlice[uint64](y.GetData()), types.DecodeSlice[uint64](out.GetData())
	xd = xd[xi*2 : (xi+n)*2]
	yd = yd[yi : yi+n]
	od = od[oi*2 : (oi+n)*2]
	zero := archsimd.BroadcastUint64x4(0)
	bias := archsimd.BroadcastUint64x4(1 << 31)
	upperMask := archsimd.BroadcastUint64x4(0xffffffff00000000)
	i := 0
	for full := n &^ 3; i < full; i += 4 {
		at := i * 2
		l, h := archsimd.LoadUint64x4(xd[at:at+4]), archsimd.LoadUint64x4(xd[at+4:at+8])
		even, odd := l.ConcatPermute128Scalars(0, 2, h), l.ConcatPermute128Scalars(1, 3, h)
		a, ah := even.InterleaveLoGrouped(odd), even.InterleaveHiGrouped(odd)
		b := archsimd.LoadUint64x4(yd[i : i+4])
		if !decimalSIMDFitsLane32(a, bias, upperMask) || !decimalSIMDSign64(a, zero).Xor(ah).IsZero() || !decimalSIMDFitsLane32(b, bias, upperMask) {
			if i == 0 {
				return false
			}
			decimalSIMDScalarSpan(MULTI, x, y, out, xi+i, yi+i, oi+i, n-i)
			return true
		}
		lo := a.ReshapeToUint32s().BitsToInt32().MulWidenEven(b.ReshapeToUint32s().BitsToInt32()).ToBits()
		hi := decimalSIMDSign64(lo, zero)
		atOut := i * 2
		low, high := lo.InterleaveLoGrouped(hi), lo.InterleaveHiGrouped(hi)
		low.ConcatPermute128Scalars(0, 2, high).Store(od[atOut : atOut+4])
		low.ConcatPermute128Scalars(1, 3, high).Store(od[atOut+4 : atOut+8])
	}
	if i < n {
		decimalSIMDScalarSpan(MULTI, x, y, out, xi+i, yi+i, oi+i, n-i)
	}
	return true
}
func decimalMul128To256Left(x, y, out *vector.Vector, xi, yi, oi, n int) bool {
	xd, yd, od := types.DecodeSlice[uint64](x.GetData()), types.DecodeSlice[uint64](y.GetData()), types.DecodeSlice[uint64](out.GetData())
	a := archsimd.BroadcastUint64x4(xd[0])
	yd = yd[yi : yi+n]
	od = od[oi*4 : (oi+n)*4]
	zero := archsimd.BroadcastUint64x4(0)
	bias := archsimd.BroadcastUint64x4(1 << 31)
	upperMask := archsimd.BroadcastUint64x4(0xffffffff00000000)
	i := 0
	for full := n &^ 3; i < full; i += 4 {
		b := archsimd.LoadUint64x4(yd[i : i+4])
		if !decimalSIMDFitsLane32(b, bias, upperMask) {
			if i == 0 {
				return false
			}
			decimalSIMDScalarSpan(MULTI, x, y, out, xi+i, yi+i, oi+i, n-i)
			return true
		}
		lo := a.ReshapeToUint32s().BitsToInt32().MulWidenEven(b.ReshapeToUint32s().BitsToInt32()).ToBits()
		hi := decimalSIMDSign64(lo, zero)
		atOut := i * 4
		low, high := lo.InterleaveLoGrouped(hi), lo.InterleaveHiGrouped(hi)
		upperLow, upperHigh := hi.InterleaveLoGrouped(hi), hi.InterleaveHiGrouped(hi)
		low.ConcatPermute128Scalars(0, 2, upperLow).Store(od[atOut : atOut+4])
		high.ConcatPermute128Scalars(0, 2, upperHigh).Store(od[atOut+4 : atOut+8])
		low.ConcatPermute128Scalars(1, 3, upperLow).Store(od[atOut+8 : atOut+12])
		high.ConcatPermute128Scalars(1, 3, upperHigh).Store(od[atOut+12 : atOut+16])
	}
	if i < n {
		decimalSIMDScalarSpan(MULTI, x, y, out, xi+i, yi+i, oi+i, n-i)
	}
	return true
}
func decimalMul128To256Right(x, y, out *vector.Vector, xi, yi, oi, n int) bool {
	xd, yd, od := types.DecodeSlice[uint64](x.GetData()), types.DecodeSlice[uint64](y.GetData()), types.DecodeSlice[uint64](out.GetData())
	b := archsimd.BroadcastUint64x4(yd[0])
	xd = xd[xi*2 : (xi+n)*2]
	od = od[oi*4 : (oi+n)*4]
	zero := archsimd.BroadcastUint64x4(0)
	bias := archsimd.BroadcastUint64x4(1 << 31)
	upperMask := archsimd.BroadcastUint64x4(0xffffffff00000000)
	i := 0
	for full := n &^ 3; i < full; i += 4 {
		at := i * 2
		l, h := archsimd.LoadUint64x4(xd[at:at+4]), archsimd.LoadUint64x4(xd[at+4:at+8])
		even, odd := l.ConcatPermute128Scalars(0, 2, h), l.ConcatPermute128Scalars(1, 3, h)
		a, ah := even.InterleaveLoGrouped(odd), even.InterleaveHiGrouped(odd)
		if !decimalSIMDFitsLane32(a, bias, upperMask) || !decimalSIMDSign64(a, zero).Xor(ah).IsZero() {
			if i == 0 {
				return false
			}
			decimalSIMDScalarSpan(MULTI, x, y, out, xi+i, yi+i, oi+i, n-i)
			return true
		}
		lo := a.ReshapeToUint32s().BitsToInt32().MulWidenEven(b.ReshapeToUint32s().BitsToInt32()).ToBits()
		hi := decimalSIMDSign64(lo, zero)
		atOut := i * 4
		low, high := lo.InterleaveLoGrouped(hi), lo.InterleaveHiGrouped(hi)
		upperLow, upperHigh := hi.InterleaveLoGrouped(hi), hi.InterleaveHiGrouped(hi)
		low.ConcatPermute128Scalars(0, 2, upperLow).Store(od[atOut : atOut+4])
		high.ConcatPermute128Scalars(0, 2, upperHigh).Store(od[atOut+4 : atOut+8])
		low.ConcatPermute128Scalars(1, 3, upperLow).Store(od[atOut+8 : atOut+12])
		high.ConcatPermute128Scalars(1, 3, upperHigh).Store(od[atOut+12 : atOut+16])
	}
	if i < n {
		decimalSIMDScalarSpan(MULTI, x, y, out, xi+i, yi+i, oi+i, n-i)
	}
	return true
}
func decimalMul128To256Flat(x, y, out *vector.Vector, xi, yi, oi, n int) bool {
	xd, yd, od := types.DecodeSlice[uint64](x.GetData()), types.DecodeSlice[uint64](y.GetData()), types.DecodeSlice[uint64](out.GetData())
	xd = xd[xi*2 : (xi+n)*2]
	yd = yd[yi : yi+n]
	od = od[oi*4 : (oi+n)*4]
	zero := archsimd.BroadcastUint64x4(0)
	bias := archsimd.BroadcastUint64x4(1 << 31)
	upperMask := archsimd.BroadcastUint64x4(0xffffffff00000000)
	i := 0
	for full := n &^ 3; i < full; i += 4 {
		at := i * 2
		l, h := archsimd.LoadUint64x4(xd[at:at+4]), archsimd.LoadUint64x4(xd[at+4:at+8])
		even, odd := l.ConcatPermute128Scalars(0, 2, h), l.ConcatPermute128Scalars(1, 3, h)
		a, ah := even.InterleaveLoGrouped(odd), even.InterleaveHiGrouped(odd)
		b := archsimd.LoadUint64x4(yd[i : i+4])
		if !decimalSIMDFitsLane32(a, bias, upperMask) || !decimalSIMDSign64(a, zero).Xor(ah).IsZero() || !decimalSIMDFitsLane32(b, bias, upperMask) {
			if i == 0 {
				return false
			}
			decimalSIMDScalarSpan(MULTI, x, y, out, xi+i, yi+i, oi+i, n-i)
			return true
		}
		lo := a.ReshapeToUint32s().BitsToInt32().MulWidenEven(b.ReshapeToUint32s().BitsToInt32()).ToBits()
		hi := decimalSIMDSign64(lo, zero)
		atOut := i * 4
		low, high := lo.InterleaveLoGrouped(hi), lo.InterleaveHiGrouped(hi)
		upperLow, upperHigh := hi.InterleaveLoGrouped(hi), hi.InterleaveHiGrouped(hi)
		low.ConcatPermute128Scalars(0, 2, upperLow).Store(od[atOut : atOut+4])
		high.ConcatPermute128Scalars(0, 2, upperHigh).Store(od[atOut+4 : atOut+8])
		low.ConcatPermute128Scalars(1, 3, upperLow).Store(od[atOut+8 : atOut+12])
		high.ConcatPermute128Scalars(1, 3, upperHigh).Store(od[atOut+12 : atOut+16])
	}
	if i < n {
		decimalSIMDScalarSpan(MULTI, x, y, out, xi+i, yi+i, oi+i, n-i)
	}
	return true
}
