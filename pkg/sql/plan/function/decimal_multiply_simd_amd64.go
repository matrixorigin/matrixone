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
	i := 0
	for ; i+4 <= n; i += 4 {
		b := archsimd.LoadUint64x4(yd[yi+i : yi+i+4])
		if !decimalSIMDFitsLane32(b) {
			if i == 0 {
				return false
			}
			decimalSIMDScalarSpan(MULTI, x, y, out, xi+i, yi+i, oi+i, n-i)
			return true
		}
		lo := a.AsInt32x8().MulWidenEven(b.AsInt32x8()).AsUint64x4()
		hi := lo.AsInt64x4().ShiftAllRight(63).AsUint64x4()
		atOut := (oi + i) * 2
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
	i := 0
	for ; i+4 <= n; i += 4 {
		a := archsimd.LoadUint64x4(xd[xi+i : xi+i+4])
		if !decimalSIMDFitsLane32(a) {
			if i == 0 {
				return false
			}
			decimalSIMDScalarSpan(MULTI, x, y, out, xi+i, yi+i, oi+i, n-i)
			return true
		}
		lo := a.AsInt32x8().MulWidenEven(b.AsInt32x8()).AsUint64x4()
		hi := lo.AsInt64x4().ShiftAllRight(63).AsUint64x4()
		atOut := (oi + i) * 2
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
	i := 0
	for ; i+4 <= n; i += 4 {
		a := archsimd.LoadUint64x4(xd[xi+i : xi+i+4])
		b := archsimd.LoadUint64x4(yd[yi+i : yi+i+4])
		if !decimalSIMDFitsLane32(a) || !decimalSIMDFitsLane32(b) {
			if i == 0 {
				return false
			}
			decimalSIMDScalarSpan(MULTI, x, y, out, xi+i, yi+i, oi+i, n-i)
			return true
		}
		lo := a.AsInt32x8().MulWidenEven(b.AsInt32x8()).AsUint64x4()
		hi := lo.AsInt64x4().ShiftAllRight(63).AsUint64x4()
		atOut := (oi + i) * 2
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
	i := 0
	for ; i+4 <= n; i += 4 {
		b := archsimd.LoadUint64x4(yd[yi+i : yi+i+4])
		if !decimalSIMDFitsLane32(b) {
			if i == 0 {
				return false
			}
			decimalSIMDScalarSpan(MULTI, x, y, out, xi+i, yi+i, oi+i, n-i)
			return true
		}
		lo := a.AsInt32x8().MulWidenEven(b.AsInt32x8()).AsUint64x4()
		hi := lo.AsInt64x4().ShiftAllRight(63).AsUint64x4()
		atOut := (oi + i) * 4
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
	i := 0
	for ; i+4 <= n; i += 4 {
		a := archsimd.LoadUint64x4(xd[xi+i : xi+i+4])
		if !decimalSIMDFitsLane32(a) {
			if i == 0 {
				return false
			}
			decimalSIMDScalarSpan(MULTI, x, y, out, xi+i, yi+i, oi+i, n-i)
			return true
		}
		lo := a.AsInt32x8().MulWidenEven(b.AsInt32x8()).AsUint64x4()
		hi := lo.AsInt64x4().ShiftAllRight(63).AsUint64x4()
		atOut := (oi + i) * 4
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
	i := 0
	for ; i+4 <= n; i += 4 {
		a := archsimd.LoadUint64x4(xd[xi+i : xi+i+4])
		b := archsimd.LoadUint64x4(yd[yi+i : yi+i+4])
		if !decimalSIMDFitsLane32(a) || !decimalSIMDFitsLane32(b) {
			if i == 0 {
				return false
			}
			decimalSIMDScalarSpan(MULTI, x, y, out, xi+i, yi+i, oi+i, n-i)
			return true
		}
		lo := a.AsInt32x8().MulWidenEven(b.AsInt32x8()).AsUint64x4()
		hi := lo.AsInt64x4().ShiftAllRight(63).AsUint64x4()
		atOut := (oi + i) * 4
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
	i := 0
	for ; i+4 <= n; i += 4 {
		b := archsimd.LoadUint64x4(yd[yi+i : yi+i+4])
		if !decimalSIMDFitsLane32(b) {
			if i == 0 {
				return false
			}
			decimalSIMDScalarSpan(MULTI, x, y, out, xi+i, yi+i, oi+i, n-i)
			return true
		}
		lo := a.AsInt32x8().MulWidenEven(b.AsInt32x8()).AsUint64x4()
		hi := lo.AsInt64x4().ShiftAllRight(63).AsUint64x4()
		atOut := (oi + i) * 2
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
	i := 0
	for ; i+4 <= n; i += 4 {
		at := (xi + i) * 2
		l, h := archsimd.LoadUint64x4(xd[at:at+4]), archsimd.LoadUint64x4(xd[at+4:at+8])
		even, odd := l.ConcatPermute128Scalars(0, 2, h), l.ConcatPermute128Scalars(1, 3, h)
		a, ah := even.InterleaveLoGrouped(odd), even.InterleaveHiGrouped(odd)
		if !decimalSIMDFitsLane32(a) || a.AsInt64x4().ShiftAllRight(63).AsUint64x4().Equal(ah).ToBits() != 15 {
			if i == 0 {
				return false
			}
			decimalSIMDScalarSpan(MULTI, x, y, out, xi+i, yi+i, oi+i, n-i)
			return true
		}
		lo := a.AsInt32x8().MulWidenEven(b.AsInt32x8()).AsUint64x4()
		hi := lo.AsInt64x4().ShiftAllRight(63).AsUint64x4()
		atOut := (oi + i) * 2
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
	i := 0
	for ; i+4 <= n; i += 4 {
		at := (xi + i) * 2
		l, h := archsimd.LoadUint64x4(xd[at:at+4]), archsimd.LoadUint64x4(xd[at+4:at+8])
		even, odd := l.ConcatPermute128Scalars(0, 2, h), l.ConcatPermute128Scalars(1, 3, h)
		a, ah := even.InterleaveLoGrouped(odd), even.InterleaveHiGrouped(odd)
		b := archsimd.LoadUint64x4(yd[yi+i : yi+i+4])
		if !decimalSIMDFitsLane32(a) || a.AsInt64x4().ShiftAllRight(63).AsUint64x4().Equal(ah).ToBits() != 15 || !decimalSIMDFitsLane32(b) {
			if i == 0 {
				return false
			}
			decimalSIMDScalarSpan(MULTI, x, y, out, xi+i, yi+i, oi+i, n-i)
			return true
		}
		lo := a.AsInt32x8().MulWidenEven(b.AsInt32x8()).AsUint64x4()
		hi := lo.AsInt64x4().ShiftAllRight(63).AsUint64x4()
		atOut := (oi + i) * 2
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
	i := 0
	for ; i+4 <= n; i += 4 {
		b := archsimd.LoadUint64x4(yd[yi+i : yi+i+4])
		if !decimalSIMDFitsLane32(b) {
			if i == 0 {
				return false
			}
			decimalSIMDScalarSpan(MULTI, x, y, out, xi+i, yi+i, oi+i, n-i)
			return true
		}
		lo := a.AsInt32x8().MulWidenEven(b.AsInt32x8()).AsUint64x4()
		hi := lo.AsInt64x4().ShiftAllRight(63).AsUint64x4()
		atOut := (oi + i) * 4
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
	i := 0
	for ; i+4 <= n; i += 4 {
		at := (xi + i) * 2
		l, h := archsimd.LoadUint64x4(xd[at:at+4]), archsimd.LoadUint64x4(xd[at+4:at+8])
		even, odd := l.ConcatPermute128Scalars(0, 2, h), l.ConcatPermute128Scalars(1, 3, h)
		a, ah := even.InterleaveLoGrouped(odd), even.InterleaveHiGrouped(odd)
		if !decimalSIMDFitsLane32(a) || a.AsInt64x4().ShiftAllRight(63).AsUint64x4().Equal(ah).ToBits() != 15 {
			if i == 0 {
				return false
			}
			decimalSIMDScalarSpan(MULTI, x, y, out, xi+i, yi+i, oi+i, n-i)
			return true
		}
		lo := a.AsInt32x8().MulWidenEven(b.AsInt32x8()).AsUint64x4()
		hi := lo.AsInt64x4().ShiftAllRight(63).AsUint64x4()
		atOut := (oi + i) * 4
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
	i := 0
	for ; i+4 <= n; i += 4 {
		at := (xi + i) * 2
		l, h := archsimd.LoadUint64x4(xd[at:at+4]), archsimd.LoadUint64x4(xd[at+4:at+8])
		even, odd := l.ConcatPermute128Scalars(0, 2, h), l.ConcatPermute128Scalars(1, 3, h)
		a, ah := even.InterleaveLoGrouped(odd), even.InterleaveHiGrouped(odd)
		b := archsimd.LoadUint64x4(yd[yi+i : yi+i+4])
		if !decimalSIMDFitsLane32(a) || a.AsInt64x4().ShiftAllRight(63).AsUint64x4().Equal(ah).ToBits() != 15 || !decimalSIMDFitsLane32(b) {
			if i == 0 {
				return false
			}
			decimalSIMDScalarSpan(MULTI, x, y, out, xi+i, yi+i, oi+i, n-i)
			return true
		}
		lo := a.AsInt32x8().MulWidenEven(b.AsInt32x8()).AsUint64x4()
		hi := lo.AsInt64x4().ShiftAllRight(63).AsUint64x4()
		atOut := (oi + i) * 4
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
