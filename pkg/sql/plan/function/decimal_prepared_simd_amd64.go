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

// Prepared expressions have already proved each original precision boundary.
func decimal128PreparedSIMD(op int32, x, y, out *vector.Vector, xi, yi, oi, n int) bool {
	a := vector.MustFixedColNoTypeCheck[types.Decimal128](x)
	b := vector.MustFixedColNoTypeCheck[types.Decimal128](y)
	if x.IsConst() {
		a = a[:1]
	} else {
		a = a[xi : xi+n]
	}
	if y.IsConst() {
		b = b[:1]
	} else {
		b = b[yi : yi+n]
	}
	if out.GetType().Oid == types.T_decimal128 {
		dst := vector.MustFixedColNoTypeCheck[types.Decimal128](out)[oi : oi+n]
		if op == PLUS {
			decimal128PreparedAdd128(a, b, dst, n)
		} else {
			decimal128PreparedSub128(a, b, dst, n)
		}
	} else {
		dst := vector.MustFixedColNoTypeCheck[types.Decimal256](out)[oi : oi+n]
		if op == PLUS {
			decimal128PreparedAdd256(a, b, dst, n)
		} else {
			decimal128PreparedSub256(a, b, dst, n)
		}
	}
	return true
}

func decimal128PreparedAdd128(a, b []types.Decimal128, out []types.Decimal128, n int) {
	ad := types.DecodeSlice[uint64](types.EncodeSlice(a))
	bd := types.DecodeSlice[uint64](types.EncodeSlice(b))
	dst := types.DecodeSlice[uint64](types.EncodeSlice(out))
	am, bm := -1, -1
	if len(a) == 1 {
		am = 0
	}
	if len(b) == 1 {
		bm = 0
	}
	ones := archsimd.BroadcastUint64x4(^uint64(0))
	i := 0
	for ; i+4 <= n; i += 4 {
		al, ah := loadDecimal128Permuted(ad, i&am, am == 0)
		bl, bh := loadDecimal128Permuted(bd, i&bm, bm == 0)
		lo := al.Add(bl)
		carry := al.And(bl).Or(al.Or(bl).And(lo.Xor(ones))).ShiftAllRight(63)
		hi := ah.Add(bh).Add(carry)
		l, h := lo.InterleaveLoGrouped(hi), lo.InterleaveHiGrouped(hi)
		l.Store(dst[i*2 : i*2+4])
		h.Store(dst[i*2+4 : i*2+8])
	}
	for ; i < n; i++ {
		v := decimalAdd128Exact(a[i&am], b[i&bm])
		out[i] = types.Decimal128{B0_63: v.B0_63, B64_127: v.B64_127}
	}
}

func decimal128PreparedAdd256(a, b []types.Decimal128, out []types.Decimal256, n int) {
	ad := types.DecodeSlice[uint64](types.EncodeSlice(a))
	bd := types.DecodeSlice[uint64](types.EncodeSlice(b))
	dst := types.DecodeSlice[uint64](types.EncodeSlice(out))
	am, bm := -1, -1
	if len(a) == 1 {
		am = 0
	}
	if len(b) == 1 {
		bm = 0
	}
	ones := archsimd.BroadcastUint64x4(^uint64(0))
	i := 0
	for ; i+4 <= n; i += 4 {
		al, ah := loadDecimal128Permuted(ad, i&am, am == 0)
		bl, bh := loadDecimal128Permuted(bd, i&bm, bm == 0)
		lo := al.Add(bl)
		carry := al.And(bl).Or(al.Or(bl).And(lo.Xor(ones))).ShiftAllRight(63)
		hi := ah.Add(bh).Add(carry)
		upper := (ah.AsInt64x4().ShiftAllRight(63).Add(bh.AsInt64x4().ShiftAllRight(63))).AsUint64x4().Add(ah.And(bh).Or(ah.Or(bh).And(hi.Xor(ones))).ShiftAllRight(63))
		// A signed128 +/- signed128 result needs129 bits, so upper is
		// already canonical sign extension. Keep all packing in registers.
		l, h := lo.InterleaveLoGrouped(hi), lo.InterleaveHiGrouped(hi)
		u, v := upper.InterleaveLoGrouped(upper), upper.InterleaveHiGrouped(upper)
		l.ConcatPermute128Scalars(0, 2, u).Store(dst[i*4 : i*4+4])
		l.ConcatPermute128Scalars(1, 3, u).Store(dst[i*4+4 : i*4+8])
		h.ConcatPermute128Scalars(0, 2, v).Store(dst[i*4+8 : i*4+12])
		h.ConcatPermute128Scalars(1, 3, v).Store(dst[i*4+12 : i*4+16])
	}
	for ; i < n; i++ {
		v := decimalAdd128Exact(a[i&am], b[i&bm])
		out[i] = v
	}
}

func decimal128PreparedSub128(a, b []types.Decimal128, out []types.Decimal128, n int) {
	ad := types.DecodeSlice[uint64](types.EncodeSlice(a))
	bd := types.DecodeSlice[uint64](types.EncodeSlice(b))
	dst := types.DecodeSlice[uint64](types.EncodeSlice(out))
	am, bm := -1, -1
	if len(a) == 1 {
		am = 0
	}
	if len(b) == 1 {
		bm = 0
	}
	ones := archsimd.BroadcastUint64x4(^uint64(0))
	i := 0
	for ; i+4 <= n; i += 4 {
		al, ah := loadDecimal128Permuted(ad, i&am, am == 0)
		bl, bh := loadDecimal128Permuted(bd, i&bm, bm == 0)
		lo := al.Sub(bl)
		carry := al.Xor(ones).And(bl).Or(al.Xor(bl).Xor(ones).And(lo)).ShiftAllRight(63)
		hi := ah.Sub(bh).Sub(carry)
		l, h := lo.InterleaveLoGrouped(hi), lo.InterleaveHiGrouped(hi)
		l.Store(dst[i*2 : i*2+4])
		h.Store(dst[i*2+4 : i*2+8])
	}
	for ; i < n; i++ {
		v := decimalSub128Exact(a[i&am], b[i&bm])
		out[i] = types.Decimal128{B0_63: v.B0_63, B64_127: v.B64_127}
	}
}

func decimal128PreparedSub256(a, b []types.Decimal128, out []types.Decimal256, n int) {
	ad := types.DecodeSlice[uint64](types.EncodeSlice(a))
	bd := types.DecodeSlice[uint64](types.EncodeSlice(b))
	dst := types.DecodeSlice[uint64](types.EncodeSlice(out))
	am, bm := -1, -1
	if len(a) == 1 {
		am = 0
	}
	if len(b) == 1 {
		bm = 0
	}
	ones := archsimd.BroadcastUint64x4(^uint64(0))
	i := 0
	for ; i+4 <= n; i += 4 {
		al, ah := loadDecimal128Permuted(ad, i&am, am == 0)
		bl, bh := loadDecimal128Permuted(bd, i&bm, bm == 0)
		lo := al.Sub(bl)
		carry := al.Xor(ones).And(bl).Or(al.Xor(bl).Xor(ones).And(lo)).ShiftAllRight(63)
		hi := ah.Sub(bh).Sub(carry)
		upper := (ah.AsInt64x4().ShiftAllRight(63).Sub(bh.AsInt64x4().ShiftAllRight(63))).AsUint64x4().Sub(ah.Xor(ones).And(bh).Or(ah.Xor(bh).Xor(ones).And(hi)).ShiftAllRight(63))
		// A signed128 +/- signed128 result needs129 bits, so upper is
		// already canonical sign extension. Keep all packing in registers.
		l, h := lo.InterleaveLoGrouped(hi), lo.InterleaveHiGrouped(hi)
		u, v := upper.InterleaveLoGrouped(upper), upper.InterleaveHiGrouped(upper)
		l.ConcatPermute128Scalars(0, 2, u).Store(dst[i*4 : i*4+4])
		l.ConcatPermute128Scalars(1, 3, u).Store(dst[i*4+4 : i*4+8])
		h.ConcatPermute128Scalars(0, 2, v).Store(dst[i*4+8 : i*4+12])
		h.ConcatPermute128Scalars(1, 3, v).Store(dst[i*4+12 : i*4+16])
	}
	for ; i < n; i++ {
		v := decimalSub128Exact(a[i&am], b[i&bm])
		out[i] = v
	}
}

func decimal64ResultSIMD(op int32, x, y, out *vector.Vector, xi, yi, oi, n int) {
	a := vector.MustFixedColNoTypeCheck[types.Decimal64](x)
	b := vector.MustFixedColNoTypeCheck[types.Decimal64](y)
	dst := vector.MustFixedColNoTypeCheck[types.Decimal64](out)[oi : oi+n]
	if x.IsConst() {
		a = a[:1]
	} else {
		a = a[xi : xi+n]
	}
	if y.IsConst() {
		b = b[:1]
	} else {
		b = b[yi : yi+n]
	}
	if op == PLUS {
		decimal64ResultAdd(a, b, dst, n)
	} else {
		decimal64ResultSub(a, b, dst, n)
	}
}
func decimal64ResultAdd(a, b, out []types.Decimal64, n int) {
	ad := types.DecodeSlice[uint64](types.EncodeSlice(a))
	bd := types.DecodeSlice[uint64](types.EncodeSlice(b))
	dst := types.DecodeSlice[uint64](types.EncodeSlice(out))
	am, bm := -1, -1
	var av, bv archsimd.Uint64x4
	if len(a) == 1 {
		am = 0
		av = archsimd.BroadcastUint64x4(ad[0])
	}
	if len(b) == 1 {
		bm = 0
		bv = archsimd.BroadcastUint64x4(bd[0])
	}
	i := 0
	for ; i+4 <= n; i += 4 {
		if am != 0 {
			av = archsimd.LoadUint64x4(ad[i : i+4])
		}
		if bm != 0 {
			bv = archsimd.LoadUint64x4(bd[i : i+4])
		}
		av.Add(bv).Store(dst[i : i+4])
	}
	for ; i < n; i++ {
		out[i] = a[i&am] + b[i&bm]
	}
}
func decimal64ResultSub(a, b, out []types.Decimal64, n int) {
	ad := types.DecodeSlice[uint64](types.EncodeSlice(a))
	bd := types.DecodeSlice[uint64](types.EncodeSlice(b))
	dst := types.DecodeSlice[uint64](types.EncodeSlice(out))
	am, bm := -1, -1
	var av, bv archsimd.Uint64x4
	if len(a) == 1 {
		am = 0
		av = archsimd.BroadcastUint64x4(ad[0])
	}
	if len(b) == 1 {
		bm = 0
		bv = archsimd.BroadcastUint64x4(bd[0])
	}
	i := 0
	for ; i+4 <= n; i += 4 {
		if am != 0 {
			av = archsimd.LoadUint64x4(ad[i : i+4])
		}
		if bm != 0 {
			bv = archsimd.LoadUint64x4(bd[i : i+4])
		}
		av.Sub(bv).Store(dst[i : i+4])
	}
	for ; i < n; i++ {
		out[i] = a[i&am] - b[i&bm]
	}
}

func decimal64WideSIMD(op int32, x, y, out *vector.Vector, xi, yi, oi, n int) {
	a := vector.MustFixedColNoTypeCheck[types.Decimal64](x)
	b := vector.MustFixedColNoTypeCheck[types.Decimal64](y)
	dst := vector.MustFixedColNoTypeCheck[types.Decimal128](out)[oi : oi+n]
	if x.IsConst() {
		a = a[:1]
	} else {
		a = a[xi : xi+n]
	}
	if y.IsConst() {
		b = b[:1]
	} else {
		b = b[yi : yi+n]
	}
	if op == PLUS {
		decimal64WideAdd(a, b, dst, n)
	} else {
		decimal64WideSub(a, b, dst, n)
	}
}
func decimal64WideAdd(a, b []types.Decimal64, out []types.Decimal128, n int) {
	ad := types.DecodeSlice[uint64](types.EncodeSlice(a))
	bd := types.DecodeSlice[uint64](types.EncodeSlice(b))
	dst := types.DecodeSlice[uint64](types.EncodeSlice(out))
	am, bm := -1, -1
	var av, bv archsimd.Uint64x4
	if len(a) == 1 {
		am = 0
		av = archsimd.BroadcastUint64x4(ad[0])
	}
	if len(b) == 1 {
		bm = 0
		bv = archsimd.BroadcastUint64x4(bd[0])
	}
	ones := archsimd.BroadcastUint64x4(^uint64(0))
	i := 0
	for ; i+4 <= n; i += 4 {
		if am != 0 {
			av = archsimd.LoadUint64x4(ad[i : i+4])
		}
		if bm != 0 {
			bv = archsimd.LoadUint64x4(bd[i : i+4])
		}
		lo := av.Add(bv)
		carry := av.And(bv).Or(av.Or(bv).And(lo.Xor(ones))).ShiftAllRight(63)
		hi := av.AsInt64x4().ShiftAllRight(63).AsUint64x4().Add(bv.AsInt64x4().ShiftAllRight(63).AsUint64x4()).Add(carry)
		l, h := lo.InterleaveLoGrouped(hi), lo.InterleaveHiGrouped(hi)
		l.ConcatPermute128Scalars(0, 2, h).Store(dst[i*2 : i*2+4])
		l.ConcatPermute128Scalars(1, 3, h).Store(dst[i*2+4 : i*2+8])
	}
	for ; i < n; i++ {
		x, y := int64(a[i&am]), int64(b[i&bm])
		v := decimalAdd128Exact(types.Decimal128FromInt64(x), types.Decimal128FromInt64(y))
		out[i] = types.Decimal128{B0_63: v.B0_63, B64_127: v.B64_127}
	}
}
func decimal64WideSub(a, b []types.Decimal64, out []types.Decimal128, n int) {
	ad := types.DecodeSlice[uint64](types.EncodeSlice(a))
	bd := types.DecodeSlice[uint64](types.EncodeSlice(b))
	dst := types.DecodeSlice[uint64](types.EncodeSlice(out))
	am, bm := -1, -1
	var av, bv archsimd.Uint64x4
	if len(a) == 1 {
		am = 0
		av = archsimd.BroadcastUint64x4(ad[0])
	}
	if len(b) == 1 {
		bm = 0
		bv = archsimd.BroadcastUint64x4(bd[0])
	}
	ones := archsimd.BroadcastUint64x4(^uint64(0))
	i := 0
	for ; i+4 <= n; i += 4 {
		if am != 0 {
			av = archsimd.LoadUint64x4(ad[i : i+4])
		}
		if bm != 0 {
			bv = archsimd.LoadUint64x4(bd[i : i+4])
		}
		lo := av.Sub(bv)
		carry := av.Xor(ones).And(bv).Or(av.Xor(bv).Xor(ones).And(lo)).ShiftAllRight(63)
		hi := av.AsInt64x4().ShiftAllRight(63).AsUint64x4().Sub(bv.AsInt64x4().ShiftAllRight(63).AsUint64x4()).Sub(carry)
		l, h := lo.InterleaveLoGrouped(hi), lo.InterleaveHiGrouped(hi)
		l.ConcatPermute128Scalars(0, 2, h).Store(dst[i*2 : i*2+4])
		l.ConcatPermute128Scalars(1, 3, h).Store(dst[i*2+4 : i*2+8])
	}
	for ; i < n; i++ {
		x, y := int64(a[i&am]), int64(b[i&bm])
		v := decimalSub128Exact(types.Decimal128FromInt64(x), types.Decimal128FromInt64(y))
		out[i] = types.Decimal128{B0_63: v.B0_63, B64_127: v.B64_127}
	}
}
