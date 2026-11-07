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

// Native owners pass disjoint or exact in-place slices. Publish only complete
// overflow-free blocks; the scalar owner retains first-error publication.
func decimal128AddSIMDPrefix(a, b, out []types.Decimal128, n int) int {
	if n < decimalSIMDAdd128MinRows || !archsimd.X86.AVX2() {
		return 0
	}
	// A broadcast can alias out[0]; capture it before publishing any result.
	var ac, bc [1]types.Decimal128
	am, bm := -1, -1
	if len(a) == 1 {
		ac[0] = a[0]
		a, am = ac[:], 0
	}
	if len(b) == 1 {
		bc[0] = b[0]
		b, bm = bc[:], 0
	}
	ad := types.DecodeSlice[uint64](types.EncodeSlice(a))
	bd := types.DecodeSlice[uint64](types.EncodeSlice(b))
	dst := types.DecodeSlice[uint64](types.EncodeSlice(out[:n]))
	sign := archsimd.BroadcastUint64x4(1 << 63)
	i := 0
	for ; i+8 <= n; i += 8 {
		al, ah := loadDecimal128Permuted(ad, i&am, am == 0)
		bl, bh := loadDecimal128Permuted(bd, i&bm, bm == 0)
		lo0 := al.Add(bl)
		carry := lo0.Xor(sign).BitsToInt64().Less(al.Xor(sign).BitsToInt64()).ToInt64x4().ToBits()
		hi0 := ah.Add(bh).Sub(carry)
		overflow := ah.Xor(hi0).AndNot(ah.Xor(bh))

		al, ah = loadDecimal128Permuted(ad, (i+4)&am, am == 0)
		bl, bh = loadDecimal128Permuted(bd, (i+4)&bm, bm == 0)
		lo1 := al.Add(bl)
		carry = lo1.Xor(sign).BitsToInt64().Less(al.Xor(sign).BitsToInt64()).ToInt64x4().ToBits()
		hi1 := ah.Add(bh).Sub(carry)
		overflow = overflow.Or(ah.Xor(hi1).AndNot(ah.Xor(bh)))
		if !overflow.And(sign).IsZero() {
			break
		}
		block := dst[i*2 : i*2+16]
		lo0.InterleaveLoGrouped(hi0).Store(block[:4])
		lo0.InterleaveHiGrouped(hi0).Store(block[4:8])
		lo1.InterleaveLoGrouped(hi1).Store(block[8:12])
		lo1.InterleaveHiGrouped(hi1).Store(block[12:16])
	}
	return i
}

func decimal128SubSIMDPrefix(a, b, out []types.Decimal128, n int) int {
	if n < decimalSIMDSub128MinRows || !archsimd.X86.AVX2() {
		return 0
	}
	var ac, bc [1]types.Decimal128
	am, bm := -1, -1
	if len(a) == 1 {
		ac[0] = a[0]
		a, am = ac[:], 0
	}
	if len(b) == 1 {
		bc[0] = b[0]
		b, bm = bc[:], 0
	}
	ad := types.DecodeSlice[uint64](types.EncodeSlice(a))
	bd := types.DecodeSlice[uint64](types.EncodeSlice(b))
	dst := types.DecodeSlice[uint64](types.EncodeSlice(out[:n]))
	sign := archsimd.BroadcastUint64x4(1 << 63)
	i := 0
	for ; i+8 <= n; i += 8 {
		al, ah := loadDecimal128Permuted(ad, i&am, am == 0)
		bl, bh := loadDecimal128Permuted(bd, i&bm, bm == 0)
		lo0 := al.Sub(bl)
		borrow := al.Xor(sign).BitsToInt64().Less(bl.Xor(sign).BitsToInt64()).ToInt64x4().ToBits()
		hi0 := ah.Sub(bh).Add(borrow)
		overflow := ah.Xor(hi0).And(ah.Xor(bh))

		al, ah = loadDecimal128Permuted(ad, (i+4)&am, am == 0)
		bl, bh = loadDecimal128Permuted(bd, (i+4)&bm, bm == 0)
		lo1 := al.Sub(bl)
		borrow = al.Xor(sign).BitsToInt64().Less(bl.Xor(sign).BitsToInt64()).ToInt64x4().ToBits()
		hi1 := ah.Sub(bh).Add(borrow)
		overflow = overflow.Or(ah.Xor(hi1).And(ah.Xor(bh)))
		if !overflow.And(sign).IsZero() {
			break
		}
		block := dst[i*2 : i*2+16]
		lo0.InterleaveLoGrouped(hi0).Store(block[:4])
		lo0.InterleaveHiGrouped(hi0).Store(block[4:8])
		lo1.InterleaveLoGrouped(hi1).Store(block[8:12])
		lo1.InterleaveHiGrouped(hi1).Store(block[12:16])
	}
	return i
}
