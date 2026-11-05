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
	"math/bits"
	"simd/archsimd"
)

// tryDecimal128SIMD preserves the ordinary same-scale kernel's first-error
// index and publication through the failing row. Exact in-place operands work:
// every block is loaded before any output from that block is published.
func tryDecimal128SIMD(op int32, a, b, out []types.Decimal128) (int, bool) {
	n := max(len(a), len(b))
	if n < 256 || !archsimd.X86.AVX2() || len(out) < n || (len(a) != n && len(a) != 1) || (len(b) != n && len(b) != 1) {
		return -1, false
	}
	if op == PLUS {
		return decimal128SIMDAdd(a, b, out, n), true
	}
	if op == MINUS {
		return decimal128SIMDSub(a, b, out, n), true
	}
	return -1, false
}

func decimal128SIMDAdd(a, b, out []types.Decimal128, n int) int {
	var ac, bc [1]types.Decimal128
	if len(a) == 1 {
		ac[0] = a[0]
		a = ac[:]
	}
	if len(b) == 1 {
		bc[0] = b[0]
		b = bc[:]
	}
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
	ones, sb := archsimd.BroadcastUint64x4(^uint64(0)), archsimd.BroadcastUint64x4(uint64(1)<<63)
	i := 0
	for ; i+4 <= n; i += 4 {
		al, ah := loadDecimal128Permuted(ad, i&am, am == 0)
		bl, bh := loadDecimal128Permuted(bd, i&bm, bm == 0)
		lo := al.Add(bl)
		carry := (lo.Xor(sb).BitsToInt64().Less(al.Xor(sb).BitsToInt64())).ToInt64x4().ToBits()
		hi := ah.Add(bh).Sub(carry)
		mask := (ah.Xor(bh).Xor(ones).And(ah.Xor(hi))).BitsToInt64().Less(archsimd.BroadcastInt64x4(0)).ToBits()
		// Lanes stay in 0,2,1,3 order throughout, removing four cross-lane
		// permutations per block. Only the rare first-error index is reordered.
		l, h := lo.InterleaveLoGrouped(hi), lo.InterleaveHiGrouped(hi)
		if mask != 0 {
			logical := (mask & 1) | ((mask & 4) >> 1) | ((mask & 2) << 1) | (mask & 8)
			last := bits.TrailingZeros8(logical)
			var words [8]uint64
			l.Store(words[:4])
			h.Store(words[4:])
			for lane := 0; lane <= last; lane++ {
				out[i+lane] = types.Decimal128{B0_63: words[2*lane], B64_127: words[2*lane+1]}
			}
			return i + last
		}
		l.Store(dst[i*2 : i*2+4])
		h.Store(dst[i*2+4 : i*2+8])
	}
	for ; i < n; i++ {
		x, y := a[i&am], b[i&bm]
		lo, c := bits.Add64(x.B0_63, y.B0_63, 0)
		hi, _ := bits.Add64(x.B64_127, y.B64_127, c)
		out[i] = types.Decimal128{B0_63: lo, B64_127: hi}
		if x.B64_127>>63 == y.B64_127>>63 && x.B64_127>>63 != hi>>63 {
			return i
		}
	}
	return -1
}

func decimal128SIMDSub(a, b, out []types.Decimal128, n int) int {
	var ac, bc [1]types.Decimal128
	if len(a) == 1 {
		ac[0] = a[0]
		a = ac[:]
	}
	if len(b) == 1 {
		bc[0] = b[0]
		b = bc[:]
	}
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
	sb := archsimd.BroadcastUint64x4(uint64(1) << 63)
	i := 0
	for ; i+4 <= n; i += 4 {
		al, ah := loadDecimal128Permuted(ad, i&am, am == 0)
		bl, bh := loadDecimal128Permuted(bd, i&bm, bm == 0)
		lo := al.Sub(bl)
		carry := (al.Xor(sb).BitsToInt64().Less(bl.Xor(sb).BitsToInt64())).ToInt64x4().ToBits()
		hi := ah.Sub(bh).Add(carry)
		mask := (ah.Xor(bh).And(ah.Xor(hi))).BitsToInt64().Less(archsimd.BroadcastInt64x4(0)).ToBits()
		// Lanes stay in 0,2,1,3 order throughout, removing four cross-lane
		// permutations per block. Only the rare first-error index is reordered.
		l, h := lo.InterleaveLoGrouped(hi), lo.InterleaveHiGrouped(hi)
		if mask != 0 {
			logical := (mask & 1) | ((mask & 4) >> 1) | ((mask & 2) << 1) | (mask & 8)
			last := bits.TrailingZeros8(logical)
			var words [8]uint64
			l.Store(words[:4])
			h.Store(words[4:])
			for lane := 0; lane <= last; lane++ {
				out[i+lane] = types.Decimal128{B0_63: words[2*lane], B64_127: words[2*lane+1]}
			}
			return i + last
		}
		l.Store(dst[i*2 : i*2+4])
		h.Store(dst[i*2+4 : i*2+8])
	}
	for ; i < n; i++ {
		x, y := a[i&am], b[i&bm]
		lo, c := bits.Sub64(x.B0_63, y.B0_63, 0)
		hi, _ := bits.Sub64(x.B64_127, y.B64_127, c)
		out[i] = types.Decimal128{B0_63: lo, B64_127: hi}
		if x.B64_127>>63 != y.B64_127>>63 && x.B64_127>>63 != hi>>63 {
			return i
		}
	}
	return -1
}

func loadDecimal128Permuted(v []uint64, i int, constant bool) (archsimd.Uint64x4, archsimd.Uint64x4) {
	if constant {
		return archsimd.BroadcastUint64x4(v[0]), archsimd.BroadcastUint64x4(v[1])
	}
	at := i * 2
	a, b := archsimd.LoadUint64x4(v[at:at+4]), archsimd.LoadUint64x4(v[at+4:at+8])
	return a.InterleaveLoGrouped(b), a.InterleaveHiGrouped(b)
}
