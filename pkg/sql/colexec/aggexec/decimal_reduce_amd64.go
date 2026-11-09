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

package aggexec

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"

	"simd/archsimd"
)

func decimalReductionAvailable() bool { return archsimd.X86.AVX2() }

// No state or arithmetic is published before the complete span is admitted.
func decimalReductionGroup(groups []uint64) (uint64, bool) {
	if len(groups) == 0 || groups[0] == GroupNotMatched {
		return 0, false
	}
	group := groups[0]
	if groups[len(groups)-1] != group {
		return 0, false
	}
	want := archsimd.BroadcastUint64x4(group)
	i := 0
	for ; i+4 <= len(groups); i += 4 {
		if !archsimd.LoadUint64x4(groups[i : i+4]).Xor(want).IsZero() {
			return 0, false
		}
	}
	for ; i < len(groups); i++ {
		if groups[i] != group {
			return 0, false
		}
	}
	return group, true
}

func reduceDecimalPrefix(v *vector.Vector, offset int, n int) (r decimalReductionPrefix) {
	if n < 256 || !archsimd.X86.AVX2() || v.IsConst() || v.HasNull() || v.HasGrouping() {
		return
	}
	width := 0
	switch v.GetType().Oid {
	case types.T_decimal64:
		width = 1
	case types.T_decimal128:
		width = 2
	case types.T_decimal256:
		width = 4
	default:
		return
	}

	return reduceDecimalContiguous(types.DecodeSlice[uint64](v.GetData()), offset, n, width)
}

// Contiguous reduction needs neither group matching nor per-group masked accumulators.
func reduceDecimalContiguous(data []uint64, offset, n, width int) (r decimalReductionPrefix) {
	zero := archsimd.BroadcastUint64x4(0)
	lo, hi := zero, zero
	ones := archsimd.BroadcastUint64x4(^uint64(0))
blocks:
	for r.rows+4 <= n {
		var add, sign archsimd.Uint64x4
		switch width {
		case 1:
			at := offset + r.rows
			add = archsimd.LoadUint64x4(data[at : at+4])
			sign = add.BitsToInt64().Less(zero.BitsToInt64()).ToInt64x4().ToBits()
		case 2:
			at := (offset + r.rows) * 2
			a := archsimd.LoadUint64x4(data[at : at+4])
			b := archsimd.LoadUint64x4(data[at+4 : at+8])
			// Retain row order 0,2,1,3 in the lanes; horizontal summation
			// does not need an additional cross-lane permutation.
			add = a.InterleaveLoGrouped(b)
			sign = add.BitsToInt64().Less(zero.BitsToInt64()).ToInt64x4().ToBits()
			if !a.InterleaveHiGrouped(b).Xor(sign).IsZero() {
				break blocks
			}
		case 4:
			at := (offset + r.rows) * 4
			a := archsimd.LoadUint64x4(data[at : at+4])
			b := archsimd.LoadUint64x4(data[at+4 : at+8])
			c := archsimd.LoadUint64x4(data[at+8 : at+12])
			d := archsimd.LoadUint64x4(data[at+12 : at+16])
			acLo, bdLo := a.InterleaveLoGrouped(c), b.InterleaveLoGrouped(d)
			acHi, bdHi := a.InterleaveHiGrouped(c), b.InterleaveHiGrouped(d)
			add = acLo.ConcatPermute128Scalars(0, 2, bdLo)
			sign = add.BitsToInt64().Less(zero.BitsToInt64()).ToInt64x4().ToBits()
			h1 := acHi.ConcatPermute128Scalars(0, 2, bdHi)
			h2 := acLo.ConcatPermute128Scalars(1, 3, bdLo)
			h3 := acHi.ConcatPermute128Scalars(1, 3, bdHi)
			if !h1.Xor(sign).Or(h2.Xor(sign)).Or(h3.Xor(sign)).IsZero() {
				break blocks
			}
		default:
			return r
		}
		next := lo.Add(add)
		carry := lo.And(add).Or(lo.Or(add).And(next.Xor(ones))).ShiftAllRight(63)
		hi = hi.Add(sign).Add(carry)
		lo = next
		r.rows += 4
	}
	if r.rows == 0 {
		return
	}

	r.count = int64(r.rows)
	var ls, hs [4]uint64
	lo.Store(ls[:])
	hi.Store(hs[:])
	for lane := 0; lane < 4; lane++ {
		r.sum = r.sum.Add128Unchecked(types.Decimal128{B0_63: ls[lane], B64_127: hs[lane]})
	}
	return
}
