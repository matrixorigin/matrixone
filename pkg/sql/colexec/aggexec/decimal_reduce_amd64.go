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
	lo, hi := archsimd.BroadcastUint64x4(0), archsimd.BroadcastUint64x4(0)
	ones := archsimd.BroadcastUint64x4(^uint64(0))
	for r.rows+4 <= n {
		var add archsimd.Uint64x4
		if width == 1 {
			at := offset + r.rows
			add = archsimd.LoadUint64x4(data[at : at+4])
		} else {
			var vals [4]uint64
			valid := true
			for lane := 0; lane < 4; lane++ {
				at := (offset + r.rows + lane) * width
				vals[lane] = data[at]
				sign := uint64(int64(vals[lane]) >> 63)
				for limb := 1; limb < width; limb++ {
					if data[at+limb] != sign {
						valid = false
					}
				}
			}
			if !valid {
				break
			}
			add = archsimd.LoadUint64x4(vals[:])
		}
		next := lo.Add(add)
		carry := lo.And(add).Or(lo.Or(add).And(next.Xor(ones))).ShiftAllRight(63)
		hi = hi.Add(add.AsInt64x4().ShiftAllRight(63).AsUint64x4()).Add(carry)
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
