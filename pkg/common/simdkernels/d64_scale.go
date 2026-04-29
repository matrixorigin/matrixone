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

package simdkernels

import "math/bits"

// Decimal64 scale-into-rs (multiply each element by a constant uint64 factor
// f = 10^scaleDiff), used by Add/Sub when input scales differ.
//
// Two variants:
//   *Unchecked — caller has prescanned that every |vec[i]| ≤ MaxInt64/f, so
//                a plain truncating 64-bit multiply suffices. Hot path for the
//                common diff-scale add/sub case where data magnitude is small.
//   *Checked   — uses bits.Mul64 / 128-bit product, returns the index of the
//                first overflowing element or -1.
//
// Signature matches the d64_addsub.go convention: slices are uint64 reinterpret
// of the underlying Decimal64. Sign is encoded in bit 63.

var (
	D64ScaleUnchecked func(vec, rs []uint64, f uint64)     = scalarD64ScaleUnchecked
	D64ScaleChecked   func(vec, rs []uint64, f uint64) int = scalarD64ScaleChecked
)

func scalarD64ScaleUnchecked(vec, rs []uint64, f uint64) {
	n := len(rs)
	if len(vec) < n {
		return
	}
	for i := 0; i < n; i++ {
		signBit := vec[i] >> 63
		mask := -signBit
		abs := (vec[i] ^ mask) + signBit
		rs[i] = (abs*f ^ mask) + signBit
	}
}

func scalarD64ScaleChecked(vec, rs []uint64, f uint64) int {
	n := len(rs)
	if len(vec) < n {
		return -1
	}
	for i := 0; i < n; i++ {
		signBit := vec[i] >> 63
		mask := -signBit
		abs := (vec[i] ^ mask) + signBit
		hi, lo := bits.Mul64(abs, f)
		if hi|(lo>>63) != 0 {
			return i
		}
		rs[i] = (lo ^ mask) + signBit
	}
	return -1
}
