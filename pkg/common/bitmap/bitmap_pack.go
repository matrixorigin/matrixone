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

package bitmap

import (
	"encoding/binary"
	"unsafe"
)

// Trusted by the Gobra proof (contract in bitmap_pack.gobra): Gobra cannot
// verify unsafe code, so this file is not part of the verified core.

// packMagic gathers bit 0 of each byte of a word into its top byte: byte j
// of x times packMagic lands in bit 56+j of the product, and no two
// partial products overlap there when every byte is 0 or 1.
const packMagic = 0x0102040810204080

// lowBits keeps bit 0 of every byte.
const lowBits = 0x0101010101010101

// packBools returns the word whose bit k is values[base+k] for the at most
// 64 rows [base, end). A Go bool occupies one byte, so eight of them are read
// as one little-endian word and packed with one multiply. A byte other than
// 0 or 1 still reads as true, as it does in an if: each byte is first folded
// to whether any of its bits is set.
func packBools(values []bool, base, end int) uint64 {
	n := end - base
	if n <= 0 {
		return 0
	}
	bs := unsafe.Slice((*byte)(unsafe.Pointer(&values[base])), n)
	var word uint64
	k := 0
	for ; k+8 <= n; k += 8 {
		x := binary.LittleEndian.Uint64(bs[k:])
		// Fold each byte onto its bit 0. The right shifts move bits of the
		// next byte only into bits 1 to 7 of this one, which lowBits drops.
		x |= x >> 4
		x |= x >> 2
		x |= x >> 1
		word |= (((x & lowBits) * packMagic) >> 56) << uint(k)
	}
	for ; k < n; k++ {
		if bs[k] != 0 {
			word |= 1 << uint(k)
		}
	}
	return word
}
