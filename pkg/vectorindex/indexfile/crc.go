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

package indexfile

import (
	"fmt"
	"hash/crc32"
)

// castagnoliReversed is the CRC32-C polynomial in the bit order hash/crc32 uses.
const castagnoliReversed = 0x82f63b78

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Checksum is the checksum recorded for an index file of content b: its CRC32-C as eight
// lowercase hex digits.
func Checksum(b []byte) string {
	return formatCRC(crc32.Checksum(b, castagnoli))
}

func formatCRC(crc uint32) string {
	return fmt.Sprintf("%08x", crc)
}

// isMD5 reports whether checksum is the hex MD5 recorded for files written before CRC32-C.
func isMD5(checksum string) bool {
	return len(checksum) == 32
}

// crcCombine returns the CRC32-C of a followed by b from crcA, crcB and len(b) in bytes.
func crcCombine(crcA, crcB uint32, lenB int64) uint32 {
	return multModP(xPow8n(lenB), crcA) ^ crcB
}

// multModP multiplies a and b modulo the CRC32-C polynomial.
func multModP(a, b uint32) uint32 {
	var p uint32
	for m := uint32(1) << 31; m != 0; m >>= 1 {
		if a&m != 0 {
			p ^= b
		}
		if b&1 != 0 {
			b = b>>1 ^ castagnoliReversed
		} else {
			b >>= 1
		}
	}
	return p
}

// xPow8n returns x^(8n) modulo the CRC32-C polynomial.
func xPow8n(n int64) uint32 {
	p := uint32(1) << 31
	x8 := uint32(1) << 23
	for e := uint64(n); e != 0; e >>= 1 {
		if e&1 != 0 {
			p = multModP(p, x8)
		}
		x8 = multModP(x8, x8)
	}
	return p
}
