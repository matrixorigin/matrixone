// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hashtable

import (
	"math/bits"
	"unsafe"
)

var BytesHash func([]byte) uint64
var IntHash func(uint64) uint64

const (
	m1 = 0xa0761d6478bd642f
	m2 = 0xe7037ed1a0b428db
	m3 = 0x8ebc6af09c88c6e3
	m4 = 0x589965cc75374cc3
	m5 = 0x1d8e4e27c47d124f
)

func wyhash(data []byte) uint64 {
	s := len(data)
	var a, b uint64
	seed := uint64(m3 ^ m1)
	switch {
	case s == 0:
		return seed
	case s < 4:
		a = uint64(data[0])
		a |= uint64(data[s>>1]) << 8
		a |= uint64(data[s-1]) << 16
	case s == 4:
		a = r4(data, 0)
		b = a
	case s < 8:
		a = r4(data, 0)
		b = r4(data, s-4)
	case s == 8:
		a = r8(data, 0)
		b = a
	case s <= 16:
		a = r8(data, 0)
		b = r8(data, s-8)
	default:
		l := s
		if l > 48 {
			seed1 := seed
			seed2 := seed
			for ; l > 48; l -= 48 {
				seed = mix(r8(data, 0)^m2, r8(data, 8)^seed)
				seed1 = mix(r8(data, 16)^m3, r8(data, 24)^seed1)
				seed2 = mix(r8(data, 32)^m4, r8(data, 40)^seed2)
				data = data[48:]
			}
			seed ^= seed1 ^ seed2
		}
		for ; l > 16; l -= 16 {
			seed = mix(r8(data, 0)^m2, r8(data, 8)^seed)
			data = data[16:]
		}
		a = r8(data, l-16)
		b = r8(data, l-8)
	}

	return mix(m5^uint64(s), mix(a^m2, b^seed))
}

func mix(a, b uint64) uint64 {
	hi, lo := bits.Mul64(uint64(a), uint64(b))
	return hi ^ lo
}

func r4(data []byte, p int) uint64 {
	return uint64(*(*uint32)(unsafe.Pointer(&data[p])))
}

func r8(data []byte, p int) uint64 {
	return *(*uint64)(unsafe.Pointer(&data[p]))
}

func intHash64(x uint64) uint64 {
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	x *= 0xc4ceb9fe1a85ec53
	x ^= x >> 33

	return x
}
