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
	"math/rand"
	"unsafe"
)

var hashkey [4]uint64

func init() {
	hashkey[0] = rand.Uint64()
	hashkey[1] = rand.Uint64()
	hashkey[2] = rand.Uint64()
	hashkey[3] = rand.Uint64()
}

const (
	m1 = 0xa0761d6478bd642f
	m2 = 0xe7037ed1a0b428db
	m3 = 0x8ebc6af09c88c6e3
	m4 = 0x589965cc75374cc3
	m5 = 0x1d8e4e27c47d124f
)

func Crc32BytesHash(data unsafe.Pointer, length int) uint64
func Crc32Int64Hash(data uint64) uint64
func Crc32Int64SliceHash(data *uint64, length int) uint64

func Crc32Int64BatchHash(data unsafe.Pointer, hashes *uint64, length int)
func Crc32Int64CellBatchHash(data unsafe.Pointer, hashes *uint64, length int)

func AesBytesHash(data unsafe.Pointer, length int) [2]uint64

func Crc32Int192Hash(data *[3]uint64) uint64
func Crc32Int192BatchHash(data *[3]uint64, hashes *uint64, length int)

func Crc32Int256Hash(data *[4]uint64) uint64
func Crc32Int256BatchHash(data *[4]uint64, hashes *uint64, length int)

func Crc32Int320Hash(data *[5]uint64) uint64
func Crc32Int320BatchHash(data *[5]uint64, hashes *uint64, length int)

func wyhash(data unsafe.Pointer, seed, s uint64) uint64 {
	var a, b uint64
	seed ^= hashkey[0] ^ m1
	switch {
	case s == 0:
		return seed
	case s < 4:
		a = uint64(*(*byte)(data))
		a |= uint64(*(*byte)(unsafe.Add(data, s>>1))) << 8
		a |= uint64(*(*byte)(unsafe.Add(data, s-1))) << 16
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
				data = unsafe.Add(data, 48)
			}
			seed ^= seed1 ^ seed2
		}
		for ; l > 16; l -= 16 {
			seed = mix(r8(data, 0)^m2, r8(data, 8)^seed)
			data = unsafe.Add(data, 16)
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

func r4(data unsafe.Pointer, p uint64) uint64 {
	return uint64(*(*uint32)(unsafe.Add(data, p)))
}

func r8(data unsafe.Pointer, p uint64) uint64 {
	return *(*uint64)(unsafe.Add(data, p))
}

func wyhash64(x uint64) uint64 {
	return mix(m5^8, mix(x^m2, x^hashkey[1]^hashkey[0]^m1))
}

func wyhash64Batch(data unsafe.Pointer, hashes *uint64, length int) {
	dataSlice := unsafe.Slice((*uint64)(data), length)
	hashSlice := unsafe.Slice(hashes, length)

	for i := 0; i < length; i++ {
		hashSlice[i] = wyhash64(dataSlice[i])
	}
}

func wyhash64CellBatch(data unsafe.Pointer, hashes *uint64, length int) {
	dataSlice := unsafe.Slice((*Int64HashMapCell)(data), length)
	hashSlice := unsafe.Slice(hashes, length)

	for i := 0; i < length; i++ {
		hashSlice[i] = wyhash64(dataSlice[i].Key)
	}
}

func dummyBytesHash(data unsafe.Pointer, length int) [2]uint64 {
	var hash [2]uint64
	copy(unsafe.Slice((*byte)(unsafe.Pointer(&hash[0])), 16), unsafe.Slice((*byte)(data), length))
	return hash
}
