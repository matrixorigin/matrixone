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

//go:build amd64 || arm64

package hashtable

import (
	"math/bits"
	"math/rand"
	"unsafe"
)

var (
	Int64BatchHash                  = wyhashInt64Batch
	Int64HashWithFixedSeed          = wyhash64WithFixedSeed
	BytesBatchGenHashStates         = wyhashBytesBatch
	BytesBatchGenHashStatesWithSeed = wyhashBytesBatchWithSeed
	Int192BatchGenHashStates        = wyhashInt192Batch
	Int256BatchGenHashStates        = wyhashInt256Batch
	Int320BatchGenHashStates        = wyhashInt320Batch
)

// Hashing algorithm inspired by
// wyhash: https://github.com/wangyi-fudan/wyhash

var randseed uint64
var hashkey [4]uint64

func init() {
	randseed = rand.Uint64()
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

func wyhash(p unsafe.Pointer, seed, s uint64) uint64 {
	var a, b uint64
	seed ^= hashkey[0] ^ m1
	switch {
	case s == 0:
		return seed
	case s < 4:
		a = uint64(*(*byte)(p))
		a |= uint64(*(*byte)(unsafe.Add(p, s>>1))) << 8
		a |= uint64(*(*byte)(unsafe.Add(p, s-1))) << 16
	case s == 4:
		a = r4(p)
		b = a
	case s < 8:
		a = r4(p)
		b = r4(unsafe.Add(p, s-4))
	case s == 8:
		a = r8(p)
		b = a
	case s <= 16:
		a = r8(p)
		b = r8(unsafe.Add(p, s-8))
	default:
		l := s
		if l > 48 {
			seed1 := seed
			seed2 := seed
			for ; l > 48; l -= 48 {
				seed = mix(r8(p)^m2, r8(unsafe.Add(p, 8))^seed)
				seed1 = mix(r8(unsafe.Add(p, 16))^m3, r8(unsafe.Add(p, 24))^seed1)
				seed2 = mix(r8(unsafe.Add(p, 32))^m4, r8(unsafe.Add(p, 40))^seed2)
				p = unsafe.Add(p, 48)
			}
			seed ^= seed1 ^ seed2
		}
		for ; l > 16; l -= 16 {
			seed = mix(r8(p)^m2, r8(unsafe.Add(p, 8))^seed)
			p = unsafe.Add(p, 16)
		}
		a = r8(unsafe.Add(p, l-16))
		b = r8(unsafe.Add(p, l-8))
	}

	return mix(m5^s, mix(a^m2, b^seed))
}

func wyhash64WithFixedSeed(x uint64) uint64 {
	return mix(m5^8, mix(x^m2, x^m3^m4^m1))
}

func wyhash64(x, seed uint64) uint64 {
	return mix(m5^8, mix(x^m2, x^seed^hashkey[0]^m1))
}

func mix(a, b uint64) uint64 {
	hi, lo := bits.Mul64(uint64(a), uint64(b))
	return uint64(hi ^ lo)
}

func r4(p unsafe.Pointer) uint64 {
	return uint64(*(*uint32)(p))
}

func r8(p unsafe.Pointer) uint64 {
	return *(*uint64)(p)
}

func wyhashInt64Batch(data unsafe.Pointer, hashes *uint64, length int) {
	dataSlice := unsafe.Slice((*uint64)(data), length)
	hashSlice := unsafe.Slice(hashes, length)

	for i := 0; i < length; i++ {
		hashSlice[i] = wyhash64(dataSlice[i], randseed)
	}
}

func wyhashBytesBatch(data *[]byte, states *[3]uint64, length int) {
	dataSlice := unsafe.Slice((*[]byte)(data), length)
	hashSlice := unsafe.Slice((*[3]uint64)(states), length)
	for i := 0; i < length; i++ {
		hashSlice[i][0] = wyhash(unsafe.Pointer(&dataSlice[i][0]), randseed, uint64(len(dataSlice[i])))
		hashSlice[i][1] = wyhash(unsafe.Pointer(&dataSlice[i][0]), randseed<<32, uint64(len(dataSlice[i])))
		hashSlice[i][2] = wyhash(unsafe.Pointer(&dataSlice[i][0]), randseed>>32, uint64(len(dataSlice[i])))
	}
}

func wyhashBytesBatchWithSeed(data *[]byte, states *[3]uint64, length int, seed uint64) {
	dataSlice := unsafe.Slice((*[]byte)(data), length)
	hashSlice := unsafe.Slice((*[3]uint64)(states), length)
	for i := 0; i < length; i++ {
		hashSlice[i][0] = wyhash(unsafe.Pointer(&dataSlice[i][0]), seed, uint64(len(dataSlice[i])))
		hashSlice[i][1] = wyhash(unsafe.Pointer(&dataSlice[i][0]), seed<<32, uint64(len(dataSlice[i])))
		hashSlice[i][2] = wyhash(unsafe.Pointer(&dataSlice[i][0]), seed>>32, uint64(len(dataSlice[i])))
	}
}

func wyhashInt192Batch(data *[3]uint64, states *[3]uint64, length int) {
	dataSlice := unsafe.Slice((*[3]uint64)(data), length)
	hashSlice := unsafe.Slice((*[3]uint64)(states), length)
	for i := 0; i < length; i++ {
		hashSlice[i][0] = wyhash(unsafe.Pointer(&dataSlice[i][0]), randseed, 24)
		hashSlice[i][1] = wyhash(unsafe.Pointer(&dataSlice[i][0]), randseed<<32, 24)
		hashSlice[i][2] = wyhash(unsafe.Pointer(&dataSlice[i][0]), randseed>>32, 24)
	}
}

func wyhashInt256Batch(data *[4]uint64, states *[3]uint64, length int) {
	dataSlice := unsafe.Slice((*[4]uint64)(data), length)
	hashSlice := unsafe.Slice((*[3]uint64)(states), length)
	for i := 0; i < length; i++ {
		hashSlice[i][0] = wyhash(unsafe.Pointer(&dataSlice[i][0]), randseed, 32)
		hashSlice[i][1] = wyhash(unsafe.Pointer(&dataSlice[i][0]), randseed<<32, 32)
		hashSlice[i][2] = wyhash(unsafe.Pointer(&dataSlice[i][0]), randseed>>32, 32)
	}
}

func wyhashInt320Batch(data *[5]uint64, states *[3]uint64, length int) {
	dataSlice := unsafe.Slice((*[5]uint64)(data), length)
	hashSlice := unsafe.Slice((*[3]uint64)(states), length)
	for i := 0; i < length; i++ {
		hashSlice[i][0] = wyhash(unsafe.Pointer(&dataSlice[i][0]), randseed, 40)
		hashSlice[i][1] = wyhash(unsafe.Pointer(&dataSlice[i][0]), randseed<<32, 40)
		hashSlice[i][2] = wyhash(unsafe.Pointer(&dataSlice[i][0]), randseed>>32, 40)
	}
}
