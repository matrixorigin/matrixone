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
	"io"
	"os"
	"reflect"
	"testing"
	"unsafe"
)

type test struct {
	crc32 uint64
	aes   [2]uint64
	in    string
}

var golden = []test{
	{0x298d35406e, [2]uint64{0x1ecb55293a89b7c2, 0x9630bc6e8c09539c}, "Discard medicine more than two years old."},
	{0x39f47be063, [2]uint64{0x383f5d01e938e757, 0x6b209f4cb8ede896}, "He who has a shady past knows that nice guys finish last."},
	{0x2a47abad31, [2]uint64{0x7ba28beb4cde5dfc, 0x09f5979a66adf578}, "I wouldn't marry him with a ten foot pole."},
	{0x39e798b7a0, [2]uint64{0x68bcfe529d368d86, 0xb8f2cf713d5b6137}, "Free! Free!/A trip/to Mars/for 900/empty jars/Burma Shave"},
	{0x3a4bf26149, [2]uint64{0x80c4531b7d1fc943, 0x6918be8d85f7f3a8}, "The days of the digital watch are numbered.  -Tom Stoppard"},
	{0x1b92642e0e, [2]uint64{0xec64e744126132b5, 0x3f6a8ccfdba7dea9}, "Nepal premier won't resign."},
	{0x43d44fe1cd, [2]uint64{0xd765c174569c492c, 0x6fa8e7f93ce09d11}, "For every action there is an equal and opposite government program."},
	{0x39fcec9cdd, [2]uint64{0x0f1d0cd8c2f03939, 0xdfb729d19b937086}, "His money is twice tainted: 'taint yours and 'taint mine."},
	{0x586e141608, [2]uint64{0x97d04bf77b9af7ca, 0x326097856e019e38}, "There is no reason for any individual to have a computer in their home. -Ken Olsen, 1977"},
	{0x4b9b00078e, [2]uint64{0x86603fc7d3317440, 0x56ff38ba789eeda8}, "It's a tiny change to the code and not completely disgusting. - Bob Manchek"},
	{0x18a8d48b1d, [2]uint64{0xe4841df2c09f139f, 0x0c646a0f7e4c73b1}, "size:  a.out:  bad magic"},
	{0x313ea4ade4, [2]uint64{0xc5552b9ca6485df3, 0xb1ef71ccd24ae45c}, "The major problem is with sendmail.  -Mark Horton"},
	{0x4863bd93af, [2]uint64{0x79d3e1508cdacf95, 0x956d3bb2d0686467}, "Give me a rock, paper and scissors and I will move the world.  CCFestoon"},
	{0x2e1ae28935, [2]uint64{0xf8ae93a01d017ac1, 0x04e2ab35f00e0fb1}, "If the enemy is within range, then so are you."},
	{0x4661b28c79, [2]uint64{0xc103df9771dc8372, 0xe238c1071e0c6556}, "It's well we cannot hear the screams/That we create in others' dreams."},
	{0x44c882cd2b, [2]uint64{0x49ddced52c42e4c3, 0x6d48d39ab768716b}, "You remind me of a TV show, but that's all right: I watch it anyway."},
	{0x2021d19a3a, [2]uint64{0x864ed090411e7cbf, 0x81eb33c958b79bd1}, "C is as portable as Stonehedge!!"},
	{0x58d6857712, [2]uint64{0x0ded8eb050cd5170, 0x45962ddc4730d77a}, "Even if I could be Shakespeare, I think I should still choose to be Faraday. - A. Huxley"},
	{0x846ef8f53a, [2]uint64{0x521ed92aaa6b6cc4, 0xda73e86630790406}, "The fugacity of a constituent in a mixture of gases at a given temperature is proportional to its mole fraction.  Lewis-Randall Rule"},
	{0x3823212ad8, [2]uint64{0xf8aec007e0d3f234, 0x060cd6a68f902cb2}, "How can you write a big system without C++?  -Paul Glick"},
}

func TestHashFn(t *testing.T) {
	for _, g := range golden {
		ptr, length := unsafe.Pointer((*reflect.StringHeader)(unsafe.Pointer(&g.in)).Data), len(g.in)
		if crc := Crc32BytesHash(ptr, length); crc != g.crc32 {
			t.Errorf("crc32Hash(%s) = 0x%016x want 0x%016x", g.in, crc, g.crc32)
		}
		if aes := AesBytesHash(ptr, length); aes != g.aes {
			t.Errorf("aesHash(%s) = 0x%016x%016x want 0x%016x%016x", g.in, aes[0], aes[1], g.aes[0], g.aes[1])
		}
	}
}

func BenchmarkCrc32Int192HashBatch(b *testing.B) {
	var data [256 * 1000][3]uint64
	var hashes [256 * 1000]uint64

	f, _ := os.Open("/dev/random")
	io.ReadFull(f, unsafe.Slice((*byte)(unsafe.Pointer(&data)), 256*1000*3))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000; j++ {
			Crc32Int192BatchHash(&data[j*256], &hashes[0], 256)
		}
	}
}

func BenchmarkCrc32Int256HashBatch(b *testing.B) {
	var data [256 * 1000][4]uint64
	var hashes [256 * 1000]uint64

	f, _ := os.Open("/dev/random")
	io.ReadFull(f, unsafe.Slice((*byte)(unsafe.Pointer(&data)), 256*1000*4))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000; j++ {
			Crc32Int256BatchHash(&data[j*256], &hashes[0], 256)
		}
	}
}

func BenchmarkCrc32Int320HashBatch(b *testing.B) {
	var data [256 * 1000][5]uint64
	var hashes [256 * 1000]uint64

	f, _ := os.Open("/dev/random")
	io.ReadFull(f, unsafe.Slice((*byte)(unsafe.Pointer(&data)), 256*1000*5))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000; j++ {
			Crc32Int320BatchHash(&data[j*256], &hashes[0], 256)
		}
	}
}

func BenchmarkAesInt192HashBatch(b *testing.B) {
	var data [256 * 1000][3]uint64
	var hashes [256 * 1000]uint64

	f, _ := os.Open("/dev/random")
	io.ReadFull(f, unsafe.Slice((*byte)(unsafe.Pointer(&data)), 256*1000*3))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000; j++ {
			AesInt192BatchHash(&data[j*256], &hashes[0], 256)
		}
	}
}

func BenchmarkAesInt256HashBatch(b *testing.B) {
	var data [256 * 1000][4]uint64
	var hashes [256 * 1000]uint64

	f, _ := os.Open("/dev/random")
	io.ReadFull(f, unsafe.Slice((*byte)(unsafe.Pointer(&data)), 256*1000*4))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000; j++ {
			AesInt256BatchHash(&data[j*256], &hashes[0], 256)
		}
	}
}

func BenchmarkAesInt320HashBatch(b *testing.B) {
	var data [256 * 1000][5]uint64
	var hashes [256 * 1000]uint64

	f, _ := os.Open("/dev/random")
	io.ReadFull(f, unsafe.Slice((*byte)(unsafe.Pointer(&data)), 256*1000*5))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000; j++ {
			AesInt320BatchHash(&data[j*256], &hashes[0], 256)
		}
	}
}
