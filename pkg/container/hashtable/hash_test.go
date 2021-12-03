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
	{0x298d35406e, [2]uint64{0x8fb82b95964584d6, 0x002be88bf0d3dd55}, "Discard medicine more than two years old."},
	{0x39f47be063, [2]uint64{0x7860dca374649da4, 0x787a7bcf9ec35e46}, "He who has a shady past knows that nice guys finish last."},
	{0x2a47abad31, [2]uint64{0x802f40d8f473b05c, 0x553c6023273220a6}, "I wouldn't marry him with a ten foot pole."},
	{0x39e798b7a0, [2]uint64{0x7fd59bd907af4ca6, 0xbf4db935a751061d}, "Free! Free!/A trip/to Mars/for 900/empty jars/Burma Shave"},
	{0x3a4bf26149, [2]uint64{0x1dfe620b667f058e, 0x5fa5d3232442c02f}, "The days of the digital watch are numbered.  -Tom Stoppard"},
	{0x1b92642e0e, [2]uint64{0x49d860244ad08724, 0x141e95cfe509d3da}, "Nepal premier won't resign."},
	{0x43d44fe1cd, [2]uint64{0x25ec95d93289c3b2, 0x7228e2d671bb2495}, "For every action there is an equal and opposite government program."},
	{0x39fcec9cdd, [2]uint64{0x772f10bdd103e8f1, 0xdeb958a7d2d4345f}, "His money is twice tainted: 'taint yours and 'taint mine."},
	{0x586e141608, [2]uint64{0x5e21d7cbbae02691, 0x86c1c68e64ce5f90}, "There is no reason for any individual to have a computer in their home. -Ken Olsen, 1977"},
	{0x4b9b00078e, [2]uint64{0x6ad642c8d29f1376, 0x681d80ab59c25960}, "It's a tiny change to the code and not completely disgusting. - Bob Manchek"},
	{0x18a8d48b1d, [2]uint64{0xf4d82605decd9beb, 0x40596f059ca21c96}, "size:  a.out:  bad magic"},
	{0x313ea4ade4, [2]uint64{0x13583ad9751cbac3, 0x22347975cdf322ce}, "The major problem is with sendmail.  -Mark Horton"},
	{0x4863bd93af, [2]uint64{0x19e820da1821f2d8, 0x436cb1241403121c}, "Give me a rock, paper and scissors and I will move the world.  CCFestoon"},
	{0x2e1ae28935, [2]uint64{0x4c92df71e6eac213, 0xedea1b46a32b818a}, "If the enemy is within range, then so are you."},
	{0x4661b28c79, [2]uint64{0xde7a55d30c6d5af6, 0xb6219b4e1af12818}, "It's well we cannot hear the screams/That we create in others' dreams."},
	{0x44c882cd2b, [2]uint64{0x94b1ce1bfe6d8f4b, 0xf2aaf6d18a861636}, "You remind me of a TV show, but that's all right: I watch it anyway."},
	{0x2021d19a3a, [2]uint64{0x8d4d61cdf65ab085, 0x62170d990a0af255}, "C is as portable as Stonehedge!!"},
	{0x58d6857712, [2]uint64{0xa813172c120a45f9, 0xc511248c219dbe28}, "Even if I could be Shakespeare, I think I should still choose to be Faraday. - A. Huxley"},
	{0x846ef8f53a, [2]uint64{0x19903c972b8d5dd6, 0xdfbf46f0a719f0a3}, "The fugacity of a constituent in a mixture of gases at a given temperature is proportional to its mole fraction.  Lewis-Randall Rule"},
	{0x3823212ad8, [2]uint64{0x808e02b3f219e062, 0x83d1ee4ff3c2b7d5}, "How can you write a big system without C++?  -Paul Glick"},
}

func TestGoldenCastagnoli(t *testing.T) {
	for _, g := range golden {
		ptr, length := unsafe.Pointer((*reflect.StringHeader)(unsafe.Pointer(&g.in)).Data), len(g.in)
		if crc := crc32BytesHash(ptr, length); crc != g.crc32 {
			t.Errorf("crc32Hash(%s) = 0x%016x want 0x%016x", g.in, crc, g.crc32)
		}
		if aes := aesBytesHash(ptr, length); aes != g.aes {
			t.Errorf("aesHash(%s) = 0x%016x%016x want 0x%016x%016x", g.in, aes[0], aes[1], g.aes[0], g.aes[1])
		}
	}
}
