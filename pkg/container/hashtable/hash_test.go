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
	Crc32 uint64
	aes   [2]uint64
	in    string
}

var golden = []test{
	{0x298d35406e, [2]uint64{0xbcd8ceaa0c3b7132, 0xe17101ad37bd02eb}, "Discard medicine more than two years old."},
	{0x39f47be063, [2]uint64{0x1ddd521c2799e2b3, 0xfa467adaf17db29e}, "He who has a shady past knows that nice guys finish last."},
	{0x2a47abad31, [2]uint64{0x46be025751067f57, 0xf8f1e8f83d7dd386}, "I wouldn't marry him with a ten foot pole."},
	{0x39e798b7a0, [2]uint64{0x1e310bb1509b5132, 0x794bf22cac73e7e3}, "Free! Free!/A trip/to Mars/for 900/empty jars/Burma Shave"},
	{0x3a4bf26149, [2]uint64{0x3d9abeaf2ac6cbc0, 0x65e71d1b9131b921}, "The days of the digital watch are numbered.  -Tom Stoppard"},
	{0x1b92642e0e, [2]uint64{0x7d6d54e5585f8e84, 0x69d33e498d22fedf}, "Nepal premier won't resign."},
	{0x43d44fe1cd, [2]uint64{0x76f8cc87eef70bcb, 0xbd9b15e809d5d9ad}, "For every action there is an equal and opposite government program."},
	{0x39fcec9cdd, [2]uint64{0x4300d9b8c8e4e860, 0x9eadecd2fc3d983f}, "His money is twice tainted: 'taint yours and 'taint mine."},
	{0x586e141608, [2]uint64{0xaaf763b4c1f56b15, 0x9deb028a6625d36c}, "There is no reason for any individual to have a computer in their home. -Ken Olsen, 1977"},
	{0x4b9b00078e, [2]uint64{0x0f5adc85bfda7fad, 0x8bb65d9fcae50d95}, "It's a tiny change to the code and not completely disgusting. - Bob Manchek"},
	{0x18a8d48b1d, [2]uint64{0x9f774873d37eae5c, 0x9e89c88a24c356df}, "size:  a.out:  bad magic"},
	{0x313ea4ade4, [2]uint64{0x403be1fcae823c9a, 0xab0a1bdc3180a439}, "The major problem is with sendmail.  -Mark Horton"},
	{0x4863bd93af, [2]uint64{0xbc15370a24046627, 0x313d1802cfb299aa}, "Give me a rock, paper and scissors and I will move the world.  CCFestoon"},
	{0x2e1ae28935, [2]uint64{0x1b41bad2bf58af63, 0x5ad4e4cc501f7caf}, "If the enemy is within range, then so are you."},
	{0x4661b28c79, [2]uint64{0x5e12de6b8cbec9ec, 0x87b98b8be1803426}, "It's well we cannot hear the screams/That we create in others' dreams."},
	{0x44c882cd2b, [2]uint64{0x4b20e77a8121282a, 0xa41c7e181b7222af}, "You remind me of a TV show, but that's all right: I watch it anyway."},
	{0x2021d19a3a, [2]uint64{0x83cea3070bacafa5, 0xc2a2af6b324e2472}, "C is as portable as Stonehedge!!"},
	{0x58d6857712, [2]uint64{0x12e4a8f6204ec452, 0xa798559ed3069dcc}, "Even if I could be Shakespeare, I think I should still choose to be Faraday. - A. Huxley"},
	{0x846ef8f53a, [2]uint64{0x509bdf13d006516c, 0xa64c027cec6bf0b2}, "The fugacity of a constituent in a mixture of gases at a given temperature is proportional to its mole fraction.  Lewis-Randall Rule"},
	{0x3823212ad8, [2]uint64{0xfe8b040f96447d67, 0x3efd8d6f714bc480}, "How can you write a big system without C++?  -Paul Glick"},
}

func TestHashFn(t *testing.T) {
	for _, g := range golden {
		ptr, length := unsafe.Pointer((*reflect.StringHeader)(unsafe.Pointer(&g.in)).Data), len(g.in)
		if crc := Crc32BytesHash(ptr, length); crc != g.Crc32 {
			t.Errorf("Crc32Hash(%s) = 0x%016x want 0x%016x", g.in, crc, g.Crc32)
		}
		if aes := AesBytesHash(ptr, length); aes != g.aes {
			t.Errorf("aesHash(%s) = 0x%016x%016x want 0x%016x%016x", g.in, aes[0], aes[1], g.aes[0], g.aes[1])
		}
	}
}
