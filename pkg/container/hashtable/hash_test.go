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
	"testing"
)

var data = [][]byte{
	[]byte(""),
	[]byte("a"),
	[]byte("ab"),
	[]byte("abc"),
	[]byte("abcd"),
	[]byte("abcde"),
	[]byte("abcdef"),
	[]byte("abcdefg"),
	[]byte("abcdefgh"),
	[]byte("abcdefghi"),
	[]byte("abcdefghij"),
	[]byte("abcdefghijk"),
	[]byte("abcdefghijkl"),
	[]byte("abcdefghijklm"),
	[]byte("abcdefghijklmn"),
	[]byte("abcdefghijklmno"),
	[]byte("abcdefghijklmnop"),

	[]byte("aaaaaaaaaaaaaaaa"),
	[]byte("aaaaaaaaaaaaaaab"),
	[]byte("aaaaaaaaaaaaaabb"),
	[]byte("aaaaaaaaaaaaabbb"),
	[]byte("aaaaaaaaaaaabbbb"),
	[]byte("aaaaaaaaaaabbbbb"),
	[]byte("aaaaaaaaaabbbbbb"),
	[]byte("aaaaaaaaabbbbbbb"),
	[]byte("aaaaaaaabbbbbbbb"),
	[]byte("aaaaaaabbbbbbbbb"),
	[]byte("aaaaaabbbbbbbbbb"),
	[]byte("aaaaabbbbbbbbbbb"),
	[]byte("aaaabbbbbbbbbbbb"),
	[]byte("aaabbbbbbbbbbbbb"),
	[]byte("aabbbbbbbbbbbbbb"),
	[]byte("abbbbbbbbbbbbbbb"),
	[]byte("bbbbbbbbbbbbbbbb"),

	[]byte("Discard medicine more than two years old."),
	[]byte("He who has a shady past knows that nice guys finish last."),
	[]byte("I wouldn't marry him with a ten foot pole."),
	[]byte("Free! Free!/A trip/to Mars/for 900/empty jars/Burma Shave"),
	[]byte("The days of the digital watch are numbered.  -Tom Stoppard"),
	[]byte("Nepal premier won't resign."),
	[]byte("For every action there is an equal and opposite government program."),
	[]byte("His money is twice tainted: 'taint yours and 'taint mine."),
	[]byte("There is no reason for any individual to have a computer in their home. -Ken Olsen, 1977"),
	[]byte("It's a tiny change to the code and not completely disgusting. - Bob Manchek"),
	[]byte("size:  a.out:  bad magic"),
	[]byte("The major problem is with sendmail.  -Mark Horton"),
	[]byte("Give me a rock, paper and scissors and I will move the world.  CCFestoon"),
	[]byte("If the enemy is within range, then so are you."),
	[]byte("It's well we cannot hear the screams/That we create in others' dreams."),
	[]byte("You remind me of a TV show, but that's all right: I watch it anyway."),
	[]byte("C is as portable as Stonehedge!!"),
	[]byte("Even if I could be Shakespeare, I think I should still choose to be Faraday. - A. Huxley"),
	[]byte("The fugacity of a constituent in a mixture of gases at a given temperature is proportional to its mole fraction.  Lewis-Randall Rule"),
	[]byte("How can you write a big system without C++?  -Paul Glick"),
}

var golden = [][3]uint64{
	{0x75d92531ee43e99f, 0xf6911ed675df8f67, 0x695d59fa38d57a7d},
	{0x700708d94571bcca, 0xf6911ed6b7683812, 0x695d59fa38d57a7d},
	{0x3ef81fc958e80fb2, 0xf6911ed6b7683812, 0xb783fe8338d57a7d},
	{0x8b983e1d6a78533b, 0xf6911ed6b7683812, 0xb783fe83b6d2f3f3},
	{0x9a5d587cd1c5e8c6, 0xb8f839f1b7683812, 0xb783fe83b6d2f3f3},
	{0xf42736b6a942b1e1, 0xc821e058b7683812, 0xb783fe83b6d2f3f3},
	{0x7e2a5cb676c2724f, 0xc821e0581cc375f4, 0xb783fe83b6d2f3f3},
	{0xd734b0232aa7ef1e, 0xc821e0581cc375f4, 0x5c4ed868b6d2f3f3},
	{0x1cdbea01cbfc0e3c, 0xc821e0581cc375f4, 0x5c4ed868cc95cece},
	{0xcf7893ea154e536d, 0xc821e0581cc375f4, 0x5c4ed868b64a116b},
	{0xca202638b9f831be, 0x9b7246ad1cc375f4, 0x5c4ed868b64a116b},
	{0x58b3e0512ed86eed, 0x9b7246ad4d6186a5, 0x5c4ed868b64a116b},
	{0xb8d465a33801990d, 0x9b7246ad4d6186a5, 0x80fcb606b64a116b},
	{0x78bdae9105c9647a, 0x9b7246ad4d6186a5, 0x187d371fb64a116b},
	{0xf41b42d15e261c13, 0x9b7246ad4d6186a5, 0x187d371f1ae65284},
	{0xc89ad9a62e3823a2, 0x874a62b14d6186a5, 0x187d371f1ae65284},
	{0xc957a90c356761bb, 0x874a62b10c8d2b08, 0x187d371f1ae65284},

	{0x4469f2a3d93b5d15, 0x33c1704ef6a01bd5, 0x824a061f1c194e81},
	{0x5da224d002c3e37a, 0x33c1704e0dabeb25, 0x824a061f1c194e81},
	{0x58e7a450ff5d0df7, 0xff423f820dabeb25, 0x824a061f1c194e81},
	{0xc2590e773478896d, 0xff423f820dabeb25, 0x824a061f71749436},
	{0x36f4da85738febc5, 0xff423f820dabeb25, 0x65175ba571749436},
	{0x76a4fad85ebb2712, 0xff423f820dabeb25, 0x800da45a71749436},
	{0x6c115b059be6dba3, 0xff423f82ca3eb9e2, 0x800da45a71749436},
	{0x25144470638c7629, 0x3e83a6daca3eb9e2, 0x800da45a71749436},
	{0xe72585d7da4d010d, 0x3e83a6daca3eb9e2, 0x800da45a596c8c06},
	{0xc2367a076ff069fe, 0x3e83a6daca3eb9e2, 0x800da45a85dee268},
	{0x762473c4ea2ec468, 0x3e83a6daca3eb9e2, 0x7fe8bea585dee268},
	{0x7a278baf71579ebe, 0x3e83a6da7682da3d, 0x7fe8bea585dee268},
	{0x91578ed0783cca0a, 0xd42c099f7682da3d, 0x7fe8bea585dee268},
	{0x1067e7d2f1c9d6aa, 0x212efe687682da3d, 0x7fe8bea585dee268},
	{0xcf5ba6b1b81628b1, 0x212efe687682da3d, 0x7fe8bea5516d85bc},
	{0x6afb18827effaf95, 0x212efe687682da3d, 0xc85fcb67516d85bc},
	{0xbfc8c6db83d247bf, 0x212efe68a4cc94a1, 0xc85fcb67516d85bc},

	{0x1460a9c0b5a2d9a8, 0x0e81acd1121ffd55, 0xf0ca176cb96764d6},
	{0x35cb771e1f0f1341, 0xe6490daeb1a804e8, 0xc54d224a69f727df},
	{0x3776a1a2c06d3719, 0x5af6484087edae94, 0x10c28b9fa10b5b67},
	{0xb58a08bb844f59ce, 0x056cb25ed1ca9992, 0x404cc96162cf52be},
	{0xa7327aaec8b0b584, 0x12a76b3b0be817ed, 0xba4148ae3789caa6},
	{0xb8cd68290bd08f64, 0x57f69ec79dc87095, 0x36bd9ac35c10bf68},
	{0x676132caedb83279, 0x775daa9ad5fb440c, 0xd9be609774f1c4cc},
	{0x2bb214e286775315, 0x611583800ee7f8bb, 0x672b4029a8708051},
	{0x0d23bb996a4ad19c, 0x676a6e56a8c9bdc2, 0xb27b7a9fb767a437},
	{0x010b08ba063b48b4, 0x9f95d840fe41d031, 0x75fd1654809e19f9},
	{0xb68e1692c2257a47, 0x98b4feee40497c0a, 0x08068577541c857c},
	{0xd88d39e82da6fd44, 0xf9aeb01a5a514f5b, 0x6319f4edf7b722df},
	{0x8d94fd64de2d3aba, 0x7e451fe699e29a7d, 0x7eb4650844a13e0b},
	{0x9209b7df997662cc, 0x4932eab5ea001a0d, 0x14f0acf443ad1f60},
	{0x6c152615be1d318b, 0x42b056ce64e3a72b, 0xeb5dcc04f9160ee3},
	{0xe271ec4a6c03dcaf, 0xea18ccafa003e96c, 0x6c28dafcd9cf455a},
	{0xe905451d37a8e40e, 0x11abb99dca7d6784, 0xc791810992f67d4d},
	{0xe164fee31a35679a, 0xae447fa70f095fba, 0x70c228d483d15bf4},
	{0x5ddc62da4d635a10, 0xd6b4a2dba42fb08f, 0x922e34b713db6621},
	{0xf396f810ff071e20, 0x7cb351e80e703fa5, 0x0faa384e1da812f1},
}

func TestHashFn(t *testing.T) {
	states := make([][3]uint64, len(data))
	for i := range data {
		if l := len(data[i]); l < 16 {
			data[i] = append(data[i], StrKeyPadding[l:]...)
		}
	}
	AesBytesBatchGenHashStates(&data[0], &states[0], len(data))
	for i := range data {
		if states[i] != golden[i] {
			t.Errorf("AesBytesHashState(%s) = {0x%016x, 0x%016x, 0x%016x} want {0x%016x, 0x%016x, 0x%016x}",
				data[i],
				states[i][0], states[i][1], states[i][2],
				golden[i][0], golden[i][1], golden[i][2])
		}
	}
}
