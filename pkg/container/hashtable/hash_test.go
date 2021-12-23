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
	{0x4be8a24289833574, 0x0e81acd1121ffd55, 0xf0ca176cb96764d6},
	{0xe3f80ef744d42dba, 0xe6490daeb1a804e8, 0xc54d224a69f727df},
	{0xdacd703644732813, 0x5af6484087edae94, 0x10c28b9fa10b5b67},
	{0x8c4ba0e74276ee2e, 0x056cb25ed1ca9992, 0x404cc96162cf52be},
	{0xaca22a4cabcbd8fe, 0x12a76b3b0be817ed, 0xba4148ae3789caa6},
	{0x6e3f3c24cae51d3a, 0x57f69ec79dc87095, 0x36bd9ac35c10bf68},
	{0xaaab464fac792fca, 0x775daa9ad5fb440c, 0xd9be609774f1c4cc},
	{0x2393198eb4e9919b, 0x611583800ee7f8bb, 0x672b4029a8708051},
	{0x9287f199a3e787cf, 0x676a6e56a8c9bdc2, 0xb27b7a9fb767a437},
	{0xb82af0449d123a1a, 0x9f95d840fe41d031, 0x75fd1654809e19f9},
	{0xf16de8df103c5aec, 0x98b4feee40497c0a, 0x08068577541c857c},
	{0xd1579f8434e169fa, 0xf9aeb01a5a514f5b, 0x6319f4edf7b722df},
	{0xc67284ad3c025fba, 0x7e451fe699e29a7d, 0x7eb4650844a13e0b},
	{0x6e82dd462b1926cc, 0x4932eab5ea001a0d, 0x14f0acf443ad1f60},
	{0x651f6c7a7543b491, 0x42b056ce64e3a72b, 0xeb5dcc04f9160ee3},
	{0x9bf66d5751889343, 0xea18ccafa003e96c, 0x6c28dafcd9cf455a},
	{0x27ffea1f037a7d82, 0x11abb99dca7d6784, 0xc791810992f67d4d},
	{0x41cdd2330eb7e72f, 0xae447fa70f095fba, 0x70c228d483d15bf4},
	{0xbc3680511c7d5032, 0xd6b4a2dba42fb08f, 0x922e34b713db6621},
	{0xa75a63e190d72688, 0x7cb351e80e703fa5, 0x0faa384e1da812f1},
}

func TestHashFn(t *testing.T) {
	states := make([][3]uint64, len(data))
	AesBytesBatchGenHashStates(&data[0], &states[0], len(data))
	for i := range data {
		if states[i] != golden[i] {
			t.Errorf("AesBytesHashState(%s) = {0x%016x,0x%016x,0x%016x} want {0x%016x,0x%016x,0x%016x}",
				data[i],
				states[i][0], states[i][1], states[i][2],
				golden[i][0], golden[i][1], golden[i][2])
		}
	}
}
