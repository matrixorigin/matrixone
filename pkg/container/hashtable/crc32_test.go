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

type test struct {
	crc32 uint64
	in    string
}

var golden = []test{
	{0xf56bde48, "abcdefgh"},
	{0x64db74c3, "abcdefghi"},
	{0xa5b3524b, "abcdefghij"},
	{0x3ad923d8, "Discard medicine more than two years old."},
	{0x326fcda0, "He who has a shady past knows that nice guys finish last."},
	{0x713cf472, "I wouldn't marry him with a ten foot pole."},
	{0x8435008, "Free! Free!/A trip/to Mars/for 900/empty jars/Burma Shave"},
	{0x45b04b53, "The days of the digital watch are numbered.  -Tom Stoppard"},
	{0xe4594ac0, "Nepal premier won't resign."},
	{0x8a0bc566, "For every action there is an equal and opposite government program."},
	{0x37f8b3ca, "His money is twice tainted: 'taint yours and 'taint mine."},
	{0x6e141608, "There is no reason for any individual to have a computer in their home. -Ken Olsen, 1977"},
	{0x4a55d67b, "It's a tiny change to the code and not completely disgusting. - Bob Manchek"},
	{0xa8d48b1d, "size:  a.out:  bad magic"},
	{0x375a8773, "The major problem is with sendmail.  -Mark Horton"},
	{0x63bd93af, "Give me a rock, paper and scissors and I will move the world.  CCFestoon"},
	{0xa5ccdeb5, "If the enemy is within range, then so are you."},
	{0xde56fb76, "It's well we cannot hear the screams/That we create in others' dreams."},
	{0xb602f42c, "You remind me of a TV show, but that's all right: I watch it anyway."},
	{0x21d19a3a, "C is as portable as Stonehedge!!"},
	{0xd6857712, "Even if I could be Shakespeare, I think I should still choose to be Faraday. - A. Huxley"},
	{0x28269e5f, "The fugacity of a constituent in a mixture of gases at a given temperature is proportional to its mole fraction.  Lewis-Randall Rule"},
	{0x23212ad8, "How can you write a big system without C++?  -Paul Glick"},
}

func TestGoldenCastagnoli(t *testing.T) {
	for _, g := range golden {
		if crc := crc32BytesHashAsm([]byte(g.in)); crc != g.crc32 {
			t.Errorf("crc32Hash(%s) = 0x%x want 0x%x", g.in, crc, g.crc32)
		}
	}
}
