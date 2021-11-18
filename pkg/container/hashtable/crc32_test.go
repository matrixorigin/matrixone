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
	in    string
}

var golden = []test{
	{0xb2d6d90f, "abcdefgh"},
	{0x5f204df0, "abcdefghi"},
	{0xf6d0f147, "abcdefghij"},
	{0x52fb12b6, "Discard medicine more than two years old."},
	{0xaa0fa47c, "He who has a shady past knows that nice guys finish last."},
	{0x3f56ef10, "I wouldn't marry him with a ten foot pole."},
	{0xb9ecf3bf, "Free! Free!/A trip/to Mars/for 900/empty jars/Burma Shave"},
	{0x5ff2f2ec, "The days of the digital watch are numbered.  -Tom Stoppard"},
	{0x31503186, "Nepal premier won't resign."},
	{0xf695b9cc, "For every action there is an equal and opposite government program."},
	{0xa298d8c2, "His money is twice tainted: 'taint yours and 'taint mine."},
	{0xbd4cfa20, "There is no reason for any individual to have a computer in their home. -Ken Olsen, 1977"},
	{0x4053066e, "It's a tiny change to the code and not completely disgusting. - Bob Manchek"},
	{0x9a3d3653, "size:  a.out:  bad magic"},
	{0xb799b206, "The major problem is with sendmail.  -Mark Horton"},
	{0x18fa0ffa, "Give me a rock, paper and scissors and I will move the world.  CCFestoon"},
	{0x10ec5116, "If the enemy is within range, then so are you."},
	{0x6bbb0124, "It's well we cannot hear the screams/That we create in others' dreams."},
	{0xd2ff9dae, "You remind me of a TV show, but that's all right: I watch it anyway."},
	{0x358dfda6, "C is as portable as Stonehedge!!"},
	{0x5dd9b3a, "Even if I could be Shakespeare, I think I should still choose to be Faraday. - A. Huxley"},
	{0xfb54049d, "The fugacity of a constituent in a mixture of gases at a given temperature is proportional to its mole fraction.  Lewis-Randall Rule"},
	{0x7051393b, "How can you write a big system without C++?  -Paul Glick"},
}

func TestGoldenCastagnoli(t *testing.T) {
	for _, g := range golden {
		if crc := Crc32BytesHashAsm(unsafe.Pointer((*reflect.StringHeader)(unsafe.Pointer(&g.in)).Data), len(g.in)); crc != g.Crc32 {
			t.Errorf("Crc32Hash(%s) = 0x%x want 0x%x", g.in, crc, g.Crc32)
		}
	}
}
