// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package memorytable

import "testing"

func TestBTreeIndexEncoding(t *testing.T) {
	index := NewBTreeIndex[Int, int]()
	for i := 0; i < 10; i++ {
		index.Set(&IndexEntry[Int, int]{
			Index: Tuple{
				Int(i),
			},
			Key: Int(i),
		})
	}
	data, err := index.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	index2 := new(BTreeIndex[Int, int])
	if err := index2.UnmarshalBinary(data); err != nil {
		t.Fatal(err)
	}

	m := make(map[Int]Int)
	iter := index.Iter()
	for ok := iter.First(); ok; ok = iter.Next() {
		entry, err := iter.Read()
		if err != nil {
			t.Fatal()
		}
		m[entry.Index[0].(Int)] = entry.Key
	}
	if len(m) != 10 {
		t.Fatal()
	}
	for i := 0; i < 10; i++ {
		if m[Int(i)] != Int(i) {
			t.Fatal()
		}
	}

}
