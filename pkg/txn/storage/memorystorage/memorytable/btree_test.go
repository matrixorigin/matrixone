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

import (
	"testing"
)

func TestBTreeKVEncoding(t *testing.T) {
	kv := NewBTree[Int, int]()
	for i := 0; i < 10; i++ {
		kv.Set(TreeNode[Int, int]{
			KVPair: &KVPair[Int, int]{
				Key: Int(i),
				KVValue: &KVValue[Int, int]{
					Value: i,
				},
			},
		})
	}
	data, err := kv.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	kv2 := new(BTree[Int, int])
	if err := kv2.UnmarshalBinary(data); err != nil {
		t.Fatal(err)
	}

	m := make(map[Int]int)
	iter := kv2.Iter()
	for ok := iter.First(); ok; ok = iter.Next() {
		node, err := iter.Read()
		if err != nil {
			t.Fatal()
		}
		pair := node.KVPair
		m[pair.Key] = pair.Value
	}
	if len(m) != 10 {
		t.Fatal()
	}
	for i := 0; i < 10; i++ {
		if m[Int(i)] != i {
			t.Fatal()
		}
	}

}
