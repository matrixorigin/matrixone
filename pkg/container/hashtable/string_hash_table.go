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

func NewStringHashTable(inlineVal bool, valueSize uint8, agg Aggregator) *StringHashTable {
	return &StringHashTable{
		inlineVal: inlineVal,
		H0:        NewFixedTable(inlineVal, 65536, valueSize),
		H1:        NewHashTable(true, inlineVal, 8, valueSize),
		H2:        NewHashTable(true, inlineVal, 16, valueSize),
		H3:        NewHashTable(true, inlineVal, 24, valueSize),
		Hs:        NewHashTable(false, inlineVal, 24, valueSize),
	}
}

func (ht *StringHashTable) Insert(hash uint64, key []byte) (inserted bool, value []byte) {
	switch l := len(key); {
	case l <= 2:
		var intKey uint32
		for i := 0; i < l; i++ {
			intKey += uint32(key[i]) << (i * 8)
		}
		return ht.H0.Insert(intKey)
	case l <= 8:
		return ht.H1.Insert(hash, key)
	case l <= 16:
		return ht.H2.Insert(hash, key)
	case l <= 24:
		return ht.H3.Insert(hash, key)
	default:
		return ht.Hs.Insert(hash, key)
	}
}
