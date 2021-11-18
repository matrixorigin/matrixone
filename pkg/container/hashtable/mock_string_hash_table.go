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
	"unsafe"
)

type MockStringHashTable struct {
	H1 *MockInt64HashTable
	H2 *MockString16HashTable
	H3 *MockString24HashTable
	Hs *MockStringPtrHashTable

	key8s  []uint64
	key16s [][2]uint64
	key24s [][3]uint64

	hashes  []uint64
	zHashes []uint64
	values  []*uint64
	zValues []*uint64
	inserts []bool

	totalKeysLen uint64
}

func (ht *MockStringHashTable) Init() error {
	var err error

	ht.H1 = &MockInt64HashTable{}
	err = ht.H1.Init()
	if err != nil {
		ht.Destroy()
		return err
	}

	ht.H2 = &MockString16HashTable{}
	err = ht.H2.Init()
	if err != nil {
		ht.Destroy()
		return err
	}

	ht.H3 = &MockString24HashTable{}
	err = ht.H3.Init()
	if err != nil {
		ht.Destroy()
		return err
	}

	ht.Hs = &MockStringPtrHashTable{}
	err = ht.Hs.Init()
	if err != nil {
		ht.Destroy()
		return err
	}

	ht.hashes = make([]uint64, 256)
	ht.zHashes = make([]uint64, 256)
	ht.values = make([]*uint64, 256)
	ht.zValues = make([]*uint64, 256)
	ht.inserts = make([]bool, 256)

	return nil
}

func (ht *MockStringHashTable) Insert(hash uint64, key []byte) (inserted bool, value *uint64, err error) {
	switch l := len(key); {
	case l <= 8:
		var key8 uint64
		copy(unsafe.Slice((*byte)(unsafe.Pointer(&key8)), unsafe.Sizeof(key8)), key)
		ht.key8s = append(ht.key8s, key8)
		if len(ht.key8s) == 256 {
			copy(ht.hashes, ht.zHashes)
			copy(ht.values, ht.zValues)
			if err = ht.H1.InsertBatch(ht.hashes, ht.key8s, ht.values, ht.inserts); err != nil {
				return
			}
			for _, v := range ht.values {
				*v++
			}
			ht.key8s = ht.key8s[:0]
		}

	case l <= 16:
		var key16 [2]uint64
		copy(unsafe.Slice((*byte)(unsafe.Pointer(&key16)), unsafe.Sizeof(key16)), key)
		ht.key16s = append(ht.key16s, key16)
		if len(ht.key16s) == 256 {
			copy(ht.hashes, ht.zHashes)
			copy(ht.values, ht.zValues)
			if err = ht.H2.InsertBatch(ht.hashes, ht.key16s, ht.values, ht.inserts); err != nil {
				return
			}
			for _, v := range ht.values {
				*v++
			}
			ht.key16s = ht.key16s[:0]
		}

	case l <= 24:
		var key24 [3]uint64
		copy(unsafe.Slice((*byte)(unsafe.Pointer(&key24)), unsafe.Sizeof(key24)), key)
		ht.key24s = append(ht.key24s, key24)
		if len(ht.key24s) == 256 {
			copy(ht.hashes, ht.zHashes)
			copy(ht.values, ht.zValues)
			if err = ht.H3.InsertBatch(ht.hashes, ht.key24s, ht.values, ht.inserts); err != nil {
				return
			}
			for _, v := range ht.values {
				*v++
			}
			ht.key24s = ht.key24s[:0]
		}

	default:
		if inserted, value, err = ht.Hs.Insert(hash, StringRef{Ptr: &key[0], Length: len(key)}); inserted {
			ht.totalKeysLen += uint64(l)
		}
	}

	return
}

func (ht *MockStringHashTable) Find(hash uint64, key []byte) *uint64 {
	switch l := len(key); {
	case l <= 8:
		var key8 uint64
		copy(unsafe.Slice((*byte)(unsafe.Pointer(&key8)), unsafe.Sizeof(key8)), key)
		return ht.H1.Find(hash, key8)

	case l <= 16:
		return ht.H2.Find(hash, key)

	case l <= 24:
		return ht.H3.Find(hash, key)

	default:
		return ht.Hs.Find(hash, StringRef{Ptr: &key[0], Length: len(key)})
	}
}

func (ht *MockStringHashTable) Cardinality() uint64 {
	return ht.H1.Cardinality() + ht.H2.Cardinality() + ht.H3.Cardinality() + ht.Hs.Cardinality()
}

func (ht *MockStringHashTable) KeysLen() uint64 {
	return ht.totalKeysLen
}

func (ht *MockStringHashTable) Destroy() {
	ht.H1.Destroy()
	ht.H2.Destroy()
	ht.H3.Destroy()
	ht.Hs.Destroy()
}
