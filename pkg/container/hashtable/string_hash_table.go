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

import "matrixone/pkg/vm/process"

func NewStringHashTable(inlineVal bool, valueSize uint8, proc process.Process) (*StringHashTable, error) {
	H0, err := NewFixedTable(inlineVal, 65536, valueSize, proc)
	if err != nil {
		return nil, err
	}

	H1, err := NewHashTable(true, inlineVal, 8, valueSize, proc)
	if err != nil {
		H0.Destroy(proc)
		return nil, err
	}

	H2, err := NewHashTable(true, inlineVal, 16, valueSize, proc)
	if err != nil {
		H0.Destroy(proc)
		H1.Destroy(proc)
		return nil, err
	}

	H3, err := NewHashTable(true, inlineVal, 24, valueSize, proc)
	if err != nil {
		H0.Destroy(proc)
		H1.Destroy(proc)
		H2.Destroy(proc)
		return nil, err
	}

	Hs, err := NewHashTable(false, inlineVal, 24, valueSize, proc)
	if err != nil {
		H0.Destroy(proc)
		H1.Destroy(proc)
		H2.Destroy(proc)
		H3.Destroy(proc)
		return nil, err
	}

	return &StringHashTable{
		H0: H0,
		H1: H1,
		H2: H2,
		H3: H3,
		Hs: Hs,
	}, nil
}

func (ht *StringHashTable) Insert(hash uint64, key []byte, proc process.Process) (inserted bool, value []byte, err error) {
	switch l := len(key); {
	case l <= 2:
		var intKey uint32
		for i := 0; i < l; i++ {
			intKey += uint32(key[i]) << (i * 8)
		}
		inserted, value = ht.H0.Insert(intKey)
		return
	case l <= 8:
		return ht.H1.Insert(hash, key, proc)
	case l <= 16:
		return ht.H2.Insert(hash, key, proc)
	case l <= 24:
		return ht.H3.Insert(hash, key, proc)
	default:
		return ht.Hs.Insert(hash, key, proc)
	}
}

func (ht *StringHashTable) Destroy(proc process.Process) {
	ht.H0.Destroy(proc)
	ht.H1.Destroy(proc)
	ht.H2.Destroy(proc)
	ht.H3.Destroy(proc)
	ht.Hs.Destroy(proc)
}
