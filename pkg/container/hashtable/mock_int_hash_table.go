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
	"errors"
	"unsafe"
)

type tInt64Cell struct {
	key    uint64
	mapped uint64
}

type MockInt64HashTable struct {
	bucketCntBits uint8
	bucketCnt     uint64
	elemCnt       uint64
	maxElemCnt    uint64
	hasZero       uint64
	zeroValue     uint64
	rawData       []byte
	bucketData    []tInt64Cell
}

type MockIntHashTableIterator struct {
	table *MockInt64HashTable
	pos   int
	len   int
}

func (ht *MockInt64HashTable) Init() error {
	const cSize = int64(unsafe.Sizeof(tInt64Cell{}))
	var bucketData []byte
	bucketData = make([]byte, cSize*kInitialBucketCnt)

	ht.bucketCntBits = kInitialBucketCntBits
	ht.bucketCnt = kInitialBucketCnt
	ht.elemCnt = 0
	ht.maxElemCnt = kInitialBucketCnt * kLoadFactorNumerator / kLoadFactorDenominator
	ht.rawData = bucketData
	ht.bucketData = unsafe.Slice((*tInt64Cell)(unsafe.Pointer(&bucketData[0])), cap(bucketData)/int(cSize))[:kInitialBucketCnt]

	return nil
}

func (ht *MockInt64HashTable) Insert(hash uint64, key uint64) (inserted bool, value *uint64, err error) {
	if key == 0 {
		inserted = ht.hasZero == 0
		value = &ht.zeroValue
		ht.hasZero = 1
		return
	}

	err = ht.resizeOnDemand(1)
	if err != nil {
		return
	}

	if hash == 0 {
		hash = Crc32IntHashAsm(key)
	}

	inserted, _, cell := ht.findBucket(hash, key)
	if inserted {
		ht.elemCnt++
		cell.key = key
	}

	value = &cell.mapped

	return
}

func (ht *MockInt64HashTable) InsertBatch(hashes []uint64, keys []uint64, values []*uint64, inserts []bool) (err error) {
	var inserted uint64

	err = ht.resizeOnDemand(uint64(len(keys)))
	if err != nil {
		return
	}

	if hashes[0] == 0 {
		Crc32Int64BatchHashAsm(&keys[0], &hashes[0], len(keys))
	}

	for i, key := range keys {
		if key == 0 {
			if ht.hasZero == 0 {
				inserted++
				inserts[i] = true
			} else {
				inserts[i] = false
			}
			ht.hasZero = 1
			values[i] = &ht.zeroValue
		} else {
			isInserted, _, cell := ht.findBucket(hashes[i], key)
			if isInserted {
				inserted++
				inserts[i] = true
				cell.key = key
			} else {
				inserts[i] = false
			}
			values[i] = &cell.mapped
		}
	}
	ht.elemCnt += inserted
	return
}

func (ht *MockInt64HashTable) Find(hash uint64, key uint64) *uint64 {
	if key == 0 {
		if ht.hasZero == 1 {
			return &ht.zeroValue
		} else {
			return nil
		}
	}

	if hash == 0 {
		hash = Crc32IntHashAsm(key)
	}
	_, _, cell := ht.findBucket(hash, key)

	return &cell.mapped
}

func (ht *MockInt64HashTable) FindBatch(hashes []uint64, keys []uint64, values []*uint64) {
	if hashes[0] == 0 {
		Crc32Int64BatchHashAsm(&keys[0], &hashes[0], len(keys))
	}

	for i, key := range keys {
		if key == 0 {
			if ht.hasZero == 1 {
				values[i] = &ht.zeroValue
			}
		} else {
			_, _, cell := ht.findBucket(hashes[i], key)
			if cell.key != 0 {
				values[i] = &cell.mapped
			}
		}
	}
}

func (ht *MockInt64HashTable) findBucket(hash uint64, key uint64) (empty bool, idx uint64, cell *tInt64Cell) {
	mask := ht.bucketCnt - 1
	var equal bool
	for idx = hash & mask; true; idx = (idx + 1) & mask {
		cell = &ht.bucketData[idx]
		empty, equal = cell.key == 0, cell.key == key
		if empty || equal {
			return
		}
	}

	return
}

func (ht *MockInt64HashTable) resizeOnDemand(n uint64) error {
	targetCnt := ht.elemCnt + n
	if targetCnt <= ht.maxElemCnt {
		return nil
	}

	var newBucketCntBits uint8
	if ht.bucketCnt >= 23 {
		newBucketCntBits = ht.bucketCntBits + 1
	} else {
		newBucketCntBits = ht.bucketCntBits + 2
	}

	newBucketCnt := uint64(1) << newBucketCntBits
	newMaxElemCnt := newBucketCnt * kLoadFactorNumerator / kLoadFactorDenominator
	for newMaxElemCnt < targetCnt {
		newBucketCntBits++
		newBucketCnt <<= 1
		newMaxElemCnt = newBucketCnt * kLoadFactorNumerator / kLoadFactorDenominator
	}

	const cSize = int64(unsafe.Sizeof(tInt64Cell{}))
	var newData []byte
	newData = make([]byte, cSize*int64(newBucketCnt))

	copy(newData, ht.rawData)
	ht.rawData = newData
	ht.bucketData = unsafe.Slice((*tInt64Cell)(unsafe.Pointer(&newData[0])), cap(newData)/int(cSize))[:newBucketCnt]

	oldBucketCnt := ht.bucketCnt
	ht.bucketCntBits = newBucketCntBits
	ht.bucketCnt = newBucketCnt
	ht.maxElemCnt = newMaxElemCnt

	var i uint64
	for i = 0; i < oldBucketCnt; i++ {
		if ht.bucketData[i].key != 0 {
			ht.reinsert(i)
		}
	}

	for ht.bucketData[i].key != 0 {
		ht.reinsert(i)
		i++
	}

	return nil
}

func (ht *MockInt64HashTable) reinsert(idx uint64) {
	cell := &ht.bucketData[idx]

	_, newIdx, _ := ht.findBucket(Crc32IntHashAsm(cell.key), cell.key)
	if newIdx == idx {
		return
	}

	ht.bucketData[newIdx] = ht.bucketData[idx]
	ht.bucketData[idx].key = 0
	ht.bucketData[idx].mapped = 0
}

func (ht *MockInt64HashTable) Cardinality() uint64 {
	return ht.elemCnt
}

func (ht *MockInt64HashTable) Destroy() {
	if ht.rawData != nil {
		ht.rawData = nil
		ht.bucketData = nil
	}
}

func (it *MockIntHashTableIterator) Init(ht *MockInt64HashTable) {
	it.table = ht
	it.pos = 0
	it.len = len(ht.bucketData)
}

func (it *MockIntHashTableIterator) Next() (cell *tInt64Cell, err error) {
	for it.pos < it.len && it.table.bucketData[it.pos].key == 0 {
		it.pos++
	}

	if it.pos >= it.len {
		err = errors.New("out of range")
		return
	}

	cell = &it.table.bucketData[it.pos]
	it.pos++

	return
}
