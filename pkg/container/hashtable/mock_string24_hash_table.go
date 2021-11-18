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

type tString24Cell struct {
	key    [3]uint64
	mapped uint64
}

type MockString24HashTable struct {
	bucketCntBits uint8
	bucketCnt     uint64
	elemCnt       uint64
	maxElemCnt    uint64
	rawData       []byte
	bucketData    []tString24Cell
}

func (ht *MockString24HashTable) Init() error {
	const cSize = int64(unsafe.Sizeof(tString24Cell{}))
	var bucketData []byte
	bucketData = make([]byte, cSize*kInitialBucketCnt)
	ht.bucketCntBits = kInitialBucketCntBits
	ht.bucketCnt = kInitialBucketCnt
	ht.elemCnt = 0
	ht.maxElemCnt = kInitialBucketCnt * kLoadFactorNumerator / kLoadFactorDenominator
	ht.rawData = bucketData
	ht.bucketData = unsafe.Slice((*tString24Cell)(unsafe.Pointer(&bucketData[0])), cap(bucketData)/int(cSize))[:kInitialBucketCnt]

	return nil
}

func (ht *MockString24HashTable) Insert(hash uint64, rawKey []byte) (inserted bool, value *uint64, err error) {
	err = ht.resizeOnDemand(1)
	if err != nil {
		return
	}

	var key [3]uint64
	copy(unsafe.Slice((*byte)(unsafe.Pointer(&key)), unsafe.Sizeof(key)), rawKey)

	if hash == 0 {
		hash = Crc32IntSliceHashAsm(&key[0], 3)
	}

	inserted, _, cell := ht.findBucket(hash, key)
	if inserted {
		ht.elemCnt++
		cell.key = key
	}

	value = &cell.mapped

	return
}

func (ht *MockString24HashTable) InsertBatch(hashes []uint64, keys [][3]uint64, values []*uint64, inserts []bool) (err error) {
	var inserted uint64

	err = ht.resizeOnDemand(uint64(len(keys)))
	if err != nil {
		return
	}

	if hashes[0] == 0 {
		Crc32Int192BatchHashAsm(&keys[0], &hashes[0], len(keys))
	}

	for i, key := range keys {
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
	ht.elemCnt += inserted
	return
}

func (ht *MockString24HashTable) Find(hash uint64, rawKey []byte) *uint64 {
	var key [3]uint64
	copy(unsafe.Slice((*byte)(unsafe.Pointer(&key)), unsafe.Sizeof(key)), rawKey)

	if hash == 0 {
		hash = Crc32IntSliceHashAsm(&key[0], 3)
	}

	_, _, cell := ht.findBucket(hash, key)

	return &cell.mapped
}

func (ht *MockString24HashTable) FindBatch(hashes []uint64, keys [][3]uint64, values []*uint64) {
	if hashes[0] == 0 {
		Crc32Int192BatchHashAsm(&keys[0], &hashes[0], len(keys))
	}

	for i, key := range keys {
		_, _, cell := ht.findBucket(hashes[i], key)
		if cell.key[0] != 0 {
			values[i] = &cell.mapped
		}
	}
}

func (ht *MockString24HashTable) findBucket(hash uint64, key [3]uint64) (empty bool, idx uint64, cell *tString24Cell) {
	mask := ht.bucketCnt - 1
	for idx = hash & mask; true; idx = (idx + 1) & mask {
		cell = &ht.bucketData[idx]
		empty = cell.key[0] == 0
		if empty || cell.key == key {
			return
		}
	}

	return
}

func (ht *MockString24HashTable) resizeOnDemand(n uint64) error {
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

	const cSize = int64(unsafe.Sizeof(tString24Cell{}))
	var newData []byte
	newData = make([]byte, cSize*int64(newBucketCnt))

	newBucketData := unsafe.Slice((*tString24Cell)(unsafe.Pointer(&newData[0])), cap(newData)/int(cSize))[:newBucketCnt]

	copy(newData, ht.rawData)
	ht.rawData = newData
	ht.bucketData = newBucketData

	oldBucketCnt := ht.bucketCnt
	ht.bucketCntBits = newBucketCntBits
	ht.bucketCnt = uint64(newBucketCnt)
	ht.bucketData = newBucketData
	ht.maxElemCnt = newMaxElemCnt

	var i uint64
	for i = 0; i < oldBucketCnt; i++ {
		if ht.bucketData[i].key[2] != 0 {
			ht.reinsert(i)
		}
	}

	for ht.bucketData[i].key[2] != 0 {
		ht.reinsert(i)
		i++
	}

	return nil
}

func (ht *MockString24HashTable) reinsert(idx uint64) {
	cell := &ht.bucketData[idx]

	_, newIdx, _ := ht.findBucket(Crc32IntSliceHashAsm(&cell.key[0], 3), cell.key)
	if newIdx == idx {
		return
	}

	ht.bucketData[newIdx] = ht.bucketData[idx]
	ht.bucketData[idx] = tString24Cell{}
}

func (ht *MockString24HashTable) Cardinality() uint64 {
	return ht.elemCnt
}

func (ht *MockString24HashTable) Destroy() {
	if ht.rawData != nil {
		ht.rawData = nil
		ht.bucketData = nil
	}
}
