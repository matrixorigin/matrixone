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
	"bytes"
	"unsafe"
)

type StringRef struct {
	Ptr    *byte
	Length int
}

type tStringPtrCell struct {
	hash   uint64
	key    StringRef
	mapped uint64
}

type MockStringPtrHashTable struct {
	bucketCntBits uint8
	bucketCnt     uint64
	elemCnt       uint64
	maxElemCnt    uint64
	rawData       []byte
	bucketData    []tStringPtrCell
}

func (ht *MockStringPtrHashTable) Init() error {
	const cSize = int64(unsafe.Sizeof(tStringPtrCell{}))
	var bucketData []byte
	bucketData = make([]byte, cSize*kInitialBucketCnt)
	ht.bucketCntBits = kInitialBucketCntBits
	ht.bucketCnt = kInitialBucketCnt
	ht.elemCnt = 0
	ht.maxElemCnt = kInitialBucketCnt * kLoadFactorNumerator / kLoadFactorDenominator
	ht.rawData = bucketData
	ht.bucketData = unsafe.Slice((*tStringPtrCell)(unsafe.Pointer(&bucketData[0])), cap(bucketData)/int(cSize))[:kInitialBucketCnt]

	return nil
}

func (ht *MockStringPtrHashTable) Insert(hash uint64, key StringRef) (inserted bool, value *uint64, err error) {
	err = ht.resizeOnDemand()
	if err != nil {
		return
	}

	if hash == 0 {
		hash = Crc32BytesHashAsm(unsafe.Pointer(key.Ptr), key.Length)
	}

	inserted, _, cell := ht.findBucket(hash, key)
	if inserted {
		ht.elemCnt++
		cell.hash = hash
		cell.key = key
	}

	value = &cell.mapped

	return
}

func (ht *MockStringPtrHashTable) Find(hash uint64, key StringRef) *uint64 {
	if hash == 0 {
		hash = Crc32BytesHashAsm(unsafe.Pointer(key.Ptr), key.Length)
	}
	_, _, cell := ht.findBucket(hash, key)

	return &cell.mapped
}

func (ht *MockStringPtrHashTable) findBucket(hash uint64, key StringRef) (empty bool, idx uint64, cell *tStringPtrCell) {
	mask := ht.bucketCnt - 1
	keySlice := unsafe.Slice(key.Ptr, key.Length)
	for idx = hash & mask; true; idx = (idx + 1) & mask {
		cell = &ht.bucketData[idx]
		empty = cell.hash == 0
		if empty || (cell.hash == hash && bytes.Equal(unsafe.Slice(cell.key.Ptr, cell.key.Length), keySlice)) {
			return
		}
	}

	return
}

func (ht *MockStringPtrHashTable) resizeOnDemand() error {
	if ht.elemCnt < ht.maxElemCnt {
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

	const cSize = int64(unsafe.Sizeof(tStringPtrCell{}))
	var newData []byte
	newData = make([]byte, cSize*int64(newBucketCnt))

	newBucketData := unsafe.Slice((*tStringPtrCell)(unsafe.Pointer(&newData[0])), cap(newData)/int(cSize))[:newBucketCnt]

	copy(newData, ht.rawData)
	ht.rawData = newData
	ht.bucketData = newBucketData

	oldBucketCnt := ht.bucketCnt
	ht.bucketCntBits = newBucketCntBits
	ht.bucketCnt = newBucketCnt
	ht.bucketData = newBucketData
	ht.maxElemCnt = newMaxElemCnt

	var i uint64
	for i = 0; i < oldBucketCnt; i++ {
		if ht.bucketData[i].hash != 0 {
			ht.reinsert(i)
		}
	}

	for ht.bucketData[i].hash != 0 {
		ht.reinsert(i)
		i++
	}

	return nil
}

func (ht *MockStringPtrHashTable) reinsert(idx uint64) {
	cell := &ht.bucketData[idx]

	_, newIdx, _ := ht.findBucket(cell.hash, cell.key)
	if newIdx == idx {
		return
	}

	ht.bucketData[newIdx] = ht.bucketData[idx]
	ht.bucketData[idx] = tStringPtrCell{}
}

func (ht *MockStringPtrHashTable) Cardinality() uint64 {
	return ht.elemCnt
}

func (ht *MockStringPtrHashTable) Destroy() {
	if ht.rawData != nil {
		ht.rawData = nil
		ht.bucketData = nil
	}
}
