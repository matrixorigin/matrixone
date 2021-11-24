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
	"math/bits"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type String40HashMapCell struct {
	Hash   uint64
	Key    [5]uint64
	Mapped uint64
}

func (hdr *String40HashMapCell) StrKey() StringRef {
	return StringRef{
		Ptr: (*byte)(unsafe.Pointer(&hdr.Key[0])),
		Len: 32 - (bits.LeadingZeros64(hdr.Key[3]) >> 3),
	}
}

type String40HashMap struct {
	bucketCntBits uint8
	bucketCnt     uint64
	elemCnt       uint64
	maxElemCnt    uint64
	rawData       []byte
	bucketData    []String40HashMapCell
}

func (ht *String40HashMap) Init(proc *process.Process) error {
	const cellSize = int(unsafe.Sizeof(String40HashMapCell{}))

	var rawData []byte
	var err error
	if proc != nil {
		rawData, err = proc.Alloc(int64(cellSize) * kInitialBucketCnt)
	} else {
		rawData = make([]byte, cellSize*kInitialBucketCnt)
	}
	if err != nil {
		return err
	}

	ht.bucketCntBits = kInitialBucketCntBits
	ht.bucketCnt = kInitialBucketCnt
	ht.elemCnt = 0
	ht.maxElemCnt = kInitialBucketCnt * kLoadFactorNumerator / kLoadFactorDenominator
	ht.rawData = rawData
	ht.bucketData = unsafe.Slice((*String40HashMapCell)(unsafe.Pointer(&rawData[0])), cap(rawData)/cellSize)[:len(rawData)/cellSize]

	return nil
}

func (ht *String40HashMap) Insert(hash uint64, key *[5]uint64, proc *process.Process) (inserted bool, value *uint64, err error) {
	err = ht.resizeOnDemand(1, proc)
	if err != nil {
		return
	}

	if hash == 0 {
		hash = crc32Int320HashAsm(key)
	}

	inserted, _, cell := ht.findBucket(hash, key)
	if inserted {
		ht.elemCnt++
		cell.Hash = hash
		cell.Key = *key
	}

	value = &cell.Mapped

	return
}

func (ht *String40HashMap) InsertBatch(hashes []uint64, keys [][5]uint64, inserted []uint8, values []*uint64, proc *process.Process) (err error) {
	err = ht.resizeOnDemand(uint64(len(keys)), proc)
	if err != nil {
		return
	}

	if hashes[0] == 0 {
		crc32Int320BatchHashAsm(&keys[0], &hashes[0], len(keys))
	}

	for i := range keys {
		isInserted, _, cell := ht.findBucket(hashes[i], &keys[i])
		if isInserted {
			ht.elemCnt++
			inserted[i] = 1
			cell.Hash = hashes[i]
			cell.Key = keys[i]
		}
		values[i] = &cell.Mapped
	}

	return
}

func (ht *String40HashMap) Find(hash uint64, key *[5]uint64) (value *uint64) {
	if hash == 0 {
		hash = crc32Int320HashAsm(key)
	}

	empty, _, cell := ht.findBucket(hash, key)
	if !empty {
		value = &cell.Mapped
	}

	return
}

func (ht *String40HashMap) FindBatch(hashes []uint64, keys [][5]uint64, values []*uint64) {
	if hashes[0] == 0 {
		crc32Int320BatchHashAsm(&keys[0], &hashes[0], len(keys))
	}

	for i := range keys {
		empty, _, cell := ht.findBucket(hashes[i], &keys[i])
		if !empty {
			values[i] = &cell.Mapped
		}
	}
}

func (ht *String40HashMap) findBucket(hash uint64, key *[5]uint64) (empty bool, idx uint64, cell *String40HashMapCell) {
	mask := ht.bucketCnt - 1
	for idx = hash & mask; true; idx = (idx + 1) & mask {
		cell = &ht.bucketData[idx]
		empty = cell.Hash == 0
		if empty || cell.Key == *key {
			return
		}
	}

	return
}

func (ht *String40HashMap) resizeOnDemand(n uint64, proc *process.Process) error {
	targetCnt := ht.elemCnt + n
	if targetCnt <= ht.maxElemCnt {
		return nil
	}

	var newBucketCntBits uint8
	if ht.bucketCntBits >= 23 {
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

	const cellSize = int(unsafe.Sizeof(String40HashMapCell{}))

	var newRawData []byte
	var err error
	if proc != nil {
		newRawData, err = proc.Alloc(int64(cellSize) * int64(newBucketCnt))
		if err != nil {
			return err
		}
	} else {
		newRawData = make([]byte, uint64(cellSize)*newBucketCnt)
	}

	copy(newRawData, ht.rawData)
	if proc != nil {
		proc.Free(ht.rawData)
	}

	oldBucketCnt := ht.bucketCnt
	ht.bucketCntBits = newBucketCntBits
	ht.bucketCnt = newBucketCnt
	ht.maxElemCnt = newMaxElemCnt
	ht.rawData = newRawData
	ht.bucketData = unsafe.Slice((*String40HashMapCell)(unsafe.Pointer(&newRawData[0])), cap(newRawData)/cellSize)[:len(newRawData)/cellSize]

	var i uint64
	for i = 0; i < oldBucketCnt; i++ {
		ht.reinsert(i)
	}

	for ht.reinsert(i) {
		i++
	}

	return nil
}

func (ht *String40HashMap) reinsert(idx uint64) bool {
	cell := &ht.bucketData[idx]
	if cell.Hash == 0 {
		return false
	}

	_, newIdx, _ := ht.findBucket(cell.Hash, &cell.Key)
	if newIdx == idx {
		return false
	}

	ht.bucketData[newIdx] = *cell
	*cell = String40HashMapCell{}

	return true
}

func (ht *String40HashMap) Cardinality() uint64 {
	return ht.elemCnt
}

func (ht *String40HashMap) Destroy(proc *process.Process) {
	if ht == nil || proc == nil {
		return
	}

	if ht.rawData != nil {
		proc.Free(ht.rawData)
		ht.rawData = nil
		ht.bucketData = nil
	}
}
