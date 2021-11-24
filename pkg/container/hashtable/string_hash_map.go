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

	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type StringRef struct {
	Ptr *byte
	Len int
}

type StringHashMapCell struct {
	Hash   uint64
	Key128 [2]uint64
	Mapped uint64
}

type StringHashMap struct {
	bucketCntBits uint8
	bucketCnt     uint64
	elemCnt       uint64
	maxElemCnt    uint64
	rawData       []byte
	bucketData    []StringHashMapCell
}

func (ht *StringHashMap) Init(proc *process.Process) error {
	const cellSize = int(unsafe.Sizeof(StringHashMapCell{}))

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
	ht.bucketData = unsafe.Slice((*StringHashMapCell)(unsafe.Pointer(&rawData[0])), cap(rawData)/cellSize)[:len(rawData)/cellSize]

	return nil
}

func (ht *StringHashMap) Insert(hash uint64, key StringRef, proc *process.Process) (inserted bool, value *uint64, err error) {
	err = ht.resizeOnDemand(proc)
	if err != nil {
		return
	}

	if hash == 0 {
		if key.Len <= 8 {
			copy(unsafe.Slice((*byte)(unsafe.Pointer(&hash)), 8), unsafe.Slice(key.Ptr, key.Len))
			hash = crc32Int64HashAsm(hash) | (uint64(key.Len) << 32)
		} else {
			hash = crc32BytesHashAsm(unsafe.Pointer(key.Ptr), key.Len)
		}
	}

	var key128 [2]uint64
	if key.Len <= 16 {
		copy(unsafe.Slice((*byte)(unsafe.Pointer(&key128[0])), 16), unsafe.Slice(key.Ptr, key.Len))
	} else {
		key128 = aesBytesHashAsm(unsafe.Pointer(key.Ptr), key.Len)
	}

	inserted, _, cell := ht.findBucket(hash, &key128)
	if inserted {
		ht.elemCnt++
		cell.Hash = hash
		cell.Key128 = key128
	}

	value = &cell.Mapped

	return
}

func (ht *StringHashMap) Find(hash uint64, key StringRef) *uint64 {
	if hash == 0 {
		if key.Len <= 8 {
			copy(unsafe.Slice((*byte)(unsafe.Pointer(&hash)), 8), unsafe.Slice(key.Ptr, key.Len))
			hash = crc32Int64HashAsm(hash) | (uint64(key.Len) << 32)
		} else {
			hash = crc32BytesHashAsm(unsafe.Pointer(key.Ptr), key.Len)
		}
	}

	var key128 [2]uint64
	if key.Len <= 16 {
		copy(unsafe.Slice((*byte)(unsafe.Pointer(&key128[0])), 16), unsafe.Slice(key.Ptr, key.Len))
	} else {
		key128 = aesBytesHashAsm(unsafe.Pointer(key.Ptr), key.Len)
	}

	_, _, cell := ht.findBucket(hash, &key128)

	return &cell.Mapped
}

func (ht *StringHashMap) findBucket(hash uint64, key128 *[2]uint64) (empty bool, idx uint64, cell *StringHashMapCell) {
	mask := ht.bucketCnt - 1
	for idx = hash & mask; true; idx = (idx + 1) & mask {
		cell = &ht.bucketData[idx]
		empty = cell.Hash == 0
		if empty || (cell.Hash == hash && cell.Key128 == *key128) {
			return
		}
	}

	return
}

func (ht *StringHashMap) resizeOnDemand(proc *process.Process) error {
	if ht.elemCnt < ht.maxElemCnt {
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

	const cellSize = int(unsafe.Sizeof(StringHashMapCell{}))

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
	ht.bucketData = unsafe.Slice((*StringHashMapCell)(unsafe.Pointer(&newRawData[0])), cap(newRawData)/cellSize)[:len(newRawData)/cellSize]

	var i uint64
	for i = 0; i < oldBucketCnt; i++ {
		ht.reinsert(i)
	}

	for ht.reinsert(i) {
		i++
	}

	return nil
}

func (ht *StringHashMap) reinsert(idx uint64) bool {
	cell := &ht.bucketData[idx]
	if cell.Hash == 0 {
		return false
	}

	_, newIdx, _ := ht.findBucket(cell.Hash, &cell.Key128)
	if newIdx == idx {
		return false
	}

	ht.bucketData[newIdx] = *cell
	*cell = StringHashMapCell{}

	return true
}

func (ht *StringHashMap) Cardinality() uint64 {
	return ht.elemCnt
}

func (ht *StringHashMap) Destroy(proc *process.Process) {
	if ht == nil || proc == nil {
		return
	}

	if ht.rawData != nil {
		proc.Free(ht.rawData)
		ht.rawData = nil
		ht.bucketData = nil
	}
}
