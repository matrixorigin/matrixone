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

func (ht *StringHashMap) Init() {
	const cellSize = int(unsafe.Sizeof(StringHashMapCell{}))

	rawData := make([]byte, cellSize*kInitialBucketCnt)

	ht.bucketCntBits = kInitialBucketCntBits
	ht.bucketCnt = kInitialBucketCnt
	ht.elemCnt = 0
	ht.maxElemCnt = kInitialBucketCnt * kLoadFactorNumerator / kLoadFactorDenominator
	ht.rawData = rawData
	ht.bucketData = unsafe.Slice((*StringHashMapCell)(unsafe.Pointer(&rawData[0])), kInitialBucketCnt)
}

func (ht *StringHashMap) Insert(key StringRef) uint64 {
	ht.resizeOnDemand()

	var hash uint64
	var key128 [2]uint64

	if key.Len <= 8 {
		copy(unsafe.Slice((*byte)(unsafe.Pointer(&key128[0])), 8), unsafe.Slice(key.Ptr, key.Len))
		hash = crc32Int64Hash(key128[0]) | (uint64(key.Len) << 32)
	} else if key.Len <= 16 {
		copy(unsafe.Slice((*byte)(unsafe.Pointer(&key128[0])), 16), unsafe.Slice(key.Ptr, key.Len))
		hash = crc32BytesHash(unsafe.Pointer(key.Ptr), key.Len)
	} else {
		hash = crc32BytesHash(unsafe.Pointer(key.Ptr), key.Len)
		key128 = aesBytesHash(unsafe.Pointer(key.Ptr), key.Len)
	}

	empty, _, cell := ht.findBucket(hash, &key128)
	if empty {
		ht.elemCnt++
		cell.Hash = hash
		cell.Key128 = key128
		cell.Mapped = ht.elemCnt
	}

	return cell.Mapped
}

func (ht *StringHashMap) Find(key StringRef) uint64 {
	var hash uint64
	var key128 [2]uint64

	if key.Len <= 8 {
		copy(unsafe.Slice((*byte)(unsafe.Pointer(&key128[0])), 8), unsafe.Slice(key.Ptr, key.Len))
		hash = crc32Int64Hash(key128[0]) | (uint64(key.Len) << 32)
	} else if key.Len <= 16 {
		copy(unsafe.Slice((*byte)(unsafe.Pointer(&key128[0])), 16), unsafe.Slice(key.Ptr, key.Len))
		hash = crc32BytesHash(unsafe.Pointer(key.Ptr), key.Len)
	} else {
		hash = crc32BytesHash(unsafe.Pointer(key.Ptr), key.Len)
		key128 = aesBytesHash(unsafe.Pointer(key.Ptr), key.Len)
	}

	_, _, cell := ht.findBucket(hash, &key128)

	return cell.Mapped
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

func (ht *StringHashMap) resizeOnDemand() {
	if ht.elemCnt < ht.maxElemCnt {
		return
	}

	newBucketCntBits := ht.bucketCntBits + 2
	newBucketCnt := uint64(1) << newBucketCntBits
	newMaxElemCnt := newBucketCnt * kLoadFactorNumerator / kLoadFactorDenominator

	const cellSize = int(unsafe.Sizeof(StringHashMapCell{}))

	oldBucketCnt := ht.bucketCnt
	oldBucketData := ht.bucketData

	newRawData := make([]byte, uint64(cellSize)*newBucketCnt)
	newBucketData := unsafe.Slice((*StringHashMapCell)(unsafe.Pointer(&newRawData[0])), newBucketCnt)

	ht.bucketCntBits = newBucketCntBits
	ht.bucketCnt = newBucketCnt
	ht.maxElemCnt = newMaxElemCnt
	ht.rawData = newRawData
	ht.bucketData = newBucketData

	for i := uint64(0); i < oldBucketCnt; i++ {
		cell := &oldBucketData[i]
		if cell.Hash != 0 {
			_, newIdx, _ := ht.findBucket(cell.Hash, &cell.Key128)
			ht.bucketData[newIdx] = *cell
		}
	}
}

func (ht *StringHashMap) Cardinality() uint64 {
	return ht.elemCnt
}

type StringHashMapIterator struct {
	table *StringHashMap
	pos   uint64
}

func (it *StringHashMapIterator) Init(ht *StringHashMap) {
	it.table = ht
}

func (it *StringHashMapIterator) Next() (cell *StringHashMapCell, err error) {
	for it.pos < it.table.bucketCnt {
		cell = &it.table.bucketData[it.pos]
		if cell.Hash != 0 {
			break
		}
		it.pos++
	}

	if it.pos >= it.table.bucketCnt {
		err = errors.New("out of range")
		return
	}

	it.pos++

	return
}
