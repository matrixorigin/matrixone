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
	"math/bits"
	"unsafe"
)

type String24HashMapCell struct {
	Hash   uint64
	Key    [3]uint64
	Mapped uint64
}

func (hdr *String24HashMapCell) StrKey() StringRef {
	return StringRef{
		Ptr: (*byte)(unsafe.Pointer(&hdr.Key[0])),
		Len: 24 - (bits.LeadingZeros64(hdr.Key[2]) >> 3),
	}
}

type String24HashMap struct {
	bucketCntBits uint8
	bucketCnt     uint64
	elemCnt       uint64
	maxElemCnt    uint64
	rawData       []byte
	bucketData    []String24HashMapCell
}

func (ht *String24HashMap) Init() {
	const cellSize = int(unsafe.Sizeof(String24HashMapCell{}))

	rawData := make([]byte, cellSize*kInitialBucketCnt)

	ht.bucketCntBits = kInitialBucketCntBits
	ht.bucketCnt = kInitialBucketCnt
	ht.elemCnt = 0
	ht.maxElemCnt = kInitialBucketCnt * kLoadFactorNumerator / kLoadFactorDenominator
	ht.rawData = rawData
	ht.bucketData = unsafe.Slice((*String24HashMapCell)(unsafe.Pointer(&rawData[0])), kInitialBucketCnt)
}

func (ht *String24HashMap) Insert(hash uint64, key *[3]uint64) (inserted bool, value *uint64) {
	ht.resizeOnDemand(1)

	if hash == 0 {
		hash = crc32Int192Hash(key)
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

func (ht *String24HashMap) InsertBatch(hashes []uint64, keys [][3]uint64, inserted []uint8, values []*uint64) {
	ht.resizeOnDemand(uint64(len(keys)))

	if hashes[0] == 0 {
		crc32Int192BatchHash(&keys[0], &hashes[0], len(keys))
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
}

func (ht *String24HashMap) Find(hash uint64, key *[3]uint64) (value *uint64) {
	if hash == 0 {
		hash = crc32Int192Hash(key)
	}

	empty, _, cell := ht.findBucket(hash, key)
	if !empty {
		value = &cell.Mapped
	}

	return
}

func (ht *String24HashMap) FindBatch(hashes []uint64, keys [][3]uint64, values []*uint64) {
	if hashes[0] == 0 {
		crc32Int192BatchHash(&keys[0], &hashes[0], len(keys))
	}

	for i := range keys {
		empty, _, cell := ht.findBucket(hashes[i], &keys[i])
		if !empty {
			values[i] = &cell.Mapped
		}
	}
}

func (ht *String24HashMap) findBucket(hash uint64, key *[3]uint64) (empty bool, idx uint64, cell *String24HashMapCell) {
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

func (ht *String24HashMap) resizeOnDemand(n uint64) {
	targetCnt := ht.elemCnt + n
	if targetCnt <= ht.maxElemCnt {
		return
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

	const cellSize = int(unsafe.Sizeof(String24HashMapCell{}))

	oldBucketCnt := ht.bucketCnt
	oldBucketData := ht.bucketData

	newRawData := make([]byte, uint64(cellSize)*newBucketCnt)
	newBucketData := unsafe.Slice((*String24HashMapCell)(unsafe.Pointer(&newRawData[0])), newBucketCnt)

	ht.bucketCntBits = newBucketCntBits
	ht.bucketCnt = newBucketCnt
	ht.maxElemCnt = newMaxElemCnt
	ht.rawData = newRawData
	ht.bucketData = newBucketData

	for i := uint64(0); i < oldBucketCnt; i++ {
		cell := &oldBucketData[i]
		if cell.Hash != 0 {
			_, newIdx, _ := ht.findBucket(cell.Hash, &cell.Key)
			ht.bucketData[newIdx] = *cell
		}
	}
}

func (ht *String24HashMap) Cardinality() uint64 {
	return ht.elemCnt
}

type String24HashMapIterator struct {
	table *String24HashMap
	pos   uint64
}

func (it *String24HashMapIterator) Init(ht *String24HashMap) {
	it.table = ht
}

func (it *String24HashMapIterator) Next() (cell *String24HashMapCell, err error) {
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
