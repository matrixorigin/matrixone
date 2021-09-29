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
	"fmt"
	"matrixone/pkg/hash"
	"unsafe"
)

const (
	initialBucketCntBits = 8
	initialBucketCnt     = 1 << initialBucketCntBits
	defaultLoadFactor    = 0.5
)

func NewHashTable(inlineKey, inlineVal bool, keySize, valueSize uint8) *HashTable {
	var hashSize uint8
	if !inlineKey {
		hashSize = 8
	}

	if inlineKey {
		keySize = 24
	}

	if inlineVal {
		valueSize = 24
	}

	table := &HashTable{
		inlineKey:     inlineKey,
		inlineVal:     inlineVal,
		bucketCntBits: initialBucketCntBits,
		bucketCnt:     initialBucketCnt,
		elemCnt:       0,
		maxElemCnt:    initialBucketCnt * defaultLoadFactor,
		keySize:       keySize,
		valSize:       valueSize,
		valOffset:     hashSize + keySize,
		bucketWidth:   hashSize + keySize + valueSize,
	}

	table.bucketData = make([]byte, int(table.bucketWidth)*initialBucketCnt)

	return table
}

func (ht *HashTable) Insert(hashVal uint64, key []byte) (inserted bool, value []byte) {
	ht.resizeOnDemand()

	inserted, idx := ht.findBucket(hashVal, key)
	if inserted {
		ht.elemCnt++
		if !ht.inlineKey {
			ht.keyHolder = append(ht.keyHolder, key)
			*(*uint64)(unsafe.Pointer(&ht.bucketData[idx*uint64(ht.bucketWidth)])) = hashVal
			copy(ht.bucketData[idx*uint64(ht.bucketWidth)+8:], key)
		} else {
			copy(ht.bucketData[idx*uint64(ht.bucketWidth):], key)
		}
	}

	if ht.inlineVal {
		value = ht.bucketData[idx*uint64(ht.bucketWidth)+uint64(ht.valOffset) : (idx+1)*uint64(ht.bucketWidth)]
	} else {
		if inserted {
			value = make([]byte, ht.valSize)
			ht.valHolder = append(ht.valHolder, value)
		} else {
			value = *(*[]byte)(unsafe.Pointer(&ht.bucketData[idx*uint64(ht.bucketWidth)+uint64(ht.valOffset)]))
		}
	}

	return
}

func (ht *HashTable) findBucket(hashVal uint64, key []byte) (empty bool, idx uint64) {
	mask := uint64((1 << ht.bucketCntBits) - 1)
	for idx = hashVal & mask; true; idx = (idx + 1) & mask {
		if *(*uint64)(unsafe.Pointer(&ht.bucketData[idx*uint64(ht.bucketWidth)])) == 0 {
			empty = true
			return
		}

		offset := idx * uint64(ht.bucketWidth)
		if ht.inlineKey {
			if bytes.Equal(ht.bucketData[offset:offset+uint64(ht.keySize)], key) {
				return
			}
		} else if *(*uint64)(unsafe.Pointer(&ht.bucketData[offset])) == hashVal {
			if bytes.Equal(*(*[]byte)(unsafe.Pointer(&ht.bucketData[offset+8])), key) {
				return
			}
		}
	}

	return
}

func (ht *HashTable) resizeOnDemand() {
	if ht.elemCnt < ht.maxElemCnt {
		return
	}

	if ht.bucketCnt >= 23 {
		ht.bucketCntBits += 1
	} else {
		ht.bucketCntBits += 2
	}

	oldBucketCnt := ht.bucketCnt
	oldBucketData := ht.bucketData
	ht.bucketCnt = 1 << ht.bucketCntBits
	ht.bucketData = make([]byte, uint64(ht.bucketWidth)*ht.bucketCnt)
	ht.maxElemCnt = uint64(float64(ht.bucketCnt) * defaultLoadFactor)

	offset := 0
	for i := 0; i < int(oldBucketCnt); i++ {
		hashVal := *(*uint64)(unsafe.Pointer(&oldBucketData[offset]))
		key := oldBucketData[offset : offset+int(ht.keySize)]
		if hashVal != 0 {
			if ht.inlineKey {
				// TODO: implement crc32 hash (different than hash/crc32 in golang library)
				hashVal = uint64(hash.Memhash(unsafe.Pointer(&oldBucketData[offset]), 0, uintptr(ht.keySize)))
			}
		}

		// TODO: optimize reinsertion
		_, value := ht.Insert(hashVal, key)
		copy(value, oldBucketData[offset+int(ht.valOffset):offset+int(ht.bucketWidth)])
		offset += int(ht.bucketWidth)
	}
}

func (ht *HashTable) Cardinality() uint64 {
	return ht.elemCnt
}

func NewHashTableIterator(ht *HashTable) *HashTableIterator {
	return &HashTableIterator{
		table:  ht,
		offset: 0,
		end:    uint64(len(ht.bucketData)),
	}
}

func (it *HashTableIterator) Next() (hashVal uint64, key, value []byte, err error) {
	hashVal = *(*uint64)(unsafe.Pointer(&it.table.bucketData[it.offset]))
	for hashVal == 0 {
		it.offset += uint64(it.table.bucketWidth)
		if it.offset >= it.end {
			break
		}
		hashVal = *(*uint64)(unsafe.Pointer(&it.table.bucketData[it.offset]))
	}

	if it.offset >= it.end {
		err = fmt.Errorf("out of range")
		return
	}

	if it.table.inlineKey {
		key = it.table.bucketData[it.offset+8 : it.offset+uint64(it.table.valOffset)]
	} else {
		key = *(*[]byte)(unsafe.Pointer(&it.table.bucketData[it.offset+8]))
	}

	if it.table.inlineVal {
		value = it.table.bucketData[it.offset+uint64(it.table.valOffset) : it.offset+uint64(it.table.bucketWidth)]
	} else {
		value = *(*[]byte)(unsafe.Pointer(&it.table.bucketData[it.offset+uint64(it.table.valOffset)]))
	}

	return
}
