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

type MockFixedTable struct {
	bucketCnt  uint32
	occupied   []byte
	rawData    []byte
	bucketData []uint64
}

type MockFixedTableIterator struct {
	table      *MockFixedTable
	bitmap     []uint64
	bitmapIdx  uint32
	bitmapSize uint32
	bitmapVal  uint64
}

func (ht *MockFixedTable) Init(bucketCnt uint32) error {
	var occupied []byte
	occupied = make([]byte, ((int64(bucketCnt)-1)/64+1)*8)

	const cSize = int64(unsafe.Sizeof(uint64(0)))
	var bucketData []byte
	bucketData = make([]byte, int64(bucketCnt)*cSize)
	ht.bucketCnt = bucketCnt
	ht.occupied = occupied
	ht.rawData = bucketData
	ht.bucketData = unsafe.Slice((*uint64)(unsafe.Pointer(&bucketData[0])), cap(bucketData)/int(cSize))[:bucketCnt]

	return nil
}

func (ht *MockFixedTable) Insert(key uint32) (inserted bool, value *uint64) {
	inserted = (ht.occupied[key/8] | (1 << (key % 8))) == 0
	ht.occupied[key/8] |= 1 << (key % 8)
	value = ht.getValue(key)
	return
}

func (ht *MockFixedTable) getValue(key uint32) *uint64 {
	return &ht.bucketData[key]
}

func (ht *MockFixedTable) Merge(other *MockFixedTable) {
	for i, v := range other.occupied {
		ht.occupied[i] |= v
	}
}

func (ht *MockFixedTable) BucketData() []uint64 {
	return ht.bucketData
}

func (ht *MockFixedTable) Cardinality() (cnt uint64) {
	occupied := unsafe.Slice((*uint64)(unsafe.Pointer(&ht.occupied[0])), cap(ht.occupied)/8)[:len(ht.occupied)/8]
	for _, v := range occupied {
		cnt += uint64(bits.OnesCount64(v))
	}
	return
}

func (ht *MockFixedTable) Destroy() {
	if ht == nil {
		return
	}

	if ht.occupied != nil {
		ht.occupied = nil
	}
	if ht.rawData != nil {
		ht.rawData = nil
		ht.bucketData = nil
	}
}

func (it *MockFixedTableIterator) Init(ht *MockFixedTable) {
	bitmap := unsafe.Slice((*uint64)(unsafe.Pointer(&ht.occupied[0])), len(ht.occupied)/8)
	it.table = ht
	it.bitmap = bitmap
	it.bitmapIdx = 0
	it.bitmapSize = uint32(len(bitmap))
	it.bitmapVal = bitmap[0]
}

func (it *MockFixedTableIterator) Next() (key uint32, value *uint64, err error) {
	if it.bitmapVal != 0 {
		tz := bits.TrailingZeros64(it.bitmapVal)
		key = 64*it.bitmapIdx + uint32(tz)
		it.bitmapVal ^= 1 << tz

		value = it.table.getValue(key)
		return
	}

	for it.bitmapIdx < it.bitmapSize && it.bitmap[it.bitmapIdx] == 0 {
		it.bitmapIdx++
	}

	if it.bitmapIdx == it.bitmapSize {
		err = errors.New("out of range")
		return
	}

	it.bitmapVal = it.bitmap[it.bitmapIdx]
	key = 64*it.bitmapIdx + uint32(bits.TrailingZeros64(it.bitmapVal))
	value = it.table.getValue(key)

	return
}
