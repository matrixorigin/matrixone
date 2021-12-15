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

type FixedMap struct {
	bucketCnt  uint32
	elemCnt    uint32
	rawData    []byte
	bucketData []uint64
}

type FixedMapIterator struct {
	table *FixedMap
	idx   uint32
}

func (ht *FixedMap) Init(bucketCnt uint32) {
	rawData := make([]byte, bucketCnt*8)

	ht.bucketCnt = bucketCnt
	ht.rawData = rawData
	ht.bucketData = unsafe.Slice((*uint64)(unsafe.Pointer(&rawData[0])), cap(rawData)/8)[:len(rawData)/8]
}

func (ht *FixedMap) Insert(key uint32) uint64 {
	value := ht.bucketData[key]
	if value == 0 {
		ht.elemCnt++
		value = uint64(ht.elemCnt)
		ht.bucketData[key] = value
	}
	return value
}

func (ht *FixedMap) BucketData() []uint64 {
	return ht.bucketData
}

func (ht *FixedMap) Cardinality() uint64 {
	return uint64(ht.elemCnt)
}

func (it *FixedMapIterator) Init(ht *FixedMap) {
	it.table = ht
	it.idx = 0
}

func (it *FixedMapIterator) Next() (key uint32, value uint64, err error) {
	for it.idx < it.table.bucketCnt && it.table.bucketData[it.idx] == 0 {
		it.idx++
	}

	if it.idx == it.table.bucketCnt {
		err = errors.New("out of range")
		return
	}

	key = it.idx
	value = it.table.bucketData[key]

	return
}
