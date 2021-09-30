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
	"fmt"
	"math/bits"
	"unsafe"
)

func NewFixedTable(inlineVal bool, bucketCnt uint32, valueSize uint8) *FixedTable {
	if inlineVal {
		valueSize = 24
	}
	table := &FixedTable{
		inlineVal:  inlineVal,
		bucketCnt:  bucketCnt,
		valSize:    valueSize,
		occupied:   make([]uint64, (bucketCnt-1)/64+1),
		bucketData: make([]byte, bucketCnt*uint32(valueSize)),
	}
	return table
}

func (ht *FixedTable) Insert(key uint32) (inserted bool, value []byte) {
	ht.occupied[key/64] |= 1 << key % 64
	value = ht.getValue(key)
	return
}

func (ht *FixedTable) getValue(key uint32) []byte {
	if ht.inlineVal {
		return ht.bucketData[key*uint32(ht.valSize) : (key+1)*uint32(ht.valSize)]
	} else {
		return *(*[]byte)(unsafe.Pointer(&ht.bucketData[key*uint32(ht.valSize)]))
	}
}

func (ht *FixedTable) Merge(other *FixedTable) {
	for i, v := range other.occupied {
		ht.occupied[i] |= v
	}
}

func (ht *FixedTable) BucketData() []byte {
	return ht.bucketData
}

func (ht *FixedTable) Cardinality() (cnt uint64) {
	for _, v := range ht.occupied {
		cnt += uint64(bits.OnesCount64(v))
	}
	return
}

func NewFixedTableIterator(ht *FixedTable) *FixedTableIterator {
	return &FixedTableIterator{
		table:     ht,
		bitmapIdx: 0,
		bitmapVal: ht.occupied[0],
	}
}

func (it *FixedTableIterator) Next() (key uint32, value []byte, err error) {
	if it.bitmapVal != 0 {
		key = 64*it.bitmapIdx + uint32(bits.TrailingZeros64(it.bitmapVal))
		if it.table.valSize > 0 {
			value = it.table.getValue(key)
		}
	} else {
		lastIdx := uint32(len(it.table.occupied))
		for it.bitmapIdx < lastIdx && it.table.occupied[it.bitmapIdx] == 0 {
			it.bitmapIdx++
		}
		if it.bitmapIdx == lastIdx {
			err = fmt.Errorf("out of range")
		} else {
			it.bitmapVal = it.table.occupied[it.bitmapIdx]
			key = 64*it.bitmapIdx + uint32(bits.TrailingZeros64(it.bitmapVal))
			if it.table.valSize > 0 {
				value = it.table.getValue(key)
			}
		}
	}
	return
}
