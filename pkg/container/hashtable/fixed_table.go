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
)

func NewFixedTable(bucketCnt, valSize uint32, agg Aggregator) *FixedTable {
	table := &FixedTable{
		bucketCnt:  bucketCnt,
		occupied:   make([]uint64, (bucketCnt-1)/64+1),
		bucketData: make([]byte, bucketCnt*valSize),
		agg:        agg,
	}

	if agg != nil {
		table.valueSize = agg.StateSize()
		table.bucketData = make([]byte, bucketCnt*uint32(table.valueSize))
		agg.ArrayInit(table.bucketData)
	}
	return table
}

func (ht *FixedTable) Insert(key uint32, value []byte, agg func([]byte, []byte)) {
	ht.occupied[key/64] |= 1 << key % 64
	if ht.agg != nil {
		ht.agg.Aggregate(ht.GetValue(key), value)
	}
}

func (ht *FixedTable) GetValue(key uint32) []byte {
	return ht.bucketData[key*uint32(ht.valueSize) : (key+1)*uint32(ht.valueSize)]
}

func (ht *FixedTable) Merge(other *FixedTable, agg func([]byte, []byte)) {
	for i, v := range other.occupied {
		ht.occupied[i] |= v
	}
	if ht.agg != nil {
		ht.agg.ArrayMerge(ht.bucketData, other.bucketData)
	}
}

func (ht *FixedTable) Count() (cnt uint) {
	for _, v := range ht.occupied {
		cnt += uint(bits.OnesCount64(v))
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
		key = uint32(64*it.bitmapIdx + bits.TrailingZeros64(it.bitmapVal))
		if it.table.agg != nil {
			value = it.table.GetValue(key)
		}
	} else {
		lastIdx := len(it.table.occupied)
		for it.bitmapIdx < lastIdx && it.table.occupied[it.bitmapIdx] == 0 {
			it.bitmapIdx++
		}
		if it.bitmapIdx == lastIdx {
			err = fmt.Errorf("out of range")
		} else {
			it.bitmapVal = it.table.occupied[it.bitmapIdx]
			key = uint32(64*it.bitmapIdx + bits.TrailingZeros64(it.bitmapVal))
			if it.table.agg != nil {
				value = it.table.GetValue(key)
			}
		}
	}
	return
}
