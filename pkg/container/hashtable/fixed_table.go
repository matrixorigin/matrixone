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
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"unsafe"
)

func NewFixedTable(inlineVal bool, bucketCnt uint32, valueSize uint8, proc process.Process) (*FixedTable, error) {
	if inlineVal {
		valueSize = 24
	}

	occupied, err := proc.Alloc(((int64(bucketCnt)-1)/64 + 1) * 8)
	if err != nil {
		return nil, err
	}

	bucketData, err := proc.Alloc(int64(bucketCnt) * int64(valueSize))
	if err != nil {
		proc.Free(occupied)
		return nil, err
	}

	return &FixedTable{
		inlineVal:  inlineVal,
		bucketCnt:  bucketCnt,
		valSize:    valueSize,
		occupied:   occupied,
		bucketData: bucketData,
	}, nil
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
	occupied := unsafe.Slice((*uint64)(unsafe.Pointer(&ht.occupied[0])), len(ht.occupied)/8)
	for _, v := range occupied {
		cnt += uint64(bits.OnesCount64(v))
	}
	return
}

func (ht *FixedTable) Destroy(proc process.Process) {
	proc.Free(ht.occupied)
	proc.Free(ht.bucketData)
}

func NewFixedTableIterator(ht *FixedTable) *FixedTableIterator {
	bitmap := unsafe.Slice((*uint64)(unsafe.Pointer(&ht.occupied[0])), len(ht.occupied)/8)
	return &FixedTableIterator{
		table:     ht,
		bitmap:    bitmap,
		bitmapIdx: 0,
		bitmapVal: bitmap[0],
	}
}

func (it *FixedTableIterator) Next() (key uint32, value []byte, err error) {
	if it.bitmapVal != 0 {
		tz := bits.TrailingZeros64(it.bitmapVal)
		key = 64*it.bitmapIdx + uint32(tz)
		it.bitmapVal ^= 1 << tz

		if it.table.valSize > 0 {
			value = it.table.getValue(key)
		}
		return
	}

	lastIdx := uint32(len(it.bitmap))
	for it.bitmapIdx < lastIdx && it.bitmap[it.bitmapIdx] == 0 {
		it.bitmapIdx++
	}

	if it.bitmapIdx == lastIdx {
		err = fmt.Errorf("out of range")
		return
	}

	it.bitmapVal = it.bitmap[it.bitmapIdx]
	key = 64*it.bitmapIdx + uint32(bits.TrailingZeros64(it.bitmapVal))
	if it.table.valSize > 0 {
		value = it.table.getValue(key)
	}

	return
}
