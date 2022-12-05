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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

type FixedSet struct {
	cellCnt uint32
	bitmap  []uint64
}

type FixedSetIterator struct {
	table      *FixedSet
	bitmapIdx  uint32
	bitmapSize uint32
	bitmapVal  uint64
}

func (ht *FixedSet) Init(cellCnt uint32) {
	ht.cellCnt = cellCnt
	ht.bitmap = make([]uint64, (int64(cellCnt)-1)/64+1)
}

func (ht *FixedSet) Insert(key uint32) (inserted bool) {
	inserted = (ht.bitmap[key/8] | (1 << (key % 8))) == 0
	ht.bitmap[key/8] |= 1 << (key % 8)
	return
}

func (ht *FixedSet) Merge(other *FixedSet) {
	for i, v := range other.bitmap {
		ht.bitmap[i] |= v
	}
}

func (ht *FixedSet) Cardinality() (cnt uint64) {
	for _, v := range ht.bitmap {
		cnt += uint64(bits.OnesCount64(v))
	}
	return
}

func (it *FixedSetIterator) Init(ht *FixedSet) {
	it.table = ht
	it.bitmapIdx = 0
	it.bitmapSize = uint32(len(ht.bitmap))
	it.bitmapVal = ht.bitmap[0]
}

func (it *FixedSetIterator) Next() (key uint32, err error) {
	if it.bitmapVal != 0 {
		tz := bits.TrailingZeros64(it.bitmapVal)
		key = 64*it.bitmapIdx + uint32(tz)
		it.bitmapVal ^= 1 << tz

		return
	}

	for it.bitmapIdx < it.bitmapSize && it.table.bitmap[it.bitmapIdx] == 0 {
		it.bitmapIdx++
	}

	if it.bitmapIdx == it.bitmapSize {
		err = moerr.NewInternalErrorNoCtx("out of range")
		return
	}

	it.bitmapVal = it.table.bitmap[it.bitmapIdx]
	key = 64*it.bitmapIdx + uint32(bits.TrailingZeros64(it.bitmapVal))

	return
}
