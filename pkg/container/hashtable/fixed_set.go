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

	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type FixedSet struct {
	bucketCnt uint32
	rawBitmap []byte
	bitmap    []uint64
}

type FixedSetIterator struct {
	table      *FixedSet
	bitmapIdx  uint32
	bitmapSize uint32
	bitmapVal  uint64
}

func (ht *FixedSet) Init(bucketCnt uint32, proc *process.Process) error {
	var occupiedRaw []byte
	var err error

	if proc != nil {
		occupiedRaw, err = proc.Alloc(((int64(bucketCnt)-1)/64 + 1) * 8)
		if err != nil {
			return err
		}
	} else {
		occupiedRaw = make([]byte, ((int64(bucketCnt)-1)/64+1)*8)
	}

	ht.bucketCnt = bucketCnt
	ht.rawBitmap = occupiedRaw
	ht.bitmap = unsafe.Slice((*uint64)(unsafe.Pointer(&occupiedRaw[0])), cap(occupiedRaw)/8)[:len(occupiedRaw)/8]

	return nil
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

func (ht *FixedSet) Destroy(proc *process.Process) {
	if ht == nil {
		return
	}

	if ht.bitmap != nil {
		proc.Free(ht.rawBitmap)
		ht.rawBitmap = nil
		ht.bitmap = nil
	}
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
		err = errors.New("out of range")
		return
	}

	it.bitmapVal = it.table.bitmap[it.bitmapIdx]
	key = 64*it.bitmapIdx + uint32(bits.TrailingZeros64(it.bitmapVal))

	return
}
