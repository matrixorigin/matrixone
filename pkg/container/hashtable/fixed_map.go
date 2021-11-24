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

type FixedMap struct {
	bucketCnt  uint32
	rawBitmap  []byte
	bitmap     []uint64
	rawData    []byte
	bucketData []uint64
}

type FixedMapIterator struct {
	table      *FixedMap
	bitmapIdx  uint32
	bitmapSize uint32
	bitmapVal  uint64
}

func (ht *FixedMap) Init(bucketCnt uint32, proc *process.Process) error {
	var rawBitmap []byte
	var rawData []byte
	var err error

	if proc != nil {
		rawBitmap, err = proc.Alloc(((int64(bucketCnt)-1)/64 + 1) * 8)
		if err != nil {
			return err
		}
	} else {
		rawBitmap = make([]byte, ((int64(bucketCnt)-1)/64+1)*8)
	}

	if proc != nil {
		rawData, err = proc.Alloc(int64(bucketCnt) * 8)
		if err != nil {
			proc.Free(rawBitmap)
			return err
		}
	} else {
		rawData = make([]byte, bucketCnt*8)
	}

	ht.bucketCnt = bucketCnt
	ht.rawBitmap = rawBitmap
	ht.bitmap = unsafe.Slice((*uint64)(unsafe.Pointer(&rawBitmap[0])), cap(rawBitmap)/8)[:len(rawBitmap)/8]
	ht.rawData = rawData
	ht.bucketData = unsafe.Slice((*uint64)(unsafe.Pointer(&rawData[0])), cap(rawData)/8)[:len(rawData)/8]

	return nil
}

func (ht *FixedMap) Insert(key uint32) (inserted bool, value *uint64) {
	inserted = (ht.bitmap[key/8] | (1 << (key % 8))) == 0
	ht.bitmap[key/8] |= 1 << (key % 8)
	value = &ht.bucketData[key]
	return
}

func (ht *FixedMap) Merge(other *FixedMap) {
	for i, v := range other.bitmap {
		ht.bitmap[i] |= v
	}
}

func (ht *FixedMap) BucketData() []uint64 {
	return ht.bucketData
}

func (ht *FixedMap) Cardinality() (cnt uint64) {
	for _, v := range ht.bitmap {
		cnt += uint64(bits.OnesCount64(v))
	}
	return
}

func (ht *FixedMap) Destroy(proc *process.Process) {
	if ht == nil {
		return
	}

	if ht.rawBitmap != nil {
		proc.Free(ht.rawBitmap)
		ht.rawBitmap = nil
		ht.bitmap = nil
	}
	if ht.rawData != nil {
		proc.Free(ht.rawData)
		ht.rawData = nil
		ht.bucketData = nil
	}
}

func (it *FixedMapIterator) Init(ht *FixedMap) {
	it.table = ht
	it.bitmapIdx = 0
	it.bitmapSize = uint32(len(ht.bitmap))
	it.bitmapVal = ht.bitmap[0]
}

func (it *FixedMapIterator) Next() (key uint32, value *uint64, err error) {
	if it.bitmapVal != 0 {
		tz := bits.TrailingZeros64(it.bitmapVal)
		key = 64*it.bitmapIdx + uint32(tz)
		it.bitmapVal ^= 1 << tz

		value = &it.table.bucketData[key]
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
	value = &it.table.bucketData[key]

	return
}
