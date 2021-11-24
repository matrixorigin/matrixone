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

	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type Int64HashMapCell struct {
	Key    uint64
	Mapped uint64
}

type Int64HashMap struct {
	hasZero       uint8
	bucketCntBits uint8
	bucketCnt     uint64
	elemCnt       uint64
	maxElemCnt    uint64
	zeroMapped    uint64
	rawData       []byte
	bucketData    []Int64HashMapCell
}

type Int64HashMapIterator struct {
	table *Int64HashMap
	pos   uint64
}

func (ht *Int64HashMap) Init(proc *process.Process) error {
	const cellSize = int(unsafe.Sizeof(Int64HashMapCell{}))

	var rawData []byte
	var err error
	if proc != nil {
		rawData, err = proc.Alloc(int64(cellSize) * kInitialBucketCnt)
		if err != nil {
			return err
		}
	} else {
		rawData = make([]byte, cellSize*kInitialBucketCnt)
	}

	ht.bucketCntBits = kInitialBucketCntBits
	ht.bucketCnt = kInitialBucketCnt
	ht.elemCnt = 0
	ht.maxElemCnt = kInitialBucketCnt * kLoadFactorNumerator / kLoadFactorDenominator
	ht.rawData = rawData
	ht.bucketData = unsafe.Slice((*Int64HashMapCell)(unsafe.Pointer(&rawData[0])), cap(rawData)/cellSize)[:len(rawData)/cellSize]

	return nil
}

func (ht *Int64HashMap) Insert(hash uint64, keyPtr unsafe.Pointer, proc *process.Process) (inserted bool, value *uint64, err error) {
	key := *(*uint64)(keyPtr)
	if key == 0 {
		inserted = ht.hasZero == 0
		ht.hasZero = 1
		value = &ht.zeroMapped
		return
	}

	err = ht.resizeOnDemand(1, proc)
	if err != nil {
		return
	}

	if hash == 0 {
		hash = crc32Int64HashAsm(key)
	}

	inserted, _, cell := ht.findBucket(hash, key)
	if inserted {
		ht.elemCnt++
		cell.Key = key
	}

	value = &cell.Mapped

	return
}

func (ht *Int64HashMap) InsertBatch(n int, hashes []uint64, keysPtr unsafe.Pointer, inserted []uint8, values []*uint64, proc *process.Process) (err error) {
	err = ht.resizeOnDemand(n, proc)
	if err != nil {
		return
	}

	if hashes[0] == 0 {
		crc32Int64BatchHashAsm(keysPtr, &hashes[0], n)
	}

	keys := unsafe.Slice((*uint64)(keysPtr), n)
	for i, key := range keys {
		if key == 0 {
			inserted[i] = 1 - ht.hasZero
			ht.hasZero = 1
			values[i] = &ht.zeroMapped
		} else {
			isInserted, _, cell := ht.findBucket(hashes[i], key)
			if isInserted {
				ht.elemCnt++
				inserted[i] = 1
				cell.Key = key
			}
			values[i] = &cell.Mapped
		}
	}

	return
}

func (ht *Int64HashMap) Find(hash uint64, keyPtr unsafe.Pointer) (value *uint64) {
	key := *(*uint64)(keyPtr)
	if key == 0 {
		if ht.hasZero == 1 {
			value = &ht.zeroMapped
		}
		return
	}

	if hash == 0 {
		hash = crc32Int64HashAsm(key)
	}

	empty, _, cell := ht.findBucket(hash, key)
	if !empty {
		value = &cell.Mapped
	}

	return
}

func (ht *Int64HashMap) FindBatch(n int, hashes []uint64, keysPtr unsafe.Pointer, values []*uint64) {
	if hashes[0] == 0 {
		crc32Int64BatchHashAsm(keysPtr, &hashes[0], n)
	}

	keys := unsafe.Slice((*uint64)(keysPtr), n)
	for i, key := range keys {
		if key == 0 {
			if ht.hasZero == 1 {
				values[i] = &ht.zeroMapped
			}
		} else {
			empty, _, cell := ht.findBucket(hashes[i], key)
			if !empty {
				values[i] = &cell.Mapped
			}
		}
	}
}

func (ht *Int64HashMap) findBucket(hash uint64, key uint64) (empty bool, idx uint64, cell *Int64HashMapCell) {
	mask := ht.bucketCnt - 1
	var equal bool
	for idx = hash & mask; true; idx = (idx + 1) & mask {
		cell = &ht.bucketData[idx]
		empty, equal = cell.Key == 0, cell.Key == key
		if empty || equal {
			return
		}
	}

	return
}

func (ht *Int64HashMap) resizeOnDemand(n int, proc *process.Process) error {
	targetCnt := ht.elemCnt + uint64(n)
	if targetCnt <= ht.maxElemCnt {
		return nil
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

	const cellSize = int(unsafe.Sizeof(Int64HashMapCell{}))

	var newRawData []byte
	var err error
	if proc != nil {
		newRawData, err = proc.Alloc(int64(cellSize) * int64(newBucketCnt))
		if err != nil {
			return err
		}
	} else {
		newRawData = make([]byte, uint64(cellSize)*newBucketCnt)
	}

	copy(newRawData, ht.rawData)
	if proc != nil {
		proc.Free(ht.rawData)
	}

	oldBucketCnt := ht.bucketCnt
	ht.bucketCntBits = newBucketCntBits
	ht.bucketCnt = newBucketCnt
	ht.maxElemCnt = newMaxElemCnt
	ht.rawData = newRawData
	ht.bucketData = unsafe.Slice((*Int64HashMapCell)(unsafe.Pointer(&newRawData[0])), cap(newRawData)/cellSize)[:len(newRawData)/cellSize]

	var hashes [256]uint64

	var i uint64
	for i = 0; i < oldBucketCnt; i += 256 {
		cells := ht.bucketData[i : i+256]
		crc32Int64CellBatchHashAsm(unsafe.Pointer(&cells[0]), &hashes[0], 256)
		ht.reinsertBatch(cells[:], hashes[:], i)
	}

	for {
		cells := ht.bucketData[i : i+256]
		crc32Int64CellBatchHashAsm(unsafe.Pointer(&cells[0]), &hashes[0], 256)
		if ht.reinsertBatch(cells[:], hashes[:], i) == 0 {
			break
		}
	}

	return nil
}

func (ht *Int64HashMap) reinsert(idx uint64) bool {
	key := ht.bucketData[idx].Key
	if key == 0 {
		return false
	}

	_, newIdx, _ := ht.findBucket(crc32Int64HashAsm(key), key)
	if newIdx == idx {
		return false
	}

	ht.bucketData[newIdx] = ht.bucketData[idx]
	ht.bucketData[idx] = Int64HashMapCell{}

	return true
}

func (ht *Int64HashMap) reinsertBatch(cells []Int64HashMapCell, hashes []uint64, offset uint64) uint64 {
	var reinserted uint64

	for idx, cell := range cells {
		if cell.Key == 0 {
			continue
		}

		_, newIdx, _ := ht.findBucket(hashes[idx], cell.Key)
		if newIdx == offset+uint64(idx) {
			continue
		}

		ht.bucketData[newIdx] = cell
		cells[idx] = Int64HashMapCell{}

		reinserted++
	}

	return reinserted
}

func (ht *Int64HashMap) Cardinality() uint64 {
	return ht.elemCnt
}

func (ht *Int64HashMap) Destroy(proc *process.Process) {
	if ht == nil || proc == nil {
		return
	}

	if ht.rawData != nil {
		proc.Free(ht.rawData)
		ht.rawData = nil
		ht.bucketData = nil
	}
}

func (it *Int64HashMapIterator) Init(ht *Int64HashMap) {
	it.table = ht
	it.pos = 0
}

func (it *Int64HashMapIterator) Next() (cell *Int64HashMapCell, err error) {
	for it.pos < it.table.bucketCnt {
		cell = &it.table.bucketData[it.pos]
		if cell.Key != 0 {
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
