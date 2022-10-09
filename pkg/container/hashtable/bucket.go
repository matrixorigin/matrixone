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
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
)

func (ht *StringHashMap) InsertStringBatchInBucket(states [][3]uint64, keys [][]byte, values []uint64, ibucket, nbucket uint64, m *mpool.MPool) error {
	if err := ht.resizeOnDemand(uint64(len(keys)), m); err != nil {
		return err
	}

	BytesBatchGenHashStates(&keys[0], &states[0], len(keys))

	for i := range keys {
		if states[i][0]%nbucket != ibucket {
			values[i] = 0
			continue
		}
		cell := ht.findCell(&states[i])
		if cell.Mapped == 0 {
			ht.elemCnt++
			cell.HashState = states[i]
			cell.Mapped = ht.elemCnt
		}
		values[i] = cell.Mapped
	}
	return nil
}

/*
func (ht *StringHashMap) InsertString24BatchInBucket(states [][3]uint64, keys [][3]uint64, values []uint64, ibucket, nbucket uint64) {
	ht.resizeOnDemand(uint64(len(keys)))

	AesInt192BatchGenHashStates(&keys[0], &states[0], len(keys))

	for i := range keys {
		if states[i][0]%nbucket != ibucket {
			continue
		}
		cell := ht.findCell(&states[i])
		if cell.Mapped == 0 {
			ht.elemCnt++
			cell.HashState = states[i]
			cell.Mapped = ht.elemCnt
		}
		values[i] = cell.Mapped
	}
}

func (ht *StringHashMap) InsertString32BatchInBucket(states [][3]uint64, keys [][4]uint64, values []uint64, ibucket, nbucket uint64) {
	ht.resizeOnDemand(uint64(len(keys)))

	AesInt256BatchGenHashStates(&keys[0], &states[0], len(keys))

	for i := range keys {
		if states[i][0]%nbucket != ibucket {
			continue
		}
		cell := ht.findCell(&states[i])
		if cell.Mapped == 0 {
			ht.elemCnt++
			cell.HashState = states[i]
			cell.Mapped = ht.elemCnt
		}
		values[i] = cell.Mapped
	}
}

func (ht *StringHashMap) InsertString40BatchInBucket(states [][3]uint64, keys [][5]uint64, values []uint64, ibucket, nbucket uint64) {
	ht.resizeOnDemand(uint64(len(keys)))

	AesInt320BatchGenHashStates(&keys[0], &states[0], len(keys))

	for i := range keys {
		if states[i][0]%nbucket != ibucket {
			continue
		}
		cell := ht.findCell(&states[i])
		if cell.Mapped == 0 {
			ht.elemCnt++
			cell.HashState = states[i]
			cell.Mapped = ht.elemCnt
		}
		values[i] = cell.Mapped
	}
}
*/

func (ht *StringHashMap) InsertStringBatchWithRingInBucket(zValues []int64, states [][3]uint64, keys [][]byte, values []uint64, ibucket, nbucket uint64, m *mpool.MPool) error {
	if err := ht.resizeOnDemand(uint64(len(keys)), m); err != nil {
		return err
	}

	BytesBatchGenHashStates(&keys[0], &states[0], len(keys))

	for i := range keys {
		if zValues[i] == 0 {
			continue
		}
		if states[i][0]%nbucket != ibucket {
			values[i] = 0
			continue
		}

		cell := ht.findCell(&states[i])
		if cell.Mapped == 0 {
			ht.elemCnt++
			cell.HashState = states[i]
			cell.Mapped = ht.elemCnt
		}
		values[i] = cell.Mapped
	}
	return nil
}

/*
func (ht *StringHashMap) InsertString24BatchWithRingInBucket(zValues []int64, states [][3]uint64, keys [][3]uint64, values []uint64, ibucket, nbucket uint64) {
	ht.resizeOnDemand(uint64(len(keys)))

	AesInt192BatchGenHashStates(&keys[0], &states[0], len(keys))

	for i := range keys {
		if zValues[i] == 0 {
			continue
		}
		if states[i][0]%nbucket != ibucket {
			continue
		}

		cell := ht.findCell(&states[i])
		if cell.Mapped == 0 {
			ht.elemCnt++
			cell.HashState = states[i]
			cell.Mapped = ht.elemCnt
		}
		values[i] = cell.Mapped
	}
}

func (ht *StringHashMap) InsertString32BatchWithRingInBucket(zValues []int64, states [][3]uint64, keys [][4]uint64, values []uint64, ibucket, nbucket uint64) {
	ht.resizeOnDemand(uint64(len(keys)))

	AesInt256BatchGenHashStates(&keys[0], &states[0], len(keys))

	for i := range keys {
		if zValues[i] == 0 {
			continue
		}
		if states[i][0]%nbucket != ibucket {
			continue
		}

		cell := ht.findCell(&states[i])
		if cell.Mapped == 0 {
			ht.elemCnt++
			cell.HashState = states[i]
			cell.Mapped = ht.elemCnt
		}
		values[i] = cell.Mapped
	}
}

func (ht *StringHashMap) InsertString40BatchWithRingInBucket(zValues []int64, states [][3]uint64, keys [][5]uint64, values []uint64, ibucket, nbucket uint64) {
	ht.resizeOnDemand(uint64(len(keys)))

	AesInt320BatchGenHashStates(&keys[0], &states[0], len(keys))

	for i := range keys {
		if zValues[i] == 0 {
			continue
		}
		if states[i][0]%nbucket != ibucket {
			continue
		}

		cell := ht.findCell(&states[i])
		if cell.Mapped == 0 {
			ht.elemCnt++
			cell.HashState = states[i]
			cell.Mapped = ht.elemCnt
		}
		values[i] = cell.Mapped
	}
}
*/

func (ht *StringHashMap) FindStringBatchInBucket(states [][3]uint64, keys [][]byte, values []uint64, inBuckets []uint8, ibucket, nbucket uint64) {
	BytesBatchGenHashStates(&keys[0], &states[0], len(keys))

	for i := range keys {
		if states[i][0]%nbucket != ibucket {
			inBuckets[i] = 0 // mark of 0 means it is not in the processed bucket
			continue
		}
		cell := ht.findCell(&states[i])
		values[i] = cell.Mapped
	}
}

func (ht *StringHashMap) FindStringBatchWithRingInBucket(states [][3]uint64, zValues []int64, keys [][]byte, values []uint64, inBuckets []uint8, ibucket, nbucket uint64) {
	BytesBatchGenHashStates(&keys[0], &states[0], len(keys))

	for i := range keys {
		if states[i][0]%nbucket != ibucket {
			inBuckets[i] = 0 // mark of 0 means it is not in the processed bucket
			continue
		}
		if zValues[i] == 0 {
			values[i] = 0
			continue
		}
		cell := ht.findCell(&states[i])
		values[i] = cell.Mapped
	}
}

func (ht *Int64HashMap) InsertBatchInBucket(n int, hashes []uint64, keysPtr unsafe.Pointer, values []uint64, ibucket, nbucket uint64, m *mpool.MPool) error {
	if err := ht.resizeOnDemand(n, m); err != nil {
		return err
	}

	if hashes[0] == 0 {
		Int64BatchHash(keysPtr, &hashes[0], n)
	}

	keys := unsafe.Slice((*uint64)(keysPtr), n)

	for i, key := range keys {
		if hashes[i]%nbucket != ibucket {
			values[i] = 0
			continue
		}
		cell := ht.findCell(hashes[i], key)
		if cell.Mapped == 0 {
			ht.elemCnt++
			cell.Key = key
			cell.Mapped = ht.elemCnt
		}
		values[i] = cell.Mapped
	}
	return nil
}

func (ht *Int64HashMap) InsertBatchWithRingInBucket(n int, zValues []int64, hashes []uint64, keysPtr unsafe.Pointer, values []uint64, ibucket, nbucket uint64, m *mpool.MPool) error {
	if err := ht.resizeOnDemand(n, m); err != nil {
		return err
	}

	if hashes[0] == 0 {
		Int64BatchHash(keysPtr, &hashes[0], n)
	}

	keys := unsafe.Slice((*uint64)(keysPtr), n)
	for i, key := range keys {
		if zValues[i] == 0 {
			continue
		}
		if hashes[i]%nbucket != ibucket {
			values[i] = 0
			continue
		}
		cell := ht.findCell(hashes[i], key)
		if cell.Mapped == 0 {
			ht.elemCnt++
			cell.Key = key
			cell.Mapped = ht.elemCnt
		}
		values[i] = cell.Mapped
	}
	return nil
}

func (ht *Int64HashMap) FindBatchInBucket(n int, hashes []uint64, keysPtr unsafe.Pointer, values []uint64, inBuckets []uint8, ibucket, nbucket uint64) {
	if hashes[0] == 0 {
		Int64BatchHash(keysPtr, &hashes[0], n)
	}

	keys := unsafe.Slice((*uint64)(keysPtr), n)
	for i, key := range keys {
		if hashes[i]%nbucket != ibucket {
			inBuckets[i] = 0 // mark of 0 means it is not in the processed bucket
			continue
		}
		cell := ht.findCell(hashes[i], key)
		values[i] = cell.Mapped
	}
}

func (ht *Int64HashMap) FindBatchWithRingInBucket(n int, zValues []int64, hashes []uint64, keysPtr unsafe.Pointer, values []uint64, inBuckets []uint8, ibucket, nbucket uint64) {
	if hashes[0] == 0 {
		Int64BatchHash(keysPtr, &hashes[0], n)
	}

	keys := unsafe.Slice((*uint64)(keysPtr), n)
	for i, key := range keys {
		if hashes[i]%nbucket != ibucket {
			inBuckets[i] = 0 // mark of 0 means it is not in the processed bucket
			continue
		}
		if zValues[i] == 0 {
			values[i] = 0
			continue
		}
		cell := ht.findCell(hashes[i], key)
		values[i] = cell.Mapped
	}
}
