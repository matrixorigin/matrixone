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

type String24HashMapCell struct {
	Hash   uint64
	Key    [3]uint64
	Mapped uint64
}

func (hdr *String24HashMapCell) StrKey() StringRef {
	return StringRef{
		Ptr: (*byte)(unsafe.Pointer(&hdr.Key[0])),
		Len: 24 - (bits.LeadingZeros64(hdr.Key[2]) >> 3),
	}
}

type String24HashMap struct {
	cellCntBits uint8
	cellCnt     uint64
	elemCnt     uint64
	maxElemCnt  uint64
	cells       []String24HashMapCell
	//confCnt     uint64
}

func (ht *String24HashMap) Init() {
	ht.cellCntBits = kInitialCellCntBits
	ht.cellCnt = kInitialCellCnt
	ht.elemCnt = 0
	ht.maxElemCnt = kInitialCellCnt * kLoadFactorNumerator / kLoadFactorDenominator
	ht.cells = make([]String24HashMapCell, kInitialCellCnt)
}

func (ht *String24HashMap) InsertBatch(hashes []uint64, keys [][3]uint64, values []uint64) {
	ht.resizeOnDemand(uint64(len(keys)))

	if hashes[0] == 0 {
		AesInt192BatchHash(&keys[0], &hashes[0], len(keys))
	}

	for i := range keys {
		cell := ht.findCell(hashes[i], &keys[i])
		if cell.Mapped == 0 {
			ht.elemCnt++
			cell.Hash = hashes[i]
			cell.Key = keys[i]
			cell.Mapped = ht.elemCnt
		}
		values[i] = cell.Mapped
	}
}

func (ht *String24HashMap) InsertBatchWithRing(zValues []int64, hashes []uint64, keys [][3]uint64, values []uint64) {
	ht.resizeOnDemand(uint64(len(keys)))

	if hashes[0] == 0 {
		AesInt192BatchHash(&keys[0], &hashes[0], len(keys))
	}

	for i := range keys {
		if zValues[i] == 0 {
			continue
		}
		cell := ht.findCell(hashes[i], &keys[i])
		if cell.Mapped == 0 {
			ht.elemCnt++
			cell.Hash = hashes[i]
			cell.Key = keys[i]
			cell.Mapped = ht.elemCnt
		}
		values[i] = cell.Mapped
	}
}

func (ht *String24HashMap) FindBatch(hashes []uint64, keys [][3]uint64, values []uint64) {
	if hashes[0] == 0 {
		AesInt192BatchHash(&keys[0], &hashes[0], len(keys))
	}

	for i := range keys {
		cell := ht.findCell(hashes[i], &keys[i])
		values[i] = cell.Mapped
	}
}

func (ht *String24HashMap) findCell(hash uint64, key *[3]uint64) *String24HashMapCell {
	mask := ht.cellCnt - 1
	for idx := hash & mask; true; idx = (idx + 1) & mask {
		cell := &ht.cells[idx]
		if cell.Mapped == 0 || cell.Key == *key {
			return cell
		}
		//ht.confCnt++
	}

	return nil
}

func (ht *String24HashMap) resizeOnDemand(n uint64) {
	targetCnt := ht.elemCnt + n
	if targetCnt <= ht.maxElemCnt {
		return
	}

	newCellCntBits := ht.cellCntBits + 2
	newCellCnt := uint64(1) << newCellCntBits
	newMaxElemCnt := newCellCnt * kLoadFactorNumerator / kLoadFactorDenominator
	for newMaxElemCnt < targetCnt {
		newCellCntBits++
		newCellCnt <<= 1
		newMaxElemCnt = newCellCnt * kLoadFactorNumerator / kLoadFactorDenominator
	}

	oldCellCnt := ht.cellCnt
	oldCells := ht.cells

	ht.cellCntBits = newCellCntBits
	ht.cellCnt = newCellCnt
	ht.maxElemCnt = newMaxElemCnt
	ht.cells = make([]String24HashMapCell, newCellCnt)

	for i := uint64(0); i < oldCellCnt; i++ {
		cell := &oldCells[i]
		if cell.Mapped != 0 {
			newCell := ht.findCell(cell.Hash, &cell.Key)
			*newCell = *cell
		}
	}
}

func (ht *String24HashMap) Cardinality() uint64 {
	return ht.elemCnt
}

type String24HashMapIterator struct {
	table *String24HashMap
	pos   uint64
}

func (it *String24HashMapIterator) Init(ht *String24HashMap) {
	it.table = ht
}

func (it *String24HashMapIterator) Next() (cell *String24HashMapCell, err error) {
	for it.pos < it.table.cellCnt {
		cell = &it.table.cells[it.pos]
		if cell.Mapped != 0 {
			break
		}
		it.pos++
	}

	if it.pos >= it.table.cellCnt {
		err = errors.New("out of range")
		return
	}

	it.pos++

	return
}
