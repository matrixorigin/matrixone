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
)

type String40HashMapCell struct {
	Hash   uint64
	AesKey [2]uint64
	Mapped uint64
}

type String40HashMap struct {
	cellCntBits uint8
	cellCnt     uint64
	elemCnt     uint64
	maxElemCnt  uint64
	cells       []String40HashMapCell
	//confCnt     uint64
}

func (ht *String40HashMap) Init() {
	ht.cellCntBits = kInitialCellCntBits
	ht.cellCnt = kInitialCellCnt
	ht.elemCnt = 0
	ht.maxElemCnt = kInitialCellCnt * kLoadFactorNumerator / kLoadFactorDenominator
	ht.cells = make([]String40HashMapCell, kInitialCellCnt)
}

func (ht *String40HashMap) InsertRawBatch(hashes []uint64, aesKeys [][2]uint64, keys [][5]uint64, values []uint64) {
	ht.resizeOnDemand(uint64(len(keys)))

	AesInt320BatchHash(&keys[0], &hashes[0], len(keys))
	AesInt320BatchGenKey(&keys[0], &aesKeys[0], len(keys))

	for i := range keys {
		cell := ht.findCell(hashes[i], &aesKeys[i])
		if cell.Mapped == 0 {
			ht.elemCnt++
			cell.Hash = hashes[i]
			cell.AesKey = aesKeys[i]
			cell.Mapped = ht.elemCnt
		}
		values[i] = cell.Mapped
	}
}

func (ht *String40HashMap) InsertHashedBatch(hashes []uint64, aesKeys [][2]uint64, values []uint64) {
	ht.resizeOnDemand(uint64(len(aesKeys)))

	for i := range aesKeys {
		cell := ht.findCell(hashes[i], &aesKeys[i])
		if cell.Mapped == 0 {
			ht.elemCnt++
			cell.Hash = hashes[i]
			cell.AesKey = aesKeys[i]
			cell.Mapped = ht.elemCnt
		}
		values[i] = cell.Mapped
	}
}

func (ht *String40HashMap) InsertRawBatchWithRing(zValues []int64, hashes []uint64, aesKeys [][2]uint64, keys [][5]uint64, values []uint64) {
	ht.resizeOnDemand(uint64(len(keys)))

	AesInt320BatchHash(&keys[0], &hashes[0], len(keys))
	AesInt320BatchGenKey(&keys[0], &aesKeys[0], len(keys))

	for i := range keys {
		if zValues[i] == 0 {
			continue
		}

		cell := ht.findCell(hashes[i], &aesKeys[i])
		if cell.Mapped == 0 {
			ht.elemCnt++
			cell.Hash = hashes[i]
			cell.AesKey = aesKeys[i]
			cell.Mapped = ht.elemCnt
		}
		values[i] = cell.Mapped
	}
}

func (ht *String40HashMap) InsertHashedBatchWithRing(zValues []int64, hashes []uint64, aesKeys [][2]uint64, values []uint64) {
	ht.resizeOnDemand(uint64(len(aesKeys)))

	for i := range aesKeys {
		if zValues[i] == 0 {
			continue
		}

		cell := ht.findCell(hashes[i], &aesKeys[i])
		if cell.Mapped == 0 {
			ht.elemCnt++
			cell.Hash = hashes[i]
			cell.AesKey = aesKeys[i]
			cell.Mapped = ht.elemCnt
		}
		values[i] = cell.Mapped
	}
}

func (ht *String40HashMap) FindRawBatch(hashes []uint64, aesKeys [][2]uint64, keys [][5]uint64, values []uint64) {
	AesInt320BatchHash(&keys[0], &hashes[0], len(keys))
	AesInt320BatchGenKey(&keys[0], &aesKeys[0], len(keys))

	for i := range keys {
		cell := ht.findCell(hashes[i], &aesKeys[i])
		values[i] = cell.Mapped
	}
}

func (ht *String40HashMap) FindHashedBatch(hashes []uint64, aesKeys [][2]uint64, values []uint64) {
	for i := range aesKeys {
		cell := ht.findCell(hashes[i], &aesKeys[i])
		values[i] = cell.Mapped
	}
}

func (ht *String40HashMap) findCell(hash uint64, aesKey *[2]uint64) *String40HashMapCell {
	mask := ht.cellCnt - 1
	for idx := hash & mask; true; idx = (idx + 1) & mask {
		cell := &ht.cells[idx]
		if cell.Mapped == 0 || (cell.Hash == hash && cell.AesKey == *aesKey) {
			return cell
		}
		//ht.confCnt++
	}

	return nil
}

func (ht *String40HashMap) resizeOnDemand(n uint64) {
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
	ht.cells = make([]String40HashMapCell, newCellCnt)

	for i := uint64(0); i < oldCellCnt; i++ {
		cell := &oldCells[i]
		if cell.Mapped != 0 {
			newCell := ht.findCell(cell.Hash, &cell.AesKey)
			*newCell = *cell
		}
	}
}

func (ht *String40HashMap) Cardinality() uint64 {
	return ht.elemCnt
}

type String40HashMapIterator struct {
	table *String40HashMap
	pos   uint64
}

func (it *String40HashMapIterator) Init(ht *String40HashMap) {
	it.table = ht
}

func (it *String40HashMapIterator) Next() (cell *String40HashMapCell, err error) {
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
