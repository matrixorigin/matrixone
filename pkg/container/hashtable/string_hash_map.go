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
)

type StringRef struct {
	Ptr *byte
	Len int
}

type StringHashMapCell struct {
	Hash   uint64
	AesKey [2]uint64
	Mapped uint64
}

type StringHashMap struct {
	cellCntBits uint8
	cellCnt     uint64
	elemCnt     uint64
	maxElemCnt  uint64
	cells       []StringHashMapCell
}

func (ht *StringHashMap) Init() {
	ht.cellCntBits = kInitialCellCntBits
	ht.cellCnt = kInitialCellCnt
	ht.elemCnt = 0
	ht.maxElemCnt = kInitialCellCnt * kLoadFactorNumerator / kLoadFactorDenominator
	ht.cells = make([]StringHashMapCell, kInitialCellCnt)
}

func (ht *StringHashMap) Insert(key StringRef) uint64 {
	ht.resizeOnDemand()

	var hash uint64
	var aesKey [2]uint64

	if key.Len <= 8 {
		copy(unsafe.Slice((*byte)(unsafe.Pointer(&aesKey[0])), 8), unsafe.Slice(key.Ptr, key.Len))
		hash = Crc32Int64Hash(aesKey[0]) | (uint64(key.Len) << 32)
	} else if key.Len <= 16 {
		copy(unsafe.Slice((*byte)(unsafe.Pointer(&aesKey[0])), 16), unsafe.Slice(key.Ptr, key.Len))
		hash = Crc32BytesHash(unsafe.Pointer(key.Ptr), key.Len)
	} else {
		hash = Crc32BytesHash(unsafe.Pointer(key.Ptr), key.Len)
		aesKey = AesBytesHash(unsafe.Pointer(key.Ptr), key.Len)
	}

	cell := ht.findCell(hash, &aesKey)
	if cell.Mapped == 0 {
		ht.elemCnt++
		cell.Hash = hash
		cell.AesKey = aesKey
		cell.Mapped = ht.elemCnt
	}

	return cell.Mapped
}

func (ht *StringHashMap) Find(key StringRef) uint64 {
	var hash uint64
	var aesKey [2]uint64

	if key.Len <= 8 {
		copy(unsafe.Slice((*byte)(unsafe.Pointer(&aesKey[0])), 8), unsafe.Slice(key.Ptr, key.Len))
		hash = Crc32Int64Hash(aesKey[0]) | (uint64(key.Len) << 32)
	} else if key.Len <= 16 {
		copy(unsafe.Slice((*byte)(unsafe.Pointer(&aesKey[0])), 16), unsafe.Slice(key.Ptr, key.Len))
		hash = Crc32BytesHash(unsafe.Pointer(key.Ptr), key.Len)
	} else {
		hash = Crc32BytesHash(unsafe.Pointer(key.Ptr), key.Len)
		aesKey = AesBytesHash(unsafe.Pointer(key.Ptr), key.Len)
	}

	cell := ht.findCell(hash, &aesKey)

	return cell.Mapped
}

func (ht *StringHashMap) findCell(hash uint64, aesKey *[2]uint64) *StringHashMapCell {
	mask := ht.cellCnt - 1
	for idx := hash & mask; true; idx = (idx + 1) & mask {
		cell := &ht.cells[idx]
		if cell.Mapped == 0 || (cell.Hash == hash && cell.AesKey == *aesKey) {
			return cell
		}
	}

	return nil
}

func (ht *StringHashMap) resizeOnDemand() {
	if ht.elemCnt < ht.maxElemCnt {
		return
	}

	newCellCntBits := ht.cellCntBits + 2
	newCellCnt := uint64(1) << newCellCntBits
	newMaxElemCnt := newCellCnt * kLoadFactorNumerator / kLoadFactorDenominator

	oldCellCnt := ht.cellCnt
	oldCells := ht.cells

	ht.cellCntBits = newCellCntBits
	ht.cellCnt = newCellCnt
	ht.maxElemCnt = newMaxElemCnt
	ht.cells = make([]StringHashMapCell, newCellCnt)

	for i := uint64(0); i < oldCellCnt; i++ {
		cell := &oldCells[i]
		if cell.Mapped != 0 {
			newCell := ht.findCell(cell.Hash, &cell.AesKey)
			*newCell = *cell
		}
	}
}

func (ht *StringHashMap) Cardinality() uint64 {
	return ht.elemCnt
}

type StringHashMapIterator struct {
	table *StringHashMap
	pos   uint64
}

func (it *StringHashMapIterator) Init(ht *StringHashMap) {
	it.table = ht
}

func (it *StringHashMapIterator) Next() (cell *StringHashMapCell, err error) {
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
