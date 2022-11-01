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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
)

type Int64HashMapCell struct {
	Key    uint64
	Mapped uint64
}

type Int64HashMap struct {
	cellCntBits uint8
	cellCnt     uint64
	cellCntMask uint64
	elemCnt     uint64
	maxElemCnt  uint64
	cells       []Int64HashMapCell
	rawData     []byte
	//confCnt     uint64
}

var intCellSize int64

func init() {
	intCellSize = int64(unsafe.Sizeof(Int64HashMapCell{}))
}

func (ht *Int64HashMap) Free(m *mpool.MPool) {
	if len(ht.rawData) > 0 {
		m.Free(unsafe.Slice((*byte)(unsafe.Pointer(&ht.cells[0])), ht.cellCnt*uint64(intCellSize)))
		ht.cells = nil
	}
	ht.rawData = nil
}

func (ht *Int64HashMap) Init(m *mpool.MPool) (err error) {
	ht.cellCntBits = kInitialCellCntBits
	ht.cellCnt = kInitialCellCnt
	ht.cellCntMask = kInitialCellCnt - 1
	ht.elemCnt = 0
	ht.maxElemCnt = kInitialCellCnt * kLoadFactorNumerator / kLoadFactorDenominator

	if ht.rawData, err = m.Alloc(int(ht.cellCnt) * int(intCellSize)); err == nil {
		ht.cells = unsafe.Slice((*Int64HashMapCell)(unsafe.Pointer(&ht.rawData[0])), ht.cellCnt)
	}

	return
}

func (ht *Int64HashMap) InsertBatch(n int, hashes []uint64, keysPtr unsafe.Pointer, values []uint64, m *mpool.MPool) error {
	if err := ht.resizeOnDemand(n, m); err != nil {
		return err
	}

	if hashes[0] == 0 {
		Int64BatchHash(keysPtr, &hashes[0], n)
	}

	keys := unsafe.Slice((*uint64)(keysPtr), n)

	for i, key := range keys {
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

func (ht *Int64HashMap) InsertBatchWithRing(n int, zValues []int64, hashes []uint64, keysPtr unsafe.Pointer, values []uint64, m *mpool.MPool) error {
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

func (ht *Int64HashMap) FindBatch(n int, hashes []uint64, keysPtr unsafe.Pointer, values []uint64) {
	if hashes[0] == 0 {
		Int64BatchHash(keysPtr, &hashes[0], n)
	}

	keys := unsafe.Slice((*uint64)(keysPtr), n)
	for i, key := range keys {
		cell := ht.findCell(hashes[i], key)
		values[i] = cell.Mapped
	}
}

func (ht *Int64HashMap) FindBatchWithRing(n int, zValues []int64, hashes []uint64, keysPtr unsafe.Pointer, values []uint64) {
	if hashes[0] == 0 {
		Int64BatchHash(keysPtr, &hashes[0], n)
	}

	keys := unsafe.Slice((*uint64)(keysPtr), n)
	for i, key := range keys {
		if zValues[i] == 0 {
			values[i] = 0
			continue
		}
		cell := ht.findCell(hashes[i], key)
		values[i] = cell.Mapped
	}
}

func (ht *Int64HashMap) findCell(hash uint64, key uint64) *Int64HashMapCell {
	for idx := hash & ht.cellCntMask; true; idx = (idx + 1) & ht.cellCntMask {
		cell := &ht.cells[idx]
		if cell.Key == key || cell.Mapped == 0 {
			return cell
		}
		//ht.confCnt++
	}

	return nil
}

func (ht *Int64HashMap) findEmptyCell(hash uint64, key uint64) *Int64HashMapCell {
	for idx := hash & ht.cellCntMask; true; idx = (idx + 1) & ht.cellCntMask {
		cell := &ht.cells[idx]
		if cell.Mapped == 0 {
			return cell
		}
		//ht.confCnt++
	}

	return nil
}

func (ht *Int64HashMap) resizeOnDemand(n int, m *mpool.MPool) error {
	targetCnt := ht.elemCnt + uint64(n)
	if targetCnt <= ht.maxElemCnt {
		return nil
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
	oldData := ht.rawData

	ht.cellCntBits = newCellCntBits
	ht.cellCnt = newCellCnt
	ht.cellCntMask = newCellCnt - 1
	ht.maxElemCnt = newMaxElemCnt
	if newData, err := m.Alloc(int(newCellCnt) * int(intCellSize)); err != nil {
		return err
	} else {
		ht.rawData = newData
		ht.cells = unsafe.Slice((*Int64HashMapCell)(unsafe.Pointer(&newData[0])), newCellCnt)
	}

	var hashes [256]uint64

	var i uint64
	for i = 0; i < oldCellCnt; i += 256 {
		cells := oldCells[i : i+256]
		Int64CellBatchHash(unsafe.Pointer(&cells[0]), &hashes[0], 256)
		for j := range cells {
			cell := &cells[j]
			if cell.Mapped != 0 {
				newCell := ht.findEmptyCell(hashes[j], cell.Key)
				*newCell = *cell
			}
		}
	}

	m.Free(oldData)

	return nil
}

func (ht *Int64HashMap) Cardinality() uint64 {
	return ht.elemCnt
}

type Int64HashMapIterator struct {
	table *Int64HashMap
	pos   uint64
}

func (it *Int64HashMapIterator) Init(ht *Int64HashMap) {
	it.table = ht
}

func (it *Int64HashMapIterator) Next() (cell *Int64HashMapCell, err error) {
	for it.pos < it.table.cellCnt {
		cell = &it.table.cells[it.pos]
		if cell.Mapped != 0 {
			break
		}
		it.pos++
	}

	if it.pos >= it.table.cellCnt {
		err = moerr.NewInternalError("out of range")
		return
	}

	it.pos++

	return
}
