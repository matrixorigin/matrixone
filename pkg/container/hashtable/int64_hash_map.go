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
	blockCellCntBits uint8
	blockCellCnt     uint64
	blockMaxElemCnt  uint64
	cellCntMask      uint64
	//confCnt     uint64

	cellCnt uint64
	elemCnt uint64
	rawData [][]byte
	cells   [][]Int64HashMapCell
}

var intCellSize int64

func init() {
	intCellSize = int64(unsafe.Sizeof(Int64HashMapCell{}))
}

func (ht *Int64HashMap) Free(m *mpool.MPool) {
	for i := range ht.rawData {
		if len(ht.rawData[i]) > 0 {
			m.Free(ht.rawData[i])
		}
		ht.rawData[i], ht.cells[i] = nil, nil
	}
	ht.rawData, ht.cells = nil, nil
}

func (ht *Int64HashMap) Init(m *mpool.MPool) (err error) {
	ht.blockCellCntBits = kInitialCellCntBits
	ht.blockCellCnt = kInitialCellCnt
	ht.blockMaxElemCnt = kInitialCellCnt * kLoadFactorNumerator / kLoadFactorDenominator
	ht.cellCntMask = kInitialCellCnt - 1
	ht.elemCnt = 0
	ht.cellCnt = kInitialCellCnt

	ht.rawData = make([][]byte, 1)
	ht.cells = make([][]Int64HashMapCell, 1)
	if ht.rawData[0], err = m.Alloc(int(ht.blockCellCnt) * int(intCellSize)); err == nil {
		ht.cells[0] = unsafe.Slice((*Int64HashMapCell)(unsafe.Pointer(&ht.rawData[0][0])), ht.blockCellCnt)
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

	for i, hash := range hashes {
		cell := ht.findCell(hash)
		if cell.Mapped == 0 {
			ht.elemCnt++
			cell.Key = hash
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

	for i, hash := range hashes {
		if zValues[i] == 0 {
			continue
		}
		cell := ht.findCell(hash)
		if cell.Mapped == 0 {
			ht.elemCnt++
			cell.Key = hash
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

	for i, hash := range hashes {
		cell := ht.findCell(hash)
		values[i] = cell.Mapped
	}
}

func (ht *Int64HashMap) FindBatchWithRing(n int, zValues []int64, hashes []uint64, keysPtr unsafe.Pointer, values []uint64) {
	if hashes[0] == 0 {
		Int64BatchHash(keysPtr, &hashes[0], n)
	}

	for i, hash := range hashes {
		if zValues[i] == 0 {
			values[i] = 0
			continue
		}
		cell := ht.findCell(hash)
		values[i] = cell.Mapped
	}
}

func (ht *Int64HashMap) findCell(hash uint64) *Int64HashMapCell {
	for idx := hash & ht.cellCntMask; true; idx = (idx + 1) & ht.cellCntMask {
		blockId := idx / ht.blockCellCnt
		cellId := idx % ht.blockCellCnt
		cell := &ht.cells[blockId][cellId]
		if cell.Key == hash || cell.Mapped == 0 {
			return cell
		}
	}
	return nil
}

func (ht *Int64HashMap) findEmptyCell(hash uint64) *Int64HashMapCell {
	for idx := hash & ht.cellCntMask; true; idx = (idx + 1) & ht.cellCntMask {
		blockId := idx / ht.blockCellCnt
		cellId := idx % ht.blockCellCnt
		cell := &ht.cells[blockId][cellId]
		if cell.Mapped == 0 {
			return cell
		}
	}
	return nil
}

func (ht *Int64HashMap) resizeOnDemand(n int, m *mpool.MPool) error {
	targetCnt := ht.elemCnt + uint64(n)
	if targetCnt <= uint64(len(ht.rawData))*ht.blockMaxElemCnt {
		return nil
	}

	var err error
	if len(ht.rawData) == 1 {
		newCellCntBits := ht.blockCellCntBits + 2
		newCellCnt := uint64(1 << newCellCntBits)
		newBlockMaxElemCnt := newCellCnt * kLoadFactorNumerator / kLoadFactorDenominator
		for newBlockMaxElemCnt < targetCnt {
			newCellCntBits++
			newCellCnt <<= 1
			newBlockMaxElemCnt = newCellCnt * kLoadFactorNumerator / kLoadFactorDenominator
		}

		newAlloc := int(newCellCnt) * int(intCellSize)
		if newAlloc <= mpool.GB {
			// update hashTable cnt.
			oldCellCnt := ht.blockCellCnt
			oldCells0 := ht.cells[0]
			oldData0 := ht.rawData[0]

			ht.blockCellCntBits = newCellCntBits
			ht.blockCellCnt = newCellCnt
			ht.blockMaxElemCnt = newBlockMaxElemCnt
			ht.cellCnt = newCellCnt
			ht.cellCntMask = newCellCnt - 1

			ht.rawData[0], err = m.Alloc(newAlloc)
			if err != nil {
				return err
			}
			ht.cells[0] = unsafe.Slice((*Int64HashMapCell)(unsafe.Pointer(&ht.rawData[0][0])), ht.blockCellCnt)

			// rearrange the cells
			for i := uint64(0); i < oldCellCnt; i += 256 {
				cells := oldCells0[i : i+256]
				for j := range cells {
					cell := &cells[j]
					if cell.Mapped != 0 {
						newCell := ht.findEmptyCell(cell.Key)
						*newCell = *cell
					}
				}
			}

			m.Free(oldData0)
			return nil
		}
	}

	// double the blocks
	oldBlockNum := len(ht.rawData)
	blockNum := oldBlockNum * 2

	ht.rawData = append(ht.rawData, make([][]byte, oldBlockNum)...)
	ht.cells = append(ht.cells, make([][]Int64HashMapCell, oldBlockNum)...)
	ht.cellCnt = ht.blockCellCnt * uint64(blockNum)
	ht.cellCntMask = ht.cellCnt - 1

	for i := oldBlockNum; i < blockNum; i++ {
		ht.rawData[i], err = m.Alloc(int(ht.blockCellCnt) * int(intCellSize))
		if err != nil {
			return err
		}
		ht.cells[i] = unsafe.Slice((*Int64HashMapCell)(unsafe.Pointer(&ht.rawData[i][0])), ht.blockCellCnt)
	}

	// rearrange the cells
	var block []Int64HashMapCell
	var emptyCell Int64HashMapCell

	for i := 0; i < oldBlockNum; i++ {
		block = ht.cells[i]
		for j := uint64(0); j < ht.blockCellCnt; j++ {
			cell := &block[j]
			if cell.Mapped == 0 {
				continue
			}
			newCell := ht.findCell(cell.Key)
			if newCell != cell {
				*newCell = *cell
				*cell = emptyCell
			}
		}
	}

	block = ht.cells[oldBlockNum]
	for j := uint64(0); j < ht.blockCellCnt; j++ {
		cell := &block[j]
		if cell.Mapped == 0 {
			break
		}
		newCell := ht.findCell(cell.Key)
		if newCell != cell {
			*newCell = *cell
			*cell = emptyCell
		}
	}

	return nil
}

func (ht *Int64HashMap) Cardinality() uint64 {
	return ht.elemCnt
}

func (ht *Int64HashMap) Size() int64 {
	// 41 is the fixed size of Int64HashMap
	ret := int64(41)
	for i := range ht.rawData {
		ret += int64(len(ht.rawData[i]))
		// 16 is the len of ht.cells[i]
		ret += 16
	}
	return ret
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
		blockId := it.pos / it.table.blockCellCnt
		cellId := it.pos % it.table.blockCellCnt
		cell = &it.table.cells[blockId][cellId]
		if cell.Mapped != 0 {
			break
		}
		it.pos++
	}

	if it.pos >= it.table.cellCnt {
		err = moerr.NewInternalErrorNoCtx("out of range")
		return
	}

	it.pos++

	return
}
