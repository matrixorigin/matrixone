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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"unsafe"
)

func (ht *Int64HashMap) Free2(m *mpool.MPool) {
	for i := range ht.rawData2 {
		m.Free(ht.rawData2[i])
		ht.rawData2[i], ht.cells2[i] = nil, nil
	}
	ht.rawData2, ht.cells2 = nil, nil
}

func (ht *Int64HashMap) Init2(m *mpool.MPool) (err error) {
	ht.cellCntBits = kInitialCellCntBits
	ht.blockMaxCellCnt = kInitialCellCnt
	ht.cellCntMask = kInitialCellCnt - 1
	ht.allElemCnt = 0
	ht.blockMaxElemCnt = kInitialCellCnt * kLoadFactorNumerator / kLoadFactorDenominator

	ht.rawData2 = make([][]byte, 1)
	ht.cells2 = make([][]Int64HashMapCell, 1)
	if ht.rawData2[0], err = m.Alloc(int(ht.blockMaxCellCnt) * int(intCellSize)); err == nil {
		ht.cells2[0] = unsafe.Slice((*Int64HashMapCell)(unsafe.Pointer(&ht.rawData2[0])), ht.cellCnt)
	}
	return
}

func (ht *Int64HashMap) InsertBatch2(n int, hashes []uint64, keysPtr unsafe.Pointer, values []uint64, m *mpool.MPool) error {
	if err := ht.resizeOnDemand2(n, m); err != nil {
		return err
	}

	if hashes[0] == 0 {
		Int64BatchHash(keysPtr, &hashes[0], n)
	}

	keys := unsafe.Slice((*uint64)(keysPtr), n)
	for i, key := range keys {
		cell := ht.findCell2(hashes[i], key)
		if cell.Mapped == 0 {
			ht.allElemCnt++
			cell.Key = key
			cell.Mapped = ht.allElemCnt
		}
		values[i] = cell.Mapped
	}
	return nil
}

func (ht *Int64HashMap) InsertBatchWithRing2(n int, zValues []int64, hashes []uint64, keysPtr unsafe.Pointer, values []uint64, m *mpool.MPool) error {
	if err := ht.resizeOnDemand2(n, m); err != nil {
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
		cell := ht.findCell2(hashes[i], key)
		if cell.Mapped == 0 {
			ht.allElemCnt++
			cell.Key = key
			cell.Mapped = ht.allElemCnt
		}
		values[i] = cell.Mapped
	}
	return nil
}

func (ht *Int64HashMap) FindBatch2(n int, hashes []uint64, keysPtr unsafe.Pointer, values []uint64) {
	if hashes[0] == 0 {
		Int64BatchHash(keysPtr, &hashes[0], n)
	}

	keys := unsafe.Slice((*uint64)(keysPtr), n)
	for i, key := range keys {
		cell := ht.findCell2(hashes[i], key)
		values[i] = cell.Mapped
	}
}

func (ht *Int64HashMap) FindBatchWithRing2(n int, zValues []int64, hashes []uint64, keysPtr unsafe.Pointer, values []uint64) {
	if hashes[0] == 0 {
		Int64BatchHash(keysPtr, &hashes[0], n)
	}

	keys := unsafe.Slice((*uint64)(keysPtr), n)
	for i, key := range keys {
		if zValues[i] == 0 {
			values[i] = 0
			continue
		}
		cell := ht.findCell2(hashes[i], key)
		values[i] = cell.Mapped
	}
}

func (ht *Int64HashMap) findCell2(hash uint64, key uint64) *Int64HashMapCell {
	for idx := hash & ht.cellCntMask; true; idx = (idx + 1) & ht.cellCntMask {
		blockId := idx / ht.blockMaxCellCnt
		cellId := idx % ht.blockMaxCellCnt
		cell := &ht.cells2[blockId][cellId]
		if cell.Key == key || cell.Mapped == 0 {
			return cell
		}
	}
	return nil
}

func (ht *Int64HashMap) findEmptyCell2(hash uint64, key uint64) *Int64HashMapCell {
	for idx := hash & ht.cellCntMask; true; idx = (idx + 1) & ht.cellCntMask {
		blockId := idx / ht.blockMaxCellCnt
		cellId := idx % ht.blockMaxCellCnt
		cell := &ht.cells2[blockId][cellId]
		if cell.Mapped == 0 {
			return cell
		}
	}
	return nil
}

func (ht *Int64HashMap) resizeOnDemand2(n int, m *mpool.MPool) error {
	targetCnt := ht.allElemCnt + uint64(n)
	if targetCnt < uint64(len(ht.rawData2))*ht.blockMaxElemCnt {
		return nil
	}

	var err error
	if len(ht.rawData2) == 1 {
		newCellCntBits := ht.cellCntBits + 2
		newCellCnt := uint64(1 << newCellCntBits)
		newBlockMaxElemCnt := newCellCnt * kLoadFactorNumerator / kLoadFactorDenominator
		for newBlockMaxElemCnt < targetCnt {
			newCellCntBits++
			newCellCnt <<= 1
			newBlockMaxElemCnt = newCellCnt * kLoadFactorNumerator / kLoadFactorDenominator
		}

		oldCellCnt := ht.blockMaxCellCnt
		oldCells0 := ht.cells2[0]
		oldData0 := ht.rawData2[0]

		newAlloc := int(newCellCnt) * int(intCellSize)
		if newAlloc < mpool.GB {
			// update hashTable cnt.
			ht.cellCntBits = newCellCntBits
			ht.blockMaxCellCnt = newCellCnt
			ht.cellCntMask = newCellCnt - 1
			ht.blockMaxElemCnt = newBlockMaxElemCnt

			ht.rawData2[0], err = m.Alloc(newAlloc)
			if err != nil {
				return err
			}
			ht.cells2[0] = unsafe.Slice((*Int64HashMapCell)(unsafe.Pointer(&ht.rawData2[0][0])), ht.cellCnt)

			// re the cell
			var hashes [256]uint64
			for i := uint64(0); i < oldCellCnt; i += 256 {
				cells := oldCells0[i : i+256]
				Int64CellBatchHash(unsafe.Pointer(&cells[0]), &hashes[0], 256)
				for j := range cells {
					cell := &cells[j]
					if cell.Mapped != 0 {
						newCell := ht.findEmptyCell2(hashes[j], cell.Key)
						*newCell = *cell
					}
				}
			}

			m.Free(oldData0)
			return nil
		}
	}

	// double the block
	blockNum := len(ht.rawData2)
	for i := range ht.rawData2 {
		newBlockIdx := blockNum + i
		ht.rawData2 = append(ht.rawData2, nil)
		ht.rawData2[newBlockIdx], err = m.Alloc(int(ht.blockMaxCellCnt) * int(intCellSize))
		if err != nil {
			return err
		}
		ht.cells2[newBlockIdx] = unsafe.Slice((*Int64HashMapCell)(unsafe.Pointer(&ht.rawData2[newBlockIdx][0])), ht.blockMaxCellCnt)
	}
	return nil
}

func (ht *Int64HashMap) Cardinality2() uint64 {
	return ht.allElemCnt
}

func (it *Int64HashMapIterator) Next2() (cell *Int64HashMapCell, err error) {
	for it.pos < it.table.cellCnt {
		blockId := it.pos / it.table.blockMaxCellCnt
		cellId := it.pos % it.table.blockMaxCellCnt
		cell = &it.table.cells2[blockId][cellId]
		if cell.Mapped != 0 {
			break
		}
		it.pos++
	}

	if it.pos >= it.table.allCellCnt {
		err = moerr.NewInternalError("out of range")
		return
	}

	it.pos++

	return
}
