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

func (ht *StringHashMap) Free(m *mpool.MPool) {
	for i := range ht.rawData2 {
		if len(ht.rawData2[i]) > 0 {
			m.Free(ht.rawData2[i])
		}
		ht.rawData2[i], ht.cells2[i] = nil, nil
	}
	ht.rawData2, ht.cells2 = nil, nil
}

func (ht *StringHashMap) Init(m *mpool.MPool) (err error) {
	ht.blockCellCntBits = kInitialCellCntBits
	ht.blockMaxCellCnt = kInitialCellCnt
	ht.blockMaxElemCnt = kInitialCellCnt * kLoadFactorNumerator / kLoadFactorDenominator
	ht.elemCnt = 0
	ht.allCellCnt = kInitialCellCnt

	ht.rawData2 = make([][]byte, 1)
	ht.cells2 = make([][]StringHashMapCell, 1)
	if ht.rawData2[0], err = m.Alloc(int(ht.blockMaxCellCnt) * int(strCellSize)); err == nil {
		ht.cells2[0] = unsafe.Slice((*StringHashMapCell)(unsafe.Pointer(&ht.rawData2[0][0])), ht.blockMaxCellCnt)
	}
	return
}

func (ht *StringHashMap) InsertStringBatch(states [][3]uint64, keys [][]byte, values []uint64, m *mpool.MPool) error {
	if err := ht.resizeOnDemand(uint64(len(keys)), m); err != nil {
		return err
	}

	BytesBatchGenHashStates(&keys[0], &states[0], len(keys))

	for i := range keys {
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

func (ht *StringHashMap) InsertStringBatchWithRing(zValues []int64, states [][3]uint64, keys [][]byte, values []uint64, m *mpool.MPool) error {
	if err := ht.resizeOnDemand(uint64(len(keys)), m); err != nil {
		return err
	}

	BytesBatchGenHashStates(&keys[0], &states[0], len(keys))

	for i := range keys {
		if zValues[i] == 0 {
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

func (ht *StringHashMap) FindStringBatch(states [][3]uint64, keys [][]byte, values []uint64) {
	BytesBatchGenHashStates(&keys[0], &states[0], len(keys))

	for i := range keys {
		cell := ht.findCell(&states[i])
		values[i] = cell.Mapped
	}
}

func (ht *StringHashMap) FindString24Batch(states [][3]uint64, keys [][3]uint64, values []uint64) {
	Int192BatchGenHashStates(&keys[0], &states[0], len(keys))

	for i := range keys {
		cell := ht.findCell(&states[i])
		values[i] = cell.Mapped
	}
}

func (ht *StringHashMap) FindString32Batch(states [][3]uint64, keys [][4]uint64, values []uint64) {
	Int256BatchGenHashStates(&keys[0], &states[0], len(keys))

	for i := range keys {
		cell := ht.findCell(&states[i])
		values[i] = cell.Mapped
	}
}

func (ht *StringHashMap) FindString40Batch(states [][3]uint64, keys [][5]uint64, values []uint64) {
	Int320BatchGenHashStates(&keys[0], &states[0], len(keys))

	for i := range keys {
		cell := ht.findCell(&states[i])
		values[i] = cell.Mapped
	}
}

func (ht *StringHashMap) FindStringBatchWithRing(states [][3]uint64, zValues []int64, keys [][]byte, values []uint64) {
	// XXX I think it is no use now.
}

func (ht *StringHashMap) FindHashStateBatch(states [][3]uint64, values []uint64) {
	for i := range states {
		cell := ht.findCell(&states[i])
		values[i] = cell.Mapped
	}
}

func (ht *StringHashMap) findCell(state *[3]uint64) *StringHashMapCell {
	mask := ht.allCellCnt - 1
	if len(ht.rawData2) == 1 {
		for idx := state[0] & mask; true; idx = (idx + 1) & mask {
			cell := &ht.cells2[0][idx]
			if cell.Mapped == 0 || cell.HashState == *state {
				return cell
			}
		}
	} else {
		for idx := state[0] & mask; true; idx = (idx + 1) & mask {
			blockId := idx / ht.blockMaxCellCnt
			cellId := idx % ht.blockMaxCellCnt
			cell := &ht.cells2[blockId][cellId]
			if cell.Mapped == 0 || cell.HashState == *state {
				return cell
			}
		}
	}
	return nil
}

func (ht *StringHashMap) findEmptyCell(state *[3]uint64) *StringHashMapCell {
	mask := ht.allCellCnt - 1
	if len(ht.rawData2) == 1 {
		for idx := state[0] & mask; true; idx = (idx + 1) & mask {
			cell := &ht.cells2[0][idx]
			if cell.Mapped == 0 {
				return cell
			}
		}
	} else {
		for idx := state[0] & mask; true; idx = (idx + 1) & mask {
			blockId := idx / ht.blockMaxCellCnt
			cellId := idx % ht.blockMaxCellCnt
			cell := &ht.cells2[blockId][cellId]
			if cell.Mapped == 0 {
				return cell
			}
		}
	}
	return nil
}

func (ht *StringHashMap) resizeOnDemand(n uint64, m *mpool.MPool) error {
	targetCnt := ht.elemCnt + n
	if targetCnt <= uint64(len(ht.rawData2))*ht.blockMaxElemCnt {
		return nil
	}

	var err error
	if len(ht.rawData2) == 1 {
		newCellCntBits := ht.blockCellCntBits + 2
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

		newAlloc := int(newCellCnt) * int(strCellSize)
		if newAlloc < mpool.GB {
			// update hashTable cnt.
			ht.blockCellCntBits = newCellCntBits
			ht.allCellCnt = newCellCnt
			ht.blockMaxCellCnt = newCellCnt
			ht.blockMaxElemCnt = newBlockMaxElemCnt

			ht.rawData2[0], err = m.Alloc(newAlloc)
			if err != nil {
				return err
			}
			ht.cells2[0] = unsafe.Slice((*StringHashMapCell)(unsafe.Pointer(&ht.rawData2[0][0])), ht.blockMaxCellCnt)

			// re the cell
			for i := uint64(0); i < oldCellCnt; i++ {
				cell := &oldCells0[i]
				if cell.Mapped != 0 {
					newCell := ht.findEmptyCell(&cell.HashState)
					*newCell = *cell
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
		ht.rawData2[newBlockIdx], err = m.Alloc(int(ht.blockMaxCellCnt) * int(strCellSize))
		if err != nil {
			return err
		}
		ht.cells2[newBlockIdx] = unsafe.Slice((*StringHashMapCell)(unsafe.Pointer(&ht.rawData2[newBlockIdx][0])), ht.blockMaxCellCnt)
	}
	ht.allCellCnt = ht.blockMaxCellCnt * uint64(len(ht.rawData2))
	return nil
}

func (ht *StringHashMap) Cardinality() uint64 {
	return ht.elemCnt
}

func (it *StringHashMapIterator) Next() (cell *StringHashMapCell, err error) {
	for it.pos < it.table.allCellCnt {
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
