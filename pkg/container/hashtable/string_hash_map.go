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

type StringRef struct {
	Ptr *byte
	Len int
}

type StringHashMapCell struct {
	HashState [3]uint64
	Mapped    uint64
}

var StrKeyPadding [16]byte

type StringHashMap struct {
	blockCellCntBits uint8
	blockMaxCellCnt  uint64
	blockMaxElemCnt  uint64
	//confCnt     uint64

	cellCnt uint64
	elemCnt uint64
	rawData [][]byte
	cells   [][]StringHashMapCell
}

var strCellSize int64

func init() {
	strCellSize = int64(unsafe.Sizeof(StringHashMapCell{}))
}

func (ht *StringHashMap) Free(m *mpool.MPool) {
	for i := range ht.rawData {
		if len(ht.rawData[i]) > 0 {
			m.Free(ht.rawData[i])
		}
		ht.rawData[i], ht.cells[i] = nil, nil
	}
	ht.rawData, ht.cells = nil, nil
}

func (ht *StringHashMap) Init(m *mpool.MPool) (err error) {
	ht.blockCellCntBits = kInitialCellCntBits
	ht.blockMaxCellCnt = kInitialCellCnt
	ht.blockMaxElemCnt = kInitialCellCnt * kLoadFactorNumerator / kLoadFactorDenominator
	ht.elemCnt = 0
	ht.cellCnt = kInitialCellCnt

	ht.rawData = make([][]byte, 1)
	ht.cells = make([][]StringHashMapCell, 1)
	if ht.rawData[0], err = m.Alloc(int(ht.blockMaxCellCnt) * int(strCellSize)); err == nil {
		ht.cells[0] = unsafe.Slice((*StringHashMapCell)(unsafe.Pointer(&ht.rawData[0][0])), ht.blockMaxCellCnt)
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
	mask := ht.cellCnt - 1
	for idx := state[0] & mask; true; idx = (idx + 1) & mask {
		blockId := idx / ht.blockMaxCellCnt
		cellId := idx % ht.blockMaxCellCnt
		cell := &ht.cells[blockId][cellId]
		if cell.Mapped == 0 || cell.HashState == *state {
			return cell
		}
	}
	return nil
}

func (ht *StringHashMap) findEmptyCell(state *[3]uint64) *StringHashMapCell {
	mask := ht.cellCnt - 1
	for idx := state[0] & mask; true; idx = (idx + 1) & mask {
		blockId := idx / ht.blockMaxCellCnt
		cellId := idx % ht.blockMaxCellCnt
		cell := &ht.cells[blockId][cellId]
		if cell.Mapped == 0 {
			return cell
		}
	}
	return nil
}

func (ht *StringHashMap) resizeOnDemand(n uint64, m *mpool.MPool) error {
	targetCnt := ht.elemCnt + n
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

		oldCellCnt := ht.blockMaxCellCnt
		oldCells0 := ht.cells[0]
		oldData0 := ht.rawData[0]

		newAlloc := int(newCellCnt) * int(strCellSize)
		if newAlloc < mpool.GB {
			// update hashTable cnt.
			ht.blockCellCntBits = newCellCntBits
			ht.cellCnt = newCellCnt
			ht.blockMaxCellCnt = newCellCnt
			ht.blockMaxElemCnt = newBlockMaxElemCnt

			ht.rawData[0], err = m.Alloc(newAlloc)
			if err != nil {
				return err
			}
			ht.cells[0] = unsafe.Slice((*StringHashMapCell)(unsafe.Pointer(&ht.rawData[0][0])), ht.blockMaxCellCnt)

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
	blockNum := len(ht.rawData)
	for i := range ht.rawData {
		newBlockIdx := blockNum + i
		ht.rawData = append(ht.rawData, nil)
		ht.cells = append(ht.cells, nil)

		ht.rawData[newBlockIdx], err = m.Alloc(int(ht.blockMaxCellCnt) * int(strCellSize))
		if err != nil {
			return err
		}
		ht.cells[newBlockIdx] = unsafe.Slice((*StringHashMapCell)(unsafe.Pointer(&ht.rawData[newBlockIdx][0])), ht.blockMaxCellCnt)
	}
	ht.cellCnt = ht.blockMaxCellCnt * uint64(len(ht.rawData))
	return nil
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
		blockId := it.pos / it.table.blockMaxCellCnt
		cellId := it.pos % it.table.blockMaxCellCnt
		cell = &it.table.cells[blockId][cellId]
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
