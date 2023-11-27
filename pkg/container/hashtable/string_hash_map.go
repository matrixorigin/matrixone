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
	blockCellCnt    uint64
	blockMaxElemCnt uint64
	cellCntMask     uint64
	//confCnt     uint64

	cellCnt uint64
	elemCnt uint64
	rawData [][]byte
	cells   [][]StringHashMapCell
}

var (
	strCellSize           uint64
	maxStrCellCntPerBlock uint64
)

func init() {
	strCellSize = uint64(unsafe.Sizeof(StringHashMapCell{}))
	maxStrCellCntPerBlock = maxBlockSize / strCellSize
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
	ht.blockCellCnt = kInitialCellCnt
	ht.blockMaxElemCnt = maxElemCnt(kInitialCellCnt, strCellSize)
	ht.elemCnt = 0
	ht.cellCnt = kInitialCellCnt
	ht.cellCntMask = kInitialCellCnt - 1

	ht.rawData = make([][]byte, 1)
	ht.cells = make([][]StringHashMapCell, 1)
	if ht.rawData[0], err = m.Alloc(int(ht.blockCellCnt * strCellSize)); err == nil {
		ht.cells[0] = unsafe.Slice((*StringHashMapCell)(unsafe.Pointer(&ht.rawData[0][0])), ht.blockCellCnt)
	}
	return
}

func (ht *StringHashMap) Dup() *StringHashMap {
	val := &StringHashMap{
		blockCellCnt:    ht.blockCellCnt,
		blockMaxElemCnt: ht.blockMaxElemCnt,
		cellCntMask:     ht.cellCntMask,

		cellCnt: ht.cellCnt,
		elemCnt: ht.elemCnt,

		rawData: make([][]byte, len(ht.rawData)),
		cells:   make([][]StringHashMapCell, len(ht.cells)),
	}

	for i, raw := range ht.rawData {
		val.rawData[i] = make([]byte, len(raw))
		copy(val.rawData[i], raw)
	}

	for i, cell := range ht.cells {
		val.cells[i] = make([]StringHashMapCell, len(cell))
		copy(val.cells[i], cell)
	}

	return val
}

func (ht *StringHashMap) InsertStringBatch(states [][3]uint64, keys [][]byte, values []uint64, m *mpool.MPool) error {
	if err := ht.ResizeOnDemand(uint64(len(keys)), m); err != nil {
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
	if err := ht.ResizeOnDemand(uint64(len(keys)), m); err != nil {
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
	for idx := state[0] & ht.cellCntMask; true; idx = (idx + 1) & ht.cellCntMask {
		blockId := idx / ht.blockCellCnt
		cellId := idx % ht.blockCellCnt
		cell := &ht.cells[blockId][cellId]
		if cell.Mapped == 0 || cell.HashState == *state {
			return cell
		}
	}
	return nil
}

func (ht *StringHashMap) findEmptyCell(state *[3]uint64) *StringHashMapCell {
	for idx := state[0] & ht.cellCntMask; true; idx = (idx + 1) & ht.cellCntMask {
		blockId := idx / ht.blockCellCnt
		cellId := idx % ht.blockCellCnt
		cell := &ht.cells[blockId][cellId]
		if cell.Mapped == 0 {
			return cell
		}
	}
	return nil
}

func (ht *StringHashMap) ResizeOnDemand(n uint64, m *mpool.MPool) error {
	var err error

	targetCnt := ht.elemCnt + n
	if targetCnt <= uint64(len(ht.rawData))*ht.blockMaxElemCnt {
		return nil
	}

	newCellCnt := ht.cellCnt << 1
	newMaxElemCnt := maxElemCnt(newCellCnt, strCellSize)
	for newMaxElemCnt < targetCnt {
		newCellCnt <<= 1
		newMaxElemCnt = maxElemCnt(newCellCnt, strCellSize)
	}

	newAlloc := int(newCellCnt * strCellSize)
	if ht.blockCellCnt == maxStrCellCntPerBlock {
		// double the blocks
		oldBlockNum := len(ht.rawData)
		newBlockNum := newAlloc / maxBlockSize

		ht.rawData = append(ht.rawData, make([][]byte, newBlockNum-oldBlockNum)...)
		ht.cells = append(ht.cells, make([][]StringHashMapCell, newBlockNum-oldBlockNum)...)
		ht.cellCnt = ht.blockCellCnt * uint64(newBlockNum)
		ht.cellCntMask = ht.cellCnt - 1

		for i := oldBlockNum; i < newBlockNum; i++ {
			ht.rawData[i], err = m.Alloc(int(ht.blockCellCnt * strCellSize))
			if err != nil {
				return err
			}
			ht.cells[i] = unsafe.Slice((*StringHashMapCell)(unsafe.Pointer(&ht.rawData[i][0])), ht.blockCellCnt)
		}

		// rearrange the cells
		var block []StringHashMapCell
		var emptyCell StringHashMapCell

		for i := 0; i < oldBlockNum; i++ {
			block = ht.cells[i]
			for j := uint64(0); j < ht.blockCellCnt; j++ {
				cell := &block[j]
				if cell.Mapped == 0 {
					continue
				}
				newCell := ht.findCell(&cell.HashState)
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
			newCell := ht.findCell(&cell.HashState)
			if newCell != cell {
				*newCell = *cell
				*cell = emptyCell
			}
		}
	} else {
		oldCells0 := ht.cells[0]
		oldData0 := ht.rawData[0]
		ht.cellCnt = newCellCnt
		ht.cellCntMask = ht.cellCnt - 1

		if newAlloc <= maxBlockSize {
			ht.blockCellCnt = newCellCnt
			ht.blockMaxElemCnt = newMaxElemCnt

			ht.rawData[0], err = m.Alloc(newAlloc)
			if err != nil {
				return err
			}
			ht.cells[0] = unsafe.Slice((*StringHashMapCell)(unsafe.Pointer(&ht.rawData[0][0])), ht.blockCellCnt)
		} else {
			ht.blockCellCnt = maxStrCellCntPerBlock
			ht.blockMaxElemCnt = maxElemCnt(ht.blockCellCnt, strCellSize)

			newBlockNum := newAlloc / maxBlockSize
			ht.rawData = make([][]byte, newBlockNum)
			ht.cells = make([][]StringHashMapCell, newBlockNum)
			ht.cellCnt = ht.blockCellCnt * uint64(newBlockNum)
			ht.cellCntMask = ht.cellCnt - 1

			for i := 0; i < newBlockNum; i++ {
				ht.rawData[i], err = m.Alloc(int(ht.blockCellCnt * strCellSize))
				if err != nil {
					return err
				}
				ht.cells[i] = unsafe.Slice((*StringHashMapCell)(unsafe.Pointer(&ht.rawData[i][0])), ht.blockCellCnt)
			}
		}

		// rearrange the cells
		for i := range oldCells0 {
			cell := &oldCells0[i]
			if cell.Mapped != 0 {
				newCell := ht.findEmptyCell(&cell.HashState)
				*newCell = *cell
			}
		}

		m.Free(oldData0)
	}

	return nil
}

func (ht *StringHashMap) Cardinality() uint64 {
	return ht.elemCnt
}

func (ht *StringHashMap) Size() int64 {
	// 33 is the origin size of StringHashMaps
	ret := int64(33)
	for i := range ht.rawData {
		ret += int64(len(ht.rawData[i]))
		// 32 is the len of ht.cells[i]
		ret += 32
	}
	return ret
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
