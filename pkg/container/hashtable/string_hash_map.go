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
	cellCntBits uint8
	cellCnt     uint64
	elemCnt     uint64
	maxElemCnt  uint64
	cells       []StringHashMapCell
	//confCnt     uint64
}

var strCellSize int64

func init() {
	strCellSize = int64(unsafe.Sizeof(StringHashMapCell{}))
}

func (ht *StringHashMap) Free(m *mpool.MPool) {
	m.Decrease(strCellSize * int64(ht.cellCnt))
}

func (ht *StringHashMap) Init(m *mpool.MPool) error {
	ht.cellCntBits = kInitialCellCntBits
	ht.cellCnt = kInitialCellCnt
	ht.elemCnt = 0
	ht.maxElemCnt = kInitialCellCnt * kLoadFactorNumerator / kLoadFactorDenominator
	ht.cells = make([]StringHashMapCell, kInitialCellCnt)
	return m.Increase(kInitialCellCnt * strCellSize)
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

/*
func (ht *StringHashMap) InsertString24Batch(states [][3]uint64, keys [][3]uint64, values []uint64) {
	ht.resizeOnDemand(uint64(len(keys)))

	AesInt192BatchGenHashStates(&keys[0], &states[0], len(keys))

	for i := range keys {
		cell := ht.findCell(&states[i])
		if cell.Mapped == 0 {
			ht.elemCnt++
			cell.HashState = states[i]
			cell.Mapped = ht.elemCnt
		}
		values[i] = cell.Mapped
	}
}

func (ht *StringHashMap) InsertString32Batch(states [][3]uint64, keys [][4]uint64, values []uint64) {
	ht.resizeOnDemand(uint64(len(keys)))

	AesInt256BatchGenHashStates(&keys[0], &states[0], len(keys))

	for i := range keys {
		cell := ht.findCell(&states[i])
		if cell.Mapped == 0 {
			ht.elemCnt++
			cell.HashState = states[i]
			cell.Mapped = ht.elemCnt
		}
		values[i] = cell.Mapped
	}
}

func (ht *StringHashMap) InsertString40Batch(states [][3]uint64, keys [][5]uint64, values []uint64) {
	ht.resizeOnDemand(uint64(len(keys)))

	AesInt320BatchGenHashStates(&keys[0], &states[0], len(keys))

	for i := range keys {
		cell := ht.findCell(&states[i])
		if cell.Mapped == 0 {
			ht.elemCnt++
			cell.HashState = states[i]
			cell.Mapped = ht.elemCnt
		}
		values[i] = cell.Mapped
	}
}

func (ht *StringHashMap) InsertHashStateBatch(states [][3]uint64, values []uint64) {
	ht.resizeOnDemand(uint64(len(states)))

	for i := range states {
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

/*
func (ht *StringHashMap) InsertString24BatchWithRing(zValues []int64, states [][3]uint64, keys [][3]uint64, values []uint64) {
	ht.resizeOnDemand(uint64(len(keys)))

	AesInt192BatchGenHashStates(&keys[0], &states[0], len(keys))

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
}

func (ht *StringHashMap) InsertString32BatchWithRing(zValues []int64, states [][3]uint64, keys [][4]uint64, values []uint64) {
	ht.resizeOnDemand(uint64(len(keys)))

	AesInt256BatchGenHashStates(&keys[0], &states[0], len(keys))

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
}

func (ht *StringHashMap) InsertString40BatchWithRing(zValues []int64, states [][3]uint64, keys [][5]uint64, values []uint64) {
	ht.resizeOnDemand(uint64(len(keys)))

	AesInt320BatchGenHashStates(&keys[0], &states[0], len(keys))

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
}

func (ht *StringHashMap) InsertHashStateBatchWithRing(zValues []int64, states [][3]uint64, values []uint64) {
	ht.resizeOnDemand(uint64(len(states)))

	for i := range states {
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
}
*/

func (ht *StringHashMap) FindStringBatch(states [][3]uint64, keys [][]byte, values []uint64) {
	BytesBatchGenHashStates(&keys[0], &states[0], len(keys))

	for i := range keys {
		cell := ht.findCell(&states[i])
		values[i] = cell.Mapped
	}
}

func (ht *StringHashMap) FindStringBatchWithRing(states [][3]uint64, zValues []int64, keys [][]byte, values []uint64) {
	BytesBatchGenHashStates(&keys[0], &states[0], len(keys))

	for i := range keys {
		if zValues[i] == 0 {
			values[i] = 0
			continue
		}
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

func (ht *StringHashMap) FindHashStateBatch(states [][3]uint64, values []uint64) {
	for i := range states {
		cell := ht.findCell(&states[i])
		values[i] = cell.Mapped
	}
}

func (ht *StringHashMap) findCell(state *[3]uint64) *StringHashMapCell {
	mask := ht.cellCnt - 1
	for idx := state[0] & mask; true; idx = (idx + 1) & mask {
		cell := &ht.cells[idx]
		if cell.Mapped == 0 || cell.HashState == *state {
			return cell
		}
		//ht.confCnt++
	}

	return nil
}

func (ht *StringHashMap) findEmptyCell(state *[3]uint64) *StringHashMapCell {
	mask := ht.cellCnt - 1
	for idx := state[0] & mask; true; idx = (idx + 1) & mask {
		cell := &ht.cells[idx]
		if cell.Mapped == 0 {
			return cell
		}
		//ht.confCnt++
	}

	return nil
}

func (ht *StringHashMap) resizeOnDemand(n uint64, m *mpool.MPool) error {
	targetCnt := ht.elemCnt + n
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

	ht.cellCntBits = newCellCntBits
	ht.cellCnt = newCellCnt
	ht.maxElemCnt = newMaxElemCnt
	if err := m.Increase(int64(newCellCnt-oldCellCnt) * strCellSize); err != nil {
		return err
	}
	ht.cells = make([]StringHashMapCell, newCellCnt)
	for i := uint64(0); i < oldCellCnt; i++ {
		cell := &oldCells[i]
		if cell.Mapped != 0 {
			newCell := ht.findEmptyCell(&cell.HashState)
			*newCell = *cell
		}
	}
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
