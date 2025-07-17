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
	"bytes"
	"io"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
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
	allocator malloc.Allocator

	blockCellCnt    uint64
	blockMaxElemCnt uint64
	cellCntMask     uint64

	cellCnt             uint64
	elemCnt             uint64
	rawData             [][]byte
	rawDataDeallocators []malloc.Deallocator
	cells               [][]StringHashMapCell
}

var (
	strCellSize           uint64
	maxStrCellCntPerBlock uint64
)

func init() {
	strCellSize = uint64(unsafe.Sizeof(StringHashMapCell{}))
	maxStrCellCntPerBlock = maxBlockSize / strCellSize
}

func (ht *StringHashMap) Free() {
	for i, de := range ht.rawDataDeallocators {
		if de != nil {
			de.Deallocate(malloc.NoHints)
		}
		ht.rawData[i], ht.cells[i] = nil, nil
	}
	ht.rawData, ht.cells = nil, nil
}

func (ht *StringHashMap) allocate(index int, size uint64) error {
	if ht.rawDataDeallocators[index] != nil {
		panic("overwriting")
	}
	bs, de, err := ht.allocator.Allocate(size, malloc.NoHints)
	if err != nil {
		return err
	}
	ht.rawData[index] = bs
	ht.rawDataDeallocators[index] = de
	ht.cells[index] = unsafe.Slice((*StringHashMapCell)(unsafe.Pointer(&ht.rawData[index][0])), ht.blockCellCnt)
	return nil
}

func (ht *StringHashMap) Init(allocator malloc.Allocator) (err error) {
	if allocator == nil {
		allocator = DefaultAllocator()
	}
	ht.allocator = allocator
	ht.blockCellCnt = kInitialCellCnt
	ht.blockMaxElemCnt = maxElemCnt(kInitialCellCnt, strCellSize)
	ht.elemCnt = 0
	ht.cellCnt = kInitialCellCnt
	ht.cellCntMask = kInitialCellCnt - 1

	ht.rawData = make([][]byte, 1)
	ht.rawDataDeallocators = make([]malloc.Deallocator, 1)
	ht.cells = make([][]StringHashMapCell, 1)

	if err := ht.allocate(0, uint64(ht.blockCellCnt*strCellSize)); err != nil {
		return err
	}

	return
}

func (ht *StringHashMap) InsertStringBatch(states [][3]uint64, keys [][]byte, values []uint64) error {
	if err := ht.ResizeOnDemand(uint64(len(keys))); err != nil {
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

func (ht *StringHashMap) InsertStringBatchWithRing(zValues []int64, states [][3]uint64, keys [][]byte, values []uint64) error {
	if err := ht.ResizeOnDemand(uint64(len(keys))); err != nil {
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

func (ht *StringHashMap) ResizeOnDemand(n uint64) error {

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
		ht.rawDataDeallocators = append(ht.rawDataDeallocators, make([]malloc.Deallocator, newBlockNum-oldBlockNum)...)
		ht.cells = append(ht.cells, make([][]StringHashMapCell, newBlockNum-oldBlockNum)...)
		ht.cellCnt = ht.blockCellCnt * uint64(newBlockNum)
		ht.cellCntMask = ht.cellCnt - 1

		for i := oldBlockNum; i < newBlockNum; i++ {
			if err := ht.allocate(i, uint64(ht.blockCellCnt*strCellSize)); err != nil {
				return err
			}
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
		oldDeallocator := ht.rawDataDeallocators[0]
		ht.rawDataDeallocators[0] = nil
		ht.cellCnt = newCellCnt
		ht.cellCntMask = ht.cellCnt - 1

		if newAlloc <= maxBlockSize {
			ht.blockCellCnt = newCellCnt
			ht.blockMaxElemCnt = newMaxElemCnt

			if err := ht.allocate(0, uint64(newAlloc)); err != nil {
				return err
			}

		} else {
			ht.blockCellCnt = maxStrCellCntPerBlock
			ht.blockMaxElemCnt = maxElemCnt(ht.blockCellCnt, strCellSize)

			newBlockNum := newAlloc / maxBlockSize
			ht.rawData = make([][]byte, newBlockNum)
			ht.rawDataDeallocators = make([]malloc.Deallocator, newBlockNum)
			ht.cells = make([][]StringHashMapCell, newBlockNum)
			ht.cellCnt = ht.blockCellCnt * uint64(newBlockNum)
			ht.cellCntMask = ht.cellCnt - 1

			for i := 0; i < newBlockNum; i++ {
				if err := ht.allocate(i, uint64(ht.blockCellCnt*strCellSize)); err != nil {
					return err
				}
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

		oldDeallocator.Deallocate(malloc.NoHints)
	}

	return nil
}

func (ht *StringHashMap) Size() int64 {
	// 88 is the origin size of StringHashMaps
	ret := int64(88)
	for i := range ht.rawData {
		ret += 24
		ret += int64(len(ht.rawData[i]))
		ret += 24
		ret += int64(32 * len(ht.cells[i]))
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

func (ht *StringHashMap) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer

	// Write basic metadata
	buf.Write(types.EncodeUint64(&ht.elemCnt))
	buf.Write(types.EncodeUint64(&ht.cellCnt))
	buf.Write(types.EncodeUint64(&ht.blockCellCnt))
	buf.Write(types.EncodeUint64(&ht.blockMaxElemCnt))
	buf.Write(types.EncodeUint64(&ht.cellCntMask))

	// Write number of active cells
	// Note: ht.elemCnt is written again here to explicitly store the number of active cells.
	// This is separate from the metadata's elemCnt to ensure clarity during deserialization.
	buf.Write(types.EncodeUint64(&ht.elemCnt))

	// Write active cells
	if ht.elemCnt > 0 {
		for _, block := range ht.cells {
			for i := range block {
				if block[i].Mapped != 0 {
					buf.Write(types.EncodeUint64(&block[i].HashState[0]))
					buf.Write(types.EncodeUint64(&block[i].HashState[1]))
					buf.Write(types.EncodeUint64(&block[i].HashState[2]))
					buf.Write(types.EncodeUint64(&block[i].Mapped))
				}
			}
		}
	}

	return buf.Bytes(), nil
}

func (ht *StringHashMap) UnmarshalBinary(data []byte, allocator malloc.Allocator) error {
	r := bytes.NewBuffer(data)

	// Read basic metadata
	var buf [8]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return moerr.NewInternalErrorNoCtxf("failed to read elemCnt: %v", err)
	}
	ht.elemCnt = types.DecodeUint64(buf[:])
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return moerr.NewInternalErrorNoCtxf("failed to read cellCnt: %v", err)
	}
	ht.cellCnt = types.DecodeUint64(buf[:])
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return moerr.NewInternalErrorNoCtxf("failed to read blockCellCnt: %v", err)
	}
	ht.blockCellCnt = types.DecodeUint64(buf[:])
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return moerr.NewInternalErrorNoCtxf("failed to read blockMaxElemCnt: %v", err)
	}
	ht.blockMaxElemCnt = types.DecodeUint64(buf[:])
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return moerr.NewInternalErrorNoCtxf("failed to read cellCntMask: %v", err)
	}
	ht.cellCntMask = types.DecodeUint64(buf[:])

	ht.allocator = allocator
	if ht.allocator == nil {
		ht.allocator = DefaultAllocator()
	}

	// Initialize internal structures based on deserialized metadata
	numBlocks := int(ht.cellCnt / ht.blockCellCnt)
	if ht.cellCnt%ht.blockCellCnt != 0 {
		numBlocks++
	}
	ht.rawData = make([][]byte, numBlocks)
	ht.rawDataDeallocators = make([]malloc.Deallocator, numBlocks)
	ht.cells = make([][]StringHashMapCell, numBlocks)

	for i := 0; i < numBlocks; i++ {
		if err := ht.allocate(i, ht.blockCellCnt*strCellSize); err != nil {
			return err
		}
	}

	// Read active cells and re-insert them
	numActiveCells := types.DecodeUint64(r.Next(8))
	for i := uint64(0); i < numActiveCells; i++ {
		var cell StringHashMapCell
		cell.HashState[0] = types.DecodeUint64(r.Next(8))
		cell.HashState[1] = types.DecodeUint64(r.Next(8))
		cell.HashState[2] = types.DecodeUint64(r.Next(8))
		cell.Mapped = types.DecodeUint64(r.Next(8))
		*ht.findEmptyCell(&cell.HashState) = cell
	}
	return nil
}
