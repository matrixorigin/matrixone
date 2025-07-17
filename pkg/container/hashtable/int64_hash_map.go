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

type Int64HashMapCell struct {
	Key    uint64
	Mapped uint64
}

type Int64HashMap struct {
	allocator malloc.Allocator

	blockCellCnt    uint64
	blockMaxElemCnt uint64
	cellCntMask     uint64

	cellCnt             uint64
	elemCnt             uint64
	rawData             [][]byte
	rawDataDeallocators []malloc.Deallocator
	cells               [][]Int64HashMapCell
}

var (
	intCellSize           uint64
	maxIntCellCntPerBlock uint64
)

func init() {
	intCellSize = uint64(unsafe.Sizeof(Int64HashMapCell{}))
	maxIntCellCntPerBlock = maxBlockSize / intCellSize
}

func (ht *Int64HashMap) Free() {
	for i, de := range ht.rawDataDeallocators {
		if de != nil {
			de.Deallocate(malloc.NoHints)
		}
		ht.rawData[i], ht.cells[i] = nil, nil
	}
	ht.rawData, ht.cells = nil, nil
}

func (ht *Int64HashMap) allocate(index int, size uint64) error {
	if ht.rawDataDeallocators[index] != nil {
		panic("overwriting")
	}
	bs, de, err := ht.allocator.Allocate(size, malloc.NoHints)
	if err != nil {
		return err
	}
	ht.rawData[index] = bs
	ht.rawDataDeallocators[index] = de
	ht.cells[index] = unsafe.Slice((*Int64HashMapCell)(unsafe.Pointer(&ht.rawData[index][0])), ht.blockCellCnt)
	return nil
}

func (ht *Int64HashMap) Init(allocator malloc.Allocator) (err error) {
	if allocator == nil {
		allocator = DefaultAllocator()
	}
	ht.allocator = allocator
	ht.blockCellCnt = kInitialCellCnt
	ht.blockMaxElemCnt = maxElemCnt(kInitialCellCnt, intCellSize)
	ht.cellCntMask = kInitialCellCnt - 1
	ht.elemCnt = 0
	ht.cellCnt = kInitialCellCnt

	ht.rawData = make([][]byte, 1)
	ht.rawDataDeallocators = make([]malloc.Deallocator, 1)
	ht.cells = make([][]Int64HashMapCell, 1)

	if err = ht.allocate(0, uint64(ht.blockCellCnt*intCellSize)); err != nil {
		return err
	}

	return
}

func (ht *Int64HashMap) InsertBatch(n int, hashes []uint64, keysPtr unsafe.Pointer, values []uint64) error {
	if err := ht.ResizeOnDemand(n); err != nil {
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

func (ht *Int64HashMap) InsertBatchWithRing(n int, zValues []int64, hashes []uint64, keysPtr unsafe.Pointer, values []uint64) error {
	if err := ht.ResizeOnDemand(n); err != nil {
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

func (ht *Int64HashMap) ResizeOnDemand(cnt int) error {

	targetCnt := ht.elemCnt + uint64(cnt)
	if targetCnt <= uint64(len(ht.rawData))*ht.blockMaxElemCnt {
		return nil
	}

	newCellCnt := ht.cellCnt << 1
	newMaxElemCnt := maxElemCnt(newCellCnt, intCellSize)
	for newMaxElemCnt < targetCnt {
		newCellCnt <<= 1
		newMaxElemCnt = maxElemCnt(newCellCnt, intCellSize)
	}

	newAllocSize := int(newCellCnt * intCellSize)
	if ht.blockCellCnt == maxIntCellCntPerBlock {
		// double the blocks
		oldBlockNum := len(ht.rawData)
		newBlockNum := newAllocSize / maxBlockSize

		ht.rawData = append(ht.rawData, make([][]byte, newBlockNum-oldBlockNum)...)
		ht.rawDataDeallocators = append(ht.rawDataDeallocators, make([]malloc.Deallocator, newBlockNum-oldBlockNum)...)
		ht.cells = append(ht.cells, make([][]Int64HashMapCell, newBlockNum-oldBlockNum)...)
		ht.cellCnt = ht.blockCellCnt * uint64(newBlockNum)
		ht.cellCntMask = ht.cellCnt - 1

		for i := oldBlockNum; i < newBlockNum; i++ {
			if err := ht.allocate(i, uint64(ht.blockCellCnt*intCellSize)); err != nil {
				return err
			}
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
	} else {
		oldCells0 := ht.cells[0]
		oldDeallocator := ht.rawDataDeallocators[0]
		ht.rawDataDeallocators[0] = nil
		ht.cellCnt = newCellCnt
		ht.cellCntMask = newCellCnt - 1

		if newAllocSize <= maxBlockSize {
			ht.blockCellCnt = newCellCnt
			ht.blockMaxElemCnt = newMaxElemCnt

			if err := ht.allocate(0, uint64(newAllocSize)); err != nil {
				return err
			}

		} else {
			ht.blockCellCnt = maxIntCellCntPerBlock
			ht.blockMaxElemCnt = maxElemCnt(ht.blockCellCnt, intCellSize)

			newBlockNum := newAllocSize / maxBlockSize
			ht.rawData = make([][]byte, newBlockNum)
			ht.rawDataDeallocators = make([]malloc.Deallocator, newBlockNum)
			ht.cells = make([][]Int64HashMapCell, newBlockNum)
			ht.cellCnt = ht.blockCellCnt * uint64(newBlockNum)
			ht.cellCntMask = ht.cellCnt - 1

			for i := 0; i < newBlockNum; i++ {
				if err := ht.allocate(i, uint64(ht.blockCellCnt*intCellSize)); err != nil {
					return err
				}
			}
		}

		// rearrange the cells
		for i := range oldCells0 {
			cell := &oldCells0[i]
			if cell.Mapped != 0 {
				newCell := ht.findEmptyCell(cell.Key)
				*newCell = *cell
			}
		}

		oldDeallocator.Deallocate(malloc.NoHints)
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

func (ht *Int64HashMap) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer

	// Write basic metadata
	buf.Write(types.EncodeUint64(&ht.elemCnt))
	buf.Write(types.EncodeUint64(&ht.cellCnt))
	buf.Write(types.EncodeUint64(&ht.blockCellCnt))
	buf.Write(types.EncodeUint64(&ht.blockMaxElemCnt))
	buf.Write(types.EncodeUint64(&ht.cellCntMask))

	// Write number of active cells
	buf.Write(types.EncodeUint64(&ht.elemCnt))

	// Write active cells
	if ht.elemCnt > 0 {
		for _, block := range ht.cells {
			for i := range block {
				if block[i].Mapped != 0 {
					buf.Write(types.EncodeUint64(&block[i].Key))
					buf.Write(types.EncodeUint64(&block[i].Mapped))
				}
			}
		}
	}

	return buf.Bytes(), nil
}

func (ht *Int64HashMap) UnmarshalBinary(data []byte, allocator malloc.Allocator) error {
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
	ht.cells = make([][]Int64HashMapCell, numBlocks)

	for i := 0; i < numBlocks; i++ {
		if err := ht.allocate(i, ht.blockCellCnt*intCellSize); err != nil {
			return err
		}
	}

	// Read active cells and re-insert them
	numActiveCells := types.DecodeUint64(r.Next(8))
	for i := uint64(0); i < numActiveCells; i++ {
		var cell Int64HashMapCell
		cell.Key = types.DecodeUint64(r.Next(8))
		cell.Mapped = types.DecodeUint64(r.Next(8))
		*ht.findEmptyCell(cell.Key) = cell
	}
	return nil
}
