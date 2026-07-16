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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

type Int64HashMapCell struct {
	Key    uint64
	Mapped uint64
}

type Int64HashMap struct {
	mp *mpool.MPool

	blockCellCnt    uint64
	blockMaxElemCnt uint64
	cellCntMask     uint64

	cellCnt uint64
	elemCnt uint64
	cells   [][]Int64HashMapCell

	version uint64
	admit   ResizeAdmission
}

var (
	intCellSize           uint64
	maxIntCellCntPerBlock uint64
)

func Int64HashMapInitialAllocationBytes() uint64 { return kInitialCellCnt * intCellSize }

func init() {
	intCellSize = uint64(unsafe.Sizeof(Int64HashMapCell{}))
	maxIntCellCntPerBlock = maxBlockSize / intCellSize
}

func (ht *Int64HashMap) Free() {
	for i, c := range ht.cells {
		mpool.FreeSlice(ht.mp, c)
		ht.cells[i] = nil
	}
	ht.cells = nil
}

func (ht *Int64HashMap) allocate(index int, ncells int) error {
	if ht.cells[index] != nil {
		panic("overwriting")
	}

	cell, err := mpool.MakeSlice[Int64HashMapCell](ncells, ht.mp, true)
	if err != nil {
		return err
	}
	ht.cells[index] = cell
	return nil
}

func (ht *Int64HashMap) Init(mp *mpool.MPool) (err error) {
	ht.mp = mp
	ht.blockCellCnt = kInitialCellCnt
	ht.blockMaxElemCnt = maxElemCnt(kInitialCellCnt, intCellSize)
	ht.cellCntMask = kInitialCellCnt - 1
	ht.elemCnt = 0
	ht.cellCnt = kInitialCellCnt
	ht.version = 0

	ht.cells = make([][]Int64HashMapCell, 1)

	if err = ht.allocate(0, int(ht.blockCellCnt)); err != nil {
		return err
	}

	return
}

func (ht *Int64HashMap) InsertBatch(n int, hashes []uint64, keysPtr unsafe.Pointer, values []uint64) error {
	if n <= 0 {
		return nil
	}
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
	if n <= 0 {
		return nil
	}
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

// SetResizeAdmission installs an optional memory admission callback. The
// callback is called once for each growth, before any allocation or mutation.
func (ht *Int64HashMap) SetResizeAdmission(admit ResizeAdmission) { ht.admit = admit }

// PlanResize computes growth accounting without allocating or changing the map.
func (ht *Int64HashMap) PlanResize(cnt uint64) ResizePlan {
	return newResizePlan(ht.elemCnt, cnt, ht.cellCnt, ht.blockCellCnt,
		ht.blockMaxElemCnt, uint64(len(ht.cells)), intCellSize,
		maxIntCellCntPerBlock, ht.version)
}

func (ht *Int64HashMap) ResizeOnDemand(cnt int) error {
	if cnt <= 0 {
		return nil
	}
	return ht.ResizeWithPlan(ht.PlanResize(uint64(cnt)))
}

// ResizeWithPlan applies a previously computed plan transactionally.
func (ht *Int64HashMap) ResizeWithPlan(plan ResizePlan) error {
	if plan.Invalid {
		return ErrInvalidResizePlan
	}
	if plan.Noop {
		return nil
	}
	if !plan.matches(ht.version, ht.cellCnt, ht.blockCellCnt, uint64(len(ht.cells))) {
		return ErrStaleResizePlan
	}
	var reservation ResizeReservation
	if ht.admit != nil {
		var err error
		if reservation, err = ht.admit(plan); err != nil {
			return err
		}
	}
	committed := false
	defer func() {
		if reservation != nil && !committed {
			reservation.Rollback()
		}
	}()

	newCells := make([][]Int64HashMapCell, int(plan.TargetBlockCount))
	freeNew := func() {
		for i := range newCells {
			if newCells[i] != nil {
				mpool.FreeSlice(ht.mp, newCells[i])
				newCells[i] = nil
			}
		}
	}
	for i := range newCells {
		cells, err := mpool.MakeSlice[Int64HashMapCell](int(plan.TargetBlockCellCount), ht.mp, true)
		if err != nil {
			freeNew()
			return err
		}
		newCells[i] = cells
	}

	newMask := plan.TargetCellCount - 1
	for i := range ht.cells {
		for j := range ht.cells[i] {
			old := ht.cells[i][j]
			if old.Mapped == 0 {
				continue
			}
			for idx := old.Key & newMask; ; idx = (idx + 1) & newMask {
				cell := &newCells[idx/plan.TargetBlockCellCount][idx%plan.TargetBlockCellCount]
				if cell.Mapped == 0 {
					*cell = old
					break
				}
			}
		}
	}

	oldCells := ht.cells
	ht.cells = newCells
	ht.cellCnt = plan.TargetCellCount
	ht.cellCntMask = newMask
	ht.blockCellCnt = plan.TargetBlockCellCount
	ht.blockMaxElemCnt = plan.TargetMaxElemCount
	ht.version++
	for i := range oldCells {
		mpool.FreeSlice(ht.mp, oldCells[i])
	}
	if reservation != nil {
		reservation.Commit(plan)
	}
	committed = true
	return nil
}

func (ht *Int64HashMap) Cardinality() uint64 {
	return ht.elemCnt
}

func (ht *Int64HashMap) Size() int64 {
	// 41 is the fixed size of Int64HashMap
	ret := int64(41)
	for i := range ht.cells {
		ret += int64(len(ht.cells[i]) * int(intCellSize))
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
	if _, err := ht.WriteTo(&buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (ht *Int64HashMap) UnmarshalBinary(data []byte, mp *mpool.MPool) error {
	_, err := ht.UnmarshalFrom(bytes.NewReader(data), mp)
	return err
}

func (ht *Int64HashMap) WriteTo(w io.Writer) (n int64, err error) {
	var wn int

	// Write element count
	if wn, err = w.Write(types.EncodeUint64(&ht.elemCnt)); err != nil {
		return
	}
	n += int64(wn)

	// Write active cells
	if ht.elemCnt > 0 {
		it := &Int64HashMapIterator{}
		it.Init(ht)
		for i := uint64(0); i < ht.elemCnt; i++ {
			cell, errNext := it.Next()
			if errNext != nil {
				return n, errNext
			}
			if wn, err = w.Write(types.EncodeUint64(&cell.Key)); err != nil {
				return n, err
			}
			n += int64(wn)
			if wn, err = w.Write(types.EncodeUint64(&cell.Mapped)); err != nil {
				return n, err
			}
			n += int64(wn)
		}
	}

	return
}

func (ht *Int64HashMap) UnmarshalFrom(r io.Reader, mp *mpool.MPool) (n int64, err error) {
	var rn int

	// Read element count
	buf := make([]byte, 8)
	if rn, err = io.ReadFull(r, buf); err != nil {
		return
	}
	n += int64(rn)
	elemCnt := types.DecodeUint64(buf)

	if err = ht.Init(mp); err != nil {
		return
	}

	if elemCnt > 0 {
		if err = ht.ResizeOnDemand(int(elemCnt)); err != nil {
			return
		}

		cellBuf := make([]byte, 16) // Key + Mapped
		for range elemCnt {
			if rn, err = io.ReadFull(r, cellBuf); err != nil {
				return
			}
			n += int64(rn)

			key := types.DecodeUint64(cellBuf[0:8])
			mapped := types.DecodeUint64(cellBuf[8:16])

			cell := ht.findEmptyCell(key)
			cell.Key = key
			cell.Mapped = mapped
		}
	}
	ht.elemCnt = elemCnt

	return
}

func (ht *Int64HashMap) FillGroupHashes(dst []uint64) []uint64 {
	dst = dst[:ht.elemCnt]
	for i := range ht.cells {
		for _, c := range ht.cells[i] {
			if c.Mapped != 0 {
				dst[c.Mapped-1] = c.Key
			}
		}
	}
	return dst
}
