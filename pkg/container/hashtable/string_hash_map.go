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
	mp *mpool.MPool

	blockCellCnt    uint64
	blockMaxElemCnt uint64
	cellCntMask     uint64

	cellCnt uint64
	elemCnt uint64
	cells   [][]StringHashMapCell

	version uint64
	admit   ResizeAdmission
}

var (
	strCellSize           uint64
	maxStrCellCntPerBlock uint64
)

func StringHashMapInitialAllocationBytes() uint64 { return kInitialCellCnt * strCellSize }

func init() {
	strCellSize = uint64(unsafe.Sizeof(StringHashMapCell{}))
	maxStrCellCntPerBlock = maxBlockSize / strCellSize
}

func (ht *StringHashMap) Free() {
	for i, c := range ht.cells {
		mpool.FreeSlice(ht.mp, c)
		ht.cells[i] = nil
	}
	ht.cells = nil
}

func (ht *StringHashMap) allocate(index int, ncells int) error {
	if ht.cells[index] != nil {
		panic("overwriting")
	}
	c, err := mpool.MakeSlice[StringHashMapCell](ncells, ht.mp, true)
	if err != nil {
		return err
	}
	ht.cells[index] = c
	return nil
}

func (ht *StringHashMap) Init(mp *mpool.MPool) (err error) {
	ht.mp = mp
	ht.blockCellCnt = kInitialCellCnt
	ht.blockMaxElemCnt = maxElemCnt(kInitialCellCnt, strCellSize)
	ht.elemCnt = 0
	ht.cellCnt = kInitialCellCnt
	ht.version = 0
	ht.cellCntMask = kInitialCellCnt - 1

	ht.cells = make([][]StringHashMapCell, 1)
	if err := ht.allocate(0, int(ht.blockCellCnt)); err != nil {
		return err
	}

	return
}

func (ht *StringHashMap) InsertStringBatch(states [][3]uint64, keys [][]byte, values []uint64) error {
	if len(keys) == 0 {
		return nil
	}
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
	if len(keys) == 0 {
		return nil
	}
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
	if n == 0 {
		return nil
	}
	return ht.ResizeWithPlan(ht.PlanResize(n))
}

// SetResizeAdmission installs an optional memory admission callback. The
// callback is called once for each growth, before any allocation or mutation.
func (ht *StringHashMap) SetResizeAdmission(admit ResizeAdmission) { ht.admit = admit }

// PlanResize computes growth accounting without allocating or changing the map.
func (ht *StringHashMap) PlanResize(n uint64) ResizePlan {
	return newResizePlan(ht.elemCnt, n, ht.cellCnt, ht.blockCellCnt,
		ht.blockMaxElemCnt, uint64(len(ht.cells)), strCellSize,
		maxStrCellCntPerBlock, ht.version)
}

// ResizeWithPlan applies a previously computed plan transactionally.
func (ht *StringHashMap) ResizeWithPlan(plan ResizePlan) error {
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

	newCells := make([][]StringHashMapCell, int(plan.TargetBlockCount))
	freeNew := func() {
		for i := range newCells {
			if newCells[i] != nil {
				mpool.FreeSlice(ht.mp, newCells[i])
				newCells[i] = nil
			}
		}
	}
	for i := range newCells {
		cells, err := mpool.MakeSlice[StringHashMapCell](int(plan.TargetBlockCellCount), ht.mp, true)
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
			for idx := old.HashState[0] & newMask; ; idx = (idx + 1) & newMask {
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

func (ht *StringHashMap) Size() int64 {
	// 88 is the origin size of StringHashMaps
	ret := int64(88)
	for i := range ht.cells {
		ret += int64(int(strCellSize) * len(ht.cells[i]))
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
	if _, err := ht.WriteTo(&buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (ht *StringHashMap) UnmarshalBinary(data []byte, mp *mpool.MPool) error {
	_, err := ht.UnmarshalFrom(bytes.NewReader(data), mp)
	return err
}

func (ht *StringHashMap) WriteTo(w io.Writer) (n int64, err error) {
	var wn int

	if wn, err = w.Write(types.EncodeUint64(&ht.elemCnt)); err != nil {
		return
	}
	n += int64(wn)

	// Write active cells
	if ht.elemCnt > 0 {
		it := &StringHashMapIterator{}
		it.Init(ht)
		for i := uint64(0); i < ht.elemCnt; i++ {
			cell, errNext := it.Next()
			if errNext != nil {
				return n, errNext
			}
			if wn, err = w.Write(types.EncodeUint64(&cell.HashState[0])); err != nil {
				return n, err
			}
			n += int64(wn)
			if wn, err = w.Write(types.EncodeUint64(&cell.HashState[1])); err != nil {
				return n, err
			}
			n += int64(wn)
			if wn, err = w.Write(types.EncodeUint64(&cell.HashState[2])); err != nil {
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

func (ht *StringHashMap) UnmarshalFrom(r io.Reader, mp *mpool.MPool) (n int64, err error) {
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
		if err = ht.ResizeOnDemand(elemCnt); err != nil {
			return
		}

		cellBuf := make([]byte, 32) // HashState + Mapped
		for range elemCnt {
			if rn, err = io.ReadFull(r, cellBuf); err != nil {
				return
			}
			n += int64(rn)

			var cell StringHashMapCell
			cell.HashState[0] = types.DecodeUint64(cellBuf[0:8])
			cell.HashState[1] = types.DecodeUint64(cellBuf[8:16])
			cell.HashState[2] = types.DecodeUint64(cellBuf[16:24])
			cell.Mapped = types.DecodeUint64(cellBuf[24:32])

			newCell := ht.findEmptyCell(&cell.HashState)
			*newCell = cell
		}
	}
	ht.elemCnt = elemCnt

	return
}

func (ht *StringHashMap) FillGroupHashes(dst []uint64) []uint64 {
	dst = dst[:ht.elemCnt]
	for i := range ht.cells {
		for _, c := range ht.cells[i] {
			if c.Mapped != 0 {
				dst[c.Mapped-1] = c.HashState[0]
			}
		}
	}
	return dst
}
