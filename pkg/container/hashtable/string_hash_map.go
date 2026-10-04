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

	blockCellCntBits uint8
	cellCntMask      uint64

	cellCnt uint64
	elemCnt uint64
	cells   [][]StringHashMapCell
	account *AllocationAccountSelection

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

func (ht *StringHashMap) blockCellCnt() uint64 {
	return uint64(1) << ht.blockCellCntBits
}

func (ht *StringHashMap) cellAt(index uint64) *StringHashMapCell {
	blockID := index >> ht.blockCellCntBits
	cellID := index & (ht.blockCellCnt() - 1)
	return &ht.cells[blockID][cellID]
}

func (ht *StringHashMap) Free() {
	ht.freeCells(ht.cells)
	ht.cells = nil
	ht.account = nil
}

func (ht *StringHashMap) freeCells(cells [][]StringHashMapCell) {
	for i, block := range cells {
		freeHashTableCellSlice(ht.mp, block)
		cells[i] = nil
	}
	freeHashTableDescriptorSlice(ht.mp, cells, ht.account)
}

func (ht *StringHashMap) allocateCells(blockCount int, blockCellCnt uint64) ([][]StringHashMapCell, error) {
	cells, err := makeHashTableDescriptorSlice[[]StringHashMapCell](
		blockCount,
		ht.mp,
		ht.account,
		ht.descriptorSite(),
	)
	if err != nil {
		return nil, err
	}
	for i := range cells {
		block, err := makeHashTableCellSlice[StringHashMapCell](
			int(blockCellCnt),
			ht.mp,
			ht.account,
			ht.cellSite(),
		)
		if err != nil {
			ht.freeCells(cells)
			return nil, err
		}
		cells[i] = block
	}
	return cells, nil
}

func (ht *StringHashMap) appendCells(
	blockCount int,
	blockCellCnt uint64,
) ([][]StringHashMapCell, error) {
	cells, err := makeHashTableDescriptorSlice[[]StringHashMapCell](
		blockCount,
		ht.mp,
		ht.account,
		ht.descriptorSite(),
	)
	if err != nil {
		return nil, err
	}
	copy(cells, ht.cells)
	for i := len(ht.cells); i < len(cells); i++ {
		block, allocErr := makeHashTableCellSlice[StringHashMapCell](
			int(blockCellCnt),
			ht.mp,
			ht.account,
			ht.cellSite(),
		)
		if allocErr != nil {
			for j := len(ht.cells); j < i; j++ {
				freeHashTableCellSlice(ht.mp, cells[j])
				cells[j] = nil
			}
			freeHashTableDescriptorSlice(ht.mp, cells, ht.account)
			return nil, allocErr
		}
		cells[i] = block
	}
	return cells, nil
}

func (ht *StringHashMap) Init(mp *mpool.MPool) (err error) {
	return ht.InitWithAllocation(mp, nil)
}

func (ht *StringHashMap) InitWithAllocation(
	mp *mpool.MPool,
	account *AllocationAccountSelection,
) (err error) {
	if account != nil {
		if err = account.validate(); err != nil {
			return err
		}
	}
	ht.mp = mp
	ht.account = account
	ht.blockCellCntBits = kInitialCellCntBits
	ht.elemCnt = 0
	ht.cellCnt = kInitialCellCnt
	ht.version = 0
	ht.cellCntMask = kInitialCellCnt - 1

	if ht.cells, err = ht.allocateCells(1, ht.blockCellCnt()); err != nil {
		ht.account = nil
		return err
	}

	return
}

func (ht *StringHashMap) cellSite() mpool.AllocationSite {
	if ht.account == nil {
		return 0
	}
	return ht.account.cellSite
}

func (ht *StringHashMap) descriptorSite() mpool.AllocationSite {
	if ht.account == nil {
		return 0
	}
	return ht.account.descriptorSite
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

// FindPrehashedStringBatch looks up already hashed states without mutating or
// allocating. It is used when an exact preview temporarily outgrows the
// current table and will be replanned after exact resize admission.
func (ht *StringHashMap) FindPrehashedStringBatch(
	zValues []int64,
	states [][3]uint64,
	values []uint64,
	useRing bool,
) error {
	if len(values) < len(states) || useRing && len(zValues) < len(states) {
		return mpool.ErrAllocationAccountInvalid
	}
	for row := range states {
		if useRing && zValues[row] == 0 {
			values[row] = 0
			continue
		}
		values[row] = ht.findCell(&states[row]).Mapped
	}
	return nil
}

// PlanInsertStringBatch computes the exact mapping and target cells for one
// bounded prehashed batch without changing the table. It models earlier new
// rows in the same batch, so duplicate hash states receive the same mapping
// that a sequential insert would publish. complete is false only when the
// current physical table has too few empty cells; the values and inserted
// outputs before that point must not be used by callers.
func (ht *StringHashMap) PlanInsertStringBatch(
	base uint64,
	zValues []int64,
	states [][3]uint64,
	values []uint64,
	slots []uint64,
	inserted []uint8,
	useRing bool,
) (newGroups uint64, version uint64, complete bool, err error) {
	if base != ht.elemCnt {
		return 0, ht.version, false, mpool.ErrAllocationAccountInvariant
	}
	n := len(states)
	if len(values) < n || len(slots) < n || len(inserted) < n ||
		useRing && len(zValues) < n {
		return 0, ht.version, false, mpool.ErrAllocationAccountInvalid
	}
	clear(values[:n])
	clear(inserted[:n])
	var planned [512]uint16
	const plannedMask = len(planned) - 1
	next := ht.elemCnt
	for row := range states {
		if useRing && zValues[row] == 0 {
			continue
		}
		state := states[row]
		index := state[0] & ht.cellCntMask
		found := false
		for probes := uint64(0); probes < ht.cellCnt; probes++ {
			cell := ht.cellAt(index)
			if cell.Mapped != 0 {
				if cell.HashState == state {
					values[row] = cell.Mapped
					slots[row] = index
					found = true
					break
				}
				index = (index + 1) & ht.cellCntMask
				continue
			}

			bucket := int(index) & plannedMask
			for planned[bucket] != 0 {
				prior := int(planned[bucket] - 1)
				if slots[prior] == index {
					if states[prior] == state {
						values[row] = values[prior]
						slots[row] = index
						found = true
					}
					break
				}
				bucket = (bucket + 1) & plannedMask
			}
			if found {
				break
			}
			if planned[bucket] == 0 {
				next++
				values[row] = next
				slots[row] = index
				inserted[row] = 1
				planned[bucket] = uint16(row + 1)
				found = true
				break
			}
			index = (index + 1) & ht.cellCntMask
		}
		if !found {
			return 0, ht.version, false, nil
		}
	}
	return next - ht.elemCnt, ht.version, true, nil
}

// CommitInsertStringBatchPlan publishes a complete prehashed plan without
// probing or allocating. The table generation and element count make stale
// plans fail before the first cell changes. A malformed plan is rolled back
// before the error is returned, keeping the successful path to one bounded
// publication pass without weakening caller-visible atomicity.
func (ht *StringHashMap) CommitInsertStringBatchPlan(
	version uint64,
	base uint64,
	states [][3]uint64,
	values []uint64,
	slots []uint64,
	inserted []uint8,
) error {
	if version != ht.version || base != ht.elemCnt ||
		len(values) < len(states) || len(slots) < len(states) ||
		len(inserted) < len(states) {
		return mpool.ErrAllocationAccountInvariant
	}
	next := base
	for row, flag := range inserted[:len(states)] {
		if flag > 1 {
			ht.rollbackInsertStringBatchPlan(slots, inserted, row)
			return mpool.ErrAllocationAccountInvalid
		}
		if flag == 0 {
			continue
		}
		next++
		if values[row] != next || slots[row] >= ht.cellCnt {
			ht.rollbackInsertStringBatchPlan(slots, inserted, row)
			return mpool.ErrAllocationAccountInvariant
		}
		cell := ht.cellAt(slots[row])
		if cell.Mapped != 0 {
			ht.rollbackInsertStringBatchPlan(slots, inserted, row)
			return mpool.ErrAllocationAccountInvariant
		}
		cell.HashState = states[row]
		cell.Mapped = values[row]
	}
	ht.elemCnt = next
	return nil
}

func (ht *StringHashMap) rollbackInsertStringBatchPlan(
	slots []uint64,
	inserted []uint8,
	before int,
) {
	for row, flag := range inserted[:before] {
		if flag != 0 {
			*ht.cellAt(slots[row]) = StringHashMapCell{}
		}
	}
}

func (ht *StringHashMap) Version() uint64 { return ht.version }

func (ht *StringHashMap) FindStringBatch(states [][3]uint64, keys [][]byte, values []uint64) {
	BytesBatchGenHashStates(&keys[0], &states[0], len(keys))

	for i := range keys {
		cell := ht.findCell(&states[i])
		values[i] = cell.Mapped
	}
}

func (ht *StringHashMap) findCell(state *[3]uint64) *StringHashMapCell {
	for idx := state[0] & ht.cellCntMask; true; idx = (idx + 1) & ht.cellCntMask {
		cell := ht.cellAt(idx)
		if cell.Mapped == 0 || cell.HashState == *state {
			return cell
		}
	}
	return nil
}

func (ht *StringHashMap) findEmptyCell(state *[3]uint64) *StringHashMapCell {
	for idx := state[0] & ht.cellCntMask; true; idx = (idx + 1) & ht.cellCntMask {
		cell := ht.cellAt(idx)
		if cell.Mapped == 0 {
			return cell
		}
	}
	return nil
}

func (ht *StringHashMap) rehashInPlace(oldCellCnt uint64) {
	// Start immediately after an old empty slot, which is a linear-probing
	// cluster boundary. The load factor guarantees at least one such slot.
	emptyIndex := uint64(0)
	for emptyIndex < oldCellCnt && ht.cellAt(emptyIndex).Mapped != 0 {
		emptyIndex++
	}
	if emptyIndex == oldCellCnt {
		panic("cannot grow a full string hash map")
	}

	var emptyCell StringHashMapCell
	oldMask := oldCellCnt - 1
	for offset := uint64(1); offset < oldCellCnt; offset++ {
		index := (emptyIndex + offset) & oldMask
		source := ht.cellAt(index)
		if source.Mapped == 0 {
			continue
		}
		cell := *source
		*source = emptyCell
		// Under the wider mask, a cell either moves into a newly allocated block
		// or into a hole at/before its old position in this scan order. It cannot
		// overwrite an unvisited old cell.
		*ht.findEmptyCell(&cell.HashState) = cell
	}
}

func (ht *StringHashMap) ResizeOnDemand(n uint64) error {
	if !resizeNeeded(ht.elemCnt, n, ht.cellCnt, strCellSize) {
		return nil
	}
	return ht.resizeOnDemand(n)
}

func (ht *StringHashMap) resizeOnDemand(additional uint64) error {
	return ht.ResizeWithPlan(ht.PlanResize(additional))
}

// SetResizeAdmission installs an optional memory admission callback. The
// callback is called once for each growth, before any allocation or mutation.
func (ht *StringHashMap) SetResizeAdmission(admit ResizeAdmission) { ht.admit = admit }

// PlanResize computes growth accounting without allocating or changing the map.
func (ht *StringHashMap) PlanResize(n uint64) ResizePlan {
	return newResizePlan(ht.elemCnt, n, ht.cellCnt, ht.blockCellCnt(),
		uint64(len(ht.cells)), strCellSize,
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
	if !plan.matches(ht.version, ht.cellCnt, ht.blockCellCnt(), uint64(len(ht.cells))) {
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

	if plan.ReuseCurrentBlocks {
		newCells, err := ht.appendCells(
			int(plan.TargetBlockCount),
			plan.TargetBlockCellCount,
		)
		if err != nil {
			return err
		}
		oldCellCnt := ht.cellCnt
		oldDescriptors := ht.cells
		ht.cells = newCells
		freeHashTableDescriptorSlice(ht.mp, oldDescriptors, ht.account)
		ht.cellCnt = plan.TargetCellCount
		ht.cellCntMask = ht.cellCnt - 1
		ht.version++
		ht.rehashInPlace(oldCellCnt)
		if reservation != nil {
			reservation.Commit(plan)
		}
		committed = true
		return nil
	}

	newCells, err := ht.allocateCells(int(plan.TargetBlockCount), plan.TargetBlockCellCount)
	if err != nil {
		return err
	}
	newMask := plan.TargetCellCount - 1
	newBlockBits := powerOfTwoBits(plan.TargetBlockCellCount)
	for i := range ht.cells {
		for j := range ht.cells[i] {
			old := ht.cells[i][j]
			if old.Mapped == 0 {
				continue
			}
			for idx := old.HashState[0] & newMask; ; idx = (idx + 1) & newMask {
				cell := &newCells[idx>>newBlockBits][idx&(plan.TargetBlockCellCount-1)]
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
	ht.blockCellCntBits = newBlockBits
	ht.version++
	ht.freeCells(oldCells)
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
	if ht.account != nil {
		ret += int64(len(ht.cells)) * int64(unsafe.Sizeof([]StringHashMapCell(nil)))
	}
	return ret
}

func (ht *StringHashMap) Cardinality() uint64 {
	if ht == nil {
		return 0
	}
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
		cell = it.table.cellAt(it.pos)
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
	if remaining, bounded := readerRemainingBytes(r); bounded &&
		elemCnt > uint64(remaining)/32 {
		return n, io.ErrUnexpectedEOF
	}

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
