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

	blockCellCntBits uint8
	cellCntMask      uint64

	cellCnt uint64
	elemCnt uint64
	cells   [][]Int64HashMapCell
	account *AllocationAccountSelection

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

func (ht *Int64HashMap) blockCellCnt() uint64 {
	return uint64(1) << ht.blockCellCntBits
}

func (ht *Int64HashMap) cellAt(index uint64) *Int64HashMapCell {
	blockID := index >> ht.blockCellCntBits
	cellID := index & (ht.blockCellCnt() - 1)
	return &ht.cells[blockID][cellID]
}

func (ht *Int64HashMap) Free() {
	ht.freeCells(ht.cells)
	ht.cells = nil
	ht.account = nil
}

func (ht *Int64HashMap) freeCells(cells [][]Int64HashMapCell) {
	for i, block := range cells {
		freeHashTableCellSlice(ht.mp, block)
		cells[i] = nil
	}
	freeHashTableDescriptorSlice(ht.mp, cells, ht.account)
}

func (ht *Int64HashMap) allocateCells(blockCount int, blockCellCnt uint64) ([][]Int64HashMapCell, error) {
	cells, err := makeHashTableDescriptorSlice[[]Int64HashMapCell](
		blockCount,
		ht.mp,
		ht.account,
		ht.descriptorSite(),
	)
	if err != nil {
		return nil, err
	}
	for i := range cells {
		block, err := makeHashTableCellSlice[Int64HashMapCell](
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

func (ht *Int64HashMap) appendCells(
	blockCount int,
	blockCellCnt uint64,
) ([][]Int64HashMapCell, error) {
	cells, err := makeHashTableDescriptorSlice[[]Int64HashMapCell](
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
		block, allocErr := makeHashTableCellSlice[Int64HashMapCell](
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

func (ht *Int64HashMap) Init(mp *mpool.MPool) (err error) {
	return ht.InitWithAllocation(mp, nil)
}

func (ht *Int64HashMap) InitWithAllocation(
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
	ht.cellCntMask = kInitialCellCnt - 1
	ht.elemCnt = 0
	ht.cellCnt = kInitialCellCnt
	ht.version = 0

	if ht.cells, err = ht.allocateCells(1, ht.blockCellCnt()); err != nil {
		ht.account = nil
		return err
	}

	return
}

func (ht *Int64HashMap) cellSite() mpool.AllocationSite {
	if ht.account == nil {
		return 0
	}
	return ht.account.cellSite
}

func (ht *Int64HashMap) descriptorSite() mpool.AllocationSite {
	if ht.account == nil {
		return 0
	}
	return ht.account.descriptorSite
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

// FindPrehashedBatch looks up already hashed keys without mutating or
// allocating. It is used when an exact preview temporarily outgrows the
// current table and will be replanned after exact resize admission.
func (ht *Int64HashMap) FindPrehashedBatch(
	zValues []int64,
	hashes []uint64,
	values []uint64,
	useRing bool,
) error {
	if len(values) < len(hashes) || useRing && len(zValues) < len(hashes) {
		return mpool.ErrAllocationAccountInvalid
	}
	for row, hash := range hashes {
		if useRing && zValues[row] == 0 {
			values[row] = 0
			continue
		}
		values[row] = ht.findCell(hash).Mapped
	}
	return nil
}

// PlanInsertBatch computes the exact mapping and target cells for one bounded
// batch without changing the table. It models earlier new rows in the same
// batch, so duplicate and colliding hashes receive the same mapping that a
// sequential insert would publish. complete is false only if the current
// physical table has too few empty cells; callers may resize exactly and plan
// again.
func (ht *Int64HashMap) PlanInsertBatch(
	n int,
	base uint64,
	zValues []int64,
	hashes []uint64,
	keysPtr unsafe.Pointer,
	values []uint64,
	slots []uint64,
	inserted []uint8,
	useRing bool,
	prehashed bool,
) (newGroups uint64, version uint64, complete bool, err error) {
	if base != ht.elemCnt {
		return 0, ht.version, false, mpool.ErrAllocationAccountInvariant
	}
	if n < 0 || n > len(hashes) || n > len(values) || n > len(slots) ||
		n > len(inserted) || useRing && n > len(zValues) {
		return 0, ht.version, false, mpool.ErrAllocationAccountInvalid
	}
	if n == 0 {
		return 0, ht.version, true, nil
	}
	if !prehashed {
		Int64BatchHash(keysPtr, &hashes[0], n)
	}
	clear(values[:n])
	clear(inserted[:n])
	var planned [512]uint16
	const plannedMask = len(planned) - 1
	next := ht.elemCnt
	for row, hash := range hashes[:n] {
		if useRing && zValues[row] == 0 {
			continue
		}
		index := hash & ht.cellCntMask
		found := false
		for probes := uint64(0); probes < ht.cellCnt; probes++ {
			cell := ht.cellAt(index)
			if cell.Mapped != 0 {
				if cell.Key == hash {
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
					if hashes[prior] == hash {
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

// CommitInsertBatchPlan publishes a complete plan without probing or
// allocating. The table generation and element count make stale plans fail
// before the first cell changes. A malformed plan is rolled back before the
// error is returned, so the successful path needs only one pass over the
// bounded plan while publication remains atomic to the caller.
func (ht *Int64HashMap) CommitInsertBatchPlan(
	version uint64,
	base uint64,
	hashes []uint64,
	values []uint64,
	slots []uint64,
	inserted []uint8,
) error {
	if version != ht.version || base != ht.elemCnt ||
		len(values) < len(hashes) || len(slots) < len(hashes) ||
		len(inserted) < len(hashes) {
		return mpool.ErrAllocationAccountInvariant
	}
	next := base
	for row, flag := range inserted[:len(hashes)] {
		if flag > 1 {
			ht.rollbackInsertBatchPlan(slots, inserted, row)
			return mpool.ErrAllocationAccountInvalid
		}
		if flag == 0 {
			continue
		}
		next++
		if values[row] != next || slots[row] >= ht.cellCnt {
			ht.rollbackInsertBatchPlan(slots, inserted, row)
			return mpool.ErrAllocationAccountInvariant
		}
		cell := ht.cellAt(slots[row])
		if cell.Mapped != 0 {
			ht.rollbackInsertBatchPlan(slots, inserted, row)
			return mpool.ErrAllocationAccountInvariant
		}
		cell.Key = hashes[row]
		cell.Mapped = values[row]
	}
	ht.elemCnt = next
	return nil
}

func (ht *Int64HashMap) rollbackInsertBatchPlan(
	slots []uint64,
	inserted []uint8,
	before int,
) {
	for row, flag := range inserted[:before] {
		if flag != 0 {
			*ht.cellAt(slots[row]) = Int64HashMapCell{}
		}
	}
}

func (ht *Int64HashMap) Version() uint64 { return ht.version }

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
		cell := ht.cellAt(idx)
		if cell.Key == hash || cell.Mapped == 0 {
			return cell
		}
	}
	return nil
}

func (ht *Int64HashMap) findEmptyCell(hash uint64) *Int64HashMapCell {
	for idx := hash & ht.cellCntMask; true; idx = (idx + 1) & ht.cellCntMask {
		cell := ht.cellAt(idx)
		if cell.Mapped == 0 {
			return cell
		}
	}
	return nil
}

func (ht *Int64HashMap) rehashInPlace(oldCellCnt uint64) {
	// Start immediately after an old empty slot, which is a linear-probing
	// cluster boundary. The load factor guarantees at least one such slot.
	emptyIndex := uint64(0)
	for emptyIndex < oldCellCnt && ht.cellAt(emptyIndex).Mapped != 0 {
		emptyIndex++
	}
	if emptyIndex == oldCellCnt {
		panic("cannot grow a full int64 hash map")
	}

	var emptyCell Int64HashMapCell
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
		*ht.findEmptyCell(cell.Key) = cell
	}
}

// SetResizeAdmission installs an optional memory admission callback. The
// callback is called once for each growth, before any allocation or mutation.
func (ht *Int64HashMap) SetResizeAdmission(admit ResizeAdmission) { ht.admit = admit }

// PlanResize computes growth accounting without allocating or changing the map.
func (ht *Int64HashMap) PlanResize(cnt uint64) ResizePlan {
	return newResizePlan(ht.elemCnt, cnt, ht.cellCnt, ht.blockCellCnt(),
		uint64(len(ht.cells)), intCellSize,
		maxIntCellCntPerBlock, ht.version)
}

func (ht *Int64HashMap) ResizeOnDemand(cnt int) error {
	additional := uint64(cnt)
	if !resizeNeeded(ht.elemCnt, additional, ht.cellCnt, intCellSize) {
		return nil
	}
	return ht.resizeOnDemand(additional)
}

func (ht *Int64HashMap) resizeOnDemand(additional uint64) error {
	return ht.ResizeWithPlan(ht.PlanResize(additional))
}

// ResizeWithPlan applies a previously computed plan transactionally.
func (ht *Int64HashMap) ResizeWithPlan(plan ResizePlan) error {
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
			for idx := old.Key & newMask; ; idx = (idx + 1) & newMask {
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

func (ht *Int64HashMap) Cardinality() uint64 {
	return ht.elemCnt
}

func (ht *Int64HashMap) Size() int64 {
	// 41 is the fixed size of Int64HashMap
	ret := int64(41)
	for i := range ht.cells {
		ret += int64(len(ht.cells[i]) * int(intCellSize))
	}
	if ht.account != nil {
		ret += int64(len(ht.cells)) * int64(unsafe.Sizeof([]Int64HashMapCell(nil)))
	} else {
		ret += int64(len(ht.cells)) * 16
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
	if remaining, bounded := readerRemainingBytes(r); bounded &&
		elemCnt > uint64(remaining)/16 {
		return n, io.ErrUnexpectedEOF
	}

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
