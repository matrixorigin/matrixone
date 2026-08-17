// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hashmap

import (
	"math"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func validateIteratorVectors(
	vecs []*vector.Vector,
	start int,
	count int,
) error {
	if len(vecs) == 0 || start < 0 || count < 0 || count > UnitLimit {
		return mpool.ErrAllocationAccountInvalid
	}
	for _, vec := range vecs {
		if vec == nil {
			return mpool.ErrAllocationAccountInvalid
		}
		// Scalar const vectors physically store one value and broadcast it across
		// the caller's logical row range. Hash encoders already handle that
		// contract by reading row zero.
		if vec.IsConst() {
			if !vec.CoversLogicalRows(start, count) {
				return mpool.ErrAllocationAccountInvalid
			}
			continue
		}
		if start > vec.Length() || count > vec.Length()-start {
			return mpool.ErrAllocationAccountInvalid
		}
	}
	return nil
}

func hasGroupingInRange(vecs []*vector.Vector, start, count int) bool {
	end := uint64(start + count)
	for _, vec := range vecs {
		if vec != nil && vec.GetGrouping().GetBitmap().CountRange(
			uint64(start), end,
		) > 0 {
			return true
		}
	}
	return false
}

func rowHasGrouping(vecs []*vector.Vector, row int) bool {
	for _, vec := range vecs {
		if vec.GetGrouping().Contains(uint64(row)) {
			return true
		}
	}
	return false
}

func rowHasNull(vecs []*vector.Vector, row int) bool {
	for _, vec := range vecs {
		if vec.IsConstNull() || vec.GetNulls().Contains(uint64(row)) {
			return true
		}
	}
	return false
}

// markNonMatchingNaNs removes non-reflexive FLOAT keys from an equality
// hashmap without conflating them with SQL NULL. The underlying map sees a
// zero ring value; finishNonMatchingKeys restores the non-NULL state with no
// matching group so MARK joins produce FALSE rather than UNKNOWN.
func markNonMatchingNaNs(
	vecs []*vector.Vector,
	start, count int,
	zValues []int64,
	nonMatching []bool,
) bool {
	if len(nonMatching) < count {
		return false
	}
	marked := false
	for _, vec := range vecs {
		if vec.IsConstNull() {
			continue
		}
		oid := vec.GetType().Oid
		if oid != types.T_float32 && oid != types.T_float64 &&
			oid != types.T_array_float32 && oid != types.T_array_float64 &&
			oid != types.T_array_bf16 && oid != types.T_array_float16 {
			continue
		}
		nulls := vec.GetNulls()
		grouping := vec.GetGrouping()
		isConst := vec.IsConst()
		switch oid {
		case types.T_float32:
			values := vector.MustFixedColNoTypeCheck[float32](vec)
			for i := 0; i < count; i++ {
				row := start + i
				if nulls.Contains(uint64(row)) || grouping.Contains(uint64(row)) {
					continue
				}
				valueRow := row
				if isConst {
					valueRow = 0
				}
				if math.IsNaN(float64(values[valueRow])) {
					nonMatching[i] = true
					zValues[i] = 0
					marked = true
				}
			}
		case types.T_float64:
			values := vector.MustFixedColNoTypeCheck[float64](vec)
			for i := 0; i < count; i++ {
				row := start + i
				if nulls.Contains(uint64(row)) || grouping.Contains(uint64(row)) {
					continue
				}
				valueRow := row
				if isConst {
					valueRow = 0
				}
				if math.IsNaN(values[valueRow]) {
					nonMatching[i] = true
					zValues[i] = 0
					marked = true
				}
			}
		case types.T_array_float32, types.T_array_float64,
			types.T_array_bf16, types.T_array_float16:
			for i := 0; i < count; i++ {
				row := start + i
				if nulls.Contains(uint64(row)) || grouping.Contains(uint64(row)) {
					continue
				}
				valueRow := row
				if isConst {
					valueRow = 0
				}
				value := vec.GetBytesAt(valueRow)
				hasNaN := false
				if oid == types.T_array_float32 {
					for _, element := range types.BytesToArray[float32](value) {
						if math.IsNaN(float64(element)) {
							hasNaN = true
							break
						}
					}
				} else if oid == types.T_array_float64 {
					for _, element := range types.BytesToArray[float64](value) {
						if math.IsNaN(element) {
							hasNaN = true
							break
						}
					}
				} else if oid == types.T_array_bf16 {
					for _, element := range types.BytesToArray[types.BF16](value) {
						if math.IsNaN(float64(element.ToFloat32())) {
							hasNaN = true
							break
						}
					}
				} else {
					for _, element := range types.BytesToArray[types.Float16](value) {
						if math.IsNaN(float64(element.ToFloat32())) {
							hasNaN = true
							break
						}
					}
				}
				if hasNaN {
					nonMatching[i] = true
					zValues[i] = 0
					marked = true
				}
			}
		}
	}
	return marked
}

func finishNonMatchingKeys(
	vecs []*vector.Vector,
	start int,
	values []uint64,
	zValues []int64,
	nonMatching []bool,
) {
	for i, marked := range nonMatching {
		if !marked || rowHasNull(vecs, start+i) {
			continue
		}
		values[i] = 0
		zValues[i] = 1
	}
}

func prepareNonMatchingMask(mask []bool, count int, enabled bool) []bool {
	if !enabled {
		return mask[:0]
	}
	if cap(mask) < count {
		mask = make([]bool, count)
	} else {
		mask = mask[:count]
		clear(mask)
	}
	return mask
}

// MaxStrIteratorCapacity limits how many bytes of backing storage we keep when
// reusing a string iterator. Avoids retaining oversized buffers after handling
// very large strings.
const MaxStrIteratorCapacity = 64 * 1024

func IteratorChangeOwner(itr Iterator, m HashMap) {
	switch it := itr.(type) {
	case *intHashMapIterator:
		it.mp = m.(*IntHashMap)
	case *transactionalIntIterator:
		it.invalidatePreview()
		it.mp = m.(*IntHashMap)
	case *strHashmapIterator:
		changeStrIteratorOwner(it, m.(*StrHashMap))
	case *transactionalStrIterator:
		it.invalidatePreview()
		changeStrIteratorOwner(it.strHashmapIterator, m.(*StrHashMap))
	}
}

func changeStrIteratorOwner(it *strHashmapIterator, next *StrHashMap) {
	if it.mp != nil && it.mp.iteratorAllocation != next.iteratorAllocation {
		it.releaseScratch()
	}
	it.mp = next
}

// IteratorClearOwner detaches the iterator from its hashmap to allow the old
// hashmap to be garbage collected when the iterator is cached.
func IteratorClearOwner(itr Iterator) {
	switch it := itr.(type) {
	case *intHashMapIterator:
		it.mp = nil
	case *transactionalIntIterator:
		it.invalidatePreview()
		it.mp = nil
	case *strHashmapIterator:
		it.releaseAccountedScratch()
		it.mp = nil
	case *transactionalStrIterator:
		it.invalidatePreview()
		it.releaseAccountedScratch()
		it.mp = nil
	}
}

func (itr *transactionalStrIterator) invalidatePreview() {
	if itr != nil {
		itr.epoch++
	}
}

func (itr *transactionalIntIterator) invalidatePreview() {
	if itr != nil {
		itr.epoch++
	}
}

// StrIteratorCapacity reports the total capacity of all key buffers maintained
// by a string iterator. Used to decide if a cached iterator should be kept.
func StrIteratorCapacity(itr Iterator) int {
	var it *strHashmapIterator
	switch typed := itr.(type) {
	case *strHashmapIterator:
		it = typed
	case *transactionalStrIterator:
		if typed != nil {
			it = typed.strHashmapIterator
		}
	}
	if it == nil {
		return 0
	}
	return cap(it.keyBuffer)
}

func (itr *strHashmapIterator) releaseScratch() {
	if itr == nil {
		return
	}
	if cap(itr.keyBuffer) > 0 && itr.mp != nil && itr.mp.mp != nil &&
		itr.mp.iteratorAllocation != nil {
		itr.mp.mp.Free(itr.keyBuffer)
	}
	itr.keyBuffer = nil
	clear(itr.keys)
}

func (itr *strHashmapIterator) releaseAccountedScratch() {
	if itr == nil || cap(itr.keyBuffer) == 0 || itr.mp == nil ||
		itr.mp.iteratorAllocation == nil {
		return
	}
	itr.mp.mp.Free(itr.keyBuffer)
	itr.keyBuffer = nil
	clear(itr.keys)
}

func (itr *strHashmapIterator) Find(start, count int, vecs []*vector.Vector) ([]uint64, []int64, error) {
	if itr == nil || itr.mp == nil {
		return nil, nil, mpool.ErrAllocationAccountInvalid
	}
	if err := itr.prepareHashKeys(vecs, start, count); err != nil {
		return nil, nil, err
	}
	copy(itr.zValues[:count], OneInt64s[:count])
	copy(itr.values[:count], zeroUint64[:count])
	itr.nonMatching = prepareNonMatchingMask(
		itr.nonMatching, count, itr.mp.rejectNaN,
	)
	itr.encodeHashKeys(vecs, start, count)
	hasNonMatching := markNonMatchingNaNs(
		vecs, start, count, itr.zValues[:count], itr.nonMatching,
	)
	itr.mp.hashMap.FindStringBatch(itr.strHashStates, itr.keys[:count], itr.values)
	if hasNonMatching {
		finishNonMatchingKeys(
			vecs, start, itr.values[:count], itr.zValues[:count], itr.nonMatching,
		)
	}
	if !itr.mp.hasNull && !itr.mp.groupingAware &&
		hasGroupingInRange(vecs, start, count) {
		for i := 0; i < count; i++ {
			if rowHasGrouping(vecs, start+i) {
				itr.values[i] = 0
				itr.zValues[i] = 0
			}
		}
	}
	return itr.values[:count], itr.zValues[:count], nil
}

func (itr *transactionalStrIterator) Find(
	start, count int,
	vecs []*vector.Vector,
) ([]uint64, []int64, error) {
	itr.invalidatePreview()
	return itr.strHashmapIterator.Find(start, count, vecs)
}

func (itr *transactionalStrIterator) Preflight(
	start, count int,
	vecs []*vector.Vector,
) error {
	if itr == nil || itr.mp == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	itr.invalidatePreview()
	return itr.prepareHashKeys(vecs, start, count)
}

func (itr *transactionalStrIterator) PreviewInsert(
	start, count int,
	vecs []*vector.Vector,
	groupCount uint64,
	plan *InsertPlan,
) error {
	if itr == nil || itr.mp == nil || plan == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	itr.invalidatePreview()
	plan.reset()
	if err := itr.prepareHashKeys(vecs, start, count); err != nil {
		return err
	}
	defer func() {
		for i := 0; i < count; i++ {
			itr.keys[i] = itr.keys[i][:0]
		}
	}()
	if count == 0 {
		plan.base = groupCount
		plan.version = itr.mp.hashMap.Version()
		plan.epoch = itr.epoch
		plan.complete = true
		plan.strOwner = itr
		plan.ready = true
		return nil
	}
	copy(itr.zValues[:count], OneInt64s[:count])
	clear(itr.values[:count])
	itr.nonMatching = prepareNonMatchingMask(
		itr.nonMatching, count, itr.mp.rejectNaN)
	itr.encodeHashKeys(vecs, start, count)
	hashtable.BytesBatchGenHashStates(
		&itr.keys[0], &itr.strHashStates[0], count)
	hasNonMatching := markNonMatchingNaNs(
		vecs, start, count, itr.zValues[:count], itr.nonMatching)
	useRing := !itr.mp.hasNull || itr.mp.rejectNaN
	newGroups, version, complete, err := itr.mp.hashMap.PlanInsertStringBatch(
		groupCount, itr.zValues[:count], itr.strHashStates[:count],
		itr.values[:count], plan.slots[:count], plan.inserted[:count], useRing)
	if err != nil {
		return err
	}
	if !complete {
		if err = itr.mp.hashMap.FindPrehashedStringBatch(
			itr.zValues[:count], itr.strHashStates[:count],
			itr.values[:count], useRing); err != nil {
			return err
		}
		newGroups, err = previewMissingStringStates(
			groupCount, itr.zValues[:count], itr.strHashStates[:count],
			itr.values[:count], plan.inserted[:count], useRing)
		if err != nil {
			return err
		}
	}
	if hasNonMatching {
		finishNonMatchingKeys(
			vecs, start, itr.values[:count], itr.zValues[:count], itr.nonMatching)
	}
	plan.count = count
	plan.newGroups = newGroups
	plan.base = groupCount
	plan.version = version
	plan.epoch = itr.epoch
	plan.complete = complete
	plan.strOwner = itr
	plan.ready = true
	return nil
}

func (itr *transactionalStrIterator) CommitPreview(
	plan *InsertPlan,
) ([]uint64, []int64, error) {
	if itr == nil || itr.mp == nil || plan == nil || !plan.ready ||
		plan.strOwner != itr || plan.intOwner != nil ||
		plan.epoch != itr.epoch ||
		plan.count < 0 || plan.count > UnitLimit {
		return nil, nil, mpool.ErrAllocationAccountInvariant
	}
	count := plan.count
	more := plan.newGroups
	plan.ready = false
	itr.invalidatePreview()
	if itr.mp.rows != plan.base {
		return nil, nil, mpool.ErrAllocationAccountInvariant
	}
	// A duplicate-only unit has nothing to publish. Preview already produced
	// the final group mapping, so avoid a second hash-table pass while still
	// rejecting a plan invalidated by an independently mutated owner.
	if more == 0 {
		if !plan.complete || plan.version != itr.mp.hashMap.Version() {
			return nil, nil, mpool.ErrAllocationAccountInvariant
		}
		return itr.values[:count], itr.zValues[:count], nil
	}
	if err := itr.mp.hashMap.ResizeWithPlan(
		itr.mp.hashMap.PlanResize(more)); err != nil {
		return nil, nil, err
	}
	useRing := !itr.mp.hasNull || itr.mp.rejectNaN
	var originalZ [UnitLimit]int64
	if itr.mp.rejectNaN {
		copy(originalZ[:count], itr.zValues[:count])
		for row, marked := range itr.nonMatching {
			if marked {
				itr.zValues[row] = 0
			}
		}
	}
	if !plan.complete || plan.version != itr.mp.hashMap.Version() {
		var flags [UnitLimit]uint8
		newGroups, version, complete, err := itr.mp.hashMap.PlanInsertStringBatch(
			plan.base, itr.zValues[:count], itr.strHashStates[:count],
			itr.values[:count], plan.slots[:count], flags[:count], useRing)
		if err != nil {
			if itr.mp.rejectNaN {
				copy(itr.zValues[:count], originalZ[:count])
			}
			return nil, nil, err
		}
		if !complete || newGroups != more {
			if itr.mp.rejectNaN {
				copy(itr.zValues[:count], originalZ[:count])
			}
			return nil, nil, mpool.ErrAllocationAccountInvariant
		}
		for row := 0; row < count; row++ {
			if flags[row] != plan.inserted[row] {
				if itr.mp.rejectNaN {
					copy(itr.zValues[:count], originalZ[:count])
				}
				return nil, nil, mpool.ErrAllocationAccountInvariant
			}
		}
		plan.version = version
		plan.complete = true
	}
	err := itr.mp.hashMap.CommitInsertStringBatchPlan(
		plan.version, plan.base, itr.strHashStates[:count],
		itr.values[:count], plan.slots[:count], plan.inserted[:count])
	if itr.mp.rejectNaN {
		copy(itr.zValues[:count], originalZ[:count])
		for row, marked := range itr.nonMatching {
			if marked {
				itr.values[row] = 0
			}
		}
	}
	if err != nil {
		return nil, nil, err
	}
	itr.mp.rows = plan.base + more
	return itr.values[:count], itr.zValues[:count], nil
}

func previewMissingStringStates(
	base uint64,
	zValues []int64,
	states [][3]uint64,
	values []uint64,
	inserted []uint8,
	useRing bool,
) (uint64, error) {
	if len(values) < len(states) || len(inserted) < len(states) ||
		useRing && len(zValues) < len(states) {
		return 0, mpool.ErrAllocationAccountInvalid
	}
	clear(inserted[:len(states)])
	next := base
	var planned [UnitLimit * 2]uint16
	const plannedMask = len(planned) - 1
	for row, state := range states {
		if useRing && zValues[row] == 0 {
			values[row] = 0
			continue
		}
		if values[row] > base {
			return 0, mpool.ErrAllocationAccountInvariant
		}
		if values[row] != 0 {
			continue
		}
		for slot := int(state[0]) & plannedMask; ; slot = (slot + 1) & plannedMask {
			entry := planned[slot]
			if entry == 0 {
				next++
				values[row] = next
				inserted[row] = 1
				planned[slot] = uint16(row + 1)
				break
			}
			prior := int(entry - 1)
			if states[prior] == state {
				values[row] = values[prior]
				break
			}
		}
	}
	return next - base, nil
}

// Insert a row from multiple columns into the hashmap, return true if it is new, otherwise false
func (itr *strHashmapIterator) DetectDup(vecs []*vector.Vector, row int) (bool, error) {
	if !itr.mp.rejectNaN {
		keys := itr.keys
		defer func() { keys[0] = keys[0][:0] }()
		if err := itr.prepareHashKeys(vecs, row, 1); err != nil {
			return false, err
		}
		itr.encodeHashKeys(vecs, row, 1)
		if err := itr.mp.hashMap.InsertStringBatch(
			itr.strHashStates, keys[:1], itr.values[:1],
		); err != nil {
			return false, err
		}
		if itr.values[0] > itr.mp.rows {
			itr.mp.rows++
			return true, nil
		}
		return false, nil
	}
	before := itr.mp.rows
	values, zValues, err := itr.Insert(row, 1, vecs)
	if err != nil {
		return false, err
	}
	return zValues[0] != 0 && values[0] > before, nil
}

func (itr *transactionalStrIterator) DetectDup(
	vecs []*vector.Vector,
	row int,
) (bool, error) {
	itr.invalidatePreview()
	return itr.strHashmapIterator.DetectDup(vecs, row)
}

func (itr *strHashmapIterator) Insert(start, count int, vecs []*vector.Vector) ([]uint64, []int64, error) {
	var err error
	if itr == nil || itr.mp == nil {
		return nil, nil, mpool.ErrAllocationAccountInvalid
	}
	if err = itr.prepareHashKeys(vecs, start, count); err != nil {
		return nil, nil, err
	}
	defer func() {
		for i := 0; i < count; i++ {
			itr.keys[i] = itr.keys[i][:0]
		}
	}()
	copy(itr.zValues[:count], OneInt64s[:count])
	itr.nonMatching = prepareNonMatchingMask(
		itr.nonMatching, count, itr.mp.rejectNaN,
	)
	itr.encodeHashKeys(vecs, start, count)
	hasNonMatching := markNonMatchingNaNs(
		vecs, start, count, itr.zValues[:count], itr.nonMatching,
	)

	if itr.mp.hasNull && !itr.mp.rejectNaN {
		err = itr.mp.hashMap.InsertStringBatch(itr.strHashStates, itr.keys[:count], itr.values)
	} else {
		err = itr.mp.hashMap.InsertStringBatchWithRing(itr.zValues, itr.strHashStates, itr.keys[:count], itr.values)
	}

	vs, zvs := itr.values[:count], itr.zValues[:count]
	if err != nil {
		return nil, nil, err
	}
	if hasNonMatching {
		finishNonMatchingKeys(vecs, start, vs, zvs, itr.nonMatching)
	}
	updateHashTableRows(&itr.mp.rows, itr.mp.hasNull && !itr.mp.rejectNaN, vs, zvs)
	return vs, zvs, err
}

func (itr *transactionalStrIterator) Insert(
	start, count int,
	vecs []*vector.Vector,
) ([]uint64, []int64, error) {
	itr.invalidatePreview()
	return itr.strHashmapIterator.Insert(start, count, vecs)
}

func (itr *intHashMapIterator) Find(start, count int, vecs []*vector.Vector) ([]uint64, []int64, error) {
	if itr == nil || itr.mp == nil {
		return nil, nil, mpool.ErrAllocationAccountInvalid
	}
	if err := validateIteratorVectors(vecs, start, count); err != nil {
		return nil, nil, err
	}
	itr.ensureCapacity(count)
	if count == 0 {
		return itr.values, itr.zValues, nil
	}
	for i := 0; i < count; i++ {
		itr.keys[i] = 0
	}
	copy(itr.keyOffs[:count], zeroUint32)
	copy(itr.zValues[:count], OneInt64s[:count])
	copy(itr.values[:count], zeroUint64[:count])
	itr.nonMatching = prepareNonMatchingMask(
		itr.nonMatching, count, itr.mp.rejectNaN,
	)
	itr.encodeHashKeys(vecs, start, count)
	hasNonMatching := markNonMatchingNaNs(
		vecs, start, count, itr.zValues[:count], itr.nonMatching,
	)
	copy(itr.hashes[:count], zeroUint64[:count])
	itr.mp.hashMap.FindBatch(count, itr.hashes[:count], unsafe.Pointer(&itr.keys[0]), itr.values[:count])
	if hasNonMatching {
		finishNonMatchingKeys(
			vecs, start, itr.values[:count], itr.zValues[:count], itr.nonMatching,
		)
	}
	if hasGroupingInRange(vecs, start, count) {
		for i := 0; i < count; i++ {
			if rowHasGrouping(vecs, start+i) {
				itr.values[i] = 0
				itr.zValues[i] = 0
			}
		}
	}
	return itr.values[:count], itr.zValues[:count], nil
}

func (itr *transactionalIntIterator) Find(
	start, count int,
	vecs []*vector.Vector,
) ([]uint64, []int64, error) {
	itr.invalidatePreview()
	return itr.intHashMapIterator.Find(start, count, vecs)
}

func (itr *transactionalIntIterator) Preflight(
	start, count int,
	vecs []*vector.Vector,
) error {
	if itr == nil || itr.mp == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	itr.invalidatePreview()
	if err := validateIteratorVectors(vecs, start, count); err != nil {
		return err
	}
	itr.ensureCapacity(count)
	return nil
}

func (itr *transactionalIntIterator) PreviewInsert(
	start, count int,
	vecs []*vector.Vector,
	groupCount uint64,
	plan *InsertPlan,
) error {
	if itr == nil || itr.mp == nil || plan == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	itr.invalidatePreview()
	plan.reset()
	if err := validateIteratorVectors(vecs, start, count); err != nil {
		return err
	}
	itr.ensureCapacity(count)
	if count == 0 {
		plan.base = groupCount
		plan.version = itr.mp.hashMap.Version()
		plan.epoch = itr.epoch
		plan.complete = true
		plan.intOwner = itr
		plan.ready = true
		return nil
	}
	clear(itr.keys[:count])
	clear(itr.keyOffs[:count])
	defer func() {
		clear(itr.keys[:count])
		clear(itr.keyOffs[:count])
	}()
	copy(itr.zValues[:count], OneInt64s[:count])
	itr.nonMatching = prepareNonMatchingMask(
		itr.nonMatching, count, itr.mp.rejectNaN)
	itr.encodeHashKeys(vecs, start, count)
	hasNonMatching := markNonMatchingNaNs(
		vecs, start, count, itr.zValues[:count], itr.nonMatching)
	useRing := !itr.mp.hasNull || itr.mp.rejectNaN
	newGroups, version, complete, err := itr.mp.hashMap.PlanInsertBatch(
		count, groupCount, itr.zValues[:count], itr.hashes[:count],
		unsafe.Pointer(&itr.keys[0]), itr.values[:count],
		plan.slots[:count], plan.inserted[:count], useRing, false)
	if err != nil {
		return err
	}
	if !complete {
		if err = itr.mp.hashMap.FindPrehashedBatch(
			itr.zValues[:count], itr.hashes[:count],
			itr.values[:count], useRing); err != nil {
			return err
		}
		newGroups, err = previewMissingIntHashes(
			groupCount, itr.zValues[:count], itr.hashes[:count],
			itr.values[:count], plan.inserted[:count], useRing)
		if err != nil {
			return err
		}
	}
	if hasNonMatching {
		finishNonMatchingKeys(
			vecs, start, itr.values[:count], itr.zValues[:count], itr.nonMatching)
	}
	plan.count = count
	plan.newGroups = newGroups
	plan.base = groupCount
	plan.version = version
	plan.epoch = itr.epoch
	plan.complete = complete
	plan.intOwner = itr
	plan.ready = true
	return nil
}

func previewMissingIntHashes(
	base uint64,
	zValues []int64,
	hashes []uint64,
	values []uint64,
	inserted []uint8,
	useRing bool,
) (uint64, error) {
	if len(values) < len(hashes) || len(inserted) < len(hashes) ||
		useRing && len(zValues) < len(hashes) {
		return 0, mpool.ErrAllocationAccountInvalid
	}
	clear(inserted[:len(hashes)])
	next := base
	var planned [UnitLimit * 2]uint16
	const plannedMask = len(planned) - 1
	for row, hash := range hashes {
		if useRing && zValues[row] == 0 {
			values[row] = 0
			continue
		}
		if values[row] > base {
			return 0, mpool.ErrAllocationAccountInvariant
		}
		if values[row] != 0 {
			continue
		}
		for slot := int(hash) & plannedMask; ; slot = (slot + 1) & plannedMask {
			entry := planned[slot]
			if entry == 0 {
				next++
				values[row] = next
				inserted[row] = 1
				planned[slot] = uint16(row + 1)
				break
			}
			prior := int(entry - 1)
			if hashes[prior] == hash {
				values[row] = values[prior]
				break
			}
		}
	}
	return next - base, nil
}

func (itr *transactionalIntIterator) CommitPreview(
	plan *InsertPlan,
) ([]uint64, []int64, error) {
	if itr == nil || itr.mp == nil || plan == nil || !plan.ready ||
		plan.intOwner != itr || plan.strOwner != nil ||
		plan.epoch != itr.epoch ||
		plan.count < 0 || plan.count > UnitLimit {
		return nil, nil, mpool.ErrAllocationAccountInvariant
	}
	count := plan.count
	more := plan.newGroups
	plan.ready = false
	itr.invalidatePreview()
	if itr.mp.rows != plan.base {
		return nil, nil, mpool.ErrAllocationAccountInvariant
	}
	// A duplicate-only unit has nothing to publish. Preview already produced
	// the final group mapping, so avoid a second hash-table pass while still
	// rejecting a plan invalidated by an independently mutated owner.
	if more == 0 {
		if !plan.complete || plan.version != itr.mp.hashMap.Version() {
			return nil, nil, mpool.ErrAllocationAccountInvariant
		}
		return itr.values[:count], itr.zValues[:count], nil
	}
	if err := itr.mp.hashMap.ResizeWithPlan(
		itr.mp.hashMap.PlanResize(more)); err != nil {
		return nil, nil, err
	}
	if !plan.complete || plan.version != itr.mp.hashMap.Version() {
		useRing := !itr.mp.hasNull || itr.mp.rejectNaN
		var originalZ [UnitLimit]int64
		if itr.mp.rejectNaN {
			copy(originalZ[:count], itr.zValues[:count])
			for row, marked := range itr.nonMatching {
				if marked {
					itr.zValues[row] = 0
				}
			}
		}
		var flags [UnitLimit]uint8
		newGroups, version, complete, err := itr.mp.hashMap.PlanInsertBatch(
			count, plan.base, itr.zValues[:count], itr.hashes[:count], nil,
			itr.values[:count], plan.slots[:count], flags[:count],
			useRing, true)
		if itr.mp.rejectNaN {
			copy(itr.zValues[:count], originalZ[:count])
		}
		if err != nil {
			return nil, nil, err
		}
		if !complete || newGroups != more {
			return nil, nil, mpool.ErrAllocationAccountInvariant
		}
		for row := 0; row < count; row++ {
			if flags[row] != plan.inserted[row] {
				return nil, nil, mpool.ErrAllocationAccountInvariant
			}
		}
		plan.version = version
		plan.complete = true
	}
	if err := itr.mp.hashMap.CommitInsertBatchPlan(
		plan.version, plan.base, itr.hashes[:count],
		itr.values[:count], plan.slots[:count],
		plan.inserted[:count]); err != nil {
		return nil, nil, err
	}
	itr.mp.rows = plan.base + more
	return itr.values[:count], itr.zValues[:count], nil
}

func (itr *intHashMapIterator) DetectDup(vecs []*vector.Vector, row int) (bool, error) {
	if itr == nil || itr.mp == nil {
		return false, mpool.ErrAllocationAccountInvalid
	}
	before := itr.mp.rows
	values, zValues, err := itr.Insert(row, 1, vecs)
	if err != nil {
		return false, err
	}
	return zValues[0] != 0 && values[0] > before, nil
}

func (itr *transactionalIntIterator) DetectDup(
	vecs []*vector.Vector,
	row int,
) (bool, error) {
	itr.invalidatePreview()
	return itr.intHashMapIterator.DetectDup(vecs, row)
}

func (itr *intHashMapIterator) Insert(start, count int, vecs []*vector.Vector) ([]uint64, []int64, error) {
	var err error
	if itr == nil || itr.mp == nil {
		return nil, nil, mpool.ErrAllocationAccountInvalid
	}
	if err = validateIteratorVectors(vecs, start, count); err != nil {
		return nil, nil, err
	}
	itr.ensureCapacity(count)
	if count == 0 {
		return itr.values, itr.zValues, nil
	}

	defer func() {
		for i := 0; i < count; i++ {
			itr.keys[i] = 0
		}
		copy(itr.keyOffs[:count], zeroUint32)
	}()

	copy(itr.zValues[:count], OneInt64s[:count])
	itr.nonMatching = prepareNonMatchingMask(
		itr.nonMatching, count, itr.mp.rejectNaN,
	)
	itr.encodeHashKeys(vecs, start, count)
	hasNonMatching := markNonMatchingNaNs(
		vecs, start, count, itr.zValues[:count], itr.nonMatching,
	)
	copy(itr.hashes[:count], zeroUint64[:count])
	if itr.mp.hasNull && !itr.mp.rejectNaN {
		err = itr.mp.hashMap.InsertBatch(count, itr.hashes[:count], unsafe.Pointer(&itr.keys[0]), itr.values)
	} else {
		err = itr.mp.hashMap.InsertBatchWithRing(count, itr.zValues, itr.hashes[:count], unsafe.Pointer(&itr.keys[0]), itr.values)
	}
	vs, zvs := itr.values[:count], itr.zValues[:count]
	if err != nil {
		return nil, nil, err
	}
	if hasNonMatching {
		finishNonMatchingKeys(vecs, start, vs, zvs, itr.nonMatching)
	}
	updateHashTableRows(&itr.mp.rows, itr.mp.hasNull && !itr.mp.rejectNaN, vs, zvs)
	return vs, zvs, err
}

func (itr *transactionalIntIterator) Insert(
	start, count int,
	vecs []*vector.Vector,
) ([]uint64, []int64, error) {
	itr.invalidatePreview()
	return itr.intHashMapIterator.Insert(start, count, vecs)
}

func (itr *intHashMapIterator) ensureCapacity(count int) {
	if count > UnitLimit {
		panic("int hashmap iterator count exceeds UnitLimit")
	}
	keyCount := count
	if count > 0 && itr.mp != nil && itr.mp.hasNull {
		// A nullable 8-byte key stores a null marker followed by the value. The
		// last key can therefore use one byte in a guard slot past the logical
		// key slice, including when count is UnitLimit.
		keyCount++
	}
	if count <= cap(itr.keyOffs) && keyCount <= cap(itr.keys) {
		itr.keys = itr.keys[:count]
		itr.keyOffs = itr.keyOffs[:count]
		itr.values = itr.values[:count]
		itr.zValues = itr.zValues[:count]
		itr.hashes = itr.hashes[:count]
		return
	}

	itr.keys = make([]uint64, keyCount)[:count]
	itr.keyOffs = make([]uint32, count)
	itr.values = make([]uint64, count)
	itr.zValues = make([]int64, count)
	itr.hashes = make([]uint64, count)
}

func updateHashTableRows(rows *uint64, hasNull bool, vs []uint64, zvs []int64) {
	groupCount := *rows
	if hasNull {
		for _, v := range vs {
			if v > groupCount {
				groupCount++
			}
		}
	} else {
		for i, v := range vs {
			if zvs[i] == 0 {
				continue
			}
			if v > groupCount {
				groupCount++
			}
		}
	}
	*rows = groupCount
}
