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
	if it, ok := itr.(*intHashMapIterator); ok {
		it.mp = m.(*IntHashMap)
		return
	}
	it := itr.(*strHashmapIterator)
	next := m.(*StrHashMap)
	if it.mp != nil &&
		it.mp.iteratorAllocation != next.iteratorAllocation {
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
	case *strHashmapIterator:
		it.releaseAccountedScratch()
		it.mp = nil
	}
}

// StrIteratorCapacity reports the total capacity of all key buffers maintained
// by a string iterator. Used to decide if a cached iterator should be kept.
func StrIteratorCapacity(itr Iterator) int {
	it, ok := itr.(*strHashmapIterator)
	if !ok || it == nil {
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

func (itr *strHashmapIterator) Insert(start, count int, vecs []*vector.Vector) ([]uint64, []int64, error) {
	var err error

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
