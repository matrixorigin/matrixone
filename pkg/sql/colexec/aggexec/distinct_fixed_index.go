// Copyright 2026 Matrix Origin
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

package aggexec

import (
	"encoding/binary"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
)

// A DISTINCT aggregate still needs its ordered skiplist for state merge and
// serialization. The skiplist is, however, a poor membership index for the
// hot row path: every duplicate probes O(log N) nodes. For one fixed-width
// argument we can keep an exact open-addressing index alongside it.
//
// The index stores complete keys (not only a hash), so hash collisions can
// never change the result. Its key/group and log columns are deliberately
// separate: Go pads a {uint64,uint16} entry to 16 bytes and a
// {uint64,int32} entry to 16 bytes, while the columnar representation uses 10
// and 12 bytes per retained key respectively. All columns are allocated via
// the aggregate account and are included in retainedBytes.
const (
	distinctFixedIndexInitialSlots = 256
	distinctFixedIndexSmallSlots   = 16
	// A state with fewer groups is normally a single-group or short-lived
	// work set; retaining an index there costs more than it saves.  Partial
	// states read from a parallel aggregate can have fewer than AggBatchSize
	// rows while still containing thousands of groups, so the activation gate
	// must be lower than the physical chunk size.
	distinctFixedIndexMinGroups       = 1024
	distinctFixedIndexLoadNumerator   = 7
	distinctFixedIndexLoadDenominator = 10
	distinctFixedIndexEmptyGroup      = math.MaxUint16
	distinctFixedBatchEmptyGroup      = math.MaxUint64
	// See preflightBatchMergeArgs: once a destination has published fixed
	// keys, switching its representation under a tight hard account would need
	// a second copy. Small accounts therefore use the existing spill-friendly
	// merge path from the beginning.
	distinctFixedIndexMinAccountLimit = 8 << 20
)

type distinctFixedIndex struct {
	slotKeys   []uint64
	slotGroups []uint16
	groupHead  []int32
	logKeys    []uint64
	logNext    []int32
	count      uint32
	groupLimit int
}

// distinctFixedBatch is the allocation-free admission set for one immutable
// UnitLimit work unit. It mirrors the resident index's exact (group, value)
// identity, so fixed-width preflight never needs to hash raw byte slices or
// compare the same row with up to eight earlier representatives.
type distinctFixedBatch struct {
	groups [distinctArgumentBatchSlots]uint64
	values [distinctArgumentBatchSlots]uint64
}

func (batch *distinctFixedBatch) reset() {
	for i := range batch.groups {
		batch.groups[i] = distinctFixedBatchEmptyGroup
	}
}

func (batch *distinctFixedBatch) seenOrInsert(group uint64, value uint64) (bool, error) {
	if batch == nil {
		return false, mpool.ErrAllocationAccountInvalid
	}
	const mask = distinctArgumentBatchSlots - 1
	hash := distinctFixedHash(group, value)
	for probes, slot := 0, int(hash&mask); probes < len(batch.groups); probes, slot = probes+1, (slot+1)&mask {
		storedGroup := batch.groups[slot]
		if storedGroup == distinctFixedBatchEmptyGroup {
			batch.groups[slot] = group
			batch.values[slot] = value
			return false, nil
		}
		if storedGroup == group && batch.values[slot] == value {
			return true, nil
		}
	}
	// The caller normally limits a work unit to hashmap.UnitLimit, leaving at
	// least half of this table empty. Return a controlled error if that
	// contract is violated instead of spinning forever on a full table.
	return false, mpool.ErrAllocationAccountInvariant
}

func distinctFixedIndexWidth(
	info *aggInfo,
	c int,
) int {
	if c < distinctFixedIndexMinGroups {
		return 0
	}
	width := distinctFixedKeyWidth(info)
	if width == 0 {
		return 0
	}
	return width
}

func distinctFixedKeyWidth(info *aggInfo) int {
	if info == nil || !info.isDistinct || !info.saveArg ||
		info.preserveDistinctInputOrder || len(info.argTypes) != 1 {
		return 0
	}
	size := int(info.argTypes[0].GetSize())
	if size <= 0 || size > 8 || !info.argTypes[0].IsFixedLen() {
		return 0
	}
	return size
}

func decodeDistinctFixedKey(key []byte, width int) (uint16, uint64, bool) {
	if width <= 0 || width > 8 || len(key) != kAggArgPrefixSz+width {
		return 0, 0, false
	}
	var raw [8]byte
	copy(raw[:], key[kAggArgPrefixSz:])
	return binary.BigEndian.Uint16(key[:kAggArgPrefixSz]),
		binary.LittleEndian.Uint64(raw[:]), true
}

func (ag *aggState) distinctFixedIndexContains(key []byte) bool {
	if ag == nil || ag.distinctKeyWidth == 0 {
		return false
	}
	group, value, ok := decodeDistinctFixedKey(key, ag.distinctKeyWidth)
	return ok && ag.distinctIndex.lookup(group, value)
}

func distinctFixedHash(group uint64, key uint64) uint64 {
	// SplitMix64's finalizer gives a cheap, high-quality probe start even when
	// values are sequential and all rows belong to one aggregate group.
	x := key ^ (uint64(group) + 0x9e3779b97f4a7c15)
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	return x ^ (x >> 31)
}

func (index *distinctFixedIndex) lookup(group uint16, key uint64) bool {
	if index == nil || len(index.slotKeys) == 0 ||
		len(index.slotKeys) != len(index.slotGroups) {
		return false
	}
	mask := uint64(len(index.slotKeys) - 1)
	for probes, slot := 0, distinctFixedHash(uint64(group), key)&mask; probes < len(index.slotGroups); probes, slot = probes+1, (slot+1)&mask {
		storedGroup := index.slotGroups[slot]
		if storedGroup == distinctFixedIndexEmptyGroup {
			return false
		}
		if storedGroup == group && index.slotKeys[slot] == key {
			return true
		}
	}
	// A valid index always leaves an empty slot because ensureCapacityFor keeps
	// the load below 70%.  Treat a fully occupied/corrupt table as a miss; the
	// subsequent insertion path returns the controlled invariant error instead
	// of spinning forever.
	return false
}

func (index *distinctFixedIndex) allocate(
	mp *mpool.MPool,
	allocation *AllocationAccount,
	slots int,
) ([]uint64, []uint16, error) {
	if slots <= 0 || slots&(slots-1) != 0 {
		return nil, nil, mpool.ErrAllocationAccountInvalid
	}
	var (
		keys   []uint64
		groups []uint16
		err    error
	)
	if allocation == nil {
		keys, err = mpool.MakeSlice[uint64](slots, mp, true)
	} else {
		keys, err = makeAccountedScratch[uint64](allocation, mp, slots)
	}
	if err != nil {
		return nil, nil, err
	}
	if allocation == nil {
		groups, err = mpool.MakeSlice[uint16](slots, mp, true)
	} else {
		groups, err = makeAccountedScratch[uint16](allocation, mp, slots)
	}
	if err != nil {
		mpool.FreeSlice(mp, keys)
		return nil, nil, err
	}
	for i := range groups {
		groups[i] = distinctFixedIndexEmptyGroup
	}
	return keys, groups, nil
}

func (index *distinctFixedIndex) allocateGroupHeads(
	mp *mpool.MPool,
	allocation *AllocationAccount,
) error {
	if len(index.groupHead) != 0 {
		return nil
	}
	var (
		heads []int32
		err   error
	)
	groupLimit := index.groupLimit
	if groupLimit <= 0 || groupLimit > AggBatchSize {
		groupLimit = AggBatchSize
	}
	if allocation == nil {
		heads, err = mpool.MakeSlice[int32](groupLimit, mp, true)
	} else {
		heads, err = makeAccountedScratch[int32](allocation, mp, groupLimit)
	}
	if err != nil {
		return err
	}
	for i := range heads {
		heads[i] = -1
	}
	index.groupHead = heads
	return nil
}

func (index *distinctFixedIndex) ensureLogCapacity(
	mp *mpool.MPool,
	allocation *AllocationAccount,
	required uint64,
) error {
	if required <= uint64(cap(index.logKeys)) {
		return nil
	}
	capacity := uint64(cap(index.logKeys))
	if capacity == 0 {
		capacity = distinctFixedIndexInitialSlots
	}
	for capacity < required {
		if capacity > math.MaxInt/2 {
			return mpool.ErrAllocationAllocatorLimit
		}
		capacity *= 2
	}
	if capacity > uint64(math.MaxInt) {
		return mpool.ErrAllocationAllocatorLimit
	}
	var (
		keys []uint64
		next []int32
		err  error
	)
	if allocation == nil {
		keys, err = mpool.MakeSlice[uint64](int(capacity), mp, true)
	} else {
		keys, err = makeAccountedScratch[uint64](allocation, mp, int(capacity))
	}
	if err != nil {
		return err
	}
	if allocation == nil {
		next, err = mpool.MakeSlice[int32](int(capacity), mp, true)
	} else {
		next, err = makeAccountedScratch[int32](allocation, mp, int(capacity))
	}
	if err != nil {
		mpool.FreeSlice(mp, keys)
		return err
	}
	copy(keys, index.logKeys)
	copy(next, index.logNext)
	oldKeys, oldNext := index.logKeys, index.logNext
	index.logKeys, index.logNext = keys, next
	if cap(oldKeys) > 0 {
		mpool.FreeSlice(mp, oldKeys)
	}
	if cap(oldNext) > 0 {
		mpool.FreeSlice(mp, oldNext)
	}
	return nil
}

func (index *distinctFixedIndex) free(mp *mpool.MPool) {
	if index == nil {
		return
	}
	if cap(index.slotKeys) > 0 {
		mpool.FreeSlice(mp, index.slotKeys)
	}
	if cap(index.slotGroups) > 0 {
		mpool.FreeSlice(mp, index.slotGroups)
	}
	if cap(index.groupHead) > 0 {
		mpool.FreeSlice(mp, index.groupHead)
	}
	if cap(index.logKeys) > 0 {
		mpool.FreeSlice(mp, index.logKeys)
	}
	if cap(index.logNext) > 0 {
		mpool.FreeSlice(mp, index.logNext)
	}
	index.slotKeys = nil
	index.slotGroups = nil
	index.groupHead = nil
	index.logKeys = nil
	index.logNext = nil
	index.count = 0
	index.groupLimit = 0
}

func (index *distinctFixedIndex) grow(
	mp *mpool.MPool,
	allocation *AllocationAccount,
) error {
	oldSlots := len(index.slotKeys)
	if oldSlots == 0 {
		initial := distinctFixedIndexInitialSlots
		if index.groupLimit > 0 && index.groupLimit < 64 {
			initial = distinctFixedIndexSmallSlots
		}
		newKeys, newGroups, err := index.allocate(mp, allocation, initial)
		if err != nil {
			return err
		}
		if err = index.allocateGroupHeads(mp, allocation); err != nil {
			mpool.FreeSlice(mp, newKeys)
			mpool.FreeSlice(mp, newGroups)
			return err
		}
		index.slotKeys = newKeys
		index.slotGroups = newGroups
		return nil
	}
	if oldSlots > math.MaxInt/2 {
		return mpool.ErrAllocationAllocatorLimit
	}
	newKeys, newGroups, err := index.allocate(mp, allocation, oldSlots*2)
	if err != nil {
		return err
	}
	mask := uint64(len(newKeys) - 1)
	for oldSlot, oldGroup := range index.slotGroups {
		if oldGroup == distinctFixedIndexEmptyGroup {
			continue
		}
		oldKey := index.slotKeys[oldSlot]
		for slot := distinctFixedHash(uint64(oldGroup), oldKey) & mask; ; slot = (slot + 1) & mask {
			if newGroups[slot] == distinctFixedIndexEmptyGroup {
				newGroups[slot] = oldGroup
				newKeys[slot] = oldKey
				break
			}
		}
	}
	oldKeys, oldGroups := index.slotKeys, index.slotGroups
	index.slotKeys, index.slotGroups = newKeys, newGroups
	if cap(oldKeys) > 0 {
		mpool.FreeSlice(mp, oldKeys)
	}
	if cap(oldGroups) > 0 {
		mpool.FreeSlice(mp, oldGroups)
	}
	return nil
}

func (index *distinctFixedIndex) ensureCapacityFor(
	mp *mpool.MPool,
	allocation *AllocationAccount,
	extra uint64,
) error {
	if extra == 0 {
		return nil
	}
	if extra > math.MaxUint32 {
		return mpool.ErrAllocationAllocatorLimit
	}
	if uint64(index.count) > math.MaxUint32-extra {
		return mpool.ErrAllocationAllocatorLimit
	}
	required := uint64(index.count) + extra
	if len(index.slotKeys) == 0 {
		if err := index.grow(mp, allocation); err != nil {
			return err
		}
	}
	for required*distinctFixedIndexLoadDenominator >=
		uint64(len(index.slotKeys))*distinctFixedIndexLoadNumerator {
		if err := index.grow(mp, allocation); err != nil {
			return err
		}
	}
	return index.ensureLogCapacity(mp, allocation, required)
}

// prepare checks for an existing key and ensures one free slot for a new key.
// The slot is not published until insert, so a later skiplist allocation
// failure cannot leave the acceleration index ahead of the source of truth.
func (index *distinctFixedIndex) prepare(
	mp *mpool.MPool,
	allocation *AllocationAccount,
	group uint16,
	key uint64,
) (bool, error) {
	if index.lookup(group, key) {
		return true, nil
	}
	if err := index.ensureCapacityFor(mp, allocation, 1); err != nil {
		return false, err
	}
	return false, nil
}

func (index *distinctFixedIndex) insert(group uint16, key uint64) error {
	if index == nil || len(index.slotKeys) == 0 ||
		len(index.slotKeys) != len(index.slotGroups) ||
		int(group) >= len(index.groupHead) ||
		uint64(index.count) >= uint64(len(index.logKeys)) ||
		len(index.logKeys) != len(index.logNext) {
		return mpool.ErrAllocationAccountInvariant
	}
	mask := uint64(len(index.slotKeys) - 1)
	for probes, slot := 0, distinctFixedHash(uint64(group), key)&mask; probes < len(index.slotGroups); probes, slot = probes+1, (slot+1)&mask {
		if index.slotGroups[slot] == distinctFixedIndexEmptyGroup {
			index.slotGroups[slot] = group
			index.slotKeys[slot] = key
			index.logKeys[index.count] = key
			index.logNext[index.count] = index.groupHead[group]
			index.groupHead[group] = int32(index.count)
			index.count++
			return nil
		}
		if index.slotGroups[slot] == group && index.slotKeys[slot] == key {
			return nil
		}
	}
	return mpool.ErrAllocationAccountInvariant
}

func (index *distinctFixedIndex) forEach(group uint16, fn func(uint64) error) error {
	if index == nil || fn == nil || int(group) >= len(index.groupHead) ||
		len(index.logKeys) != len(index.logNext) {
		return mpool.ErrAllocationAccountInvalid
	}
	visited := uint32(0)
	for pos := index.groupHead[group]; pos >= 0; pos = index.logNext[pos] {
		if visited >= index.count {
			return mpool.ErrAllocationAccountInvariant
		}
		visited++
		if int(pos) >= len(index.logKeys) {
			return mpool.ErrAllocationAccountInvariant
		}
		if err := fn(index.logKeys[pos]); err != nil {
			return err
		}
	}
	return nil
}

// mergeInto imports one source group directly into a destination fixed index.
// BatchMerge has already admitted the exact candidate count, so this path can
// avoid rebuilding the [group|value] key and re-entering the skiplist wrapper
// for every value.  prepare is retained as the safety boundary for callers
// that do not share the normal preflight protocol: it is a no-op after the
// reservation made by preflight and still returns a controlled capacity error
// instead of allowing insert to observe an unallocated table.
func (index *distinctFixedIndex) mergeInto(
	mp *mpool.MPool,
	allocation *AllocationAccount,
	targetGroup uint16,
	source *distinctFixedIndex,
	sourceGroup uint16,
) (uint32, error) {
	if index == nil || source == nil ||
		int(targetGroup) >= index.groupLimit ||
		int(sourceGroup) >= source.groupLimit ||
		len(source.logKeys) != len(source.logNext) {
		return 0, mpool.ErrAllocationAccountInvariant
	}
	// An empty source has never needed to allocate its optional index columns.
	// It is still a valid fixed-width state and contributes no keys.
	if len(source.groupHead) == 0 {
		return 0, nil
	}
	var added uint32
	visited := uint32(0)
	for pos := source.groupHead[sourceGroup]; pos >= 0; pos = source.logNext[pos] {
		if visited >= source.count {
			return added, mpool.ErrAllocationAccountInvariant
		}
		visited++
		if int(pos) >= len(source.logKeys) {
			return added, mpool.ErrAllocationAccountInvariant
		}
		value := source.logKeys[pos]
		duplicate, err := index.prepare(
			mp, allocation, targetGroup, value)
		if err != nil {
			return added, err
		}
		if duplicate {
			continue
		}
		if err := index.insert(targetGroup, value); err != nil {
			return added, err
		}
		added++
	}
	return added, nil
}

func (index *distinctFixedIndex) retainedBytes() uint64 {
	if index == nil {
		return 0
	}
	return uint64(cap(index.slotKeys))*8 +
		uint64(cap(index.slotGroups))*2 +
		uint64(cap(index.groupHead))*4 +
		uint64(cap(index.logKeys))*8 +
		uint64(cap(index.logNext))*4
}
