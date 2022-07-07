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

package segmentio

import (
	"sync"
)

const UNIT_BYTES = 8 // Length of uint64 bytes
const UNITS_PER_UNITSET = 8
const UNITSET_BYTES = UNIT_BYTES * UNITS_PER_UNITSET
const BITS_PER_UNIT = UNIT_BYTES * 8
const BITS_PER_UNITSET = UNITSET_BYTES * 8
const ALL_UNIT_SET = 0xffffffffffffffff
const ALL_UNIT_CLEAR = 0

type BitmapAllocator struct {
	pageSize  uint32
	level0    []uint64
	level1    []uint64
	available uint64
	lastPos   uint64
	mutex     sync.RWMutex
}

func NewBitmapAllocator(capacity uint64, pageSize uint32) Allocator {
	bitmap := &BitmapAllocator{
		pageSize: pageSize,
	}
	bitmap.Init(capacity, pageSize)
	return bitmap
}

func p2align(x uint64, align uint64) uint64 {
	return x & -align
}

func p2roundup(x uint64, align uint64) uint64 {
	return -(-x & -align)
}

func (b *BitmapAllocator) Init(capacity uint64, pageSize uint32) {
	b.pageSize = pageSize
	l0granularity := pageSize
	l1granularity := l0granularity * BITS_PER_UNITSET
	l0UnitCount := capacity / uint64(l0granularity) / BITS_PER_UNIT
	b.level0 = make([]uint64, l0UnitCount)
	for i := range b.level0 {
		b.level0[i] = ALL_UNIT_SET
	}

	l1UnitCount := capacity / uint64(l1granularity) / BITS_PER_UNIT
	if l1UnitCount == 0 {
		l1UnitCount = 1
	}
	b.level1 = make([]uint64, l1UnitCount)
	for i := range b.level1 {
		b.level1[i] = ALL_UNIT_SET
	}
	b.available = p2align(capacity, uint64(pageSize))
	b.lastPos = 0
}

func (b *BitmapAllocator) markAllocFree0(start, length uint64, free bool) {
	pos := start
	var bit uint64 = 1 << (start % BITS_PER_UNIT)
	bitpos := pos / BITS_PER_UNIT
	val := &(b.level0[bitpos])
	end := length
	if end > p2roundup(pos+1, BITS_PER_UNIT) {
		end = p2roundup(pos+1, BITS_PER_UNIT)
	}
	for {
		if pos >= end {
			break
		}
		if free {
			*val |= bit
		} else {
			*val &= ^bit
		}
		bit <<= 1
		pos++
	}

	end = length
	if end > p2align(length, BITS_PER_UNIT) {
		end = p2align(length, BITS_PER_UNIT)
	}
	if int(bitpos) >= len(b.level0)-1 {
		return
	}
	for {
		if pos >= end {
			break
		}
		bitpos++
		val = &(b.level0[bitpos])
		if free {
			*val = ALL_UNIT_SET
		} else {
			*val = ALL_UNIT_CLEAR
		}
		pos += BITS_PER_UNIT
	}

	bit = 1
	bitpos++
	val = &(b.level0[bitpos])
	for {
		if pos >= length {
			break
		}
		if free {
			*val |= bit
		} else {
			*val &= ^bit
		}
		bit <<= 1
		pos++
	}
}

func (b *BitmapAllocator) markUnitLevel1(start, length uint64, free bool) {
	clear := true
	for idx := start / BITS_PER_UNIT; idx < length/BITS_PER_UNIT; idx++ {
		val := &(b.level0[idx])
		if *val != ALL_UNIT_CLEAR {
			clear = false
			break
		}
	}
	pos := start / BITS_PER_UNIT
	//end := length / BITS_PER_UNIT
	l1pos := start / BITS_PER_UNITSET
	pos++
	pos = p2roundup(pos, UNITS_PER_UNITSET)
	if !free && clear {

		if (pos % UNITS_PER_UNITSET) == 0 {
			l1val := &(b.level1[l1pos/BITS_PER_UNIT])
			var bit uint64 = 1 << (l1pos % BITS_PER_UNIT)
			*l1val &= ^bit
		}
	} else if free && !clear {
		if (pos % UNITS_PER_UNITSET) == 0 {
			l1val := &(b.level1[l1pos/BITS_PER_UNIT])
			var bit uint64 = 1 << (l1pos % BITS_PER_UNIT)
			*l1val |= bit
		}
	}
}

func (b *BitmapAllocator) markLevel1(start, length uint64, free bool) {
	if start%UNITSET_BYTES != 0 {
		panic(any("start align error"))
	} else if length%UNITSET_BYTES != 0 {
		panic(any("length align error"))
	}
	idx := uint64(0)
	idxEnd := (length - start) / BITS_PER_UNITSET
	for {
		if idx >= idxEnd {
			break
		}
		if (start+(idx+1)*BITS_PER_UNITSET)/BITS_PER_UNIT > uint64(len(b.level0)) {
			break
		}
		b.markUnitLevel1(start+idx*BITS_PER_UNITSET, start+(idx+1)*BITS_PER_UNITSET, free)
		idx++
	}
}

func (b *BitmapAllocator) getBitPos(val uint64, start uint32) uint32 {
	var mask uint64 = 1 << start
	for {
		if (start < BITS_PER_UNIT) && (val&mask) == 0 {
			mask <<= 1
			start++
			continue
		}
		break
	}
	return start
}

func (b *BitmapAllocator) CheckAllocations(start uint32, len uint32) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	pos := start / b.pageSize
	end := pos + len/b.pageSize
	b.markAllocFree0(uint64(pos), uint64(end), false)
	l0start := p2align(uint64(pos), BITS_PER_UNITSET)
	l0end := p2roundup(uint64(end), BITS_PER_UNITSET)
	b.markLevel1(l0start, l0end, false)
	b.lastPos = uint64(start)
}

func (b *BitmapAllocator) Free(start uint32, len uint32) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	pos := start / b.pageSize
	end := pos + len/b.pageSize
	b.markAllocFree0(uint64(pos), uint64(end), true)
	l0start := p2align(uint64(pos), BITS_PER_UNITSET)
	l0end := p2roundup(uint64(end), BITS_PER_UNITSET)
	b.markLevel1(l0start, l0end, true)
	b.lastPos = uint64(start)
}

func (b *BitmapAllocator) Allocate(needLen uint64) (uint64, uint64) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	length := p2roundup(needLen, uint64(b.pageSize))
	var allocated uint64 = 0
	l1pos := b.lastPos / uint64(b.pageSize) / BITS_PER_UNITSET / BITS_PER_UNIT
	l1end := cap(b.level1)
	var needPage, allocatedPage, l0freePos, nextPos uint32
	needPage = uint32(length-allocated) / b.pageSize
	allocatedPage = 0
	//pos := b.lastPos / uint64(b.pageSize)
	for ; length > allocated && l1pos < uint64(l1end); l1pos++ {
		l1bit := b.level1[l1pos]
		if b.pageSize == INODE_SIZE {
			l1bit = ALL_UNIT_SET
		}
		if l1bit == ALL_UNIT_CLEAR {
			b.lastPos += BITS_PER_UNITSET * uint64(b.pageSize)
			continue
		}
		// get level1 free start bit
		l1freePos := b.getBitPos(l1bit, 0)
		var startIdx uint32 = 0
		var startPos uint32 = 0
		var setStart bool = false
		for {
			l0pos := l1freePos*BITS_PER_UNITSET + uint32(l1pos*BITS_PER_UNITSET*BITS_PER_UNIT)
			l0end := (l1freePos+1)*BITS_PER_UNITSET + uint32(l1pos*BITS_PER_UNITSET*BITS_PER_UNIT)
			for idx := l0pos / BITS_PER_UNIT; idx < l0end/BITS_PER_UNIT &&
				length > allocated; idx++ {
				if idx >= uint32(len(b.level0)) {
					return 0, 0
				}
				val := &(b.level0[idx])
				if *val == ALL_UNIT_CLEAR {
					// ALL_UNIT_CLEAR needs to be reset
					startIdx = 0
					setStart = false
					allocatedPage = 0
					continue
				}
				//TODO:Need to allocate huge pages to debug
				l0freePos = b.getBitPos(*val, 0)
				nextPos = l0freePos + 1
				if startIdx == 0 && !setStart {
					startIdx = idx
					startPos = l0freePos
					setStart = true
				}
				if setStart {
					if (l0freePos > 0 && allocatedPage > 0) ||
						(l0freePos == 0 && allocatedPage == 1) {
						// (l0freePos > 0 && allocatedPage > 0)
						// Fragmented pages exist during allocation,
						// we only need contiguous pages

						// (l0freePos == 0 && allocatedPage == 1)
						//Represents a page that has not previously met the conditions.
						//Even if the 0th bit of the current idx is an assignable page,
						//"start" needs to be reset.
						allocatedPage = 0
						startIdx = idx
						startPos = l0freePos
					}
				}

				for {
					if nextPos >= BITS_PER_UNIT ||
						allocatedPage >= needPage-1 {
						break
					}
					if (*val & (1 << nextPos)) == 0 {
						l0freePos = b.getBitPos(*val, nextPos+1)
						startPos = l0freePos
						nextPos = l0freePos + 1
						allocatedPage = 0
						startIdx = idx
					} else {
						nextPos++
						allocatedPage++
					}
				}
				allocatedPage++
				if allocatedPage < needPage {
					l0freePos = 0
					nextPos = l0freePos + 1
					continue
				}
				allocated += uint64(needPage * b.pageSize)
				l0start := uint64(startIdx)*BITS_PER_UNIT + uint64(startPos)
				b.lastPos = l0start * uint64(b.pageSize)
				l0end := l0start + uint64(needPage)
				b.markAllocFree0(l0start, l0end, false)
				l0start = p2align(l0start, BITS_PER_UNITSET)
				l0end = p2roundup(l0end, BITS_PER_UNITSET)
				b.markLevel1(l0start, l0end, false)
				offset := b.lastPos
				b.lastPos += allocated
				return offset, allocated
			}
			l1freePos++
			if l1freePos >= BITS_PER_UNIT*BITS_PER_UNIT {
				break
			}
		}
	}
	return 0, 0
}
