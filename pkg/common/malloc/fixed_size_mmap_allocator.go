// Copyright 2024 Matrix Origin
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

package malloc

import (
	"math/bits"
	"slices"
	"sync"
	"sync/atomic"
	"unsafe"

	"golang.org/x/sys/unix"
)

type fixedSizeMmapAllocator struct {
	size uint64

	mu              sync.Mutex // it's OK to use mutex since this allocator will be in ShardedAllocator
	activeSlabs     []*_Slab   // active slabs
	maxActiveSlabs  int        // max active slabs
	standbySlabs    []*_Slab   // slabs still mapped but no physical memory backed
	maxStandbySlabs int        // max standby slabs

	deallocatorPool *ClosureDeallocatorPool[fixedSizeMmapDeallocatorArgs, *fixedSizeMmapDeallocatorArgs]
}

type fixedSizeMmapDeallocatorArgs struct {
	slab   *_Slab
	ptr    unsafe.Pointer
	length uint64
}

func (f fixedSizeMmapDeallocatorArgs) As(trait Trait) bool {
	if info, ok := trait.(*MmapInfo); ok {
		info.Addr = f.ptr
		info.Length = f.length
		return true
	}
	return false
}

type MmapInfo struct {
	Addr   unsafe.Pointer
	Length uint64
}

func (*MmapInfo) IsTrait() {}

const (
	maxActiveSlabs  = 256
	maxActiveBytes  = 1 * MB
	minActiveSlabs  = 4
	maxStandbySlabs = 1024
	maxStandbyBytes = 16 * MB
	minStandbySlabs = 4
)

func NewFixedSizeMmapAllocator(
	size uint64,
) (ret *fixedSizeMmapAllocator) {

	ret = &fixedSizeMmapAllocator{
		size: size,

		maxActiveSlabs: max(
			min(
				maxActiveSlabs,
				maxActiveBytes/(int(size)*slabCapacity),
			),
			minActiveSlabs,
		),

		maxStandbySlabs: max(
			min(
				maxStandbySlabs,
				maxStandbyBytes/(int(size)*slabCapacity),
			),
			minStandbySlabs,
		),

		deallocatorPool: NewClosureDeallocatorPool(
			func(hints Hints, args *fixedSizeMmapDeallocatorArgs) {
				empty := args.slab.free(args.ptr)
				if empty {
					ret.freeSlab(args.slab)
				}
			},
		),
	}

	return ret
}

var _ FixedSizeAllocator = new(fixedSizeMmapAllocator)

func (f *fixedSizeMmapAllocator) Allocate(hints Hints, clearSize uint64) ([]byte, Deallocator, error) {
	slab, ptr, err := f.allocate()
	if err != nil {
		return nil, nil, err
	}

	slice := unsafe.Slice(
		(*byte)(ptr),
		f.size,
	)
	if hints&NoClear == 0 {
		clear(slice[:min(clearSize, f.size)])
	}

	return slice, f.deallocatorPool.Get(fixedSizeMmapDeallocatorArgs{
		slab:   slab,
		ptr:    ptr,
		length: f.size,
	}), nil
}

func (f *fixedSizeMmapAllocator) allocate() (*_Slab, unsafe.Pointer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// from existing
	for _, slab := range f.activeSlabs {
		ptr, ok := slab.allocate()
		if ok {
			return slab, ptr, nil
		}
	}

	// empty or all full
	// from standby slabs
	if len(f.standbySlabs) > 0 {
		slab := f.standbySlabs[len(f.standbySlabs)-1]
		f.standbySlabs = f.standbySlabs[:len(f.standbySlabs)-1]
		reuseMem(slab.base, slab.objectSize*slabCapacity)
		f.activeSlabs = append(f.activeSlabs, slab)
		ptr, _ := slab.allocate()
		return slab, ptr, nil
	}

	// allocate new slab
	slice, err := unix.Mmap(
		-1, 0,
		int(f.size*slabCapacity),
		unix.PROT_READ|unix.PROT_WRITE,
		unix.MAP_PRIVATE|unix.MAP_ANONYMOUS,
	)
	if err != nil {
		return nil, nil, err
	}

	base := unsafe.Pointer(unsafe.SliceData(slice))
	slab := &_Slab{
		base:       base,
		objectSize: int(f.size),
	}
	f.activeSlabs = append(f.activeSlabs, slab)

	ptr, _ := slab.allocate()
	return slab, ptr, nil
}

func (f *fixedSizeMmapAllocator) freeSlab(slab *_Slab) {
	f.mu.Lock() // to prevent new allocation
	defer f.mu.Unlock()

	if len(f.activeSlabs) < f.maxActiveSlabs {
		return
	}

	if slab.mask.Load() != 0 {
		// has new allocation
		return
	}

	offset := -1
	for i, s := range f.activeSlabs {
		if s == slab {
			offset = i
			break
		}
	}
	if offset == -1 {
		// already moved
		return
	}

	// free slab memory
	freeMem(slab.base, slab.objectSize*slabCapacity)

	// move to standby slabs
	f.activeSlabs = slices.Delete(f.activeSlabs, offset, offset+1)
	f.standbySlabs = append(f.standbySlabs, slab)

	// unmap standby slabs
	for len(f.standbySlabs) > f.maxStandbySlabs {
		slab := f.standbySlabs[0]
		f.standbySlabs = slices.Delete(f.standbySlabs, 0, 1)
		unix.Munmap(
			unsafe.Slice(
				(*byte)(slab.base),
				slab.objectSize*slabCapacity,
			),
		)
	}

}

const slabCapacity = 64 // uint64 masked, so 64

type _Slab struct {
	base       unsafe.Pointer
	objectSize int
	mask       atomic.Uint64
}

func (s *_Slab) allocate() (unsafe.Pointer, bool) {
	for {
		mask := s.mask.Load()
		reverse := ^mask
		if reverse == 0 {
			// full
			return nil, false
		}
		offset := bits.TrailingZeros64(reverse)
		addr := unsafe.Add(s.base, offset*s.objectSize)
		if s.mask.CompareAndSwap(mask, mask|(1<<offset)) {
			return addr, true
		}
	}
}

func (s *_Slab) free(ptr unsafe.Pointer) bool {
	offset := (uintptr(ptr) - uintptr(s.base)) / uintptr(s.objectSize)
	for {
		mask := s.mask.Load()
		newMask := mask & ^(uint64(1) << offset)
		if s.mask.CompareAndSwap(mask, newMask) {
			return newMask == 0
		}
	}
}
