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

	mu           sync.Mutex
	slabs        []*_Slab
	maxSlabs     int
	freeSlabs    []*_Slab
	maxFreeSlabs int

	deallocatorPool *ClosureDeallocatorPool[fixedSizeMmapDeallocatorArgs, *fixedSizeMmapDeallocatorArgs]
}

type fixedSizeMmapDeallocatorArgs struct {
	slab *_Slab
	ptr  unsafe.Pointer
}

func (f fixedSizeMmapDeallocatorArgs) As(trait Trait) bool {
	//if info, ok := trait.(*MmapInfo); ok {
	//	info.Addr = f.ptr
	//	info.Length = f.length
	//	return true
	//}
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
	maxStandbySlabs = 1024
	maxStandbyBytes = 128 * MB
)

func NewFixedSizeMmapAllocator(
	size uint64,
) (ret *fixedSizeMmapAllocator) {

	ret = &fixedSizeMmapAllocator{
		size: size,
		maxSlabs: min(
			maxActiveSlabs,
			maxActiveBytes/(int(size)*slabCapacity),
		),
		maxFreeSlabs: min(
			maxStandbySlabs,
			maxStandbyBytes/(int(size)*slabCapacity),
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
		slab: slab,
		ptr:  ptr,
	}), nil
}

func (f *fixedSizeMmapAllocator) allocate() (*_Slab, unsafe.Pointer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// from existing
	for _, slab := range f.slabs {
		ptr, ok := slab.allocate()
		if ok {
			return slab, ptr, nil
		}
	}

	// empty or all full
	// from freeSlabs
	if len(f.freeSlabs) > 0 {
		slab := f.freeSlabs[len(f.freeSlabs)-1]
		f.freeSlabs = f.freeSlabs[:len(f.freeSlabs)-1]
		reuseMem(slab.base, slab.objectSize*slabCapacity)
		f.slabs = append(f.slabs, slab)
		ptr, _ := slab.allocate()
		return slab, ptr, nil
	}

	// allocate new
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
	f.slabs = append(f.slabs, slab)

	ptr, _ := slab.allocate()
	return slab, ptr, nil
}

func (f *fixedSizeMmapAllocator) freeSlab(slab *_Slab) {
	f.mu.Lock() // to prevent new allocation
	defer f.mu.Unlock()

	if len(f.slabs) < f.maxSlabs {
		return
	}

	if slab.mask.Load() != 0 {
		// has new allocation
		return
	}

	offset := -1
	for i, s := range f.slabs {
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

	// move to freeSlabs
	f.slabs = slices.Delete(f.slabs, offset, offset+1)
	f.freeSlabs = append(f.freeSlabs, slab)

	for len(f.freeSlabs) > f.maxFreeSlabs {
		slab := f.freeSlabs[len(f.freeSlabs)-1]
		f.freeSlabs = f.freeSlabs[:len(f.freeSlabs)-1]
		unix.Munmap(
			unsafe.Slice(
				(*byte)(slab.base),
				slab.objectSize*slabCapacity,
			),
		)
	}

}

const slabCapacity = 64 // uint64 masked

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
