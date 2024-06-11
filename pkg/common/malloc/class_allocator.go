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
	"fmt"
	"math/bits"
	"unsafe"

	"golang.org/x/sys/unix"
)

const (
	MB = 1 << 20
	GB = 1 << 30

	// There are 14 classes under this threshold,
	// so the default sharded allocator will consume less than 14MB per CPU core.
	// On a 256 core machine, it will be 3584MB, not too high.
	// We may tune this if required.
	smallClassCap = 1 * MB

	// A large class size will not consume more memory unless we actually allocate on that class
	// Classes with size larger than smallClassCap will always buffering MADV_DONTNEED-advised objects.
	// MADV_DONTNEED-advised objects consume no memory.
	maxClassSize = 32 * GB

	maxBuffer1Cap = 256
	// objects in buffer2 will be MADV_DONTNEED-advised and will not occupy RSS, so it's safe to use a large number
	buffer2Cap = 1024
)

type ClassAllocator struct {
	classes []Class
}

type Class struct {
	size      uint64
	allocator *fixedSizeMmapAllocator
}

func NewClassAllocator(
	checkFraction *uint32,
) *ClassAllocator {
	ret := &ClassAllocator{}

	// init classes
	for size := uint64(1); size <= maxClassSize; size *= 2 {
		ret.classes = append(ret.classes, Class{
			size:      size,
			allocator: newFixedSizedAllocator(size, checkFraction),
		})
	}

	return ret
}

var _ Allocator = new(ClassAllocator)

func (c *ClassAllocator) Allocate(size uint64) (unsafe.Pointer, Deallocator) {
	if size == 0 {
		panic("invalid size: 0")
	}
	// class size factor is 2, so we can calculate the class index
	var i int
	if bits.OnesCount64(size) > 1 {
		// round to next bucket
		i = bits.Len64(size)
	} else {
		// power of two
		i = bits.Len64(size) - 1
	}
	if i >= len(c.classes) {
		panic(fmt.Sprintf("cannot allocate %v bytes: too large", size))
	}
	return c.classes[i].allocator.Allocate(size)
}

type fixedSizeMmapAllocator struct {
	size          uint64
	checkFraction *uint32
	// buffer1 buffers objects
	buffer1 chan unsafe.Pointer
	// buffer2 buffers MADV_DONTNEED objects
	buffer2 chan unsafe.Pointer
}

func newFixedSizedAllocator(
	size uint64,
	checkFraction *uint32,
) *fixedSizeMmapAllocator {
	// if size is larger than smallClassCap, num1 will be zero, buffer1 will be empty
	num1 := smallClassCap / size
	if num1 > maxBuffer1Cap {
		// don't buffer too much, since chans with larger buffer consume more memory
		num1 = maxBuffer1Cap
	}
	ret := &fixedSizeMmapAllocator{
		size:          size,
		checkFraction: checkFraction,
		buffer1:       make(chan unsafe.Pointer, num1),
		buffer2:       make(chan unsafe.Pointer, buffer2Cap),
	}
	return ret
}

var _ Allocator = new(fixedSizeMmapAllocator)

func (f *fixedSizeMmapAllocator) Allocate(_ uint64) (ptr unsafe.Pointer, dec Deallocator) {
	if f.checkFraction != nil {
		defer func() {
			if fastrand()%*f.checkFraction == 0 {
				dec = newCheckedDeallocator(dec)
			}
		}()
	}

	select {

	case ptr := <-f.buffer1:
		// from buffer1
		clear(unsafe.Slice((*byte)(ptr), f.size))
		return ptr, f

	default:

		select {

		case ptr := <-f.buffer2:
			// from buffer2
			f.reuseMem(ptr)
			return ptr, f

		default:
			// allocate new
			data, err := unix.Mmap(
				-1, 0,
				int(f.size),
				unix.PROT_READ|unix.PROT_WRITE,
				unix.MAP_PRIVATE|unix.MAP_ANONYMOUS,
			)
			if err != nil {
				panic(fmt.Sprintf("error %v, size %v",
					err, f.size,
				))
			}
			return unsafe.Pointer(unsafe.SliceData(data)), f

		}

	}
}

var _ Deallocator = new(fixedSizeMmapAllocator)

func (f *fixedSizeMmapAllocator) Deallocate(ptr unsafe.Pointer) {

	if f.checkFraction != nil &&
		fastrand()%*f.checkFraction == 0 {
		// do not reuse to detect use-after-free
		if err := unix.Munmap(
			unsafe.Slice((*byte)(ptr), f.size),
		); err != nil {
			panic(err)
		}
		return
	}

	select {

	case f.buffer1 <- ptr:
		// buffer in buffer1

	default:

		f.freeMem(ptr)

		select {

		case f.buffer2 <- ptr:

		default:
			// unmap
			if err := unix.Munmap(
				unsafe.Slice((*byte)(ptr), f.size),
			); err != nil {
				panic(err)
			}

		}

	}

}
