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
	"unsafe"

	"golang.org/x/sys/unix"
)

const (
	// Classes with smaller size than smallClassCap will buffer min((smallClassCap/size), maxBuffer1Cap) objects in buffer1
	smallClassCap = 1 * MB
	maxBuffer1Cap = 256

	// objects in buffer2 will be MADV_DONTNEED-advised and will not occupy RSS, so it's safe to use a large number
	buffer2Cap = 1024
)

type fixedSizeMmapAllocator struct {
	size uint64
	// buffer1 buffers objects
	buffer1 chan unsafe.Pointer
	// buffer2 buffers MADV_DONTNEED objects
	buffer2 chan unsafe.Pointer
}

func NewFixedSizeMmapAllocator(
	size uint64,
) *fixedSizeMmapAllocator {
	// if size is larger than smallClassCap, num1 will be zero, buffer1 will be empty
	num1 := smallClassCap / size
	if num1 > maxBuffer1Cap {
		// don't buffer too much, since chans with larger buffer consume more memory
		num1 = maxBuffer1Cap
	}
	ret := &fixedSizeMmapAllocator{
		size:    size,
		buffer1: make(chan unsafe.Pointer, num1),
		buffer2: make(chan unsafe.Pointer, buffer2Cap),
	}
	return ret
}

var _ FixedSizeAllocator = new(fixedSizeMmapAllocator)

func (f *fixedSizeMmapAllocator) Allocate(hints Hints) (ptr unsafe.Pointer, dec Deallocator, err error) {

	select {

	case ptr := <-f.buffer1:
		// from buffer1
		if hints&NoClear == 0 {
			clear(unsafe.Slice((*byte)(ptr), f.size))
		}
		return ptr, f, nil

	default:

		select {

		case ptr := <-f.buffer2:
			// from buffer2
			f.reuseMem(ptr, hints)
			return ptr, f, nil

		default:
			// allocate new
			data, err := unix.Mmap(
				-1, 0,
				int(f.size),
				unix.PROT_READ|unix.PROT_WRITE,
				unix.MAP_PRIVATE|unix.MAP_ANONYMOUS,
			)
			if err != nil {
				return nil, nil, err
			}
			return unsafe.Pointer(unsafe.SliceData(data)), f, nil

		}

	}
}

var _ Deallocator = new(fixedSizeMmapAllocator)

func (f *fixedSizeMmapAllocator) Deallocate(ptr unsafe.Pointer, hints Hints) {

	if hints&DoNotReuse > 0 {
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
			// buffer in buffer2

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
