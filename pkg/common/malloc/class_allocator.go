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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const (
	maxClassSize = 32 * GB
)

type ClassAllocator[T FixedSizeAllocator] struct {
	classes []Class[T]
}

type Class[T FixedSizeAllocator] struct {
	size      uint64
	allocator T
}

func NewClassAllocator[T FixedSizeAllocator](
	newAllocator func(size uint64) T,
) *ClassAllocator[T] {
	ret := &ClassAllocator[T]{}

	// init classes
	for size := uint64(1); size <= maxClassSize; size *= 2 {
		ret.classes = append(ret.classes, Class[T]{
			size:      size,
			allocator: newAllocator(size),
		})
	}

	return ret
}

var _ Allocator = new(ClassAllocator[*fixedSizeMmapAllocator])

// ClassAllocationSize reports the backing allocation used by ClassAllocator
// for a request. Callers which account memory before allocating can use the
// same size-class contract instead of under-counting the requested slice as
// though it were the backing allocation.
func ClassAllocationSize(size uint64) (uint64, bool) {
	if size == 0 {
		return 0, false
	}
	var classSize uint64
	if bits.OnesCount64(size) > 1 {
		shift := bits.Len64(size)
		if shift >= 64 {
			return 0, false
		}
		classSize = uint64(1) << shift
	} else {
		classSize = size
	}
	if classSize > maxClassSize {
		return 0, false
	}
	return classSize, true
}

func (c *ClassAllocator[T]) Allocate(size uint64, hints Hints) ([]byte, Deallocator, error) {
	classSize, ok := ClassAllocationSize(size)
	if !ok {
		if size == 0 {
			return nil, nil, moerr.NewInternalErrorNoCtx("invalid allocate size: 0")
		}
		return nil, nil, moerr.NewInternalErrorNoCtxf("cannot allocate %v bytes: too large", size)
	}
	i := bits.TrailingZeros64(classSize)
	slice, dec, err := c.classes[i].allocator.Allocate(hints, size)
	if err != nil {
		return nil, nil, err
	}
	slice = slice[:size]
	return slice, dec, nil
}
