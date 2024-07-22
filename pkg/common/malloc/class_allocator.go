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

func (c *ClassAllocator[T]) Allocate(size uint64, hints Hints) ([]byte, Deallocator, error) {
	if size == 0 {
		return nil, nil, moerr.NewInternalErrorNoCtx("invalid allocate size: 0")
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
		return nil, nil, moerr.NewInternalErrorNoCtx("cannot allocate %v bytes: too large", size)
	}
	slice, dec, err := c.classes[i].allocator.Allocate(hints)
	if err != nil {
		return nil, nil, err
	}
	slice = slice[:size]
	return slice, dec, nil
}
