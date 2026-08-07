// Copyright 2026 Matrix Origin
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

import "github.com/matrixorigin/matrixone/pkg/common/moerr"

// BackingSizer reports the number of bytes an allocator reserves for a
// requested allocation. Cache admission must use this value before allocating,
// rather than the visible slice length returned by Allocate.
type BackingSizer interface {
	BackingSize(size uint64) (uint64, error)
}

// BackingSize reports the allocator-backed size for a request. Allocators that
// cannot state this contract are rejected instead of silently under-counting
// cache capacity.
func BackingSize(allocator Allocator, size uint64) (uint64, error) {
	if size == 0 {
		return 0, moerr.NewInvalidInputNoCtx("backing size requires a positive request")
	}
	sizer, ok := allocator.(BackingSizer)
	if !ok {
		return 0, moerr.NewInvalidStateNoCtxf(
			"allocator %T does not report backing allocation size", allocator)
	}
	backingSize, err := sizer.BackingSize(size)
	if err != nil {
		return 0, err
	}
	if backingSize < size {
		return 0, moerr.NewInternalErrorNoCtxf(
			"allocator %T reported backing size %d smaller than request %d",
			allocator,
			backingSize,
			size,
		)
	}
	return backingSize, nil
}

func (c *ClassAllocator[T]) BackingSize(size uint64) (uint64, error) {
	backingSize, ok := ClassAllocationSize(size)
	if !ok {
		if size == 0 {
			return 0, moerr.NewInvalidInputNoCtx("backing size requires a positive request")
		}
		return 0, moerr.NewInternalErrorNoCtxf("cannot allocate %d bytes: too large", size)
	}
	return backingSize, nil
}

func (c *CAllocator) BackingSize(size uint64) (uint64, error) {
	return size, nil
}

func (s ShardedAllocator[T]) BackingSize(size uint64) (uint64, error) {
	if len(s) == 0 {
		return 0, moerr.NewInternalErrorNoCtx("backing size requested from empty sharded allocator")
	}
	return BackingSize(s[0].Allocator, size)
}

func (m *MetricsAllocator[U]) BackingSize(size uint64) (uint64, error) {
	return BackingSize(m.upstream, size)
}

func (r *RandomAllocator[A, B]) BackingSize(size uint64) (uint64, error) {
	first, err := BackingSize(r.upstream1, size)
	if err != nil {
		return 0, err
	}
	second, err := BackingSize(r.upstream2, size)
	if err != nil {
		return 0, err
	}
	if first != second {
		return 0, moerr.NewInvalidStateNoCtxf(
			"random allocator backing sizes differ: %d and %d", first, second)
	}
	return first, nil
}

func (r *ReadOnlyAllocator[U]) BackingSize(size uint64) (uint64, error) {
	return BackingSize(r.upstream, size)
}

func (c *CheckedAllocator[U]) BackingSize(size uint64) (uint64, error) {
	return BackingSize(c.upstream, size)
}

func (p *ProfileAllocator[U]) BackingSize(size uint64) (uint64, error) {
	return BackingSize(p.upstream, size)
}

func (s *InuseTrackingAllocator[U]) BackingSize(size uint64) (uint64, error) {
	return BackingSize(s.upstream, size)
}

func (t *LeaksTrackingAllocator[U]) BackingSize(size uint64) (uint64, error) {
	return BackingSize(t.upstream, size)
}

func (s *SizeBoundedAllocator[U]) BackingSize(size uint64) (uint64, error) {
	return BackingSize(s.upstream, size)
}
