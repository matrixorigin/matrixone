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

// BackingSizeContract identifies an immutable backing-size mapping. Unknown
// values are rejected, which limits sharded allocators to built-in,
// verifiable mappings rather than accepting an arbitrary caller-provided key.
type BackingSizeContract uint8

const (
	backingSizeContractUnknown BackingSizeContract = iota
	BackingSizeContractExact
	BackingSizeContractClass
)

func (c BackingSizeContract) String() string {
	switch c {
	case BackingSizeContractExact:
		return "exact"
	case BackingSizeContractClass:
		return "class"
	default:
		return "unknown"
	}
}

// BackingSizeContracter exposes the stable backing-size mapping of an
// allocator. ShardedAllocator requires this contract so it can validate all
// shards once without memoizing per-request predictions.
type BackingSizeContracter interface {
	BackingSizeContract() (BackingSizeContract, error)
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

func backingSizeContract(allocator Allocator) (BackingSizeContract, error) {
	contracter, ok := allocator.(BackingSizeContracter)
	if !ok {
		return backingSizeContractUnknown, moerr.NewInvalidStateNoCtxf(
			"allocator %T does not report a stable backing-size contract", allocator)
	}
	contract, err := contracter.BackingSizeContract()
	if err != nil {
		return backingSizeContractUnknown, err
	}
	if contract != BackingSizeContractExact && contract != BackingSizeContractClass {
		return backingSizeContractUnknown, moerr.NewInvalidStateNoCtxf(
			"allocator %T reported an unknown backing-size contract", allocator)
	}
	return contract, nil
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

func (*ClassAllocator[T]) BackingSizeContract() (BackingSizeContract, error) {
	return BackingSizeContractClass, nil
}

func (c *CAllocator) BackingSize(size uint64) (uint64, error) {
	return size, nil
}

func (*CAllocator) BackingSizeContract() (BackingSizeContract, error) {
	return BackingSizeContractExact, nil
}

func (s ShardedAllocator[T]) BackingSize(size uint64) (uint64, error) {
	if len(s.shards) == 0 {
		return 0, moerr.NewInternalErrorNoCtx("backing size requested from empty sharded allocator")
	}
	if _, err := s.BackingSizeContract(); err != nil {
		return 0, err
	}
	return BackingSize(s.shards[0].allocator, size)
}

func (s ShardedAllocator[T]) BackingSizeContract() (BackingSizeContract, error) {
	if len(s.shards) == 0 {
		return backingSizeContractUnknown, moerr.NewInternalErrorNoCtx("backing-size contract requested from empty sharded allocator")
	}
	if state := s.backingSizeContractState; state != nil {
		state.once.Do(func() {
			state.contract, state.err = s.validateBackingSizeContract()
		})
		return state.contract, state.err
	}
	return s.validateBackingSizeContract()
}

func (s ShardedAllocator[T]) validateBackingSizeContract() (BackingSizeContract, error) {
	contract, err := backingSizeContract(s.shards[0].allocator)
	if err != nil {
		return backingSizeContractUnknown, err
	}
	for i := 1; i < len(s.shards); i++ {
		shardContract, err := backingSizeContract(s.shards[i].allocator)
		if err != nil {
			return backingSizeContractUnknown, err
		}
		if shardContract != contract {
			return backingSizeContractUnknown, moerr.NewInvalidStateNoCtxf(
				"sharded allocator backing-size contracts differ: shard 0 reports %s, shard %d reports %s",
				contract,
				i,
				shardContract,
			)
		}
	}
	return contract, nil
}

func (m *MetricsAllocator[U]) BackingSize(size uint64) (uint64, error) {
	return BackingSize(m.upstream, size)
}

func (m *MetricsAllocator[U]) BackingSizeContract() (BackingSizeContract, error) {
	return backingSizeContract(m.upstream)
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

func (r *RandomAllocator[A, B]) BackingSizeContract() (BackingSizeContract, error) {
	first, err := backingSizeContract(r.upstream1)
	if err != nil {
		return backingSizeContractUnknown, err
	}
	second, err := backingSizeContract(r.upstream2)
	if err != nil {
		return backingSizeContractUnknown, err
	}
	if first != second {
		return backingSizeContractUnknown, moerr.NewInvalidStateNoCtxf(
			"random allocator backing-size contracts differ: %s and %s", first, second)
	}
	return first, nil
}

func (r *ReadOnlyAllocator[U]) BackingSize(size uint64) (uint64, error) {
	return BackingSize(r.upstream, size)
}

func (r *ReadOnlyAllocator[U]) BackingSizeContract() (BackingSizeContract, error) {
	return backingSizeContract(r.upstream)
}

func (c *CheckedAllocator[U]) BackingSize(size uint64) (uint64, error) {
	return BackingSize(c.upstream, size)
}

func (c *CheckedAllocator[U]) BackingSizeContract() (BackingSizeContract, error) {
	return backingSizeContract(c.upstream)
}

func (p *ProfileAllocator[U]) BackingSize(size uint64) (uint64, error) {
	return BackingSize(p.upstream, size)
}

func (p *ProfileAllocator[U]) BackingSizeContract() (BackingSizeContract, error) {
	return backingSizeContract(p.upstream)
}

func (s *InuseTrackingAllocator[U]) BackingSize(size uint64) (uint64, error) {
	return BackingSize(s.upstream, size)
}

func (s *InuseTrackingAllocator[U]) BackingSizeContract() (BackingSizeContract, error) {
	return backingSizeContract(s.upstream)
}

func (t *LeaksTrackingAllocator[U]) BackingSize(size uint64) (uint64, error) {
	return BackingSize(t.upstream, size)
}

func (t *LeaksTrackingAllocator[U]) BackingSizeContract() (BackingSizeContract, error) {
	return backingSizeContract(t.upstream)
}

func (s *SizeBoundedAllocator[U]) BackingSize(size uint64) (uint64, error) {
	return BackingSize(s.upstream, size)
}

func (s *SizeBoundedAllocator[U]) BackingSizeContract() (BackingSizeContract, error) {
	return backingSizeContract(s.upstream)
}
