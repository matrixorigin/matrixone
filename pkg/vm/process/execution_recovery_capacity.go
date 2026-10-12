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

package process

import (
	"errors"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
)

// ExecutionRecoveryCapacity owns query/CN headroom which physical recovery
// allocations borrow through an allocation-account capacity class. The
// physical allocation remains the sole allocation-ledger owner; borrowing
// prevents the same bytes from being charged to the shared budget twice.
type ExecutionRecoveryCapacity struct {
	mu sync.Mutex

	generation *ExecutionResourceGeneration
	capacity   uint64
	borrowed   uint64
	closed     bool
}

func NewExecutionRecoveryCapacity(
	generation *ExecutionResourceGeneration,
) (*ExecutionRecoveryCapacity, error) {
	if generation == nil || generation.budget == nil || generation.Closed() {
		return nil, ErrExecutionResourceInvalid
	}
	return &ExecutionRecoveryCapacity{generation: generation}, nil
}

// NewExecutionRecoveryCapacitySlot creates an inactive controller whose stable
// address can be registered with an allocation account before an execution
// attempt starts. Activate binds the slot to that attempt. Keeping registration
// in the allocation-owner lifecycle avoids rebuilding controller metadata in
// every operator Prepare while preserving the same per-attempt capacity floor.
func NewExecutionRecoveryCapacitySlot() *ExecutionRecoveryCapacity {
	return &ExecutionRecoveryCapacity{closed: true}
}

// Activate binds an inactive slot to one execution generation. A live slot may
// only be activated again with the same generation; Close must first drain it
// before it can be reused by another attempt.
func (c *ExecutionRecoveryCapacity) Activate(
	generation *ExecutionResourceGeneration,
) error {
	if c == nil || generation == nil || generation.budget == nil || generation.Closed() {
		return ErrExecutionResourceInvalid
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.generation != nil && !c.closed {
		if c.generation == generation {
			return nil
		}
		return ErrExecutionResourceInvalid
	}
	if c.capacity != 0 || c.borrowed != 0 {
		return mpool.ErrAllocationAccountLive
	}
	c.generation = generation
	c.closed = false
	return nil
}

// EnsureCapacity raises the reusable recovery floor before an operator retains
// state which may later need that floor to make spill progress.
func (c *ExecutionRecoveryCapacity) EnsureCapacity(target uint64) error {
	if c == nil {
		return ErrExecutionResourceInvalid
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.generation == nil {
		return ErrExecutionSpillReservationInactive
	}
	if target <= c.capacity {
		return nil
	}
	delta := target - c.capacity
	if err := c.generation.acquireRecoveryCapacity(delta); err != nil {
		return errors.Join(mpool.ErrAllocationAccountCapacity, err)
	}
	c.capacity = target
	return nil
}

func (c *ExecutionRecoveryCapacity) AcquireAllocationCapacity(size uint64) error {
	if size == 0 {
		return nil
	}
	if c == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.generation == nil {
		return errors.Join(
			mpool.ErrAllocationAccountSealed,
			ErrExecutionSpillReservationInactive,
		)
	}
	if c.borrowed > c.capacity || size > c.capacity-c.borrowed {
		delta := size
		if c.borrowed <= c.capacity {
			delta = size - (c.capacity - c.borrowed)
		}
		if err := c.generation.acquireRecoveryCapacity(delta); err != nil {
			return errors.Join(mpool.ErrAllocationAccountCapacity, err)
		}
		c.capacity += delta
	}
	if err := c.generation.borrowRecoveryCapacity(size); err != nil {
		if errors.Is(err, ErrExecutionResourceClosed) {
			return errors.Join(mpool.ErrAllocationAccountSealed, err)
		}
		return errors.Join(mpool.ErrAllocationAccountInvariant, err)
	}
	c.borrowed += size
	return nil
}

func (c *ExecutionRecoveryCapacity) ReleaseAllocationCapacity(size uint64) {
	if size == 0 {
		return
	}
	if c == nil {
		panic("nil execution recovery capacity")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if size > c.borrowed {
		panic("execution recovery capacity release underflow")
	}
	c.borrowed -= size
	c.generation.returnRecoveryCapacity(size)
}

func (c *ExecutionRecoveryCapacity) Snapshot() (capacity, borrowed uint64) {
	if c == nil {
		return 0, 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.capacity, c.borrowed
}

// TrimUnusedCapacity releases the part of the pre-admitted floor which is not
// currently backing a physical allocation. Operators call this after a
// recovery/finalization phase completes so conservative peak headroom does not
// remain unavailable to downstream operators. Borrowed physical allocations
// keep their exact charge and may return it to this floor later.
func (c *ExecutionRecoveryCapacity) TrimUnusedCapacity() (uint64, error) {
	if c == nil {
		return 0, ErrExecutionResourceInvalid
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.generation == nil {
		return 0, ErrExecutionSpillReservationInactive
	}
	if c.borrowed > c.capacity {
		return 0, mpool.ErrAllocationAccountInvariant
	}
	unused := c.capacity - c.borrowed
	if unused != 0 {
		c.generation.releaseRecoveryCapacity(unused)
		c.capacity = c.borrowed
	}
	return c.capacity, nil
}

// Close releases the recovery floor only after all physical borrowers have
// returned it. A failed close keeps ownership intact for terminal diagnostics.
func (c *ExecutionRecoveryCapacity) Close() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	if c.borrowed != 0 {
		return mpool.ErrAllocationAccountLive
	}
	if c.capacity != 0 {
		c.generation.releaseRecoveryCapacity(c.capacity)
	}
	c.capacity = 0
	c.generation = nil
	c.closed = true
	return nil
}

var _ mpool.AllocationCapacityController = (*ExecutionRecoveryCapacity)(nil)
