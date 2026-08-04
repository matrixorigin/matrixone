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

// HashBuildRecoveryCapacity owns query/CN headroom which physical recovery
// allocations borrow through an allocation-account capacity class. The
// physical allocation remains the sole allocation-ledger owner; borrowing
// prevents the same bytes from being charged to the shared budget twice.
type HashBuildRecoveryCapacity struct {
	mu sync.Mutex

	generation *HashBuildBudgetGeneration
	capacity   uint64
	borrowed   uint64
	closed     bool
}

func NewHashBuildRecoveryCapacity(
	generation *HashBuildBudgetGeneration,
) (*HashBuildRecoveryCapacity, error) {
	if generation == nil || generation.budget == nil || generation.Closed() {
		return nil, ErrHashBuildBudgetInvalid
	}
	return &HashBuildRecoveryCapacity{generation: generation}, nil
}

// EnsureCapacity raises the reusable recovery floor before HashBuild retains
// a source which may later need that floor to make spill progress.
func (c *HashBuildRecoveryCapacity) EnsureCapacity(target uint64) error {
	if c == nil {
		return ErrHashBuildBudgetInvalid
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.generation == nil {
		return ErrHashBuildSpillReservationInactive
	}
	if target <= c.capacity {
		return nil
	}
	delta := target - c.capacity
	if err := c.generation.acquireMemory(delta); err != nil {
		return errors.Join(mpool.ErrAllocationAccountCapacity, err)
	}
	c.capacity = target
	return nil
}

func (c *HashBuildRecoveryCapacity) AcquireAllocationCapacity(size uint64) error {
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
			ErrHashBuildSpillReservationInactive,
		)
	}
	if c.borrowed > c.capacity || size > c.capacity-c.borrowed {
		delta := size
		if c.borrowed <= c.capacity {
			delta = size - (c.capacity - c.borrowed)
		}
		if err := c.generation.acquireMemory(delta); err != nil {
			return errors.Join(mpool.ErrAllocationAccountCapacity, err)
		}
		c.capacity += delta
	}
	c.borrowed += size
	return nil
}

func (c *HashBuildRecoveryCapacity) ReleaseAllocationCapacity(size uint64) {
	if size == 0 {
		return
	}
	if c == nil {
		panic("nil hash build recovery capacity")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if size > c.borrowed {
		panic("hash build recovery capacity release underflow")
	}
	c.borrowed -= size
}

func (c *HashBuildRecoveryCapacity) Snapshot() (capacity, borrowed uint64) {
	if c == nil {
		return 0, 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.capacity, c.borrowed
}

// Close releases the recovery floor only after all physical borrowers have
// returned it. A failed close keeps ownership intact for terminal diagnostics.
func (c *HashBuildRecoveryCapacity) Close() error {
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
		c.generation.ReleaseAllocationCapacity(c.capacity)
	}
	c.capacity = 0
	c.generation = nil
	c.closed = true
	return nil
}

var _ mpool.AllocationCapacityController = (*HashBuildRecoveryCapacity)(nil)
