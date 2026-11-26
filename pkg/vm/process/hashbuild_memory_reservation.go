// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package process

import (
	"context"
	"sync/atomic"
)

// HashBuildMemoryReservation admits non-MPool storage against the same query/CN
// memory policy as the physical allocation account. It must not be used for
// storage that already has a physical allocation-account lease.
type HashBuildMemoryReservation struct {
	generation *HashBuildBudgetGeneration
	bytes      uint64
	released   atomic.Bool
}

func (g *HashBuildBudgetGeneration) ReserveMemory(ctx context.Context, size uint64) (*HashBuildMemoryReservation, error) {
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	if err := g.acquireMemory(size); err != nil {
		return nil, err
	}
	if err := context.Cause(ctx); err != nil {
		g.ReleaseAllocationCapacity(size)
		return nil, err
	}
	return &HashBuildMemoryReservation{generation: g, bytes: size}, nil
}

func (r *HashBuildMemoryReservation) Release() bool {
	if r == nil || !r.released.CompareAndSwap(false, true) {
		return false
	}
	r.generation.ReleaseAllocationCapacity(r.bytes)
	return true
}
