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
	"math"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const (
	CTEMemoryQuotaVariable     = "cte_max_memory_bytes"
	DefaultCTEMemoryQuotaBytes = uint64(1 << 30)
	MaximumCTEMemoryQuotaBytes = uint64(1 << 40)
)

var (
	ErrCTEMemoryBudgetClosed        = moerr.NewInternalErrorNoCtx("recursive CTE memory budget is closed")
	ErrCTEMemoryBudgetInvalid       = moerr.NewInternalErrorNoCtx("invalid recursive CTE memory budget accounting")
	ErrCTEMemoryReservationInactive = moerr.NewInternalErrorNoCtx("recursive CTE memory reservation is inactive")
)

// CTEMemoryBudget is a statement-generation budget shared by every child
// Process. It accounts only for batches retained by recursive CTE operators;
// it is an OOM circuit breaker, not byte-exact query memory accounting.
type CTEMemoryBudget struct {
	mu     sync.Mutex
	limit  uint64
	used   uint64
	closed bool
}

// CTEMemoryReservation is the charge owned by one recursive CTE operator.
type CTEMemoryReservation struct {
	budget *CTEMemoryBudget
	bytes  uint64
	active bool
}

func NewCTEMemoryBudget(limit uint64) *CTEMemoryBudget {
	return &CTEMemoryBudget{limit: limit}
}

func (b *CTEMemoryBudget) Reserve(ctx context.Context, bytes uint64) (*CTEMemoryReservation, error) {
	if b == nil {
		return nil, ErrCTEMemoryBudgetInvalid
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, ErrCTEMemoryBudgetClosed
	}
	projected, ok := checkedAddUint64(b.used, bytes)
	if !ok {
		return nil, ErrCTEMemoryBudgetInvalid
	}
	if b.limit != 0 && projected > b.limit {
		return nil, moerr.NewCteMemoryQuotaExceeded(ctx, projected, b.limit)
	}
	b.used = projected
	return &CTEMemoryReservation{budget: b, bytes: bytes, active: true}, nil
}

func (r *CTEMemoryReservation) Resize(ctx context.Context, bytes uint64) error {
	if r == nil || r.budget == nil {
		return ErrCTEMemoryReservationInactive
	}
	b := r.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	if !r.active {
		return ErrCTEMemoryReservationInactive
	}
	if b.closed {
		return ErrCTEMemoryBudgetClosed
	}
	if bytes == r.bytes {
		return nil
	}
	if bytes < r.bytes {
		delta := r.bytes - bytes
		if delta > b.used {
			return ErrCTEMemoryBudgetInvalid
		}
		b.used -= delta
		r.bytes = bytes
		return nil
	}
	delta := bytes - r.bytes
	projected, ok := checkedAddUint64(b.used, delta)
	if !ok {
		return ErrCTEMemoryBudgetInvalid
	}
	if b.limit != 0 && projected > b.limit {
		return moerr.NewCteMemoryQuotaExceeded(ctx, projected, b.limit)
	}
	b.used = projected
	r.bytes = bytes
	return nil
}

func (r *CTEMemoryReservation) Release() {
	if r == nil || r.budget == nil {
		return
	}
	b := r.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	if !r.active {
		return
	}
	if !b.closed {
		if r.bytes <= b.used {
			b.used -= r.bytes
		} else {
			b.used = 0
		}
	}
	r.bytes = 0
	r.active = false
}

func (r *CTEMemoryReservation) Bytes() uint64 {
	if r == nil || r.budget == nil {
		return 0
	}
	b := r.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	if !r.active {
		return 0
	}
	return r.bytes
}

func (r *CTEMemoryReservation) Budget() *CTEMemoryBudget {
	if r == nil {
		return nil
	}
	return r.budget
}

func (b *CTEMemoryBudget) Close() {
	if b == nil {
		return
	}
	b.mu.Lock()
	b.closed = true
	b.used = 0
	b.mu.Unlock()
}

func (b *CTEMemoryBudget) Snapshot() (limit, used uint64, closed bool) {
	if b == nil {
		return 0, 0, true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.limit, b.used, b.closed
}

func (proc *Process) GetCTEMemoryBudget() *CTEMemoryBudget {
	if proc == nil || proc.Base == nil {
		return NewCTEMemoryBudget(DefaultCTEMemoryQuotaBytes)
	}
	proc.Base.cteMemoryBudgetMu.Lock()
	defer proc.Base.cteMemoryBudgetMu.Unlock()
	if proc.Base.cteMemoryBudget != nil {
		return proc.Base.cteMemoryBudget
	}
	limit := DefaultCTEMemoryQuotaBytes
	if resolve := proc.GetResolveVariableFunc(); resolve != nil {
		if value, err := resolve(CTEMemoryQuotaVariable, true, false); err == nil {
			if configured, ok := value.(int64); ok && configured >= 0 && uint64(configured) <= MaximumCTEMemoryQuotaBytes {
				limit = uint64(configured)
			}
		}
	}
	proc.Base.cteMemoryBudget = NewCTEMemoryBudget(limit)
	return proc.Base.cteMemoryBudget
}

func checkedAddUint64(left, right uint64) (uint64, bool) {
	if right > math.MaxUint64-left {
		return 0, false
	}
	return left + right, true
}
