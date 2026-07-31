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

// Package cteaccount accounts for batches retained by recursive CTE
// operators. Batch metadata and shared buffers make this deliberately
// approximate: it is an OOM circuit breaker, not byte-exact billing.
package cteaccount

import (
	"context"
	"math"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type Accountant struct {
	budget      *process.CTEMemoryBudget
	reservation *process.CTEMemoryReservation
	retained    uint64
}

type Replacement struct {
	accountant    *Accountant
	ctx           context.Context
	base          uint64
	originalTotal uint64
	active        bool
}

// Bind connects one operator owner to the statement budget. Existing reusable
// cache slots are admitted into a new statement generation at their actual
// allocated size.
func (a *Accountant) Bind(proc *process.Process, cached []*batch.Batch) error {
	budget := proc.GetCTEMemoryBudget()
	if a.reservation != nil && a.budget == budget {
		return nil
	}
	a.Release()
	retained, ok := batchesAllocated(cached)
	if !ok {
		return process.ErrCTEMemoryBudgetInvalid
	}
	reservation, err := budget.Reserve(proc.Ctx, retained)
	if err != nil {
		return err
	}
	a.budget = budget
	a.reservation = reservation
	a.retained = retained
	return nil
}

// BeginReplacement performs admission before copying src into a retained
// cache slot. The source logical payload is added to the current charge because
// the existing slot must remain intact until the replacement commits.
func (a *Accountant) BeginReplacement(ctx context.Context, old, src *batch.Batch) (*Replacement, error) {
	if a.reservation == nil {
		return nil, process.ErrCTEMemoryReservationInactive
	}
	oldBytes := batchAllocated(old)
	if oldBytes > a.retained {
		return nil, process.ErrCTEMemoryBudgetInvalid
	}
	base := a.retained - oldBytes
	estimate := batchLogicalPayload(src)
	target, ok := checkedAdd(a.retained, estimate)
	if !ok {
		return nil, process.ErrCTEMemoryBudgetInvalid
	}
	txn := &Replacement{
		accountant:    a,
		ctx:           ctx,
		base:          base,
		originalTotal: a.retained,
		active:        true,
	}
	if target > a.retained {
		if err := a.reservation.Resize(ctx, target); err != nil {
			return nil, err
		}
	}
	return txn, nil
}

// Commit reconciles the estimate to the allocation retained by cached.
func (r *Replacement) Commit(cached *batch.Batch) error {
	if !r.active || r.accountant == nil || r.accountant.reservation == nil {
		return process.ErrCTEMemoryReservationInactive
	}
	target, ok := checkedAdd(r.base, batchAllocated(cached))
	if !ok {
		return process.ErrCTEMemoryBudgetInvalid
	}
	if err := r.accountant.reservation.Resize(r.ctx, target); err != nil {
		return err
	}
	r.accountant.retained = target
	r.active = false
	return nil
}

// Rollback is used when the old cache slot is still intact.
func (r *Replacement) Rollback() {
	if !r.active || r.accountant == nil || r.accountant.reservation == nil {
		return
	}
	_ = r.accountant.reservation.Resize(r.ctx, r.originalTotal)
	r.accountant.retained = r.originalTotal
	r.active = false
}

// Discard is used after a failed copy or reconcile has made the old slot
// unusable and the caller has fully cleaned that slot.
func (r *Replacement) Discard() {
	if !r.active || r.accountant == nil || r.accountant.reservation == nil {
		return
	}
	_ = r.accountant.reservation.Resize(r.ctx, r.base)
	r.accountant.retained = r.base
	r.active = false
}

func (a *Accountant) Release() {
	if a.reservation != nil {
		a.reservation.Release()
	}
	a.budget = nil
	a.reservation = nil
	a.retained = 0
}

func (a *Accountant) Retained() uint64 {
	return a.retained
}

func batchLogicalPayload(bat *batch.Batch) uint64 {
	if bat == nil || bat.Last() {
		return 0
	}
	size := bat.Size()
	if size <= 0 {
		return 0
	}
	return uint64(size)
}

func batchAllocated(bat *batch.Batch) uint64 {
	if bat == nil || bat.Last() {
		return 0
	}
	allocated := bat.Allocated()
	if allocated <= 0 {
		return 0
	}
	return uint64(allocated)
}

func batchesAllocated(bats []*batch.Batch) (uint64, bool) {
	var total uint64
	for _, bat := range bats {
		var ok bool
		total, ok = checkedAdd(total, batchAllocated(bat))
		if !ok {
			return 0, false
		}
	}
	return total, true
}

func checkedAdd(left, right uint64) (uint64, bool) {
	if right > math.MaxUint64-left {
		return 0, false
	}
	return left + right, true
}
