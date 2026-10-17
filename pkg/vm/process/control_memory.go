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
	"errors"

	"github.com/matrixorigin/matrixone/pkg/common/system"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"
)

type controlMemoryAdmissionKey struct{}

// WithControlMemoryAdmission marks a narrowly scoped authentication or task
// control statement. It does not exempt allocations from accounting or hard
// domain limits. Callers must not use it for arbitrary internal/user queries.
func WithControlMemoryAdmission(ctx context.Context, enabled bool) context.Context {
	return context.WithValue(ctx, controlMemoryAdmissionKey{}, enabled)
}

func HasControlMemoryAdmission(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	marked, _ := ctx.Value(controlMemoryAdmissionKey{}).(bool)
	return marked
}

func (b *ExecutionResourceBudget) openControlProcessGeneration(id, memoryCap, spillDiskCap uint64, control bool) (*ExecutionResourceGeneration, error) {
	return b.openControlProcessGenerationWithEntry(id, memoryCap, spillDiskCap, control, nil)
}

func (b *ExecutionResourceBudget) openControlProcessGenerationWithEntry(id, memoryCap, spillDiskCap uint64, control bool, entry *system.MemoryControlLease) (*ExecutionResourceGeneration, error) {
	g, err := b.openProcessGeneration(id, memoryCap, spillDiskCap)
	if err != nil || !control {
		return g, err
	}
	b.mu.Lock()
	ledger := b.physicalLedger
	b.mu.Unlock()
	if ledger == nil {
		return g, nil
	}
	lease := entry
	borrowed := entry != nil
	ok := borrowed
	if !borrowed {
		lease, ok = ledger.AcquireControl()
	}
	if !ok {
		// A fifth control statement (or a domain without runtime allowance)
		// may still execute against the ordinary, fully accounted budget.
		return g, nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		if !borrowed {
			lease.Close()
		}
		g.closed = true
		return nil, ErrExecutionResourceClosed
	}
	// Healthy control queries retain ordinary query limits. Use the small
	// reserve only when the ordinary physical or CN margin cannot support it;
	// otherwise large catalogs would regress merely because login is marked.
	allowance := min(g.cap, lease.Limit())
	available, measured := ledger.Available()
	if !borrowed && measured && available >= allowance && b.aggregateUsed <= b.aggregateCap &&
		b.aggregateCap-b.aggregateUsed >= allowance {
		lease.Close()
		return g, nil
	}
	g.controlMemory = lease
	g.controlMemoryBorrowed = borrowed
	g.cap = allowance
	return g, nil
}

// EnterStatementMemoryAdmission runs before transaction creation/planning.
// Unit fixtures explicitly select their private ledger; real processes do not
// acquire a second CN budget or precharge bytes merely to enter a statement.
func (proc *Process) EnterStatementMemoryAdmission(ctx context.Context) (context.Context, *system.MemoryStatementLease, error) {
	if proc == nil || proc.Base == nil {
		return ctx, nil, ErrExecutionResourceInvalid
	}
	proc.Base.executionResourceBudgetMu.Lock()
	isolated := proc.Base.executionResourceBudgetIsolatedTest
	g := proc.Base.executionResourceBudget
	proc.Base.executionResourceBudgetMu.Unlock()
	if isolated {
		return ctx, nil, nil
	}
	ledger := serviceMemoryAdmissionBudget(proc.GetService())
	if g != nil {
		g.budget.mu.Lock()
		ledger = g.budget.physicalLedger
		g.budget.mu.Unlock()
		if ledger == nil { // explicit private generation installed by a fixture
			return ctx, nil, nil
		}
	}
	next, lease, err := ledger.EnterStatement(ctx, HasControlMemoryAdmission(ctx))
	if err != nil {
		return ctx, nil, errors.Join(ErrExecutionResourceAdmission, errutil.Wrap(err, "statement entry"))
	}
	return next, lease, nil
}

func (g *ExecutionResourceGeneration) releasePhysicalMemory(size uint64) {
	if g.controlMemory != nil {
		g.controlMemory.Release(size)
	} else if g.budget.physicalLedger != nil {
		g.budget.physicalLedger.Release(size)
	}
}
