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

package cteaccount

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// DefaultMemQuotaBytes is the fallback quota used when the session variable
// cannot be resolved. Matches the default of @@cte_max_memory_bytes.
const DefaultMemQuotaBytes int64 = 1 << 30

// QuotaVarName is the session variable that controls the per-operator quota.
// 0 disables the quota (escape hatch for operators).
const QuotaVarName = "cte_max_memory_bytes"

// Accountant tracks the retained-batch byte footprint of a recursive CTE
// operator and rejects writes that would push it past the resolved quota.
//
// Not safe for concurrent use; intended to be owned by a single operator
// instance. The quota is resolved lazily on the first Account call and cached
// for the operator's lifetime, so a SET of @@cte_max_memory_bytes mid-query
// takes effect only at the next Reset.
//
// Concurrency: the recursive CTE merge operator funnels parallel feeders through
// a single-threaded Call, so no mutex is needed. A future refactor that lifts
// the merge must introduce synchronization.
//
// Accuracy: Vector.Size() is approximate for variable-width types and aliased
// buffers (see vector.go:188), so the quota is approximate-only — sufficient
// for OOM prevention but not for byte-precise accounting.
//
// Scoping: quota is per-operator, per-CN. Two CTEs in one query each have an
// independent Accountant. With N CNs the cluster-wide bound is
// N × cte_max_memory_bytes.
type Accountant struct {
	totalBytes    int64
	memQuotaBytes int64
	resolved      bool
}

// Account records that prev (which may be nil) is being replaced by next in
// the operator's retained-batch slot. It returns ErrCteMemoryQuotaExceeded if
// the resulting total would exceed the resolved quota; in that case the
// internal counter is left untouched so callers can surface the error without
// poisoning later accounting.
//
// Sentinel "last marker" batches carry no payload and are excluded from the
// Reset baseline; both next and prev are skipped here so totalBytes cannot
// drift each Reset cycle.
func (a *Accountant) Account(proc *process.Process, next, prev *batch.Batch) error {
	if next == nil || next.Last() {
		return nil
	}
	if !a.resolved {
		a.resolved = true
		a.memQuotaBytes = ResolveQuotaBytes(proc)
	}

	newBytes := int64(next.Size())
	var oldBytes int64
	if prev != nil && !prev.Last() {
		oldBytes = int64(prev.Size())
	}
	delta := newBytes - oldBytes

	if a.memQuotaBytes > 0 && delta > 0 {
		projected := a.totalBytes + delta
		if projected > a.memQuotaBytes {
			return moerr.NewErrCteMemoryQuotaExceeded(proc.Ctx, projected, a.memQuotaBytes)
		}
	}
	a.totalBytes += delta
	return nil
}

// AccountSlot is the operator-side helper: prev is whatever batch currently
// occupies slot i in the operator's retained-batch slice (nil if i is past
// the end). Centralized here so MergeCTE and MergeRecursive don't drift.
func (a *Accountant) AccountSlot(proc *process.Process, slots []*batch.Batch, i int, next *batch.Batch) error {
	var prev *batch.Batch
	if len(slots) > i {
		prev = slots[i]
	}
	return a.Account(proc, next, prev)
}

// SetBaseline forces the running total to bytes and clears the resolved flag
// so the next Account call re-reads @@cte_max_memory_bytes. Operators call
// this from Reset() to seed the total with batches retained across runs.
func (a *Accountant) SetBaseline(bytes int64) {
	a.totalBytes = bytes
	a.memQuotaBytes = 0
	a.resolved = false
}

// Release subtracts the pre-clean size of bat from totalBytes. Callers must
// call Release before freeing the data (CleanOnlyData / Clean) so totalBytes
// stays consistent. Sentinels and nil batches are no-ops.
func (a *Accountant) Release(bat *batch.Batch) {
	if bat == nil || bat.Last() {
		return
	}
	a.totalBytes -= int64(bat.Size())
	if a.totalBytes < 0 {
		a.totalBytes = 0
	}
}

func (a *Accountant) TotalBytes() int64 { return a.totalBytes }

// QuotaBytes returns 0 when the quota has not been resolved yet or is disabled.
func (a *Accountant) QuotaBytes() int64 { return a.memQuotaBytes }

func (a *Accountant) Resolved() bool { return a.resolved }

// ResolveQuotaBytes reads QuotaVarName from the session, falling back to
// DefaultMemQuotaBytes whenever the variable is unavailable, errors out, has
// an unexpected type, or is negative. A value of 0 is honored and disables
// the quota.
func ResolveQuotaBytes(proc *process.Process) int64 {
	resolveFunc := proc.GetResolveVariableFunc()
	if resolveFunc == nil {
		return DefaultMemQuotaBytes
	}
	val, err := resolveFunc(QuotaVarName, true, false)
	if err != nil {
		return DefaultMemQuotaBytes
	}
	v, ok := val.(int64)
	if !ok {
		return DefaultMemQuotaBytes
	}
	if v < 0 {
		return DefaultMemQuotaBytes
	}
	return v
}
