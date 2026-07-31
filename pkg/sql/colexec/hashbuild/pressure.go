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

package hashbuild

import (
	"errors"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// MemoryPressureReason is the single classification used by HashBuild and all
// spilled join consumers. Only Capacity may enter reclaim/spill/reduce/degrade
// control flow. Sealed and invariant failures are lifecycle bugs and must
// remain terminal. HashBuildBudgetError now exposes disjoint lifecycle and
// capacity identities; this classifier is the one control-flow boundary for
// physical-allocation and spill-resource failures.
type MemoryPressureReason uint8

const (
	MemoryPressureNone MemoryPressureReason = iota
	MemoryPressureCapacity
	MemoryPressureSealed
	MemoryPressureMismatch
	MemoryPressureAllocatorLimit
	MemoryPressureInvariant
	MemoryPressureInvalid
	MemoryPressureMinimumUnit
	MemoryPressureSpillDiskLimit
	MemoryPressureSpillFDLimit
)

func MemoryPressureReasonOf(err error) MemoryPressureReason {
	if err == nil {
		return MemoryPressureNone
	}
	var minimum *MinimumAllocationPressureError
	if errors.As(err, &minimum) {
		return MemoryPressureMinimumUnit
	}

	var budgetErr *process.HashBuildBudgetError
	if errors.As(err, &budgetErr) {
		switch budgetErr.Kind {
		case process.HashBuildBudgetErrorAdmission:
			switch budgetErr.Component {
			case process.HashBuildBudgetComponentMemory:
				return MemoryPressureCapacity
			case process.HashBuildBudgetComponentSpillDisk:
				return MemoryPressureSpillDiskLimit
			case process.HashBuildBudgetComponentSpillFD:
				return MemoryPressureSpillFDLimit
			default:
				return MemoryPressureInvalid
			}
		case process.HashBuildBudgetErrorClosed:
			return MemoryPressureSealed
		case process.HashBuildBudgetErrorInvalid,
			process.HashBuildBudgetErrorCeilingMissing:
			return MemoryPressureInvalid
		default:
			return MemoryPressureInvalid
		}
	}

	switch mpool.AllocationFailureReasonOf(err) {
	case mpool.AllocationFailureCapacity:
		return MemoryPressureCapacity
	case mpool.AllocationFailureSealed,
		mpool.AllocationFailureSuspended:
		return MemoryPressureSealed
	case mpool.AllocationFailureMismatch:
		return MemoryPressureMismatch
	case mpool.AllocationFailureAllocatorLimit:
		return MemoryPressureAllocatorLimit
	case mpool.AllocationFailureInvariant:
		return MemoryPressureInvariant
	}

	// Resource-ledger helpers may still return a bare lifecycle sentinel.
	if errors.Is(err, process.ErrHashBuildBudgetClosed) {
		return MemoryPressureSealed
	}
	if errors.Is(err, process.ErrHashBuildBudgetAdmission) {
		return MemoryPressureCapacity
	}
	if errors.Is(err, process.ErrHashBuildBudgetInvalid) ||
		errors.Is(err, process.ErrHashBuildCeilingMissing) {
		return MemoryPressureInvalid
	}
	return MemoryPressureNone
}

func IsRetryableMemoryCapacity(err error) bool {
	return MemoryPressureReasonOf(err) == MemoryPressureCapacity
}

// MinimumAllocationPressureError means the operation has already reclaimed
// optional storage and reduced itself to one indivisible input unit. It does
// not unwrap the last capacity error: callers must not mistake the terminal
// boundary for another retryable admission failure.
type MinimumAllocationPressureError struct {
	Owner    string
	Site     string
	Response string
	Used     uint64
	Limit    uint64
}

func (e *MinimumAllocationPressureError) Error() string {
	if e == nil {
		return "minimum allocation cannot be admitted"
	}
	return fmt.Sprintf(
		"minimum allocation cannot be admitted: owner=%s site=%s response=%s used=%d limit=%d",
		e.Owner,
		e.Site,
		e.Response,
		e.Used,
		e.Limit,
	)
}

func NewMinimumAllocationPressureError(
	owner string,
	site string,
	account *mpool.AllocationAccount,
) error {
	err := &MinimumAllocationPressureError{
		Owner:    owner,
		Site:     site,
		Response: "reclaim/reduce/degrade exhausted",
	}
	if account != nil {
		snapshot := account.Snapshot()
		err.Used = snapshot.Used
		err.Limit = snapshot.Limit
	}
	return err
}

// PressureProgress is the monotonic proof required before retrying one
// logical operation. A retry is legal only after memory was reclaimed, spill
// state advanced, the input unit shrank, or optional work was disabled.
type PressureProgress struct {
	Used             uint64
	SpillEpoch       uint64
	InputUnits       int
	OptionalDisabled bool
}

type PressureRetryGuard struct {
	previous PressureProgress
	attempts int
	limit    int
}

func NewPressureRetryGuard(initial PressureProgress, limit int) *PressureRetryGuard {
	if limit <= 0 {
		limit = 64
	}
	return &PressureRetryGuard{previous: initial, limit: limit}
}

func (g *PressureRetryGuard) Advance(next PressureProgress) error {
	if g == nil || next.InputUnits < 0 {
		return process.ErrHashBuildBudgetInvalid
	}
	if g.attempts >= g.limit {
		return errors.Join(
			process.ErrHashBuildBudgetInvalid,
			moerr.NewInternalErrorNoCtx(
				"memory-pressure retry limit exceeded",
			),
		)
	}
	progress := next.Used < g.previous.Used ||
		next.SpillEpoch > g.previous.SpillEpoch ||
		(g.previous.InputUnits > 0 && next.InputUnits < g.previous.InputUnits) ||
		(!g.previous.OptionalDisabled && next.OptionalDisabled)
	if !progress {
		return errors.Join(
			process.ErrHashBuildBudgetInvalid,
			moerr.NewInternalErrorNoCtx(
				"memory-pressure retry made no progress",
			),
		)
	}
	g.previous = next
	g.attempts++
	return nil
}

func (g *PressureRetryGuard) Attempts() int {
	if g == nil {
		return 0
	}
	return g.attempts
}
