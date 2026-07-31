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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestMemoryPressureReasonSeparatesCapacityFromLifecycle(t *testing.T) {
	tests := []struct {
		err    error
		reason MemoryPressureReason
	}{
		{nil, MemoryPressureNone},
		{&process.HashBuildBudgetError{Kind: process.HashBuildBudgetErrorAdmission}, MemoryPressureCapacity},
		{&process.HashBuildBudgetError{Kind: process.HashBuildBudgetErrorAdmission, Component: process.HashBuildBudgetComponentMemory}, MemoryPressureCapacity},
		{&process.HashBuildBudgetError{Kind: process.HashBuildBudgetErrorAdmission, Component: process.HashBuildBudgetComponentSpillDisk}, MemoryPressureSpillDiskLimit},
		{&process.HashBuildBudgetError{Kind: process.HashBuildBudgetErrorAdmission, Component: process.HashBuildBudgetComponentSpillFD}, MemoryPressureSpillFDLimit},
		{&process.HashBuildBudgetError{Kind: process.HashBuildBudgetErrorClosed}, MemoryPressureSealed},
		{&process.HashBuildBudgetError{Kind: process.HashBuildBudgetErrorInvalid}, MemoryPressureInvalid},
		{fmt.Errorf("wrapped: %w", process.ErrHashBuildBudgetAdmission), MemoryPressureCapacity},
		{mpool.ErrAllocationAccountCapacity, MemoryPressureCapacity},
		{mpool.ErrAllocationMetadataSlots, MemoryPressureCapacity},
		{mpool.ErrAllocationAccountSealed, MemoryPressureSealed},
		{mpool.ErrAllocationAccountMismatch, MemoryPressureMismatch},
		{mpool.ErrAllocationAllocatorLimit, MemoryPressureAllocatorLimit},
		{mpool.ErrAllocationAccountInvariant, MemoryPressureInvariant},
		{NewMinimumAllocationPressureError("hashbuild", "spill", nil), MemoryPressureMinimumUnit},
	}
	for _, test := range tests {
		require.Equal(t, test.reason, MemoryPressureReasonOf(test.err))
		require.Equal(t, test.reason == MemoryPressureCapacity, IsRetryableMemoryCapacity(test.err))
	}
}

func TestPressureRetryGuardRequiresMonotonicProgress(t *testing.T) {
	initial := PressureProgress{Used: 100, SpillEpoch: 1, InputUnits: 16}
	for _, next := range []PressureProgress{
		{Used: 99, SpillEpoch: 1, InputUnits: 16},
		{Used: 99, SpillEpoch: 2, InputUnits: 16},
		{Used: 99, SpillEpoch: 2, InputUnits: 8},
		{Used: 99, SpillEpoch: 2, InputUnits: 8, OptionalDisabled: true},
	} {
		guard := NewPressureRetryGuard(initial, 1)
		require.NoError(t, guard.Advance(next))
		require.Equal(t, 1, guard.Attempts())
		require.Error(t, guard.Advance(next), "the retry limit remains fail-closed")
	}
	guard := NewPressureRetryGuard(initial, 4)
	require.Error(t, guard.Advance(initial))
	require.Zero(t, guard.Attempts())
}
