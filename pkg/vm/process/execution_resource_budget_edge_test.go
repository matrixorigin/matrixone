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
	"errors"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/stretchr/testify/require"
)

func TestExecutionResourceErrorAndNilReceiverContracts(t *testing.T) {
	var nilErr *ExecutionResourceError
	require.Equal(t, "<nil>", nilErr.Error())
	require.NoError(t, nilErr.Unwrap())
	require.False(t, nilErr.Is(ErrExecutionResourceInvalid))

	tests := []struct {
		kind   ExecutionResourceErrorKind
		target error
	}{
		{ExecutionResourceErrorAdmission, ErrExecutionResourceAdmission},
		{ExecutionResourceErrorClosed, ErrExecutionResourceClosed},
		{ExecutionResourceErrorInvalid, ErrExecutionResourceInvalid},
		{ExecutionResourceErrorCeilingMissing, ErrExecutionMemoryCeilingMissing},
	}
	for _, tc := range tests {
		err := &ExecutionResourceError{
			Kind: tc.kind, Requested: 3, Used: 4, Cap: 5,
		}
		require.NotEmpty(t, err.Error())
		require.ErrorIs(t, err, tc.target)
		require.ErrorIs(t, err.Unwrap(), tc.target)
		require.False(t, err.Is(errors.New("different")))
	}
	custom := &ExecutionResourceError{Message: "custom resource failure"}
	require.Equal(t, custom.Message, custom.Error())
	unknown := &ExecutionResourceError{Kind: ExecutionResourceErrorKind(255)}
	require.Contains(t, unknown.Error(), "unknown kind")
	require.ErrorIs(t, unknown.Unwrap(), ErrExecutionResourceInvalid)

	var budget *ExecutionResourceBudget
	require.True(t, budget.Snapshot().Closed)
	require.Zero(t, budget.SpillDiskCap())
	require.Zero(t, budget.SpillDiskUsed())
	require.Zero(t, budget.SpillFDCap())
	require.Zero(t, budget.SpillFDUsed())
	require.Zero(t, budget.AggregateCap())
	require.Zero(t, budget.QueryCap())
	require.Zero(t, budget.AggregateUsed())
	require.True(t, budget.Closed())
	budget.Close()
	require.ErrorIs(t, budget.SetSpillCaps(1, 1), ErrExecutionResourceInvalid)
	require.ErrorIs(t, budget.raiseSpillDiskCapToExplicitLimit(1), ErrExecutionResourceInvalid)
	_, err := budget.OpenGeneration(1)
	require.ErrorIs(t, err, ErrExecutionResourceInvalid)
	_, err = budget.OpenGenerationWithCap(1, 1)
	require.ErrorIs(t, err, ErrExecutionResourceInvalid)
	_, err = budget.openProcessGeneration(1, 1, 0)
	require.ErrorIs(t, err, ErrExecutionResourceInvalid)

	var generation *ExecutionResourceGeneration
	require.Zero(t, generation.ID())
	require.Zero(t, generation.Cap())
	require.Zero(t, generation.Used())
	require.Zero(t, generation.Peak())
	require.Zero(t, generation.RejectCount())
	require.Zero(t, generation.SpillDiskCap())
	require.Zero(t, generation.SpillDiskUsed())
	require.Zero(t, generation.SpillFDCap())
	require.Zero(t, generation.SpillFDUsed())
	require.True(t, generation.Snapshot().Closed)
	require.True(t, generation.Closed())
	generation.Close()
	require.NoError(t, generation.AcquireAllocationCapacity(0))
	require.ErrorIs(t, generation.AcquireAllocationCapacity(1), mpool.ErrAllocationAccountInvariant)
	_, err = generation.AllocationAccountRegistry()
	require.ErrorIs(t, err, ErrExecutionResourceInvalid)
	_, err = generation.ReserveSpillDisk(1)
	require.ErrorIs(t, err, ErrExecutionResourceInvalid)
	_, err = generation.ReserveSpillFD(1)
	require.ErrorIs(t, err, ErrExecutionResourceInvalid)

	var disk *ExecutionSpillDiskReservation
	var fd *ExecutionSpillFDReservation
	require.Zero(t, disk.Size())
	require.Zero(t, fd.Size())
	require.False(t, disk.Release())
	require.False(t, fd.Release())
	require.ErrorIs(t, disk.Grow(1), ErrExecutionSpillReservationInactive)
	ok, err := disk.ReconcileDown(0)
	require.False(t, ok)
	require.ErrorIs(t, err, ErrExecutionSpillReservationInactive)
}

func TestExecutionResourceBudgetUnhappyPathMatrix(t *testing.T) {
	for _, tc := range []struct {
		aggregate uint64
		query     uint64
	}{
		{0, 1}, {1, 0}, {1, 2},
	} {
		_, err := NewExecutionResourceBudget(tc.aggregate, tc.query)
		require.ErrorIs(t, err, ErrExecutionResourceInvalid)
	}
	require.Panics(t, func() { MustNewExecutionResourceBudget(1, 2) })

	budget := MustNewExecutionResourceBudget(1024, 256)
	require.NoError(t, budget.raiseSpillDiskCapToExplicitLimit(0))
	oldDiskCap := budget.SpillDiskCap()
	require.NoError(t, budget.raiseSpillDiskCapToExplicitLimit(oldDiskCap+1))
	require.Equal(t, oldDiskCap+1, budget.SpillDiskCap())
	require.NoError(t, budget.SetSpillCaps(16, 2))

	generation, err := budget.OpenGenerationWithSpillCaps(1, 128, 8, 1)
	require.NoError(t, err)
	disk, err := generation.ReserveSpillDisk(8)
	require.NoError(t, err)
	require.Equal(t, uint64(8), disk.Size())
	_, err = generation.ReserveSpillDisk(1)
	require.ErrorIs(t, err, ErrExecutionResourceAdmission)
	require.ErrorIs(t, disk.Grow(1), ErrExecutionResourceAdmission)
	require.NoError(t, disk.Grow(0))
	ok, err := disk.ReconcileDown(9)
	require.False(t, ok)
	require.ErrorIs(t, err, ErrExecutionSpillReservationUpward)
	ok, err = disk.ReconcileDown(3)
	require.True(t, ok)
	require.NoError(t, err)
	require.Equal(t, uint64(3), disk.Size())
	require.True(t, disk.Release())
	require.False(t, disk.Release())
	require.ErrorIs(t, disk.Grow(1), ErrExecutionSpillReservationInactive)
	ok, err = disk.ReconcileDown(0)
	require.False(t, ok)
	require.ErrorIs(t, err, ErrExecutionSpillReservationInactive)

	fd, err := generation.ReserveSpillFD(1)
	require.NoError(t, err)
	require.Equal(t, uint64(1), fd.Size())
	_, err = generation.ReserveSpillFD(1)
	require.ErrorIs(t, err, ErrExecutionResourceAdmission)
	require.True(t, fd.Release())
	require.False(t, fd.Release())

	require.NoError(t, generation.AcquireAllocationCapacity(128))
	require.ErrorIs(t, generation.AcquireAllocationCapacity(1), mpool.ErrAllocationAccountCapacity)
	generation.ReleaseAllocationCapacity(128)
	require.NoError(t, generation.AcquireAllocationCapacity(0))
	generation.Close()
	require.ErrorIs(t, generation.AcquireAllocationCapacity(1), mpool.ErrAllocationAccountSealed)
	_, err = generation.ReserveSpillDisk(1)
	require.ErrorIs(t, err, ErrExecutionResourceClosed)
	_, err = generation.ReserveSpillFD(1)
	require.ErrorIs(t, err, ErrExecutionResourceClosed)

	_, err = budget.OpenGenerationWithCap(2, 0)
	require.ErrorIs(t, err, ErrExecutionResourceInvalid)
	_, err = budget.OpenGenerationWithCap(2, 2048)
	require.ErrorIs(t, err, ErrExecutionResourceInvalid)
	budget.Close()
	require.ErrorIs(t, budget.raiseSpillDiskCapToExplicitLimit(1), ErrExecutionResourceClosed)
	_, err = budget.OpenGeneration(2)
	require.ErrorIs(t, err, ErrExecutionResourceClosed)
	_, err = budget.OpenGenerationWithCap(2, 1)
	require.ErrorIs(t, err, ErrExecutionResourceClosed)
	_, err = budget.openProcessGeneration(2, 1, 0)
	require.ErrorIs(t, err, ErrExecutionResourceClosed)
}

func TestResolveExecutionMemoryCeilingBoundarySources(t *testing.T) {
	_, err := ResolveExecutionMemoryCeiling(ExecutionMemoryCeilingInputs{
		CgroupMemoryMax: math.MaxUint64,
		HostMemTotal:    math.MaxUint64,
	})
	require.ErrorIs(t, err, ErrExecutionMemoryCeilingMissing)

	ceiling, err := ResolveExecutionMemoryCeiling(ExecutionMemoryCeilingInputs{
		CgroupMemoryMax:       10,
		HostMemTotal:          20,
		GlobalMpoolCap:        30,
		FileCacheHint:         9,
		ProcessLimitationSize: 1,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(10), ceiling.EffectiveCN)
	require.Equal(t, uint64(1), ceiling.QueryCap)
}

func TestExecutionResourceLiveReservationRejectsCapShrinkAndClose(t *testing.T) {
	budget := MustNewExecutionResourceBudget(1024, 1024)
	require.NoError(t, budget.SetSpillCaps(64, 4))
	generation, err := budget.OpenGenerationWithSpillCaps(1, 1024, 64, 4)
	require.NoError(t, err)
	disk, err := generation.ReserveSpillDisk(32)
	require.NoError(t, err)
	fd, err := generation.ReserveSpillFD(2)
	require.NoError(t, err)
	require.ErrorIs(t, budget.SetSpillCaps(31, 4), ErrExecutionResourceAdmission)
	require.ErrorIs(t, budget.SetSpillCaps(64, 1), ErrExecutionResourceAdmission)

	generation.Close()
	require.ErrorIs(t, disk.Grow(1), ErrExecutionResourceClosed)
	require.True(t, disk.Release())
	require.True(t, fd.Release())
	budget.Close()
}

func TestExecutionResourceRefreshRejectsZeroCeiling(t *testing.T) {
	var nilBudget *ExecutionResourceBudget
	_, _, _, err := nilBudget.refreshAggregateCap(false, 0)
	require.ErrorIs(t, err, ErrExecutionResourceInvalid)
	require.ErrorIs(t, nilBudget.UpdateAggregateCap(1), ErrExecutionResourceInvalid)

	budget := MustNewExecutionResourceBudget(1024, 1024)
	budget.SetAggregateCapProvider(func() (uint64, error) {
		return 0, nil
	})
	_, _, _, err = budget.refreshAggregateCap(true, 0)
	require.ErrorIs(t, err, ErrExecutionMemoryCeilingMissing)
	budget.Close()
}

func TestExecutionRecoveryCapacityReusableSlotLifecycle(t *testing.T) {
	_, err := NewExecutionRecoveryCapacity(nil)
	require.ErrorIs(t, err, ErrExecutionResourceInvalid)

	budget := MustNewExecutionResourceBudget(1024, 512)
	first, err := budget.OpenGenerationWithCap(1, 256)
	require.NoError(t, err)
	secondBudget := MustNewExecutionResourceBudget(1024, 512)
	second, err := secondBudget.OpenGenerationWithCap(2, 256)
	require.NoError(t, err)

	slot := NewExecutionRecoveryCapacitySlot()
	require.ErrorIs(t, slot.EnsureCapacity(1), ErrExecutionSpillReservationInactive)
	require.ErrorIs(t, slot.AcquireAllocationCapacity(1), mpool.ErrAllocationAccountSealed)
	require.NoError(t, slot.Activate(first))
	require.NoError(t, slot.Activate(first))
	require.ErrorIs(t, slot.Activate(second), ErrExecutionResourceInvalid)
	require.NoError(t, slot.EnsureCapacity(32))
	require.NoError(t, slot.EnsureCapacity(16))
	require.NoError(t, slot.AcquireAllocationCapacity(16))
	require.NoError(t, slot.AcquireAllocationCapacity(32))
	capacity, borrowed := slot.Snapshot()
	require.Equal(t, uint64(48), capacity)
	require.Equal(t, uint64(48), borrowed)
	require.ErrorIs(t, slot.Close(), mpool.ErrAllocationAccountLive)
	require.Panics(t, func() { slot.ReleaseAllocationCapacity(49) })
	slot.ReleaseAllocationCapacity(48)
	require.NoError(t, slot.Close())
	require.NoError(t, slot.Close())

	// The stable slot address is reusable only after the previous generation's
	// recovery floor and all physical borrowers have drained.
	require.NoError(t, slot.Activate(second))
	require.NoError(t, slot.AcquireAllocationCapacity(8))
	slot.ReleaseAllocationCapacity(8)
	require.NoError(t, slot.Close())

	first.Close()
	second.Close()
	budget.Close()
	secondBudget.Close()
}
