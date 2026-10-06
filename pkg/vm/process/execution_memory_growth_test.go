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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/stretchr/testify/require"
)

func TestExecutionMemoryGrowthParticipantSharesLiveHeadroom(t *testing.T) {
	budget := MustNewExecutionResourceBudget(1_000, 800)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	fixed, err := generation.ReserveTransientMemory(200)
	require.NoError(t, err)
	firstMemory, err := generation.ReserveTransientMemory(200)
	require.NoError(t, err)

	first, err := generation.RegisterMemoryGrowthParticipant()
	require.NoError(t, err)
	limit, err := first.RetainedLimit(200)
	require.NoError(t, err)
	require.Equal(t, uint64(600), limit)

	secondMemory, err := generation.ReserveTransientMemory(200)
	require.NoError(t, err)
	second, err := generation.RegisterMemoryGrowthParticipant()
	require.NoError(t, err)
	limit, err = first.RetainedLimit(200)
	require.NoError(t, err)
	require.Equal(t, uint64(200), limit)
	limit, err = second.RetainedLimit(200)
	require.NoError(t, err)
	require.Equal(t, uint64(300), limit)
	require.Equal(t, uint64(2), generation.Snapshot().MemoryGrowthParticipants)
	require.Equal(t, uint64(400), generation.Snapshot().MemoryGrowthReportedBytes)

	require.True(t, second.Release())
	require.False(t, second.Release())
	limit, err = first.RetainedLimit(200)
	require.NoError(t, err)
	require.Equal(t, uint64(400), limit)
	secondMemory.Release()
	limit, err = first.RetainedLimit(200)
	require.NoError(t, err)
	require.Equal(t, uint64(600), limit)
	require.True(t, first.Release())
	firstMemory.Release()
	fixed.Release()
	require.Zero(t, generation.Snapshot().MemoryGrowthParticipants)
	require.Zero(t, generation.Snapshot().MemoryGrowthReportedBytes)
}

func TestExecutionMemoryGrowthParticipantUsesCNAggregateHeadroom(t *testing.T) {
	budget := MustNewExecutionResourceBudget(1_000, 800)
	firstGeneration, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	secondGeneration, err := budget.OpenGeneration(2)
	require.NoError(t, err)
	other, err := secondGeneration.ReserveTransientMemory(700)
	require.NoError(t, err)
	participantMemory, err := firstGeneration.ReserveTransientMemory(100)
	require.NoError(t, err)
	participant, err := firstGeneration.RegisterMemoryGrowthParticipant()
	require.NoError(t, err)

	limit, err := participant.RetainedLimit(100)
	require.NoError(t, err)
	require.Equal(t, uint64(300), limit)

	fixed, err := firstGeneration.ReserveTransientMemory(100)
	require.NoError(t, err)
	limit, err = participant.RetainedLimit(100)
	require.NoError(t, err)
	require.Equal(t, uint64(200), limit)

	require.True(t, participant.Release())
	fixed.Release()
	participantMemory.Release()
	other.Release()
}

func TestExecutionMemoryGrowthParticipantsShareAcrossGenerations(t *testing.T) {
	budget := MustNewExecutionResourceBudget(1_000, 800)
	firstGeneration, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	secondGeneration, err := budget.OpenGeneration(2)
	require.NoError(t, err)
	firstMemory, err := firstGeneration.ReserveTransientMemory(100)
	require.NoError(t, err)
	secondMemory, err := secondGeneration.ReserveTransientMemory(100)
	require.NoError(t, err)
	first, err := firstGeneration.RegisterMemoryGrowthParticipant()
	require.NoError(t, err)
	second, err := secondGeneration.RegisterMemoryGrowthParticipant()
	require.NoError(t, err)

	limit, err := first.RetainedLimit(100)
	require.NoError(t, err)
	require.Equal(t, uint64(450), limit)
	limit, err = second.RetainedLimit(100)
	require.NoError(t, err)
	require.Equal(t, uint64(500), limit)
	limit, err = first.RetainedLimit(100)
	require.NoError(t, err)
	require.Equal(t, uint64(500), limit)

	require.True(t, first.Release())
	require.True(t, second.Release())
	firstMemory.Release()
	secondMemory.Release()
	require.Zero(t, budget.memoryGrowthParticipants)
	require.Zero(t, budget.memoryGrowthReportedBytes)
}

func TestExecutionMemoryGrowthParticipantUsesPhysicalHeadroom(t *testing.T) {
	budget := MustNewExecutionResourceBudget(1_000, 800)
	budget.memoryHeadroomProvider = func() (uint64, bool) { return 300, true }
	budget.memoryHeadroomSafety = 100
	firstGeneration, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	secondGeneration, err := budget.OpenGeneration(2)
	require.NoError(t, err)
	firstMemory, err := firstGeneration.ReserveTransientMemory(100)
	require.NoError(t, err)
	secondMemory, err := secondGeneration.ReserveTransientMemory(100)
	require.NoError(t, err)
	first, err := firstGeneration.RegisterMemoryGrowthParticipant()
	require.NoError(t, err)
	second, err := secondGeneration.RegisterMemoryGrowthParticipant()
	require.NoError(t, err)

	limit, err := first.RetainedLimit(100)
	require.NoError(t, err)
	require.Equal(t, uint64(200), limit)
	limit, err = second.RetainedLimit(100)
	require.NoError(t, err)
	require.Equal(t, uint64(200), limit)

	second.Release()
	first.Release()
	secondMemory.Release()
	firstMemory.Release()
}

func TestExecutionMemoryAdmissionConsumesCachedPhysicalHeadroom(t *testing.T) {
	budget := MustNewExecutionResourceBudget(1_000, 800)
	budget.memoryHeadroomProvider = func() (uint64, bool) { return 300, true }
	budget.memoryHeadroomSafety = 100
	budget.memoryHeadroomTTL = time.Hour
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)

	reservation, err := generation.ReserveTransientMemory(200)
	require.NoError(t, err)
	_, err = generation.ReserveTransientMemory(1)
	require.ErrorIs(t, err, ErrExecutionResourceAdmission)
	require.Equal(t, uint64(200), generation.Used())

	require.True(t, reservation.Release())
	reused, err := generation.ReserveTransientMemory(200)
	require.NoError(t, err)
	require.True(t, reused.Release())
}

func TestExecutionRecoveryBorrowsAlreadyAdmittedPhysicalHeadroom(t *testing.T) {
	budget := MustNewExecutionResourceBudget(1_000, 800)
	budget.memoryHeadroomProvider = func() (uint64, bool) { return 300, true }
	budget.memoryHeadroomSafety = 100
	budget.memoryHeadroomTTL = time.Hour
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	registry, err := mpool.NewAllocationAccountRegistry(1, 8)
	require.NoError(t, err)
	account, err := registry.OpenWithController(800, generation)
	require.NoError(t, err)
	recovery, err := NewExecutionRecoveryCapacity(generation)
	require.NoError(t, err)
	class, err := account.RegisterCapacityController(recovery)
	require.NoError(t, err)
	require.NoError(t, recovery.EnsureCapacity(200))
	require.Equal(t, uint64(200), generation.recoveryCapacityUnused)
	require.Equal(t, uint64(200), budget.recoveryCapacityUnused)
	_, err = generation.ReserveTransientMemory(1)
	require.ErrorIs(t, err, ErrExecutionResourceAdmission)

	mp := mpool.MustNewZero()
	buf, err := mp.AllocAccountedWithCapacityClass(
		200, account, mpool.AllocationOwnerHashBuild, 1, class,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(200), generation.Used())
	require.Equal(t, uint64(200), account.Snapshot().Used)
	require.Zero(t, generation.recoveryCapacityUnused)
	require.Zero(t, budget.recoveryCapacityUnused)
	_, err = generation.ReserveTransientMemory(1)
	require.ErrorIs(t, err, ErrExecutionResourceAdmission)

	mp.Free(buf)
	require.Equal(t, uint64(200), generation.recoveryCapacityUnused)
	require.Equal(t, uint64(200), budget.recoveryCapacityUnused)
	require.NoError(t, recovery.Close())
	require.Zero(t, generation.recoveryCapacityUnused)
	require.Zero(t, budget.recoveryCapacityUnused)
	require.NoError(t, account.UnregisterCapacityController(class, recovery))
	_, _, err = registry.CompleteTerminal(account)
	require.NoError(t, err)
}

func TestExecutionMemoryGrowthParticipantLifecycle(t *testing.T) {
	var nilGeneration *ExecutionResourceGeneration
	_, err := nilGeneration.RegisterMemoryGrowthParticipant()
	require.ErrorIs(t, err, ErrExecutionResourceInvalid)

	budget := MustNewExecutionResourceBudget(100, 100)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	participant, err := generation.RegisterMemoryGrowthParticipant()
	require.NoError(t, err)
	generation.Close()
	_, err = participant.RetainedLimit(0)
	require.ErrorIs(t, err, ErrExecutionResourceClosed)
	require.True(t, participant.Release())
	require.Zero(t, generation.Snapshot().MemoryGrowthParticipants)
	_, err = generation.RegisterMemoryGrowthParticipant()
	require.ErrorIs(t, err, ErrExecutionResourceClosed)
}
