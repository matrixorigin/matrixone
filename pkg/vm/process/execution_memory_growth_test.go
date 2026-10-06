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
	"sync"
	"sync/atomic"
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
	for _, tc := range []struct {
		name     string
		physical bool
		limits   [3]uint64
	}{
		{name: "CN headroom", limits: [3]uint64{450, 500, 500}},
		{name: "physical headroom", physical: true, limits: [3]uint64{200, 200, 200}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			budget := MustNewExecutionResourceBudget(1_000, 800)
			if tc.physical {
				budget.memoryHeadroomProvider = func() (uint64, bool) { return 300, true }
				budget.memoryHeadroomSafety = 100
			}
			firstGeneration, err := budget.OpenGeneration(1)
			require.NoError(t, err)
			secondGeneration, err := budget.OpenGeneration(2)
			require.NoError(t, err)
			registry, err := firstGeneration.AllocationAccountRegistry()
			require.NoError(t, err)
			mp := mpool.MustNewZero()
			t.Cleanup(func() { mpool.DeleteMPool(mp) })
			for _, generation := range []*ExecutionResourceGeneration{firstGeneration, secondGeneration} {
				account, err := registry.OpenWithController(800, generation)
				require.NoError(t, err)
				t.Cleanup(func() {
					_, _, err := registry.CompleteTerminal(account)
					require.NoError(t, err)
				})
				buf, err := mp.AllocAccounted(100, account, mpool.AllocationOwnerHashBuild, 1)
				require.NoError(t, err)
				t.Cleanup(func() { mp.Free(buf) })
			}
			first, err := firstGeneration.RegisterMemoryGrowthParticipant()
			require.NoError(t, err)
			defer first.Release()
			second, err := secondGeneration.RegisterMemoryGrowthParticipant()
			require.NoError(t, err)
			defer second.Release()

			for i, participant := range []*ExecutionMemoryGrowthParticipant{first, second, first} {
				limit, err := participant.RetainedLimit(100)
				require.NoError(t, err)
				require.Equal(t, tc.limits[i], limit)
			}
			require.True(t, first.Release())
			require.True(t, second.Release())
			require.Zero(t, budget.memoryGrowthParticipants)
			require.Zero(t, budget.memoryGrowthReportedBytes)
		})
	}
}

type pausedMemoryCapacityController struct {
	mpool.AllocationCapacityController
	admitted chan struct{}
	resume   chan struct{}
	paused   atomic.Bool
}

func (c *pausedMemoryCapacityController) AcquireAllocationCapacity(size uint64) error {
	if err := c.AllocationCapacityController.AcquireAllocationCapacity(size); err != nil {
		return err
	}
	if c.paused.CompareAndSwap(false, true) {
		close(c.admitted)
		<-c.resume
	}
	return nil
}

func TestExecutionMemoryRefreshPreservesPendingBacking(t *testing.T) {
	for _, tc := range []struct {
		name               string
		recovery           bool
		commitDuringSample bool
	}{
		{name: "ordinary"},
		{name: "recovery", recovery: true},
		{name: "completion during sample", commitDuringSample: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			budget := MustNewExecutionResourceBudget(1000, 800)
			now := time.Unix(1, 0)
			available := uint64(300)
			var duringSample func()
			budget.memoryHeadroomProvider = func() (uint64, bool) {
				if duringSample != nil {
					duringSample()
				}
				return available, true
			}
			budget.memoryHeadroomSafety = 100
			budget.memoryHeadroomTTL = time.Second
			budget.memoryHeadroomNow = func() time.Time { return now }
			generation, err := budget.OpenGeneration(1)
			require.NoError(t, err)
			registry, err := generation.AllocationAccountRegistry()
			require.NoError(t, err)
			mp := mpool.MustNewZero()
			t.Cleanup(func() { mpool.DeleteMPool(mp) })
			controller := &pausedMemoryCapacityController{
				AllocationCapacityController: generation,
				admitted:                     make(chan struct{}), resume: make(chan struct{}),
			}
			var defaultController mpool.AllocationCapacityController = controller
			if tc.recovery {
				defaultController = generation
			}
			account, err := registry.OpenWithController(800, defaultController)
			require.NoError(t, err)
			t.Cleanup(func() {
				_, _, err := registry.CompleteTerminal(account)
				require.NoError(t, err)
				require.Zero(t, registry.CommittedCapacity())
				require.Zero(t, generation.Used())
			})
			class := mpool.AllocationCapacityClassDefault
			if tc.recovery {
				recovery, err := NewExecutionRecoveryCapacity(generation)
				require.NoError(t, err)
				controller.AllocationCapacityController = recovery
				class, err = account.RegisterCapacityController(controller)
				require.NoError(t, err)
				t.Cleanup(func() {
					require.NoError(t, recovery.Close())
					require.NoError(t, account.UnregisterCapacityController(class, controller))
				})
				require.NoError(t, recovery.EnsureCapacity(200))
			}
			type result struct {
				buf []byte
				err error
			}
			finished := make(chan result, 1)
			go func() {
				buf, err := mp.AllocAccountedWithCapacityClass(200, account, mpool.AllocationOwnerHashBuild, 1, class)
				finished <- result{buf, err}
			}()
			var resumeOnce sync.Once
			var first result
			var joinOnce sync.Once
			join := func() {
				resumeOnce.Do(func() { close(controller.resume) })
				joinOnce.Do(func() { first = <-finished })
			}
			t.Cleanup(func() { join(); mp.Free(first.buf) })
			select {
			case <-controller.admitted:
			case <-time.After(5 * time.Second):
				t.Fatal("allocation did not reach the admission boundary")
			}
			require.Zero(t, mp.CurrNB(), "admitted allocation has no physical backing yet")
			now = now.Add(2 * time.Second)
			if tc.commitDuringSample {
				duringSample = join
			}
			second, err := mp.AllocAccounted(200, account, mpool.AllocationOwnerHashBuild, 2)
			duringSample = nil
			t.Cleanup(func() { mp.Free(second) })
			require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
			require.ErrorIs(t, err, ErrExecutionResourceAdmission)
			require.Equal(t, uint64(200), generation.Used())
			join()
			require.NoError(t, first.err)
			require.Equal(t, uint64(200), registry.CommittedCapacity())
			// Once backing exists, a new physical sample may grant genuinely free
			// space again. Pending protection must not become a sticky lower cap.
			available = 500
			now = now.Add(2 * time.Second)
			grown, err := mp.AllocAccounted(200, account, mpool.AllocationOwnerHashBuild, 2)
			require.NoError(t, err)
			t.Cleanup(func() { mp.Free(grown) })
		})
	}
}

func TestExecutionMemoryAdmissionConsumesCachedPhysicalHeadroom(t *testing.T) {
	budget := MustNewExecutionResourceBudget(1_000, 800)
	budget.memoryHeadroomProvider = func() (uint64, bool) { return 300, true }
	budget.memoryHeadroomSafety = 100
	budget.memoryHeadroomTTL = time.Second
	now := time.Unix(1, 0)
	budget.memoryHeadroomNow = func() time.Time { return now }
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)

	reservation, err := generation.ReserveTransientMemory(200)
	require.NoError(t, err)
	_, err = generation.ReserveTransientMemory(1)
	require.ErrorIs(t, err, ErrExecutionResourceAdmission)
	require.Equal(t, uint64(200), generation.Used())
	now = now.Add(2 * time.Second)
	_, err = generation.ReserveTransientMemory(1)
	require.ErrorIs(t, err, ErrExecutionResourceAdmission, "refresh must retain an unbacked scratch promise")

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
