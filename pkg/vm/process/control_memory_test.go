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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/system"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/stretchr/testify/require"
)

func controlTestBudget(t *testing.T, free uint64) (*ExecutionResourceBudget, *system.MemoryDomainSnapshot) {
	t.Helper()
	snapshot := &system.MemoryDomainSnapshot{
		Used: (8 << 30) - (512 << 20) - free, Limit: 8 << 30,
		Source: system.MemoryDomainSourceCgroupV2, Measured: true,
	}
	b, err := NewExecutionResourceBudget(1<<30, 1<<30)
	require.NoError(t, err)
	b.physicalLedger = system.NewMemoryAdmissionBudgetWithProvider(system.MemoryDomainReservePolicy{
		MinimumBytes: 256 << 20, RuntimeBytes: 256 << 20, ObservedNonGoMultiplier: 1,
	}, func() (system.MemoryDomainSnapshot, error) { return *snapshot, nil })
	return b, snapshot
}

func TestProcessEntryChargesActualAllocationWithoutFixedFloor(t *testing.T) {
	b, _ := controlTestBudget(t, 1<<20)
	g, err := b.openProcessGeneration(1, 1<<30, 0)
	require.NoError(t, err, "opening a small SQL does not require 256 MiB")
	defer g.Close()
	require.NoError(t, g.AcquireAllocationCapacity(1<<20))
	require.ErrorIs(t, g.AcquireAllocationCapacity(1), ErrExecutionResourceAdmission)
	g.ReleaseAllocationCapacity(1 << 20)
	require.Zero(t, b.physicalLedger.Reserved())
}

func TestControlGenerationProgressUnderOrdinaryPressure(t *testing.T) {
	b, sample := controlTestBudget(t, 0)
	// Ordinary execution's cached headroom can also be empty. The bounded
	// control ledger, not this ordinary safety margin, owns control admission.
	b.memoryHeadroomMeasured = true
	b.memoryHeadroomBytes = 0
	ordinary, err := b.openControlProcessGeneration(1, 1<<30, 0, false)
	require.NoError(t, err)
	defer ordinary.Close()
	require.ErrorIs(t, ordinary.AcquireAllocationCapacity(1), ErrExecutionResourceAdmission)
	control, err := b.openControlProcessGeneration(2, 1<<30, 0, true)
	require.NoError(t, err)
	require.Equal(t, uint64(16<<20), control.Cap())
	require.NoError(t, control.AcquireAllocationCapacity(1<<20))
	scratch, err := control.ReserveTransientMemory(1 << 20)
	require.NoError(t, err)
	require.Equal(t, uint64(2<<20), b.AggregateUsed())
	require.Equal(t, uint64(2<<20), b.physicalLedger.Snapshot().ControlReserved)
	require.ErrorIs(t, control.AcquireAllocationCapacity(16<<20), ErrExecutionResourceAdmission)
	sample.Used = sample.Limit
	require.ErrorIs(t, control.AcquireAllocationCapacity(1), ErrExecutionResourceAdmission)
	control.Close()
	require.Equal(t, uint64(1), b.physicalLedger.Snapshot().ControlActive)
	control.ReleaseAllocationCapacity(1 << 20)
	require.True(t, scratch.Release())
	require.False(t, scratch.Release())
	require.Zero(t, b.AggregateUsed())
	require.Zero(t, b.physicalLedger.Reserved())
	require.Zero(t, b.physicalLedger.Snapshot().ControlActive)
}

func TestControlGenerationCannotSpendAnotherStatementsAllowance(t *testing.T) {
	b, _ := controlTestBudget(t, 0)
	controls := make([]*ExecutionResourceGeneration, 0, 4)
	for i := uint64(1); i <= 4; i++ {
		g, err := b.openControlProcessGeneration(i, 1<<30, 0, true)
		require.NoError(t, err)
		require.NoError(t, g.AcquireAllocationCapacity(16<<20))
		controls = append(controls, g)
	}
	fifth, err := b.openControlProcessGeneration(5, 1<<30, 0, true)
	require.NoError(t, err)
	defer fifth.Close()
	require.Nil(t, fifth.controlMemory)
	require.ErrorIs(t, fifth.AcquireAllocationCapacity(1), ErrExecutionResourceAdmission)
	for _, g := range controls {
		g.Close()
		g.ReleaseAllocationCapacity(16 << 20)
	}
	require.Zero(t, b.physicalLedger.Reserved())
}

func TestControlMemoryContextCanBeClearedAtNextStatement(t *testing.T) {
	ctx := WithControlMemoryAdmission(context.Background(), true)
	require.True(t, HasControlMemoryAdmission(ctx))
	require.False(t, HasControlMemoryAdmission(WithControlMemoryAdmission(ctx, false)))
}

func TestControlExecutionBorrowsStatementAdmissionWithoutSecondSlot(t *testing.T) {
	b, _ := controlTestBudget(t, 0)
	ctx, entry, err := b.physicalLedger.EnterStatement(t.Context(), true)
	require.NoError(t, err)
	g, err := b.openControlProcessGenerationWithEntry(1, 1<<30, 0, true, b.physicalLedger.StatementControlMemory(ctx))
	require.NoError(t, err)
	require.NotNil(t, g.controlMemory)
	require.Equal(t, uint64(1), b.physicalLedger.Snapshot().ControlActive)
	require.NoError(t, g.AcquireAllocationCapacity(120))
	g.Close()
	child, err := b.openControlProcessGenerationWithEntry(2, 1<<30, 0, true, b.physicalLedger.StatementControlMemory(ctx))
	require.NoError(t, err)
	require.NoError(t, child.AcquireAllocationCapacity(8), "a nested generation must not close its caller's control lease")
	child.Close()
	child.ReleaseAllocationCapacity(8)
	entry.Close()
	require.Equal(t, uint64(1), b.physicalLedger.Snapshot().ControlActive)
	g.Close()
	g.ReleaseAllocationCapacity(120)
	require.Zero(t, b.physicalLedger.Snapshot().ControlActive)
	require.Zero(t, b.physicalLedger.Reserved())
}

func TestStatementEntryFailurePrecedesAllocationAndPreservesCause(t *testing.T) {
	t.Setenv("MO_MEMORY_STATEMENT_WAIT_TIMEOUT", "0s")
	b, _ := controlTestBudget(t, 0)
	g, err := b.openProcessGeneration(1, 1<<30, 0)
	require.NoError(t, err)
	defer g.Close()
	proc := &Process{Ctx: t.Context(), Base: &BaseProcess{}}
	require.NoError(t, proc.SetExecutionResourceBudgetForTesting(g))
	_, _, err = proc.EnterStatementMemoryAdmission(t.Context())
	require.ErrorIs(t, err, ErrExecutionResourceAdmission)
	require.ErrorIs(t, err, system.ErrMemoryStatementTimeout)
	require.Zero(t, g.Used())
	require.Zero(t, b.physicalLedger.Reserved())
}

func TestSharedPhysicalBudgetIsSoleAllocationAndGrowthAuthority(t *testing.T) {
	b, _ := controlTestBudget(t, 1<<20)
	b.memoryHeadroomMeasured = true
	b.memoryHeadroomBytes = 0 // obsolete independent observation must not veto the owner
	g, err := b.openProcessGeneration(1, 1<<30, 0)
	require.NoError(t, err)
	defer g.Close()
	require.NoError(t, g.AcquireAllocationCapacity(120))
	b.mu.Lock()
	left, measured := b.physicalGrowthHeadroomLocked()
	b.mu.Unlock()
	require.True(t, measured)
	require.Equal(t, b.physicalLedger.Snapshot().Available, left)
	require.Equal(t, uint64(120), b.physicalLedger.Reserved())
	g.ReleaseAllocationCapacity(120)
	require.Zero(t, b.physicalLedger.Reserved())
}

func TestHealthyControlQueryRetainsOrdinaryLimits(t *testing.T) {
	b, _ := controlTestBudget(t, 256<<20)
	g, err := b.openControlProcessGeneration(1, 1<<30, 0, true)
	require.NoError(t, err)
	defer g.Close()
	require.Nil(t, g.controlMemory)
	require.Equal(t, uint64(1<<30), g.Cap())
	require.NoError(t, g.AcquireAllocationCapacity(32<<20))
	g.ReleaseAllocationCapacity(32 << 20)
	require.Zero(t, b.physicalLedger.Snapshot().ControlActive)
}

func TestControlProcessWithFileServiceChargesPhysicalMPool(t *testing.T) {
	const service = "__process_local_cn__"
	b, _ := controlTestBudget(t, 0)
	executionResourceCNBudgets.Store(service, b)
	t.Cleanup(func() { executionResourceCNBudgets.Delete(service) })
	fs, err := fileservice.NewMemoryFS("LOCAL", fileservice.CacheConfig{}, nil)
	require.NoError(t, err)
	defer fs.Close(t.Context())
	proc := &Process{
		Ctx:  WithControlMemoryAdmission(t.Context(), true),
		Base: &BaseProcess{FileService: fs, Lim: Limitation{Size: 1 << 30}},
	}
	g, err := proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	defer g.Close()
	require.NotNil(t, g.controlMemory, "a FileService-backed process must use the production control path")
	registry, err := g.AllocationAccountRegistry()
	require.NoError(t, err)
	account, err := registry.OpenWithController(g.Cap(), g)
	require.NoError(t, err)
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	buf, err := mp.AllocAccounted(4096, account, 1, 1)
	require.NoError(t, err)
	require.Equal(t, uint64(4096), b.physicalLedger.Snapshot().ControlReserved)
	g.Close()
	require.Equal(t, uint64(1), b.physicalLedger.Snapshot().ControlActive)
	mp.Free(buf)
	require.Zero(t, b.physicalLedger.Reserved())
	require.Zero(t, b.physicalLedger.Snapshot().ControlActive)
	account.Seal()
	_, err = registry.Finalize(account)
	require.NoError(t, err)
}

func TestFileServiceProcessHostCacheAndTrueExhaustion(t *testing.T) {
	const service = "__process_local_cn__"
	const gib = uint64(1 << 30)
	sample := system.MemoryDomainSnapshot{
		Used: 14 * gib, Limit: 16 * gib, Reclaimable: 8 * gib,
		Source: system.MemoryDomainSourceHost, Measured: true,
	}
	b, err := NewExecutionResourceBudget(gib, gib)
	require.NoError(t, err)
	b.physicalLedger = system.NewMemoryAdmissionBudgetWithProvider(system.DefaultMemoryDomainReservePolicy(),
		func() (system.MemoryDomainSnapshot, error) { return sample, nil })
	b.memoryHeadroomProvider = func() (uint64, bool) {
		return sample.AdmissionReclaimableHeadroom(), sample.Measured
	}
	executionResourceCNBudgets.Store(service, b)
	t.Cleanup(func() { executionResourceCNBudgets.Delete(service) })
	fs, err := fileservice.NewMemoryFS("LOCAL", fileservice.CacheConfig{}, nil)
	require.NoError(t, err)
	defer fs.Close(t.Context())
	proc := &Process{Ctx: t.Context(), Base: &BaseProcess{FileService: fs, Lim: Limitation{Size: int64(gib)}}}
	g, err := proc.GetExecutionResourceBudget()
	require.NoError(t, err, "warm host cache must not reject a FileService-backed statement")
	defer g.Close()
	registry, err := g.AllocationAccountRegistry()
	require.NoError(t, err)
	account, err := registry.OpenWithController(g.Cap(), g)
	require.NoError(t, err)
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	buf, err := mp.AllocAccounted(1<<20, account, 1, 1)
	require.NoError(t, err)
	require.Equal(t, uint64(1<<20), b.physicalLedger.Reserved())
	sample.Used = sample.Limit
	sample.Reclaimable = 0
	_, err = mp.AllocAccounted(1<<20, account, 1, 1)
	require.ErrorIs(t, err, ErrExecutionResourceAdmission, "true exhaustion must still reject allocation")
	require.Equal(t, uint64(1<<20), b.physicalLedger.Reserved())
	control, err := b.openControlProcessGeneration(2, gib, 0, true)
	require.NoError(t, err)
	defer control.Close()
	require.ErrorIs(t, control.AcquireAllocationCapacity(1), ErrExecutionResourceAdmission,
		"control work cannot bypass actual host exhaustion")
	g.Close()
	mp.Free(buf)
	require.Zero(t, b.physicalLedger.Reserved())
	account.Seal()
	_, err = registry.Finalize(account)
	require.NoError(t, err)
}

func TestFileServiceCgroupCatalogProgressAndExhaustion(t *testing.T) {
	for _, source := range []system.MemoryDomainSource{system.MemoryDomainSourceCgroupV1, system.MemoryDomainSourceCgroupV2} {
		for _, control := range []bool{false, true} {
			t.Run(string(source)+map[bool]string{false: "/ordinary-cache", true: "/control-no-cache"}[control], func(t *testing.T) {
				const service = "__process_local_cn__"
				const gib = uint64(1 << 30)
				sample := system.MemoryDomainSnapshot{
					Used: 1432 * gib / 100, Limit: 16 * gib, SoftLimit: 15 * gib,
					NonGo: 978 * gib / 1000, Reclaimable: 7 * gib, Source: source, Measured: true,
				}
				if control {
					sample.Reclaimable = 0
				}
				b := MustNewExecutionResourceBudget(gib, gib)
				b.physicalLedger = system.NewMemoryAdmissionBudgetWithProvider(system.DefaultMemoryDomainReservePolicy(),
					func() (system.MemoryDomainSnapshot, error) { return sample, nil })
				b.memoryHeadroomProvider = func() (uint64, bool) { return sample.AdmissionReclaimableHeadroom(), true }
				executionResourceCNBudgets.Store(service, b)
				t.Cleanup(func() { executionResourceCNBudgets.Delete(service) })
				fs, err := fileservice.NewMemoryFS("LOCAL", fileservice.CacheConfig{}, nil)
				require.NoError(t, err)
				defer fs.Close(t.Context())
				proc := &Process{Ctx: WithControlMemoryAdmission(t.Context(), control),
					Base: &BaseProcess{FileService: fs, Lim: Limitation{Size: int64(gib)}}}
				g, err := proc.GetExecutionResourceBudget()
				require.NoError(t, err)
				defer g.Close()
				if control {
					require.NotNil(t, g.controlMemory)
				}
				registry, err := g.AllocationAccountRegistry()
				require.NoError(t, err)
				account, err := registry.OpenWithController(g.Cap(), g)
				require.NoError(t, err)
				mp := mpool.MustNewZero()
				defer mpool.DeleteMPool(mp)
				buf, err := mp.AllocAccounted(120, account, 1, 1)
				require.NoError(t, err, "the real FileService/MPool path must admit the catalog read")
				require.Positive(t, b.physicalLedger.Reserved())
				sample.Used = sample.SoftLimit
				_, err = mp.AllocAccounted(8, account, 1, 1)
				require.ErrorIs(t, err, ErrExecutionResourceAdmission, "neither control nor cache may cross the soft boundary")
				g.Close()
				mp.Free(buf)
				require.Zero(t, b.physicalLedger.Reserved())
				require.Zero(t, b.physicalLedger.Snapshot().ControlActive)
				account.Seal()
				_, err = registry.Finalize(account)
				require.NoError(t, err)
			})
		}
	}
}

func TestIsolatedFixtureBudgetIsLazyFiniteAndPrivate(t *testing.T) {
	for range 2 {
		proc := &Process{Base: &BaseProcess{Lim: Limitation{Size: 1024}}}
		require.NoError(t, proc.UseIsolatedExecutionResourceBudgetForTesting())
		proc.Base.Lim.Size = 512
		proc.Base.Lim.SpillSize = 8192
		g, err := proc.GetExecutionResourceBudget()
		require.NoError(t, err)
		require.Equal(t, uint64(512), g.Cap())
		require.Equal(t, uint64(8192), g.SpillDiskCap())
		require.Nil(t, g.budget.physicalLedger)
		require.Nil(t, g.budget.memoryHeadroomProvider)
		require.NoError(t, g.AcquireAllocationCapacity(512))
		require.ErrorIs(t, g.AcquireAllocationCapacity(1), ErrExecutionResourceAdmission)
		require.ErrorIs(t, proc.UseIsolatedExecutionResourceBudgetForTesting(), ErrExecutionResourceInvalid)
		g.Close()
		g.ReleaseAllocationCapacity(512)
		require.Zero(t, g.budget.AggregateUsed())
	}
	var proc *Process
	require.ErrorIs(t, proc.UseIsolatedExecutionResourceBudgetForTesting(), ErrExecutionResourceInvalid)
}
