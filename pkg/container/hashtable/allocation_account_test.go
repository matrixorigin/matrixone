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

package hashtable

import (
	"testing"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/stretchr/testify/require"
)

const (
	testHashTableOwner          mpool.AllocationOwner = 1
	testHashTableCellSite       mpool.AllocationSite  = 24
	testHashTableDescriptorSite mpool.AllocationSite  = 25
)

func newHashTableAllocation(
	t testing.TB,
	limit uint64,
	metadataSlots uint64,
) (*mpool.AllocationAccountRegistry, *mpool.AllocationAccount, *AllocationAccountSelection) {
	t.Helper()
	registry, err := mpool.NewAllocationAccountRegistry(1, metadataSlots)
	require.NoError(t, err)
	account, err := registry.Open(limit)
	require.NoError(t, err)
	selection, err := NewAllocationAccountSelection(
		account,
		testHashTableOwner,
		testHashTableCellSite,
		testHashTableDescriptorSite,
	)
	require.NoError(t, err)
	return registry, account, selection
}

func completeHashTableAllocation(
	t testing.TB,
	registry *mpool.AllocationAccountRegistry,
	account *mpool.AllocationAccount,
) {
	t.Helper()
	snapshot, first, err := registry.CompleteTerminal(account)
	require.NoError(t, err)
	require.True(t, first)
	require.Equal(t, mpool.AllocationAccountTerminalValid, snapshot.State)
	require.Zero(t, snapshot.Used)
}

func TestHashTableAllocationAccountsCellAndDescriptorStorage(t *testing.T) {
	t.Run("int", func(t *testing.T) {
		registry, account, selection := newHashTableAllocation(t, 64<<20, 64)
		mp := mpool.MustNewZero()
		var table Int64HashMap
		require.NoError(t, table.InitWithAllocation(mp, selection))
		descriptorBytes := uint64(unsafe.Sizeof([]Int64HashMapCell(nil)))
		require.Equal(
			t,
			Int64HashMapInitialAllocationBytes()+descriptorBytes,
			account.Snapshot().Used,
		)
		table.Free()
		completeHashTableAllocation(t, registry, account)
	})

	t.Run("string", func(t *testing.T) {
		registry, account, selection := newHashTableAllocation(t, 64<<20, 64)
		mp := mpool.MustNewZero()
		var table StringHashMap
		require.NoError(t, table.InitWithAllocation(mp, selection))
		descriptorBytes := uint64(unsafe.Sizeof([]StringHashMapCell(nil)))
		require.Equal(
			t,
			StringHashMapInitialAllocationBytes()+descriptorBytes,
			account.Snapshot().Used,
		)
		table.Free()
		completeHashTableAllocation(t, registry, account)
	})
}

func TestIntHashTableAccountedReplacementPeakAndRollback(t *testing.T) {
	const requestedRows = uint64(20_000)
	probeMP := mpool.MustNewZero()
	var probe Int64HashMap
	require.NoError(t, probe.Init(probeMP))
	plan := probe.PlanResize(requestedRows)
	require.False(t, plan.Noop)
	require.False(t, plan.ReuseCurrentBlocks)
	probe.Free()
	mpool.DeleteMPool(probeMP)

	descriptorSize := uint64(unsafe.Sizeof([]Int64HashMapCell(nil)))
	initialUsed := Int64HashMapInitialAllocationBytes() + descriptorSize
	targetDescriptors := plan.TargetBlockCount * descriptorSize
	expectedPeak := initialUsed + plan.AdditionalBytes + targetDescriptors

	t.Run("commit", func(t *testing.T) {
		registry, account, selection := newHashTableAllocation(t, expectedPeak, 64)
		mp := mpool.MustNewZero()
		var table Int64HashMap
		require.NoError(t, table.InitWithAllocation(mp, selection))
		require.NoError(t, table.ResizeWithPlan(table.PlanResize(requestedRows)))
		require.Equal(t, expectedPeak, account.Snapshot().Peak)
		require.Equal(
			t,
			plan.NewBytes+targetDescriptors,
			account.Snapshot().Used,
		)
		table.Free()
		completeHashTableAllocation(t, registry, account)
	})

	t.Run("one byte short", func(t *testing.T) {
		registry, account, selection := newHashTableAllocation(t, expectedPeak-1, 64)
		mp := mpool.MustNewZero()
		var table Int64HashMap
		require.NoError(t, table.InitWithAllocation(mp, selection))
		beforeCells := table.cells
		before := account.Snapshot()
		err := table.ResizeWithPlan(table.PlanResize(requestedRows))
		require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
		require.Same(t, &beforeCells[0][0], &table.cells[0][0])
		require.Equal(t, before.Used, account.Snapshot().Used)
		require.Equal(t, uint64(2), registry.LiveAllocationMetadata())
		table.Free()
		completeHashTableAllocation(t, registry, account)
	})

	t.Run("metadata rollback", func(t *testing.T) {
		// Initial descriptor+cell consume two slots. The replacement descriptor
		// consumes the last one; the first replacement cell must reject and the
		// complete private replacement rolls back before returning.
		registry, account, selection := newHashTableAllocation(t, expectedPeak, 3)
		mp := mpool.MustNewZero()
		var table Int64HashMap
		require.NoError(t, table.InitWithAllocation(mp, selection))
		beforeCells := table.cells
		before := account.Snapshot()
		err := table.ResizeWithPlan(table.PlanResize(requestedRows))
		require.ErrorIs(t, err, mpool.ErrAllocationMetadataSlots)
		require.Same(t, &beforeCells[0][0], &table.cells[0][0])
		require.Equal(t, before.Used, account.Snapshot().Used)
		require.Equal(t, uint64(2), registry.LiveAllocationMetadata())
		table.Free()
		completeHashTableAllocation(t, registry, account)
	})
}

func TestIntHashTableAccountedSegmentedGrowth(t *testing.T) {
	registry, account, selection := newHashTableAllocation(t, 96<<20, 128)
	mp := mpool.MustNewZero()
	var table Int64HashMap
	require.NoError(t, table.InitWithAllocation(mp, selection))

	firstTarget := maxElemCnt(maxIntCellCntPerBlock, intCellSize)
	require.NoError(t, table.ResizeOnDemand(int(firstTarget)))
	plan := table.PlanResize(firstTarget + 1)
	require.True(t, plan.ReuseCurrentBlocks)
	before := account.Snapshot().Used
	descriptorSize := uint64(unsafe.Sizeof([]Int64HashMapCell(nil)))
	expectedPeak := before + plan.AdditionalBytes + plan.TargetBlockCount*descriptorSize
	require.NoError(t, table.ResizeWithPlan(plan))
	require.Equal(t, expectedPeak, account.Snapshot().Peak)
	require.Equal(
		t,
		plan.NewBytes+plan.TargetBlockCount*descriptorSize,
		account.Snapshot().Used,
	)

	table.Free()
	completeHashTableAllocation(t, registry, account)
}

func TestStringHashTableAccountedReplacementPeakAndRollback(t *testing.T) {
	const requestedRows = uint64(20_000)
	probeMP := mpool.MustNewZero()
	var probe StringHashMap
	require.NoError(t, probe.Init(probeMP))
	plan := probe.PlanResize(requestedRows)
	require.False(t, plan.Noop)
	require.False(t, plan.ReuseCurrentBlocks)
	probe.Free()
	mpool.DeleteMPool(probeMP)

	descriptorSize := uint64(unsafe.Sizeof([]StringHashMapCell(nil)))
	initialUsed := StringHashMapInitialAllocationBytes() + descriptorSize
	targetDescriptors := plan.TargetBlockCount * descriptorSize
	expectedPeak := initialUsed + plan.AdditionalBytes + targetDescriptors

	for _, tc := range []struct {
		name  string
		limit uint64
		ok    bool
	}{
		{name: "commit", limit: expectedPeak, ok: true},
		{name: "one byte short", limit: expectedPeak - 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			registry, account, selection := newHashTableAllocation(t, tc.limit, 64)
			mp := mpool.MustNewZero()
			var table StringHashMap
			require.NoError(t, table.InitWithAllocation(mp, selection))
			beforeCells := table.cells
			before := account.Snapshot()
			err := table.ResizeWithPlan(table.PlanResize(requestedRows))
			if tc.ok {
				require.NoError(t, err)
				require.Equal(t, expectedPeak, account.Snapshot().Peak)
				require.Equal(
					t,
					plan.NewBytes+targetDescriptors,
					account.Snapshot().Used,
				)
			} else {
				require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
				require.Same(t, &beforeCells[0][0], &table.cells[0][0])
				require.Equal(t, before.Used, account.Snapshot().Used)
				require.Equal(t, uint64(2), registry.LiveAllocationMetadata())
			}
			table.Free()
			completeHashTableAllocation(t, registry, account)
		})
	}
}

func TestStringHashTableAccountedSegmentedGrowth(t *testing.T) {
	registry, account, selection := newHashTableAllocation(t, 192<<20, 128)
	mp := mpool.MustNewZero()
	var table StringHashMap
	require.NoError(t, table.InitWithAllocation(mp, selection))

	firstTarget := maxElemCnt(maxStrCellCntPerBlock, strCellSize)
	require.NoError(t, table.ResizeOnDemand(firstTarget))
	plan := table.PlanResize(firstTarget + 1)
	require.True(t, plan.ReuseCurrentBlocks)
	before := account.Snapshot().Used
	descriptorSize := uint64(unsafe.Sizeof([]StringHashMapCell(nil)))
	expectedPeak := before + plan.AdditionalBytes + plan.TargetBlockCount*descriptorSize
	require.NoError(t, table.ResizeWithPlan(plan))
	require.Equal(t, expectedPeak, account.Snapshot().Peak)
	require.Equal(
		t,
		plan.NewBytes+plan.TargetBlockCount*descriptorSize,
		account.Snapshot().Used,
	)

	table.Free()
	completeHashTableAllocation(t, registry, account)
}

func TestAccountedHashTableNoopAndStalePlanDoNotChangeCharge(t *testing.T) {
	registry, account, selection := newHashTableAllocation(t, 64<<20, 64)
	mp := mpool.MustNewZero()
	var table Int64HashMap
	require.NoError(t, table.InitWithAllocation(mp, selection))

	before := account.Snapshot()
	noop := table.PlanResize(1)
	require.True(t, noop.Noop)
	require.NoError(t, table.ResizeWithPlan(noop))
	require.Equal(t, before, account.Snapshot())

	stale := table.PlanResize(20_000)
	require.NoError(t, table.ResizeOnDemand(2_000))
	before = account.Snapshot()
	err := table.ResizeWithPlan(stale)
	require.ErrorIs(t, err, ErrStaleResizePlan)
	require.Equal(t, before, account.Snapshot())

	table.Free()
	completeHashTableAllocation(t, registry, account)
}

func TestHashTableAccountedHighCardinalityResizeReturnsToZero(t *testing.T) {
	const rows = 1_000_000
	for _, tc := range []struct {
		name string
		run  func(*mpool.MPool, *AllocationAccountSelection) error
	}{
		{
			name: "int",
			run: func(mp *mpool.MPool, selection *AllocationAccountSelection) error {
				var table Int64HashMap
				if err := table.InitWithAllocation(mp, selection); err != nil {
					return err
				}
				defer table.Free()
				return table.ResizeOnDemand(rows)
			},
		},
		{
			name: "string",
			run: func(mp *mpool.MPool, selection *AllocationAccountSelection) error {
				var table StringHashMap
				if err := table.InitWithAllocation(mp, selection); err != nil {
					return err
				}
				defer table.Free()
				return table.ResizeOnDemand(rows)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			registry, account, selection := newHashTableAllocation(t, 256<<20, 256)
			mp := mpool.MustNewZero()
			require.NoError(t, tc.run(mp, selection))
			require.Positive(t, account.Snapshot().Peak)
			require.Zero(t, account.Snapshot().Used)
			require.Zero(t, registry.LiveAllocationMetadata())
			completeHashTableAllocation(t, registry, account)
		})
	}
}

func BenchmarkHashTableResizeAccounting(b *testing.B) {
	const rows = 100_000
	b.Run("legacy", func(b *testing.B) {
		mp := mpool.MustNewZero()
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			var table Int64HashMap
			if err := table.Init(mp); err != nil {
				b.Fatal(err)
			}
			if err := table.ResizeOnDemand(rows); err != nil {
				b.Fatal(err)
			}
			table.Free()
		}
	})
	b.Run("accounted", func(b *testing.B) {
		registry, account, selection := newHashTableAllocation(b, 256<<20, 256)
		mp := mpool.MustNewZero()
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			var table Int64HashMap
			if err := table.InitWithAllocation(mp, selection); err != nil {
				b.Fatal(err)
			}
			if err := table.ResizeOnDemand(rows); err != nil {
				b.Fatal(err)
			}
			table.Free()
		}
		b.StopTimer()
		completeHashTableAllocation(b, registry, account)
	})
}
