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

package mpool

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

const (
	testAllocationOwner AllocationOwner = 1
	testAllocationSite  AllocationSite  = 1
)

func newTestAllocationAccount(
	t testing.TB,
	limit uint64,
	metadataSlots uint64,
) (*AllocationAccountRegistry, *AllocationAccount) {
	t.Helper()
	registry, err := NewAllocationAccountRegistry(4, metadataSlots)
	require.NoError(t, err)
	account, err := registry.Open(limit)
	require.NoError(t, err)
	return registry, account
}

func finalizeTestAllocationAccount(
	t testing.TB,
	registry *AllocationAccountRegistry,
	account *AllocationAccount,
) {
	t.Helper()
	account.Seal()
	_, err := registry.Finalize(account)
	require.NoError(t, err)
}

func TestMPoolAccountedAllocGrowFree(t *testing.T) {
	require.Equal(t, uintptr(kMemHdrSz), unsafe.Sizeof(memHdr{}))
	require.Equal(t, uintptr(16), unsafe.Sizeof(allocationLease{}))
	// Owner current/peak counters are fixed to this binary's catalog and remain
	// allocation-free at physical allocation boundaries.
	require.LessOrEqual(t, unsafe.Sizeof(AllocationAccount{}), uintptr(320))
	require.LessOrEqual(t, unsafe.Sizeof(allocationAccountRegistrySlot{}), uintptr(32))

	registry, account := newTestAllocationAccount(t, 1024, 8)
	mp := MustNew("accounted-alloc-grow")
	defer DeleteMPool(mp)

	empty, err := mp.AllocAccounted(
		0,
		account,
		testAllocationOwner,
		testAllocationSite,
	)
	require.NoError(t, err)
	require.Nil(t, empty)
	require.Zero(t, account.Snapshot().Used)
	require.Zero(t, registry.LiveAllocationMetadata())

	buffer, err := mp.AllocAccounted(
		64,
		account,
		testAllocationOwner,
		testAllocationSite,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(64), account.Snapshot().Used)
	require.Equal(t, uint64(1), registry.LiveAllocationMetadata())

	var lease allocationLease
	hdr, ok := mp.getPtrMetadata(
		unsafe.Pointer(unsafe.SliceData(buffer)),
		&lease,
	)
	require.True(t, ok)
	require.True(t, hdr.isOffHeap())
	require.True(t, hdr.isAccounted())
	require.Same(t, account, lease.account)
	require.Equal(t, testAllocationOwner, lease.owner)
	require.Equal(t, testAllocationSite, lease.site)

	same, err := mp.Grow(buffer, 32, true)
	require.NoError(t, err)
	require.Equal(
		t,
		unsafe.Pointer(unsafe.SliceData(buffer)),
		unsafe.Pointer(unsafe.SliceData(same)),
	)
	require.Equal(t, uint64(64), account.Snapshot().Used)

	grown, err := mp.Grow(same, 128, true)
	require.NoError(t, err)
	require.Equal(t, uint64(cap(grown)), account.Snapshot().Used)
	require.Equal(
		t,
		uint64(64+cap(grown)),
		account.Snapshot().Peak,
	)
	require.Equal(t, uint64(1), registry.LiveAllocationMetadata())
	require.Equal(t, uint64(2), registry.PeakAllocationMetadata())

	mp.Free(grown)
	require.Zero(t, account.Snapshot().Used)
	require.Zero(t, registry.LiveAllocationMetadata())
	finalizeTestAllocationAccount(t, registry, account)
}

func TestMPoolAccountedOwnerAttribution(t *testing.T) {
	registry, account := newTestAllocationAccount(t, 1024, 3)
	mp := MustNew("accounted-owner-attribution")
	defer DeleteMPool(mp)

	hashBuffer, err := mp.AllocAccounted(
		64,
		account,
		AllocationOwnerHashBuild,
		testAllocationSite,
	)
	require.NoError(t, err)
	indexBuffer, err := mp.AllocAccounted(
		32,
		account,
		AllocationOwnerIndexBuild,
		testAllocationSite,
	)
	require.NoError(t, err)
	hashBuffer, err = mp.Grow(hashBuffer, 128, true)
	require.NoError(t, err)

	hash, ok := account.OwnerUsage(AllocationOwnerHashBuild)
	require.True(t, ok)
	require.Equal(t, uint64(128), hash.Current)
	require.Equal(t, uint64(192), hash.Peak)
	index, ok := account.OwnerUsage(AllocationOwnerIndexBuild)
	require.True(t, ok)
	require.Equal(t, uint64(32), index.Current)
	require.Equal(t, uint64(32), index.Peak)
	require.Equal(t, uint64(160), account.Snapshot().Used)
	require.Equal(t, uint64(224), account.Snapshot().Peak)

	mp.Free(indexBuffer)
	mp.Free(hashBuffer)
	snapshot, first, err := registry.CompleteTerminal(account)
	require.NoError(t, err)
	require.True(t, first)
	require.Equal(t, AllocationAccountTerminalValid, snapshot.State)
	require.Zero(t, snapshot.Used)
	require.Equal(t, []AllocationAccountOwnerSnapshot{
		{Owner: AllocationOwnerHashBuild, Peak: 192},
		{Owner: AllocationOwnerIndexBuild, Peak: 32},
	}, snapshot.Owners)
}

func TestMPoolTerminalLeakDiagnosticUsesPublishedProvenance(t *testing.T) {
	registry, account := newTestAllocationAccount(t, 64, 1)
	mp := MustNew("accounted-terminal-diagnostic")
	defer DeleteMPool(mp)
	buffer, err := mp.AllocAccounted(
		64,
		account,
		testAllocationOwner,
		testAllocationSite,
	)
	require.NoError(t, err)
	snapshot, first, err := registry.CompleteTerminal(account)
	require.True(t, first)
	require.ErrorIs(t, err, ErrAllocationAccountInvariant)
	require.Equal(t, testAllocationOwner, snapshot.LiveOwner)
	require.Equal(t, testAllocationSite, snapshot.LiveSite)
	require.Equal(t, uint64(1), snapshot.LiveAllocations)
	require.Contains(t, err.Error(), "owner=1 site=1 live-allocations=1")
	require.Contains(t, err.Error(), "owner-name=hash_build")
	mp.Free(buffer)
	require.False(t, registry.AdmissionSuspended())
	_, ok := registry.Resolve(snapshot.Handle)
	require.False(t, ok)
}

func TestMPoolTerminalLeakDiagnosticScansNoLockPool(t *testing.T) {
	registry, account := newTestAllocationAccount(t, 64, 1)
	mp := MustNewNoLock("accounted-terminal-no-lock-diagnostic")
	require.NoError(t, mp.BindAllocationAccount(account))
	buffer, err := mp.AllocAccounted(
		64,
		account,
		testAllocationOwner,
		testAllocationSite,
	)
	require.NoError(t, err)

	snapshot, first, err := registry.CompleteTerminal(account)
	require.True(t, first)
	require.ErrorIs(t, err, ErrAllocationAccountInvariant)
	require.Equal(t, testAllocationOwner, snapshot.LiveOwner)
	require.Equal(t, testAllocationSite, snapshot.LiveSite)
	require.Equal(t, uint64(1), snapshot.LiveAllocations)

	mp.Free(buffer)
	DeleteMPool(mp)
	require.False(t, registry.AdmissionSuspended())
	_, ok := registry.Resolve(snapshot.Handle)
	require.False(t, ok)
}

func TestMPoolTerminalLeakDiagnosticSkipsUnrelatedActiveNoLockPool(t *testing.T) {
	firstRegistry, first := newTestAllocationAccount(t, 64, 1)
	secondRegistry, second := newTestAllocationAccount(t, 64, 1)
	firstMP := MustNewNoLock("accounted-terminal-first-no-lock-diagnostic")
	secondMP := MustNewNoLock("accounted-terminal-second-no-lock-diagnostic")
	require.NoError(t, firstMP.BindAllocationAccount(first))
	require.NoError(t, secondMP.BindAllocationAccount(second))

	firstBuffer, err := firstMP.AllocAccounted(
		64, first, testAllocationOwner, testAllocationSite)
	require.NoError(t, err)
	secondBuffer, err := secondMP.AllocAccounted(
		64, second, testAllocationOwner, testAllocationSite)
	require.NoError(t, err)

	snapshot, firstPublication, err := firstRegistry.CompleteTerminal(first)
	require.True(t, firstPublication)
	require.ErrorIs(t, err, ErrAllocationAccountInvariant)
	require.Equal(t, uint64(1), snapshot.LiveAllocations)

	firstMP.Free(firstBuffer)
	secondMP.Free(secondBuffer)
	DeleteMPool(firstMP)
	DeleteMPool(secondMP)
	require.False(t, firstRegistry.AdmissionSuspended())
	finalizeTestAllocationAccount(t, secondRegistry, second)
}

func TestMPoolAccountedNoLockPoolRequiresMatchingBinding(t *testing.T) {
	firstRegistry, first := newTestAllocationAccount(t, 64, 1)
	secondRegistry, second := newTestAllocationAccount(t, 64, 1)
	mp := MustNewNoLock("accounted-no-lock-binding")

	_, err := mp.AllocAccounted(
		64, first, testAllocationOwner, testAllocationSite)
	require.ErrorIs(t, err, ErrAllocationAccountInvariant)
	require.Zero(t, first.Snapshot().Used)

	require.NoError(t, mp.BindAllocationAccount(first))
	_, err = mp.AllocAccounted(
		64, second, testAllocationOwner, testAllocationSite)
	require.ErrorIs(t, err, ErrAllocationAccountMismatch)
	require.Zero(t, second.Snapshot().Used)

	buffer, err := mp.AllocAccounted(
		64, first, testAllocationOwner, testAllocationSite)
	require.NoError(t, err)
	mp.Free(buffer)
	DeleteMPool(mp)
	finalizeTestAllocationAccount(t, firstRegistry, first)
	finalizeTestAllocationAccount(t, secondRegistry, second)
}

func TestMPoolMakeSliceAccounted(t *testing.T) {
	registry, account := newTestAllocationAccount(t, 64, 2)
	mp := MustNew("accounted-typed-slice")
	defer DeleteMPool(mp)

	values, err := MakeSliceAccounted[int64](
		4,
		mp,
		account,
		testAllocationOwner,
		testAllocationSite,
	)
	require.NoError(t, err)
	require.Len(t, values, 4)
	require.Equal(t, uint64(32), account.Snapshot().Used)
	require.Equal(t, uint64(1), registry.LiveAllocationMetadata())
	for i := range values {
		values[i] = int64(i + 1)
	}
	require.Equal(t, []int64{1, 2, 3, 4}, values)

	_, err = MakeSliceAccounted[int64](
		5,
		mp,
		account,
		testAllocationOwner,
		testAllocationSite,
	)
	require.ErrorIs(t, err, ErrAllocationAccountCapacity)
	require.Equal(t, uint64(32), account.Snapshot().Used)
	require.Equal(t, uint64(1), registry.LiveAllocationMetadata())

	empty, err := MakeSliceAccounted[int64](
		0,
		mp,
		account,
		testAllocationOwner,
		testAllocationSite,
	)
	require.NoError(t, err)
	require.Nil(t, empty)
	_, err = MakeSliceAccounted[int64](
		-1,
		mp,
		account,
		testAllocationOwner,
		testAllocationSite,
	)
	require.ErrorIs(t, err, ErrAllocationAccountInvalid)
	_, err = MakeSliceAccounted[struct{}](
		1,
		mp,
		account,
		testAllocationOwner,
		testAllocationSite,
	)
	require.ErrorIs(t, err, ErrAllocationAllocatorLimit)

	FreeSlice(mp, values[:0])
	require.Zero(t, account.Snapshot().Used)
	require.Zero(t, registry.LiveAllocationMetadata())
	finalizeTestAllocationAccount(t, registry, account)
}

func TestMPoolAccountedRollback(t *testing.T) {
	t.Run("account-capacity", func(t *testing.T) {
		registry, account := newTestAllocationAccount(t, 63, 1)
		mp := MustNew("accounted-capacity")
		defer DeleteMPool(mp)

		_, err := mp.AllocAccounted(
			64,
			account,
			testAllocationOwner,
			testAllocationSite,
		)
		require.ErrorIs(t, err, ErrAllocationAccountCapacity)
		require.Contains(t, err.Error(), "owner=1 site=1")
		require.Contains(t, err.Error(), "owner-name=hash_build")
		require.Zero(t, account.Snapshot().Used)
		require.Zero(t, registry.LiveAllocationMetadata())
		finalizeTestAllocationAccount(t, registry, account)
	})

	t.Run("metadata-capacity", func(t *testing.T) {
		registry, account := newTestAllocationAccount(t, 64, 0)
		mp := MustNew("accounted-metadata-capacity")
		defer DeleteMPool(mp)

		_, err := mp.AllocAccounted(
			64,
			account,
			testAllocationOwner,
			testAllocationSite,
		)
		require.ErrorIs(t, err, ErrAllocationMetadataSlots)
		require.Contains(t, err.Error(), "owner=1 site=1")
		require.Zero(t, account.Snapshot().Used)
		owner, ok := account.OwnerUsage(testAllocationOwner)
		require.True(t, ok)
		require.Zero(t, owner.Current)
		// The exact admission was acquired and rolled back before publication.
		require.Equal(t, uint64(64), owner.Peak)
		require.Zero(t, registry.LiveAllocationMetadata())
		finalizeTestAllocationAccount(t, registry, account)
	})

	t.Run("pool-capacity", func(t *testing.T) {
		const allocationSize = 768 << 10
		registry, account := newTestAllocationAccount(
			t,
			2<<20,
			2,
		)
		mp, err := NewMPool("accounted-pool-capacity", 1<<20, NoFixed)
		require.NoError(t, err)
		defer DeleteMPool(mp)

		first, err := mp.AllocAccounted(
			allocationSize,
			account,
			testAllocationOwner,
			testAllocationSite,
		)
		require.NoError(t, err)
		_, err = mp.AllocAccounted(
			allocationSize,
			account,
			testAllocationOwner,
			testAllocationSite,
		)
		require.Error(t, err)
		require.True(t, IsMPoolCapacityFailure(err))
		require.Equal(t, AllocationFailureCapacity, AllocationFailureReasonOf(err))
		require.True(t, IsRetryableAllocationCapacity(err))
		require.Equal(t, uint64(allocationSize), account.Snapshot().Used)
		require.Equal(t, uint64(1), registry.LiveAllocationMetadata())
		require.Equal(t, uint64(2), registry.PeakAllocationMetadata())

		mp.Free(first)
		require.Zero(t, account.Snapshot().Used)
		require.Zero(t, registry.LiveAllocationMetadata())
		finalizeTestAllocationAccount(t, registry, account)
	})

	t.Run("global-capacity", func(t *testing.T) {
		registry, account := newTestAllocationAccount(t, 64, 1)
		mp := MustNew("accounted-global-capacity")
		defer DeleteMPool(mp)

		oldGlobalCap := globalCap.Load()
		globalBefore := GlobalStats().NumCurrBytes.Load()
		globalCap.Store(globalBefore + 63)
		t.Cleanup(func() {
			globalCap.Store(oldGlobalCap)
		})

		_, err := mp.AllocAccounted(
			64,
			account,
			testAllocationOwner,
			testAllocationSite,
		)
		require.Error(t, err)
		require.True(t, IsMPoolCapacityFailure(err))
		require.Equal(t, AllocationFailureCapacity, AllocationFailureReasonOf(err))
		require.True(t, IsRetryableAllocationCapacity(err))
		require.Equal(t, globalBefore, GlobalStats().NumCurrBytes.Load())
		require.Zero(t, mp.CurrNB())
		require.Zero(t, account.Snapshot().Used)
		require.Zero(t, registry.LiveAllocationMetadata())
		finalizeTestAllocationAccount(t, registry, account)
	})

	t.Run("grow-metadata-capacity", func(t *testing.T) {
		registry, account := newTestAllocationAccount(t, 512, 1)
		mp := MustNew("accounted-grow-metadata-capacity")
		defer DeleteMPool(mp)

		buffer, err := mp.AllocAccounted(
			64,
			account,
			testAllocationOwner,
			testAllocationSite,
		)
		require.NoError(t, err)
		copy(buffer, bytes.Repeat([]byte{0x5a}, len(buffer)))

		_, err = mp.Grow(buffer, 128, true)
		require.ErrorIs(t, err, ErrAllocationMetadataSlots)
		require.Equal(t, bytes.Repeat([]byte{0x5a}, len(buffer)), buffer)
		require.Equal(t, uint64(64), account.Snapshot().Used)
		require.Equal(t, uint64(1), registry.LiveAllocationMetadata())

		mp.Free(buffer)
		finalizeTestAllocationAccount(t, registry, account)
	})

	t.Run("realloc-zero-metadata-capacity", func(t *testing.T) {
		registry, account := newTestAllocationAccount(t, 512, 1)
		mp := MustNew("accounted-realloc-zero-metadata-capacity")
		defer DeleteMPool(mp)

		buffer, err := mp.AllocAccounted(
			64,
			account,
			testAllocationOwner,
			testAllocationSite,
		)
		require.NoError(t, err)
		copy(buffer, bytes.Repeat([]byte{0x5a}, len(buffer)))

		_, err = mp.ReallocZero(buffer, 128, true)
		require.ErrorIs(t, err, ErrAllocationMetadataSlots)
		require.Equal(t, bytes.Repeat([]byte{0x5a}, len(buffer)), buffer)
		require.Equal(t, uint64(64), account.Snapshot().Used)
		require.Equal(t, uint64(1), registry.LiveAllocationMetadata())

		mp.Free(buffer)
		finalizeTestAllocationAccount(t, registry, account)
	})

	t.Run("allocator-size-limit-precedes-admission", func(t *testing.T) {
		registry, account := newTestAllocationAccount(t, 0, 1)
		mp := MustNew("accounted-allocator-size-limit")
		defer DeleteMPool(mp)

		_, err := mp.AllocAccounted(
			CapLimit-kMemHdrSz,
			account,
			testAllocationOwner,
			testAllocationSite,
		)
		require.ErrorIs(t, err, ErrAllocationAccountCapacity)
		_, err = mp.AllocAccounted(
			CapLimit-kMemHdrSz+1,
			account,
			testAllocationOwner,
			testAllocationSite,
		)
		require.ErrorIs(t, err, ErrAllocationAllocatorLimit)
		require.NotErrorIs(t, err, ErrAllocationAccountCapacity)
		require.Zero(t, account.Snapshot().Used)
		require.Zero(t, registry.LiveAllocationMetadata())
		finalizeTestAllocationAccount(t, registry, account)
	})
}

func TestMPoolAccountedGrowCapacityBoundary(t *testing.T) {
	const (
		oldCapacity = 64
		required    = 65
	)
	newCapacity, ok := GrowCapacity(oldCapacity, required)
	require.True(t, ok)
	require.Greater(t, newCapacity, int64(required))

	for _, testCase := range []struct {
		name      string
		limit     uint64
		wantError bool
	}{
		{
			name:  "exact-old-plus-rounded-new",
			limit: oldCapacity + uint64(newCapacity),
		},
		{
			name:      "one-byte-short",
			limit:     oldCapacity + uint64(newCapacity) - 1,
			wantError: true,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			registry, account := newTestAllocationAccount(
				t,
				testCase.limit,
				2,
			)
			mp := MustNew("accounted-grow-capacity-boundary")
			defer DeleteMPool(mp)

			buffer, err := mp.AllocAccounted(
				oldCapacity,
				account,
				testAllocationOwner,
				testAllocationSite,
			)
			require.NoError(t, err)
			grown, err := mp.Grow(buffer, required, true)
			if testCase.wantError {
				require.ErrorIs(t, err, ErrAllocationAccountCapacity)
				require.Equal(t, uint64(oldCapacity), account.Snapshot().Used)
				require.Equal(t, uint64(1), registry.LiveAllocationMetadata())
				mp.Free(buffer)
			} else {
				require.NoError(t, err)
				require.Equal(t, newCapacity, int64(cap(grown)))
				require.Equal(t, uint64(newCapacity), account.Snapshot().Used)
				require.Equal(t, testCase.limit, account.Snapshot().Peak)
				mp.Free(grown)
			}
			finalizeTestAllocationAccount(t, registry, account)
		})
	}
}

func TestMPoolAccountedReallocZero(t *testing.T) {
	registry, account := newTestAllocationAccount(t, 512, 2)
	mp := MustNew("accounted-realloc-zero")
	defer DeleteMPool(mp)

	buffer, err := mp.AllocAccounted(
		64,
		account,
		testAllocationOwner,
		testAllocationSite,
	)
	require.NoError(t, err)
	copy(buffer, bytes.Repeat([]byte{0x5a}, 64))

	_, err = mp.ReallocZero(buffer, 128, false)
	require.ErrorIs(t, err, ErrAllocationAccountMismatch)
	require.Equal(t, uint64(64), account.Snapshot().Used)

	replacement, err := mp.ReallocZero(buffer, 128, true)
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte{0x5a}, 64), replacement[:64])
	require.Equal(t, make([]byte, 64), replacement[64:])
	require.Equal(t, uint64(128), account.Snapshot().Used)
	require.Equal(t, uint64(192), account.Snapshot().Peak)
	require.Equal(t, uint64(1), registry.LiveAllocationMetadata())
	require.Equal(t, uint64(2), registry.PeakAllocationMetadata())

	mp.Free(replacement)
	finalizeTestAllocationAccount(t, registry, account)
}

func TestMPoolAccountedSealRejectsGrowth(t *testing.T) {
	registry, account := newTestAllocationAccount(t, 512, 2)
	mp := MustNew("accounted-sealed-growth")
	defer DeleteMPool(mp)

	buffer, err := mp.AllocAccounted(
		64,
		account,
		testAllocationOwner,
		testAllocationSite,
	)
	require.NoError(t, err)
	account.Seal()
	_, err = mp.Grow(buffer, 128, true)
	require.ErrorIs(t, err, ErrAllocationAccountSealed)
	require.Equal(t, uint64(64), account.Snapshot().Used)
	require.Equal(t, uint64(1), registry.LiveAllocationMetadata())

	mp.Free(buffer)
	_, err = registry.Finalize(account)
	require.NoError(t, err)
}

func TestMPoolAccountedCrossPoolAndTeardown(t *testing.T) {
	t.Run("cross-pool", func(t *testing.T) {
		registry, account := newTestAllocationAccount(t, 64, 1)
		owner := MustNew("accounted-cross-owner")
		other := MustNew("accounted-cross-other")
		defer DeleteMPool(owner)
		defer DeleteMPool(other)

		buffer, err := owner.AllocAccounted(
			64,
			account,
			testAllocationOwner,
			testAllocationSite,
		)
		require.NoError(t, err)
		other.Free(buffer)
		require.Panics(t, func() {
			other.Free(buffer)
		})
		require.Zero(t, account.Snapshot().Used)
		require.Zero(t, registry.LiveAllocationMetadata())
		finalizeTestAllocationAccount(t, registry, account)
	})

	t.Run("deleted-owner", func(t *testing.T) {
		registry, account := newTestAllocationAccount(t, 64, 1)
		owner := MustNew("accounted-deleted-owner")
		other := MustNew("accounted-deleted-other")
		defer DeleteMPool(other)

		globalBefore := GlobalStats().NumCurrBytes.Load()
		buffer, err := owner.AllocAccounted(
			64,
			account,
			testAllocationOwner,
			testAllocationSite,
		)
		require.NoError(t, err)
		DeleteMPool(owner)
		require.Equal(t, globalBefore+64, GlobalStats().NumCurrBytes.Load())
		require.Equal(t, uint64(64), account.Snapshot().Used)
		require.Equal(t, uint64(1), registry.LiveAllocationMetadata())

		other.Free(buffer)
		require.Equal(t, globalBefore, GlobalStats().NumCurrBytes.Load())
		require.Zero(t, account.Snapshot().Used)
		require.Zero(t, registry.LiveAllocationMetadata())
		finalizeTestAllocationAccount(t, registry, account)
	})

	t.Run("no-lock-teardown", func(t *testing.T) {
		registry, account := newTestAllocationAccount(t, 64, 1)
		mp := MustNewNoLock("accounted-no-lock-teardown")
		require.NoError(t, mp.BindAllocationAccount(account))
		globalBefore := GlobalStats().NumCurrBytes.Load()
		_, err := mp.AllocAccounted(
			64,
			account,
			testAllocationOwner,
			testAllocationSite,
		)
		require.NoError(t, err)
		DeleteMPool(mp)
		require.Equal(t, globalBefore, GlobalStats().NumCurrBytes.Load())
		require.Zero(t, account.Snapshot().Used)
		require.Zero(t, registry.LiveAllocationMetadata())
		finalizeTestAllocationAccount(t, registry, account)
	})
}

func TestMPoolAccountedConcurrentAllocFree(t *testing.T) {
	const (
		workers = 32
		rounds  = 200
		size    = 64
	)
	registry, account := newTestAllocationAccount(
		t,
		workers*size,
		workers,
	)
	mp := MustNew("accounted-concurrent")
	defer DeleteMPool(mp)

	var wait sync.WaitGroup
	wait.Add(workers)
	for range workers {
		go func() {
			defer wait.Done()
			for range rounds {
				buffer, err := mp.AllocAccounted(
					size,
					account,
					testAllocationOwner,
					testAllocationSite,
				)
				if err != nil {
					t.Errorf("accounted alloc: %v", err)
					return
				}
				mp.Free(buffer)
			}
		}()
	}
	wait.Wait()
	require.Zero(t, account.Snapshot().Used)
	require.Zero(t, registry.LiveAllocationMetadata())
	finalizeTestAllocationAccount(t, registry, account)
}

func BenchmarkMPoolAccountedAllocation(b *testing.B) {
	for _, accounted := range []bool{false, true} {
		mode := "unaccounted"
		if accounted {
			mode = "accounted"
		}
		for _, size := range []int{64, 4 << 10, 16 << 10, 64 << 10} {
			b.Run(fmt.Sprintf("%s/alloc-free/%d", mode, size), func(b *testing.B) {
				mp := MustNew("benchmark-allocation-account")
				defer DeleteMPool(mp)
				var registry *AllocationAccountRegistry
				var account *AllocationAccount
				if accounted {
					var err error
					registry, err = NewAllocationAccountRegistry(1, 1)
					require.NoError(b, err)
					account, err = registry.Open(1 << 60)
					require.NoError(b, err)
				}
				b.ReportAllocs()
				b.SetBytes(int64(size))
				b.ResetTimer()
				for range b.N {
					var buffer []byte
					var allocErr error
					if accounted {
						buffer, allocErr = mp.AllocAccounted(
							size,
							account,
							testAllocationOwner,
							testAllocationSite,
						)
					} else {
						buffer, allocErr = mp.Alloc(size, true)
					}
					if allocErr != nil {
						b.Fatal(allocErr)
					}
					mp.Free(buffer)
				}
				b.StopTimer()
				if accounted {
					finalizeTestAllocationAccount(b, registry, account)
				}
			})
		}

		b.Run(mode+"/grow-replacement", func(b *testing.B) {
			mp := MustNew("benchmark-allocation-account-grow")
			defer DeleteMPool(mp)
			var registry *AllocationAccountRegistry
			var account *AllocationAccount
			if accounted {
				var err error
				registry, err = NewAllocationAccountRegistry(1, 2)
				require.NoError(b, err)
				account, err = registry.Open(1 << 60)
				require.NoError(b, err)
			}
			b.ReportAllocs()
			b.SetBytes(64 << 10)
			b.ResetTimer()
			for range b.N {
				var buffer []byte
				var allocErr error
				if accounted {
					buffer, allocErr = mp.AllocAccounted(
						64,
						account,
						testAllocationOwner,
						testAllocationSite,
					)
				} else {
					buffer, allocErr = mp.Alloc(64, true)
				}
				if allocErr != nil {
					b.Fatal(allocErr)
				}
				buffer, allocErr = mp.Grow(buffer, 64<<10, true)
				if allocErr != nil {
					b.Fatal(allocErr)
				}
				mp.Free(buffer)
			}
			b.StopTimer()
			if accounted {
				finalizeTestAllocationAccount(b, registry, account)
			}
		})
	}

	b.Run("accounted/parallel-alloc-free/65536", func(b *testing.B) {
		mp := MustNew("benchmark-allocation-account-parallel")
		defer DeleteMPool(mp)
		registry, err := NewAllocationAccountRegistry(1, 1024)
		require.NoError(b, err)
		account, err := registry.Open(1 << 60)
		require.NoError(b, err)
		b.ReportAllocs()
		b.SetBytes(64 << 10)
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				buffer, allocErr := mp.AllocAccounted(
					64<<10,
					account,
					testAllocationOwner,
					testAllocationSite,
				)
				if allocErr != nil {
					b.Fatal(allocErr)
				}
				mp.Free(buffer)
			}
		})
		b.StopTimer()
		finalizeTestAllocationAccount(b, registry, account)
	})
}
