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
	"errors"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
)

type testAllocationCapacityController struct {
	used           atomic.Uint64
	reject         atomic.Bool
	acquireStarted chan struct{}
	acquireProceed chan struct{}
	releaseStarted chan struct{}
	releaseProceed chan struct{}
}

func (c *testAllocationCapacityController) AcquireAllocationCapacity(
	capacity uint64,
) error {
	if c.reject.Load() {
		return ErrAllocationAccountCapacity
	}
	c.used.Add(capacity)
	if c.acquireStarted != nil {
		close(c.acquireStarted)
		<-c.acquireProceed
	}
	return nil
}

func (c *testAllocationCapacityController) ReleaseAllocationCapacity(
	capacity uint64,
) {
	c.used.Add(^uint64(capacity - 1))
	if c.releaseStarted != nil {
		close(c.releaseStarted)
		<-c.releaseProceed
	}
}

func TestAllocationAccountRegistryLifecycle(t *testing.T) {
	registry, err := NewAllocationAccountRegistry(1, 2)
	require.NoError(t, err)

	account, err := registry.Open(128)
	require.NoError(t, err)
	firstHandle := account.Handle()
	resolved, ok := registry.Resolve(firstHandle)
	require.True(t, ok)
	require.Same(t, account, resolved)

	_, err = registry.Open(128)
	require.ErrorIs(t, err, ErrAllocationGenerationSlots)
	_, err = registry.Finalize(account)
	require.ErrorIs(t, err, ErrAllocationAccountLive)

	require.NoError(t, account.acquire(64))
	require.NoError(t, account.acquire(64))
	err = account.acquire(1)
	require.ErrorIs(t, err, ErrAllocationAccountCapacity)
	require.Equal(t, AllocationAccountSnapshot{
		Handle: firstHandle,
		Limit:  128,
		Used:   128,
		Peak:   128,
	}, account.Snapshot())

	account.release(64)
	sealed := account.Seal()
	require.True(t, sealed.Sealed)
	require.Equal(t, uint64(64), sealed.Used)
	require.ErrorIs(t, account.acquire(1), ErrAllocationAccountSealed)
	// A zero-byte request is always a no-op, including after Seal.
	require.NoError(t, account.acquire(0))

	account.release(64)
	final, err := registry.Finalize(account)
	require.NoError(t, err)
	require.True(t, final.Sealed)
	require.Zero(t, final.Used)
	_, ok = registry.Resolve(firstHandle)
	require.False(t, ok)
	_, err = registry.Finalize(account)
	require.ErrorIs(t, err, ErrAllocationAccountStale)

	reused, err := registry.Open(128)
	require.NoError(t, err)
	require.NotEqual(t, firstHandle, reused.Handle())
	require.Equal(t, firstHandle.slot(), reused.Handle().slot())
	require.Equal(t, firstHandle.generation()+1, reused.Handle().generation())
	reused.Seal()
	_, err = registry.Finalize(reused)
	require.NoError(t, err)
}

func TestAllocationAccountControllerRollback(t *testing.T) {
	registry, err := NewAllocationAccountRegistry(1, 1)
	require.NoError(t, err)
	controller := &testAllocationCapacityController{}
	account, err := registry.OpenWithController(8, controller)
	require.NoError(t, err)

	controller.reject.Store(true)
	require.ErrorIs(t, account.acquire(1), ErrAllocationAccountCapacity)
	require.Zero(t, controller.used.Load())
	require.Zero(t, account.Snapshot().Used)

	controller.reject.Store(false)
	controller.acquireStarted = make(chan struct{})
	controller.acquireProceed = make(chan struct{})
	acquireResult := make(chan error, 1)
	go func() {
		acquireResult <- account.acquire(1)
	}()
	<-controller.acquireStarted
	sealed := make(chan struct{})
	go func() {
		account.Seal()
		close(sealed)
	}()
	for !account.Snapshot().Sealed {
		runtime.Gosched()
	}
	close(controller.acquireProceed)
	require.ErrorIs(t, <-acquireResult, ErrAllocationAccountSealed)
	<-sealed
	require.Zero(t, controller.used.Load())
	require.Zero(t, account.Snapshot().Used)
	_, err = registry.Finalize(account)
	require.NoError(t, err)
}

func TestAllocationAccountFinalizeWaitsForRelease(t *testing.T) {
	registry, err := NewAllocationAccountRegistry(1, 1)
	require.NoError(t, err)
	controller := &testAllocationCapacityController{
		releaseStarted: make(chan struct{}),
		releaseProceed: make(chan struct{}),
	}
	t.Cleanup(func() {
		select {
		case <-controller.releaseProceed:
		default:
			close(controller.releaseProceed)
		}
	})
	account, err := registry.OpenWithController(1, controller)
	require.NoError(t, err)
	require.NoError(t, account.acquire(1))
	require.NoError(t, registry.reserveMetadata())
	account.Seal()

	released := make(chan struct{})
	go func() {
		defer close(released)
		allocationLease{account: account, owner: 1, site: 1}.release(1)
	}()
	<-controller.releaseStarted
	require.Equal(t, uint64(1), account.Snapshot().Used)
	_, err = registry.Finalize(account)
	require.ErrorIs(t, err, ErrAllocationAccountLive)

	close(controller.releaseProceed)
	<-released
	_, err = registry.Finalize(account)
	require.NoError(t, err)
}

func TestAllocationAccountRegistryBounds(t *testing.T) {
	_, err := NewAllocationAccountRegistry(0, 1)
	require.ErrorIs(t, err, ErrAllocationAccountInvalid)

	registry, err := NewAllocationAccountRegistry(1, 0)
	require.NoError(t, err)
	_, err = registry.Open(allocationAccountUsedMask + 1)
	require.ErrorIs(t, err, ErrAllocationAccountInvalid)

	account, err := registry.Open(1)
	require.NoError(t, err)
	require.NoError(t, account.acquire(1))
	require.ErrorIs(t, registry.reserveMetadata(), ErrAllocationMetadataSlots)
	account.release(1)
	account.Seal()
	_, err = registry.Finalize(account)
	require.NoError(t, err)

	registry.mu.Lock()
	registry.generations[1] = math.MaxUint32
	registry.mu.Unlock()
	_, err = registry.Open(1)
	require.ErrorIs(t, err, ErrAllocationGenerationSlots)
}

func TestAllocationAccountRequestValidation(t *testing.T) {
	registry, account := newTestAllocationAccount(t, 64, 1)
	mp := MustNew("accounted-request-validation")
	defer DeleteMPool(mp)

	for _, testCase := range []struct {
		name    string
		account *AllocationAccount
		owner   AllocationOwner
		site    AllocationSite
	}{
		{name: "nil-account", owner: 1, site: 1},
		{name: "zero-owner", account: account, site: 1},
		{name: "owner-out-of-range", account: account, owner: 64, site: 1},
		{name: "zero-site", account: account, owner: 1},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := mp.AllocAccounted(
				64,
				testCase.account,
				testCase.owner,
				testCase.site,
			)
			require.ErrorIs(t, err, ErrAllocationAccountInvalid)
			require.Zero(t, account.Snapshot().Used)
			require.Zero(t, registry.LiveAllocationMetadata())
		})
	}
	finalizeTestAllocationAccount(t, registry, account)
	_, err := mp.AllocAccounted(
		64,
		account,
		testAllocationOwner,
		testAllocationSite,
	)
	require.ErrorIs(t, err, ErrAllocationAccountStale)
}

func TestAllocationAccountSealLinearization(t *testing.T) {
	const contenders = 128

	registry, err := NewAllocationAccountRegistry(1, contenders)
	require.NoError(t, err)
	account, err := registry.Open(contenders)
	require.NoError(t, err)

	start := make(chan struct{})
	sealed := make(chan struct{})
	var wait sync.WaitGroup
	var acquired atomic.Uint64
	wait.Add(contenders)
	for range contenders {
		go func() {
			defer wait.Done()
			<-start
			err := account.acquire(1)
			if err == nil {
				acquired.Add(1)
				<-sealed
				account.release(1)
				return
			}
			if !errors.Is(err, ErrAllocationAccountSealed) {
				t.Errorf("unexpected acquire error: %v", err)
			}
		}()
	}

	close(start)
	snapshot := account.Seal()
	close(sealed)
	wait.Wait()
	require.Equal(t, acquired.Load(), snapshot.Used)
	require.Equal(t, acquired.Load(), snapshot.Peak)
	require.Zero(t, account.Snapshot().Used)
	_, err = registry.Finalize(account)
	require.NoError(t, err)
}

func TestAllocationAccountReleaseUnderflow(t *testing.T) {
	registry, err := NewAllocationAccountRegistry(1, 1)
	require.NoError(t, err)
	account, err := registry.Open(1)
	require.NoError(t, err)
	require.Panics(t, func() {
		account.release(1)
	})
	account.Seal()
	_, err = registry.Finalize(account)
	require.NoError(t, err)
}

func TestAllocationAccountTerminalTombstoneSuspendsAdmission(t *testing.T) {
	registry, err := NewAllocationAccountRegistry(3, 2)
	require.NoError(t, err)
	account, err := registry.Open(64)
	require.NoError(t, err)
	require.NoError(t, account.acquire(64))

	snapshot, first, err := registry.CompleteTerminal(account)
	require.ErrorIs(t, err, ErrAllocationAccountInvariant)
	require.True(t, first)
	require.Equal(t, AllocationAccountTerminalInvariantFailure, snapshot.State)
	require.Equal(t, uint64(64), snapshot.Used)
	require.True(t, snapshot.Sealed)
	require.True(t, registry.AdmissionSuspended())
	require.Equal(t, uint32(1), registry.LiveTombstones())
	require.ErrorIs(t, account.acquire(1), ErrAllocationAccountSealed)
	_, err = registry.Open(64)
	require.ErrorIs(t, err, ErrAllocationAdmissionSuspended)

	repeated, first, err := registry.CompleteTerminal(account)
	require.ErrorIs(t, err, ErrAllocationAccountInvariant)
	require.False(t, first)
	require.Equal(t, snapshot, repeated)

	account.release(64)
	require.False(t, registry.AdmissionSuspended())
	require.Zero(t, registry.LiveTombstones())
	_, ok := registry.Resolve(snapshot.Handle)
	require.False(t, ok)

	next, err := registry.Open(64)
	require.NoError(t, err)
	require.NotEqual(t, snapshot.Handle, next.Handle())
	valid, first, err := registry.CompleteTerminal(next)
	require.NoError(t, err)
	require.True(t, first)
	require.Equal(t, AllocationAccountTerminalValid, valid.State)
	require.Zero(t, valid.Used)
	_, ok = registry.Resolve(valid.Handle)
	require.False(t, ok)
}

func TestAllocationAccountZeroLiveOwnerInvariantExportsFailure(t *testing.T) {
	registry, err := NewAllocationAccountRegistry(1, 1)
	require.NoError(t, err)
	account, err := registry.Open(1)
	require.NoError(t, err)
	ownerErr := errors.New("owner teardown failed")

	snapshot, first, err := registry.CompleteTerminalWithError(account, ownerErr)
	require.True(t, first)
	require.ErrorIs(t, err, ownerErr)
	require.ErrorIs(t, err, ErrAllocationAccountInvariant)
	require.Equal(t, AllocationAccountTerminalInvariantFailure, snapshot.State)
	require.Zero(t, snapshot.Used)
	require.False(t, registry.AdmissionSuspended())
	_, ok := registry.Resolve(account.Handle())
	require.False(t, ok)
}

func TestAllocationAccountMultipleTombstonesDrainBeforeResume(t *testing.T) {
	registry, err := NewAllocationAccountRegistry(3, 2)
	require.NoError(t, err)
	first, err := registry.Open(64)
	require.NoError(t, err)
	second, err := registry.Open(64)
	require.NoError(t, err)
	require.NoError(t, first.acquire(1))
	require.NoError(t, second.acquire(1))

	_, created, err := registry.CompleteTerminal(first)
	require.ErrorIs(t, err, ErrAllocationAccountInvariant)
	require.True(t, created)
	_, created, err = registry.CompleteTerminal(second)
	require.ErrorIs(t, err, ErrAllocationAccountInvariant)
	require.True(t, created)
	require.Equal(t, uint32(2), registry.LiveTombstones())

	first.release(1)
	require.True(t, registry.AdmissionSuspended())
	require.Equal(t, uint32(1), registry.LiveTombstones())
	second.release(1)
	require.False(t, registry.AdmissionSuspended())
	require.Zero(t, registry.LiveTombstones())
}

func TestAllocationAccountOpenSuspendLinearization(t *testing.T) {
	const contenders = 128

	registry, err := NewAllocationAccountRegistry(contenders+1, 1)
	require.NoError(t, err)
	leaked, err := registry.Open(1)
	require.NoError(t, err)
	require.NoError(t, leaked.acquire(1))

	start := make(chan struct{})
	opened := make(chan *AllocationAccount, contenders)
	var wait sync.WaitGroup
	wait.Add(contenders)
	for range contenders {
		go func() {
			defer wait.Done()
			<-start
			account, openErr := registry.Open(1)
			if openErr == nil {
				opened <- account
				return
			}
			if !errors.Is(openErr, ErrAllocationAdmissionSuspended) &&
				!errors.Is(openErr, ErrAllocationGenerationSlots) {
				t.Errorf("unexpected open error: %v", openErr)
			}
		}()
	}
	close(start)
	_, first, err := registry.CompleteTerminal(leaked)
	require.ErrorIs(t, err, ErrAllocationAccountInvariant)
	require.True(t, first)
	wait.Wait()
	close(opened)

	_, err = registry.Open(1)
	require.ErrorIs(t, err, ErrAllocationAdmissionSuspended)
	for account := range opened {
		_, _, finishErr := registry.CompleteTerminal(account)
		require.NoError(t, finishErr)
	}
	leaked.release(1)
	require.False(t, registry.AdmissionSuspended())
}

func TestAllocationFailureReasonsAreNonOverlapping(t *testing.T) {
	testCases := []struct {
		err       error
		reason    AllocationFailureReason
		retryable bool
	}{
		{ErrAllocationAccountCapacity, AllocationFailureCapacity, true},
		{ErrAllocationMetadataSlots, AllocationFailureCapacity, true},
		{moerr.NewMPoolCapacityNoCtxf("test"), AllocationFailureCapacity, true},
		{ErrAllocationAccountSealed, AllocationFailureSealed, false},
		{ErrAllocationAccountMismatch, AllocationFailureMismatch, false},
		{ErrAllocationAllocatorLimit, AllocationFailureAllocatorLimit, false},
		{ErrAllocationAccountInvariant, AllocationFailureInvariant, false},
		{ErrAllocationAccountInvalid, AllocationFailureInvariant, false},
		{ErrAllocationAccountStale, AllocationFailureInvariant, false},
		{ErrAllocationAccountLive, AllocationFailureInvariant, false},
		{ErrAllocationGenerationSlots, AllocationFailureInvariant, false},
		{errors.Join(moerr.NewMPoolCapacityNoCtxf("test"), ErrAllocationAccountInvariant), AllocationFailureInvariant, false},
		{errors.Join(moerr.NewMPoolCapacityNoCtxf("test"), ErrAllocationAccountInvalid), AllocationFailureInvariant, false},
		{errors.Join(moerr.NewMPoolCapacityNoCtxf("test"), ErrAllocationAccountSealed), AllocationFailureSealed, false},
		{ErrAllocationAdmissionSuspended, AllocationFailureSuspended, false},
		{errors.New("unrelated"), AllocationFailureNone, false},
	}
	for _, testCase := range testCases {
		require.Equal(t, testCase.reason, AllocationFailureReasonOf(testCase.err))
		require.Equal(t, testCase.retryable, IsRetryableAllocationCapacity(testCase.err))
	}

	// A terminal invariant dominates a joined underlying capacity error, so a
	// failed terminal cleanup can never enter the pressure retry loop.
	joined := errors.Join(
		ErrAllocationAccountCapacity,
		ErrAllocationAccountInvariant,
	)
	require.Equal(t, AllocationFailureInvariant, AllocationFailureReasonOf(joined))
	require.False(t, IsRetryableAllocationCapacity(joined))
}
