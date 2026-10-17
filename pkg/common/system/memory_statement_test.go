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

package system

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

func statementTestBudget(free uint64) (*MemoryAdmissionBudget, *atomic.Pointer[MemoryDomainSnapshot]) {
	s := new(atomic.Pointer[MemoryDomainSnapshot])
	s.Store(&MemoryDomainSnapshot{Used: (16 << 30) - (512 << 20) - free,
		Limit: 16 << 30, Source: MemoryDomainSourceCgroupV2, Measured: true})
	b := NewMemoryAdmissionBudgetWithProvider(MemoryDomainReservePolicy{
		MinimumBytes: 256 << 20, RuntimeBytes: 256 << 20, ObservedNonGoMultiplier: 1,
	}, func() (MemoryDomainSnapshot, error) { return *s.Load(), nil })
	b.statementPolicy = MemoryStatementAdmissionPolicy{PressureConcurrency: 1, MaxQueued: 2, WaitTimeout: time.Second}
	return b, s
}

func TestStatementEntryHasNoFixedByteFloor(t *testing.T) {
	b, _ := statementTestBudget(120)
	_, lease, err := b.EnterStatement(t.Context(), false)
	require.NoError(t, err, "120 bytes of actual headroom must not require a 256 MiB entry floor")
	require.Zero(t, b.Reserved(), "statement entry is not a physical allocation")
	_, ok := b.TryReserve(120)
	require.True(t, ok)
	_, ok = b.TryReserve(1)
	require.False(t, ok)
	lease.Close()
	lease.Close()
	require.Equal(t, uint64(120), b.Reserved(), "entry release cannot refund a live allocation")
	b.Release(120)
	require.Zero(t, b.Snapshot().StatementActive)
}

func TestHealthyStatementEntryDoesNotThrottleConcurrency(t *testing.T) {
	b, _ := statementTestBudget(8 << 30)
	leases := make([]*MemoryStatementLease, 0, 32)
	for range 32 {
		_, lease, err := b.EnterStatement(t.Context(), false)
		require.NoError(t, err)
		leases = append(leases, lease)
	}
	require.Equal(t, uint64(32), b.Snapshot().StatementActive)
	for _, lease := range leases {
		lease.Close()
	}
	require.Zero(t, b.Snapshot().StatementActive)
}

func TestHealthyStatementEntryDoesNotAllocateRejectionDiagnostics(t *testing.T) {
	b, _ := statementTestBudget(8 << 30)
	allocs := testing.AllocsPerRun(10, func() {
		_, lease, err := b.EnterStatement(t.Context(), false)
		if err != nil {
			t.Fatal(err)
		}
		lease.Close()
	})
	// Only the admitted lease and its context value belong on the healthy path.
	require.LessOrEqual(t, allocs, 2.0)
}

func TestStatementQueueIsFIFOAndBounded(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b, _ := statementTestBudget(1 << 20)
		_, active, err := b.EnterStatement(t.Context(), false)
		require.NoError(t, err)
		type result struct {
			id    int
			lease *MemoryStatementLease
			err   error
		}
		done := make(chan result, 2)
		for id := range 2 {
			go func() {
				_, lease, err := b.EnterStatement(t.Context(), false)
				done <- result{id, lease, err}
			}()
			synctest.Wait()
		}
		require.Equal(t, uint64(2), b.Snapshot().StatementQueued)
		_, _, err = b.EnterStatement(t.Context(), false)
		require.ErrorIs(t, err, ErrMemoryStatementQueueFull)
		active.Close()
		first := <-done
		require.NoError(t, first.err)
		require.Equal(t, 0, first.id)
		require.Equal(t, uint64(1), b.Snapshot().StatementQueued)
		first.lease.Close()
		second := <-done
		require.NoError(t, second.err)
		require.Equal(t, 1, second.id)
		second.lease.Close()
		require.Zero(t, b.Snapshot().StatementQueued)
		require.Zero(t, b.Snapshot().StatementActive)
	})
}

func TestStatementQueueCancellationAndTimeoutRemoveWaiters(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b, _ := statementTestBudget(0)
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { _, _, err := b.EnterStatement(ctx, false); done <- err }()
		synctest.Wait()
		require.Equal(t, uint64(1), b.Snapshot().StatementQueued)
		cancel()
		require.ErrorIs(t, <-done, context.Canceled)
		require.Zero(t, b.Snapshot().StatementQueued)
		_, _, err := b.EnterStatement(t.Context(), false)
		require.ErrorIs(t, err, ErrMemoryStatementTimeout)
		require.Zero(t, b.Snapshot().StatementQueued)
		require.Equal(t, uint64(2), b.Snapshot().StatementRejected)
	})
}

func TestStatementQueueObservesExternalRecovery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b, sample := statementTestBudget(0)
		done := make(chan *MemoryStatementLease, 1)
		go func() {
			_, lease, err := b.EnterStatement(t.Context(), false)
			require.NoError(t, err)
			done <- lease
		}()
		synctest.Wait()
		s := *sample.Load()
		s.Used -= 1 << 20
		sample.Store(&s)
		lease := <-done // TTL poll, no allocation Release to wake the waiter.
		require.NotNil(t, lease)
		lease.Close()
		require.Zero(t, b.Snapshot().StatementQueued)
	})
}

func TestNestedStatementBorrowsOnlyLiveSameOwner(t *testing.T) {
	b, _ := statementTestBudget(120)
	ctx, parent, err := b.EnterStatement(t.Context(), false)
	require.NoError(t, err)
	_, child, err := b.EnterStatement(ctx, false)
	require.NoError(t, err)
	require.Nil(t, child)
	require.Equal(t, uint64(1), b.Snapshot().StatementActive)
	other, _ := statementTestBudget(0)
	other.statementPolicy.WaitTimeout = 0
	_, _, err = other.EnterStatement(ctx, false)
	require.ErrorIs(t, err, ErrMemoryStatementTimeout)
	parent.Close()
	_, next, err := b.EnterStatement(ctx, false)
	require.NoError(t, err)
	require.NotNil(t, next, "a stale context cannot bypass the next statement's gate")
	next.Close()
}

func TestControlStatementProgressIsBoundedAndSharedWithAllocation(t *testing.T) {
	b, _ := statementTestBudget(0)
	leases := make([]*MemoryStatementLease, 0, memoryControlConcurrency)
	for range memoryControlConcurrency {
		ctx, lease, err := b.EnterStatement(t.Context(), true)
		require.NoError(t, err)
		require.Same(t, lease.control, b.StatementControlMemory(ctx))
		_, ok := lease.control.TryReserve(120)
		require.True(t, ok)
		leases = append(leases, lease)
	}
	b.statementPolicy.WaitTimeout = 0
	_, _, err := b.EnterStatement(t.Context(), true)
	require.ErrorIs(t, err, ErrMemoryStatementTimeout)
	require.Equal(t, memoryControlConcurrency, b.Snapshot().ControlActive)
	for _, lease := range leases {
		lease.Close()
		_, ok := lease.control.TryReserve(1)
		require.False(t, ok)
		lease.control.Release(120)
	}
	require.Zero(t, b.Snapshot().ControlActive)
	require.Zero(t, b.Reserved())
}

func TestStatementAdmissionRejectsExhaustedAndUnknownDomain(t *testing.T) {
	for _, source := range []MemoryDomainSource{MemoryDomainSourceCgroupV1, MemoryDomainSourceCgroupV2, MemoryDomainSourceHost} {
		for _, control := range []bool{false, true} {
			b, sample := statementTestBudget(0)
			b.statementPolicy.WaitTimeout = 0
			s := *sample.Load()
			s.Source, s.SoftLimit = source, s.Used
			sample.Store(&s)
			_, _, err := b.EnterStatement(t.Context(), control)
			require.ErrorIs(t, err, ErrMemoryStatementTimeout)
			require.Zero(t, b.Snapshot().ControlActive)
			b.domainProvider = func() (MemoryDomainSnapshot, error) { return MemoryDomainSnapshot{}, errors.New("unreadable") }
			_, _, err = b.EnterStatement(t.Context(), control)
			require.ErrorIs(t, err, ErrMemoryStatementTimeout)
		}
	}
}

func TestStatementRejectionPreservesDecisionWithoutResampling(t *testing.T) {
	for _, control := range []bool{false, true} {
		b, sample := statementTestBudget(0)
		b.statementPolicy.WaitTimeout = 0
		s := *sample.Load()
		s.SoftLimit = s.Used
		s.ObservedAt = time.Now()
		var reads int
		b.domainProvider = func() (MemoryDomainSnapshot, error) {
			reads++
			return s, nil
		}
		_, _, err := b.EnterStatement(t.Context(), control)
		require.ErrorIs(t, err, ErrMemoryStatementTimeout)
		var detail *MemoryStatementAdmissionError
		require.ErrorAs(t, err, &detail)
		require.Equal(t, s, detail.Snapshot.Domain)
		require.Equal(t, "no-headroom", detail.OrdinaryReason)
		require.Equal(t, control, detail.Control)
		require.Zero(t, detail.ControlAvailable)
		s.Used = 0 // Later recovery must not change the failure's observation.
		require.Contains(t, err.Error(), "available=0")
		require.Contains(t, err.Error(), "soft_limit=")
		require.NotZero(t, detail.Snapshot.Domain.Used)
		require.Equal(t, 1, reads, "formatting and unwrapping must not read the domain")
		require.Zero(t, b.controlActive)
		require.Zero(t, b.controlReserved)
	}
}

func TestStatementRejectionDistinguishesControlSlotExhaustion(t *testing.T) {
	b, _ := statementTestBudget(0)
	b.statementPolicy.WaitTimeout = 0
	for range memoryControlConcurrency {
		_, lease, err := b.EnterStatement(t.Context(), true)
		require.NoError(t, err)
		defer lease.Close()
	}
	_, _, err := b.EnterStatement(t.Context(), true)
	var detail *MemoryStatementAdmissionError
	require.ErrorAs(t, err, &detail)
	require.Equal(t, "slots-exhausted", detail.ControlReason)
	require.Equal(t, memoryControlConcurrency, detail.Snapshot.ControlActive)
	require.Zero(t, detail.Snapshot.ControlReserved)
}
