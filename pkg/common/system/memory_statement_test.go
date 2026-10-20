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

	metric "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/prometheus/client_golang/prometheus/testutil"
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

func TestStatementPressureUsesWorkingSetWithoutRelaxingHardGate(t *testing.T) {
	for _, source := range []MemoryDomainSource{MemoryDomainSourceCgroupV1, MemoryDomainSourceCgroupV2} {
		for _, scenario := range []string{"clean-cache", "dirty-cache", "raw-runtime-floor", "soft-limit", "pending-claims"} {
			t.Run(string(source)+"/"+scenario, func(t *testing.T) {
				b, s := ciCgroupBudget(source)
				b.statementPolicy = MemoryStatementAdmissionPolicy{PressureConcurrency: 1, MaxQueued: 2}
				_, active, err := b.EnterStatement(t.Context(), false)
				require.NoError(t, err)
				defer active.Close()
				switch scenario {
				case "dirty-cache":
					s.Reclaimable = 0
					s.Used = s.Limit - b.policy.Reserve(*s) - (128 << 20)
				case "raw-runtime-floor":
					s.Used = s.Limit - b.policy.RuntimeBytes
				case "soft-limit":
					s.SoftLimit = s.Used + b.policy.RuntimeBytes
				case "pending-claims":
					_, ok := b.TryReserve(b.Snapshot().Available)
					require.True(t, ok)
					defer func() { b.Release(b.Reserved()) }()
				}
				_, next, err := b.EnterStatement(t.Context(), false)
				if scenario == "clean-cache" {
					require.NoError(t, err, "clean charged cache must not impose the pressure concurrency cap")
					next.Close()
					view := b.Snapshot()
					require.Positive(t, view.Available)
					require.Less(t, view.Available, view.Reserve, "the old pressure decision would throttle here")
					_, ok := b.TryReserve(view.Available + 1)
					require.False(t, ok, "relaxed statement scheduling cannot spend clean-cache credit past the raw gate")
					return
				}
				require.ErrorIs(t, err, ErrMemoryStatementTimeout)
				require.Nil(t, next)
				var detail *MemoryStatementAdmissionError
				require.ErrorAs(t, err, &detail)
				if scenario == "dirty-cache" {
					require.True(t, detail.UnderPressure)
					require.Equal(t, "pressure-concurrency", detail.OrdinaryReason)
					require.Equal(t, uint64(128<<20), detail.WorkingSetAvailable)
					require.Positive(t, detail.Snapshot.Available)
				} else {
					require.False(t, detail.UnderPressure, "healthy working-set headroom does not override hard rejection")
					require.Equal(t, "no-headroom", detail.OrdinaryReason)
					require.Zero(t, detail.RawAvailable)
					_, ok := b.TryReserve(1)
					require.False(t, ok)
				}
			})
		}
	}
}

func TestStatementPressureIncidentShapeRetainsRawAllocationBoundary(t *testing.T) {
	const gib = uint64(1 << 30)
	s := MemoryDomainSnapshot{Used: 793 * gib / 10, Limit: 84 * gib,
		Reclaimable: 30 * gib, NonGo: 23 * gib / 10,
		Source: MemoryDomainSourceCgroupV1, Measured: true}
	b := NewMemoryAdmissionBudgetWithProvider(DefaultMemoryDomainReservePolicy(), func() (MemoryDomainSnapshot, error) { return s, nil })
	b.statementPolicy = MemoryStatementAdmissionPolicy{PressureConcurrency: 28}
	for range 32 {
		_, lease, err := b.EnterStatement(t.Context(), false)
		require.NoError(t, err, "incident's clean-cache shape must not trip the 28-statement cap")
		defer lease.Close()
	}
	view := b.Snapshot()
	require.Equal(t, uint64(32), view.StatementActive)
	require.Less(t, view.Available, view.Reserve)
	require.Zero(t, view.Reserved, "scheduling cannot precharge unallocated bytes")
	_, ok := b.TryReserve(view.Available)
	require.True(t, ok)
	_, ok = b.TryReserve(1)
	require.False(t, ok, "pending claims still exhaust raw headroom despite 30 GiB of cache credit")
	b.Release(view.Available)
}

func TestStatementPressureViewRetainsCalibratedReserveAndPendingClaims(t *testing.T) {
	s := MemoryDomainSnapshot{Used: 1, Limit: 100, NonGo: 20,
		Source: MemoryDomainSourceCgroupV2, Measured: true}
	b := NewMemoryAdmissionBudgetWithProvider(MemoryDomainReservePolicy{
		MinimumBytes: 10, RuntimeBytes: 5, ObservedNonGoMultiplier: 2,
	}, func() (MemoryDomainSnapshot, error) { return s, nil })
	_, ok := b.TryReserve(3)
	require.True(t, ok)
	s.Used, s.NonGo, s.Reclaimable = 70, 8, 20
	b.mu.Lock()
	hardAvailable := b.availableLocked(s)
	pressureAvailable := b.availableForPressureLocked(s)
	b.mu.Unlock()
	require.Equal(t, uint64(2), hardAvailable)
	require.Equal(t, uint64(2), pressureAvailable, "100 - (70-20) - 45 calibrated reserve - 3 pending")
	require.Equal(t, uint64(3), b.Reserved(), "a scheduling read must not spend or release claims")
	b.Release(3)
	b.mu.Lock()
	b.availableLocked(s)
	pressureAvailable = b.availableForPressureLocked(s)
	b.mu.Unlock()
	require.Equal(t, uint64(5), pressureAvailable)
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
		canceled := metric.MemoryStatementRejectedCounter.WithLabelValues("context-canceled")
		before := testutil.ToFloat64(canceled)
		done := make(chan error, 1)
		go func() { _, _, err := b.EnterStatement(ctx, false); done <- err }()
		synctest.Wait()
		require.Equal(t, uint64(1), b.Snapshot().StatementQueued)
		cancel()
		require.ErrorIs(t, <-done, context.Canceled)
		require.Equal(t, before+1, testutil.ToFloat64(canceled))
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

func TestStatementQueueObservesWorkingSetRecoveryWithoutRawChargeChange(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b, sample := statementTestBudget(128 << 20)
		_, active, err := b.EnterStatement(t.Context(), false)
		require.NoError(t, err)
		defer active.Close()
		done := make(chan *MemoryStatementLease, 1)
		go func() {
			_, lease, err := b.EnterStatement(t.Context(), false)
			require.NoError(t, err)
			done <- lease
		}()
		synctest.Wait()
		before := b.Snapshot()
		require.Equal(t, uint64(1), before.StatementQueued)
		s := *sample.Load()
		s.Reclaimable = 1 << 30
		sample.Store(&s)
		lease := <-done // The periodic check sees clean-cache recovery; no Close/Release wakeup.
		require.NotNil(t, lease)
		defer lease.Close()
		after := b.Snapshot()
		require.Equal(t, before.Domain.Used, after.Domain.Used)
		require.Equal(t, s.Limit-s.Used-b.policy.RuntimeBytes, after.Available, "the raw admission gate remains binding")
		require.Less(t, after.Available, after.Reserve, "raw headroom alone would still trigger the old pressure cap")
		require.Equal(t, uint64(2), after.StatementActive)
		require.Zero(t, after.StatementQueued)
		require.Zero(t, after.StatementRejected)
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
		require.Zero(t, detail.RawAvailable)
		require.Zero(t, detail.WorkingSetAvailable)
		require.True(t, detail.UnderPressure)
		s.Used = 0 // Later recovery must not change the failure's observation.
		require.Contains(t, err.Error(), "available=0")
		require.Contains(t, err.Error(), "soft_limit=")
		require.Contains(t, err.Error(), "reclaimable=0 raw_available=0 ws_available=0 pressure=true")
		require.NotZero(t, detail.Snapshot.Domain.Used)
		require.Equal(t, 1, reads, "formatting and unwrapping must not read the domain")
		require.Zero(t, b.controlActive)
		require.Zero(t, b.controlReserved)
	}
}

func TestStatementRejectionsCountTerminalDecisionsOnly(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b, _ := statementTestBudget(0)
		counter := metric.MemoryStatementRejectedCounter.WithLabelValues("no-headroom")
		before := testutil.ToFloat64(counter)
		done := make(chan error, 1)
		go func() { _, _, err := b.EnterStatement(t.Context(), false); done <- err }()
		synctest.Wait()
		time.Sleep(3 * memoryDomainRefreshInterval)
		synctest.Wait()
		require.Equal(t, before, testutil.ToFloat64(counter), "polling is not a terminal rejection")
		require.ErrorIs(t, <-done, ErrMemoryStatementTimeout)
		require.Equal(t, before+1, testutil.ToFloat64(counter))
		require.Equal(t, uint64(1), b.Snapshot().StatementRejected)
		require.Zero(t, b.Snapshot().StatementQueued)
	})
}

func TestStatementRejectionMetricsDistinguishBlockingReasons(t *testing.T) {
	for _, reason := range []string{"domain-unmeasured", "no-headroom", "pressure-concurrency", "queue-order"} {
		t.Run(reason, func(t *testing.T) {
			b, _ := statementTestBudget(1 << 20)
			b.statementPolicy.WaitTimeout = 0
			switch reason {
			case "domain-unmeasured":
				b.domainProvider = func() (MemoryDomainSnapshot, error) { return MemoryDomainSnapshot{}, errors.New("unreadable") }
			case "no-headroom":
				b, _ = statementTestBudget(0)
				b.statementPolicy.WaitTimeout = 0
			case "pressure-concurrency":
				_, lease, err := b.EnterStatement(t.Context(), false)
				require.NoError(t, err)
				defer lease.Close()
			case "queue-order":
				b.statementQueue.PushBack(&memoryStatementWaiter{wake: make(chan struct{}, 1)})
				b.statementPolicy.MaxQueued = 1
			}
			counter := metric.MemoryStatementRejectedCounter.WithLabelValues(reason)
			before := testutil.ToFloat64(counter)
			_, _, err := b.EnterStatement(t.Context(), false)
			var detail *MemoryStatementAdmissionError
			require.ErrorAs(t, err, &detail)
			require.Equal(t, reason, detail.OrdinaryReason)
			require.Equal(t, before+1, testutil.ToFloat64(counter))
			require.Equal(t, uint64(1), b.statementRejected,
				"even an unmeasured domain must count the terminal rejection")
		})
	}
}

func TestStatementRejectionMetricsCountContextDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b, _ := statementTestBudget(0)
		counter := metric.MemoryStatementRejectedCounter.WithLabelValues("deadline-exceeded")
		before := testutil.ToFloat64(counter)
		ctx, cancel := context.WithTimeout(t.Context(), memoryDomainRefreshInterval)
		defer cancel()
		_, _, err := b.EnterStatement(ctx, false)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Equal(t, before+1, testutil.ToFloat64(counter))
		require.Equal(t, uint64(1), b.Snapshot().StatementRejected)
		require.Zero(t, b.Snapshot().StatementQueued)
	})
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
