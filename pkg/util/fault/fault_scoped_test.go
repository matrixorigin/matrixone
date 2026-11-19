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

package fault

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTriggerFaultScopedRequiresEnabledFaultInjection(t *testing.T) {
	require.False(t, Status())
	_, _, injected := TriggerFaultScoped("sc_barrier", IntSelector("sc_tenant", 0))
	require.False(t, injected)
}

func TestTriggerFaultScopedMatchesSelectors(t *testing.T) {
	ctx := context.Background()
	Enable()
	defer Disable()

	require.NoError(t, AddFaultPoint(ctx, "sc_tenant", ":::", "echo", 7, "", false))
	require.NoError(t, AddFaultPoint(ctx, "sc_txn", ":::", "echo", 0, "accepted", false))
	require.NoError(t, AddFaultPoint(ctx, "sc_barrier", "1:1::", "echo", 0, "barrier", false))
	require.NoError(t, AddFaultPoint(ctx, "sc_count", ":::", "getcount", 0, "sc_barrier", false))

	_, _, injected := TriggerFaultScoped("sc_barrier", IntSelector("sc_tenant", 8), StringSelector("sc_txn", "accepted"))
	require.False(t, injected, "a different tenant must not fire the scoped barrier")
	_, _, injected = TriggerFaultScoped("sc_barrier", IntSelector("sc_tenant", 7), StringSelector("sc_txn", "other"))
	require.False(t, injected, "a different transaction must not fire the scoped barrier")
	count, _, ok := TriggerFault("sc_count")
	require.True(t, ok)
	require.Zero(t, count, "nonmatching calls must not consume the barrier's trigger count")

	_, sret, injected := TriggerFaultScoped("sc_barrier", IntSelector("sc_tenant", 7), StringSelector("sc_txn", "accepted"))
	require.True(t, injected)
	require.Equal(t, "barrier", sret)
	_, _, injected = TriggerFaultScoped("sc_barrier", IntSelector("sc_tenant", 7), StringSelector("sc_txn", "accepted"))
	require.False(t, injected, "a single-shot barrier fires exactly once")
	count, _, ok = TriggerFault("sc_count")
	require.True(t, ok)
	// The frequency window gates firing only; every matching trigger still
	// evaluates the barrier once, as with legacy unscoped triggers.
	require.Equal(t, int64(2), count)
}

func TestTriggerFaultScopedUninstalledSelectorDoesNotRestrict(t *testing.T) {
	ctx := context.Background()
	Enable()
	defer Disable()

	require.NoError(t, AddFaultPoint(ctx, "sc_barrier", ":::", "echo", 0, "barrier", false))

	// Without any installed selector the scoped trigger keeps the legacy
	// process-wide behavior.
	_, sret, injected := TriggerFaultScoped("sc_barrier", IntSelector("sc_tenant", 8), StringSelector("sc_txn", "other"))
	require.True(t, injected)
	require.Equal(t, "barrier", sret)

	// A partially installed scope restricts only its own dimension.
	require.NoError(t, AddFaultPoint(ctx, "sc_tenant", ":::", "echo", 7, "", false))
	_, _, injected = TriggerFaultScoped("sc_barrier", IntSelector("sc_tenant", 8), StringSelector("sc_txn", "other"))
	require.False(t, injected)
	_, sret, injected = TriggerFaultScoped("sc_barrier", IntSelector("sc_tenant", 7), StringSelector("sc_txn", "other"))
	require.True(t, injected, "an uninstalled transaction selector must not restrict the trigger")
	require.Equal(t, "barrier", sret)
}

// TestTriggerFaultRawCompositionStraddlesInstallation characterizes why the
// pre-fix CommitWaitInjected was unsafe: composing one TriggerFault call per
// selector with a final trigger is not atomic. A caller admitted past both
// selector checks consumes a matching-scoped barrier installed afterwards.
// The deterministic interleaving below replays the issue 31120 report. Callers
// must use TriggerFaultScoped so selector matching and frequency consumption
// share one faultMap operation.
func TestTriggerFaultRawCompositionStraddlesInstallation(t *testing.T) {
	ctx := context.Background()
	Enable()
	defer Disable()

	// An unrelated caller passes both selector checks before any point is
	// installed. Its process stopped here in the failing shutdown test.
	_, _, selected := TriggerFault("sc_tenant")
	require.False(t, selected)
	_, _, selected = TriggerFault("sc_txn")
	require.False(t, selected)

	// The controller then arms the scope and the single-shot barrier.
	require.NoError(t, AddFaultPoint(ctx, "sc_tenant", ":::", "echo", 7, "", false))
	require.NoError(t, AddFaultPoint(ctx, "sc_txn", ":::", "echo", 0, "accepted", false))
	require.NoError(t, AddFaultPoint(ctx, "sc_barrier", "1:1::", "echo", 0, "barrier", false))
	require.NoError(t, AddFaultPoint(ctx, "sc_count", ":::", "getcount", 0, "sc_barrier", false))

	// The already admitted caller consumes the new barrier although the
	// installed selectors exclude it.
	_, _, injected := TriggerFault("sc_barrier")
	require.True(t, injected, "characterization: raw composed triggers observe separate faultMap snapshots")
	count, _, ok := TriggerFault("sc_count")
	require.True(t, ok)
	require.Equal(t, int64(1), count, "the unrelated call spent the single-shot trigger count")
}

// TestTriggerFaultScopedInFlightInstallation replays the same interleaving
// against the atomic operation: an unrelated call in flight while the scope
// and the barrier are installed can never consume the barrier. Installation
// order mirrors the shutdown test: tenant selector, transaction selector,
// then the barrier, so every observable snapshot either lacks the barrier or
// already excludes the tenant-0 caller.
func TestTriggerFaultScopedInFlightInstallation(t *testing.T) {
	ctx := context.Background()
	Enable()
	defer Disable()

	for round := 0; round < 50; round++ {
		victim := make(chan bool, 1)
		go func() {
			_, _, injected := TriggerFaultScoped("sc_barrier",
				IntSelector("sc_tenant", 0), StringSelector("sc_txn", "other"))
			victim <- injected
		}()
		require.NoError(t, AddFaultPoint(ctx, "sc_tenant", ":::", "echo", 7, "", false))
		require.NoError(t, AddFaultPoint(ctx, "sc_txn", ":::", "echo", 0, "accepted", false))
		require.NoError(t, AddFaultPoint(ctx, "sc_barrier", "1:1::", "echo", 0, "barrier", false))
		require.False(t, <-victim, "round %d: the in-flight unrelated call must not consume the scoped barrier", round)

		_, sret, injected := TriggerFaultScoped("sc_barrier",
			IntSelector("sc_tenant", 7), StringSelector("sc_txn", "accepted"))
		require.True(t, injected, "round %d: the accepted transaction must still claim the barrier", round)
		require.Equal(t, "barrier", sret)
		_, _, injected = TriggerFaultScoped("sc_barrier",
			IntSelector("sc_tenant", 7), StringSelector("sc_txn", "accepted"))
		require.False(t, injected, "round %d: the single-shot barrier fires exactly once", round)

		for _, point := range []string{"sc_tenant", "sc_txn", "sc_barrier"} {
			_, err := RemoveFaultPoint(ctx, point)
			require.NoError(t, err)
		}
	}
}

// TestIssue31120ScopedAcceptance runs the shared issue 31120 behavioral
// acceptance oracle (issue31120_red_repro_test.go) against the production
// composition - one atomic TriggerFaultScoped call carrying both selectors,
// the same composition objectio.CommitWaitInjected uses. Same harness, same
// deterministic interleaving, same assertions as the red proof; expected to
// pass.
func TestIssue31120ScopedAcceptance(t *testing.T) {
	issue31120AcceptanceOracle(t, issue31120ScopedCommitWaitInjected)
}

func issue31120ScopedCommitWaitInjected(tenantID uint32, txnID string) (string, bool) {
	if issue31120ProbeHook != nil {
		issue31120ProbeHook()
	}
	_, sret, injected := TriggerFaultScoped(issue31120Barrier,
		IntSelector(issue31120TenantPoint, int64(tenantID)),
		StringSelector(issue31120TxnPoint, txnID))
	return sret, injected
}

// TestTriggerFaultScopedWaitReleaseAndRemoval keeps the shutdown test's
// WAIT/GETWAITERS/NOTIFYALL plumbing intact through the atomic trigger:
// the matching caller registers exactly one waiter, NOTIFYALL releases it,
// and removing the barrier wakes a later waiter instead of hanging it.
func TestTriggerFaultScopedWaitReleaseAndRemoval(t *testing.T) {
	ctx := context.Background()
	Enable()
	defer Disable()

	require.NoError(t, AddFaultPoint(ctx, "sc_tenant", ":::", "echo", 7, "", false))
	require.NoError(t, AddFaultPoint(ctx, "sc_txn", ":::", "echo", 0, "accepted", false))
	require.NoError(t, AddFaultPoint(ctx, "sc_barrier", ":::", "wait", 0, "", false))
	require.NoError(t, AddFaultPoint(ctx, "sc_waiters", ":::", "getwaiters", 0, "sc_barrier", false))
	require.NoError(t, AddFaultPoint(ctx, "sc_release", ":::", "notifyall", 0, "sc_barrier", false))

	waiterCount := func() int64 {
		count, _, ok := TriggerFault("sc_waiters")
		require.True(t, ok)
		return count
	}

	done := make(chan bool, 1)
	go func() {
		_, _, injected := TriggerFaultScoped("sc_barrier",
			IntSelector("sc_tenant", 7), StringSelector("sc_txn", "accepted"))
		done <- injected
	}()
	require.Eventually(t, func() bool { return waiterCount() == 1 }, 5*time.Second, 10*time.Millisecond)

	// A nonmatching call neither waits nor registers another waiter.
	_, _, injected := TriggerFaultScoped("sc_barrier",
		IntSelector("sc_tenant", 8), StringSelector("sc_txn", "accepted"))
	require.False(t, injected)
	require.Equal(t, int64(1), waiterCount())

	_, _, released := TriggerFault("sc_release")
	require.True(t, released)
	require.True(t, <-done, "NOTIFYALL releases the matching waiter with injected=true")
	require.Eventually(t, func() bool { return waiterCount() == 0 }, 5*time.Second, 10*time.Millisecond)

	// Removing a WAIT barrier wakes a later matching waiter.
	done2 := make(chan bool, 1)
	go func() {
		_, _, injected := TriggerFaultScoped("sc_barrier",
			IntSelector("sc_tenant", 7), StringSelector("sc_txn", "accepted"))
		done2 <- injected
	}()
	require.Eventually(t, func() bool { return waiterCount() == 1 }, 5*time.Second, 10*time.Millisecond)
	removed, err := RemoveFaultPoint(ctx, "sc_barrier")
	require.NoError(t, err)
	require.True(t, removed)
	require.True(t, <-done2, "removing the barrier releases the matching waiter with injected=true")
}
