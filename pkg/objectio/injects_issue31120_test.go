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

package objectio

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/stretchr/testify/require"
)

// TestCommitWaitInjectedSelectorsAreNotTriggeredSeparately pins the atomicity
// contract: a helper that matched each selector with its own TriggerFault
// (the pre-issue-31120 implementation) increments every selector point's
// trigger count per call, evaluating selectors and the barrier against
// separate faultMap snapshots. The atomic evaluation leaves both counts at
// zero while still scoping the barrier.
func TestCommitWaitInjectedSelectorsAreNotTriggeredSeparately(t *testing.T) {
	faultEnabledHere := fault.Enable()
	t.Cleanup(func() {
		for _, point := range []string{
			FJ_CommitWait, FJ_CommitWaitTargetTenant, FJ_CommitWaitTargetTxn,
			"issue31120_tenant_count", "issue31120_txn_count",
		} {
			_, _ = fault.RemoveFaultPoint(t.Context(), point)
		}
		if faultEnabledHere {
			fault.Disable()
		}
	})

	require.NoError(t, fault.AddFaultPoint(t.Context(), FJ_CommitWaitTargetTenant, ":::", "ECHO", 7, "", false))
	require.NoError(t, fault.AddFaultPoint(t.Context(), FJ_CommitWaitTargetTxn, ":::", "ECHO", 0, "accepted", false))
	require.NoError(t, fault.AddFaultPoint(t.Context(), FJ_CommitWait, ":::", "ECHO", 0, "barrier", false))
	require.NoError(t, fault.AddFaultPoint(t.Context(), "issue31120_tenant_count", ":::", "GETCOUNT", 0, FJ_CommitWaitTargetTenant, false))
	require.NoError(t, fault.AddFaultPoint(t.Context(), "issue31120_txn_count", ":::", "GETCOUNT", 0, FJ_CommitWaitTargetTxn, false))

	_, injected := CommitWaitInjected(0, "other")
	require.False(t, injected)
	message, injected := CommitWaitInjected(7, "accepted")
	require.True(t, injected)
	require.Equal(t, "barrier", message)

	count, _, ok := fault.TriggerFault("issue31120_tenant_count")
	require.True(t, ok)
	require.Zero(t, count, "the tenant selector must be read as configuration, not triggered separately")
	count, _, ok = fault.TriggerFault("issue31120_txn_count")
	require.True(t, ok)
	require.Zero(t, count, "the transaction selector must be read as configuration, not triggered separately")
}

// TestCommitWaitInjectedInFlightInstallation replays issue 31120 at the
// helper boundary: a background commit of an unrelated tenant is already in
// flight while the test arms both selectors and the single-shot barrier. In
// every snapshot of that race the unrelated call must return injected=false
// while the accepted transaction still claims the barrier exactly once.
func TestCommitWaitInjectedInFlightInstallation(t *testing.T) {
	faultEnabledHere := fault.Enable()
	t.Cleanup(func() {
		for _, point := range []string{FJ_CommitWait, FJ_CommitWaitTargetTenant, FJ_CommitWaitTargetTxn} {
			_, _ = fault.RemoveFaultPoint(t.Context(), point)
		}
		if faultEnabledHere {
			fault.Disable()
		}
	})

	for round := 0; round < 30; round++ {
		victim := make(chan bool, 1)
		go func() {
			_, injected := CommitWaitInjected(0, "other")
			victim <- injected
		}()
		require.NoError(t, fault.AddFaultPoint(t.Context(), FJ_CommitWaitTargetTenant, ":::", "ECHO", 7, "", false))
		require.NoError(t, fault.AddFaultPoint(t.Context(), FJ_CommitWaitTargetTxn, ":::", "ECHO", 0, "accepted", false))
		require.NoError(t, fault.AddFaultPoint(t.Context(), FJ_CommitWait, "1:1::", "ECHO", 0, "barrier", false))
		require.False(t, <-victim, "round %d: the in-flight tenant 0 call must not consume the scoped barrier", round)

		message, injected := CommitWaitInjected(7, "accepted")
		require.True(t, injected, "round %d: the accepted transaction must still claim the barrier", round)
		require.Equal(t, "barrier", message)
		_, injected = CommitWaitInjected(7, "accepted")
		require.False(t, injected, "round %d: the single-shot barrier fires exactly once", round)

		for _, point := range []string{FJ_CommitWait, FJ_CommitWaitTargetTenant, FJ_CommitWaitTargetTxn} {
			_, err := fault.RemoveFaultPoint(t.Context(), point)
			require.NoError(t, err)
		}
	}
}

// TestCommitWaitInjectedWaitRelease keeps the shutdown test's
// WAIT/GETWAITERS/NOTIFYALL plumbing intact through the helper: the accepted
// commit registers exactly one barrier waiter, NOTIFYALL releases it, and
// removing the barrier wakes a later waiter instead of hanging it.
func TestCommitWaitInjectedWaitRelease(t *testing.T) {
	faultEnabledHere := fault.Enable()
	waiters := "issue31120_commit_waiters"
	release := "issue31120_commit_release"
	t.Cleanup(func() {
		for _, point := range []string{FJ_CommitWait, FJ_CommitWaitTargetTenant, FJ_CommitWaitTargetTxn, waiters, release} {
			_, _ = fault.RemoveFaultPoint(t.Context(), point)
		}
		if faultEnabledHere {
			fault.Disable()
		}
	})

	require.NoError(t, fault.AddFaultPoint(t.Context(), FJ_CommitWaitTargetTenant, ":::", "ECHO", 7, "", false))
	require.NoError(t, fault.AddFaultPoint(t.Context(), FJ_CommitWaitTargetTxn, ":::", "ECHO", 0, "accepted", false))
	require.NoError(t, fault.AddFaultPoint(t.Context(), FJ_CommitWait, ":::", "WAIT", 0, "", false))
	require.NoError(t, fault.AddFaultPoint(t.Context(), waiters, ":::", "GETWAITERS", 0, FJ_CommitWait, false))
	require.NoError(t, fault.AddFaultPoint(t.Context(), release, ":::", "NOTIFYALL", 0, FJ_CommitWait, false))

	waiterCount := func() int64 {
		count, _, ok := fault.TriggerFault(waiters)
		require.True(t, ok)
		return count
	}

	done := make(chan bool, 1)
	go func() {
		_, injected := CommitWaitInjected(7, "accepted")
		done <- injected
	}()
	require.Eventually(t, func() bool { return waiterCount() == 1 }, 5*time.Second, 10*time.Millisecond)

	// An unrelated tenant neither waits on the barrier nor registers a waiter.
	_, injected := CommitWaitInjected(8, "accepted")
	require.False(t, injected)
	require.Equal(t, int64(1), waiterCount())

	_, _, released := fault.TriggerFault(release)
	require.True(t, released)
	require.True(t, <-done, "NOTIFYALL releases the accepted commit with injected=true")
	require.Eventually(t, func() bool { return waiterCount() == 0 }, 5*time.Second, 10*time.Millisecond)

	// Removing the barrier wakes a later accepted commit (the test cleanup
	// path of the shutdown test relies on this to avoid hangs).
	done2 := make(chan bool, 1)
	go func() {
		_, injected := CommitWaitInjected(7, "accepted")
		done2 <- injected
	}()
	require.Eventually(t, func() bool { return waiterCount() == 1 }, 5*time.Second, 10*time.Millisecond)
	removed, err := fault.RemoveFaultPoint(t.Context(), FJ_CommitWait)
	require.NoError(t, err)
	require.True(t, removed)
	require.True(t, <-done2, "removing the barrier releases the accepted commit with injected=true")
}
