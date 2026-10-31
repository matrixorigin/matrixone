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
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Deterministic issue 31120 reproduction and its behavioral acceptance
// oracle. Only pre-existing fault package API is used, so this file also
// builds against the pre-fix parent (a1562a07), whose fault package predates
// TriggerFaultScoped.

// Point names mirror the production commit-wait scope
// (pkg/objectio/injects.go: FJ_CommitWaitTargetTenant, FJ_CommitWaitTargetTxn,
// FJ_CommitWait). Local replicas keep this file independent of objectio,
// which imports this package.
const (
	issue31120TenantPoint = "fj/commit/wait-target-tenant"
	issue31120TxnPoint    = "fj/commit/wait-target-txn"
	issue31120Barrier     = "fj/commit/wait"
)

// issue31120ProbeHook, when non-nil, runs in the trigger adapters immediately
// before the barrier trigger - the controlled-scheduling point named in the
// issue's reproduction ("a test-only pause immediately before the final
// trigger"). Only the acceptance oracle sets it, before spawning the victim
// goroutine, and clears it after collecting the victim result.
var issue31120ProbeHook func()

// issue31120LegacyCommitWaitInjected preserves the pre-fix helper body from
// origin/main a1562a07 pkg/objectio/injects.go (func CommitWaitInjected),
// retargeted to the local replicas, plus the test-only pause. Three
// independent TriggerFault round trips are the defect: an in-flight caller
// past both selector probes consumes a scoped barrier installed afterwards.
func issue31120LegacyCommitWaitInjected(tenantID uint32, txnID string) (string, bool) {
	if target, _, selected := TriggerFault(issue31120TenantPoint); selected && uint32(target) != tenantID {
		return "", false
	}
	if _, target, selected := TriggerFault(issue31120TxnPoint); selected && target != txnID {
		return "", false
	}
	if issue31120ProbeHook != nil {
		issue31120ProbeHook()
	}
	_, sarg, injected := TriggerFault(issue31120Barrier)
	return sarg, injected
}

// issue31120AcceptanceOracle is the behavioral acceptance check shared by the
// red proof (legacy composition, expected failure) and the green proof
// (TriggerFaultScoped adapter, expected pass):
//
//	(a) an in-flight nonmatching call admitted before arming must neither
//	    consume nor fire the scoped single-shot barrier;
//	(b) the selected (tenant, transaction) call must still fire it exactly
//	    once; the second identical call must observe the spent shot.
//
// The interleaving is deterministic: the victim goroutine is held at the
// probe hook until the scope and the barrier are fully installed.
func issue31120AcceptanceOracle(t *testing.T, trigger func(tenantID uint32, txnID string) (string, bool)) {
	t.Helper()
	ctx := context.Background()
	Enable()
	t.Cleanup(func() {
		issue31120ProbeHook = nil
		for _, point := range []string{issue31120Barrier, issue31120TenantPoint, issue31120TxnPoint} {
			_, _ = RemoveFaultPoint(ctx, point)
		}
		Disable()
	})

	probesPassed := make(chan struct{})
	installDone := make(chan struct{})
	// The victim's call is the first through the hook; the acceptance (b)
	// calls reach the hook after installDone is closed and pass through.
	var hookOnce sync.Once
	issue31120ProbeHook = func() {
		hookOnce.Do(func() { close(probesPassed) })
		<-installDone
	}

	// An unrelated tenant-0 call, in flight before anything is installed.
	type callResult struct {
		message  string
		injected bool
	}
	inflight := make(chan callResult, 1)
	go func() {
		message, injected := trigger(0, "issue31120-unrelated-txn")
		inflight <- callResult{message, injected}
	}()
	<-probesPassed // the in-flight call reached its pre-barrier point

	// Arm the exact scope from the issue report: tenant 4242, one accepted
	// transaction, single-shot ECHO barrier. ECHO terminates the
	// counterexample like the issue's controlled reproduction; the WAIT path
	// is covered by TestTriggerFaultScopedWaitReleaseAndRemoval.
	require.NoError(t, AddFaultPoint(ctx, issue31120TenantPoint, ":::", "echo", 4242, "", false))
	require.NoError(t, AddFaultPoint(ctx, issue31120TxnPoint, ":::", "echo", 0, "issue31120-accepted-txn", false))
	require.NoError(t, AddFaultPoint(ctx, issue31120Barrier, "1:1::", "echo", 0, "barrier", false))
	close(installDone)

	got := <-inflight
	assert.Falsef(t, got.injected,
		"acceptance (a): the in-flight nonmatching call must not consume the scoped barrier, got message %q",
		got.message)

	message, injected := trigger(4242, "issue31120-accepted-txn")
	require.True(t, injected, "acceptance (b): the selected transaction must claim the single-shot barrier")
	require.Equal(t, "barrier", message)
	_, injected = trigger(4242, "issue31120-accepted-txn")
	require.False(t, injected, "acceptance (b): the single-shot barrier fires exactly once")
}

// TestIssue31120RedStraddle is the preserved deterministic reproducer of the
// parent composition from the issue report. It is EXPECTED TO FAIL: with the
// legacy three-step trigger the unrelated in-flight call consumes the scoped
// barrier (acceptance (a) fails) and the selected transaction then loses its
// single firing (acceptance (b) fails). Ordinary package runs skip it; run it
// explicitly to reproduce the base failure:
//
//	ISSUE31120_RED_REPRO=1 go test ./pkg/util/fault -run TestIssue31120RedStraddle -count=1
func TestIssue31120RedStraddle(t *testing.T) {
	if os.Getenv("ISSUE31120_RED_REPRO") == "" {
		t.Skip("expected-failure red proof of the parent composition; run with ISSUE31120_RED_REPRO=1")
	}
	issue31120AcceptanceOracle(t, issue31120LegacyCommitWaitInjected)
}
