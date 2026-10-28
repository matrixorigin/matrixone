// Copyright 2026 MatrixOne
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

package compile

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	pbtimestamp "github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/stretchr/testify/require"
)

// stalledFloorWaiter is a TxnClient stand-in for the log-tail wait the
// publication barrier uses: it records the floor it was asked to wait for and
// applies exactly that floor as the visible watermark (the logtail pipeline
// has reached the retry token, and nothing beyond it).
type stalledFloorWaiter struct {
	client.TxnClient
	waitedFor pbtimestamp.Timestamp
}

func (w *stalledFloorWaiter) WaitLogTailAppliedAt(
	ctx context.Context, ts pbtimestamp.Timestamp,
) (pbtimestamp.Timestamp, error) {
	w.waitedFor = ts
	return ts, nil
}

// Pre-PR review round 4, finding 3: a retry token was previously validated
// only against the execution chain's FLOOR, never against the just-executed
// attempt's published frontier. With chain floor {100,1} and an executing
// frontier of {300,1}, a token of {200,5} was admitted although it cannot
// advance the executing read — the next attempt would publish at {200,5},
// BEHIND the frontier that just failed. The production validator now derives
// the progressing floor from max(chain floor, the executing attempt's active
// frontier) — design D-5's F(k+1) = max(requiredTS, F(k)) > F(k) — and any
// token that cannot strictly advance it is terminal. This member drives the
// production method the retry loop calls, over equal, regressing, malformed
// and progressive tokens, without ever mutating the attempt's or the
// transaction's snapshots.
func TestLockingReadRetryProgressMeasuredAgainstExecutingAttemptFrontier(t *testing.T) {
	sessionTS := pbtimestamp.Timestamp{PhysicalTime: 100, LogicalTime: 1}
	c, op, teardown := admissionFixture(t, sessionTS)
	defer teardown()

	c.execReadRetryFloor = pbtimestamp.Timestamp{PhysicalTime: 100, LogicalTime: 1}
	// The just-executed attempt published its read frontier at {300,1} before
	// the lock conflicted: its per-attempt state survives on runC until the
	// retry transition, so the loop sees it at the validation site.
	runC := &Compile{}
	runC.execReadFrontierSet = true
	runC.execReadFrontier = pbtimestamp.Timestamp{PhysicalTime: 300, LogicalTime: 1}

	tokenAt := func(phys int64, log uint32) error {
		return moerr.NewTxnNeedRetryLockingRead(context.Background(), phys, log)
	}
	terminal := "no strict progress past the executing attempt's frontier"

	// The reviewer's exact counterexample: token beyond the chain floor but
	// BEHIND the executing attempt's published frontier — terminal now, and
	// the same-test red under the old floor-only comparison.
	err := c.validateLockingReadRetryProgress(runC, tokenAt(200, 5))
	require.Error(t, err, "a token behind the executing attempt's frontier cannot re-drive the retry")
	require.ErrorContains(t, err, terminal)

	// Equal to the executing frontier can never be strict progress.
	err = c.validateLockingReadRetryProgress(runC, tokenAt(300, 1))
	require.Error(t, err)
	require.ErrorContains(t, err, terminal)

	// Below even the chain floor is regressing on both terms.
	err = c.validateLockingReadRetryProgress(runC, tokenAt(99, 9))
	require.Error(t, err)
	require.ErrorContains(t, err, terminal)

	// A malformed (zeroed) token cannot advance anything.
	err = c.validateLockingReadRetryProgress(runC, tokenAt(0, 0))
	require.Error(t, err)
	require.ErrorContains(t, err, terminal)

	// Strict progress past BOTH the chain floor and the executing frontier
	// passes through UNTOUCHED — the loop may set its new floor from it.
	progressing := tokenAt(350, 1)
	err = c.validateLockingReadRetryProgress(runC, progressing)
	require.Same(t, progressing, err,
		"a progressive token passes through unchanged: the loop owns the floor-set")
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryLockingRead))

	// An attempt that failed BEFORE its first carrier published has no active
	// frontier: the contract degenerates to the pre-existing chain-floor
	// comparison.
	runC.execReadFrontierSet = false
	runC.execReadFrontier = pbtimestamp.Timestamp{}
	degenerate := tokenAt(200, 5)
	err = c.validateLockingReadRetryProgress(runC, degenerate)
	require.Same(t, degenerate, err,
		"with no published executing frontier, a token beyond the chain floor passes through unchanged")

	// Errors that are not locking-read tokens pass through untouched: the
	// retry loop's other machinery decides them.
	other := moerr.NewTxnNeedRetryNoCtx()
	require.True(t, moerr.IsMoErrCode(other, moerr.ErrTxnNeedRetry))
	passed := c.validateLockingReadRetryProgress(runC, other)
	require.Same(t, other, passed, "the validator never rewrites non-token errors")

	// Purity: re-arm the executing-attempt state and re-validate — the
	// decision never mutates the attempt's published frontier, the chain
	// floor or the session transaction's snapshot.
	runC.execReadFrontierSet = true
	runC.execReadFrontier = pbtimestamp.Timestamp{PhysicalTime: 300, LogicalTime: 1}
	require.True(t, moerr.IsMoErrCode(
		c.validateLockingReadRetryProgress(runC, tokenAt(350, 1)), moerr.ErrTxnNeedRetryLockingRead))
	require.Equal(t, pbtimestamp.Timestamp{PhysicalTime: 300, LogicalTime: 1}, runC.execReadFrontier)
	require.True(t, runC.execReadFrontierSet)
	require.Equal(t, pbtimestamp.Timestamp{PhysicalTime: 100, LogicalTime: 1}, c.execReadRetryFloor,
		"the validator never mutates the chain floor: the loop's accept-site owns that")
	require.Equal(t, sessionTS, op.Txn().SnapshotTS,
		"the validation decision never touches the session snapshot")
	require.False(t, c.execReadDisabled)
}

// The publication side of the retry-progress contract (review round 4,
// finding 3): after a PROGRESSIVE token is accepted, the loop's floor rides
// strictly above the failed attempt's frontier, and the next attempt's
// carrier cannot publish at or below that frontier even when the publication
// barrier itself acquires an earlier watermark. Execution reads at the
// standard retry floor, and the failed attempt's own frontier ({300,1})
// becomes a floor the barrier must be lifted past — the shape the old
// floor-only acceptance made unreachable: a regressing token {200,5} would
// have left the floor at {200,5} and let the next carrier publish BEHIND
// {300,1}.
func TestLockingReadRetryFloorPublicationCannotReacquireEarlierFrontier(t *testing.T) {
	sessionTS := pbtimestamp.Timestamp{PhysicalTime: 100, LogicalTime: 1}
	c, _, teardown := admissionFixture(t, sessionTS)
	defer teardown()

	executingFrontier := pbtimestamp.Timestamp{PhysicalTime: 300, LogicalTime: 1}
	// The floor the retry loop derives from the accepted progressive token
	// {350,1} (compile2.go's floor-set: max of the chain floor and the token)
	// — strictly beyond the executing attempt's {300,1}.
	c.execReadRetryFloor = pbtimestamp.Timestamp{PhysicalTime: 350, LogicalTime: 1}
	// The stalled barrier acquires an EARLIER watermark than the failed
	// attempt read at; only the floor wait may raise publication past it.
	c.e = &reviewBarrierEngine{frontier: pbtimestamp.Timestamp{PhysicalTime: 200, LogicalTime: 0}}
	waiter := &stalledFloorWaiter{}
	c.proc.Base.TxnClient = waiter

	op, gotFrontier, err := c.executionReadOperator()
	require.NoError(t, err)
	require.NotNil(t, op)
	require.Equal(t, pbtimestamp.Timestamp{PhysicalTime: 350, LogicalTime: 1}, waiter.waitedFor,
		"publication waits for exactly the validated retry floor")
	require.True(t, gotFrontier.Greater(executingFrontier),
		"a retry attempt NEVER publishes at or beneath the frontier that just failed: %v vs %v",
		gotFrontier, executingFrontier)
	require.True(t, c.execReadFrontierSet)
	require.Equal(t, gotFrontier, c.execReadFrontier)
}

// Pre-PR review round 4, finding 4: the capacity-transition proof must
// include the DROP window between the frontend protocol check and Run. This
// member drives the production Run admission itself: the same marked plan
// the frontend validator admitted while the capability was up is refused by
// Run once the negotiated capability drops — every execution path recrosses
// the Run validator, so a retained plan object can never execute marked
// semantics under a lowered gate.
func TestRunAdmissionRejectsMarkedPlanInDropWindow(t *testing.T) {
	proc := testutil.NewProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	require.NotNil(t, rt)
	orig, had := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if had {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, orig)
		}
	})

	pn := presencePlan(reviewMarkedScan(0, "t"))
	c := &Compile{proc: proc, ncpu: 1}
	c.pn = pn

	// The frontend protocol check admitted the marked plan while the
	// negotiated capability was up.
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion135)
	require.NoError(t, validateLockingCurrentReadCapability(proc, pn))

	// The capability dropped between that check and Run: the Run admission
	// rejects the SAME marked object with the recorded terminal error.
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, int64(128))
	_, err := c.Run(0)
	require.Error(t, err, "the drop window cannot ship a marked execution the cluster cannot run")
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported), "%v", err)
	require.ErrorContains(t, err, "requires rebinding",
		"the residual past-the-guards error is the recorded NotSupported rebinding contract")
	require.Nil(t, c.scopes)

	// With capability restored, the same retained marked object re-passes the
	// Run admission: the lowered-gate verdict is never sticky.
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion135)
	require.NoError(t, validateLockingCurrentReadCapability(proc, pn),
		"a retained marked plan re-admits once the negotiated capability rises again")
}
