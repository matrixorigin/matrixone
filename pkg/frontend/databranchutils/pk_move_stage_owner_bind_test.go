// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package databranchutils

// Deterministic interleaving cells for the bind/abandon ownership
// transition (review F1): publication of the operator slot, ClosedEvent
// callback installation and the abandonment decision are one transition
// under the owner mutex, and a binding that arrives after a recorded
// terminal decision is rejected - the caller rolls back its own unbound
// operator, exactly like every production bind site in
// pkg/sql/compile/pk_move_fencing_cs.go. The stub operator mirrors the
// platform's hard panic on AppendEventCallback after close
// (operator_events.go:65-67), so any regression of the transition window
// fails these cells loudly instead of corrupting silently.

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestDataBranchStageOwnerAbandonBeforeBindRejectsLateBinding: the
// abandonment-before-binding window. The owner records the vacuous
// no-transaction reap (D187(B.2)(iv)); a late BindOperator is rejected,
// the caller rolls back the unbound operator itself, and the owner's
// accounting completes on the recorded vacuous close - never on a close
// it could not observe, and never by skipping the operator's terminal
// call.
func TestDataBranchStageOwnerAbandonBeforeBindRejectsLateBinding(t *testing.T) {
	gauges := &stageGauges{}
	journal := &stageJournal{}
	ls := newStubLockService(gauges, journal)
	defer ls.Close()
	cli := newStubTxnClient(gauges, journal, ls)
	pool := NewDataBranchFencingPool(DataBranchFencingDriverConfig{})

	owner, class := pool.MintStageOwner(77, 1, "abandon-before-bind")
	require.Equal(t, DataBranchFencingRoundCommitted, class)

	// Abandonment recorded before any stage transaction existed.
	owner.Abandon(context.Background())
	owner.NoteNoMoreLanes()
	waitForCondition(t, 2*time.Second, "owner done on vacuous reap",
		func() bool { return owner.State() == "done" })

	// The late binding is rejected; the owner adopts nothing.
	op := cli.New(1)
	err := owner.BindOperator(op, 77)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "binding rejected"),
		"bind error must name the rejection, have: %v", err)
	require.Nil(t, owner.BoundOperator())

	// Caller-side cleanup of the unbound operator: one public rollback,
	// performed by the caller, mirrored by every production bind site.
	require.NoError(t, op.Rollback(context.Background()))
	require.Equal(t, 1, journal.count("workspace-rollback"))

	// The owner's completion already rode the vacuous close: no observed
	// close was required, and the charge was released exactly once.
	require.False(t, owner.ClosedObserved())
	require.Equal(t, "done", owner.State())
	require.Eventually(t, func() bool { return pool.Dump().Owners == 0 },
		2*time.Second, time.Millisecond)
}

// TestDataBranchStageOwnerBindThenAbandonObservesClose: the ordered
// witness of the same transition - binding first installs the close
// observation; the abandonment's single rollback then feeds the recorded
// ClosedEvent publication and the owner completes on the observed close.
func TestDataBranchStageOwnerBindThenAbandonObservesClose(t *testing.T) {
	gauges := &stageGauges{}
	journal := &stageJournal{}
	ls := newStubLockService(gauges, journal)
	defer ls.Close()
	cli := newStubTxnClient(gauges, journal, ls)
	pool := NewDataBranchFencingPool(DataBranchFencingDriverConfig{})

	owner, class := pool.MintStageOwner(78, 1, "bind-then-abandon")
	require.Equal(t, DataBranchFencingRoundCommitted, class)

	op := cli.New(1)
	require.NoError(t, owner.BindOperator(op, 78))
	require.Same(t, op, owner.BoundOperator())

	owner.AbandonAsync()
	waitForCondition(t, 2*time.Second, "bound operator closed by abandonment",
		func() bool {
			op.mu.Lock()
			defer op.mu.Unlock()
			return op.closed
		})
	require.Equal(t, 1, journal.count("workspace-rollback"))
	owner.NoteNoMoreLanes()
	waitForCondition(t, 2*time.Second, "owner done on observed close",
		func() bool { return owner.State() == "done" })
	require.True(t, owner.ClosedObserved())
	require.Eventually(t, func() bool { return pool.Dump().Owners == 0 },
		2*time.Second, time.Millisecond)
}

// TestDataBranchStageOwnerBindAbandonRaceNeverDiverges: the
// between-publication-and-callback window under maximal contention.
// Whichever side wins, the transition is atomic: either the binding
// lands while ACTIVE and the abandonment rolls the operator back through
// the owner, or the abandonment records first and the binding is
// rejected with the caller rolling back its own operator. The operator
// closes exactly once per round, the owner always reaches DONE, and the
// stub's closed-append panic never fires. Pre-fix, the publication and
// callback registration straddled an unlock, and this schedule could
// close the operator under the in-flight registration (panic).
func TestDataBranchStageOwnerBindAbandonRaceNeverDiverges(t *testing.T) {
	for i := 0; i < 200; i++ {
		gauges := &stageGauges{}
		journal := &stageJournal{}
		ls := newStubLockService(gauges, journal)
		cli := newStubTxnClient(gauges, journal, ls)
		pool := NewDataBranchFencingPool(DataBranchFencingDriverConfig{})

		scratch := uint64(900 + i)
		owner, class := pool.MintStageOwner(scratch, 1, "bind-abandon-race")
		require.Equal(t, DataBranchFencingRoundCommitted, class)

		op := cli.New(1)
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			if err := owner.BindOperator(op, scratch); err != nil {
				// Rejected late binding: the caller owns the rollback,
				// mirroring the production bind sites.
				_ = op.Rollback(context.Background())
			}
		}()
		go func() {
			defer wg.Done()
			<-start
			owner.AbandonAsync()
		}()
		close(start)
		wg.Wait()

		owner.NoteNoMoreLanes()
		waitForCondition(t, 2*time.Second, "operator closed exactly once",
			func() bool {
				op.mu.Lock()
				defer op.mu.Unlock()
				return op.closed
			})
		// Exactly one public rollback path ran, whichever side won.
		require.Equal(t, 1, journal.count("workspace-rollback"), "iteration %d", i)
		waitForCondition(t, 2*time.Second, "owner done",
			func() bool { return owner.State() == "done" })
		ls.Close()
	}
}
