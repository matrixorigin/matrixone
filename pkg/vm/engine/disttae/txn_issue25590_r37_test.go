// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disttae

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/stretchr/testify/require"
)

// issue25590FreezeUnpublishedDrainScheduling parks the drain pipeline's
// coalescing slot for the whole test, so every enqueue below stays fully
// synchronous: no background ladder task can compete with this
// regression's own direct invocations of the once-drain unit (the same
// unit the ladder and the shutdown drain share) and contaminate the
// allocation or call-count oracles. The returned restore reopens
// opportunistic scheduling.
func issue25590FreezeUnpublishedDrainScheduling(t *testing.T, e *Engine) (restore func()) {
	t.Helper()
	e.unpublishedPending.mu.Lock()
	prev := e.unpublishedPending.drainScheduled
	e.unpublishedPending.drainScheduled = true
	e.unpublishedPending.mu.Unlock()
	return func() {
		e.unpublishedPending.mu.Lock()
		e.unpublishedPending.drainScheduled = prev
		e.unpublishedPending.mu.Unlock()
	}
}

// issue25590RetireNoopFS answers every unpublished-object delete with an
// immediate success: the cost regression measures the QUEUE-side
// retirement mechanics — the snapshot, the batch partition and the
// retire step — while per-path storage work is a constant the
// unchanged batch loop performs identically on any tree. Wrapping the
// swapped test service keeps every other consumer of e.fs exactly as
// before; only the drain's delete unit sees the no-op.
type issue25590RetireNoopFS struct {
	fileservice.FileService
}

func (fs *issue25590RetireNoopFS) Delete(
	ctx context.Context, paths ...string,
) error {
	return nil
}

// TestIssue25590DrainRetirementCostStaysLinear pins the round-37 review's
// second finding against the once-drain unit's OWN accounting: retiring
// the names of the batches one storage round accepted must cost ONE
// linear pass over the snapshot and the live queue, because the engine's
// whole ladder, every retry tick and Close's shutdown tail reuse this
// unit. The pre-fix shape rebuilt the queue once per successful
// 1,000-name batch — each rebuild allocated a fresh copy of the whole
// remaining backlog and rescanned it under the queue mutex admission
// shares — quadratic work that measured ~95 MB at 100,000 queued names
// and ~2 GB at the 500,000-name admission limit while blocking the
// ledger's linearization point for seconds. The bound this regression
// enforces is generous on purpose (bytes are Go-version dependent): it
// separates the two shapes by roughly an order of magnitude, while the
// semantics retirements are pinned by the two siblings below.
func TestIssue25590DrainRetirementCostStaysLinear(t *testing.T) {
	colexec.NewServer("")
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	e := txn.engine
	failFS := issue25590SwapProcSharedFS(t, txn)
	e.fs = &issue25590RetireNoopFS{FileService: failFS}
	restoreScheduling := issue25590FreezeUnpublishedDrainScheduling(t, e)
	defer restoreScheduling()

	// 120 queued names make 120 storage batches: enough for the
	// per-batch rebuild's quadratic term to dominate, small enough for
	// a deterministic unit-test budget.
	const nameCount = 120_000
	unique := "issue25590-" + uuid.NewString()[:8]
	names := make([]string, 0, nameCount)
	for i := 0; i < nameCount; i++ {
		names = append(names, fmt.Sprintf("%s-drain-%08d", unique, i))
	}
	e.enqueueUnpublishedPending(names)
	require.Len(t, issue25590QueuedNames(e), nameCount,
		"the whole synthetic backlog is queued before the measured pass")

	// The once-drain runs alone - the coalescing slot is parked - so the
	// bytes read between these two counters belong to the unit's own
	// snapshot, batch loop and retirement only.
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	onceErr := e.deleteQueuedUnpublishedPendingOnce(context.Background())
	runtime.ReadMemStats(&after)
	require.NoError(t, onceErr,
		"the no-op storage service accepts every batch")
	require.Empty(t, issue25590QueuedNames(e),
		"the healthy pass retires every queued name")

	allocated := after.TotalAlloc - before.TotalAlloc
	// One linear pass: the snapshot clone, the done-name accumulation and
	// ONE in-place compaction of the live queue stay in the tens of
	// megabytes here, while the per-batch-rebuild tree needed hundreds
	// of megabytes on the same input. 48 MiB leaves a wide margin on
	// both sides of the measured split.
	const linearBudgetBytes = 48 << 20
	t.Logf("round-37 drain retirement measured %d bytes for %d accepted names (linear budget %d)",
		allocated, nameCount, linearBudgetBytes)
	require.LessOrEqualf(t, int64(allocated), int64(linearBudgetBytes),
		"retiring %d accepted names must cost one linear pass over the snapshot and the live queue (issue #25590 I2), not a full queue rebuild per 1,000-name batch; measured %d bytes",
		nameCount, allocated)

	// The membership index stays in lockstep with the retirement: the
	// SAME names are accepted again as new queued members, which only
	// holds when each retired name really left the index.
	e.enqueueUnpublishedPending(names[:3])
	queuedAgain := issue25590QueuedNames(e)
	require.Len(t, queuedAgain, 3,
		"retired names re-enter the queue on a fresh delivery: the membership index dropped them in the same pass")
	require.Equal(t, names[:3], queuedAgain)
}

// TestIssue25590DrainRetiresEachBatchInOnePass pins the per-batch
// contract the round-37 single-pass retirement keeps: a failing batch
// keeps exactly its own names and never walls the later queued names
// behind every retry, a successful batch retires exactly its own names,
// and one invocation's retirement is complete before it returns — the
// queue holds exactly the failed batch's thousand names in queue order.
func TestIssue25590DrainRetiresEachBatchInOnePass(t *testing.T) {
	colexec.NewServer("")
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	e := txn.engine
	failFS := issue25590SwapProcSharedFS(t, txn)
	e.fs = failFS
	restoreScheduling := issue25590FreezeUnpublishedDrainScheduling(t, e)
	defer restoreScheduling()

	// Five batches; the third carries one poisoned object. The poisoned
	// object's file is real so the surviving batch's storage state is
	// observable, every other name needs no file (tolerated deletion).
	const nameCount = 5_000
	unique := "issue25590-" + uuid.NewString()[:8]
	names := make([]string, 0, nameCount)
	for i := 0; i < nameCount; i++ {
		names = append(names, fmt.Sprintf("%s-batch-%08d", unique, i))
	}
	poisonPath := names[2_500]
	issue25590WriteRawFile(t, failFS.FileService, poisonPath)
	failFS.armDeleteFailureFor(poisonPath)
	e.enqueueUnpublishedPending(names)
	require.Len(t, issue25590QueuedNames(e), nameCount)

	onceErr := e.deleteQueuedUnpublishedPendingOnce(context.Background())
	require.Error(t, onceErr,
		"the poisoned batch fails its own deletion and the pass reports it")
	require.Equal(t, 5, failFS.deleteCallCount(),
		"one pass attempts every batch independently: five batches, five storage calls")
	require.Equal(t, names[2_000:3_000], issue25590QueuedNames(e),
		"exactly the poisoned batch's thousand names stay queued, in queue order")
	issue25590RequireKept(t, failFS.FileService, poisonPath,
		"the poisoned object's file survives its failing batch")

	// The next pass re-attempts exactly the surviving thousand — nothing
	// else re-enters storage, and the failure keeps its isolation.
	onceErr = e.deleteQueuedUnpublishedPendingOnce(context.Background())
	require.Error(t, onceErr)
	require.Equal(t, 6, failFS.deleteCallCount(),
		"the retry attempts only the retained batch, not the retired backlog")
	require.Equal(t, names[2_000:3_000], issue25590QueuedNames(e))
}

// TestIssue25590DrainKeepsConcurrentArrivals pins the arrival property
// the round-37 single-pass retirement must preserve: names enqueued
// while the drain is between its batches — its snapshot already taken —
// stay owned by the queue and survive that invocation's retirement
// exactly once. The pre-fix per-batch rebuild and the fixed single
// compaction both retire by name against a snapshot that does not
// contain the arrivals, so the arrivals drain on the next pass instead
// of being silently consumed.
func TestIssue25590DrainKeepsConcurrentArrivals(t *testing.T) {
	colexec.NewServer("")
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	e := txn.engine
	failFS := issue25590SwapProcSharedFS(t, txn)
	e.fs = failFS
	restoreScheduling := issue25590FreezeUnpublishedDrainScheduling(t, e)
	defer restoreScheduling()

	const nameCount = 3_000
	unique := "issue25590-" + uuid.NewString()[:8]
	names := make([]string, 0, nameCount)
	for i := 0; i < nameCount; i++ {
		names = append(names, fmt.Sprintf("%s-arrival-%08d", unique, i))
	}
	e.enqueueUnpublishedPending(names)
	require.Len(t, issue25590QueuedNames(e), nameCount)

	// Park the drain's first storage call inside the file service so the
	// enqueues below land strictly between the drain's first and second
	// batches.
	release := failFS.armDeleteBlock()
	onceDone := make(chan error, 1)
	go func() {
		onceDone <- e.deleteQueuedUnpublishedPendingOnce(context.Background())
	}()
	require.Eventually(t, func() bool {
		return failFS.blockedDeleteCalls() >= 1
	}, 10*time.Second, 10*time.Millisecond,
		"the drain parked inside its first batch")

	// Fresh names — none ever queued before — arrive while the drain
	// holds its snapshot: exactly the lost-response shape of a producer
	// racing the GC tick.
	const arrivals = 200
	arriving := make([]string, 0, arrivals)
	for i := 0; i < arrivals; i++ {
		arriving = append(arriving, fmt.Sprintf("%s-arrived-%08d", unique, i))
	}
	e.enqueueUnpublishedPending(arriving)

	release()
	require.NoError(t, <-onceDone,
		"the released drain finishes its snapshot's batches successfully")
	require.Equal(t, arriving, issue25590QueuedNames(e),
		"the concurrent arrivals stay queued for the next pass, exactly the names the snapshot never saw")
	for _, n := range names {
		require.NotContains(t, issue25590QueuedNames(e), n)
	}
}
