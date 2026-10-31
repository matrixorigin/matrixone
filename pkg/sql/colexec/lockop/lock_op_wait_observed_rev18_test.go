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

package lockop

// rev-18 members (pre-PR review round 4, finding 5): the lock-wait phase
// barrier. rev-17 replaced the version probe's semantics but never
// ESTABLISHED the commit-while-waiting interleaving: injecting
// changedInRange=true upfront proves the retry surface, not that the peer
// commit lands INSIDE the lock-wait window. These members drive the same
// production lock/retry consumers through a REAL wait: an upstream holder
// holds the row through the production lock-service API, the marked request
// registers its wait on the real lock service, the member observes the
// registration through GetWaitingList BEFORE releasing the holder, and the
// peer "commit" (the holder's commit release, with its commit timestamp)
// can only land inside the observed wait window. A broken barrier fails
// loudly: a request that never waits never arms the probe, and a member
// whose barrier never fires never records a phantom retry.

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	pbtimestamp "github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// encodeMarkedLockKey mirrors the production row encoding for the member's
// int32 primary key (lockop/fetch.go: types.Packer EncodeInt32).
func encodeMarkedLockKey(v int32) []byte {
	packer := types.NewPacker()
	defer packer.Close()
	packer.EncodeInt32(v)
	return packer.Bytes()
}

// observedWaitProbe arms ONLY after the test has observed the marked
// request's wait registration on the real lock service. A call before the
// barrier reports no peer change (the row was visible all along); after the
// barrier the peer commit has provably landed inside the wait window.
func observedWaitProbe(armed *atomic.Bool) hasNewVersionInRangeFunc {
	return func(
		proc *process.Process,
		rel engine.Relation,
		analyzer process.Analyzer,
		tableID uint64,
		eng engine.Engine,
		bat *batch.Batch,
		idx int32,
		partitionIdx int32,
		from, to pbtimestamp.Timestamp,
	) (bool, error) {
		if !armed.Load() {
			return false, nil
		}
		return true, nil
	}
}

// The production lock/retry chain executes a REAL lock wait: the upstream
// holder owns the row through the production lock-service API, the marked
// request registers its wait on the same real lock service, the member
// observes that registration (GetWaitingList) and only then releases the
// holder with a commit timestamp — the peer commit lands strictly inside
// the observed wait window, the marked SI path records the refresh outcome
// as its progressive retry token, and the SI snapshot never moves.
func TestMarkedLockOpRetryProvesCommitLandsInsideObservedWaitWindow(t *testing.T) {
	tw, stop := startMarkedTimestampWaiter()
	defer stop()

	runLockOpTestWithTxnOptions(
		t,
		func(proc *process.Process) {
			lockSvc := proc.GetLockService()
			ctx := proc.Ctx
			tableID := uint64(1)
			upstreamID := []byte("rev18-upstream-holder")

			// The upstream peer holds the row through the REAL lock service —
			// exactly the interface a commit-time lock release uses.
			_, err := lockSvc.Lock(ctx, tableID,
				[][]byte{encodeMarkedLockKey(5)}, upstreamID,
				lock.LockOptions{Granularity: lock.Granularity_Row})
			require.NoError(t, err, "the upstream holder acquires the row lock")

			var armed atomic.Bool
			arg := buildMarkedArgument(proc, []uint64{tableID}, [][]int32{{5}})
			defer arg.Free(proc, false, nil)
			require.NoError(t, arg.Prepare(proc))
			arg.ctr.hasNewVersionInRange = observedWaitProbe(&armed)

			arg.MarkLockingReadTarget(0, true)
			attemptFrontier := pbtimestamp.Timestamp{PhysicalTime: 100, LogicalTime: 1}
			arg.SetLockingReadCurrentFrontier(attemptFrontier, proc.GetTxnOperator().Txn().SnapshotTS)
			sessionTS := proc.GetTxnOperator().Txn().SnapshotTS

			var execErr error
			done := make(chan struct{})
			go func() {
				defer close(done)
				_, execErr = vm.Exec(arg, proc)
			}()

			// THE BARRIER: the request must REGISTER its wait on the real lock
			// service before the peer commit is allowed to land. Every check
			// here fails loudly — a request that completes without waiting, or
			// a registration window that closes without a waiter, is a broken
			// barrier, never a green timing guess.
			waitObserved := false
			deadline := time.Now().Add(5 * time.Second)
			for !waitObserved {
				require.NoError(t, ctx.Err(), "harness context expired")
				select {
				case <-done:
					require.FailNow(t, "barrier broken",
						"the marked lock request completed without waiting on the open holder")
				default:
				}
				require.False(t, time.Now().After(deadline),
					"barrier broken: the marked request never registered its wait")
				found, waiters, listErr := lockSvc.GetWaitingList(ctx, upstreamID)
				if listErr == nil && found {
					for _, w := range waiters {
						if string(w.TxnID) == string(proc.GetTxnOperator().Txn().ID) {
							waitObserved = true
							armed.Store(true)
							break
						}
					}
				}
				if !waitObserved {
					time.Sleep(5 * time.Millisecond)
				}
			}
			require.True(t, waitObserved,
				"the real lock service observed the marked request waiting ON the upstream holder")
			select {
			case <-done:
				require.FailNow(t, "barrier broken",
					"the request returned while the holder was still open: it never really waited")
			default:
			}

			// THE PEER COMMIT lands inside the observed wait window: the
			// holder's commit release carries its commit timestamp past the
			// reading attempt's frontier.
			commitTS := pbtimestamp.Timestamp{PhysicalTime: 250, LogicalTime: 1}
			require.NoError(t, lockSvc.Unlock(ctx, upstreamID, commitTS),
				"the upstream peer commits and releases the row inside the wait window")

			select {
			case <-done:
			case <-time.After(5 * time.Second):
				require.FailNow(t, "barrier broken",
					"the marked operator never woke after the peer commit released the row")
			}

			require.Error(t, execErr, "the commit-while-waiting outcome surfaces as the marked SI retry")
			require.True(t, moerr.IsMoErrCode(execErr, moerr.ErrTxnNeedRetryLockingRead), "%v", execErr)
			phys, logical, ok := moerr.GetTxnNeedRetryLockingReadFrontier(execErr)
			require.True(t, ok, "the observed-window retry carries its frontier token")
			token := pbtimestamp.Timestamp{PhysicalTime: phys, LogicalTime: logical}
			require.True(t, token.Greater(attemptFrontier),
				"the token strictly progresses past the attempt frontier it will be validated against (D-5)")
			require.Equal(t, sessionTS, proc.GetTxnOperator().Txn().SnapshotTS,
				"an observed wait, commit release and retry token never move the SI snapshot")
		},
		[]client.TxnOption{client.WithTxnIsolation(pbtxn.TxnIsolation_SI)},
		client.WithTimestampWaiter(tw),
	)
}

// The no-contention counterexample (review round 4, finding 5): with the
// same gated probe but NO upstream holder, nothing waits, the registration
// barrier never fires, the probe reports no peer change, and the marked
// execution completes WITHOUT any retry token — an observed barrier cannot
// record a phantom commit-while-waiting on a request that never waited,
// which is exactly what a timing-based guess could not guarantee.
func TestMarkedLockOpWithoutContentionRecordsNoPhantomRetry(t *testing.T) {
	tw, stop := startMarkedTimestampWaiter()
	defer stop()

	runLockOpTestWithTxnOptions(
		t,
		func(proc *process.Process) {
			var armed atomic.Bool
			arg := buildMarkedArgument(proc, []uint64{1}, [][]int32{{5}})
			defer arg.Free(proc, false, nil)
			require.NoError(t, arg.Prepare(proc))
			arg.ctr.hasNewVersionInRange = observedWaitProbe(&armed)

			arg.MarkLockingReadTarget(0, true)
			attemptFrontier := pbtimestamp.Timestamp{PhysicalTime: 100, LogicalTime: 1}
			arg.SetLockingReadCurrentFrontier(attemptFrontier, proc.GetTxnOperator().Txn().SnapshotTS)
			sessionTS := proc.GetTxnOperator().Txn().SnapshotTS

			_, err := vm.Exec(arg, proc)
			require.NoError(t, err,
				"without contention the marked lock pass completes: no wait, no retry")
			require.False(t, armed.Load(),
				"the gated probe never armed: the observed-barrier contract holds both directions")
			require.Equal(t, sessionTS, proc.GetTxnOperator().Txn().SnapshotTS)
		},
		[]client.TxnOption{client.WithTxnIsolation(pbtxn.TxnIsolation_SI)},
		client.WithTimestampWaiter(tw),
	)
}
