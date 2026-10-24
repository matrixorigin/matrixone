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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/stretchr/testify/require"
)

// TestIssue25590ReusedWriterReservationsSettleUnderReboundWorkspace pins the
// round-35 review's engine-level impact through REAL workspaces and the
// engine's own capacity ledger. A persistent writer a cached plan keeps alive
// across transactions must have its per-object admission REBOUND to each
// execution's workspace — CNS3Writer.RebindWorkspaceSinkerCapacity is the
// operation MultiUpdate.Prepare now performs on every reuse — because only
// the CURRENT workspace's terminal can still settle a reservation. Before
// the fix, the reused writer's spill for the SECOND transaction reserved
// through the FINALIZED first transaction (queued=0, live=0, inFlight=1
// stranded in the review's probe), permanently consuming engine-wide
// admission capacity while nothing could ever release it.
func TestIssue25590ReusedWriterReservationsSettleUnderReboundWorkspace(t *testing.T) {
	for _, failedFirst := range []bool{false, true} {
		name := "successful_write"
		if failedFirst {
			name = "post_persist_failure"
		}
		t.Run(name, func(t *testing.T) {
			colexec.NewServer("")
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			// Transaction 1 on a REAL queue-enabled engine, entered through the
			// production statement ceremony a cached plan's execution drives.
			txn1, failFS := issue25590NewRound21TxnForTest(t)
			e := txn1.engine

			// Hold deletion failures before handoff, so the background drain
			// cannot consume a one-shot fault or retire the asserted ownership.
			// Join its completion before starting the next writer generation.
			drain := func(name string) {
				t.Helper()
				require.Equal(t, []string{name}, issue25590QueuedNames(e))
				require.Error(t, e.deleteQueuedUnpublishedPendingOnce(ctx))
				require.Equal(t, []string{name}, issue25590QueuedNames(e))
				e.unpublishedPending.mu.Lock()
				idle := e.unpublishedPending.drainIdle
				e.unpublishedPending.mu.Unlock()
				require.NotNil(t, idle)
				failFS.armDeleteFailureFor("")
				require.NoError(t, e.deleteQueuedUnpublishedPendingOnce(ctx))
				select {
				case <-idle:
				case <-ctx.Done():
					t.Fatal("cleanup drain did not finish:", ctx.Err())
				}
				require.Empty(t, issue25590QueuedNames(e))
				_, err := failFS.FileService.StatFile(ctx, name)
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound),
					"cleanup must physically delete the object before reuse")
			}

			// The reused writer shape binds the FIRST workspace through the same
			// resolution the multi_update delegate applies at construction.
			wOpts := make([]ioutil.SinkerOption, 0, 1)
			if opt := colexec.WorkspaceSinkerCapacityOption(txn1.proc); opt != nil {
				wOpts = append(wOpts, opt)
			}
			w := colexec.NewCNS3TombstoneWriterForService(
				txn1.proc.GetService(), txn1.proc.Mp(), failFS,
				types.T_int64.ToType(), -1, wOpts...)
			t.Cleanup(func() { w.Close() })

			// spill persists ONE object through the reused writer and hands out
			// the exact name it persisted — the LAST of the writer's tracked
			// names from the current execution. Reset retires the previous snapshot. The
			// batch is the round-21 push helper's shape: workspace-allocated
			// rowids plus an int64 pk column.
			spill := func(t *testing.T, txn *Transaction, expectFailure bool) string {
				t.Helper()
				rowidVec, err := txn.batchAllocNewRowIds(2)
				require.NoError(t, err)
				defer rowidVec.Free(txn.proc.Mp())
				rowids := vector.MustFixedColNoTypeCheck[types.Rowid](rowidVec)
				bat := batch.NewWithSize(2)
				defer bat.Clean(txn.proc.Mp())
				bat.SetAttributes([]string{objectio.PhysicalAddr_Attr, "pk"})
				rowidCol := vector.NewVec(types.T_Rowid.ToType())
				bat.Vecs[0] = rowidCol
				require.NoError(t, vector.AppendFixedList(rowidCol, rowids, nil, txn.proc.Mp()))
				pkCol := vector.NewVec(types.T_int64.ToType())
				bat.Vecs[1] = pkCol
				require.NoError(t, vector.AppendFixedList(pkCol, []int64{1, 2}, nil, txn.proc.Mp()))
				bat.SetRowCount(2)
				require.NoError(t, w.Write(ctx, bat))
				statsList, err := w.Sync(ctx)
				if expectFailure {
					require.ErrorContains(t, err, "injected post-persist fs write failure")
				} else {
					require.NoError(t, err)
					require.NotEmpty(t, statsList)
				}
				// Both successful stats and ambiguous failures must expose their exact name.
				names := w.PersistedNames()
				require.NotEmpty(t, names)
				return names[len(names)-1]
			}

			// Execution 1: its spill reserves one in-flight unit under the
			// CURRENT workspace, which finalizes and settles its own object.
			txn1.StartStatement()
			require.NoError(t, txn1.IncrStatementID(ctx, false))
			txn1.UpdateSnapshotWriteOffset()
			if failedFirst {
				failFS.armPostPersistFailure(func() bool { return true })
			}
			name1 := spill(t, txn1, failedFirst)
			failFS.stopPostPersistFailure()
			require.True(t, txn1.hasUnpublishedDumpReservation(name1),
				"execution 1's reservation rides its own workspace")
			_, live1, inFlight1 := issue25590LedgerNow(e)
			require.Zero(t, live1)
			require.Equal(t, int64(1), inFlight1,
				"the ledger holds the spilled object's in-flight unit")
			failFS.armDeleteFailureFor(name1)
			txn1.RetainUnrecordedS3Objects(w.PersistedNames(), "writer reset before metadata recording")
			w.Reset()
			require.Empty(t, w.PersistedNames())
			txn1.EndStatement()
			txn1.FinalizeCommit(ctx)
			_, live1, inFlight1 = issue25590LedgerNow(e)
			require.Zero(t, inFlight1,
				"the finalized FIRST workspace settled its own reservation")
			require.Zero(t, live1)
			drain(name1)

			// Execution 2: a NEW transaction on the SAME engine reuses the
			// persistent writer exactly the way a cached plan does, and the fix's
			// rebind moves its admission to the CURRENT workspace.
			txn2 := newTransactionWithActivePKTableForTest(t, "pk")
			issue25590BindRound21Txn(t, txn2)
			txn2.engine = e
			w.RebindWorkspaceSinkerCapacity(txn2.proc)

			txn2.StartStatement()
			require.NoError(t, txn2.IncrStatementID(ctx, false))
			txn2.UpdateSnapshotWriteOffset()
			name2 := spill(t, txn2, false)
			require.Equal(t, []string{name2}, w.PersistedNames(), "new execution owns no retired names")
			require.True(t, txn2.hasUnpublishedDumpReservation(name2),
				"the rebound writer's reservation rides the CURRENT workspace, whose terminal still lies ahead")
			require.False(t, txn1.hasUnpublishedDumpReservation(name2),
				"the FINALIZED first workspace received nothing it could never settle")
			_, live2, inFlight2 := issue25590LedgerNow(e)
			require.Zero(t, live2)
			require.Equal(t, int64(1), inFlight2,
				"the shared engine's ledger holds exactly the current reservation")
			failFS.armDeleteFailureFor(name2)
			txn2.RetainUnrecordedS3Objects(w.PersistedNames(), "writer reset before metadata recording")
			w.Reset()
			txn2.EndStatement()
			txn2.FinalizeCommit(ctx)

			_, live2, inFlight2 = issue25590LedgerNow(e)
			require.Zero(t, inFlight2,
				"the second execution's reservation settled — the pre-fix stale hook stranded exactly this unit")
			require.Zero(t, live2)
			drain(name2)
			_, live2, inFlight2 = issue25590LedgerNow(e)
			require.Zero(t, live2)
			require.Zero(t, inFlight2,
				"the engine's admission bound closes with no stranded in-flight unit")

		})
	}
}
