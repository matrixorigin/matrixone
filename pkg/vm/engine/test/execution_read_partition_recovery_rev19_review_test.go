// Copyright 2026 MatrixOne
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

package test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	catalog2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	testutil "github.com/matrixorigin/matrixone/pkg/vm/engine/test/testutil"
)

// Pre-PR review round 6, finding 2: the executionReadOp carrier deliberately
// resolves SnapshotTS() to the locking execution's frontier F while the
// transaction snapshot stays S; the historical-partition recovery that
// getPartitionState's fallback performs was anchored on Txn().SnapshotTS
// instead, so an admitted scan whose live table coverage was lost (the
// too-old table id class, issue 17012) recovered the S-anchored state — and
// the snapshot cache answered that S-request with an S-covering partition,
// which the read at F consumed as if it covered the frontier (incomplete
// history despite the retained F timestamp).
//
// This member drives the REAL recovery once for each operator shape on one
// deterministic stage: distinct S and F (an SI subject that stays open across
// a peer commit and an in-window compaction), a lost live coverage for the
// recovery id (a "table not exist" subscribe answer — the production
// SubRspTableNotExist reply class the recovery exists for), an OLDER cached
// historical partition that serves S but not F, the live partition (which
// covers F with the committed (S, F] content) registered under the same cache
// key, a deterministic checkpoint authority behind the swapped snapshot-read
// transport, and the reader-level current-read contract. Expected:
//   - the CARRIER-bound recovery requests and receives the F-covering state
//     (identity: the live partition's own state), which reports the persisted
//     in-window compaction objects;
//   - the ordinary (carrier-less) recovery keeps requesting S and receives
//     the older S-serving historical partition, which carries NONE of the
//     in-window content — the exact incomplete-history state an S-anchored
//     recovery would hand an F-read;
//   - the exclusive current read at F discovers the peer's rows through the
//     same fed cluster state (current-read results), while the parent
//     transaction's snapshot keeps its seed-only view (unchanged parent
//     snapshot results).
func TestExecutionReadRecoversHistoricalStateAtTheExecutingReadPoint(t *testing.T) {
	h := newExecReadCompactionHarness(t, 20)

	relA, txnA := h.getTxnA()
	sTS := types.TimestampToTS(h.subjectSnapshotTS)

	// A PEER commits inside (S, F]: rows the S-anchored history cannot cover.
	_, relPeer, txnPeer, err := h.dEngine.GetTable(h.ctx, h.dbName, h.tblName)
	require.NoError(t, err)
	bat := catalog2.MockBatch(h.schema, h.rowsCount)
	cnBat := containers.ToCNBatch(bat)
	pk := h.schema.GetPrimaryKey()
	pkIdx := -1
	for i, attr := range cnBat.Attrs {
		if attr == pk.Name {
			pkIdx = i
			break
		}
	}
	require.GreaterOrEqual(t, pkIdx, 0, "the mock batch carries the pk column")
	for i := 0; i < h.rowsCount; i++ {
		require.NoError(t,
			vector.SetFixedAtWithTypeCheck[int32](cnBat.Vecs[pkIdx], i, int32(h.rowsCount+i)))
	}
	require.NoError(t, testutil.WriteToRelation(
		h.ctx, txnPeer, relPeer, cnBat, false, true))
	require.NoError(t, txnPeer.Commit(h.ctx))
	waitTS := h.dEngine.Now()
	h.waitApplied(waitTS)
	// Persist the in-window content: the compaction turns the peer's rows
	// into data objects whose create entries land inside (S, F] — the
	// production object-index observable the recovered state must report
	// for the window (a fresh in-memory insert is only in the state's rows
	// btree, invisible to the timestamp-index walk).
	h.compact(txnA)

	f := h.frontier()
	fTS := types.TimestampToTS(f)
	require.True(t, sTS.LT(&fTS), "the member's stage needs distinct S and F")

	dbID := relA.GetDBID(h.ctx)
	realTableID := relA.GetTableID(h.ctx)
	eng := h.dEngine.Engine

	// The parent-snapshot read FIRST: the member (and this harness) has not
	// read the table yet, so the production subscribe handshake has not
	// started for it and no logtail can ever arrive — the reader is what
	// drives toSubscribeTable and waitCanServeTableSnapshot, synchronizing
	// the feed exactly like production. The read must stay at the
	// transaction snapshot: the peer's in-window rows stay invisible even
	// though the subscribe's catch-up will carry them into the state.
	snapshotView := h.readPKs(txnA, relA)
	require.Len(t, snapshotView, h.rowsCount,
		"the parent snapshot keeps seeing exactly its seed rows")

	// Readiness: the subscribe consume is asynchronous; wait — bounded,
	// exactly like the harness's waitApplied — until the SUBSCRIBED
	// partition's state reports at least one in-window data-object create
	// (the merged generation the compaction produced). Everything the
	// member then observes is fed state, never feed-race state.
	deadline := time.Now().Add(60 * time.Second)
	livePart := eng.GetOrCreateLatestPart(
		h.ctx, uint64(catalog.System_Account), dbID, realTableID)
	for {
		liveState := livePart.Snapshot()
		ins, _ := liveState.GetChangedObjsBetweenForTombstoneTransfer(sTS, types.MaxTs())
		if len(ins) > 0 {
			break
		}
		require.Less(t, time.Now(), deadline,
			"the subscribed live partition must consume the in-window compaction transitions")
		time.Sleep(10 * time.Millisecond)
	}

	// The recovery-catalog twin lives on the harness's committed history
	// (its cache key and markers are the test's own; the seeds mirror what an
	// hour-gone real cluster retained: an older S-serving historical
	// partition and the live state that keeps covering F).
	const lostTableID = uint64(771234501)

	// Latest-state coverage lost AFTER frontier acquisition: the twin id
	// holds the production "table not exist" subscribe answer (issue
	// 17012's class) — the next getPartitionState for it takes the REAL
	// recovery branch.
	eng.MarkSubscribeAnsweredTableNotExistForTest(dbID, lostTableID)

	// The OLDER cached historical partition: it serves S and never F, and
	// carries none of the (S, F] content by construction.
	early := types.TimestampToTS(timestamp.Timestamp{})
	sDecoyState := eng.TrackHistoricalSnapshotForTest(
		dbID, lostTableID, h.tblName, early, sTS)
	require.True(t, sDecoyState.CanServe(sTS))
	require.False(t, sDecoyState.CanServe(fTS),
		"the seeded historical partition covers the transaction snapshot, never the frontier")

	// The LIVE partition of the real table — the state that truly covers F
	// with the committed (S, F] content — registered under the same lost key.
	// The harness's mock subscribe flow never installs a live partition's
	// production serving duration (consumeSubscribeResp's UpdateDuration(
	// checkpoint start, MaxTs)), so the real state would serve nothing
	// through the recovery's CanServe lookup. Install exactly that duration
	// (the production-assigned one) while the table is quiescent, then track
	// the live partition: an F-anchored lookup must reach this state — with
	// the fed committed (S, F] content on it — and never the older
	// S-serving historical partition.
	eng.ServePartitionRangeForTest(
		livePart, types.TimestampToTS(timestamp.Timestamp{}), types.MaxTs())
	liveState := livePart.Snapshot()
	require.True(t, liveState.CanServe(fTS),
		"the live partition state (with the production subscribe duration installed) must serve the frontier read point")
	eng.TrackSnapshotForTest(dbID, lostTableID, h.tblName, livePart, fTS)

	// A deterministic checkpoint authority for the recovery's request: one
	// authoritative global entry covering both anchors, so the cache lookup —
	// the recovery's real discriminator — decides what is served.
	var requested *types.TS
	restore := disttae.SwapRequestSnapshotReadForTest(
		func(ctx context.Context, snapshot *types.TS) (any, error) {
			snapshotCopy := *snapshot
			requested = &snapshotCopy
			zero := timestamp.Timestamp{}
			end := fTS.ToTimestamp()
			return &cmd_util.SnapshotReadResp{
				Succeed: true,
				Entries: []*cmd_util.CheckpointEntryResp{{
					Start:     &zero,
					End:       &end,
					EntryType: int32(checkpoint.ET_Global),
				}},
			}, nil
		})
	defer restore()

	// THE CARRIER-BOUND recovery (the executing current read): the relation
	// operator resolves to F — the recovery must request F and receive the
	// F-covering live state, not the older S-serving cache entry.
	requested = nil
	carrier := client.ApplyExecutionReadTS(txnA, f)
	psF, err := eng.GetPartitionStateThroughRecoveryForTest(
		h.ctx, relA, lostTableID, carrier)
	require.NoError(t, err)
	require.NotNil(t, requested, "the recovery consulted the checkpoint authority")
	require.Equal(t, fTS, *requested,
		"the carrier-bound recovery requests the EXECUTION READ POINT F, not the transaction snapshot")
	require.NotNil(t, psF)
	require.True(t, psF.CanServe(fTS),
		"the recovered state provably covers the executing read point")
	require.Same(t, livePart.Snapshot(), psF,
		"the F-anchored request is served by the F-covering state, never by the older historical partition")
	_, createdF := psF.GetChangedObjsBetweenForTombstoneTransfer(sTS, fTS)
	require.NotEmpty(t, createdF,
		"the recovered F-covering state reports the persisted (S, F] objects: the in-window commits it must carry")

	// THE ORDINARY recovery (the parent transaction's snapshot read):
	// the operator's effective read point is S, the S-request returns the
	// older historical partition — today's behavior, byte-identical for
	// every carrier-less relation — and that partition carries NONE of the
	// in-window content: the incomplete-history state an F-read must never
	// consume.
	requested = nil
	psS, err := eng.GetPartitionStateThroughRecoveryForTest(
		h.ctx, relA, lostTableID, txnA)
	require.NoError(t, err)
	require.NotNil(t, requested)
	require.Equal(t, sTS, *requested,
		"an ordinary relation still recovers at its transaction snapshot S")
	require.Same(t, sDecoyState, psS,
		"the S-anchored request keeps serving the S-covering historical partition (unchanged parent semantics)")
	require.True(t, psS.CanServe(sTS))
	_, createdS := psS.GetChangedObjsBetweenForTombstoneTransfer(sTS, fTS)
	require.Empty(t, createdS,
		"the S-serving cached partition carries none of the (S, F] content: serving it to an F-read would lose every in-window commit")

	// UNCHANGED PARENT SNAPSHOT RESULTS, re-checked after the recovery
	// phases and the checkpoint-authority swap: the subject's snapshot never
	// moved across any of them.
	h.assertSubjectSnapshotUnchanged(txnA, "the recovery phases")

	// CURRENT-READ RESULTS: the exclusive execution-read relation at F
	// discovers the peer's in-window rows through the same fed cluster
	// state — the locked-read shape of the delivered statement.
	hcar, handle := h.exclusiveAtF(relA, txnA)
	currentView := h.readPKs(hcar, handle)
	require.Len(t, currentView, 2*h.rowsCount,
		"the carrier-bound current read discovers the peer's persisted in-window rows")
	for i := 0; i < h.rowsCount; i++ {
		require.Contains(t, currentView, int32(i),
			"every seed row stays visible at the frontier")
		require.Contains(t, currentView, int32(i+h.rowsCount),
			"every peer row is discovered through the fed frontier state")
	}
	h.assertSubjectSnapshotUnchanged(txnA, "the exclusive current-read binding")
	require.NoError(t, txnA.Rollback(h.ctx))
}
