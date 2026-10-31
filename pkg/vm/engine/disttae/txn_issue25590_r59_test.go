// Copyright 2026 Matrix One
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

// Round-59 (issue #25590 review of PR #30988 head 19358d2881, P2 at
// pkg/vm/engine/disttae/txn.go:3914): the RC tombstone-object transfer
// was the last writer class whose sinker carried NO per-object
// admission hook. TransferFlow.fillDefaults built the transfer's
// tombstone sinker unhooked, so transferTombstoneObjects persisted the
// replacement objects at flow.Process (transfer.go:111) BEFORE the
// DELETE record at writeFileLockedWithAutoIncrEpoch ever reached the
// registerUnpublishedObjectRecordLocked admission choke — the record's
// names were the FRESH class there, so at a saturated engine bound the
// choke refused the record but the r5 fail-open orphan retain
// (issue #25590 round-21 review) booked every physically persisted
// replacement anyway: further transfers kept growing the retained
// backlog despite admission refusal. The reviewer's producer/gate probe
// observed exactly retained=2 behind a limit=1 bound with a physical
// replacement file behind every refused record, and the SAME producer
// built with SinkerCapacityOption refused before persistence.
//
// The fix binds ConstructCNTombstoneObjectsTransferFlow's sinker to
// txn.SinkerCapacityOption(), so the transfer joins the reservation
// lifecycle every other producer already runs: each replacement
// reserves one in-flight unit BEFORE it goes durable, a saturated bound
// refuses the transfer with nothing half-persisted, the record below
// converts the reservation into recorded ownership, and a transfer
// interrupted between spill and record settles through the same
// statement-rollback/terminal reservation drains.
//
// The regressions below are behavioral on both heads:
//
//   - the pre-fix leg rebuilds the unhooked producer (the construction
//     fillDefaults still allows) and drives the production record route,
//     reproducing the probe's exact overshoot — recorded object plus a
//     refused record's fail-open retained replacement, lived=2 behind a
//     one-unit bound, two physical files;
//   - the bound leg proves the production wiring refuses each saturated
//     transfer BEFORE persistence and books nothing new across rounds —
//     the backlog cannot grow despite the refusal;
//   - the conversion leg proves the reservation's full lifecycle record
//     (reserve before durable, convert at the record), and the
//     interrupted leg proves the never-recorded reservation keeps a
//     reachable cleanup owner through the terminal drain and the queue
//     deletes the physical file.

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/stretchr/testify/require"
)

// r59DoneReader ends the flow's source immediately: Process performs no
// row lookup, so every leg below drives the sinker tail — the exact
// persistence sequence the RC transfer runs at transfer.go:111 — while
// the sinker wiring stays wholly production's.
type r59DoneReader struct{}

func (r59DoneReader) Read(
	context.Context, []string, *plan.Expr, *mpool.MPool, *batch.Batch,
) (bool, error) {
	return true, nil
}

func (r59DoneReader) Close() error                         { return nil }
func (r59DoneReader) SetOrderBy([]*plan.OrderBySpec)       {}
func (r59DoneReader) GetOrderBy() []*plan.OrderBySpec      { return nil }
func (r59DoneReader) SetIndexParam(*plan.IndexReaderParam) {}
func (r59DoneReader) SetFilterZM(objectio.ZoneMap)         {}

func TestIssue25590RCTombstoneTransferBoundToTheAdmissionLedger(t *testing.T) {
	colexec.NewServer("")
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	issue25590InitDumpableFixture(t, txn)
	defer func() {
		for i := range txn.writes {
			txn.releaseWorkspaceEntryBatchLocked(i)
		}
	}()

	ctx := context.Background()
	e := txn.engine
	e.pClient = PushClient{}
	failFS := issue25590SwapProcSharedFS(t, txn)
	e.fs = failFS

	rel, err := txn.getTable(ctx, 1, "db", "tbl")
	require.NoError(t, err)
	tbl, ok := rel.(*txnTable)
	require.True(t, ok)

	// The flow exactly as ConstructCNTombstoneObjectsTransferFlow builds
	// it: the stubbed reader ends immediately, the deleted-window filter
	// accepts everything, and the sinker wiring — the only production
	// surface this review touches — is what each leg varies.
	newFlow := func(sinkerOpts ...TransferOption) *TransferFlow {
		return ConstructTransferFlow(
			tbl, objectio.HiddenColumnSelection_None,
			r59DoneReader{},
			func(*objectio.ObjectId) bool { return true },
			nil, txn.proc.Mp(), failFS, sinkerOpts...)
	}

	// One tombstone row in the sinker's own schema: (rowid, pk).
	tombstoneBatch := func() *batch.Batch {
		attrs, _ := objectio.GetTombstoneSchema(
			types.T_int64.ToType(), objectio.HiddenColumnSelection_None)
		objID := objectio.NewObjectid()
		rowID := types.NewRowIDWithObjectIDBlkNumAndRowID(objID, 0, 0)
		bat := batch.NewWithSize(2)
		bat.SetAttributes(attrs)
		rowids := vector.NewVec(types.T_Rowid.ToType())
		pks := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(rowids, rowID, false, txn.proc.Mp()))
		require.NoError(t, vector.AppendFixed(pks, int64(1), false, txn.proc.Mp()))
		bat.Vecs[0] = rowids
		bat.Vecs[1] = pks
		bat.SetRowCount(1)
		return bat
	}

	ledger := func() (queued int, live int64, inFlight int64) {
		e.unpublishedPending.mu.Lock()
		defer e.unpublishedPending.mu.Unlock()
		return len(e.unpublishedPending.names), e.unpublishedPending.live,
			e.unpublishedPending.inFlight
	}

	// driveTransfer runs the flow's persistence tail — Write then Sync,
	// the sequence transferTombstoneObjects runs before its record — and
	// returns the persisted object stats.
	driveTransfer := func(flow *TransferFlow) ([]objectio.ObjectStats, error) {
		bat := tombstoneBatch()
		if err := flow.sinker.Write(ctx, bat); err != nil {
			return nil, err
		}
		bat.Clean(txn.proc.Mp())
		if err := flow.sinker.Sync(ctx); err != nil {
			return nil, err
		}
		slist, _ := flow.sinker.GetResult()
		return slist, nil
	}

	// recordTransfer reproduces the production record race - the DELETE
	// record the transfer assembles after its replacements are durable
	// (transfer.go:142-152), with the transaction lock held exactly as
	// forEachTableHasDeletesLocked holds it.
	recordTransfer := func(stats objectio.ObjectStats) error {
		recBat := colexec.AllocCNS3ResultBat(true)
		defer recBat.Clean(txn.proc.Mp())
		require.NoError(t, colexec.ExpandObjectStatsToBatch(
			txn.proc.Mp(), true, recBat, false, stats))
		txn.Lock()
		defer txn.Unlock()
		// The fixture table carries no extraInfo - a table without
		// auto-increment records at epoch 0, exactly as the resolved
		// extraInfo.AutoIncrEpoch would be.
		return txn.writeFileLockedWithAutoIncrEpoch(
			DELETE, 1, issue25590DBID, issue25590TableID,
			"db", "tbl", stats.ObjectName().String(), recBat,
			txn.tnStores[0], 0)
	}

	// Leg 1 - the reviewer's exact probe on the PRE-FIX producer class:
	// the unhooked flow sink's transfer objects persist BEFORE the record
	// gate runs, so at a one-unit bound the first record converts fine,
	// and the SECOND transfer's record gets refused while its physical
	// replacement file lands anyway — the fail-open retain books every
	// refused record's persisted name, and the retained population ends
	// up beyond the configured bound.
	knobSaved := unpublishedPendingAdmissionLimit
	unpublishedPendingAdmissionLimit = 1
	t.Cleanup(func() { unpublishedPendingAdmissionLimit = knobSaved })

	unhooked1 := newFlow()
	defer func() { require.NoError(t, unhooked1.Close()) }()
	require.False(t, unhooked1.sinker.HasUncommittedCapacityHook(),
		"the pre-fix construction class stays reachable - the emptied option list")
	slist1, err := driveTransfer(unhooked1)
	require.NoError(t, err)
	require.Len(t, slist1, 1)
	name1 := slist1[0].ObjectName().String()
	require.NoError(t, recordTransfer(slist1[0]),
		"the first transfer's record records fine - the bound takes its obligation")
	require.Contains(t, issue25590RecordedSnapshot(txn), name1)
	_, live, inFlight := ledger()
	require.Equal(t, int64(1), live,
		"the recorded replacement is the one-unit bound's only member (retained=1)")
	require.Zero(t, inFlight)
	require.Len(t, failFS.persistedSnapshot(), 1,
		"the first transfer's replacement file is physically durable")

	// The second unhooked transfer behind the full bound: the sinker
	// persists FIRST (no hook consults the ledger), so the physical
	// replacement file exists before the record choke ever runs. Each
	// transfer cycle owns a fresh flow, exactly as the production
	// constructor builds one per table pass.
	unhooked2 := newFlow()
	defer func() { require.NoError(t, unhooked2.Close()) }()
	require.False(t, unhooked2.sinker.HasUncommittedCapacityHook())
	slist2, err := driveTransfer(unhooked2)
	require.NoError(t, err,
		"the unhooked transfer persists with no admission consulting its spill")
	require.Len(t, slist2, 1)
	name2 := slist2[0].ObjectName().String()
	require.ErrorContains(t, recordTransfer(slist2[0]), "workspace admission paused",
		"the refused transfer record cannot pass behind the one-unit bound - the reviewer's verdict shape")
	// The pre-fix impact, verbatim: the refusal fail-open-retains the
	// reservation-less name, so the retained population is now 2 behind
	// the limit of 1, with a physical replacement file behind each name.
	require.Equal(t, []string{name2}, txn.pendingObjectCleanupNames(),
		"the refused record's fail-open retain kept the persisted replacement reachable")
	_, live, inFlight = ledger()
	require.Equal(t, int64(2), live,
		"retained=2 behind a limit=1 - the exact storage bound the review's probe observed")
	require.Zero(t, inFlight)
	require.Len(t, failFS.persistedSnapshot(), 2,
		"both physical replacement files are durable - the transfer persisted both before their records")

	persistedAtSatsuration := len(failFS.persistedSnapshot())
	retainedAtSaturation := txn.pendingObjectCleanupNames()

	// Leg 2 - the fix: the production transfer wiring
	// (ConstructCNTombstoneObjectsTransferFlow's new option) refuses
	// each saturated transfer BEFORE anything goes durable, so repeated
	// refused rounds keep persisting NOTHING and keep the retained
	// population exactly where the refused backlog sits — the transfer
	// can no longer grow it despite the admission refusal.
	for round := 0; round < 3; round++ {
		hooked := newFlow(WithTransferSinkerOption(txn.SinkerCapacityOption()))
		require.True(t, hooked.sinker.HasUncommittedCapacityHook(),
			"round %d: the production transfer's sinker carries the workspace's per-object admission hook", round)
		_, err := driveTransfer(hooked)
		require.ErrorContains(t, err, "workspace admission paused",
			"round %d: the saturated bound refuses the replacement BEFORE it goes durable", round)
		require.NoError(t, hooked.Close())
		require.Len(t, failFS.persistedSnapshot(), persistedAtSatsuration,
			"round %d: the refused transfer persisted nothing", round)
		_, live, inFlight := ledger()
		require.Equal(t, int64(2), live,
			"round %d: the retained population stays exactly the refused backlog - no growth", round)
		require.Zero(t, inFlight,
			"round %d: the refusal booked no in-flight unit - the reservation happens before the dump", round)
		require.Equal(t, retainedAtSaturation, txn.pendingObjectCleanupNames(),
			"round %d: the refusal fail-open-retained nothing new", round)
	}

	// Leg 3 - the reservation's full record lifecycle on the production
	// wiring: under the restored bound the transfer persists its
	// replacement, the hook holds one in-flight unit and the name's
	// reservation on the transaction, and the record below CONVERTs the
	// reservation into recorded ownership with the physical file kept.
	unpublishedPendingAdmissionLimit = knobSaved

	hooked3 := newFlow(WithTransferSinkerOption(txn.SinkerCapacityOption()))
	defer func() { require.NoError(t, hooked3.Close()) }()
	slist3, err := driveTransfer(hooked3)
	require.NoError(t, err)
	require.Len(t, slist3, 1)
	name3 := slist3[0].ObjectName().String()
	_, _, inFlight3 := ledger()
	require.Equal(t, int64(1), inFlight3,
		"the hook reserved the replacement's in-flight unit before it went durable")
	require.True(t, txn.hasUnpublishedDumpReservation(name3),
		"the reservation is keyed by the persisted name on the owning transaction")
	require.NoError(t, recordTransfer(slist3[0]),
		"the reserved name's record converts - the capacity was already accounted at persistence")
	require.False(t, txn.hasUnpublishedDumpReservation(name3),
		"the record consumed the reservation exactly")
	_, _, inFlight3 = ledger()
	require.Zero(t, inFlight3,
		"the record converted the in-flight unit into recorded ownership")
	require.Contains(t, issue25590RecordedSnapshot(txn), name3)
	issue25590RequireKept(t, failFS.FileService, name3,
		"the recorded replacement's file keeps its record's own lifecycle - nothing deletes it")

	// Leg 4 - the cleanup lifecycle's interrupted window: a transfer
	// whose reservation is already booked (the replacement is durable,
	// the record never lands) settles through the terminal reservation
	// drain — the leftover keeps a reachable cleanup owner, the engine
	// bound gets its in-flight unit back, and the queue deletes the
	// physical file afterwards.
	hooked4 := newFlow(WithTransferSinkerOption(txn.SinkerCapacityOption()))
	slist4, err := driveTransfer(hooked4)
	require.NoError(t, err)
	require.Len(t, slist4, 1)
	name4 := slist4[0].ObjectName().String()
	require.True(t, txn.hasUnpublishedDumpReservation(name4))
	_, _, inFlight4 := ledger()
	require.Equal(t, int64(1), inFlight4)

	// The production terminal drain the commit/rollback/fragment finals
	// run for every leftover reservation owner.
	txn.Lock()
	txn.drainUnpublishedDumpReservationsLocked(
		drainUnpublishedDumpReservationsTerminalReason)
	txn.Unlock()
	require.False(t, txn.hasUnpublishedDumpReservation(name4),
		"the drain settled the never-recorded transfer's reservation exactly")
	// The drain reached the interrupted transfer's replacement through
	// the SAME pending owner leg 1's fail-open retain already holds:
	// name2 is still there because its statement is this round's own
	// refused record — the terminal handoff collects both together.
	require.ElementsMatch(t, []string{name2, name4}, txn.pendingObjectCleanupNames(),
		"the interrupted transfer's physical replacement reached its cleanup owner")
	_, _, inFlight4 = ledger()
	require.Zero(t, inFlight4,
		"the drain returned the leftover's in-flight unit to the engine bound")

	// The queue handoff and the bounded delete collect the physical
	// files — the interrupted transfer leaks nothing ownerless.
	txn.Lock()
	txn.handoffPendingObjectCleanup()
	txn.Unlock()
	require.ElementsMatch(t, []string{name2, name4}, issue25590QueuedNames(e))
	require.NoError(t, e.deleteQueuedUnpublishedPendingOnce(ctx))
	issue25590RequireGone(t, failFS.FileService, name4,
		"the queue deleted the never-recorded transfer's replacement file")
	issue25590RequireGone(t, failFS.FileService, name2,
		"the queue also deleted leg 1's fail-open-retained file")
	require.NoError(t, hooked4.Close())
}
