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

// TestIssue25590MaterializingWritersReserveBeforePersistence pins the
// round-32 pre-PR review's finding 2 against design r11's I4: BOTH
// materializing writer families the TableMetaReader builds - the data
// writer of collectDataOfAObjsAndInMem and the tombstone writer of
// collectTombstoneOfAObjsAndInMem - must carry the RECORDING
// transaction's per-object admission option, so every materialized
// object reserves one in-flight unit BEFORE it goes durable, the
// destination's record choke converts the reservation, and a saturated
// ledger refuses the spill with nothing half-persisted behind it. The
// pre-fix reader built both writers without any capacity hook: repeated
// clone/ALTER attempts at a saturated bound kept persisting fresh
// objects whose never-recording faces failed only at the record choke's
// fail-open retain, violating I4's "nothing beyond the bound". The legs
// here also pin the two ownership controls the review demanded: the
// failure-before-recording cleanup (an unrecorded materialization's
// reservation settles to an owner the queue's drain physically deletes)
// and the committed-borrowed preservation (a shared committed file never
// enters the attempt's cleanup path and survives the same drain).

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/stretchr/testify/require"
)

const r32TableID = uint64(46)

// newR32MaterializingFixture builds the fixture both drives share: a
// transaction carrying the dumpable production wiring, a tracking
// shared filesystem, a well-typed [a(pk), v, rowid] source table and
// the capability option table_clone plumbs - the RECORDING
// transaction's, never one derived from the source table's workspace.
func newR32MaterializingFixture(
	t *testing.T,
) (*Transaction, *Engine, *issue25590PostPersistFailFS, *txnTable) {
	t.Helper()
	colexec.NewServer("")
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	issue25590InitDumpableFixture(t, txn)

	e := txn.engine
	e.pClient = PushClient{}
	failFS := issue25590SwapProcSharedFS(t, txn)
	e.fs = failFS

	dbv := txn.databaseOps.existAndActive(genDatabaseKey(1, "db"))
	require.NotNil(t, dbv)
	tbl := &txnTable{
		accountId: 1,
		tableId:   r32TableID,
		tableName: "r32tbl",
		db:        dbv,
		tableDef: &pbplan.TableDef{
			Cols: []*pbplan.ColDef{
				{Name: "a", Seqnum: 0, Typ: pbplan.Type{Id: int32(types.T_int64)},
					Primary: true},
				{Name: "v", Seqnum: 1, Typ: pbplan.Type{Id: int32(types.T_int64)}},
				{Name: catalog.Row_ID, Seqnum: 2,
					Typ: pbplan.Type{Id: int32(types.T_Rowid)}},
			},
			Pkey: &pbplan.PrimaryKeyDef{PkeyColName: "a"},
		},
	}
	tbl.proc.Store(txn.proc)
	txn.tableOps.addCreateTable(genTableKey(1, "r32tbl", 7, "db"), 0, tbl)
	op, ok := txn.op.(*mock_frontend.MockTxnOperator)
	require.True(t, ok, "fixture must expose the mock operator")
	op.EXPECT().SnapshotTS().Return(timestamp.Timestamp{}).AnyTimes()
	// The ACTUAL exported reader constructor (round-33 finding 1's
	// regression drives it) asks the snapshot-op question when it picks
	// the reader's write boundary.
	op.EXPECT().IsSnapOp().Return(false).AnyTimes()
	if e.cloneTxnCache == nil {
		e.cloneTxnCache = newCloneTxnCache()
	}
	return txn, e, failFS, tbl
}

func TestIssue25590MaterializingWritersReserveBeforePersistence(t *testing.T) {
	txn, e, failFS, tbl := newR32MaterializingFixture(t)
	ctx := context.Background()
	mp := txn.proc.Mp()

	// Force the sinker to spill every object at its Sync so each drive
	// deterministically consults the capacity hook for exactly one
	// fresh object, whatever the row count.
	disarmSmallObjs := issue25590ArmKeyedFault(t, objectio.FJ_CNFlushSmallObjs, 20, "")
	defer disarmSmallObjs()

	knobSaved := unpublishedPendingAdmissionLimit
	t.Cleanup(func() { unpublishedPendingAdmissionLimit = knobSaved })

	// The reader exactly as table_clone materializes, with the RECORDING
	// transaction's admission option the production Prepare plumbs
	// through the constructor.
	newReader := func() *TableMetaReader {
		return &TableMetaReader{
			table:        tbl,
			fs:           failFS,
			snapshot:     types.TimestampToTS(txn.op.SnapshotTS()),
			txnOffset:    len(txn.writes),
			pState:       logtailreplay.NewPartitionState("", true, 0, false),
			sinkerOption: txn.SinkerCapacityOption(),
		}
	}
	newDeleteEntry := func(rows int) {
		rowids := make([]types.Rowid, rows)
		pks := make([]int64, rows)
		for i := range rowids {
			rowids[i] = types.RandomRowid()
			pks[i] = int64(i + 1)
		}
		delBat := batch.NewWithSize(2)
		delBat.SetAttributes([]string{objectio.PhysicalAddr_Attr, "a"})
		rv := vector.NewVec(types.T_Rowid.ToType())
		require.NoError(t, vector.AppendFixedList(rv, rowids, nil, mp))
		pv := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixedList(pv, pks, nil, mp))
		delBat.Vecs[0] = rv
		delBat.Vecs[1] = pv
		delBat.SetRowCount(rows)
		txn.appendWorkspaceEntryLocked(Entry{
			typ:          DELETE,
			accountId:    1,
			databaseId:   tbl.db.databaseId,
			tableId:      r32TableID,
			databaseName: "db",
			tableName:    "r32tbl",
			bat:          delBat,
		})
	}
	driveTombstone := func() (string, error) {
		reader := newReader()
		outBatch := colexec.AllocCNS3ResultBat(true)
		defer outBatch.Clean(mp)
		_, err := reader.collectTombstoneOfAObjsAndInMem(
			ctx, mp, outBatch,
			[]uint16{0, 1},
			objectio.TombstoneAttrs_CN_Created,
			[]types.Type{types.T_Rowid.ToType(), types.T_int64.ToType()},
			readutil.ObjListRelData{},
		)
		if err != nil {
			return "", err
		}
		st := r32FirstOutBatchStats(t, outBatch)
		return st.ObjectName().String(), nil
	}

	// ------------------------------------------------------------------
	// Leg 1: the DATA family reserves before persistence and the
	// destination's record choke converts the reservation.
	// ------------------------------------------------------------------
	// One real readable source object, written by an UNHOOKED writer
	// exactly the committed source state owns it.
	srcRows := batch.NewWithSize(3)
	srcRows.SetAttributes([]string{objectio.PhysicalAddr_Attr, "a", "v"})
	srcRowids := make([]types.Rowid, 16)
	srcPKs := make([]int64, 16)
	for i := range srcRowids {
		srcRowids[i] = types.RandomRowid()
		srcPKs[i] = int64(i + 1)
	}
	rv := vector.NewVec(types.T_Rowid.ToType())
	require.NoError(t, vector.AppendFixedList(rv, srcRowids, nil, mp))
	av := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(av, srcPKs, nil, mp))
	vv := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(vv, srcPKs, nil, mp))
	srcRows.Vecs[0] = rv
	srcRows.Vecs[1] = av
	srcRows.Vecs[2] = vv
	srcRows.SetRowCount(16)
	srcWriter := colexec.NewCNS3DataWriterForService(
		txn.proc.GetService(), mp, failFS, tbl.tableDef, -1, false)
	require.NoError(t, srcWriter.Write(ctx, srcRows))
	srcStatsList, err := srcWriter.Sync(ctx)
	require.NoError(t, err)
	srcWriter.Close()
	srcRows.Clean(mp)
	require.Len(t, srcStatsList, 1, "the source materializes exactly one object")
	srcStats := srcStatsList[0]
	require.Equal(t, 1, len(failFS.persistedSnapshot()),
		"before the drives only the source object exists")

	reader := newReader()
	dataOut := colexec.AllocCNS3ResultBat(false)
	defer dataOut.Clean(mp)
	seqnums, colTypes, attrs, _, _ := colexec.GetSequmsAttrsSortKeyIdxFromTableDef(tbl.tableDef)
	objRelData := readutil.ObjListRelData{}
	objRelData.AppendObj(&srcStats)
	require.NoError(t, err)
	_, err = reader.collectDataOfAObjsAndInMem(
		ctx, mp, dataOut, seqnums, attrs, colTypes, objRelData)
	require.NoError(t, err,
		"the bound-free data materialization succeeds")
	matStats := r32FirstOutBatchStats(t, dataOut)
	matName := matStats.ObjectName().String()
	require.NotEqual(t, srcStats.ObjectName().String(), matName,
		"the materialized object is a FRESH object, not the source's name")
	require.Equal(t, 2, len(failFS.persistedSnapshot()),
		"the data family materialized exactly one fresh object")

	// The reservation is outstanding on the RECORDING transaction and
	// the in-flight unit is accounted: nothing was recorded yet.
	pending, reserved := txn.UnsettledCleanupOwnershipForTest()
	require.Empty(t, pending)
	require.Equal(t, []string{matName}, reserved,
		"the data family reserved its materialized name BEFORE the object went durable")
	queued, live, inFlight := e.InspectUnpublishedPendingForTest()
	require.Empty(t, queued)
	require.Equal(t, 1, inFlight,
		"the materialized object holds exactly one in-flight unit under the ledger")
	require.Zero(t, live)

	// The destination relation's record choke converts the reservation -
	// the same conversion table_clone's dstRel.Write drives.
	recBat := colexec.AllocCNS3ResultBat(false)
	defer recBat.Clean(mp)
	require.NoError(t, colexec.ExpandObjectStatsToBatch(
		mp, false, recBat, false, r32FirstOutBatchStats(t, dataOut)))
	require.NoError(t, txn.WriteFile(
		INSERT, 1, tbl.db.databaseId, r32TableID, "db", "r32tbl",
		matName, recBat, txn.tnStores[0]))
	_, reserved = txn.UnsettledCleanupOwnershipForTest()
	require.Empty(t, reserved,
		"the record converted the data family's reservation")
	queued, live, inFlight = e.InspectUnpublishedPendingForTest()
	require.Zero(t, queued)
	require.Zero(t, inFlight)
	require.Equal(t, 1, live,
		"the converted reservation became the recorded object's owned membership")
	require.Contains(t, txn.recordedUnpublishedObjectNames, matName)

	// ------------------------------------------------------------------
	// Leg 2: at the saturated bound, repeated attempts of BOTH families
	// persist NOTHING beyond the bound.
	// ------------------------------------------------------------------
	const roundRows = 16
	const bound = 2
	newDeleteEntry(roundRows)

	unpublishedPendingAdmissionLimit = bound
	// The recorded object (owned) plus the filler (in-flight) pin the
	// ledger AT the bound; materializing anything fresh must refuse at
	// the writer's hook and persist nothing.
	require.NoError(t, txn.AdoptRemoteTransferredObjectNames(
		[]string{"r32-filler"}))
	persistedAtSaturation := len(failFS.persistedSnapshot())

	for round := 0; round < 3; round++ {
		_, err = driveTombstone()
		require.ErrorContains(t, err, "workspace admission paused",
			"tombstone round %d: the saturated bound refuses the tombstone family's spill", round)
		require.Len(t, failFS.persistedSnapshot(), persistedAtSaturation,
			"tombstone round %d: the refused attempt persisted nothing", round)

		tombReader := newReader()
		dataOut2 := colexec.AllocCNS3ResultBat(false)
		_, err = tombReader.collectDataOfAObjsAndInMem(
			ctx, mp, dataOut2, seqnums, attrs, colTypes, objRelData)
		dataOut2.Clean(mp)
		require.ErrorContains(t, err, "workspace admission paused",
			"data round %d: the saturated bound refuses the data family's spill", round)
		require.Len(t, failFS.persistedSnapshot(), persistedAtSaturation,
			"data round %d: the refused attempt persisted nothing", round)

		queued, live, inFlight := e.InspectUnpublishedPendingForTest()
		require.Zero(t, queued)
		require.Equal(t, 1, live,
			"round %d: only the recorded object's owned membership pins the bound", round)
		require.Equal(t, 1, inFlight,
			"round %d: only the filler's reservation stays in flight", round)
	}
	_, reserved = txn.UnsettledCleanupOwnershipForTest()
	require.Equal(t, []string{"r32-filler"}, reserved,
		"the repeated refused attempts booked no reservation at all")

	// ------------------------------------------------------------------
	// Leg 3: the failure-before-recording cleanup and the
	// committed-borrowed preservation.
	// ------------------------------------------------------------------
	// One tombstone materialization that never records (the pipeline
	// broke between persistence and the destination relation's Write):
	// the reservation stays the object's only owner.
	unpublishedPendingAdmissionLimit = knobSaved
	unrecordedTombName, err := driveTombstone()
	require.NoError(t, err,
		"under the restored bound the tombstone family materializes")
	require.Equal(t, persistedAtSaturation+1, len(failFS.persistedSnapshot()),
		"sanity: the tombstone materialization persisted exactly its fresh object")
	_, reserved = txn.UnsettledCleanupOwnershipForTest()
	require.Len(t, reserved, 2,
		"the filler's and the unrecorded materialization's reservations are outstanding")
	require.Contains(t, reserved, unrecordedTombName)

	// A committed-borrowed shared file: registered on the clone cache
	// and physically present, never entering the materializing
	// attempts' cleanup path.
	borrowedStats := mockStatsList(t, 1)
	borrowedName := borrowedStats[0].ObjectName().String()
	require.NoError(t, writeObjectToFS(ctx, failFS, borrowedName))
	// The cache keys by the transaction's UUID (the comparator views the
	// id as one), so the fixture txn runs under a uuid-shaped id for the
	// committed-borrowed registration.
	// A uuid-shaped (16-byte) id is what the cache's comparator views
	// the entry keys as, so the fixture txn registers under one.
	cacheRowID := types.RandomRowid()
	var cacheTxnID [16]byte
	copy(cacheTxnID[:], cacheRowID[:16])
	cacheTxnIDBytes := cacheTxnID[:]
	e.cloneTxnCache.AddTxn(cacheTxnIDBytes, txn.op.Txn().SnapshotTS.PhysicalTime)
	e.cloneTxnCache.AddSharedFile(cacheTxnIDBytes, borrowedName)
	require.True(t, e.cloneTxnCache.IsSharedFile(cacheTxnIDBytes, borrowedName))

	// The terminal reservation drain settles never-recorded names into
	// enumerable ownership; the pending handoff moves them to the
	// engine's lossless queue; the drain retires them physically.
	txn.Lock()
	txn.drainUnpublishedDumpReservationsLocked(drainUnpublishedDumpReservationsTerminalReason)
	txn.Unlock()
	pending, reserved = txn.UnsettledCleanupOwnershipForTest()
	require.Empty(t, reserved, "the terminal closed every reservation")
	require.ElementsMatch(t, []string{"r32-filler", unrecordedTombName}, pending,
		"the never-recorded materializations settled to pending ownership")
	require.NotContains(t, pending, borrowedName,
		"the committed-borrowed share never entered the attempts' cleanup path")
	require.NotContains(t, pending, matName,
		"the recorded object keeps its record's own lifecycle")

	txn.Lock()
	txn.handoffPendingObjectCleanup()
	txn.Unlock()
	queuedNames := issue25590QueuedNames(e)
	require.ElementsMatch(t, []string{"r32-filler", unrecordedTombName}, queuedNames,
		"only never-recorded materializations handed to the engine queue")
	require.NotContains(t, queuedNames, borrowedName)

	require.NoError(t, e.DrainUnpublishedPendingForTest(ctx))
	issue25590RequireGone(t, failFS, unrecordedTombName,
		"the unrecorded tombstone materialization deleted once the terminal owned it")
	issue25590RequireKept(t, failFS, borrowedName,
		"the committed-borrowed share survives the same drain")
	require.True(t, e.cloneTxnCache.IsSharedFile(cacheTxnIDBytes, borrowedName),
		"the committed-borrowed share stays registered")
	queued, _, _ = e.InspectUnpublishedPendingForTest()
	require.Empty(t, queued, "the queue retired everything it drained")
}

// r32FirstOutBatchStats unpacks the first object's stats from a
// successful drive's result batch: the data family's result keeps the
// [BlockInfo, ObjectStats] layout while the tombstone family's keeps a
// single ObjectStats column.
func r32FirstOutBatchStats(
	t *testing.T, outBatch *batch.Batch,
) objectio.ObjectStats {
	t.Helper()
	statsVecIdx := 1
	if len(outBatch.Attrs) == 1 {
		statsVecIdx = 0
	}
	require.Greater(t, outBatch.RowCount(), 0,
		"the drive appended no object metadata to its result batch")
	col, area := vector.MustVarlenaRawData(outBatch.Vecs[statsVecIdx])
	stats := objectio.ObjectStats(col[0].GetByteSlice(area))
	require.NotEmpty(t, stats.ObjectName().String())
	return stats
}

// TestIssue25590MaterializingOptionBindsRecordingTxnNotSource pins the
// snapshot-source half of the round-32 review's finding 2: the
// capacity option the materializing writers carry belongs to the
// RECORDING transaction - the one whose workspace records the
// destination relation's metadata batch and whose terminal drain
// converts leftover reservations - never to the source table's
// workspace, which for a snapshot clone belongs to a snapshot operator
// that never records this metadata. The reader here reads one table
// (the source workspace's) while carrying a second transaction's
// option: reservations must land ONLY on the recording side, so a
// regression that "simplifies" the plumbing to the source table's own
// transaction (whose terminal cannot settle the names) goes red.
func TestIssue25590MaterializingOptionBindsRecordingTxnNotSource(t *testing.T) {
	srcTxn, _, failFS, tbl := newR32MaterializingFixture(t)
	recTxn := newTransactionWithActivePKTableForTest(t, "pk2")
	ctx := context.Background()
	mp := srcTxn.proc.Mp()

	disarmSmallObjs := issue25590ArmKeyedFault(t, objectio.FJ_CNFlushSmallObjs, 20, "")
	defer disarmSmallObjs()

	// The source table's workspace holds the rows the tombstone family
	// materializes - sixteen delete-tombstone rows below the reader's
	// write boundary.
	rowids := make([]types.Rowid, 16)
	pks := make([]int64, 16)
	for i := range rowids {
		rowids[i] = types.RandomRowid()
		pks[i] = int64(i + 1)
	}
	delBat := batch.NewWithSize(2)
	delBat.SetAttributes([]string{objectio.PhysicalAddr_Attr, "a"})
	rv := vector.NewVec(types.T_Rowid.ToType())
	require.NoError(t, vector.AppendFixedList(rv, rowids, nil, mp))
	pv := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(pv, pks, nil, mp))
	delBat.Vecs[0] = rv
	delBat.Vecs[1] = pv
	delBat.SetRowCount(16)
	srcTxn.appendWorkspaceEntryLocked(Entry{
		typ:          DELETE,
		accountId:    1,
		databaseId:   tbl.db.databaseId,
		tableId:      r32TableID,
		databaseName: "db",
		tableName:    "r32tbl",
		bat:          delBat,
	})

	// The reader reads the SOURCE table's data, but carries the
	// RECORDING transaction's option - exactly what a snapshot-source
	// clone's Prepare resolves and passes.
	reader := &TableMetaReader{
		table:        tbl,
		fs:           failFS,
		snapshot:     types.TimestampToTS(srcTxn.op.SnapshotTS()),
		txnOffset:    len(srcTxn.writes),
		pState:       logtailreplay.NewPartitionState("", true, 0, false),
		sinkerOption: recTxn.SinkerCapacityOption(),
	}
	outBatch := colexec.AllocCNS3ResultBat(true)
	defer outBatch.Clean(mp)
	_, err := reader.collectTombstoneOfAObjsAndInMem(
		ctx, mp, outBatch,
		[]uint16{0, 1},
		objectio.TombstoneAttrs_CN_Created,
		[]types.Type{types.T_Rowid.ToType(), types.T_int64.ToType()},
		readutil.ObjListRelData{},
	)
	require.NoError(t, err, "the materialization succeeds under the bound")

	st := r32FirstOutBatchStats(t, outBatch)
	matName := st.ObjectName().String()

	// The reservation landed ONLY on the recording transaction: the
	// source workspace's transaction books nothing, because its
	// terminal can never convert or settle a name it never records.
	srcPending, srcReserved := srcTxn.UnsettledCleanupOwnershipForTest()
	require.Empty(t, srcPending)
	require.Empty(t, srcReserved,
		"the SOURCE workspace's transaction booked nothing - a snapshot operator never records this metadata")
	recPending, recReserved := recTxn.UnsettledCleanupOwnershipForTest()
	require.Empty(t, recPending)
	require.Equal(t, []string{matName}, recReserved,
		"the RECORDING transaction holds the materialized object's reservation")
}
