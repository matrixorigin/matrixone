// Copyright 2026 Matrix Origin
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

package rpc

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	objectioio "github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	txnpb "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/stretchr/testify/require"
)

type lifecycleRPCRewriteFixture struct {
	control *api.LifecycleCommitEntry
	created objectio.ObjectStats
}

// newLifecycleRPCRewriteFixture constructs a valid TN consumer fixture: one
// Root-owned live Object and one immutable external transfer booking. The CN
// producer contract is exercised separately through DoMergeAndWrite. The first
// physical source block is expired and the second block is live, which makes
// both transfer and NoTransfer post-S paths precise.
func newLifecycleRPCRewriteFixture(
	t *testing.T,
	ctx context.Context,
	h *mockHandle,
	table lifecycleRPCTable,
	sourceSnapshot types.TS,
) lifecycleRPCRewriteFixture {
	t.Helper()
	require.Equal(t, uint32(2), table.source.BlkCnt())
	require.Equal(t, uint32(10), table.source.Rows())

	seqnums := make([]uint16, 0, len(table.schema.ColDefs)-1)
	columnIndexes := make([]int, 0, len(table.schema.ColDefs)-1)
	for index, column := range table.schema.ColDefs {
		if column.IsPhyAddr() {
			continue
		}
		seqnums = append(seqnums, column.SeqNum)
		columnIndexes = append(columnIndexes, index)
	}
	segmentID := objectio.NewSegmentid()
	writer := objectioio.ConstructWriterWithSegmentID(
		segmentID,
		0,
		table.schema.Version,
		seqnums,
		table.schema.GetSingleSortKeyIdx(),
		true,
		false,
		h.db.Runtime.Fs,
		nil,
	)

	txn, err := h.db.StartTxn(nil)
	require.NoError(t, err)
	database, err := txn.GetDatabaseByID(table.databaseID)
	require.NoError(t, err)
	relation, err := database.GetRelationByID(table.tableID)
	require.NoError(t, err)
	source, err := relation.GetObject(table.source.ObjectName().ObjectId(), false)
	require.NoError(t, err)

	rowCounts := make([]int32, table.source.BlkCnt())
	for blockOrdinal := uint32(0); blockOrdinal < table.source.BlkCnt(); blockOrdinal++ {
		var sourceBlock *containers.Batch
		require.NoError(t, source.HybridScan(
			ctx,
			&sourceBlock,
			uint16(blockOrdinal),
			columnIndexes,
			common.DefaultAllocator,
		))
		require.NotNil(t, sourceBlock)
		sourceBlock.Compact()
		require.Equal(t, 5, sourceBlock.Length())
		rowCounts[blockOrdinal] = int32(sourceBlock.Length())
		if blockOrdinal == 1 {
			_, err = writer.WriteBatch(containers.ToCNBatch(sourceBlock))
			require.NoError(t, err)
		}
		sourceBlock.Close()
	}
	require.NoError(t, source.Close())
	require.NoError(t, txn.Commit(ctx))
	_, _, err = writer.Sync(ctx)
	require.NoError(t, err)
	created := writer.GetObjectStats()
	require.Equal(t, uint32(5), created.Rows())
	require.Equal(t, uint32(1), created.BlkCnt())

	transferMaps := make(api.TransferMaps, len(rowCounts))
	for blockOrdinal, rowCount := range rowCounts {
		transferMaps[blockOrdinal] = make(api.TransferMap, rowCount)
		for row := range transferMaps[blockOrdinal] {
			transferMaps[blockOrdinal][row].ObjIdx = api.NoTransfer
		}
	}
	for row := range transferMaps[1] {
		transferMaps[1][row] = api.TransferDestPos{
			ObjIdx: 0,
			BlkIdx: 0,
			RowIdx: uint32(row),
		}
	}
	bookingLocation := writeLifecycleRPCBooking(
		t,
		ctx,
		h,
		table,
		transferMaps,
	)
	bookingHeader := make([]string, 0, len(rowCounts)+2)
	blockCount := int32(len(rowCounts))
	bookingHeader = append(
		bookingHeader,
		string(types.EncodeInt32(&blockCount)),
	)
	for index := range rowCounts {
		bookingHeader = append(
			bookingHeader,
			string(types.EncodeInt32(&rowCounts[index])),
		)
	}
	bookingHeader = append(bookingHeader, bookingLocation)

	createdStats := [][]byte{append([]byte(nil), created[:]...)}
	transferTable := mergesort.NewTransferTableFromMaps(transferMaps)
	transferDigest := mergesort.TransferMappingDigest(
		createdStats,
		transferTable,
	)
	transferTable.Release()
	sourceStats := [][]byte{append([]byte(nil), table.source[:]...)}
	control := &api.LifecycleCommitEntry{
		ProtocolVersion:              lifecycleCommitProtocolVersion,
		RetireMode:                   api.LifecycleCommitEntry_Rewrite,
		RootId:                       "root-rewrite",
		AttemptId:                    "attempt-rewrite",
		DatasetId:                    "dataset-rewrite",
		DatabaseId:                   table.databaseID,
		LogicalTableId:               table.tableID,
		PhysicalTableId:              table.tableID,
		BindingGeneration:            1,
		SchemaDigest:                 make([]byte, 32),
		SourceSnapshotTs:             pointerToTimestamp(sourceSnapshot),
		SourceSetDigest:              lifecycleSourceSetDigest(sourceStats),
		DataSourceObjectStats:        sourceStats,
		CreatedObjectStats:           createdStats,
		TransferBookingLocations:     bookingHeader,
		TransferMappingDigest:        append([]byte(nil), transferDigest[:]...),
		FinalPrepareDeadlineUnixNano: time.Now().Add(time.Minute).UnixNano(),
		MaxDeltaRows:                 uint64(table.source.Rows()),
		MaxDeltaBytes:                lifecycleRewriteMaxDeltaBytes,
		MaxDeltaBlocks:               uint32(table.source.BlkCnt()),
		MergeLevel:                   int32(table.source.GetLevel()),
	}
	return lifecycleRPCRewriteFixture{
		control: control,
		created: created,
	}
}

func writeLifecycleRPCBooking(
	t *testing.T,
	ctx context.Context,
	h *mockHandle,
	table lifecycleRPCTable,
	transferMaps api.TransferMaps,
) string {
	t.Helper()
	booking := containers.BuildBatch(
		[]string{"src_blk", "src_row", "dest_obj", "dest_blk", "dest_row"},
		[]types.Type{
			types.T_int32.ToType(),
			types.T_uint32.ToType(),
			types.T_uint8.ToType(),
			types.T_uint16.ToType(),
			types.T_uint32.ToType(),
		},
		containers.Options{Allocator: common.DefaultAllocator},
	)
	defer booking.Close()
	for blockOrdinal, transferMap := range transferMaps {
		for row, destination := range transferMap {
			if destination.ObjIdx == api.NoTransfer {
				continue
			}
			booking.Vecs[0].Append(int32(blockOrdinal), false)
			booking.Vecs[1].Append(uint32(row), false)
			booking.Vecs[2].Append(destination.ObjIdx, false)
			booking.Vecs[3].Append(destination.BlkIdx, false)
			booking.Vecs[4].Append(destination.RowIdx, false)
		}
	}
	location := objectioio.EncodeTmpFileName(
		"tmp",
		fmt.Sprintf("lifecycle-rewrite-%d", table.tableID),
		time.Now().UnixNano(),
	)
	writer, err := objectio.NewObjectWriterSpecial(
		objectio.WriterTmp,
		location,
		h.db.Runtime.Fs,
	)
	require.NoError(t, err)
	_, err = writer.Write(containers.ToCNBatch(booking))
	require.NoError(t, err)
	_, err = writer.WriteEnd(ctx)
	require.NoError(t, err)
	return location
}

func lifecycleRewriteCommitRequest(
	t *testing.T,
	control *api.LifecycleCommitEntry,
	jobID string,
) *txnpb.TxnCommitRequest {
	t.Helper()
	command := &api.PrecommitWriteCmd{
		EntryList: []*api.Entry{{
			EntryType:       api.Entry_LifecycleCommit,
			LifecycleCommit: control,
		}},
		SyncProtectionJobId: jobID,
	}
	payload, err := command.MarshalBinary()
	require.NoError(t, err)
	return &txnpb.TxnCommitRequest{Payload: []*txnpb.TxnRequest{{
		CNRequest: &txnpb.CNOpRequest{
			OpCode:  uint32(api.OpCode_OpPreCommit),
			Payload: payload,
		},
	}}}
}

func deleteLifecycleRPCSourceRow(
	t *testing.T,
	ctx context.Context,
	h *mockHandle,
	table lifecycleRPCTable,
	blockOrdinal uint16,
) int16 {
	t.Helper()
	txn, err := h.db.StartTxn(nil)
	require.NoError(t, err)
	database, err := txn.GetDatabaseByID(table.databaseID)
	require.NoError(t, err)
	relation, err := database.GetRelationByID(table.tableID)
	require.NoError(t, err)
	object, err := relation.GetObject(table.source.ObjectName().ObjectId(), false)
	require.NoError(t, err)
	defer object.Close()
	var value *containers.Batch
	require.NoError(t, object.HybridScan(
		ctx,
		&value,
		blockOrdinal,
		[]int{
			table.schema.GetColIdx(catalog.PhyAddrColumnName),
			table.schema.GetPrimaryKey().Idx,
		},
		common.DefaultAllocator,
	))
	defer value.Close()
	value.Compact()
	require.NotZero(t, value.Length())
	rowIDs := value.Vecs[0].CloneWindow(0, 1)
	primaryKeys := value.Vecs[1].CloneWindow(0, 1)
	defer rowIDs.Close()
	defer primaryKeys.Close()
	deletedPrimaryKey := primaryKeys.Get(0).(int16)
	require.NoError(t, relation.DeleteByPhyAddrKeys(
		rowIDs,
		primaryKeys,
		handle.DT_Normal,
	))
	require.NoError(t, txn.Commit(ctx))
	return deletedPrimaryKey
}

func requireLifecycleRPCRows(
	t *testing.T,
	ctx context.Context,
	h *mockHandle,
	table lifecycleRPCTable,
	expected int,
) {
	t.Helper()
	txn, err := h.db.StartTxn(nil)
	require.NoError(t, err)
	database, err := txn.GetDatabaseByID(table.databaseID)
	require.NoError(t, err)
	relation, err := database.GetRelationByID(table.tableID)
	require.NoError(t, err)
	testutil.CheckAllColRowsByScan(t, relation, expected, true)
	require.NoError(t, txn.Commit(ctx))
}

func lifecycleRPCPrimaryKeys(
	t *testing.T,
	ctx context.Context,
	h *mockHandle,
	table lifecycleRPCTable,
) []int16 {
	t.Helper()
	txn, err := h.db.StartTxn(nil)
	require.NoError(t, err)
	database, err := txn.GetDatabaseByID(table.databaseID)
	require.NoError(t, err)
	relation, err := database.GetRelationByID(table.tableID)
	require.NoError(t, err)
	keys := make([]int16, 0, table.source.Rows())
	testutil.ForEachColumnView(
		t,
		relation,
		table.schema.GetPrimaryKey().Idx,
		func(view *containers.Batch) error {
			view.Compact()
			for row := 0; row < view.Length(); row++ {
				keys = append(keys, view.Vecs[0].Get(row).(int16))
			}
			return nil
		},
	)
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	require.NoError(t, txn.Commit(ctx))
	return keys
}

func lifecycleRPCObjectStatsVisible(
	t *testing.T,
	ctx context.Context,
	h *mockHandle,
	table lifecycleRPCTable,
	stats objectio.ObjectStats,
) bool {
	t.Helper()
	txn, err := h.db.StartTxn(nil)
	require.NoError(t, err)
	database, err := txn.GetDatabaseByID(table.databaseID)
	require.NoError(t, err)
	relation, err := database.GetRelationByID(table.tableID)
	require.NoError(t, err)
	iterator := relation.MakeObjectIt(false)
	visible := false
	for iterator.Next() {
		if *iterator.GetObject().GetID() == *stats.ObjectName().ObjectId() {
			visible = true
		}
	}
	require.NoError(t, iterator.Close())
	require.NoError(t, txn.Commit(ctx))
	return visible
}

func requireLifecycleRPCRootFiles(
	t *testing.T,
	ctx context.Context,
	h *mockHandle,
	rewrite lifecycleRPCRewriteFixture,
) {
	t.Helper()
	_, err := h.db.Runtime.Fs.StatFile(
		ctx,
		rewrite.created.ObjectName().String(),
	)
	require.NoError(t, err, "TAE rollback must not delete Root-owned live staging")
	bookingPath := rewrite.control.TransferBookingLocations[len(rewrite.control.TransferBookingLocations)-1]
	_, err = h.db.Runtime.Fs.StatFile(ctx, bookingPath)
	require.NoError(t, err, "TAE rollback must not delete Root-owned booking")
}

func installLifecycleRPCFault(
	t *testing.T,
	ctx context.Context,
	point string,
	message string,
) {
	t.Helper()
	require.True(t, fault.Enable())
	require.NoError(t, fault.AddFaultPoint(
		ctx,
		point,
		":::",
		"echo",
		0,
		message,
		false,
	))
	t.Cleanup(func() {
		removed, err := fault.RemoveFaultPoint(ctx, point)
		require.NoError(t, err)
		require.True(t, removed)
		require.True(t, fault.Disable())
	})
}

func TestLifecycleRewriteTransfersLiveDeleteAndSurvivesReplay(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()
	opts := config.WithLongScanAndCKPOpts(nil)
	h := mockTAEHandle(ctx, t, opts)
	t.Cleanup(func() { opts.Fs.Close(ctx) })
	t.Cleanup(func() {
		if h.Handle != nil {
			_ = h.HandleClose(ctx)
		}
	})

	table := newLifecycleRPCTable(t, ctx, h)
	sourceSnapshot := h.db.TxnMgr.Now()
	jobID := "attempt-rewrite-live-delete"
	registerLifecycleRPCProtection(t, h, jobID, table.source)
	rewrite := newLifecycleRPCRewriteFixture(
		t,
		ctx,
		h,
		table,
		sourceSnapshot,
	)

	// The second source block is L.  This DELETE commits after S and before
	// finalization, so Lifecycle must transfer it to the new Object.
	deletedPrimaryKey := deleteLifecycleRPCSourceRow(t, ctx, h, table, 1)
	require.Equal(t, int16(5), deletedPrimaryKey)
	request := lifecycleRewriteCommitRequest(t, rewrite.control, jobID)
	_, err := h.HandleCommit(ctx, mock1PCTxn(h.db), nil, request)
	require.NoError(t, err)
	require.False(t, lifecycleRPCObjectVisible(t, ctx, h.db, table))
	require.True(t, lifecycleRPCObjectStatsVisible(
		t,
		ctx,
		h,
		table,
		rewrite.created,
	))
	requireLifecycleRPCRows(t, ctx, h, table, 4)
	require.Equal(t, []int16{6, 7, 8, 9}, lifecycleRPCPrimaryKeys(t, ctx, h, table))

	directory := h.db.Dir
	require.NoError(t, h.HandleClose(ctx))
	h.Handle = nil
	reopened, err := db.Open(ctx, directory, opts)
	require.NoError(t, err)
	h.Handle = &Handle{db: reopened}
	require.False(t, lifecycleRPCObjectVisible(t, ctx, h.db, table))
	require.True(t, lifecycleRPCObjectStatsVisible(
		t,
		ctx,
		h,
		table,
		rewrite.created,
	))
	requireLifecycleRPCRows(t, ctx, h, table, 4)
	require.Equal(t, []int16{6, 7, 8, 9}, lifecycleRPCPrimaryKeys(t, ctx, h, table))

	// Lifecycle only records the source retirement.  The old source file is
	// still present after commit/replay and must be reclaimed by the ordinary
	// checkpoint/TAE GC path, while the created live Object remains intact.
	_, err = h.db.Runtime.Fs.StatFile(ctx, table.source.ObjectName().String())
	require.NoError(t, err)
	_, err = h.db.Runtime.Fs.StatFile(ctx, rewrite.created.ObjectName().String())
	require.NoError(t, err)
	require.False(t, h.db.DiskCleaner.GetCleaner().GetSyncProtectionManager().IsProtected(
		table.source.ObjectName().String(),
	))

	checkpointCtx, cancel := context.WithTimeout(ctx, testutil.TestCheckpointTimeout)
	defer cancel()
	target := h.db.TxnMgr.Now()
	require.NoError(t, h.db.ForceCheckpoint(checkpointCtx, target))
	incremental := h.db.BGCheckpointRunner.MaxIncrementalCheckpoint()
	require.NotNil(t, incremental)
	incrementalEnd := incremental.GetEnd()
	require.True(t, incrementalEnd.GE(&target))
	checkpointReader := logtail.NewCKPReader(
		incremental.GetVersion(),
		incremental.GetLocation(),
		common.DefaultAllocator,
		h.db.Runtime.Fs,
	)
	require.NoError(t, checkpointReader.ReadMeta(checkpointCtx))
	sourceCheckpointed := false
	sourceDeleteTS := types.TS{}
	require.NoError(t, checkpointReader.ForEachRow(
		checkpointCtx,
		func(
			_ uint32,
			_, tableID uint64,
			_ int8,
			stats objectio.ObjectStats,
			_, deleteTS types.TS,
			_ types.Rowid,
		) error {
			if tableID == table.tableID &&
				stats.ObjectName().String() == table.source.ObjectName().String() {
				sourceCheckpointed = true
				sourceDeleteTS = deleteTS
			}
			return nil
		},
	))
	require.True(t, sourceCheckpointed)
	require.False(t, sourceDeleteTS.IsEmpty())
	require.True(t, sourceDeleteTS.LT(&incrementalEnd))
	allowAll := func(*checkpoint.CheckpointEntry) bool { return true }
	require.NoError(t, h.db.DiskCleaner.GetCleaner().Process(checkpointCtx, allowAll))
	require.NotNil(t, h.db.DiskCleaner.GetCleaner().GetScanWaterMark())
	_, err = h.db.Runtime.Fs.StatFile(ctx, table.source.ObjectName().String())
	require.NoError(t, err)

	// Production's ordinary Catalog-GC cron first removes the retired Catalog
	// node after its retention watermark.  Advance that same public step
	// synchronously here (without a sleep); the following checkpoint then makes
	// the source absent from the global live-object set used by physical GC.
	h.db.Catalog.GCByTS(checkpointCtx, incrementalEnd)
	gcTarget := h.db.TxnMgr.Now()
	require.NoError(t, h.db.ForceCheckpoint(checkpointCtx, gcTarget))
	gcIncremental := h.db.BGCheckpointRunner.MaxIncrementalCheckpoint()
	require.NotNil(t, gcIncremental)
	gcIncrementalEnd := gcIncremental.GetEnd()
	require.True(t, gcIncrementalEnd.GE(&gcTarget))
	require.NoError(t, h.db.DiskCleaner.GetCleaner().Process(checkpointCtx, allowAll))
	require.NoError(t, h.db.ForceGlobalCheckpoint(
		checkpointCtx,
		gcIncrementalEnd,
		0,
	))
	require.NoError(t, h.db.DiskCleaner.GetCleaner().Process(checkpointCtx, allowAll))
	_, err = h.db.Runtime.Fs.StatFile(ctx, table.source.ObjectName().String())
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound), err)
	_, err = h.db.Runtime.Fs.StatFile(ctx, rewrite.created.ObjectName().String())
	require.NoError(t, err)
	require.False(t, lifecycleRPCObjectVisible(t, ctx, h.db, table))
	require.True(t, lifecycleRPCObjectStatsVisible(t, ctx, h, table, rewrite.created))
	requireLifecycleRPCRows(t, ctx, h, table, 4)
	require.Equal(t, []int16{6, 7, 8, 9}, lifecycleRPCPrimaryKeys(t, ctx, h, table))
}

func TestLifecycleRewriteNoTransferDeleteAborts(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()
	opts := config.WithLongScanAndCKPOpts(nil)
	h := mockTAEHandle(ctx, t, opts)
	t.Cleanup(func() { opts.Fs.Close(ctx) })
	t.Cleanup(func() { _ = h.HandleClose(ctx) })

	table := newLifecycleRPCTable(t, ctx, h)
	sourceSnapshot := h.db.TxnMgr.Now()
	jobID := "attempt-rewrite-no-transfer-delete"
	registerLifecycleRPCProtection(t, h, jobID, table.source)
	rewrite := newLifecycleRPCRewriteFixture(
		t,
		ctx,
		h,
		table,
		sourceSnapshot,
	)

	// Register the entry first so the following DELETE is collected in phase 2.
	// Block zero is E/NoTransfer; the conservative Archive contract must abort.
	finalTxn, err := h.db.StartTxn(nil)
	require.NoError(t, err)
	finalTxn.SetSyncProtectionJobID(jobID)
	require.NoError(t, h.HandleLifecycleCommit(ctx, finalTxn, rewrite.control))
	deleteLifecycleRPCSourceRow(t, ctx, h, table, 0)
	err = finalTxn.Commit(ctx)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict), err)

	require.True(t, lifecycleRPCObjectVisible(t, ctx, h.db, table))
	require.False(t, lifecycleRPCObjectStatsVisible(
		t,
		ctx,
		h,
		table,
		rewrite.created,
	))
	requireLifecycleRPCRootFiles(t, ctx, h, rewrite)
	requireLifecycleRPCRows(t, ctx, h, table, 9)
}

func TestLifecycleRewritePostSnapshotDeleteBudgetAborts(t *testing.T) {
	for _, test := range []struct {
		name          string
		configure     func(*api.LifecycleCommitEntry)
		beforeBlocks  []uint16
		afterBlocks   []uint16
		errorContains string
	}{
		{
			name: "rows accumulate across phase one and phase two",
			configure: func(control *api.LifecycleCommitEntry) {
				control.MaxDeltaRows = 1
			},
			beforeBlocks:  []uint16{1},
			afterBlocks:   []uint16{1},
			errorContains: "post-snapshot Tombstone budget exceeded",
		},
		{
			name: "bytes",
			configure: func(control *api.LifecycleCommitEntry) {
				control.MaxDeltaBytes = 1
			},
			afterBlocks:   []uint16{1},
			errorContains: "post-snapshot Tombstone budget exceeded",
		},
		{
			name: "distinct blocks accumulate across phases",
			configure: func(control *api.LifecycleCommitEntry) {
				control.MaxDeltaBlocks = 1
			},
			beforeBlocks:  []uint16{1},
			afterBlocks:   []uint16{0},
			errorContains: "post-snapshot block budget exceeded",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			defer testutils.AfterTest(t)()
			ctx := context.Background()
			opts := config.WithLongScanAndCKPOpts(nil)
			h := mockTAEHandle(ctx, t, opts)
			t.Cleanup(func() { opts.Fs.Close(ctx) })
			t.Cleanup(func() { _ = h.HandleClose(ctx) })

			table := newLifecycleRPCTable(t, ctx, h)
			sourceSnapshot := h.db.TxnMgr.Now()
			jobID := "attempt-rewrite-delta-budget-" + test.name
			registerLifecycleRPCProtection(t, h, jobID, table.source)
			rewrite := newLifecycleRPCRewriteFixture(
				t,
				ctx,
				h,
				table,
				sourceSnapshot,
			)
			test.configure(rewrite.control)
			for _, block := range test.beforeBlocks {
				deleteLifecycleRPCSourceRow(t, ctx, h, table, block)
			}

			finalTxn, err := h.db.StartTxn(nil)
			require.NoError(t, err)
			finalTxn.SetSyncProtectionJobID(jobID)
			require.NoError(t, h.HandleLifecycleCommit(
				ctx,
				finalTxn,
				rewrite.control,
			))
			for _, block := range test.afterBlocks {
				deleteLifecycleRPCSourceRow(t, ctx, h, table, block)
			}
			err = finalTxn.Commit(ctx)
			require.ErrorContains(t, err, test.errorContains)

			require.True(t, lifecycleRPCObjectVisible(t, ctx, h.db, table))
			require.False(t, lifecycleRPCObjectStatsVisible(
				t,
				ctx,
				h,
				table,
				rewrite.created,
			))
			createdBlock := objectio.NewBlockidWithObjectID(
				rewrite.created.ObjectName().ObjectId(),
				0,
			)
			require.Nil(t, h.db.Runtime.TransferDelsMap.GetDelsForBlk(createdBlock))
			requireLifecycleRPCRootFiles(t, ctx, h, rewrite)
			requireLifecycleRPCRows(
				t,
				ctx,
				h,
				table,
				10-len(test.beforeBlocks)-len(test.afterBlocks),
			)
		})
	}
}

func TestLifecycleRewritePreRegistrationTransferFaultCleansRuntimeOnly(
	t *testing.T,
) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()
	opts := config.WithLongScanAndCKPOpts(nil)
	h := mockTAEHandle(ctx, t, opts)
	t.Cleanup(func() { opts.Fs.Close(ctx) })
	t.Cleanup(func() { _ = h.HandleClose(ctx) })

	table := newLifecycleRPCTable(t, ctx, h)
	sourceSnapshot := h.db.TxnMgr.Now()
	jobID := "attempt-rewrite-pre-registration-fault"
	registerLifecycleRPCProtection(t, h, jobID, table.source)
	rewrite := newLifecycleRPCRewriteFixture(t, ctx, h, table, sourceSnapshot)
	deleteLifecycleRPCSourceRow(t, ctx, h, table, 1)
	installLifecycleRPCFault(
		t,
		ctx,
		objectio.FJ_TransferErrorAfterTransfer,
		"injected Lifecycle phase-1 transfer failure",
	)

	finalTxn, err := h.db.StartTxn(nil)
	require.NoError(t, err)
	finalTxn.SetSyncProtectionJobID(jobID)
	require.ErrorContains(
		t,
		h.HandleLifecycleCommit(ctx, finalTxn, rewrite.control),
		"injected Lifecycle phase-1 transfer failure",
	)
	require.NoError(t, finalTxn.Rollback(ctx))

	createdBlock := objectio.NewBlockidWithObjectID(
		rewrite.created.ObjectName().ObjectId(),
		0,
	)
	require.Nil(t, h.db.Runtime.TransferDelsMap.GetDelsForBlk(createdBlock))
	require.True(t, lifecycleRPCObjectVisible(t, ctx, h.db, table))
	require.False(t, lifecycleRPCObjectStatsVisible(t, ctx, h, table, rewrite.created))
	requireLifecycleRPCRootFiles(t, ctx, h, rewrite)
	requireLifecycleRPCRows(t, ctx, h, table, 9)
}

func TestLifecycleRewriteRegisteredTransferFaultRollsBackCatalogOnly(
	t *testing.T,
) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()
	opts := config.WithLongScanAndCKPOpts(nil)
	h := mockTAEHandle(ctx, t, opts)
	t.Cleanup(func() { opts.Fs.Close(ctx) })
	t.Cleanup(func() { _ = h.HandleClose(ctx) })

	table := newLifecycleRPCTable(t, ctx, h)
	sourceSnapshot := h.db.TxnMgr.Now()
	jobID := "attempt-rewrite-registered-fault"
	registerLifecycleRPCProtection(t, h, jobID, table.source)
	rewrite := newLifecycleRPCRewriteFixture(t, ctx, h, table, sourceSnapshot)
	finalTxn, err := h.db.StartTxn(nil)
	require.NoError(t, err)
	finalTxn.SetSyncProtectionJobID(jobID)
	require.NoError(t, h.HandleLifecycleCommit(ctx, finalTxn, rewrite.control))
	deleteLifecycleRPCSourceRow(t, ctx, h, table, 1)
	installLifecycleRPCFault(
		t,
		ctx,
		objectio.FJ_TransferErrorAfterTransfer,
		"injected Lifecycle phase-2 transfer failure",
	)

	require.ErrorContains(
		t,
		finalTxn.Commit(ctx),
		"injected Lifecycle phase-2 transfer failure",
	)
	createdBlock := objectio.NewBlockidWithObjectID(
		rewrite.created.ObjectName().ObjectId(),
		0,
	)
	require.Nil(t, h.db.Runtime.TransferDelsMap.GetDelsForBlk(createdBlock))
	require.True(t, lifecycleRPCObjectVisible(t, ctx, h.db, table))
	require.False(t, lifecycleRPCObjectStatsVisible(t, ctx, h, table, rewrite.created))
	requireLifecycleRPCRootFiles(t, ctx, h, rewrite)
	requireLifecycleRPCRows(t, ctx, h, table, 9)
}

func TestLifecycleRewriteNeedRetryAbortsWithoutReplayingBooking(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()
	opts := config.WithLongScanAndCKPOpts(nil)
	h := mockTAEHandle(ctx, t, opts)
	t.Cleanup(func() { opts.Fs.Close(ctx) })
	t.Cleanup(func() {
		if h.Handle != nil {
			_ = h.HandleClose(ctx)
		}
	})

	table := newLifecycleRPCTable(t, ctx, h)
	sourceSnapshot := h.db.TxnMgr.Now()
	jobID := "attempt-rewrite-need-retry"
	registerLifecycleRPCProtection(t, h, jobID, table.source)
	rewrite := newLifecycleRPCRewriteFixture(t, ctx, h, table, sourceSnapshot)
	deletedPrimaryKey := deleteLifecycleRPCSourceRow(t, ctx, h, table, 1)
	require.Equal(t, int16(5), deletedPrimaryKey)

	request := lifecycleRewriteCommitRequest(t, rewrite.control, jobID)
	originalPayload := bytes.Clone(request.Payload[0].CNRequest.Payload)
	meta := mock1PCTxn(h.db)
	generationOne, err := h.db.GetOrCreateTxnWithMeta(
		nil,
		meta.GetID(),
		types.TimestampToTS(meta.GetSnapshotTS()),
	)
	require.NoError(t, err)
	prepareCount := 0
	generationOne.SetPrepareCommitFn(func(txn txnif.AsyncTxn) error {
		prepareCount++
		if err := txn.GetStore().PrepareCommit(); err != nil {
			return err
		}
		return txnif.ErrTxnNeedRetry
	})

	_, err = h.HandleCommit(ctx, meta, nil, request)
	require.ErrorIs(t, err, txnif.ErrTxnNeedRetry)
	require.Equal(t, 1, prepareCount)
	require.Equal(t, txnif.TxnStateRollbacked, generationOne.GetTxnState(true))
	createdBlock := objectio.NewBlockidWithObjectID(rewrite.created.ObjectName().ObjectId(), 0)
	require.Nil(t, h.db.Runtime.TransferDelsMap.GetDelsForBlk(createdBlock))
	require.True(t, bytes.Equal(originalPayload, request.Payload[0].CNRequest.Payload))
	require.True(t, lifecycleRPCObjectVisible(t, ctx, h.db, table))
	require.False(t, lifecycleRPCObjectStatsVisible(t, ctx, h, table, rewrite.created))
	requireLifecycleRPCRootFiles(t, ctx, h, rewrite)
	requireLifecycleRPCRows(t, ctx, h, table, 9)

	directory := h.db.Dir
	require.NoError(t, h.HandleClose(ctx))
	h.Handle = nil
	reopened, err := db.Open(ctx, directory, opts)
	require.NoError(t, err)
	h.Handle = &Handle{db: reopened}
	require.True(t, lifecycleRPCObjectVisible(t, ctx, h.db, table))
	require.False(t, lifecycleRPCObjectStatsVisible(t, ctx, h, table, rewrite.created))
	requireLifecycleRPCRows(t, ctx, h, table, 9)
}

func TestLifecycleRewriteRestartLosesProtectionAndFailsClosed(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()
	opts := config.WithLongScanAndCKPOpts(nil)
	h := mockTAEHandle(ctx, t, opts)
	t.Cleanup(func() { opts.Fs.Close(ctx) })
	t.Cleanup(func() {
		if h.Handle != nil {
			_ = h.HandleClose(ctx)
		}
	})

	table := newLifecycleRPCTable(t, ctx, h)
	sourceSnapshot := h.db.TxnMgr.Now()
	jobID := "attempt-rewrite-protection-restart"
	registerLifecycleRPCProtection(t, h, jobID, table.source)
	rewrite := newLifecycleRPCRewriteFixture(t, ctx, h, table, sourceSnapshot)

	directory := h.db.Dir
	require.NoError(t, h.HandleClose(ctx))
	h.Handle = nil
	reopened, err := db.Open(ctx, directory, opts)
	require.NoError(t, err)
	h.Handle = &Handle{db: reopened}

	request := lifecycleRewriteCommitRequest(t, rewrite.control, jobID)
	_, err = h.HandleCommit(ctx, mock1PCTxn(h.db), nil, request)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrSyncProtectionNotFound), err)
	require.True(t, lifecycleRPCObjectVisible(t, ctx, h.db, table))
	require.False(t, lifecycleRPCObjectStatsVisible(t, ctx, h, table, rewrite.created))
	requireLifecycleRPCRootFiles(t, ctx, h, rewrite)
	requireLifecycleRPCRows(t, ctx, h, table, 10)
}
