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
	"context"
	"encoding/base64"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	txnpb "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/jobs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/stretchr/testify/require"
)

type lifecycleRPCTable struct {
	databaseID uint64
	tableID    uint64
	schema     *catalog.Schema
	source     objectio.ObjectStats
	sources    []objectio.ObjectStats
}

type lifecycleRPCMarkerTable struct {
	databaseID uint64
	tableID    uint64
	name       string
	schema     *catalog.Schema
}

func newLifecycleRPCMultiSourceTable(
	t *testing.T,
	ctx context.Context,
	h *mockHandle,
) lifecycleRPCTable {
	t.Helper()
	table := newLifecycleRPCTable(t, ctx, h)
	value := containers.MockBatchWithAttrsAndOffset(
		table.schema.Types(),
		table.schema.Attrs(),
		10,
		100,
	)
	t.Cleanup(value.Close)

	txn, err := h.db.StartTxn(nil)
	require.NoError(t, err)
	database, err := txn.GetDatabaseByID(table.databaseID)
	require.NoError(t, err)
	relation, err := database.GetRelationByID(table.tableID)
	require.NoError(t, err)
	require.NoError(t, relation.Append(ctx, value))
	require.NoError(t, txn.Commit(ctx))

	txn, err = h.db.StartTxn(nil)
	require.NoError(t, err)
	database, err = txn.GetDatabaseByID(table.databaseID)
	require.NoError(t, err)
	relation, err = database.GetRelationByID(table.tableID)
	require.NoError(t, err)
	var appendable []*catalog.ObjectEntry
	iterator := relation.MakeObjectIt(false)
	for iterator.Next() {
		entry := iterator.GetObject().GetMeta().(*catalog.ObjectEntry)
		if entry.IsAppendable() {
			appendable = append(appendable, entry)
		}
	}
	require.NoError(t, iterator.Close())
	require.NotEmpty(t, appendable)
	require.NoError(t, txn.Commit(ctx))

	flushTxn, err := h.db.StartTxn(nil)
	require.NoError(t, err)
	flushTask, err := jobs.NewFlushTableTailTask(
		nil,
		flushTxn,
		appendable,
		nil,
		h.db.Runtime,
	)
	require.NoError(t, err)
	require.NoError(t, flushTask.OnExec(ctx))
	require.NoError(t, flushTxn.Commit(ctx))

	txn, err = h.db.StartTxn(nil)
	require.NoError(t, err)
	database, err = txn.GetDatabaseByID(table.databaseID)
	require.NoError(t, err)
	relation, err = database.GetRelationByID(table.tableID)
	require.NoError(t, err)
	iterator = relation.MakeObjectIt(false)
	table.sources = table.sources[:0]
	for iterator.Next() {
		entry := iterator.GetObject().GetMeta().(*catalog.ObjectEntry)
		if !entry.IsAppendable() {
			table.sources = append(table.sources, *entry.GetObjectStats())
		}
	}
	require.NoError(t, iterator.Close())
	require.NoError(t, txn.Commit(ctx))
	require.Len(t, table.sources, 2)
	table.source = table.sources[0]
	return table
}

func newLifecycleRPCTable(
	t *testing.T,
	ctx context.Context,
	h *mockHandle,
) lifecycleRPCTable {
	t.Helper()
	schema := catalog.MockSchemaAll(2, 1)
	schema.Name = "lifecycle_rpc"
	schema.Extra.BlockMaxRows = 5
	schema.Extra.ObjectMaxBlocks = 2
	value := catalog.MockBatch(schema, 10)
	t.Cleanup(value.Close)

	txn, err := h.db.StartTxn(nil)
	require.NoError(t, err)
	database, err := txn.CreateDatabase("lifecycle_db", "create database", "")
	require.NoError(t, err)
	_, err = database.CreateRelation(schema)
	require.NoError(t, err)
	require.NoError(t, txn.Commit(ctx))

	txn, err = h.db.StartTxn(nil)
	require.NoError(t, err)
	database, err = txn.GetDatabase("lifecycle_db")
	require.NoError(t, err)
	relation, err := database.GetRelationByName(schema.Name)
	require.NoError(t, err)
	require.NoError(t, relation.Append(ctx, value))
	databaseID := database.GetID()
	tableID := relation.GetMeta().(*catalog.TableEntry).GetID()
	require.NoError(t, txn.Commit(ctx))

	// Flush the appendable tail through the existing TAE path so Lifecycle
	// receives the same immutable ObjectStats used in production.
	txn, err = h.db.StartTxn(nil)
	require.NoError(t, err)
	database, err = txn.GetDatabase("lifecycle_db")
	require.NoError(t, err)
	relation, err = database.GetRelationByName(schema.Name)
	require.NoError(t, err)
	var appendable []*catalog.ObjectEntry
	iterator := relation.MakeObjectIt(false)
	for iterator.Next() {
		entry := iterator.GetObject().GetMeta().(*catalog.ObjectEntry)
		if entry.IsAppendable() {
			appendable = append(appendable, entry)
		}
	}
	require.NoError(t, iterator.Close())
	require.NotEmpty(t, appendable)
	require.NoError(t, txn.Commit(ctx))

	flushTxn, err := h.db.StartTxn(nil)
	require.NoError(t, err)
	flushTask, err := jobs.NewFlushTableTailTask(
		nil,
		flushTxn,
		appendable,
		nil,
		h.db.Runtime,
	)
	require.NoError(t, err)
	require.NoError(t, flushTask.OnExec(ctx))
	require.NoError(t, flushTxn.Commit(ctx))

	txn, err = h.db.StartTxn(nil)
	require.NoError(t, err)
	database, err = txn.GetDatabase("lifecycle_db")
	require.NoError(t, err)
	relation, err = database.GetRelationByName(schema.Name)
	require.NoError(t, err)
	iterator = relation.MakeObjectIt(false)
	var sources []objectio.ObjectStats
	for iterator.Next() {
		entry := iterator.GetObject().GetMeta().(*catalog.ObjectEntry)
		if !entry.IsAppendable() {
			sources = append(sources, *entry.GetObjectStats())
		}
	}
	require.NoError(t, iterator.Close())
	require.NoError(t, txn.Commit(ctx))
	require.Len(t, sources, 1)
	require.False(t, sources[0].GetAppendable())

	return lifecycleRPCTable{
		databaseID: databaseID,
		tableID:    tableID,
		schema:     schema,
		source:     sources[0],
		sources:    sources,
	}
}

func newLifecycleRPCMarkerTable(
	t *testing.T,
	ctx context.Context,
	h *mockHandle,
	databaseID uint64,
) lifecycleRPCMarkerTable {
	t.Helper()
	schema := catalog.MockSchemaAll(2, 1)
	schema.Name = "lifecycle_rpc_marker"

	txn, err := h.db.StartTxn(nil)
	require.NoError(t, err)
	database, err := txn.GetDatabaseByID(databaseID)
	require.NoError(t, err)
	relation, err := database.CreateRelation(schema)
	require.NoError(t, err)
	tableID := relation.GetMeta().(*catalog.TableEntry).GetID()
	require.NoError(t, txn.Commit(ctx))

	return lifecycleRPCMarkerTable{
		databaseID: databaseID,
		tableID:    tableID,
		name:       schema.Name,
		schema:     schema,
	}
}

func lifecycleRPCMarkerInsertEntry(
	t *testing.T,
	marker lifecycleRPCMarkerTable,
) *api.Entry {
	t.Helper()
	value := catalog.MockBatch(marker.schema, 1)
	t.Cleanup(value.Close)
	entry, err := makePBEntry(
		INSERT,
		marker.databaseID,
		marker.tableID,
		"lifecycle_db",
		marker.name,
		"",
		containers.ToCNBatch(value),
	)
	require.NoError(t, err)
	return entry
}

func prependLifecycleRPCEntry(
	t *testing.T,
	request *txnpb.TxnCommitRequest,
	entry *api.Entry,
) *api.LifecycleCommitEntry {
	t.Helper()
	require.Len(t, request.Payload, 1)
	command := new(api.PrecommitWriteCmd)
	require.NoError(t, command.UnmarshalBinary(request.Payload[0].CNRequest.Payload))
	require.Len(t, command.EntryList, 1)
	require.Equal(t, api.Entry_LifecycleCommit, command.EntryList[0].EntryType)
	control := command.EntryList[0].LifecycleCommit
	require.NotNil(t, control)
	command.EntryList = append([]*api.Entry{entry}, command.EntryList...)
	payload, err := command.MarshalBinary()
	require.NoError(t, err)
	request.Payload[0].CNRequest.Payload = payload
	return control
}

func lifecycleRPCMarkerRows(
	t *testing.T,
	ctx context.Context,
	database *db.DB,
	marker lifecycleRPCMarkerTable,
) uint64 {
	t.Helper()
	txn, err := database.StartTxn(nil)
	require.NoError(t, err)
	databaseHandle, err := txn.GetDatabaseByID(marker.databaseID)
	require.NoError(t, err)
	relation, err := databaseHandle.GetRelationByID(marker.tableID)
	require.NoError(t, err)
	rows := relation.GetMeta().(*catalog.TableEntry).GetRows()
	require.NoError(t, txn.Commit(ctx))
	return rows
}

func registerLifecycleRPCProtection(
	t *testing.T,
	h *mockHandle,
	jobID string,
	source objectio.ObjectStats,
) {
	registerLifecycleRPCProtectionSet(t, h, jobID, []objectio.ObjectStats{source})
}

func registerLifecycleRPCProtectionSet(
	t *testing.T,
	h *mockHandle,
	jobID string,
	sources []objectio.ObjectStats,
) {
	t.Helper()
	values := containers.MakeVector(types.T_varchar.ToType(), common.DefaultAllocator)
	defer values.Close()
	for index := range sources {
		values.Append([]byte(sources[index].ObjectName().String()), false)
	}
	filter, err := index.NewBloomFilter(values, nil, nil, nil)
	require.NoError(t, err)
	encoded, err := filter.Marshal()
	require.NoError(t, err)
	require.NoError(t,
		h.db.DiskCleaner.GetCleaner().GetSyncProtectionManager().
			RegisterSyncProtection(
				jobID,
				base64.StdEncoding.EncodeToString(encoded),
				time.Now().Add(time.Hour).UnixNano(),
				"lifecycle-rpc-test",
			),
	)
}

func lifecycleWholeCommitRequest(
	t *testing.T,
	table lifecycleRPCTable,
	sourceSnapshot types.TS,
	jobID string,
) *txnpb.TxnCommitRequest {
	t.Helper()
	sources := make([][]byte, len(table.sources))
	for index := range table.sources {
		sources[index] = append([]byte(nil), table.sources[index][:]...)
	}
	control := &api.LifecycleCommitEntry{
		ProtocolVersion:              lifecycleCommitProtocolVersion,
		RetireMode:                   api.LifecycleCommitEntry_Whole,
		RootId:                       "root-whole",
		AttemptId:                    "attempt-whole",
		DatasetId:                    "dataset-whole",
		DatabaseId:                   table.databaseID,
		LogicalTableId:               table.tableID,
		PhysicalTableId:              table.tableID,
		BindingGeneration:            1,
		SchemaDigest:                 make([]byte, 32),
		SourceSnapshotTs:             pointerToTimestamp(sourceSnapshot),
		DataSourceObjectStats:        sources,
		FinalPrepareDeadlineUnixNano: time.Now().Add(time.Minute).UnixNano(),
	}
	control.SourceSetDigest = lifecycleSourceSetDigest(
		control.DataSourceObjectStats,
	)
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

func pointerToTimestamp(value types.TS) *timestamp.Timestamp {
	converted := value.ToTimestamp()
	return &converted
}

func lifecycleRPCObjectVisible(
	t *testing.T,
	ctx context.Context,
	database *db.DB,
	table lifecycleRPCTable,
) bool {
	t.Helper()
	txn, err := database.StartTxn(nil)
	require.NoError(t, err)
	databaseHandle, err := txn.GetDatabaseByID(table.databaseID)
	require.NoError(t, err)
	relation, err := databaseHandle.GetRelationByID(table.tableID)
	require.NoError(t, err)
	iterator := relation.MakeObjectIt(false)
	visible := false
	for iterator.Next() {
		if *iterator.GetObject().GetID() == *table.source.ObjectName().ObjectId() {
			visible = true
		}
	}
	require.NoError(t, iterator.Close())
	require.NoError(t, txn.Commit(ctx))
	return visible
}

func lifecycleRPCVisibleObjectCount(
	t *testing.T,
	ctx context.Context,
	database *db.DB,
	table lifecycleRPCTable,
) int {
	t.Helper()
	txn, err := database.StartTxn(nil)
	require.NoError(t, err)
	databaseHandle, err := txn.GetDatabaseByID(table.databaseID)
	require.NoError(t, err)
	relation, err := databaseHandle.GetRelationByID(table.tableID)
	require.NoError(t, err)
	iterator := relation.MakeObjectIt(false)
	count := 0
	for iterator.Next() {
		count++
	}
	require.NoError(t, iterator.Close())
	require.NoError(t, txn.Commit(ctx))
	return count
}

func deleteLifecycleRPCSourceRows(
	t *testing.T,
	ctx context.Context,
	h *mockHandle,
	table lifecycleRPCTable,
) {
	deleteLifecycleRPCObjectRows(t, ctx, h, table, table.source)
}

func deleteLifecycleRPCObjectRows(
	t *testing.T,
	ctx context.Context,
	h *mockHandle,
	table lifecycleRPCTable,
	source objectio.ObjectStats,
) {
	t.Helper()
	txn, err := h.db.StartTxn(nil)
	require.NoError(t, err)
	database, err := txn.GetDatabaseByID(table.databaseID)
	require.NoError(t, err)
	relation, err := database.GetRelationByID(table.tableID)
	require.NoError(t, err)
	object, err := relation.GetObject(source.ObjectName().ObjectId(), false)
	require.NoError(t, err)
	defer object.Close()
	var value *containers.Batch
	require.NoError(t, object.HybridScan(
		ctx,
		&value,
		0,
		[]int{
			table.schema.GetColIdx(catalog.PhyAddrColumnName),
			table.schema.GetPrimaryKey().Idx,
		},
		common.DefaultAllocator,
	))
	defer value.Close()
	value.Compact()
	require.NotZero(t, value.Length())
	require.NoError(t, relation.DeleteByPhyAddrKeys(
		value.Vecs[0],
		value.Vecs[1],
		handle.DT_Normal,
	))
	require.NoError(t, txn.Commit(ctx))
}

func TestLifecycleWholeCommitUsesTAECommitAndSurvivesReplay(t *testing.T) {
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
	jobID := "attempt-whole-replay"
	registerLifecycleRPCProtection(t, h, jobID, table.source)
	request := lifecycleWholeCommitRequest(t, table, sourceSnapshot, jobID)
	_, err := h.HandleCommit(ctx, mock1PCTxn(h.db), nil, request)
	require.NoError(t, err)
	require.False(t, lifecycleRPCObjectVisible(t, ctx, h.db, table))

	directory := h.db.Dir
	require.NoError(t, h.HandleClose(ctx))
	h.Handle = nil
	reopened, err := db.Open(ctx, directory, opts)
	require.NoError(t, err)
	h.Handle = &Handle{db: reopened}
	require.False(t, lifecycleRPCObjectVisible(t, ctx, h.db, table))
}

func TestLifecycleWholeCommitIsAtomicWithOrdinaryWrite(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()
	opts := config.WithLongScanAndCKPOpts(nil)
	h := mockTAEHandle(ctx, t, opts)
	t.Cleanup(func() { opts.Fs.Close(ctx) })
	t.Cleanup(func() { _ = h.HandleClose(ctx) })

	table := newLifecycleRPCTable(t, ctx, h)
	marker := newLifecycleRPCMarkerTable(t, ctx, h, table.databaseID)
	sourceSnapshot := h.db.TxnMgr.Now()
	jobID := "attempt-whole-atomic-success"
	registerLifecycleRPCProtection(t, h, jobID, table.source)
	request := lifecycleWholeCommitRequest(t, table, sourceSnapshot, jobID)
	prependLifecycleRPCEntry(t, request, lifecycleRPCMarkerInsertEntry(t, marker))
	meta := mock1PCTxn(h.db)
	meta.Mode = txnpb.TxnMode_Pessimistic

	_, err := h.HandleCommit(ctx, meta, nil, request)
	require.NoError(t, err)
	require.Equal(t, uint64(1), lifecycleRPCMarkerRows(t, ctx, h.db, marker))
	require.False(t, lifecycleRPCObjectVisible(t, ctx, h.db, table))
}

func TestLifecycleWholeFailureRollsBackOrdinaryWrite(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()
	opts := config.WithLongScanAndCKPOpts(nil)
	h := mockTAEHandle(ctx, t, opts)
	t.Cleanup(func() { opts.Fs.Close(ctx) })
	t.Cleanup(func() { _ = h.HandleClose(ctx) })

	table := newLifecycleRPCTable(t, ctx, h)
	marker := newLifecycleRPCMarkerTable(t, ctx, h, table.databaseID)
	sourceSnapshot := h.db.TxnMgr.Now()
	jobID := "attempt-whole-atomic-failure"
	registerLifecycleRPCProtection(t, h, jobID, table.source)
	request := lifecycleWholeCommitRequest(t, table, sourceSnapshot, jobID)
	control := prependLifecycleRPCEntry(
		t,
		request,
		lifecycleRPCMarkerInsertEntry(t, marker),
	)

	// Keep the real Object identity, but change the exact ObjectStats bytes.
	// The ordinary write is built first; the Lifecycle exact-source CAS then
	// fails and must roll the whole pessimistic TN transaction back.
	var mismatched objectio.ObjectStats
	copy(mismatched[:], control.DataSourceObjectStats[0])
	if mismatched.GetLevel() == 7 {
		mismatched.SetLevel(6)
	} else {
		mismatched.SetLevel(7)
	}
	control.DataSourceObjectStats[0] = append([]byte(nil), mismatched[:]...)
	control.SourceSetDigest = lifecycleSourceSetDigest(control.DataSourceObjectStats)
	command := new(api.PrecommitWriteCmd)
	require.NoError(t, command.UnmarshalBinary(request.Payload[0].CNRequest.Payload))
	command.EntryList[1].LifecycleCommit = control
	payload, err := command.MarshalBinary()
	require.NoError(t, err)
	request.Payload[0].CNRequest.Payload = payload

	meta := mock1PCTxn(h.db)
	meta.Mode = txnpb.TxnMode_Pessimistic
	_, err = h.HandleCommit(ctx, meta, nil, request)
	require.Error(t, err)
	require.Zero(t, lifecycleRPCMarkerRows(t, ctx, h.db, marker))
	require.True(t, lifecycleRPCObjectVisible(t, ctx, h.db, table))
}

func TestLifecycleWholeMultiSourceExactSetIsAtomic(t *testing.T) {
	t.Run("all sources retire", func(t *testing.T) {
		defer testutils.AfterTest(t)()
		ctx := context.Background()
		opts := config.WithLongScanAndCKPOpts(nil)
		h := mockTAEHandle(ctx, t, opts)
		t.Cleanup(func() { opts.Fs.Close(ctx) })
		t.Cleanup(func() { _ = h.HandleClose(ctx) })

		table := newLifecycleRPCMultiSourceTable(t, ctx, h)
		require.Len(t, table.sources, 2)
		sourceSnapshot := h.db.TxnMgr.Now()
		jobID := "attempt-whole-multi-success"
		registerLifecycleRPCProtectionSet(t, h, jobID, table.sources)
		request := lifecycleWholeCommitRequest(t, table, sourceSnapshot, jobID)
		meta := mock1PCTxn(h.db)
		meta.Mode = txnpb.TxnMode_Pessimistic

		_, err := h.HandleCommit(ctx, meta, nil, request)
		require.NoError(t, err)
		for index := range table.sources {
			require.False(t, lifecycleRPCObjectStatsVisible(
				t,
				ctx,
				h,
				table,
				table.sources[index],
			))
		}
	})

	t.Run("one source mismatch retires none", func(t *testing.T) {
		defer testutils.AfterTest(t)()
		ctx := context.Background()
		opts := config.WithLongScanAndCKPOpts(nil)
		h := mockTAEHandle(ctx, t, opts)
		t.Cleanup(func() { opts.Fs.Close(ctx) })
		t.Cleanup(func() { _ = h.HandleClose(ctx) })

		table := newLifecycleRPCMultiSourceTable(t, ctx, h)
		require.Len(t, table.sources, 2)
		sourceSnapshot := h.db.TxnMgr.Now()
		jobID := "attempt-whole-multi-conflict"
		registerLifecycleRPCProtectionSet(t, h, jobID, table.sources)
		request := lifecycleWholeCommitRequest(t, table, sourceSnapshot, jobID)
		command := new(api.PrecommitWriteCmd)
		require.NoError(t, command.UnmarshalBinary(request.Payload[0].CNRequest.Payload))
		control := command.EntryList[0].LifecycleCommit
		var mismatched objectio.ObjectStats
		copy(mismatched[:], control.DataSourceObjectStats[1])
		if mismatched.GetLevel() == 7 {
			mismatched.SetLevel(6)
		} else {
			mismatched.SetLevel(7)
		}
		control.DataSourceObjectStats[1] = append([]byte(nil), mismatched[:]...)
		control.SourceSetDigest = lifecycleSourceSetDigest(control.DataSourceObjectStats)
		payload, err := command.MarshalBinary()
		require.NoError(t, err)
		request.Payload[0].CNRequest.Payload = payload
		meta := mock1PCTxn(h.db)
		meta.Mode = txnpb.TxnMode_Pessimistic

		_, err = h.HandleCommit(ctx, meta, nil, request)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict), err)
		for index := range table.sources {
			require.True(t, lifecycleRPCObjectStatsVisible(
				t,
				ctx,
				h,
				table,
				table.sources[index],
			))
		}
	})

	t.Run("second source post snapshot delete rolls back the set", func(t *testing.T) {
		defer testutils.AfterTest(t)()
		ctx := context.Background()
		opts := config.WithLongScanAndCKPOpts(nil)
		h := mockTAEHandle(ctx, t, opts)
		t.Cleanup(func() { opts.Fs.Close(ctx) })
		t.Cleanup(func() { _ = h.HandleClose(ctx) })

		table := newLifecycleRPCMultiSourceTable(t, ctx, h)
		marker := newLifecycleRPCMarkerTable(t, ctx, h, table.databaseID)
		require.Len(t, table.sources, 2)
		sourceSnapshot := h.db.TxnMgr.Now()
		deleteLifecycleRPCObjectRows(t, ctx, h, table, table.sources[1])
		jobID := "attempt-whole-multi-post-s-delete"
		registerLifecycleRPCProtectionSet(t, h, jobID, table.sources)
		request := lifecycleWholeCommitRequest(t, table, sourceSnapshot, jobID)
		prependLifecycleRPCEntry(t, request, lifecycleRPCMarkerInsertEntry(t, marker))
		meta := mock1PCTxn(h.db)
		meta.Mode = txnpb.TxnMode_Pessimistic

		_, err := h.HandleCommit(ctx, meta, nil, request)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict), err)
		require.Zero(t, lifecycleRPCMarkerRows(t, ctx, h.db, marker))
		for index := range table.sources {
			require.True(t, lifecycleRPCObjectStatsVisible(
				t,
				ctx,
				h,
				table,
				table.sources[index],
			))
		}
	})
}

func TestLifecycleWholePostSnapshotDeleteAbortsRetirement(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()
	opts := config.WithLongScanAndCKPOpts(nil)
	h := mockTAEHandle(ctx, t, opts)
	t.Cleanup(func() { opts.Fs.Close(ctx) })
	t.Cleanup(func() { _ = h.HandleClose(ctx) })

	table := newLifecycleRPCTable(t, ctx, h)
	sourceSnapshot := h.db.TxnMgr.Now()
	deleteLifecycleRPCSourceRows(t, ctx, h, table)
	jobID := "attempt-whole-post-s-delete"
	registerLifecycleRPCProtection(t, h, jobID, table.source)
	request := lifecycleWholeCommitRequest(t, table, sourceSnapshot, jobID)
	_, err := h.HandleCommit(ctx, mock1PCTxn(h.db), nil, request)
	require.Error(t, err)
	require.True(t, lifecycleRPCObjectVisible(t, ctx, h.db, table))
}

func TestLifecycleWholeExactSourceCASLosesToOrdinaryMerge(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()
	opts := config.WithLongScanAndCKPOpts(nil)
	h := mockTAEHandle(ctx, t, opts)
	t.Cleanup(func() { opts.Fs.Close(ctx) })
	t.Cleanup(func() { _ = h.HandleClose(ctx) })

	table := newLifecycleRPCTable(t, ctx, h)
	sourceSnapshot := h.db.TxnMgr.Now()
	mergeTxn, err := h.db.StartTxn(nil)
	require.NoError(t, err)
	database, err := mergeTxn.GetDatabaseByID(table.databaseID)
	require.NoError(t, err)
	relation, err := database.GetRelationByID(table.tableID)
	require.NoError(t, err)
	object, err := relation.GetObject(table.source.ObjectName().ObjectId(), false)
	require.NoError(t, err)
	sourceEntry := object.GetMeta().(*catalog.ObjectEntry)
	require.NoError(t, object.Close())
	mergeTask, err := jobs.NewMergeObjectsTask(
		nil,
		mergeTxn,
		[]*catalog.ObjectEntry{sourceEntry},
		h.db.Runtime,
		0,
		false,
	)
	require.NoError(t, err)
	require.NoError(t, mergeTask.OnExec(ctx))
	require.NoError(t, mergeTxn.Commit(ctx))
	require.False(t, lifecycleRPCObjectVisible(t, ctx, h.db, table))
	require.Positive(t, lifecycleRPCVisibleObjectCount(t, ctx, h.db, table))

	jobID := "attempt-whole-merge-conflict"
	registerLifecycleRPCProtection(t, h, jobID, table.source)
	request := lifecycleWholeCommitRequest(t, table, sourceSnapshot, jobID)
	_, err = h.HandleCommit(ctx, mock1PCTxn(h.db), nil, request)
	require.Error(t, err)
	require.Positive(t, lifecycleRPCVisibleObjectCount(t, ctx, h.db, table))
}
