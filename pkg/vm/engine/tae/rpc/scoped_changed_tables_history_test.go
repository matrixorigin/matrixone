// Copyright 2021 - 2022 Matrix Origin
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
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/ckputil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	taelogtail "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/stretchr/testify/require"
)

type scopedHistoryCountingFS struct {
	fileservice.FileService
	path          string
	blockByOffset map[int64]int
	blocksPerRead []int
	entriesRead   int
	failOnSecond  error
}

func (fs *scopedHistoryCountingFS) Read(ctx context.Context, request *fileservice.IOVector) error {
	if request.FilePath == fs.path {
		dataBlocks := make(map[int]struct{})
		columns := 0
		for _, entry := range request.Entries {
			if block, ok := fs.blockByOffset[entry.Offset]; ok {
				dataBlocks[block] = struct{}{}
				columns++
			}
		}
		if len(dataBlocks) != 0 {
			if fs.failOnSecond != nil && len(fs.blocksPerRead) == 1 {
				return fs.failOnSecond
			}
			fs.blocksPerRead = append(fs.blocksPerRead, len(dataBlocks))
			fs.entriesRead += columns
		}
	}
	return fs.FileService.Read(ctx, request)
}

func newScopedCheckpointTestHandle(t *testing.T, opts *options.Options) *mockHandle {
	t.Helper()
	ioutil.Start("")
	engine, err := db.Open(t.Context(), t.TempDir(), opts)
	require.NoError(t, err)
	h := &mockHandle{Handle: &Handle{db: engine}, m: mpool.MustNewZero()}
	t.Cleanup(func() { require.NoError(t, h.HandleClose(context.Background())) })
	registerScopedChangedTablesPublisher(t, h.Handle)
	return h
}

func persistScopedHistoryIndex(
	t *testing.T,
	h *mockHandle,
	feed func(func(cmd_util.ScopedChangedTable, types.TS, types.TS)),
) objectio.LocationSlice {
	t.Helper()
	ctx := t.Context()
	factory := ioutil.NewFSinkerImplFactory(taelogtail.TableIDSeqnums, -1, false, false, 0)
	sinker := ioutil.NewSinker(-1, taelogtail.TableIDAttrs, taelogtail.TableIDTypes, factory, h.m, h.db.Runtime.Fs,
		ioutil.WithMemorySizeThreshold(4*1024*1024))
	defer sinker.Close()
	bat := batch.NewWithTypes(taelogtail.TableIDTypes, h.m)
	defer bat.Clean(h.m)

	accounts := make([]uint32, 0, objectio.BlockMaxRows)
	dbs := make([]uint64, 0, objectio.BlockMaxRows)
	tables := make([]uint64, 0, objectio.BlockMaxRows)
	starts := make([]types.TS, 0, objectio.BlockMaxRows)
	ends := make([]types.TS, 0, objectio.BlockMaxRows)
	write := func() {
		t.Helper()
		if len(tables) == 0 {
			return
		}
		require.NoError(t, vector.AppendFixedList(bat.Vecs[0], accounts, nil, h.m))
		require.NoError(t, vector.AppendFixedList(bat.Vecs[1], dbs, nil, h.m))
		require.NoError(t, vector.AppendFixedList(bat.Vecs[2], tables, nil, h.m))
		require.NoError(t, vector.AppendFixedList(bat.Vecs[3], starts, nil, h.m))
		require.NoError(t, vector.AppendFixedList(bat.Vecs[4], ends, nil, h.m))
		bat.SetRowCount(len(tables))
		require.NoError(t, sinker.Write(ctx, bat))
		bat.CleanOnlyData()
		accounts, dbs, tables = accounts[:0], dbs[:0], tables[:0]
		starts, ends = starts[:0], ends[:0]
	}
	feed(func(key cmd_util.ScopedChangedTable, start, end types.TS) {
		accounts = append(accounts, key.AccountID)
		dbs = append(dbs, key.DatabaseID)
		tables = append(tables, key.TableID)
		starts = append(starts, start)
		ends = append(ends, end)
		if len(tables) == objectio.BlockMaxRows {
			write()
		}
	})
	write()
	require.NoError(t, sinker.Sync(ctx))
	files, memory := sinker.GetResult()
	require.Empty(t, memory)
	var locs objectio.LocationSlice
	for _, file := range files {
		location := file.ObjectLocation()
		location.SetID(uint16(file.BlkCnt()))
		locs.Append(location)
	}
	require.NotEmpty(t, locs)
	return locs
}

type scopedCheckpointRunner struct {
	checkpoint.Runner
	latest atomic.Pointer[checkpoint.CheckpointEntry]
}

func (r *scopedCheckpointRunner) MaxCheckpoint() *checkpoint.CheckpointEntry {
	return r.latest.Load()
}

func TestScopedChangedTablesRetriesRetiredCheckpointIndex(t *testing.T) {
	ctx := t.Context()
	fs := &scopedIndexReadFS{FileService: objectio.TmpNewFileservice(ctx, t.TempDir())}
	t.Cleanup(func() { fs.Close(context.Background()) })
	opts := config.WithLongScanAndCKPOpts(nil)
	opts.Fs = fs
	h := newScopedCheckpointTestHandle(t, opts)

	write := startScopedChangedTablesTxn(t, h.Handle)
	database, err := testutil.CreateDatabase2(ctx, write, "retired_checkpoint_index")
	require.NoError(t, err)
	schema := catalog.MockSchema(2, 0)
	relation, err := testutil.CreateRelation2(ctx, write, database, schema)
	require.NoError(t, err)
	rows := catalog.MockBatch(schema, 1)
	defer rows.Close()
	require.NoError(t, relation.Append(ctx, rows))
	require.NoError(t, write.Commit(ctx))
	commit := write.GetCommitTS()
	table := cmd_util.ScopedChangedTable{DatabaseID: database.GetID(), TableID: relation.ID()}

	require.NoError(t, h.db.ForceCheckpoint(ctx, h.db.TxnMgr.Now()))
	runner := h.db.BGCheckpointRunner
	predecessor := runner.MaxCheckpoint()
	require.NotNil(t, predecessor)
	require.True(t, predecessor.IsIncremental())
	oldIndex := predecessor.GetTableIDLocation()
	require.Equal(t, 1, oldIndex.Len())
	oldPath := oldIndex.Get(0).Name().String()
	require.NoError(t, h.db.ForceGlobalCheckpoint(ctx, predecessor.GetEnd(), time.Hour))
	successor := runner.MaxCheckpoint()
	require.NotNil(t, successor)
	require.True(t, successor.IsGlobal())
	newIndex := successor.GetTableIDLocation()
	require.Equal(t, 1, newIndex.Len())
	newPath := newIndex.Get(0).Name().String()
	require.NotEqual(t, oldPath, newPath)

	// Build both indexes before starting the RPC, then control when the
	// handler sees the successor. Stop periodic jobs before replacing the runner.
	h.db.CronJobs.Reset()
	selection := &scopedCheckpointRunner{Runner: runner}
	selection.latest.Store(predecessor)
	h.db.BGCheckpointRunner = selection
	defer func() { h.db.BGCheckpointRunner = runner }()
	h.db.LogtailMgr.SetReplayHistoryFloor(predecessor.GetEnd())
	req := cmd_util.GetScopedChangedTablesReq{
		From: commit.Prev().ToTimestamp(), To: commit.ToTimestamp(),
		Tables: []cmd_util.ScopedChangedTable{table},
	}
	var resp cmd_util.GetScopedChangedTablesResp
	check := func(t *testing.T) {
		t.Helper()
		_, err := h.HandleGetScopedChangedTables(t.Context(), txn.TxnMeta{}, &req, &resp)
		require.NoError(t, err)
		require.NoError(t, cmd_util.ValidateScopedChangedTablesResponse(t.Context(), &req, &resp))
		require.Equal(t, []cmd_util.ScopedChangedTable{table}, resp.Tables)
	}
	check(t)

	t.Run("retired index", func(t *testing.T) {
		failed, err := readScopedChangedTablesWithIndexPaused(t, h.Handle, fs, oldPath, &req, func(ctx context.Context) {
			// Model GC removing the old object after the covering checkpoint is published.
			selection.latest.Store(successor)
			require.NoError(t, fs.Delete(ctx, oldPath))
		})
		require.Empty(t, failed)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrChangeWatchRetry), "%v", err)
	})
	check(t)

	t.Run("missing current index", func(t *testing.T) {
		require.NoError(t, fs.Delete(t.Context(), newPath))
		_, err := h.HandleGetScopedChangedTables(t.Context(), txn.TxnMeta{}, &req, &resp)
		require.Empty(t, resp)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound), "%v", err)
	})
}

func TestScopedChangedTablesBatchesCheckpointReads(t *testing.T) {
	ctx := t.Context()
	fs := objectio.TmpNewSharedFileservice(ctx, t.TempDir())
	t.Cleanup(func() { fs.Close(context.Background()) })
	mp := mpool.MustNewZero()
	start := types.BuildTS(100, 0)
	end := types.BuildTS(101, 0)
	target := cmd_util.ScopedChangedTable{AccountID: 3, DatabaseID: 4000, TableID: 9000}

	const objectBlocks = scopedChangedTablesIndexGroupBlocks + 1
	name := objectio.BuildObjectName(objectio.NewSegmentid(), 0)
	writer, err := ioutil.NewBlockWriterNew(fs, name, 0, taelogtail.TableIDSeqnums, false)
	require.NoError(t, err)
	bat := batch.NewWithTypes(taelogtail.TableIDTypes, mp)
	defer bat.Clean(mp)
	for i := range objectBlocks {
		identity := cmd_util.ScopedChangedTable{AccountID: 3, DatabaseID: 4000, TableID: uint64(1000 + i)}
		first, last := start, start
		switch i {
		case 0:
			identity, last = cmd_util.ScopedChangedTable{}, end
		case objectBlocks - 1:
			identity, first, last = target, end, end
		}
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], identity.AccountID, false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], identity.DatabaseID, false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[2], identity.TableID, false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[3], first, false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[4], last, false, mp))
		bat.SetRowCount(1)
		_, err := writer.WriteBatch(bat)
		require.NoError(t, err)
		bat.CleanOnlyData()
	}
	blocks, meta, err := writer.Sync(ctx)
	require.NoError(t, err)
	require.Len(t, blocks, objectBlocks)
	var locations objectio.LocationSlice
	locations.Append(objectio.BuildLocation(name, meta, objectBlocks, objectBlocks))
	reads := &scopedHistoryCountingFS{
		FileService:   fs,
		path:          name.String(),
		blockByOffset: make(map[int64]int),
		failOnSecond:  errors.New("checkpoint index payload read failed"),
	}
	for i, block := range blocks {
		for _, col := range taelogtail.TableIDSeqnums {
			reads.blockByOffset[int64(block.ColumnMeta(col).Location().Offset())] = i
		}
	}
	require.Len(t, reads.blockByOffset, objectBlocks*len(taelogtail.TableIDSeqnums))
	var candidates []cmd_util.ScopedChangedTable
	add := func(candidate cmd_util.ScopedChangedTable) {
		candidates = append(candidates, candidate)
	}
	err = scanScopedChangedTableIndexLocations(ctx, locations, reads, end, end, add)
	require.ErrorIs(t, err, reads.failOnSecond)
	require.Len(t, reads.blocksPerRead, 1)
	require.Empty(t, candidates)

	reads.failOnSecond = nil
	reads.blocksPerRead = nil
	reads.entriesRead = 0
	require.NoError(t, scanScopedChangedTableIndexLocations(ctx, locations, reads, end, end, add))
	require.Equal(t, []cmd_util.ScopedChangedTable{target}, candidates)
	for _, count := range reads.blocksPerRead {
		require.LessOrEqual(t, count, scopedChangedTablesIndexGroupBlocks)
	}
	require.Equal(t, len(reads.blockByOffset), reads.entriesRead)
	require.Less(t, len(reads.blocksPerRead), objectBlocks)
}

func TestScopedChangedTablesLongCheckpointHistory(t *testing.T) {
	ctx := t.Context()
	h := newScopedCheckpointTestHandle(t, config.WithLongScanAndCKPOpts(nil))
	publish := startScopedChangedTablesTxn(t, h.Handle)
	_, err := testutil.CreateDatabase2(ctx, publish, "long_history_frontier")
	require.NoError(t, err)
	require.NoError(t, publish.Commit(ctx))
	published, err := h.db.LogtailMgr.ReadBarrier(ctx)
	require.NoError(t, err)
	end := types.TimestampToTS(published)
	require.Positive(t, end.Physical())
	start := types.BuildTS(end.Physical()-int64(24*time.Hour), end.Logical())
	target := cmd_util.ScopedChangedTable{AccountID: 17, DatabaseID: 4000, TableID: 9000}
	firstOnly := cmd_util.ScopedChangedTable{AccountID: 17, DatabaseID: 4000, TableID: 9001}
	absent := cmd_util.ScopedChangedTable{AccountID: 17, DatabaseID: 4100, TableID: 9002}
	wrongAccount := cmd_util.ScopedChangedTable{AccountID: 18, DatabaseID: absent.DatabaseID, TableID: absent.TableID}
	marker := cmd_util.ScopedChangedTable{}

	const intervals, tablesPerInterval = 288, 3500
	locations := persistScopedHistoryIndex(t, h, func(add func(cmd_util.ScopedChangedTable, types.TS, types.TS)) {
		// A first-block marker claims an extra tick. Only the intersection
		// with the final-block marker can authorize replay.
		add(marker, start.Prev(), end)
		for interval := range intervals {
			at := types.BuildTS(start.Physical()+int64(interval)*int64(5*time.Minute), start.Logical())
			for row := range tablesPerInterval {
				key := cmd_util.ScopedChangedTable{AccountID: 19, DatabaseID: 5000, TableID: uint64(10_000 + row)}
				switch {
				case interval == 0 && row == 0:
					key = target
				case interval == 0 && row == 1:
					key = firstOnly
				case interval == intervals-1 && row == tablesPerInterval-2:
					add(wrongAccount, end, end)
					continue
				case interval == intervals-1 && row == tablesPerInterval-1:
					add(target, end, end)
					continue
				}
				add(key, at, at)
			}
		}
		add(marker, start, end)
	})
	var blocks int
	for i := range locations.Len() {
		blocks += int(locations.Get(i).ID())
	}
	require.Greater(t, intervals*tablesPerInterval, scopedChangedTablesMaxWork)
	require.Equal(t, (intervals*tablesPerInterval+2+objectio.BlockMaxRows-1)/objectio.BlockMaxRows, blocks)
	require.Less(t, blocks, scopedChangedTablesMaxIndexBlocks)
	t.Logf("persisted %d rows across %d blocks for a 24-hour history", intervals*tablesPerInterval+2, blocks)

	entry := checkpoint.NewCheckpointEntry("", types.TS{}, end, checkpoint.ET_Global)
	entry.SetTableIDLocation(locations)
	original := h.db.BGCheckpointRunner
	h.db.BGCheckpointRunner = &checkpointRunnerWithMax{Runner: original, entry: entry}
	defer func() { h.db.BGCheckpointRunner = original }()
	h.db.LogtailMgr.SetReplayHistoryFloor(end)
	req := cmd_util.GetScopedChangedTablesReq{
		From:   start.Prev().ToTimestamp(),
		To:     end.ToTimestamp(),
		Tables: []cmd_util.ScopedChangedTable{target, firstOnly, absent},
	}
	var resp cmd_util.GetScopedChangedTablesResp
	check := func(want ...cmd_util.ScopedChangedTable) {
		t.Helper()
		_, err := h.HandleGetScopedChangedTables(ctx, txn.TxnMeta{}, &req, &resp)
		require.NoError(t, err)
		require.NoError(t, cmd_util.ValidateScopedChangedTablesResponse(ctx, &req, &resp))
		require.ElementsMatch(t, want, resp.Tables)
	}
	check(target, firstOnly)
	req.From = end.Prev().ToTimestamp()
	check(target)

	req.From = start.Prev().Prev().ToTimestamp()
	_, err = h.HandleGetScopedChangedTables(ctx, txn.TxnMeta{}, &req, &resp)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrChangeWatchHistoryUnavailable), "%v", err)
	require.Empty(t, resp)

	req.From = start.Prev().ToTimestamp()
	excessive := append(objectio.LocationSlice(nil), locations...)
	excessive.Get(0).SetID(scopedChangedTablesMaxIndexBlocks + 1)
	entry.SetTableIDLocation(excessive)
	_, err = h.HandleGetScopedChangedTables(ctx, txn.TxnMeta{}, &req, &resp)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrChangeWatchCapacity), "%v", err)
	require.Empty(t, resp)

	valid := persistScopedHistoryIndex(t, h, func(add func(cmd_util.ScopedChangedTable, types.TS, types.TS)) {
		add(marker, start, end)
		add(target, end, end)
	})
	invalid := persistScopedHistoryIndex(t, h, func(add func(cmd_util.ScopedChangedTable, types.TS, types.TS)) {
		add(cmd_util.ScopedChangedTable{TableID: 9876}, end, end)
	})
	entry.SetTableIDLocation(append(valid, invalid...))
	req.Tables = []cmd_util.ScopedChangedTable{target}
	_, err = h.HandleGetScopedChangedTables(ctx, txn.TxnMeta{}, &req, &resp)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal), "%v", err)
	require.ErrorContains(t, err, "invalid scoped changed-table checkpoint identity")
	require.Empty(t, resp)
}

func TestScopedChangedTablesReadsIncrementalAppendableHistory(t *testing.T) {
	ctx := t.Context()
	opts := config.WithLongScanAndCKPOpts(nil)
	h := newScopedCheckpointTestHandle(t, opts)

	const databaseName = "incremental_watch_history"
	insertSchema := catalog.MockSchema(2, 0)
	insertSchema.Name = "append_target"
	deleteSchema := catalog.MockSchema(2, 0)
	deleteSchema.Name = "delete_target"
	rows := catalog.MockBatch(deleteSchema, 1)
	defer rows.Close()

	create := startScopedChangedTablesTxn(t, h.Handle)
	database, err := testutil.CreateDatabase2(ctx, create, databaseName)
	require.NoError(t, err)
	appendRelation, err := testutil.CreateRelation2(ctx, create, database, insertSchema)
	require.NoError(t, err)
	deleteRelation, err := testutil.CreateRelation2(ctx, create, database, deleteSchema)
	require.NoError(t, err)
	insertScope := cmd_util.ScopedChangedTable{DatabaseID: database.GetID(), TableID: appendRelation.ID()}
	deleteScope := cmd_util.ScopedChangedTable{DatabaseID: database.GetID(), TableID: deleteRelation.ID()}
	require.NoError(t, deleteRelation.Append(ctx, rows))
	require.NoError(t, create.Commit(ctx))
	require.NoError(t, h.db.ForceCheckpoint(ctx, h.db.TxnMgr.Now()))
	first := h.db.BGCheckpointRunner.MaxIncrementalCheckpoint()
	require.NotNil(t, first)

	insert := startScopedChangedTablesTxn(t, h.Handle)
	database, err = insert.GetDatabase(databaseName)
	require.NoError(t, err)
	appendRelation, err = database.GetRelationByName(insertSchema.Name)
	require.NoError(t, err)
	require.NoError(t, appendRelation.Append(ctx, rows))
	require.NoError(t, insert.Commit(ctx))
	require.True(t, insert.GetMemo().HasTableDataChanges(insertScope.TableID))

	remove := startScopedChangedTablesTxn(t, h.Handle)
	database, err = remove.GetDatabase(databaseName)
	require.NoError(t, err)
	deleteRelation, err = database.GetRelationByName(deleteSchema.Name)
	require.NoError(t, err)
	filter := handle.NewEQFilter(rows.Vecs[deleteSchema.GetPrimaryKey().Idx].Get(0))
	require.NoError(t, deleteRelation.DeleteByFilter(ctx, filter))
	require.NoError(t, remove.Commit(ctx))
	require.True(t, remove.GetMemo().HasTableDataChanges(deleteScope.TableID))
	require.False(t, remove.GetMemo().HasTableDataChanges(insertScope.TableID))

	require.NoError(t, h.db.ForceCheckpoint(ctx, h.db.TxnMgr.Now()))
	entry := h.db.BGCheckpointRunner.MaxIncrementalCheckpoint()
	require.NotNil(t, entry)
	coveredFrom, coveredTo := first.GetEnd(), entry.GetEnd()
	insertTS, deleteTS := insert.GetCommitTS(), remove.GetCommitTS()
	require.True(t, coveredFrom.LT(&insertTS))
	require.True(t, insertTS.LT(&deleteTS))
	require.True(t, deleteTS.LE(&coveredTo))
	require.NotEmpty(t, entry.GetTableIDLocation())
	ckpReader := taelogtail.NewCKPReader(entry.GetVersion(), entry.GetLocation(), common.CheckpointAllocator, h.db.Runtime.Fs)
	require.NoError(t, ckpReader.ReadMeta(ctx))
	var appendableData, appendableTombstone bool
	require.NoError(t, ckpReader.ForEachRow(ctx, func(
		_ uint32, _ uint64, table uint64, kind int8, stats objectio.ObjectStats, _, _ types.TS, _ types.Rowid,
	) error {
		if stats.GetAppendable() {
			appendableData = appendableData || table == insertScope.TableID && kind == ckputil.ObjectType_Data
			appendableTombstone = appendableTombstone || table == deleteScope.TableID && kind == ckputil.ObjectType_Tombstone
		}
		return nil
	}))
	require.True(t, appendableData, "expected real appendable data in the incremental checkpoint")
	require.True(t, appendableTombstone, "expected real appendable tombstone in the incremental checkpoint")

	// Advance another durable boundary while both watched tables are idle.
	// The replayed index must retain their original physical IDs and windows.
	idle := startScopedChangedTablesTxn(t, h.Handle)
	_, err = testutil.CreateDatabase2(ctx, idle, "unrelated_idle_checkpoint")
	require.NoError(t, err)
	require.NoError(t, idle.Commit(ctx))
	require.False(t, idle.GetMemo().HasTableDataChanges(insertScope.TableID))
	require.False(t, idle.GetMemo().HasTableDataChanges(deleteScope.TableID))
	require.NoError(t, h.db.ForceCheckpoint(ctx, h.db.TxnMgr.Now()))
	idleEntry := h.db.BGCheckpointRunner.MaxIncrementalCheckpoint()
	require.NotNil(t, idleEntry)
	replayEnd := idleEntry.GetEnd()
	require.True(t, coveredTo.LT(&replayEnd))
	require.NotEmpty(t, idleEntry.GetTableIDLocation())

	dir := h.db.Dir
	require.NoError(t, h.db.Close())
	reopened, err := db.Open(ctx, dir, opts)
	require.NoError(t, err)
	h.Handle = &Handle{db: reopened}
	registerScopedChangedTablesPublisher(t, h.Handle)
	require.Equal(t, replayEnd, reopened.LogtailMgr.GetDirtyHistoryFloor())
	publish := startScopedChangedTablesTxn(t, h.Handle)
	_, err = testutil.CreateDatabase2(ctx, publish, "incremental_history_frontier")
	require.NoError(t, err)
	require.NoError(t, publish.Commit(ctx))

	for _, actual := range []struct {
		at    types.TS
		table cmd_util.ScopedChangedTable
	}{{insertTS, insertScope}, {deleteTS, deleteScope}} {
		req := cmd_util.GetScopedChangedTablesReq{
			From:   actual.at.Prev().ToTimestamp(),
			To:     actual.at.ToTimestamp(),
			Tables: []cmd_util.ScopedChangedTable{actual.table},
		}
		var resp cmd_util.GetScopedChangedTablesResp
		_, err = h.HandleGetScopedChangedTables(ctx, txn.TxnMeta{}, &req, &resp)
		require.NoError(t, err)
		require.NoError(t, cmd_util.ValidateScopedChangedTablesResponse(ctx, &req, &resp))
		require.Equal(t, []cmd_util.ScopedChangedTable{actual.table}, resp.Tables)
	}
	t.Logf("incremental checkpoint end=%s idle checkpoint end=%s insert=%s delete=%s", coveredTo.ToString(), replayEnd.ToString(), insertTS.ToString(), deleteTS.ToString())
}
