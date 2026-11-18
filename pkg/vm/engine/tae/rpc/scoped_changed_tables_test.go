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
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	taelogtail "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/stretchr/testify/require"
)

func registerScopedChangedTablesPublisher(t *testing.T, h *Handle) {
	t.Helper()
	require.NoError(t, h.db.LogtailMgr.RegisterCallback(func(_, _ timestamp.Timestamp, closeCB func(), _ ...logtail.TableLogtail) error {
		if closeCB != nil {
			closeCB()
		}
		return nil
	}))
}

func startScopedChangedTablesTxn(t *testing.T, h *Handle) txnif.AsyncTxn {
	t.Helper()
	tx, err := h.db.StartTxn(nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		if tx.GetTxnState(false) == txnif.TxnStateActive {
			_ = tx.Rollback(context.Background())
		}
	})
	return tx
}

func TestScopedChangedTablesReadDirtyAndReplay(t *testing.T) {
	ctx := t.Context()
	h := mockTAEHandle(ctx, t, config.WithLongScanAndCKPOpts(nil))
	t.Cleanup(func() { require.NoError(t, h.HandleClose(context.Background())) })
	registerScopedChangedTablesPublisher(t, h.Handle)

	schema := catalog.MockSchema(2, 0)
	schema.Name = "scoped"
	bat := catalog.MockBatch(schema, 1)
	defer bat.Close()
	create := startScopedChangedTablesTxn(t, h.Handle)
	database, err := testutil.CreateDatabase2(ctx, create, "watch")
	require.NoError(t, err)
	relation, err := testutil.CreateRelation2(ctx, create, database, schema)
	require.NoError(t, err)
	scope := cmd_util.ScopedChangedTable{DatabaseID: database.GetID(), TableID: relation.ID()}
	other := catalog.MockSchema(2, 0)
	other.Name = "unscoped"
	unscoped, err := testutil.CreateRelation2(ctx, create, database, other)
	require.NoError(t, err)
	require.NoError(t, relation.Append(ctx, bat))
	require.NoError(t, unscoped.Append(ctx, bat))
	require.NoError(t, create.Commit(ctx))
	require.NoError(t, h.db.ForceCheckpoint(ctx, h.db.TxnMgr.Now()))
	ckp := h.db.BGCheckpointRunner.MaxCheckpoint()
	require.NotNil(t, ckp)
	checkpointEnd := ckp.GetEnd()

	del := startScopedChangedTablesTxn(t, h.Handle)
	database, err = del.GetDatabase("watch")
	require.NoError(t, err)
	relation, err = database.GetRelationByName(schema.Name)
	require.NoError(t, err)
	unscoped, err = database.GetRelationByName(other.Name)
	require.NoError(t, err)
	filter := handle.NewEQFilter(bat.Vecs[schema.GetPrimaryKey().Idx].Get(0))
	require.NoError(t, relation.DeleteByFilter(ctx, filter))
	require.NoError(t, unscoped.DeleteByFilter(ctx, filter))
	require.NoError(t, del.Commit(ctx))

	commit := del.GetCommitTS()
	require.Equal(t, commit, del.GetPrepareTS(), "single-TN prepare and commit must share one HLC")
	require.True(t, del.GetMemo().HasTableDataChanges(scope.TableID), "delete-only transactions must retain their base table identity")
	require.True(t, h.db.LogtailMgr.GetReader(commit, commit).IsDirtyOnTable(scope.DatabaseID, scope.TableID), "exact-cut legacy control")
	req := cmd_util.GetScopedChangedTablesReq{
		From:   commit.Prev().ToTimestamp(),
		To:     commit.ToTimestamp(),
		Tables: []cmd_util.ScopedChangedTable{scope},
	}
	var resp cmd_util.GetScopedChangedTablesResp
	check := func() {
		t.Helper()
		_, err := h.HandleGetScopedChangedTables(ctx, txn.TxnMeta{}, &req, &resp)
		require.NoError(t, err)
		require.NoError(t, cmd_util.ValidateScopedChangedTablesResponse(ctx, &req, &resp))
		require.Equal(t, []cmd_util.ScopedChangedTable{scope}, resp.Tables)
	}
	check()

	excluded := req
	excluded.From = excluded.To
	_, err = h.HandleGetScopedChangedTables(ctx, txn.TxnMeta{}, &excluded, &resp)
	require.NoError(t, err)
	require.NoError(t, cmd_util.ValidateScopedChangedTablesResponse(ctx, &excluded, &resp))
	require.Empty(t, resp.Tables)

	malformed := req
	malformed.Tables = []cmd_util.ScopedChangedTable{{AccountID: 91, DatabaseID: scope.DatabaseID, TableID: scope.TableID}}
	_, err = h.HandleGetScopedChangedTables(ctx, txn.TxnMeta{}, &malformed, &resp)
	require.ErrorContains(t, err, "database identity could not be verified")
	require.Empty(t, resp)

	future := req
	future.To.PhysicalTime += time.Hour.Nanoseconds()
	_, err = h.HandleGetScopedChangedTables(ctx, txn.TxnMeta{}, &future, &resp)
	require.ErrorContains(t, err, "publication frontier")
	require.Empty(t, resp)

	reopenDir, reopenOpts := h.db.Dir, h.db.Opts
	require.NoError(t, h.db.Close())
	reopened, err := db.Open(ctx, reopenDir, reopenOpts)
	require.NoError(t, err)
	h.Handle = &Handle{db: reopened}
	registerScopedChangedTablesPublisher(t, h.Handle)
	// WAL replay does not publish logtail again. A normal commit establishes a
	// fresh generation's frontier while the earlier changed row comes from replay.
	publish := startScopedChangedTablesTxn(t, h.Handle)
	_, err = testutil.CreateDatabase2(ctx, publish, "advance_frontier")
	require.NoError(t, err)
	require.NoError(t, publish.Commit(ctx))
	require.Equal(t, checkpointEnd, h.db.LogtailMgr.GetDirtyHistoryFloor())
	check()

	// The same scan can cross the durable checkpoint into WAL replay.
	req.From = create.GetCommitTS().Prev().ToTimestamp()
	check()
}

type scopedIndexReadGate struct {
	path            string
	entered, resume chan struct{}
	hit             atomic.Bool
}

type scopedIndexReadFS struct {
	fileservice.FileService
	gate atomic.Pointer[scopedIndexReadGate]
}

func (fs *scopedIndexReadFS) Read(ctx context.Context, request *fileservice.IOVector) error {
	gate := fs.gate.Load()
	if gate != nil && request.FilePath == gate.path && gate.hit.CompareAndSwap(false, true) {
		close(gate.entered)
		select {
		case <-gate.resume:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return fs.FileService.Read(ctx, request)
}

func TestScopedChangedTablesReadIndexAndFailClosed(t *testing.T) {
	ctx := t.Context()
	opts := config.WithLongScanAndCKPOpts(nil)
	fs := &scopedIndexReadFS{FileService: objectio.TmpNewFileservice(ctx, t.TempDir())}
	opts.Fs = fs
	h := mockTAEHandle(ctx, t, opts)
	t.Cleanup(func() { require.NoError(t, h.HandleClose(context.Background())) })
	registerScopedChangedTablesPublisher(t, h.Handle)

	publish := startScopedChangedTablesTxn(t, h.Handle)
	_, err := testutil.CreateDatabase2(ctx, publish, "advance_frontier")
	require.NoError(t, err)
	require.NoError(t, publish.Commit(ctx))
	published := publish.GetCommitTS()
	end := published.Prev()
	start := types.BuildTS(end.Physical()-time.Hour.Nanoseconds(), 0)
	locations, err := taelogtail.MockTableIDBatch(ctx, start, end, 64, 2, h.m, h.db.Runtime.Fs)
	require.NoError(t, err)
	entry := checkpoint.NewCheckpointEntry("", types.TS{}, end, checkpoint.ET_Global)
	entry.SetTableIDLocation(locations)
	original := h.db.BGCheckpointRunner
	h.db.BGCheckpointRunner = &checkpointRunnerWithMax{Runner: original, entry: entry}
	defer func() { h.db.BGCheckpointRunner = original }()
	h.db.LogtailMgr.SetReplayCheckpointEnd(end)

	requested := cmd_util.ScopedChangedTable{AccountID: 0, DatabaseID: 3000, TableID: 1000}
	wrongAccount := cmd_util.ScopedChangedTable{AccountID: 1, DatabaseID: 3001, TableID: 1001}
	req := cmd_util.GetScopedChangedTablesReq{
		From:   start.Prev().ToTimestamp(),
		To:     published.ToTimestamp(),
		Tables: []cmd_util.ScopedChangedTable{requested, wrongAccount},
	}
	var resp cmd_util.GetScopedChangedTablesResp
	_, err = h.HandleGetScopedChangedTables(ctx, txn.TxnMeta{}, &req, &resp)
	require.NoError(t, err)
	require.NoError(t, cmd_util.ValidateScopedChangedTablesResponse(ctx, &req, &resp))
	require.Equal(t, []cmd_util.ScopedChangedTable{requested}, resp.Tables)

	stale := req
	stale.From = start.Prev().Prev().ToTimestamp()
	_, err = h.HandleGetScopedChangedTables(ctx, txn.TxnMeta{}, &stale, &resp)
	require.ErrorContains(t, err, "history is incomplete")
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrChangeWatchHistoryUnavailable))
	require.Empty(t, resp)

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = h.HandleGetScopedChangedTables(canceled, txn.TxnMeta{}, &req, &resp)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, resp)
	require.False(t, h.scopedChangedTablesInFlight.Load())

	t.Run("concurrent GC discards candidates", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		gate := &scopedIndexReadGate{
			path:    locations.Get(0).Name().UnsafeString(),
			entered: make(chan struct{}),
			resume:  make(chan struct{}),
		}
		fs.gate.Store(gate)
		defer fs.gate.Store(nil)
		type probeResult struct {
			resp cmd_util.GetScopedChangedTablesResp
			err  error
		}
		done := make(chan probeResult, 1)
		finished := make(chan struct{})
		go func() {
			defer close(finished)
			var out probeResult
			_, out.err = h.HandleGetScopedChangedTables(ctx, txn.TxnMeta{}, &req, &out.resp)
			done <- out
		}()
		defer func() {
			cancel()
			<-finished
		}()
		select {
		case <-gate.entered:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		h.db.LogtailMgr.GCByTS(ctx, published)
		close(gate.resume)
		var outcome probeResult
		select {
		case outcome = <-done:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		require.ErrorContains(t, outcome.err, "crossed checkpoint coverage")
		require.False(t, moerr.IsMoErrCode(outcome.err, moerr.ErrChangeWatchHistoryUnavailable))
		require.Empty(t, outcome.resp)
	})
}
