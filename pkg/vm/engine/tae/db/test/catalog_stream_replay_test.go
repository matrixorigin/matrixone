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

package test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/stretchr/testify/require"
)

// Exercise the actual system-table scan and checkpoint restart with columns
// spanning blocks and persisted tombstones. Verify both schema and user data.
func TestCatalogReplayStreamsPersistedSystemTables(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	tae := testutil.NewTestEngine(ctx, ModuleName, t, config.WithLongScanAndCKPOpts(nil))
	defer tae.Close()

	txn, err := tae.StartTxn(nil)
	require.NoError(t, err)
	_, err = testutil.CreateDatabase2(ctx, txn, "stream_replay")
	require.NoError(t, err)
	require.NoError(t, txn.Commit(ctx))

	const tableCount = 80
	const droppedTable = 0
	const columns = 128
	var savedKey int32
	for first := 0; first < tableCount; first += 10 {
		txn, err = tae.StartTxn(nil)
		require.NoError(t, err)
		db, e := txn.GetDatabase("stream_replay")
		require.NoError(t, e)
		for i := first; i < first+10; i++ {
			schema := catalog.MockSchema(columns, 0)
			schema.Name = fmt.Sprintf("table_%03d", i)
			rel, e := testutil.CreateRelation2(ctx, txn, db, schema)
			require.NoError(t, e)
			if i == tableCount-1 {
				bat := catalog.MockBatch(schema, 1)
				savedKey = vector.GetFixedAtNoTypeCheck[int32](bat.GetVectorByName("mock_0").GetDownstreamVector(), 0)
				e = rel.Append(ctx, bat)
				bat.Close()
				require.NoError(t, e)
			}
		}
		require.NoError(t, txn.Commit(ctx))
	}
	require.NoError(t, tae.DB.ForceFlush(ctx, tae.TxnMgr.Now()))
	oldSnapshot, err := tae.StartTxn(nil)
	require.NoError(t, err)

	txn, err = tae.StartTxn(nil)
	require.NoError(t, err)
	db, err := txn.GetDatabase("stream_replay")
	require.NoError(t, err)
	// One committed drop exercises persisted tombstones and both snapshots.
	// Each additional drop looks up every column in the persisted catalog.
	require.NoError(t, testutil.DropRelation2(ctx, txn, db, fmt.Sprintf("table_%03d", droppedTable)))
	require.NoError(t, txn.Commit(ctx))
	require.NoError(t, tae.DB.ForceFlush(ctx, tae.TxnMgr.Now()))

	// The column catalog must really cross blocks; otherwise this would not
	// exercise the block ownership and cross-block association being changed.
	txn, err = tae.StartTxn(nil)
	require.NoError(t, err)
	catDB, err := tae.Catalog.GetDatabaseByID(pkgcatalog.MO_CATALOG_ID)
	require.NoError(t, err)
	colTable, err := catDB.GetTableEntryByID(pkgcatalog.MO_COLUMNS_ID)
	require.NoError(t, err)
	countColumns := func(readTxn txnif.AsyncTxn) (int, int, error) {
		blocks := make(map[*containers.Batch]struct{})
		visible := 0
		err := tables.ScanSysTable(ctx, colTable, readTxn, func(b *containers.Batch, row int) error {
			blocks[b] = struct{}{}
			if b.GetVectorByName(pkgcatalog.SystemColAttr_DBName).GetDownstreamVector().GetStringAt(row) == "stream_replay" {
				visible++
			}
			return nil
		})
		return visible, len(blocks), err
	}
	visibleColumns, blockCount, err := countColumns(txn)
	require.NoError(t, err)
	require.Greater(t, blockCount, 1)
	require.Equal(t, (tableCount-1)*(columns+1), visibleColumns)
	oldColumns, _, err := countColumns(oldSnapshot)
	require.NoError(t, err)
	require.Equal(t, tableCount*(columns+1), oldColumns)
	require.NoError(t, oldSnapshot.Commit(ctx))
	visitError := errors.New("stop visiting")
	err = tables.ScanSysTable(ctx, colTable, txn, func(*containers.Batch, int) error { return visitError })
	require.ErrorIs(t, err, visitError)
	cancelled, cancelScan := context.WithCancelCause(ctx)
	cancelCause := errors.New("replay cancelled")
	cancelScan(cancelCause)
	err = tables.ScanSysTable(cancelled, colTable, txn, func(*containers.Batch, int) error {
		t.Fatal("visitor invoked after cancellation")
		return nil
	})
	require.ErrorIs(t, err, cancelCause)
	require.NoError(t, txn.Commit(ctx))

	// A transaction's own uncommitted deletes must also be reflected, without
	// hiding the same table after the transaction rolls back.
	txn, err = tae.StartTxn(nil)
	require.NoError(t, err)
	db, err = txn.GetDatabase("stream_replay")
	require.NoError(t, err)
	require.NoError(t, testutil.DropRelation2(ctx, txn, db, fmt.Sprintf("table_%03d", droppedTable+1)))
	workspaceColumns, _, err := countColumns(txn)
	require.NoError(t, err)
	require.Equal(t, (tableCount-2)*(columns+1), workspaceColumns)
	require.NoError(t, txn.Rollback(ctx))

	require.NoError(t, tae.DB.ForceGlobalCheckpoint(ctx, tae.TxnMgr.Now(), 0))
	tae.Restart(ctx)
	txn, err = tae.StartTxn(nil)
	require.NoError(t, err)
	db, err = txn.GetDatabase("stream_replay")
	require.NoError(t, err)
	for i := 0; i < tableCount; i++ {
		rel, e := db.GetRelationByName(fmt.Sprintf("table_%03d", i))
		if i == droppedTable {
			require.Error(t, e)
			continue
		}
		require.NoError(t, e)
		schema := rel.Schema(false).(*catalog.Schema)
		require.Len(t, schema.ColDefs, columns+1) // includes the physical row ID
		require.Equal(t, "mock_0", schema.ColDefs[0].Name)
		require.Equal(t, "mock_127", schema.ColDefs[127].Name)
		if i == tableCount-1 {
			_, _, e = rel.GetByFilter(ctx, handle.NewEQFilter(savedKey))
			require.NoError(t, e)
		}
	}
	require.NoError(t, txn.Commit(ctx))
}

type failingCatalogRead struct {
	fileservice.FileService
	object string
	err    error
	fail   atomic.Bool
}

func (fs *failingCatalogRead) Read(ctx context.Context, vec *fileservice.IOVector) error {
	if fs.fail.Load() && strings.Contains(vec.FilePath, fs.object) {
		return fs.err
	}
	return fs.FileService.Read(ctx, vec)
}

func TestCatalogReplayReadFailureReleasesPools(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	tae := testutil.NewTestEngine(ctx, ModuleName, t, config.WithLongScanAndCKPOpts(nil))
	closed := false
	defer func() {
		if !closed {
			_ = tae.Close()
		}
	}()
	txn, err := tae.StartTxn(nil)
	require.NoError(t, err)
	dbh, err := testutil.CreateDatabase2(ctx, txn, "read_failure")
	require.NoError(t, err)
	for i := 0; i < 50; i++ {
		schema := catalog.MockSchema(32, 0)
		schema.Name = fmt.Sprintf("table_%03d", i)
		_, err = testutil.CreateRelation2(ctx, txn, dbh, schema)
		require.NoError(t, err)
	}
	require.NoError(t, txn.Commit(ctx))
	require.NoError(t, tae.DB.ForceFlush(ctx, tae.TxnMgr.Now()))
	require.NoError(t, tae.DB.ForceGlobalCheckpoint(ctx, tae.TxnMgr.Now(), 0))

	txn, err = tae.StartTxn(nil)
	require.NoError(t, err)
	catDB, err := tae.Catalog.GetDatabaseByID(pkgcatalog.MO_CATALOG_ID)
	require.NoError(t, err)
	columns, err := catDB.GetTableEntryByID(pkgcatalog.MO_COLUMNS_ID)
	require.NoError(t, err)
	it := columns.MakeDataVisibleObjectIt(txn)
	require.True(t, it.Next())
	object := it.Item().ObjectStats.ObjectName().String()
	it.Release()
	require.NoError(t, txn.Commit(ctx))
	dir := tae.Dir
	require.NoError(t, tae.Close())
	closed = true

	ioutil.Start("")
	defer ioutil.Stop("")
	opts := config.WithLongScanAndCKPOpts(nil).FillDefaults(dir)
	want := errors.New("catalog column read failed")
	fs := &failingCatalogRead{FileService: opts.Fs, object: object, err: want}
	fs.fail.Store(true)
	opts.Fs = fs
	defer opts.Fs.Close(ctx)
	defer opts.LocalFs.Close(ctx)
	defer opts.TmpFs.Close(ctx)
	for i := 0; i < 3; i++ {
		failed, openErr := db.Open(ctx, dir, opts)
		require.ErrorIs(t, openErr, want)
		require.NotNil(t, failed)
		require.Zero(t, failed.Runtime.VectorPool.Small.Allocated())
		require.Zero(t, failed.Runtime.VectorPool.Transient.Allocated())
	}

	fs.fail.Store(false)
	reopened, err := db.Open(ctx, dir, opts)
	require.NoError(t, err)
	reopenedClosed := false
	defer func() {
		if !reopenedClosed {
			_ = reopened.Close()
		}
	}()
	txn, err = reopened.StartTxn(nil)
	require.NoError(t, err)
	dbh, err = txn.GetDatabase("read_failure")
	require.NoError(t, err)
	_, err = dbh.GetRelationByName("table_049")
	require.NoError(t, err)
	require.NoError(t, txn.Commit(ctx))

	// Closing the DB must not invalidate a vector still owned by a caller.
	typ := types.T_int64.ToType()
	held := reopened.Runtime.VectorPool.Small.GetVector(&typ)
	defer held.Close()
	held.Append(int64(42), false)
	err = reopened.Close()
	reopenedClosed = true
	require.NoError(t, err)
	require.Equal(t, int64(42), held.Get(0))
	held.Close()
	require.Zero(t, reopened.Runtime.VectorPool.Small.Allocated())
	require.Zero(t, reopened.Runtime.VectorPool.Transient.Allocated())
}
