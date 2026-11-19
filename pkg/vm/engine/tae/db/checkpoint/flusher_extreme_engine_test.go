// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package checkpoint_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/stretchr/testify/require"
)

func TestPartialFlushBusyTombstoneKeepsDeleteAcrossRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	engine := testutil.NewTestEngineWithDir(ctx, t.TempDir(), t, config.WithLongScanAndCKPOpts(nil))
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	engine.BGFlusher.Stop()

	schema := catalog.MockSchema(2, 0)
	schema.Extra.BlockMaxRows = 2
	engine.BindSchema(schema)
	rows := catalog.MockBatch(schema, 3)
	t.Cleanup(rows.Close)

	setupTxn, finishSetup := flushProgressTxn(t, ctx, engine)
	db, err := setupTxn.CreateDatabase(testutil.DefaultTestDB, "", "")
	require.NoError(t, err)
	rel, err := db.CreateRelation(schema)
	require.NoError(t, err)
	table := rel.GetMeta().(*catalog.TableEntry)
	require.NoError(t, rel.Append(ctx, rows))
	require.NoError(t, setupTxn.Commit(ctx))
	finishSetup()

	deleteTxn, finishDelete := flushProgressTxn(t, ctx, engine)
	deleteRel := flushProgressRelation(t, deleteTxn, schema.Name)
	require.NoError(t, deleteRel.DeleteByFilter(ctx, handle.NewEQFilter(rows.Vecs[0].Get(0))))
	require.NoError(t, deleteTxn.Commit(ctx))
	finishDelete()

	var tombstones []*catalog.ObjectEntry
	func() {
		it := table.MakeTombstoneObjectIt()
		defer it.Release()
		for it.Next() {
			tombstones = append(tombstones, it.Item())
		}
	}()
	require.Len(t, tombstones, 1)
	tomb := tombstones[0]
	appender := table.GetTableData().GetHandle(true).SetAppender(tomb.AsCommonID())
	releaseAppender := sync.OnceFunc(appender.Close)
	t.Cleanup(releaseAppender)

	scheduler := &stagedFlushScheduler{TaskScheduler: engine.Runtime.Scheduler}
	rt := &dbutils.Runtime{
		Now: engine.Runtime.Now, Fs: engine.Runtime.Fs,
		LocalFs: engine.Runtime.LocalFs, TmpFS: engine.Runtime.TmpFS,
		TransferTable:   engine.Runtime.TransferTable,
		TransferDelsMap: engine.Runtime.TransferDelsMap,
		BigDeleteHinter: engine.Runtime.BigDeleteHinter,
		Options:         engine.Runtime.Options, Scheduler: scheduler,
		SyncProtectionValidator:  engine.Runtime.SyncProtectionValidator,
		UnpublishedObjectCleaner: engine.Runtime.UnpublishedObjectCleaner,
	}
	rt.VectorPool = engine.Runtime.VectorPool

	err = checkpoint.FireFlushTabletailForTest(rt, table, engine.TxnMgr.Now())
	require.True(t, moerr.IsMoErrCode(err, moerr.OkExpectedEOB), "got %v", err)
	require.Len(t, scheduler.admitted, 1)
	for _, scope := range scheduler.admitted[0].scopes {
		require.NotEqual(t, *tomb.AsCommonID(), scope)
	}
	scheduler.execute(t, ctx)
	latest, err := table.GetObjectByID(tomb.ID(), true)
	require.NoError(t, err)
	require.False(t, latest.HasDropCommitted())

	checkRows := func() {
		txn, finishTxn := flushProgressTxn(t, ctx, engine)
		readRel := flushProgressRelation(t, txn, schema.Name)
		testutil.CheckAllColRowsByScan(t, readRel, 2, true)
		for i := 0; i < 3; i++ {
			_, _, lookupErr := readRel.GetByFilter(ctx, handle.NewEQFilter(rows.Vecs[0].Get(i)))
			if i == 0 {
				require.True(t, moerr.IsMoErrCode(lookupErr, moerr.ErrNotFound), "deleted key: %v", lookupErr)
			} else {
				require.NoError(t, lookupErr, "key %d", i)
			}
		}
		require.NoError(t, txn.Commit(ctx))
		finishTxn()
	}
	checkRows()

	releaseAppender()
	require.NoError(t, checkpoint.FireFlushTabletailForTest(rt, table, engine.TxnMgr.Now()))
	require.Len(t, scheduler.admitted, 1)
	require.Contains(t, scheduler.admitted[0].scopes, *tomb.AsCommonID())
	scheduler.execute(t, ctx)
	latest, err = table.GetObjectByID(tomb.ID(), true)
	require.NoError(t, err)
	require.True(t, latest.HasDropCommitted())
	checkRows()
	engine.Restart(ctx)
	checkRows()
}
