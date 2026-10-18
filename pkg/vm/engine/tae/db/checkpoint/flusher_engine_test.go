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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/stretchr/testify/require"
)

type stagedFlush struct {
	scopes  []common.ID
	factory tasks.TxnTaskFactory
}

type stagedFlushScheduler struct {
	tasks.TaskScheduler
	admitted []stagedFlush
}

func (s *stagedFlushScheduler) ScheduleMultiScopedTxnTask(
	_ *tasks.Context, taskType tasks.TaskType, scopes []common.ID, factory tasks.TxnTaskFactory,
) (tasks.Task, error) {
	if taskType != tasks.FlushTableTailTask {
		panic("unexpected task type")
	}
	s.admitted = append(s.admitted, stagedFlush{scopes: scopes, factory: factory})
	return nil, nil
}

func (s *stagedFlushScheduler) execute(t *testing.T, ctx context.Context) {
	t.Helper()
	// Invoke the actual captured factories only after the selector has returned.
	// This checks the payload, not just the scopes passed during admission.
	for _, admitted := range s.admitted {
		task, err := s.TaskScheduler.ScheduleMultiScopedTxnTask(
			tasks.WaitableCtx, tasks.FlushTableTailTask, admitted.scopes, admitted.factory,
		)
		require.NoError(t, err)
		require.NoError(t, task.WaitDone(ctx))
	}
	s.admitted = nil
}

func flushProgressTxn(t *testing.T, ctx context.Context, engine *testutil.TestEngine) (txnif.AsyncTxn, func()) {
	t.Helper()
	txn, err := engine.StartTxn(nil)
	require.NoError(t, err)
	finished := false
	t.Cleanup(func() {
		if !finished {
			require.NoError(t, txn.Rollback(ctx))
		}
	})
	txn.BindAccessInfo(0, 0, 0)
	return txn, func() { finished = true }
}

func flushProgressRelation(t *testing.T, txn txnif.AsyncTxn, name string) handle.Relation {
	t.Helper()
	db, err := txn.GetDatabase(testutil.DefaultTestDB)
	require.NoError(t, err)
	rel, err := db.GetRelationByName(name)
	require.NoError(t, err)
	return rel
}

func TestPartialFlushPreservesDeletesAndWriter(t *testing.T) {
	for _, finish := range []string{"commit", "rollback"} {
		t.Run(finish, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			t.Cleanup(cancel)
			engine := testutil.NewTestEngineWithDir(ctx, t.TempDir(), t, config.WithLongScanAndCKPOpts(nil))
			t.Cleanup(func() { require.NoError(t, engine.Close()) })
			engine.BGFlusher.Stop()
			schema := catalog.MockSchema(2, 0)
			schema.Extra.BlockMaxRows = 2
			engine.BindSchema(schema)
			rows := catalog.MockBatch(schema, 6)
			t.Cleanup(rows.Close)
			initial := rows.CloneWindow(0, 5)
			t.Cleanup(initial.Close)
			setupTxn, finishSetup := flushProgressTxn(t, ctx, engine)
			db, err := setupTxn.CreateDatabase(testutil.DefaultTestDB, "", "")
			require.NoError(t, err)
			rel, err := db.CreateRelation(schema)
			require.NoError(t, err)
			table := rel.GetMeta().(*catalog.TableEntry)
			require.NoError(t, rel.Append(ctx, initial))
			require.NoError(t, setupTxn.Commit(ctx))
			finishSetup()

			deleteTxn, finishDelete := flushProgressTxn(t, ctx, engine)
			deleteRel := flushProgressRelation(t, deleteTxn, schema.Name)
			readyID, _, err := deleteRel.GetByFilter(ctx, handle.NewEQFilter(rows.Vecs[0].Get(0)))
			require.NoError(t, err)
			busyID, _, err := deleteRel.GetByFilter(ctx, handle.NewEQFilter(rows.Vecs[0].Get(4)))
			require.NoError(t, err)
			require.NotEqual(t, *readyID.ObjectID(), *busyID.ObjectID())
			for _, i := range []int{0, 4} {
				require.NoError(t, deleteRel.DeleteByFilter(ctx, handle.NewEQFilter(rows.Vecs[0].Get(i))))
			}
			require.NoError(t, deleteTxn.Commit(ctx))
			finishDelete()
			var tombstoneMetas []*catalog.ObjectEntry
			func() {
				it := table.MakeTombstoneObjectIt()
				defer it.Release()
				for it.Next() {
					tombstoneMetas = append(tombstoneMetas, it.Item())
				}
			}()
			require.NotEmpty(t, tombstoneMetas)

			var sourceMetas []*catalog.ObjectEntry
			func() {
				it := table.MakeDataObjectIt()
				defer it.Release()
				for it.Next() {
					sourceMetas = append(sourceMetas, it.Item())
				}
			}()
			require.Len(t, sourceMetas, 3)
			var busy *catalog.ObjectEntry
			for _, source := range sourceMetas {
				if source.ID().EQ(busyID.ObjectID()) {
					busy = source
				}
			}
			require.NotNil(t, busy)
			appender := table.GetTableData().GetHandle(false).SetAppender(busy.AsCommonID())
			releaseAppender := sync.OnceFunc(appender.Close)
			t.Cleanup(releaseAppender)

			writer, finishWriter := flushProgressTxn(t, ctx, engine)
			writerRel := flushProgressRelation(t, writer, schema.Name)
			pending := rows.CloneWindow(5, 1)
			t.Cleanup(pending.Close)
			require.NoError(t, writerRel.Append(ctx, pending))

			scheduler := &stagedFlushScheduler{TaskScheduler: engine.Runtime.Scheduler}
			// Borrow the engine-owned resources without copying atomic counters or
			// replacing the scheduler of a runtime used by background goroutines.
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
			require.Len(t, scheduler.admitted[0].scopes, 2+len(tombstoneMetas))
			require.Contains(t, scheduler.admitted[0].scopes, *readyID)
			for _, scope := range scheduler.admitted[0].scopes {
				require.NotEqual(t, *busy.AsCommonID(), scope)
			}
			scheduler.execute(t, ctx)
			for _, source := range sourceMetas {
				latest, err := table.GetObjectByID(source.ID(), false)
				require.NoError(t, err)
				require.Equal(t, !source.ID().EQ(busy.ID()), latest.HasDropCommitted())
			}
			for _, source := range tombstoneMetas {
				latest, err := table.GetObjectByID(source.ID(), true)
				require.NoError(t, err)
				require.True(t, latest.HasDropCommitted())
			}

			checkKeys := func(wantPending bool) {
				txn, finishTxn := flushProgressTxn(t, ctx, engine)
				rel := flushProgressRelation(t, txn, schema.Name)
				wantRows := 3
				if wantPending {
					wantRows++
				}
				testutil.CheckAllColRowsByScan(t, rel, wantRows, true)
				for i := 0; i < 6; i++ {
					id, row, err := rel.GetByFilter(ctx, handle.NewEQFilter(rows.Vecs[0].Get(i)))
					if i == 0 || i == 4 || (i == 5 && !wantPending) {
						require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotFound), "key %v: %v", rows.Vecs[0].Get(i), err)
					} else {
						require.NoError(t, err, "key %v", rows.Vecs[0].Get(i))
						value, isNull, err := rel.GetValue(id, row, 1, false)
						require.NoError(t, err)
						require.False(t, isNull)
						require.Equal(t, rows.Vecs[1].Get(i), value)
					}
				}
				require.NoError(t, txn.Commit(ctx))
				finishTxn()
			}
			checkKeys(false)
			releaseAppender()
			if finish == "commit" {
				require.NoError(t, writer.Commit(ctx))
			} else {
				require.NoError(t, writer.Rollback(ctx))
			}
			finishWriter()
			require.NoError(t, checkpoint.FireFlushTabletailForTest(rt, table, engine.TxnMgr.Now()))
			require.NotEmpty(t, scheduler.admitted)
			scheduler.execute(t, ctx)
			checkKeys(finish == "commit")
			engine.Restart(ctx)
			checkKeys(finish == "commit")
		})
	}
}
