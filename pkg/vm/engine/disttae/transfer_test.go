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

package disttae

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	rt "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestTombstoneTransferCandidates(t *testing.T) {
	bat := batch.NewWithSize(0)
	bat.SetRowCount(1)
	empty := batch.NewWithSize(0)
	base := Entry{typ: DELETE, bat: bat, tableId: catalog.MO_COLUMNS_ID,
		databaseId: catalog.MO_CATALOG_ID, databaseName: catalog.MO_CATALOG,
		tableName: catalog.MO_COLUMNS_UPDATE}
	txn := &Transaction{}
	require.Nil(t, txn.collectTombstoneTransferTablesLocked())
	for _, modify := range []func(*Entry){
		func(e *Entry) { e.typ = INSERT },
		func(e *Entry) { e.bat = nil },
		func(e *Entry) { e.bat = empty },
		func(e *Entry) { e.skipTransfer = true },
	} {
		e := base
		modify(&e)
		txn.writes = append(txn.writes, e)
	}
	require.Nil(t, txn.collectTombstoneTransferTablesLocked())
	txn.writes = append(txn.writes, base, base)
	persisted := base
	persisted.fileName = "tombstone"
	persisted.tableName = catalog.MO_COLUMNS
	txn.writes = append(txn.writes, persisted)
	for _, modify := range []func(*Entry){
		func(e *Entry) { e.accountId++ },
		func(e *Entry) { e.databaseId++ },
		func(e *Entry) { e.tableId++ },
	} {
		e := base
		modify(&e)
		txn.writes = append(txn.writes, e)
	}
	tables := txn.collectTombstoneTransferTablesLocked()
	require.Len(t, tables, 4)
	key := tombstoneTransferKey{base.accountId, base.databaseId, base.tableId}
	mixed := tables[key]
	require.Equal(t, [2]bool{true, true}, mixed.hasDeletes)
	require.Equal(t, [2]string{catalog.MO_COLUMNS_UPDATE, catalog.MO_COLUMNS}, mixed.writeNames)
	require.Equal(t, catalog.MO_COLUMNS, physicalCatalogTableName(key.databaseId, key.tableId, mixed.writeNames[0]))
	// Appending/replacing workspace entries cannot invalidate collected metadata.
	txn.writes = nil
	require.Equal(t, catalog.MO_CATALOG, mixed.databaseName)
}

func TestTombstoneTransferCallbacks(t *testing.T) {
	txn := &Transaction{}
	tables := map[tombstoneTransferKey]tombstoneTransferTable{}
	for id := uint64(1); id <= 3; id++ {
		tables[tombstoneTransferKey{tableId: id}] = tombstoneTransferTable{
			table: &txnTable{tableId: id}, hasDeletes: [2]bool{true, true},
			writeNames: [2]string{"memory", "persisted"},
		}
	}
	txn.Lock()
	defer txn.Unlock()
	seen := make(map[uint64]*txnTable)
	require.NoError(t, txn.forEachTableHasDeletesLocked(tables, 0, func(tbl *txnTable, name string) error {
		require.Equal(t, "memory", name)
		seen[tbl.tableId] = tbl
		return nil
	}))
	require.NoError(t, txn.forEachTableHasDeletesLocked(tables, 1, func(tbl *txnTable, name string) error {
		require.Len(t, seen, 3)
		require.Same(t, seen[tbl.tableId], tbl)
		require.Equal(t, "persisted", name)
		return nil
	}))
	failure := errors.New("transfer failed")
	calls := 0
	require.ErrorIs(t, txn.forEachTableHasDeletesLocked(tables, 0, func(*txnTable, string) error {
		calls++
		return failure
	}), failure)
	require.Equal(t, 1, calls)
}

func TestTombstoneTransferLookupReentry(t *testing.T) {
	enableReenterSnapshotOffsetFault(t)
	txn := &Transaction{engine: &Engine{}, op: transferBenchmarkOperator{},
		proc: &process.Process{Ctx: context.Background()}}
	tables := map[tombstoneTransferKey]tombstoneTransferTable{
		{tableId: 42}: {databaseName: "db", hasDeletes: [2]bool{true}, writeNames: [2]string{"tbl"}},
	}
	txn.Lock()
	err := txn.forEachTableHasDeletesLocked(tables, 0, func(*txnTable, string) error {
		t.Fatal("callback must not run after lookup failure")
		return nil
	})
	require.ErrorContains(t, err, "reenter snapshot write offset")
	require.False(t, txn.TryLock(), "lookup failure must return with workspace locked")
	txn.Unlock()
	require.True(t, txn.TryLock(), "no lock may remain after caller unlocks")
	txn.Unlock()
}

func TestTombstoneTransferUsesOpenedRelation(t *testing.T) {
	for _, name := range []string{"opened relation", "other account", "other database", "deleted relation", "deleted database", "recreated database", "removed workspace", "canceled lookup"} {
		t.Run(name, func(t *testing.T) {
			lookupErr := errors.New("internal SQL is unavailable during commit")
			calls := 0
			sql := executor.NewMemExecutor(func(string) (executor.Result, error) {
				calls++
				return executor.Result{}, lookupErr
			})
			eng, db, txn, _ := newResultOwnershipFixture(t, sql, false)
			catalogCache := cache.NewCatalog()
			catalogCache.UpdateDuration(types.BuildTS(200, 0), types.MaxTs())
			eng.catalog.Store(catalogCache)
			txn.op.(*mock_frontend.MockTxnOperator).EXPECT().SnapshotTS().Return(timestamp.Timestamp{PhysicalTime: 100}).AnyTimes()
			origin := &txnTable{eng: eng, db: db, accountId: 1, tableId: 42, tableName: "tbl"}
			origin.proc.Store(txn.proc)
			key := genTableKey(1, "tbl", 7, "db")
			txn.tableCache.Store(key, &txnTableDelegate{origin: origin})
			lookup := tombstoneTransferKey{accountId: 1, databaseId: 7, tableId: 42}
			switch name {
			case "other account":
				lookup.accountId++
			case "other database":
				lookup.databaseId++
			case "deleted relation":
				txn.tableOps.addDeleteTable(key, 1, origin.tableId)
			case "deleted database", "recreated database":
				databaseKey := genDatabaseKey(1, "db")
				txn.databaseOps.addDeleteDatabase(databaseKey, 1, 7)
				if name == "recreated database" {
					txn.databaseOps.addCreateDatabase(databaseKey, 2, &txnDatabase{
						op: txn.op, accountId: 1, databaseId: 8, databaseName: "db",
					})
				}
			case "removed workspace":
				txn.removed = true
			case "canceled lookup":
				canceled, cancel := context.WithCancel(t.Context())
				cancel()
				txn.proc.Ctx = canceled
			}
			tables := map[tombstoneTransferKey]tombstoneTransferTable{
				lookup: {databaseName: "db", hasDeletes: [2]bool{true, true}, writeNames: [2]string{"tbl", "tbl"}},
			}
			txn.Lock()
			defer txn.Unlock()
			callbacks := 0
			err := txn.forEachTableHasDeletesLocked(tables, 0, func(got *txnTable, writeName string) error {
				callbacks++
				require.Same(t, origin, got)
				require.Equal(t, "tbl", writeName)
				return nil
			})
			if name != "opened relation" {
				switch name {
				case "deleted relation", "recreated database":
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrNoSuchTable), err)
					require.Zero(t, calls)
				case "deleted database":
					require.ErrorIs(t, err, moerr.GetOkExpectedEOB())
				case "removed workspace":
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnClosed), err)
				case "canceled lookup":
					require.ErrorIs(t, err, context.Canceled)
				default:
					require.ErrorIs(t, err, lookupErr)
				}
				require.Zero(t, callbacks)
				return
			}
			require.NoError(t, err)
			require.Equal(t, 1, callbacks)
			require.NoError(t, txn.forEachTableHasDeletesLocked(tables, 1, func(got *txnTable, _ string) error {
				require.Same(t, origin, got)
				return nil
			}))
			require.Zero(t, calls)
		})
	}
}

// Empty/insert-only workspaces must not require a process or SHARED FS.
func TestTombstoneTransferWithoutDeletes(t *testing.T) {
	txn := &Transaction{op: transferBenchmarkOperator{}, writes: []Entry{{typ: INSERT}}}
	txn.Lock()
	defer txn.Unlock()
	require.NoError(t, txn.transferTombstones(context.Background()))
}

// Record actual prefetch completions; metadata reads deliberately fail before
// row relocation, so this control needs no real object files.
type transferPrefetchFS struct {
	fileservice.FileService
	calls   chan string
	readErr error
}

func (fs *transferPrefetchFS) PrefetchFile(_ context.Context, name string) error {
	fs.calls <- name
	return nil
}
func (fs *transferPrefetchFS) Read(context.Context, *fileservice.IOVector) error { return fs.readErr }

func TestTransferTargetsRequireWorkspaceIntent(t *testing.T) {
	ctx := context.Background()
	proc := testutil.NewProc(t)
	state := logtailreplay.NewPartitionState("", false, 0x3fff, false)
	makeObject := func(created, deleted int64) objectio.ObjectEntry {
		id := types.NewObjectid()
		stats := objectio.NewObjectStatsWithObjectID(&id, false, true, false)
		require.NoError(t, objectio.SetObjectStatsSize(stats, 1024))
		require.NoError(t, objectio.SetObjectStatsRowCnt(stats, 1))
		require.NoError(t, objectio.SetObjectStatsBlkCnt(stats, 1))
		entry := objectio.ObjectEntry{ObjectStats: *stats, CreateTime: types.BuildTS(created, 0)}
		if deleted != 0 {
			entry.DeleteTime = types.BuildTS(deleted, 0)
		}
		require.NoError(t, state.HandleObjectEntry(ctx, nil, entry, false))
		return entry
	}
	stable := makeObject(5, 0)
	var source objectio.ObjectEntry
	for range 10 {
		source = makeObject(11, 15)
		makeObject(16, 0)
	}
	deleted, created := state.GetChangedObjsBetweenForTombstoneTransfer(types.BuildTS(10, 0), types.BuildTS(20, 0))
	require.Len(t, deleted, 10)
	require.Len(t, created, 10)
	require.NotContains(t, deleted, *stable.ObjectShortName())
	for _, name := range []string{"unrelated merges", "no target lookup", "matching intent"} {
		t.Run(name, func(t *testing.T) {
			// Isolate the existing pipeline runtime; any no-demand prefetch fails
			// synchronously at its lookup instead of racing an async zero-call check.
			previous := rt.ServiceRuntime("")
			runtime := rt.NewRuntime(metadata.ServiceType_CN, "", zap.NewNop())
			rt.SetupServiceBasedRuntime("", runtime)
			t.Cleanup(func() { rt.SetupServiceBasedRuntime("", previous) })
			runtime.SetGlobalVariables("blockio", "unexpected prefetch")
			fs := &transferPrefetchFS{FileService: proc.GetFileService(), calls: make(chan string, 10), readErr: errors.New("metadata read failed")}
			bat := batch.NewWithSize(2)
			t.Cleanup(func() { bat.Clean(proc.Mp()) })
			bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
			bat.Vecs[1] = vector.NewVec(types.T_int32.ToType())
			obj := stable
			if name == "matching intent" {
				obj = source
			}
			block := objectio.NewBlockidWithObjectID(obj.ObjectName().ObjectId(), 0)
			rowID := types.NewRowid(&block, 0)
			require.NoError(t, vector.AppendFixed(bat.Vecs[0], rowID, false, proc.Mp()))
			require.NoError(t, vector.AppendFixed(bat.Vecs[1], int32(7), false, proc.Mp()))
			bat.SetRowCount(1)
			txn := &Transaction{writes: []Entry{{typ: DELETE, tableId: 42, bat: bat}}}
			op := newTxnOperatorForTestWithWorkspace(t, txn)
			tbl := &txnTable{tableId: 42, db: &txnDatabase{op: op}, tableDef: &plan.TableDef{
				Name: "prefetch_intent", Cols: []*plan.ColDef{{Name: "pk", Typ: plan.Type{Id: int32(types.T_int32)}}},
				Pkey: &plan.PrimaryKeyDef{PkeyColName: "pk"}, Name2ColIndex: map[string]int32{"pk": 0},
			}}
			tbl.proc.Store(proc)
			partition := state
			if name == "no target lookup" {
				partition = nil
			}
			before := proc.Mp().CurrNB()
			if name == "matching intent" {
				require.True(t, runtime.CompareAndDeleteGlobalVariables("blockio", "unexpected prefetch"))
				ioutil.Start("")
				t.Cleanup(func() { ioutil.Stop("") })
				op.EXPECT().SnapshotTS().Return(timestamp.Timestamp{}).AnyTimes()
				require.ErrorIs(t, transferTombstones(ctx, tbl, partition, deleted, created, proc.Mp(), fs), fs.readErr)
				deadline := time.NewTimer(5 * time.Second)
				defer deadline.Stop()
				seen := make(map[string]struct{})
				for range 10 {
					select {
					case file := <-fs.calls:
						seen[file] = struct{}{}
					case <-deadline.C:
						t.Fatal("prefetch callbacks did not complete")
					}
				}
				require.Len(t, seen, 10)
			} else {
				require.NoError(t, transferTombstones(ctx, tbl, partition, deleted, created, proc.Mp(), fs))
				require.Empty(t, fs.calls)
			}
			require.Equal(t, before, proc.Mp().CurrNB(), "transfer scratch must be freed on success and read failure")
			require.Equal(t, rowID, vector.MustFixedColWithTypeCheck[types.Rowid](bat.Vecs[0])[0])
		})
	}
}
