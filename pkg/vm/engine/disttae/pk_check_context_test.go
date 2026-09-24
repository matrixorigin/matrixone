// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disttae

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// Reuse the ready-subscription fixture and persist just one row. No cluster,
// checkpoint replay, or concurrent DDL is needed to reach the persisted boundary.
func newPersistedPKCheckTable(t *testing.T, oid types.T, tombstone bool) (*txnTable, *Engine, fileservice.FileService, *batch.Batch) {
	t.Helper()
	tbl, eng := newPrimaryKeyCheckTableForTest(t)
	proc := tbl.proc.Load()
	mp := proc.Mp()
	t.Cleanup(func() { proc.GetFileService().Close(context.Background()) })
	fs, err := fileservice.Get[fileservice.FileService](proc.GetFileService(), defines.SharedFileServiceName)
	require.NoError(t, err)
	eng.fs, eng.mp = fs, mp
	if tombstone && oid == types.T_varchar {
		tbl.accountId, tbl.tableId, tbl.tableName = 0, catalog.MO_TABLES_ID, catalog.MO_TABLES
		tbl.db.databaseId, tbl.db.databaseName = catalog.MO_CATALOG_ID, catalog.MO_CATALOG
	}
	tbl.primaryIdx = 0
	tbl.tableDef = &plan.TableDef{
		Cols: []*plan.ColDef{{Name: "pk", Typ: plan.Type{Id: int32(oid)}}},
		Pkey: &plan.PrimaryKeyDef{PkeyColName: "pk"}, Name2ColIndex: map[string]int32{"pk": 0},
	}
	tbl.db.op.AddWorkspace(&Transaction{engine: eng, proc: proc})
	data := batch.NewWithSize(3)
	defer data.Clean(mp)
	pk := vector.NewVec(oid.ToType())
	ts := vector.NewVec(types.T_TS.ToType())
	var writer *ioutil.BlockWriter
	if tombstone {
		data.Vecs[0], data.Vecs[1], data.Vecs[2] = vector.NewVec(types.T_Rowid.ToType()), pk, ts
		require.NoError(t, vector.AppendFixed(data.Vecs[0], types.RandomRowid(), false, mp))
		writer = ioutil.ConstructTombstoneWriter(objectio.HiddenColumnSelection_CommitTS, fs)
	} else {
		data.Vecs[0], data.Vecs[1], data.Vecs[2] = pk, ts, vector.NewVec(types.T_bool.ToType())
		require.NoError(t, vector.AppendFixed(data.Vecs[2], false, false, mp))
		writer = ioutil.ConstructWriter(0, []uint16{0, objectio.SEQNUM_COMMITTS, objectio.SEQNUM_ABORT}, 0, true, false, fs)
	}
	if oid == types.T_varchar {
		require.NoError(t, vector.AppendBytes(pk, []byte("target"), false, mp))
	} else {
		require.NoError(t, vector.AppendFixed(pk, int64(7), false, mp))
	}
	require.NoError(t, vector.AppendFixed(ts, types.BuildTS(20, 0), false, mp))
	data.SetRowCount(1)
	_, err = writer.WriteBatch(data)
	require.NoError(t, err)
	_, _, err = writer.Sync(proc.Ctx)
	require.NoError(t, err)
	part := eng.GetOrCreateLatestPart(proc.Ctx, uint64(tbl.accountId), tbl.db.databaseId, tbl.tableId)
	state, done := part.MutateState()
	state.UpdateDuration(types.TS{}, types.MaxTs())
	err = state.HandleObjectEntry(proc.Ctx, fs, objectio.ObjectEntry{
		ObjectStats: writer.GetObjectStats(), CreateTime: types.BuildTS(40, 0),
	}, tombstone)
	done()
	require.NoError(t, err)
	eng.pClient.SetSubscribeState(tbl.db.databaseId, tbl.tableId, Subscribed)
	keys := batch.NewWithSize(1)
	t.Cleanup(func() { keys.Clean(mp) })
	keys.Vecs[0], err = pk.Dup(mp)
	require.NoError(t, err)
	keys.SetRowCount(1)
	return tbl, eng, fs, keys
}

func TestPersistedPKCheckUsesCallerContext(t *testing.T) {
	for _, tombstone := range []bool{false, true} {
		for _, oid := range []types.T{types.T_varchar, types.T_int64} {
			t.Run(fmt.Sprintf("tombstone=%t/%s", tombstone, oid), func(t *testing.T) {
				tbl, eng, fs, keys := newPersistedPKCheckTable(t, oid, tombstone)
				proc := tbl.proc.Load()
				from, to := types.BuildTS(10, 0), types.BuildTS(50, 0)
				if !tombstone {
					t.Run("metadata-read", func(t *testing.T) {
						ctx, cancel := context.WithCancel(proc.Ctx)
						defer cancel()
						readCalled := false
						var readContextErr error
						eng.fs = tombstoneReadFS{FileService: fs, read: func(ioCtx context.Context, v *fileservice.IOVector) error {
							readCalled = true
							cancel()
							readContextErr = ioCtx.Err()
							return fs.Read(ioCtx, v)
						}}
						defer func() { eng.fs = fs }()
						changed, err := tbl.PrimaryKeysMayBeModified(ctx, from, to, keys, 0, -1)
						require.True(t, readCalled)
						require.ErrorIs(t, readContextErr, context.Canceled)
						require.ErrorIs(t, err, context.Canceled)
						require.False(t, changed)
						require.Empty(t, pkCheckSemaphore)
					})
				}
				methods := []string{"modified"}
				if !tombstone {
					methods = append(methods, "upserted")
				}
				for _, method := range methods {
					t.Run(method, func(t *testing.T) {
						check := func(ctx context.Context) (bool, error) {
							if method == "upserted" {
								return tbl.PrimaryKeysMayBeUpserted(ctx, from, to, keys, 0)
							}
							return tbl.PrimaryKeysMayBeModified(ctx, from, to, keys, 0, -1)
						}
						ctx, cancel := context.WithCancel(proc.Ctx)
						defer cancel()
						readCalled := false
						var readContextErr error
						eng.fs = tombstoneReadFS{FileService: fs, read: func(ioCtx context.Context, v *fileservice.IOVector) error {
							if len(pkCheckSemaphore) > 0 {
								// Cancel only after admission at the actual block read,
								// not during metadata selection or subscription readiness.
								readCalled = true
								cancel()
								readContextErr = ioCtx.Err()
							}
							return fs.Read(ioCtx, v)
						}}
						defer func() { eng.fs = fs }()
						baseline := proc.Mp().CurrNB()
						changed, err := check(ctx)
						require.True(t, readCalled, "must reach persisted block I/O")
						require.ErrorIs(t, readContextErr, context.Canceled)
						require.ErrorIs(t, err, context.Canceled)
						require.False(t, changed, "cancellation is not a metadata conflict")
						require.NoError(t, proc.Ctx.Err(), "the transaction process must remain alive")
						require.Equal(t, baseline, proc.Mp().CurrNB())
						require.Empty(t, pkCheckSemaphore)
						eng.fs = fs
						changed, err = check(proc.Ctx)
						require.NoError(t, err)
						require.True(t, changed, "a subsequent call must still detect the real committed PK")
						require.Equal(t, baseline, proc.Mp().CurrNB())
						// Cache-backed/synchronous readers may complete despite a
						// concurrent cancellation. It must still not become a retry.
						finishingCtx, finishCancel := context.WithCancel(proc.Ctx)
						defer finishCancel()
						eng.fs = tombstoneReadFS{FileService: fs, read: func(ioCtx context.Context, v *fileservice.IOVector) error {
							if len(pkCheckSemaphore) > 0 {
								finishCancel()
							}
							return fs.Read(context.WithoutCancel(ioCtx), v)
						}}
						changed, err = check(finishingCtx)
						require.ErrorIs(t, err, context.Canceled)
						require.False(t, changed)
						require.NoError(t, proc.Ctx.Err())
						require.Empty(t, pkCheckSemaphore)
						require.Equal(t, baseline, proc.Mp().CurrNB())
					})
				}
			})
		}
	}
}

func TestPersistedPKCheckCallerCancelsPermitWait(t *testing.T) {
	tbl, _, _, keys := newPersistedPKCheckTable(t, types.T_varchar, true)
	proc := tbl.proc.Load()
	guard, stop := context.WithTimeout(proc.Ctx, 10*time.Second)
	defer stop()
	ctx, cancel := context.WithCancel(guard)
	defer cancel()
	entered := make(chan struct{})
	checkCtx := &tombstoneWaitContext{Context: ctx, entered: entered}
	for i := 0; i < cap(pkCheckSemaphore); i++ {
		pkCheckSemaphore <- struct{}{}
	}
	var releaseOnce sync.Once
	releaseHeld := func() {
		releaseOnce.Do(func() {
			for i := 0; i < cap(pkCheckSemaphore); i++ {
				releasePKCheckSemaphore()
			}
		})
	}
	defer releaseHeld()
	baseline := proc.Mp().CurrNB()
	done := make(chan error, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		_, err := tbl.PrimaryKeysMayBeModified(checkCtx, types.BuildTS(10, 0), types.BuildTS(50, 0), keys, 0, -1)
		done <- err
	}()
	defer func() { cancel(); releaseHeld(); <-finished }()
	select {
	case <-entered:
	case <-guard.Done():
		t.Fatal("PK check did not reach admission", guard.Err())
	}
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-guard.Done():
		t.Fatal("child cancellation did not release the waiting check", guard.Err())
	}
	<-finished
	require.NoError(t, proc.Ctx.Err())
	require.Len(t, pkCheckSemaphore, cap(pkCheckSemaphore), "cancelled waiter must not consume another operation's permit")
	require.Equal(t, baseline, proc.Mp().CurrNB())
	releaseHeld()
	changed, err := tbl.PrimaryKeysMayBeModified(proc.Ctx, types.BuildTS(10, 0), types.BuildTS(50, 0), keys, 0, -1)
	require.NoError(t, err)
	require.True(t, changed)
	require.Empty(t, pkCheckSemaphore)
}
