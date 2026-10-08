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

package disttae

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/require"
)

type loadCleanupFileService struct {
	fileservice.FileService
	err            error
	waitForContext bool
	deleteCalled   bool
	entered        chan struct{}
	release        chan struct{}
}

func (f *loadCleanupFileService) Delete(ctx context.Context, _ ...string) error {
	f.deleteCalled = true
	if f.entered != nil {
		close(f.entered)
	}
	if f.release != nil {
		select {
		case <-f.release:
			return f.err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if f.waitForContext {
		<-ctx.Done()
		return ctx.Err()
	}
	return f.err
}

func TestTableMetaReaderRecordsCloneObjectOwnership(t *testing.T) {
	tbl := newTxnTableForTest()
	txn := tbl.getTxn()
	txn.BindTxnOp(tbl.db.op)
	txn.engine.cloneTxnCache = newCloneTxnCache()
	txn.SetCloneTxn(1)

	stats := mockStatsList(t, 2)
	reader := &TableMetaReader{table: tbl}
	reader.addCloneSharedFile(&stats[0], cloneObjectFromCommittedState)
	reader.addCloneSharedFile(&stats[1], cloneObjectFromTxnWorkspace)

	txnID := txn.op.Txn().ID
	require.True(t, txn.engine.cloneTxnCache.IsSharedFile(txnID, stats[0].ObjectName().String()))
	require.False(t, txn.engine.cloneTxnCache.IsSharedFile(txnID, stats[1].ObjectName().String()))
	require.True(t, txn.engine.cloneTxnCache.IsTxnLocalSharedFile(txnID, stats[1].ObjectName().String()))
}

func TestNewImmutableTableMetaReader(t *testing.T) {
	tbl := newTxnTableForTest()
	txn := tbl.getTxn()
	txn.proc = testutil.NewProcess(t)
	txn.workspace = newTxnWorkspace()
	txn.BindTxnOp(tbl.db.op)
	require.NoError(t, txn.workspace.addCreatedTable(tbl.tableId))
	txn.engine.partitions = make(map[[2]uint64]*logtailreplay.Partition)
	txn.engine.cloneTxnCache = newCloneTxnCache()
	txn.SetCloneTxn(1)
	defer closeWorkspaceForTest(t, txn)
	stats := mockStatsList(t, 1)
	statsBat := cloneObjectStatsBatchForTest(t, txn.proc.Mp(), stats...)
	txn.appendWorkspaceEntryLocked(Entry{
		typ: INSERT, databaseId: tbl.db.databaseId, tableId: tbl.tableId,
		fileName: stats[0].ObjectName().String(), bat: statsBat,
	})
	reader, err := NewImmutableTableMetaReader(context.Background(), tbl, 7)
	require.NoError(t, err)
	metaReader := reader.(*TableMetaReader)
	require.True(t, metaReader.immutableOnly)
	require.Equal(t, 7, metaReader.maxObjects)
	tbl.tableDef = &plan.TableDef{
		Pkey:          &plan.PrimaryKeyDef{Names: []string{"a"}, PkeyColName: "a"},
		Cols:          []*plan.ColDef{{Name: "a", Typ: plan.Type{Id: int32(types.T_int64)}, Seqnum: 0}},
		Name2ColIndex: map[string]int32{"a": 0},
	}
	mp := mpool.MustNewZero()
	dataBat := colexec.AllocCNS3ResultBat(false)
	defer dataBat.Clean(mp)
	end, err := reader.Read(context.Background(), nil, nil, mp, dataBat)
	require.NoError(t, err)
	require.False(t, end)
	require.Equal(t, 1, dataBat.RowCount())
	tombstoneBat := colexec.AllocCNS3ResultBat(true)
	defer tombstoneBat.Clean(mp)
	end, err = reader.Read(context.Background(), nil, nil, mp, tombstoneBat)
	require.NoError(t, err)
	require.False(t, end)
	require.Zero(t, tombstoneBat.RowCount())
	end, err = reader.Read(context.Background(), nil, nil, mp, tombstoneBat)
	require.NoError(t, err)
	require.True(t, end)
	require.NoError(t, reader.Close())

	limited, err := NewImmutableTableMetaReader(context.Background(), tbl, 1)
	require.NoError(t, err)
	limited.(*TableMetaReader).objectCount = 1
	limitedBat := colexec.AllocCNS3ResultBat(false)
	defer limitedBat.Clean(mp)
	_, err = limited.Read(context.Background(), nil, nil, mp, limitedBat)
	require.Error(t, err)
	require.NoError(t, limited.Close())

	appendableStats := mockStatsList(t, 1)
	objectio.WithAppendable()(&appendableStats[0])
	appendableBat := cloneObjectStatsBatchForTest(t, txn.proc.Mp(), appendableStats...)
	txn.appendWorkspaceEntryLocked(Entry{
		typ: INSERT, databaseId: tbl.db.databaseId, tableId: tbl.tableId,
		fileName: appendableStats[0].ObjectName().String(), bat: appendableBat,
	})
	appendableReader, err := NewImmutableTableMetaReader(context.Background(), tbl, 7)
	require.NoError(t, err)
	appendableOut := colexec.AllocCNS3ResultBat(false)
	defer appendableOut.Clean(mp)
	_, err = appendableReader.Read(context.Background(), nil, nil, mp, appendableOut)
	require.Error(t, err)
	require.NoError(t, appendableReader.Close())
}

func TestImmutableTableMetaReaderDoesNotRetainCloneFilesAcrossStatements(t *testing.T) {
	tbl := newTxnTableForTest()
	txn := tbl.getTxn()
	txn.proc = testutil.NewProcess(t)
	txn.workspace = newTxnWorkspace()
	txn.BindTxnOp(tbl.db.op)
	require.NoError(t, txn.workspace.addCreatedTable(tbl.tableId))
	txn.engine.partitions = make(map[[2]uint64]*logtailreplay.Partition)
	txn.engine.cloneTxnCache = newCloneTxnCache()
	txn.SetCloneTxn(1)
	tbl.tableDef = &plan.TableDef{
		Pkey:          &plan.PrimaryKeyDef{Names: []string{"a"}, PkeyColName: "a"},
		Cols:          []*plan.ColDef{{Name: "a", Typ: plan.Type{Id: int32(types.T_int64)}, Seqnum: 0}},
		Name2ColIndex: map[string]int32{"a": 0},
	}

	stats := mockStatsList(t, 2)
	mp := mpool.MustNewZero()
	defer closeWorkspaceForTest(t, txn)
	for statement := range stats {
		statsBat := cloneObjectStatsBatchForTest(t, txn.proc.Mp(), stats[statement])
		txn.appendWorkspaceEntryLocked(Entry{
			typ: INSERT, databaseId: tbl.db.databaseId, tableId: tbl.tableId,
			fileName: stats[statement].ObjectName().String(), bat: statsBat,
		})

		txn.StartStatement()
		reader, err := NewImmutableTableMetaReader(context.Background(), tbl, 7)
		require.NoError(t, err)
		dataBat := colexec.AllocCNS3ResultBat(false)
		_, err = reader.Read(context.Background(), nil, nil, mp, dataBat)
		require.NoError(t, err)
		dataBat.Clean(mp)
		require.NoError(t, reader.Close())
		txn.EndStatement()

		for i := 0; i <= statement; i++ {
			name := stats[i].ObjectName().String()
			txnID := txn.op.Txn().ID
			require.False(t, txn.engine.cloneTxnCache.IsSharedFile(txnID, name))
			require.False(t, txn.engine.cloneTxnCache.IsTxnLocalSharedFile(txnID, name))
		}
	}
}

func TestProtectCloneFilesDistinguishesExistingOwnership(t *testing.T) {
	tbl := newTxnTableForTest()
	txn := tbl.getTxn()
	txn.BindTxnOp(tbl.db.op)
	txn.engine.cloneTxnCache = newCloneTxnCache()
	txn.SetCloneTxn(1)

	stats := mockStatsList(t, 2)
	mp := mpool.MustNewZero()
	bat := cloneObjectStatsBatchForTest(t, mp, stats[1])
	defer bat.Clean(mp)
	txn.workspace = newTxnWorkspace()
	txn.workspace.append(Entry{typ: INSERT, fileName: "load-meta", bat: bat})

	txn.ProtectCloneFiles(stats[0].ObjectName().String(), stats[1].ObjectName().String())
	txnID := txn.op.Txn().ID
	require.True(t, txn.engine.cloneTxnCache.IsSharedFile(txnID, stats[0].ObjectName().String()))
	require.True(t, txn.engine.cloneTxnCache.IsTxnLocalSharedFile(txnID, stats[1].ObjectName().String()))
}

func TestTrackLoadFilesDeletesOnlyRolledBackGeneration(t *testing.T) {
	ctx := context.Background()
	fs := newCleanFS(t)
	txnOp, closeFn := client.NewTestTxnOperator(ctx)
	defer closeFn()
	txn := &Transaction{
		engine:    &Engine{cloneTxnCache: newCloneTxnCache(), fs: fs},
		op:        txnOp,
		workspace: newTxnWorkspace(),
	}
	txnOp.AddWorkspace(txn)
	txn.SetCloneTxn(1)

	first := mockStatsList(t, 1)[0].ObjectName().String()
	second := mockStatsList(t, 1)[0].ObjectName().String()
	require.NoError(t, writeObjectToFS(ctx, fs, first))
	require.NoError(t, writeObjectToFS(ctx, fs, second))
	txn.TrackLoadFiles(first)
	txn.workspace.advanceStatement()
	txn.TrackLoadFiles(second)
	require.True(t, txn.engine.cloneTxnCache.IsSharedFile(txnOp.Txn().ID, first))
	require.True(t, txn.engine.cloneTxnCache.IsSharedFile(txnOp.Txn().ID, second))

	rolledBack, err := txn.workspace.rollbackCurrentAttempt()
	require.NoError(t, err)
	defer rolledBack.Close()
	deleted, err := txn.deleteLoadFiles(ctx, rolledBack.loadFiles)
	txn.Lock()
	txn.removeLoadFileProtectionsLocked(deleted)
	txn.Unlock()
	require.NoError(t, err)
	_, err = fs.StatFile(ctx, first)
	require.NoError(t, err)
	_, err = fs.StatFile(ctx, second)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound))
	require.False(t, txn.engine.cloneTxnCache.IsSharedFile(txnOp.Txn().ID, second))
	require.True(t, txn.engine.cloneTxnCache.IsSharedFile(txnOp.Txn().ID, first))

	deleted, err = txn.deleteLoadFiles(ctx, txn.workspace.allLoadFiles())
	txn.Lock()
	txn.removeLoadFileProtectionsLocked(deleted)
	txn.Unlock()
	require.NoError(t, err)
	_, err = fs.StatFile(ctx, first)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound))
	require.False(t, txn.engine.cloneTxnCache.IsSharedFile(txnOp.Txn().ID, first))
}

func TestLoadFileCleanupFailureDoesNotAbortTransactionTeardown(t *testing.T) {
	for _, tc := range []struct {
		name           string
		deleteErr      error
		waitForContext bool
		timeout        time.Duration
	}{
		{name: "delete error", deleteErr: errors.New("delete failed"), timeout: 20 * time.Millisecond},
		{name: "blocked delete", waitForContext: true, timeout: 20 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			baseFS := newCleanFS(t)
			fs := &loadCleanupFileService{
				FileService:    baseFS,
				err:            tc.deleteErr,
				waitForContext: tc.waitForContext,
			}
			txnOp, closeFn := client.NewTestTxnOperator(ctx)
			defer closeFn()
			proc := testutil.NewProc(t)
			t.Cleanup(proc.Free)
			colexec.NewServer("")
			txn := &Transaction{
				engine:             &Engine{cloneTxnCache: newCloneTxnCache(), fs: fs},
				op:                 txnOp,
				proc:               proc,
				workspace:          newTxnWorkspace(),
				loadCleanupTimeout: tc.timeout,
			}
			txnOp.AddWorkspace(txn)
			txn.SetCloneTxn(1)
			name := mockStatsList(t, 1)[0].ObjectName().String()
			txn.TrackLoadFiles(name)

			start := time.Now()
			err := txn.Rollback(ctx)
			require.Error(t, err)
			require.True(t, fs.deleteCalled)
			require.True(t, txn.removed)
			require.False(t, txn.engine.cloneTxnCache.IsSharedFile(txnOp.Txn().ID, name))
			if tc.waitForContext {
				require.ErrorIs(t, err, context.DeadlineExceeded)
				require.Less(t, time.Since(start), time.Second)
			} else {
				require.ErrorIs(t, err, tc.deleteErr)
			}
		})
	}
}

type retryLoadCleanupFileService struct {
	fileservice.FileService
	failures int32
	calls    int32
}

func (f *retryLoadCleanupFileService) Delete(ctx context.Context, names ...string) error {
	f.calls++
	if f.calls <= f.failures {
		return context.DeadlineExceeded
	}
	if err := f.FileService.Delete(ctx, names...); err != nil {
		return err
	}
	return nil
}

func TestLoadFileCleanupRetriesBeforeTransactionTeardown(t *testing.T) {
	ctx := context.Background()
	baseFS := newCleanFS(t)
	fs := &retryLoadCleanupFileService{
		FileService: baseFS,
		failures:    2,
	}
	txnOp, closeFn := client.NewTestTxnOperator(ctx)
	defer closeFn()
	proc := testutil.NewProc(t)
	t.Cleanup(proc.Free)
	colexec.NewServer("")
	txn := &Transaction{
		engine:             &Engine{cloneTxnCache: newCloneTxnCache(), fs: fs},
		op:                 txnOp,
		proc:               proc,
		workspace:          newTxnWorkspace(),
		loadCleanupTimeout: time.Second,
	}
	txnOp.AddWorkspace(txn)
	txn.SetCloneTxn(1)
	name := mockStatsList(t, 1)[0].ObjectName().String()
	require.NoError(t, baseFS.Write(ctx, fileservice.IOVector{
		FilePath: name,
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: 1, Data: []byte("x")}},
	}))
	txn.TrackLoadFiles(name)

	require.NoError(t, txn.Rollback(ctx))
	require.True(t, txn.removed)
	_, err := baseFS.StatFile(ctx, name)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound))
	require.Equal(t, int32(3), fs.calls)
}

func TestLoadFileCleanupDoesNotHoldTransactionMutex(t *testing.T) {
	ctx := context.Background()
	baseFS := newCleanFS(t)
	fs := &loadCleanupFileService{
		FileService: baseFS,
		entered:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	txnOp, closeFn := client.NewTestTxnOperator(ctx)
	defer closeFn()
	txn := &Transaction{
		engine:    &Engine{cloneTxnCache: newCloneTxnCache(), fs: fs},
		op:        txnOp,
		workspace: newTxnWorkspace(),
	}
	txnOp.AddWorkspace(txn)
	txn.SetCloneTxn(1)
	txn.TrackLoadFiles(mockStatsList(t, 1)[0].ObjectName().String())

	done := make(chan error, 1)
	go func() {
		_, err := txn.deleteLoadFiles(ctx, txn.workspace.allLoadFiles())
		done <- err
	}()
	<-fs.entered
	released := false
	defer func() {
		if !released {
			close(fs.release)
		}
	}()
	require.True(t, txn.TryLock(), "file deletion must not hold the transaction mutex")
	txn.Unlock()
	close(fs.release)
	released = true
	require.NoError(t, <-done)
}

func TestCloneTxnIntermediateGCKeepsTxnLocalSharedObjects(t *testing.T) {
	ctx := context.Background()
	fs := newCleanFS(t)

	gcPool, err := ants.NewPool(1)
	require.NoError(t, err)
	defer gcPool.Release()

	txnOp, closeFn := client.NewTestTxnOperator(ctx)
	defer closeFn()

	txn := &Transaction{
		engine: &Engine{
			cloneTxnCache: newCloneTxnCache(),
			gcPool:        gcPool,
			fs:            fs,
		},
		op:        txnOp,
		workspace: newTxnWorkspace(),
	}
	txnOp.AddWorkspace(txn)
	txn.SetCloneTxn(1)

	stats := mockStatsList(t, 2)
	committedSourceName := stats[0].ObjectName().String()
	txnLocalName := stats[1].ObjectName().String()
	require.NoError(t, writeObjectToFS(ctx, fs, committedSourceName))
	require.NoError(t, writeObjectToFS(ctx, fs, txnLocalName))

	txn.engine.cloneTxnCache.AddSharedFile(txn.op.Txn().ID, committedSourceName)
	txn.engine.cloneTxnCache.AddTxnLocalSharedFile(txn.op.Txn().ID, txnLocalName)
	mp := mpool.MustNewZero()
	bat := cloneObjectStatsBatchForTest(t, mp, stats...)
	defer func() {
		bat.Clean(mp)
		require.Zero(t, mp.CurrNB())
	}()
	txn.workspace = newTxnWorkspace()
	txn.workspace.append(Entry{
		typ:      INSERT,
		fileName: "clone-meta",
		bat:      bat,
	})

	entries, err := txn.workspace.commitEntries()
	require.NoError(t, err)
	require.NoError(t, txn.gcWorkspaceEntries(entries.entries, cloneGCIntermediate))
	entries.Close()
	require.True(t, objectExistsInFS(ctx, fs, committedSourceName))
	require.True(t, objectExistsInFS(ctx, fs, txnLocalName))
}

func TestCloneTxnRollbackGCDeletesTxnLocalSharedObjects(t *testing.T) {
	ctx := context.Background()
	fs := newCleanFS(t)

	gcPool, err := ants.NewPool(1)
	require.NoError(t, err)
	defer gcPool.Release()

	txnOp, closeFn := client.NewTestTxnOperator(ctx)
	defer closeFn()

	txn := &Transaction{
		engine: &Engine{
			cloneTxnCache: newCloneTxnCache(),
			gcPool:        gcPool,
			fs:            fs,
		},
		op:        txnOp,
		workspace: newTxnWorkspace(),
	}
	txnOp.AddWorkspace(txn)
	txn.SetCloneTxn(1)

	stats := mockStatsList(t, 2)
	committedSourceName := stats[0].ObjectName().String()
	txnLocalName := stats[1].ObjectName().String()
	require.NoError(t, writeObjectToFS(ctx, fs, committedSourceName))
	require.NoError(t, writeObjectToFS(ctx, fs, txnLocalName))

	txn.engine.cloneTxnCache.AddSharedFile(txn.op.Txn().ID, committedSourceName)
	txn.engine.cloneTxnCache.AddTxnLocalSharedFile(txn.op.Txn().ID, txnLocalName)
	mp := mpool.MustNewZero()
	bat := cloneObjectStatsBatchForTest(t, mp, stats...)
	defer func() {
		bat.Clean(mp)
		require.Zero(t, mp.CurrNB())
	}()
	txn.workspace.append(Entry{
		typ:      INSERT,
		fileName: "clone-meta",
		bat:      bat,
	})

	entries, err := txn.workspace.commitEntries()
	require.NoError(t, err)
	require.NoError(t, txn.gcWorkspaceEntries(entries.entries, cloneGCTxnRollback))
	entries.Close()
	require.Eventually(t, func() bool {
		return objectExistsInFS(ctx, fs, committedSourceName) &&
			!objectExistsInFS(ctx, fs, txnLocalName)
	}, time.Second, 10*time.Millisecond)
}

func TestCloneTxnCompactionGCDeletesUnreferencedTxnLocalSharedObject(t *testing.T) {
	ctx := context.Background()
	fs := newCleanFS(t)

	gcPool, err := ants.NewPool(1)
	require.NoError(t, err)
	defer gcPool.Release()

	txnOp, closeFn := client.NewTestTxnOperator(ctx)
	defer closeFn()

	txn := &Transaction{
		engine: &Engine{
			cloneTxnCache: newCloneTxnCache(),
			gcPool:        gcPool,
			fs:            fs,
		},
		op:        txnOp,
		workspace: newTxnWorkspace(),
	}
	txnOp.AddWorkspace(txn)
	txn.SetCloneTxn(1)

	stats := mockStatsList(t, 1)
	name := stats[0].ObjectName().String()
	require.NoError(t, writeObjectToFS(ctx, fs, name))
	txn.engine.cloneTxnCache.AddTxnLocalSharedFile(txn.op.Txn().ID, name)

	require.NoError(t, txn.unprotectUnreferencedTxnLocalSharedFilesLocked(stats, nil))
	require.False(t, txn.engine.cloneTxnCache.IsTxnLocalSharedFile(txn.op.Txn().ID, name))
	require.NoError(t, txn.GCObjsByStats(stats...))
	require.Eventually(t, func() bool {
		return !objectExistsInFS(ctx, fs, name)
	}, time.Second, 10*time.Millisecond)
}

func TestCloneTxnCompactionGCKeepsLiveTxnLocalSharedObject(t *testing.T) {
	ctx := context.Background()
	fs := newCleanFS(t)

	gcPool, err := ants.NewPool(1)
	require.NoError(t, err)
	defer gcPool.Release()

	txnOp, closeFn := client.NewTestTxnOperator(ctx)
	defer closeFn()

	txn := &Transaction{
		engine: &Engine{
			cloneTxnCache: newCloneTxnCache(),
			gcPool:        gcPool,
			fs:            fs,
		},
		op:        txnOp,
		workspace: newTxnWorkspace(),
	}
	txnOp.AddWorkspace(txn)
	txn.SetCloneTxn(1)

	stats := mockStatsList(t, 1)
	name := stats[0].ObjectName().String()
	require.NoError(t, writeObjectToFS(ctx, fs, name))
	txn.engine.cloneTxnCache.AddTxnLocalSharedFile(txn.op.Txn().ID, name)

	mp := mpool.MustNewZero()
	liveBat := cloneObjectStatsBatchForTest(t, mp, stats...)
	defer func() {
		require.NoError(t, txn.workspace.close(mp))
		require.Zero(t, mp.CurrNB())
	}()
	txn.workspace.append(Entry{
		typ:      INSERT,
		fileName: name,
		bat:      liveBat,
	})

	require.NoError(t, txn.unprotectUnreferencedTxnLocalSharedFilesLocked(stats, nil))
	require.True(t, txn.engine.cloneTxnCache.IsTxnLocalSharedFile(txn.op.Txn().ID, name))
	require.NoError(t, txn.GCObjsByStats(stats...))
	require.True(t, objectExistsInFS(ctx, fs, name))
}

func TestCloneTxnTablesInVainGCDeletesUnreferencedTxnLocalSharedObject(t *testing.T) {
	ctx := context.Background()
	fs := newCleanFS(t)

	gcPool, err := ants.NewPool(1)
	require.NoError(t, err)
	defer gcPool.Release()

	txnOp, closeFn := client.NewTestTxnOperator(ctx)
	defer closeFn()

	proc := testutil.NewProc(t)
	txn := &Transaction{
		proc: proc,
		engine: &Engine{
			cloneTxnCache: newCloneTxnCache(),
			gcPool:        gcPool,
			fs:            fs,
		},
		op:        txnOp,
		workspace: newTxnWorkspace(),
	}
	txnOp.AddWorkspace(txn)
	txn.SetCloneTxn(1)
	require.NoError(t, txn.workspace.markTableDropped(0, 0, 42))

	stats := mockStatsList(t, 1)
	name := stats[0].ObjectName().String()
	require.NoError(t, writeObjectToFS(ctx, fs, name))
	txn.engine.cloneTxnCache.AddTxnLocalSharedFile(txn.op.Txn().ID, name)

	txn.appendWorkspaceEntryLocked(Entry{
		typ:      INSERT,
		tableId:  42,
		fileName: name,
		bat:      cloneObjectStatsBatchForTest(t, proc.Mp(), stats...),
	})

	require.NoError(t, txn.mergeTxnWorkspaceLocked(ctx))
	require.Zero(t, txn.workspace.activeMutationCount())
	require.False(t, txn.engine.cloneTxnCache.IsTxnLocalSharedFile(txn.op.Txn().ID, name))
	require.Eventually(t, func() bool {
		return !objectExistsInFS(ctx, fs, name)
	}, time.Second, 10*time.Millisecond)
}

func TestCloneTxnTablesInVainGCKeepsLiveTxnLocalSharedObject(t *testing.T) {
	ctx := context.Background()
	fs := newCleanFS(t)

	gcPool, err := ants.NewPool(1)
	require.NoError(t, err)
	defer gcPool.Release()

	txnOp, closeFn := client.NewTestTxnOperator(ctx)
	defer closeFn()

	proc := testutil.NewProc(t)
	txn := &Transaction{
		proc: proc,
		engine: &Engine{
			cloneTxnCache: newCloneTxnCache(),
			gcPool:        gcPool,
			fs:            fs,
		},
		op:        txnOp,
		workspace: newTxnWorkspace(),
	}
	txnOp.AddWorkspace(txn)
	txn.SetCloneTxn(1)
	require.NoError(t, txn.workspace.markTableDropped(0, 0, 42))

	stats := mockStatsList(t, 1)
	name := stats[0].ObjectName().String()
	require.NoError(t, writeObjectToFS(ctx, fs, name))
	txn.engine.cloneTxnCache.AddTxnLocalSharedFile(txn.op.Txn().ID, name)

	liveBat := cloneObjectStatsBatchForTest(t, proc.Mp(), stats...)
	txn.appendWorkspaceEntryLocked(Entry{
		typ:      INSERT,
		tableId:  42,
		fileName: name,
		bat:      cloneObjectStatsBatchForTest(t, proc.Mp(), stats...),
	})
	txn.appendWorkspaceEntryLocked(Entry{
		typ:      INSERT,
		tableId:  43,
		fileName: name,
		bat:      liveBat,
	})

	require.NoError(t, txn.mergeTxnWorkspaceLocked(ctx))
	entries := workspaceEntriesForTest(t, txn)
	require.Len(t, entries, 1)
	require.Equal(t, uint64(43), entries[0].tableId)
	require.NotNil(t, entries[0].bat)
	require.True(t, txn.engine.cloneTxnCache.IsTxnLocalSharedFile(txn.op.Txn().ID, name))
	require.True(t, objectExistsInFS(ctx, fs, name))

	retireWorkspaceMutationForTest(t, txn, 0)
}

func TestCloneTxnUnknownCommitReleasesLocalStateWithoutObjectGC(t *testing.T) {
	ctx := context.Background()
	fs := newCleanFS(t)

	gcPool, err := ants.NewPool(1)
	require.NoError(t, err)
	defer gcPool.Release()

	txnOp, closeFn := client.NewTestTxnOperator(ctx)
	defer closeFn()
	colexec.NewServer("")

	proc := testutil.NewProc(t)
	txn := &Transaction{
		proc: proc,
		engine: &Engine{
			cloneTxnCache: newCloneTxnCache(),
			gcPool:        gcPool,
			fs:            fs,
		},
		op:        txnOp,
		workspace: newTxnWorkspace(),
	}
	txnOp.AddWorkspace(txn)
	txn.SetCloneTxn(1)

	stats := mockStatsList(t, 1)
	name := stats[0].ObjectName().String()
	require.NoError(t, writeObjectToFS(ctx, fs, name))
	txn.engine.cloneTxnCache.AddTxnLocalSharedFile(txn.op.Txn().ID, name)
	txn.appendWorkspaceEntryLocked(Entry{
		typ:      INSERT,
		tableId:  42,
		fileName: name,
		bat:      cloneObjectStatsBatchForTest(t, proc.Mp(), stats...),
	})

	txn.FinalizeCommitWithUnknownResult(ctx)
	require.True(t, txn.removed)
	require.Zero(t, txn.workspace.activeMutationCount())
	require.False(t, txn.engine.cloneTxnCache.IsTxnLocalSharedFile(txn.op.Txn().ID, name))
	require.True(t, objectExistsInFS(ctx, fs, name))
}

func cloneObjectStatsBatchForTest(
	t *testing.T,
	mp *mpool.MPool,
	statsList ...objectio.ObjectStats,
) *batch.Batch {
	bat := batch.New([]string{catalog.BlockMeta_BlockInfo, catalog.ObjectMeta_ObjectStats})
	bat.SetVector(0, vector.NewVec(types.T_varchar.ToType()))
	bat.SetVector(1, vector.NewVec(types.T_varchar.ToType()))
	for i := range statsList {
		require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte("block-info"), false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[1], statsList[i].Marshal(), false, mp))
	}
	bat.SetRowCount(len(statsList))
	return bat
}
