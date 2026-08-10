// Copyright 2021 Matrix Origin
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

package txnimpl

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	txnpb "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/stretchr/testify/require"
)

type failFirstDeleteFileService struct {
	fileservice.FileService
	err     error
	deletes atomic.Int64
}

type rejectObjectDeleteFileService struct {
	fileservice.FileService
	reject        atomic.Bool
	objectDeletes atomic.Int64
}

func (fs *rejectObjectDeleteFileService) Delete(
	ctx context.Context,
	paths ...string,
) error {
	if fs.reject.Load() {
		for _, path := range paths {
			if !strings.HasPrefix(path, "gc/unpublished/") {
				fs.objectDeletes.Add(1)
				return errors.New("injected persistent object delete failure")
			}
		}
	}
	return fs.FileService.Delete(ctx, paths...)
}

func (fs *failFirstDeleteFileService) Delete(
	ctx context.Context,
	paths ...string,
) error {
	if fs.deletes.Add(1) == 1 {
		return fs.err
	}
	return fs.FileService.Delete(ctx, paths...)
}

func listTransferredTombstoneTestFiles(
	t *testing.T,
	fs fileservice.FileService,
) []string {
	t.Helper()
	entries, err := fileservice.SortedList(fs.List(context.Background(), ""))
	require.NoError(t, err)
	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir {
			files = append(files, entry.Name)
		}
	}
	return files
}

type stubTransferredTombstoneSinker struct {
	writeErr  error
	syncErr   error
	deleteErr error
	closeErr  error
	stats     []objectio.ObjectStats
	tail      []*batch.Batch

	writes          int
	syncs           int
	deletes         int
	closes          int
	deleteCtxErr    error
	deleteDeadline  time.Time
	deleteHasBound  bool
	deleteObjectCnt int
}

func (s *stubTransferredTombstoneSinker) Write(context.Context, *batch.Batch) error {
	s.writes++
	return s.writeErr
}

func (s *stubTransferredTombstoneSinker) Sync(context.Context) error {
	s.syncs++
	return s.syncErr
}

func (s *stubTransferredTombstoneSinker) GetResult() ([]objectio.ObjectStats, []*batch.Batch) {
	return s.stats, s.tail
}

func (s *stubTransferredTombstoneSinker) DeletePersisted(ctx context.Context) ([]string, error) {
	s.deletes++
	s.deleteCtxErr = ctx.Err()
	s.deleteDeadline, s.deleteHasBound = ctx.Deadline()
	files := make([]string, s.deleteObjectCnt)
	for i := range files {
		files[i] = fmt.Sprintf("unpublished-%d", i)
	}
	return files, s.deleteErr
}

func (s *stubTransferredTombstoneSinker) Close() error {
	s.closes++
	return s.closeErr
}

func TestTransferredTombstoneSinkPublishesOwnership(t *testing.T) {
	objectID := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&objectID, false, false, true)
	stub := &stubTransferredTombstoneSinker{stats: []objectio.ObjectStats{*stats}}
	sink := &transferredTombstoneSink{sinker: stub}

	var registered []objectio.ObjectStats
	require.NoError(t, sink.write(context.Background(), nil))
	require.NoError(t, sink.publish(context.Background(), func(stats ...objectio.ObjectStats) {
		registered = append(registered, stats...)
	}))
	require.ErrorContains(t, sink.write(context.Background(), nil), "after publication")
	require.ErrorContains(t, sink.publish(context.Background(), func(...objectio.ObjectStats) {}), "more than once")
	require.NoError(t, sink.close(context.Background(), nil))

	require.Equal(t, []objectio.ObjectStats{*stats}, registered)
	require.Equal(t, 1, stub.writes)
	require.Equal(t, 1, stub.syncs)
	require.Zero(t, stub.deletes, "published objects belong to the transaction")
	require.Equal(t, 1, stub.closes)
}

func TestTransferredTombstoneSinkPublishedCloseFailureUsesTxnRollback(t *testing.T) {
	objectID := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&objectID, false, false, true)
	closeErr := errors.New("close published sink")
	stub := &stubTransferredTombstoneSinker{
		stats:    []objectio.ObjectStats{*stats},
		closeErr: closeErr,
	}
	sink := &transferredTombstoneSink{sinker: stub}

	require.NoError(t, sink.write(context.Background(), nil))
	require.NoError(t, sink.publish(context.Background(), func(...objectio.ObjectStats) {}))
	err := sink.close(context.Background(), nil)

	require.Same(t, closeErr, err)
	require.Zero(t, stub.deletes,
		"published objects must remain owned by transaction rollback")
	require.Equal(t, 1, stub.closes)
}

func TestTransferredTombstoneSinkFailsClosedBeforePublication(t *testing.T) {
	writeErr := errors.New("write transferred tombstone")
	syncErr := errors.New("sync transferred tombstone")
	objectID := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&objectID, false, false, true)
	testCases := []struct {
		name      string
		stub      *stubTransferredTombstoneSinker
		operation func(*transferredTombstoneSink) error
		wantErr   error
	}{
		{
			name: "write",
			stub: &stubTransferredTombstoneSinker{
				writeErr:        writeErr,
				deleteObjectCnt: 1,
			},
			operation: func(sink *transferredTombstoneSink) error {
				return sink.write(context.Background(), nil)
			},
			wantErr: writeErr,
		},
		{
			name: "sync",
			stub: &stubTransferredTombstoneSinker{
				syncErr:         syncErr,
				deleteObjectCnt: 2,
			},
			operation: func(sink *transferredTombstoneSink) error {
				require.NoError(t, sink.write(context.Background(), nil))
				return sink.publish(context.Background(), func(...objectio.ObjectStats) {
					t.Fatal("failed sync must not publish object stats")
				})
			},
			wantErr: syncErr,
		},
		{
			name: "unexpected in-memory tail",
			stub: &stubTransferredTombstoneSinker{
				stats:           []objectio.ObjectStats{*stats},
				tail:            []*batch.Batch{batch.NewWithSize(0)},
				deleteObjectCnt: 1,
			},
			operation: func(sink *transferredTombstoneSink) error {
				require.NoError(t, sink.write(context.Background(), nil))
				return sink.publish(context.Background(), func(...objectio.ObjectStats) {
					t.Fatal("invalid sink result must not publish object stats")
				})
			},
		},
		{
			name: "empty persisted result",
			stub: &stubTransferredTombstoneSinker{
				deleteObjectCnt: 1,
			},
			operation: func(sink *transferredTombstoneSink) error {
				require.NoError(t, sink.write(context.Background(), nil))
				return sink.publish(context.Background(), func(...objectio.ObjectStats) {
					t.Fatal("empty sink result must not publish")
				})
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			sink := &transferredTombstoneSink{sinker: testCase.stub}
			opErr := testCase.operation(sink)
			require.Error(t, opErr)

			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			err := sink.close(ctx, opErr)
			require.Error(t, err)
			if testCase.wantErr != nil {
				require.ErrorIs(t, err, testCase.wantErr)
			}
			require.Equal(t, 1, testCase.stub.deletes)
			require.NoError(t, testCase.stub.deleteCtxErr,
				"cleanup must not inherit operation cancellation")
			require.True(t, testCase.stub.deleteHasBound,
				"detached cleanup must have an operation-level deadline")
			require.WithinDuration(t,
				time.Now().Add(transferredTombstoneCleanupTimeout),
				testCase.stub.deleteDeadline,
				time.Second,
			)
			require.Equal(t, 1, testCase.stub.closes)
		})
	}
}

func TestTransferredTombstoneSinkRetriesRealPersistedObject(t *testing.T) {
	ctx := context.Background()
	proc := testutil.NewProc(t)
	baseFS, err := fileservice.Get[fileservice.FileService](
		proc.GetFileService(),
		defines.SharedFileServiceName,
	)
	require.NoError(t, err)
	filesBeforeSpill := listTransferredTombstoneTestFiles(t, baseFS)

	deleteErr := errors.New("injected first object delete failure")
	fs := &failFirstDeleteFileService{FileService: baseFS, err: deleteErr}
	pkType := types.T_int32.ToType()
	raw := ioutil.NewTombstoneSinker(
		objectio.HiddenColumnSelection_None,
		pkType,
		proc.Mp(),
		fs,
		ioutil.WithMemorySizeThreshold(1),
	)
	sink := &transferredTombstoneSink{sinker: raw}
	bat := catalog.NewCNTombstoneBatchByPKType(pkType, proc.Mp())
	defer bat.Close()
	objectID := objectio.NewObjectid()
	rowID := types.NewRowIDWithObjectIDBlkNumAndRowID(objectID, 0, 0)
	bat.GetVectorByName(objectio.TombstoneAttr_Rowid_Attr).Append(rowID, false)
	bat.GetVectorByName(objectio.TombstoneAttr_PK_Attr).Append(int32(1), false)

	require.NoError(t, sink.write(ctx, containers.ToCNBatch(bat)))
	require.NoError(t, raw.Sync(ctx))
	stats, tail := raw.GetResult()
	require.NotEmpty(t, stats, "the test must create a persisted spill")
	require.Empty(t, tail)
	require.NotEqual(t, filesBeforeSpill, listTransferredTombstoneTestFiles(t, baseFS))

	operationErr := moerr.NewTxnWWConflictNoCtx(0, "")
	require.Same(t, operationErr, sink.close(ctx, operationErr))
	require.True(t, sink.cleanupPending)
	require.False(t, sink.closed)
	require.Equal(t, int64(1), fs.deletes.Load())

	tbl := &txnTable{
		store:                            &txnStore{ctx: ctx},
		pendingTransferredTombstoneSinks: []*transferredTombstoneSink{sink},
	}
	require.NoError(t, tbl.rollbackTransferredTombstones())
	require.Equal(t, int64(2), fs.deletes.Load())
	require.True(t, sink.closed)
	require.Empty(t, tbl.pendingTransferredTombstoneSinks)
	require.Equal(t, filesBeforeSpill, listTransferredTombstoneTestFiles(t, baseFS),
		"the retry must delete the real unpublished object before Close")
}

func TestTransferredTombstoneTerminalCleanupHandoff(t *testing.T) {
	ctx := context.Background()
	proc := testutil.NewProc(t)
	baseFS, err := fileservice.Get[fileservice.FileService](
		proc.GetFileService(),
		defines.SharedFileServiceName,
	)
	require.NoError(t, err)
	filesBeforeSpill := listTransferredTombstoneTestFiles(t, baseFS)
	fs := &rejectObjectDeleteFileService{FileService: baseFS}
	fs.reject.Store(true)

	pkType := types.T_int32.ToType()
	raw := ioutil.NewTombstoneSinker(
		objectio.HiddenColumnSelection_None,
		pkType,
		proc.Mp(),
		fs,
		ioutil.WithMemorySizeThreshold(1),
	)
	sink := &transferredTombstoneSink{sinker: raw}
	bat := catalog.NewCNTombstoneBatchByPKType(pkType, proc.Mp())
	defer bat.Close()
	objectID := objectio.NewObjectid()
	rowID := types.NewRowIDWithObjectIDBlkNumAndRowID(objectID, 0, 0)
	bat.GetVectorByName(objectio.TombstoneAttr_Rowid_Attr).Append(rowID, false)
	bat.GetVectorByName(objectio.TombstoneAttr_PK_Attr).Append(int32(1), false)

	require.NoError(t, sink.write(ctx, containers.ToCNBatch(bat)))
	require.NoError(t, raw.Sync(ctx))
	stats, tail := raw.GetResult()
	require.NotEmpty(t, stats, "the test must create a persisted spill")
	require.Empty(t, tail)

	operationErr := moerr.NewTxnWWConflictNoCtx(0, "")
	require.Same(t, operationErr, sink.close(ctx, operationErr),
		"cleanup handling must not replace the primary transaction error")
	require.True(t, sink.cleanupPending)

	rt := dbutils.NewRuntime(dbutils.WithRuntimeObjectFS(fs))
	rt.HandoffUnpublishedObjects = func(ctx context.Context, files ...string) error {
		_, err := ioutil.RecordUnpublishedObjectCleanup(ctx, fs, files...)
		return err
	}
	store := &txnStore{ctx: ctx, rt: rt}
	tbl := &txnTable{
		store:                             store,
		dataTable:                         &baseTable{},
		txnEntries:                        newTxnEntries(),
		pendingTransferredTombstoneSinks:  []*transferredTombstoneSink{sink},
		transferredTombstoneCleanupLogged: true,
	}
	db := &txnDB{store: store, tables: map[uint64]*txnTable{1: tbl}}
	store.dbs = map[uint64]*txnDB{1: db}

	require.Error(t, tbl.PrepareRollback(),
		"PrepareRollback must retain ownership when object deletion still fails")
	require.NoError(t, store.Close(),
		"final Store.Close must durably hand off before releasing transaction state")
	require.Equal(t, int64(3), fs.objectDeletes.Load(),
		"initial cleanup, PrepareRollback, and final Store.Close must all be exercised")
	require.True(t, sink.closed)
	require.Empty(t, tbl.pendingTransferredTombstoneSinks)
	require.Nil(t, store.dbs,
		"successful terminal handoff must allow transaction state to be released")
	require.Nil(t, db.tables)
	require.NotEqual(t, filesBeforeSpill, listTransferredTombstoneTestFiles(t, baseFS),
		"the failed transaction-scoped delete leaves the object for its new owner")

	_, _, err = ioutil.ReplayUnpublishedObjectCleanup(ctx, fs)
	require.ErrorContains(t, err, "injected persistent object delete failure")
	fs.reject.Store(false)
	replayed, remaining, err := ioutil.ReplayUnpublishedObjectCleanup(ctx, fs)
	require.NoError(t, err)
	require.Equal(t, 1, replayed)
	require.False(t, remaining)
	require.Equal(t, filesBeforeSpill, listTransferredTombstoneTestFiles(t, baseFS),
		"the post-transaction owner must eventually remove the physical orphan")
}

func TestPrepareRollbackVisitsEveryTransferredTombstoneOwner(t *testing.T) {
	ctx := context.Background()
	baseFS, err := fileservice.NewMemoryFS(
		"shared", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	fs := &rejectObjectDeleteFileService{FileService: baseFS}
	fs.reject.Store(true)

	rt := dbutils.NewRuntime(dbutils.WithRuntimeObjectFS(fs))
	rt.HandoffUnpublishedObjects = func(ctx context.Context, files ...string) error {
		_, err := ioutil.RecordUnpublishedObjectCleanup(ctx, fs, files...)
		return err
	}
	store := &txnStore{ctx: ctx, rt: rt, dbs: make(map[uint64]*txnDB)}
	var tables []*txnTable
	var objects []string
	for dbID := uint64(1); dbID <= 2; dbID++ {
		db := &txnDB{store: store, tables: make(map[uint64]*txnTable)}
		store.dbs[dbID] = db
		for tableID := uint64(1); tableID <= 2; tableID++ {
			object := fmt.Sprintf("unpublished-%d-%d", dbID, tableID)
			require.NoError(t, baseFS.Write(ctx, fileservice.IOVector{
				FilePath: object,
				Entries: []fileservice.IOEntry{{
					Offset: 0,
					Size:   1,
					Data:   []byte{1},
				}},
			}))
			tbl := &txnTable{
				store:                             store,
				dataTable:                         &baseTable{},
				txnEntries:                        newTxnEntries(),
				transferredTombstoneObjects:       []string{object},
				transferredTombstoneCleanupLogged: true,
			}
			db.tables[tableID] = tbl
			tables = append(tables, tbl)
			objects = append(objects, object)
		}
	}

	require.Error(t, store.PrepareRollback())
	require.Equal(t, int64(len(tables)), fs.objectDeletes.Load(),
		"one failed owner must not prevent later tables or databases from preparing rollback")
	for _, tbl := range tables {
		require.True(t, tbl.transferredTombstoneRollback)
	}

	require.NoError(t, store.Close())
	require.Nil(t, store.dbs)
	require.Equal(t, int64(2*len(tables)), fs.objectDeletes.Load())

	fs.reject.Store(false)
	replayed, remaining, err := ioutil.ReplayUnpublishedObjectCleanup(ctx, fs)
	require.NoError(t, err)
	require.Equal(t, len(tables), replayed)
	require.False(t, remaining)
	for _, object := range objects {
		_, err = baseFS.StatFile(ctx, object)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound))
	}
}

type deadlineTrackingDeleteFileService struct {
	fileservice.FileService
	deletes     int
	deadline    time.Time
	hasDeadline bool
}

func (fs *deadlineTrackingDeleteFileService) Delete(
	ctx context.Context,
	_ ...string,
) error {
	fs.deletes++
	fs.deadline, fs.hasDeadline = ctx.Deadline()
	return nil
}

func TestTxnTableCloseDoesNotDeleteWithoutConfirmedRollback(t *testing.T) {
	newTable := func(fs fileservice.FileService) *txnTable {
		return &txnTable{
			store: &txnStore{
				ctx: context.Background(),
				rt:  dbutils.NewRuntime(dbutils.WithRuntimeObjectFS(fs)),
			},
			dataTable:                   &baseTable{},
			transferredTombstoneObjects: []string{"transferred-tombstone"},
		}
	}

	t.Run("uncertain commit", func(t *testing.T) {
		fs := &deadlineTrackingDeleteFileService{}
		tbl := newTable(fs)
		require.NoError(t, tbl.Close())
		require.Zero(t, fs.deletes,
			"generic Close must not delete an object that commit recovery may reference")
	})

	t.Run("confirmed rollback", func(t *testing.T) {
		fs := &deadlineTrackingDeleteFileService{}
		tbl := newTable(fs)
		tbl.transferredTombstoneRollback = true
		require.NoError(t, tbl.Close())
		require.Equal(t, 1, fs.deletes,
			"confirmed rollback must delete its unpublished object")
	})
}

func TestTransferredTombstoneCleanupSharesOneTransactionDeadline(t *testing.T) {
	deleteErr := errors.New("injected initial delete failure")
	stub := &stubTransferredTombstoneSinker{
		deleteErr:       deleteErr,
		deleteObjectCnt: 1,
	}
	sink := &transferredTombstoneSink{sinker: stub}
	operationErr := moerr.NewTxnWWConflictNoCtx(0, "")
	require.Same(t, operationErr, sink.close(context.Background(), operationErr))
	require.True(t, sink.cleanupPending)

	fs := &deadlineTrackingDeleteFileService{}
	tbl := &txnTable{
		store: &txnStore{
			ctx: context.Background(),
			rt:  dbutils.NewRuntime(dbutils.WithRuntimeObjectFS(fs)),
		},
		transferredTombstoneObjects: []string{"published-before-rollback"},
	}
	tbl.registerPendingTransferredTombstoneSink(sink)
	stub.deleteErr = nil

	require.NoError(t, tbl.rollbackTransferredTombstones())
	require.True(t, stub.deleteHasBound)
	require.True(t, fs.hasDeadline)
	require.Equal(t, tbl.store.transferredTombstoneCleanupDeadline, stub.deleteDeadline)
	require.Equal(t, tbl.store.transferredTombstoneCleanupDeadline, fs.deadline)
	require.Equal(t, 2, stub.deletes)
	require.Equal(t, 1, fs.deletes)

	otherTable := &txnTable{store: tbl.store}
	otherCleanupCtx, cancel := otherTable.newTransferredTombstoneCleanupContext()
	defer cancel()
	otherDeadline, ok := otherCleanupCtx.Deadline()
	require.True(t, ok)
	require.Equal(t, tbl.store.transferredTombstoneCleanupDeadline, otherDeadline,
		"all tables in one transaction must share the earliest cleanup deadline")
}

func TestTransferredTombstoneSinkPreservesMoErrClassification(t *testing.T) {
	operationErr := moerr.NewTxnRWConflictNoCtx()
	stub := &stubTransferredTombstoneSinker{}
	sink := &transferredTombstoneSink{sinker: stub}

	err := sink.close(context.Background(), operationErr)

	require.Same(t, operationErr, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnRWConflict))
	encoded := txnpb.WrapError(err, 0)
	require.Equal(t, uint32(moerr.ErrTxnRWConflict), encoded.Code)
	require.True(t,
		moerr.IsMoErrCode(encoded.UnwrapError(), moerr.ErrTxnRWConflict),
		"TN RPC serialization must retain the retryable conflict class",
	)
}

func TestTransferredTombstoneSinkRejectsCloseWithoutPublication(t *testing.T) {
	t.Run("close", func(t *testing.T) {
		stub := &stubTransferredTombstoneSinker{}
		sink := &transferredTombstoneSink{sinker: stub}

		err := sink.close(context.Background(), nil)

		require.ErrorContains(t, err, "closed before publication")
		require.Equal(t, 1, stub.deletes)
		require.True(t, stub.deleteHasBound)
		require.Equal(t, 1, stub.closes)
	})

	t.Run("publish", func(t *testing.T) {
		stub := &stubTransferredTombstoneSinker{}
		sink := &transferredTombstoneSink{sinker: stub}

		opErr := sink.publish(context.Background(), func(...objectio.ObjectStats) {})
		require.ErrorContains(t, opErr, "before write")
		err := sink.close(context.Background(), opErr)

		require.ErrorContains(t, err, "before write")
		require.Equal(t, 1, stub.deletes)
		require.Equal(t, 1, stub.closes)
	})
}
