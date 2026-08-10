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
	"github.com/stretchr/testify/require"
)

type failFirstDeleteFileService struct {
	fileservice.FileService
	err     error
	deletes atomic.Int64
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
	writeErr error
	syncErr  error
	closeErr error
	stats    []objectio.ObjectStats
	tail     []*batch.Batch

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

func (s *stubTransferredTombstoneSinker) DeletePersisted(ctx context.Context) (int, error) {
	s.deletes++
	s.deleteCtxErr = ctx.Err()
	s.deleteDeadline, s.deleteHasBound = ctx.Deadline()
	return s.deleteObjectCnt, nil
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
