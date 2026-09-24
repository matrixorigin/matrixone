// Copyright 2024 Matrix Origin
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

package deletion

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type relationHandleFactory struct {
	engine.Relation
	handle         engine.Relation
	newHandleCalls int
}

func (f *relationHandleFactory) NewRelationHandle() engine.Relation {
	f.newHandleCalls++
	return f.handle
}

func TestString(t *testing.T) {
	buf := new(bytes.Buffer)
	arg := &Deletion{}
	arg.String(buf)
}

func prepareDeletionTest(
	t *testing.T,
	ctrl *gomock.Controller,
) (*process.Process, engine.Engine, *mock_frontend.MockRelation, *relationHandleFactory) {
	ctx := context.TODO()
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Rollback(ctx).Return(nil).AnyTimes()
	txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
	txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{}).AnyTimes()
	txnOperator.EXPECT().NextSequence().Return(uint64(0)).AnyTimes()

	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	eng.EXPECT().Hints().Return(engine.Hints{
		CommitOrRollbackTimeout: time.Second,
	}).AnyTimes()

	database := mock_frontend.NewMockDatabase(ctrl)
	eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).Return(database, nil).AnyTimes()

	relation := mock_frontend.NewMockRelation(ctrl)
	relation.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	relation.EXPECT().Delete(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	factory := &relationHandleFactory{Relation: relation, handle: relation}
	database.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).Return(factory, nil).AnyTimes()

	proc := testutil.NewProc(t)
	proc.Base.TxnClient = txnClient
	proc.Ctx = ctx
	proc.Base.TxnOperator = txnOperator
	return proc, eng, relation, factory
}

func TestNormalDeletion(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc, eng, relation, factory := prepareDeletionTest(t, ctrl)
	relation.EXPECT().Reset(gomock.Any()).Return(nil).Times(1)
	arg := Deletion{
		DeleteCtx: &DeleteCtx{
			Ref: &plan.ObjectRef{
				Obj:        0,
				SchemaName: "testDb",
				ObjName:    "testTable",
			},
			Engine:        eng,
			PrimaryKeyIdx: 1,
		},
		ctr: container{},
	}

	resetChildren(&arg, proc.Mp())
	err := arg.Prepare(proc)
	require.NoError(t, err)
	firstSource := arg.ctr.source
	require.Same(t, relation, firstSource)
	require.Equal(t, 1, factory.newHandleCalls)
	_, err = vm.Exec(&arg, proc)
	require.NoError(t, err)

	arg.Reset(proc, false, nil)
	require.Same(t, firstSource, arg.ctr.source)

	err = arg.Prepare(proc)
	require.NoError(t, err)
	require.Same(t, firstSource, arg.ctr.source)
	require.Equal(t, 1, factory.newHandleCalls)
	_, err = vm.Exec(&arg, proc)
	require.NoError(t, err)
	arg.Free(proc, false, nil)
	require.Nil(t, arg.ctr.source)
	proc.Free()
	require.Equal(t, int64(0), proc.GetMPool().CurrNB())
}

func TestNormalDeletionResetErrorKeepsHandle(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc, eng, relation, factory := prepareDeletionTest(t, ctrl)
	resetErr := errors.New("reset relation")
	relation.EXPECT().Reset(gomock.Any()).Return(resetErr).Times(1)

	arg := Deletion{
		DeleteCtx: &DeleteCtx{
			Ref: &plan.ObjectRef{
				Obj:        0,
				SchemaName: "testDb",
				ObjName:    "testTable",
			},
			Engine:        eng,
			PrimaryKeyIdx: 1,
		},
		ctr: container{},
	}

	resetChildren(&arg, proc.Mp())
	err := arg.Prepare(proc)
	require.NoError(t, err)
	_, err = vm.Exec(&arg, proc)
	require.NoError(t, err)

	arg.Reset(proc, false, nil)
	firstSource := arg.ctr.source
	require.Same(t, relation, firstSource)

	err = arg.Prepare(proc)
	require.ErrorIs(t, err, resetErr)
	require.Same(t, firstSource, arg.ctr.source)
	require.Equal(t, 1, factory.newHandleCalls)
	arg.Free(proc, true, resetErr)
	require.Nil(t, arg.ctr.source)
	proc.Free()
	require.Equal(t, int64(0), proc.GetMPool().CurrNB())
}

func resetChildren(arg *Deletion, m *mpool.MPool) {
	op := colexec.NewMockOperator()
	bat := colexec.MakeMockBatchsWithRowID(m)
	op.WithBatchs([]*batch.Batch{bat})
	arg.Children = nil
	arg.AppendChild(op)
}

func TestFlush(t *testing.T) {
	proc := &process.Process{
		Base: &process.BaseProcess{
			FileService: nil,
		},
	}

	ct := container{}
	_, err := ct.flush(proc, nil)
	require.Error(t, err)
}

func TestNewDeletionTombstoneWriterUsesProcessService(t *testing.T) {
	proc := testutil.NewProc(t)
	defer proc.Free()
	fs, err := colexec.GetSharedFSFromProc(proc)
	require.NoError(t, err)

	writer := newDeletionTombstoneWriter(proc, fs, types.T_int64.ToType())
	require.NotNil(t, writer)
	require.NoError(t, writer.Close())
}

func TestRemoteDeleteFlushTransfersOwnershipBeforeReset(t *testing.T) {
	for _, accepted := range []bool{false, true} {
		t.Run(fmt.Sprintf("accepted=%v", accepted), func(t *testing.T) {
			proc := testutil.NewProc(t)
			defer proc.Free()
			ctr, objectName := flushTombstoneObjectForTest(t, proc)
			workspace := proc.GetTxnOperator().GetWorkspace().(*deletionS3CleanupWorkspace)
			require.Len(t, workspace.owners, 1)
			require.Empty(t, ctr.s3Writers, "completed writer buffers must be released")
			if accepted {
				workspace.AcceptUnpublishedS3ObjectNames(objectName)
			}
			arg := &Deletion{RemoteDelete: true, ctr: *ctr}
			// Both success and later failure teardown must leave transferred
			// objects alone. Only the workspace knows which names registered.
			arg.Reset(proc, false, nil)
			arg.Free(proc, true, errors.New("consumer failed"))
			_, err := ctr.fs.StatFile(proc.Ctx, objectName)
			require.NoError(t, err)
			require.NoError(t, workspace.Cleanup(proc.Ctx))
			_, err = ctr.fs.StatFile(proc.Ctx, objectName)
			if accepted {
				require.NoError(t, err)
			} else {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound), "%v", err)
			}
		})
	}
}

type deletionS3CleanupWorkspace struct {
	client.Workspace
	owners   []*colexec.UnpublishedS3ObjectOwner
	cleanups []func(context.Context) error
}

func (w *deletionS3CleanupWorkspace) RetainUnpublishedS3Cleanup(cleanup func(context.Context) error) {
	w.cleanups = append(w.cleanups, cleanup)
}

func TestRemoteDeleteFreeRetainsFailedWriterCleanup(t *testing.T) {
	proc := testutil.NewProc(t)
	defer proc.Free()
	baseFS, err := colexec.GetSharedFSFromProc(proc)
	require.NoError(t, err)
	deleteErr := errors.New("injected remote delete cleanup failure")
	fs := &failOnceDeleteFileService{FileService: baseFS, failErr: deleteErr, failCount: 2}
	proc.Base.FileService = fs
	ctr, name := flushTombstoneObjectForTest(t, proc, true)
	require.Len(t, ctr.s3Writers, 1, "failed producer handoff must keep the writer")

	workspace := &deletionS3CleanupWorkspace{}
	txnOp := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
	txnOp.EXPECT().GetWorkspace().Return(workspace).AnyTimes()
	proc.Base.TxnOperator = txnOp
	arg := &Deletion{RemoteDelete: true, ctr: *ctr}
	arg.Free(proc, true, deleteErr)
	require.Empty(t, arg.ctr.s3Writers)
	require.Len(t, workspace.cleanups, 1, "transaction must own the failed cleanup")
	_, err = baseFS.StatFile(proc.Ctx, name)
	require.NoError(t, err)
	require.NoError(t, workspace.cleanups[0](proc.Ctx))
	_, err = baseFS.StatFile(proc.Ctx, name)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound), "%v", err)
}

func TestRemoteDeleteWorkspaceRetriesCleanupAfterProducerReuse(t *testing.T) {
	proc := testutil.NewProc(t)
	defer proc.Free()
	baseFS, err := colexec.GetSharedFSFromProc(proc)
	require.NoError(t, err)
	deleteErr := errors.New("injected tombstone cleanup failure")
	fs := &failOnceDeleteFileService{FileService: baseFS, failErr: deleteErr, failCount: 1}
	proc.Base.FileService = fs
	ctr, name := flushTombstoneObjectForTest(t, proc)
	workspace := proc.GetTxnOperator().GetWorkspace().(*deletionS3CleanupWorkspace)
	arg := &Deletion{RemoteDelete: true, ctr: *ctr}
	arg.Reset(proc, true, errors.New("consumer failed"))
	require.ErrorIs(t, workspace.Cleanup(proc.Ctx), deleteErr)
	require.True(t, workspace.HasUnpublishedS3ObjectOwners())
	require.Equal(t, []string{name}, workspace.owners[0].Names())
	_, err = fs.StatFile(proc.Ctx, name)
	require.NoError(t, err)
	// Reuse is safe now: the workspace outlives the reusable operator and keeps
	// the retry ledger, without retaining old stats batches or writer buffers.
	require.NoError(t, arg.retryPendingS3Cleanup(proc))
	arg.Free(proc, false, nil)
	require.True(t, workspace.HasUnpublishedS3ObjectOwners())
	require.NoError(t, workspace.Cleanup(proc.Ctx))
	require.False(t, workspace.HasUnpublishedS3ObjectOwners())
	_, err = fs.StatFile(proc.Ctx, name)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound), "%v", err)
}

func TestRemoteDeleteMissingWorkspacePreservesWriterCleanup(t *testing.T) {
	proc := testutil.NewProc(t)
	defer proc.Free()
	baseFS, err := colexec.GetSharedFSFromProc(proc)
	require.NoError(t, err)
	deleteErr := errors.New("injected writer cleanup failure")
	fs := &failOnceDeleteFileService{FileService: baseFS, failErr: deleteErr, failCount: 1}
	proc.Base.FileService = fs
	ctr, name := flushTombstoneObjectForTest(t, proc, true)
	require.Len(t, ctr.s3Writers, 1, "failed retention and deletion must keep the writer")
	arg := &Deletion{RemoteDelete: true, ctr: *ctr}
	arg.Reset(proc, false, nil)
	require.Empty(t, arg.ctr.s3Writers)
	_, err = fs.StatFile(proc.Ctx, name)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound), "%v", err)
	arg.Free(proc, false, nil)
}

func (w *deletionS3CleanupWorkspace) RetainUnpublishedS3ObjectOwner(owner *colexec.UnpublishedS3ObjectOwner) {
	w.owners = append(w.owners, owner)
}
func (w *deletionS3CleanupWorkspace) AcceptUnpublishedS3ObjectNames(names ...string) {
	for _, owner := range w.owners {
		owner.Accept(names...)
	}
}
func (w *deletionS3CleanupWorkspace) HasUnpublishedS3ObjectOwners() bool {
	for _, owner := range w.owners {
		if owner.Pending() {
			return true
		}
	}
	return false
}
func (w *deletionS3CleanupWorkspace) AcceptAllUnpublishedS3ObjectOwners() {
	for _, owner := range w.owners {
		owner.AcceptAll()
	}
}
func (w *deletionS3CleanupWorkspace) Cleanup(ctx context.Context) error {
	for _, owner := range w.owners {
		if err := owner.Cleanup(ctx); err != nil {
			return err
		}
	}
	return nil
}

func flushTombstoneObjectForTest(t *testing.T, proc *process.Process, missingWorkspace ...bool) (*container, string) {
	t.Helper()
	if len(missingWorkspace) == 0 {
		ctrl := gomock.NewController(t)
		txnOp := mock_frontend.NewMockTxnOperator(ctrl)
		txnOp.EXPECT().GetWorkspace().Return(&deletionS3CleanupWorkspace{}).AnyTimes()
		proc.Base.TxnOperator = txnOp
	}
	blockID := types.BuildTestBlockid(1, 1)
	rowID := types.NewRowid(&blockID, 0)
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], rowID, false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(bat.Vecs[1], int64(7), false, proc.Mp()))
	bat.SetRowCount(1)

	ctr := &container{
		partitionId_blockId_rowIdBatch: map[int]map[types.Blockid]*batch.Batch{
			0: {blockID: bat},
		},
		partitionId_tombstoneObjectStatsBats: make(map[int][]*batch.Batch),
		blockId_type:                         map[types.Blockid]int8{blockID: DeletionOnCommitted},
		pool:                                 &BatchPool{},
	}
	size, err := ctr.flush(proc, process.NewAnalyzer(0, false, false, "deletion-flush"))
	if len(missingWorkspace) == 0 {
		require.NoError(t, err)
		require.NotZero(t, size)
	} else {
		require.ErrorContains(t, err, "transaction workspace cannot retain")
	}
	require.Empty(t, ctr.partitionId_blockId_rowIdBatch[0])
	require.Len(t, ctr.partitionId_tombstoneObjectStatsBats[0], 1)
	statsBat := ctr.partitionId_tombstoneObjectStatsBats[0][0]
	data, area := vector.MustVarlenaRawData(statsBat.Vecs[0])
	stats := objectio.ObjectStats(data[0].GetByteSlice(area))
	objectName := stats.ObjectName().String()
	_, err = ctr.fs.StatFile(proc.Ctx, objectName)
	require.NoError(t, err)
	return ctr, objectName
}

type failOnceDeleteFileService struct {
	fileservice.FileService
	failErr   error
	failCount int
}

func (fs *failOnceDeleteFileService) Delete(ctx context.Context, names ...string) error {
	if fs.failCount > 0 {
		fs.failCount--
		return fs.failErr
	}
	return fs.FileService.Delete(ctx, names...)
}
