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

func TestFlushCreatesTombstoneWriterForFirstBlock(t *testing.T) {
	proc := testutil.NewProc(t)
	defer proc.Free()
	ctr, objectName := flushTombstoneObjectForTest(t, proc)
	require.Empty(t, ctr.s3Writers, "successful flush transfers object ownership to its stats batch")
	_, err := ctr.fs.StatFile(proc.Ctx, objectName)
	require.NoError(t, err)
	ctr.fs = &failOnceDeleteFileService{
		FileService: ctr.fs,
		failErr:     errors.New("injected tombstone cleanup failure"),
		failCount:   2,
	}

	arg := &Deletion{RemoteDelete: true, ctr: *ctr}
	arg.Reset(proc, true, errors.New("downstream pipeline failed"))
	require.Empty(t, arg.ctr.partitionId_tombstoneObjectStatsBats, "failed output must not remain visible to the next execution")
	require.Len(t, arg.ctr.pendingTombstoneObjectStatsBats, 1, "failed deletion must retain its private cleanup ledger")
	require.Error(t, arg.Prepare(proc), "reuse must fail while the prior object's cleanup is pending")
	_, err = arg.ctr.fs.StatFile(proc.Ctx, objectName)
	require.NoError(t, err, "failed first cleanup should leave the object available for retry")
	arg.Free(proc, false, nil)
	require.Empty(t, arg.ctr.partitionId_tombstoneObjectStatsBats)
	require.Empty(t, arg.ctr.pendingTombstoneObjectStatsBats)
	_, err = arg.ctr.fs.StatFile(proc.Ctx, objectName)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound), "object should be deleted, got %v", err)
}

type deletionS3CleanupWorkspace struct {
	client.Workspace
	cleanups []func(context.Context) error
}

func (w *deletionS3CleanupWorkspace) RetainUnpublishedS3Cleanup(
	cleanup func(context.Context) error,
) {
	w.cleanups = append(w.cleanups, cleanup)
}

func TestRemoteDeleteFreeTransfersFailedCleanupToTransaction(t *testing.T) {
	proc := testutil.NewProc(t)
	defer proc.Free()
	ctr, objectName := flushTombstoneObjectForTest(t, proc)
	deleteErr := errors.New("injected persistent tombstone cleanup failure")
	ctr.fs = &failOnceDeleteFileService{
		FileService: ctr.fs,
		failErr:     deleteErr,
		failCount:   2,
	}

	ctrl := gomock.NewController(t)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	workspace := &deletionS3CleanupWorkspace{}
	txnOp.EXPECT().GetWorkspace().Return(workspace).AnyTimes()
	proc.Base.TxnOperator = txnOp

	arg := NewArgument()
	arg.RemoteDelete = true
	arg.ctr = *ctr
	arg.Reset(proc, true, deleteErr)
	arg.Free(proc, false, nil)
	require.Empty(t, arg.ctr.pendingTombstoneObjectStatsBats,
		"transaction callback must own cleanup after operator release")
	require.Len(t, workspace.cleanups, 1)
	arg.Release()

	_, err := ctr.fs.StatFile(proc.Ctx, objectName)
	require.NoError(t, err, "failed deletes should leave the object pending")
	require.NoError(t, workspace.cleanups[0](proc.Ctx))
	_, err = ctr.fs.StatFile(proc.Ctx, objectName)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound), "transaction retry should delete the object, got %v", err)
}

func TestRemoteDeleteSuccessfulResetPreservesTransferredTombstones(t *testing.T) {
	proc := testutil.NewProc(t)
	defer proc.Free()
	ctr, objectName := flushTombstoneObjectForTest(t, proc)
	arg := &Deletion{RemoteDelete: true, ctr: *ctr}

	arg.Reset(proc, false, nil)
	require.Empty(t, arg.ctr.partitionId_tombstoneObjectStatsBats)
	_, err := ctr.fs.StatFile(proc.Ctx, objectName)
	require.NoError(t, err, "successful pipeline reset must leave transferred tombstones intact")
	arg.Free(proc, false, nil)
}

func flushTombstoneObjectForTest(t *testing.T, proc *process.Process) (*container, string) {
	t.Helper()
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
	require.NoError(t, err)
	require.NotZero(t, size)
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
