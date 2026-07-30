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

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
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
