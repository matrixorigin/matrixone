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
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func TestString(t *testing.T) {
	buf := new(bytes.Buffer)
	arg := &Deletion{}
	arg.String(buf)
}

func prepareDeletionTest(t *testing.T, ctrl *gomock.Controller, relResetExpectErr bool) (*process.Process, engine.Engine) {
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
	if relResetExpectErr {
		relation.EXPECT().Reset(gomock.Any()).Return(moerr.NewInternalErrorNoCtx("")).AnyTimes()
	} else {
		relation.EXPECT().Reset(gomock.Any()).Return(nil).AnyTimes()
	}

	database.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).Return(relation, nil).AnyTimes()

	proc := testutil.NewProc(t)
	proc.Base.TxnClient = txnClient
	proc.Ctx = ctx
	proc.Base.TxnOperator = txnOperator
	return proc, eng
}

func TestNormalDeletion(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc, eng := prepareDeletionTest(t, ctrl, false)
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

	resetChildren(&arg)
	err := arg.Prepare(proc)
	require.NoError(t, err)
	_, err = vm.Exec(&arg, proc)
	require.NoError(t, err)

	arg.Reset(proc, false, nil)

	err = arg.Prepare(proc)
	require.NoError(t, err)
	_, err = vm.Exec(&arg, proc)
	require.NoError(t, err)
	arg.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.GetMPool().CurrNB())
}

func TestNormalDeletionError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc, eng := prepareDeletionTest(t, ctrl, true)

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

	resetChildren(&arg)
	err := arg.Prepare(proc)
	require.NoError(t, err)
	_, err = vm.Exec(&arg, proc)
	require.NoError(t, err)

	arg.Reset(proc, false, nil)

	err = arg.Prepare(proc)
	require.Error(t, err)
	arg.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.GetMPool().CurrNB())
}

func resetChildren(arg *Deletion) {
	op := colexec.NewMockOperator()
	bat := colexec.MakeMockBatchsWithRowID()
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

func TestRemoteDeleteFlushesFinalCommittedBatch(t *testing.T) {
	proc := testutil.NewProc(t)
	proc.Ctx = context.Background()
	defer proc.Free()

	SetCNFlushDeletesThreshold(1)
	t.Cleanup(func() {
		SetCNFlushDeletesThreshold(5)
	})

	arg := NewArgument()
	defer arg.Release()
	arg.RemoteDelete = true
	arg.Nbucket = 1
	arg.DeleteCtx = &DeleteCtx{
		Ref: &plan.ObjectRef{
			Obj:        1,
			SchemaName: "fts_debug",
			ObjName:    "docs",
		},
		RowIdIdx:      0,
		PrimaryKeyIdx: 1,
	}

	first := makeRemoteDeleteInputBatch(t, proc, 0, 4000, 512)
	second := makeRemoteDeleteInputBatch(t, proc, 4000, 500, 512)
	arg.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{first, second}))

	require.NoError(t, arg.Prepare(proc))

	result, err := vm.Exec(arg, proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)

	typesVec := vector.MustFixedColWithTypeCheck[int8](result.Batch.GetVector(2))
	flushDeltaRows := 0
	committedRows := 0
	for _, typ := range typesVec {
		switch typ {
		case FlushDeltaLoc:
			flushDeltaRows++
		case DeletionOnCommitted:
			committedRows++
		}
	}
	require.Equal(t, 0, committedRows)
	require.Equal(t, 2, flushDeltaRows)

	deltaLocs, area := vector.MustVarlenaRawData(result.Batch.GetVector(1))
	totalPersistedRows := 0
	for i, typ := range typesVec {
		if typ != FlushDeltaLoc {
			continue
		}
		bat := batch.New([]string{catalog.ObjectMeta_ObjectStats})
		bat.SetVector(0, vector.NewVec(types.T_text.ToType()))
		require.NoError(t, bat.UnmarshalBinary(deltaLocs[i].GetByteSlice(area)))
		stats := objectio.ObjectStats(bat.Vecs[0].GetBytesAt(0))
		totalPersistedRows += int(stats.Rows())
		bat.Clean(proc.GetMPool())
	}
	require.Equal(t, 4500, totalPersistedRows)
}

func makeRemoteDeleteInputBatch(
	t *testing.T,
	proc *process.Process,
	start int,
	rows int,
	pkBytes int,
) *batch.Batch {
	t.Helper()

	bat := batch.New([]string{catalog.Row_ID, "pk"})
	bat.SetVector(0, vector.NewVec(types.T_Rowid.ToType()))
	bat.SetVector(1, vector.NewVec(types.T_text.ToType()))

	var objID types.Objectid
	objID[len(objID)-1] = 1
	pk := strings.Repeat("x", pkBytes)
	for i := 0; i < rows; i++ {
		row := start + i
		rowID := types.NewRowIDWithObjectIDBlkNumAndRowID(
			objID,
			uint16(row/objectio.BlockMaxRows),
			uint32(row%objectio.BlockMaxRows),
		)
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], rowID, false, proc.GetMPool()))
		require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte(pk), false, proc.GetMPool()))
	}
	bat.SetRowCount(rows)
	return bat
}
