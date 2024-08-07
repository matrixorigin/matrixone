// Copyright 2022 Matrix Origin
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

package preinsert

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

var (
	i64typ     = plan.Type{Id: int32(types.T_int64)}
	varchartyp = plan.Type{Id: int32(types.T_varchar)}
)

func TestPreInsertNormal(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.TODO()
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Rollback(ctx).Return(nil).AnyTimes()

	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	eng.EXPECT().Hints().Return(engine.Hints{
		CommitOrRollbackTimeout: time.Second,
	}).AnyTimes()

	proc := testutil.NewProc()
	proc.Base.TxnClient = txnClient
	proc.Base.SessionInfo.StorageEngine = eng
	argument1 := PreInsert{
		ctr:        container{},
		SchemaName: "testDb",
		TableDef: &plan.TableDef{
			Cols: []*plan.ColDef{
				{Name: "int64_column", Typ: i64typ},
				{Name: "scalar_int64", Typ: i64typ},
				{Name: "varchar_column", Typ: varchartyp},
				{Name: "scalar_varchar", Typ: varchartyp},
				{Name: "int64_column", Typ: i64typ},
			},
			Pkey: &plan.PrimaryKeyDef{},
		},
		Attrs:      []string{"int64_column", "scalar_int64", "varchar_column", "scalar_varchar", "int64_column"},
		IsUpdate:   false,
		HasAutoCol: false,
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{
				Idx:     0,
				IsFirst: false,
				IsLast:  false,
			},
		},
	}
	resetChildren(&argument1)
	err := argument1.Prepare(proc)
	require.NoError(t, err)
	_, err = argument1.Call(proc)
	require.NoError(t, err)
	argument1.Reset(proc, false, nil)
	resetChildren(&argument1)
	err = argument1.Prepare(proc)
	require.NoError(t, err)
	_, err = argument1.Call(proc)
	require.NoError(t, err)
	argument1.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.GetMPool().CurrNB())
}

func TestPreInsertNullCheck(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.TODO()
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Rollback(ctx).Return(nil).AnyTimes()

	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	eng.EXPECT().Hints().Return(engine.Hints{
		CommitOrRollbackTimeout: time.Second,
	}).AnyTimes()

	proc := testutil.NewProc()
	proc.Base.TxnClient = txnClient
	proc.Ctx = ctx
	proc.Base.SessionInfo.StorageEngine = eng
	argument2 := PreInsert{
		ctr:        container{},
		SchemaName: "testDb",
		Attrs:      []string{"int64_column_primary"},
		TableDef: &plan.TableDef{
			Cols: []*plan.ColDef{
				{Name: "int64_column_primary", Primary: true, Typ: i64typ,
					Default: &plan.Default{
						NullAbility: false,
					},
				},
			},
			Pkey: &plan.PrimaryKeyDef{
				PkeyColName: "int64_column_primary",
			},
		},
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{
				Idx:     1,
				IsFirst: false,
				IsLast:  false,
			},
		},
	}

	resetChildren(&argument2)
	err2 := argument2.Prepare(proc)
	require.NoError(t, err2)
	_, err2 = argument2.Call(proc)
	require.Error(t, err2, "should return error when insert null into primary key column")
	argument2.Reset(proc, false, nil)
	resetChildren(&argument2)
	err2 = argument2.Prepare(proc)
	require.NoError(t, err2)
	_, err2 = argument2.Call(proc)
	require.Error(t, err2, "should return error when insert null into primary key column")
	argument2.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.GetMPool().CurrNB())
}

func resetChildren(arg *PreInsert) {
	bat := colexec.MakeMockBatchsWithNullVec()
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	arg.Children = nil
	arg.AppendChild(op)
}
