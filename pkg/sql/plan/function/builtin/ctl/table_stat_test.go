// Copyright 2021 - 2022 Matrix Origin
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

package ctl

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestMoTableRowsAndTableSize(t *testing.T) {

	v1 := testutil.MakeVarcharVector([]string{""}, []uint64{})
	v2 := testutil.MakeVarcharVector([]string{""}, []uint64{})

	inputVectors := []*vector.Vector{v1, v2}
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc := testutil.NewProc()
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()

	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

	db := mock_frontend.NewMockDatabase(ctrl)
	db.EXPECT().Relations(gomock.Any()).Return(nil, nil).AnyTimes()

	table := mock_frontend.NewMockRelation(ctrl)
	table.EXPECT().Ranges(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	table.EXPECT().TableDefs(gomock.Any()).Return(nil, nil).AnyTimes()
	table.EXPECT().GetPrimaryKeys(gomock.Any()).Return(nil, nil).AnyTimes()
	table.EXPECT().GetHideKeys(gomock.Any()).Return(nil, nil).AnyTimes()
	table.EXPECT().Stats(gomock.Any(), gomock.Any()).Return(false).AnyTimes()

	attrs := []*engine.Attribute{
		{Name: "a", Type: types.T_int32.ToType()},
		{Name: "b", Type: types.T_int64.ToType()},
	}

	table.EXPECT().TableColumns(gomock.Any()).Return(attrs, nil).AnyTimes()

	table.EXPECT().GetTableID(gomock.Any()).Return(uint64(10)).AnyTimes()
	table.EXPECT().Rows(gomock.Any()).Return(int64(10), nil).AnyTimes()
	table.EXPECT().Size(gomock.Any(), gomock.Any()).Return(int64(0), nil).AnyTimes()
	db.EXPECT().Relation(gomock.Any(), gomock.Any()).Return(table, nil).AnyTimes()

	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	eng.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	eng.EXPECT().Rollback(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	eng.EXPECT().Hints().Return(engine.Hints{
		CommitOrRollbackTimeout: time.Second,
	}).AnyTimes()
	eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).Return(db, nil).AnyTimes()
	proc.Ctx = context.WithValue(proc.Ctx, defines.EngineKey{}, eng)
	proc.TxnClient = txnClient
	proc.TxnOperator = txnOperator

	r, err := MoTableRows(inputVectors, proc)
	require.Nil(t, err)
	require.Equal(t, "10", r.String())

	size, err := MoTableSize(inputVectors, proc)
	require.Nil(t, err)
	require.Equal(t, "120", size.String())

}
