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

package seq

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestCurrvalSingle(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc := testutil.NewProc()
	proc.SessionInfo.SeqCurValues = make(map[uint64]string)
	proc.SessionInfo.SeqCurValues[uint64(10)] = "1000"

	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()

	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

	db := mock_frontend.NewMockDatabase(ctrl)
	table := mock_frontend.NewMockRelation(ctrl)

	// Table id is 10.
	table.EXPECT().GetTableID(gomock.Any()).Return(uint64(10)).AnyTimes()
	db.EXPECT().Relation(gomock.Any(), "t2").Return(table, nil).AnyTimes()

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

	tests := []struct {
		name    string
		vectors []*vector.Vector
	}{
		{
			name: "test01",
			vectors: []*vector.Vector{
				testutil.MakeVarcharVector([]string{"t2"}, []uint64{}),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := Currval(tt.vectors, proc)
			if err != nil {
				t.Errorf("Currval() error = %v", err)
				return
			}
			require.Nil(t, err)
			require.Equal(t, "1000", r.String())
		})
	}
}

func TestCurrvalMulti(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc := testutil.NewProc()
	proc.SessionInfo.SeqCurValues = make(map[uint64]string)
	proc.SessionInfo.SeqCurValues[uint64(10)] = "1000"
	proc.SessionInfo.SeqCurValues[uint64(20)] = "876"

	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()

	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

	db := mock_frontend.NewMockDatabase(ctrl)
	table1 := mock_frontend.NewMockRelation(ctrl)
	table2 := mock_frontend.NewMockRelation(ctrl)

	// t1 got table id 10.
	table1.EXPECT().GetTableID(gomock.Any()).Return(uint64(10)).AnyTimes()
	db.EXPECT().Relation(gomock.Any(), "t1").Return(table1, nil).AnyTimes()
	// t2 got table id 20.
	table2.EXPECT().GetTableID(gomock.Any()).Return(uint64(20)).AnyTimes()
	db.EXPECT().Relation(gomock.Any(), "t2").Return(table2, nil).AnyTimes()

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

	tests := []struct {
		name    string
		vectors []*vector.Vector
	}{
		{
			name: "test01",
			vectors: []*vector.Vector{
				testutil.MakeVarcharVector([]string{"t1", "t2"}, []uint64{}),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := Currval(tt.vectors, proc)
			if err != nil {
				t.Errorf("Currval() error = %v", err)
				return
			}
			require.Nil(t, err)
			ress := vector.MustStrCol(r)
			require.Equal(t, "1000", ress[0])
			require.Equal(t, "876", ress[1])
		})
	}
}
