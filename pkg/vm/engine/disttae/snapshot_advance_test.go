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
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
)

func TestAdvanceSnapshotBranches(t *testing.T) {
	ctx := context.Background()
	target := timestamp.Timestamp{PhysicalTime: 20}

	t.Run("non-RC delegates snapshot update", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		op := mock_frontend.NewMockTxnOperator(ctrl)
		op.EXPECT().EnterIncrStmt()
		op.EXPECT().ExitIncrStmt()
		op.EXPECT().Txn().Return(txn.TxnMeta{Isolation: txn.TxnIsolation_SI})
		op.EXPECT().UpdateSnapshot(ctx, target).Return(nil)

		workspace := &Transaction{op: op}
		require.NoError(t, workspace.AdvanceSnapshot(ctx, target))
		require.True(t, workspace.transfer.lastTransferred.IsEmpty())
	})

	t.Run("RC initializes transfer boundary and preserves update error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		op := mock_frontend.NewMockTxnOperator(ctrl)
		initial := timestamp.Timestamp{PhysicalTime: 10}
		updateErr := errors.New("update snapshot failed")
		op.EXPECT().EnterIncrStmt()
		op.EXPECT().ExitIncrStmt()
		op.EXPECT().Txn().Return(txn.TxnMeta{Isolation: txn.TxnIsolation_RC})
		op.EXPECT().SnapshotTS().Return(initial)
		op.EXPECT().UpdateSnapshot(ctx, target).Return(updateErr)

		workspace := &Transaction{op: op}
		err := workspace.AdvanceSnapshot(ctx, target)
		require.ErrorIs(t, err, updateErr)
		require.Equal(t, types.TimestampToTS(initial), workspace.transfer.lastTransferred)
		require.False(t, workspace.start.IsZero())
	})
}

func TestStatementRollbackRestoresTransferState(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		frontier, snapshot   int64
		pending, wantPending bool
	}{
		{name: "completed transfer", frontier: 20, snapshot: 20},
		{name: "snapshot ahead of transfer", frontier: 10, snapshot: 20, wantPending: true},
		{name: "existing debt", frontier: 20, snapshot: 20, pending: true, wantPending: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			op := mock_frontend.NewMockTxnOperator(ctrl)
			op.EXPECT().EnterRollbackStmt().Times(2)
			op.EXPECT().ExitRollbackStmt().Times(2)
			op.EXPECT().Txn().Return(txn.TxnMeta{Isolation: txn.TxnIsolation_RC}).AnyTimes()
			op.EXPECT().SnapshotTS().Return(timestamp.Timestamp{PhysicalTime: tc.snapshot}).Times(2)
			workspace := &Transaction{
				op: op, statementID: 2, offsets: []int{0, 0}, isCCPRTxn: true,
				tableCache: new(sync.Map), tableOps: newTableOps(), databaseOps: newDbOps(),
				batchSelectList: make(map[*batch.Batch][]int64), deletedBlocks: &deletedBlocks{},
			}
			first := statementTransferState{lastTransferred: types.BuildTS(5, 0)}
			entry := statementTransferState{lastTransferred: types.BuildTS(tc.frontier, 0), pendingTransfer: tc.pending}
			workspace.transfer.statements = []statementTransferState{first, entry}
			workspace.transfer.lastTransferred = types.BuildTS(30, 0)
			require.NoError(t, workspace.RollbackLastStatement(context.Background()))
			require.Equal(t, entry.lastTransferred, workspace.transfer.lastTransferred)
			require.Equal(t, tc.wantPending, workspace.transfer.pendingTransfer)
			require.Equal(t, []statementTransferState{first}, workspace.transfer.statements)
			// Rolling back the first statement must retain its original frontier,
			// not initialize recovery from the already advanced operator snapshot.
			require.NoError(t, workspace.RollbackLastStatement(context.Background()))
			require.Equal(t, first.lastTransferred, workspace.transfer.lastTransferred)
			require.True(t, workspace.transfer.pendingTransfer)
			require.Empty(t, workspace.transfer.statements)
		})
	}
}
