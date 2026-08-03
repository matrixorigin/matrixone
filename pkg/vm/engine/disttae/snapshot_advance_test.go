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
	"testing"

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
