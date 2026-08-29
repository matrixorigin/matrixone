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

package frontend

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

type lineagePublicationLockExec struct {
	backgroundExecTest
	accountID uint32
}

func TestLockRestoreLineageOwnerLifecycleCoversWholeCatalogRestore(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), 42)
	gateSQL := databranchutils.LineageOwnerLifecycleLockSQL()

	for _, level := range []tree.RestoreLevel{
		tree.RESTORELEVELCLUSTER,
		tree.RESTORELEVELACCOUNT,
	} {
		t.Run(level.String(), func(t *testing.T) {
			bh := &lineagePublicationLockExec{}
			bh.init()
			require.NoError(t, lockRestoreLineageOwnerLifecycle(ctx, bh, level))
			require.Equal(t, []string{gateSQL}, bh.executedSQLs)
			require.Equal(t, uint32(catalog.System_Account), bh.accountID)
		})
	}

	for _, level := range []tree.RestoreLevel{
		tree.RESTORELEVELDATABASE,
		tree.RESTORELEVELTABLE,
	} {
		t.Run(level.String(), func(t *testing.T) {
			bh := &lineagePublicationLockExec{}
			bh.init()
			require.NoError(t, lockRestoreLineageOwnerLifecycle(ctx, bh, level))
			require.Empty(t, bh.executedSQLs)
		})
	}

	t.Run("gate error aborts restore admission", func(t *testing.T) {
		bh := &lineagePublicationLockExec{}
		bh.init()
		wantErr := errors.New("lifecycle gate failed")
		bh.sql2err[gateSQL] = wantErr
		require.ErrorIs(t,
			lockRestoreLineageOwnerLifecycle(ctx, bh, tree.RESTORELEVELACCOUNT),
			wantErr,
		)
		require.Equal(t, []string{gateSQL}, bh.executedSQLs)
	})
}

func (e *lineagePublicationLockExec) Exec(ctx context.Context, sql string) error {
	e.accountID, _ = defines.GetAccountId(ctx)
	return e.backgroundExecTest.Exec(ctx, sql)
}

func TestLockDataBranchLineageOwnerLifecycleUsesSystemAccount(t *testing.T) {
	bh := &lineagePublicationLockExec{}
	bh.init()
	ctx := defines.AttachAccountId(context.Background(), 42)

	require.NoError(t, lockDataBranchLineageOwnerLifecycle(ctx, bh))
	require.Equal(t, uint32(catalog.System_Account), bh.accountID)
	require.Equal(t,
		[]string{databranchutils.LineageOwnerLifecycleLockSQL()},
		bh.executedSQLs,
	)
}

func TestGetDataBranchMutationExecutorAdmitsBeforeMutation(t *testing.T) {
	for _, featureLimited := range []bool{false, true} {
		t.Run(fmt.Sprintf("feature-limited=%t", featureLimited), func(t *testing.T) {
			ctrl := gomock.NewController(t)
			ses := newTestSession(t, ctrl)
			t.Cleanup(ses.Close)
			txnOp := mock_frontend.NewMockTxnOperator(ctrl)
			txnOp.EXPECT().TxnOptions().Return(txn.TxnOptions{}).Times(2)
			ses.proc.Base.TxnOperator = txnOp

			bh := &backgroundExecTestWithHistory{}
			bh.init()
			stub := gostub.StubFunc(&NewBackgroundExec, bh)
			t.Cleanup(stub.Reset)

			returned, cleanup, err := getDataBranchMutationExecutor(
				context.Background(), ses, featureLimited)
			require.NoError(t, err)
			require.Same(t, bh, returned)
			require.NotNil(t, cleanup)
			require.Equal(t, []string{
				"begin",
				databranchutils.LineageOwnerLifecycleLockSQL(),
			}, bh.executedSqls)
			require.NoError(t, cleanup(nil))
			require.Equal(t, "commit;", bh.executedSqls[len(bh.executedSqls)-1])
		})
	}
}

func TestGetDataBranchMutationExecutorRollsBackOnAdmissionFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
	t.Cleanup(ses.Close)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().TxnOptions().Return(txn.TxnOptions{}).Times(2)
	ses.proc.Base.TxnOperator = txnOp

	bh := &backgroundExecTestWithHistory{}
	bh.init()
	wantErr := errors.New("lifecycle gate failed")
	gateSQL := databranchutils.LineageOwnerLifecycleLockSQL()
	bh.sql2err[gateSQL] = wantErr
	stub := gostub.StubFunc(&NewBackgroundExec, bh)
	t.Cleanup(stub.Reset)

	returned, cleanup, err := getDataBranchMutationExecutor(context.Background(), ses, false)
	require.ErrorIs(t, err, wantErr)
	require.Nil(t, returned)
	require.Nil(t, cleanup)
	require.Equal(t, []string{"begin", gateSQL, "rollback;"}, bh.executedSqls)
}

func TestGetDataBranchMutationExecutorRejectsExplicitTransactionBeforeAdmission(t *testing.T) {
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
	t.Cleanup(ses.Close)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().TxnOptions().Return(txn.TxnOptions{ByBegin: true})
	ses.proc.Base.TxnOperator = txnOp

	returned, cleanup, err := getDataBranchMutationExecutor(context.Background(), ses, true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "DATA BRANCH create/delete is not supported inside an explicit transaction")
	require.Nil(t, returned)
	require.Nil(t, cleanup)
}
