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
	"time"

	"github.com/golang/mock/gomock"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	lockpb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

type lineagePublicationLockExec struct {
	backgroundExecTest
	accountID uint32
}

type lineageLifecycleCommitSQLExecutor struct {
	sqls       []string
	opts       []executor.Options
	err        error
	beforeExec func()
}

func (e *lineageLifecycleCommitSQLExecutor) Exec(
	_ context.Context,
	sql string,
	opts executor.Options,
) (executor.Result, error) {
	if e.beforeExec != nil {
		e.beforeExec()
	}
	e.sqls = append(e.sqls, sql)
	e.opts = append(e.opts, opts)
	return executor.Result{}, e.err
}

func (e *lineageLifecycleCommitSQLExecutor) ExecTxn(
	_ context.Context,
	_ func(executor.TxnExecutor) error,
	_ executor.Options,
) error {
	panic("unexpected transaction creation")
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

func TestLineageOwnerLifecycleLockSQLForTxnUsesPessimisticRowLock(t *testing.T) {
	ctrl := gomock.NewController(t)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().Txn().Return(txn.TxnMeta{Mode: txn.TxnMode_Pessimistic})

	require.Equal(t,
		databranchutils.LineageOwnerLifecyclePessimisticLockSQL(),
		databranchutils.LineageOwnerLifecycleLockSQLForTxn(txnOp),
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

func TestValidateDataBranchLineageOwnerLifecycleAtCommitFastFailsStableWrite(t *testing.T) {
	ctrl := gomock.NewController(t)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	sqlExecutor := &lineageLifecycleCommitSQLExecutor{}
	require.NoError(t, validateDataBranchLineageOwnerLifecycleWithExecutor(
		context.Background(), sqlExecutor, txnOp, time.UTC,
	))
	require.Equal(t, []string{databranchutils.LineageOwnerLifecycleLockSQL()}, sqlExecutor.sqls)
	require.Len(t, sqlExecutor.opts, 1)
	opts := sqlExecutor.opts[0]
	require.True(t, opts.DisableIncrStatement())
	require.Same(t, txnOp, opts.Txn())
	require.True(t, opts.KeepTxnAlive())
	require.True(t, opts.HasAccountID())
	require.Equal(t, uint32(catalog.System_Account), opts.AccountID())
	require.Equal(t, lockpb.WaitPolicy_FastFail, opts.StatementOption().WaitPolicy())
	require.True(t, opts.StatementOption().HasAccountID())
	require.Equal(t, uint32(catalog.System_Account), opts.StatementOption().AccountID())

	wantErr := errors.New("validation failed")
	sqlExecutor = &lineageLifecycleCommitSQLExecutor{err: wantErr}
	require.ErrorIs(t,
		validateDataBranchLineageOwnerLifecycleWithExecutor(
			context.Background(), sqlExecutor, txnOp, time.UTC,
		),
		wantErr,
	)
}
