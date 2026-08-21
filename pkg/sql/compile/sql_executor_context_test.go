// Copyright 2023 Matrix Origin
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

package compile

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"

	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/util/resource"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
)

func TestSQLExecutorCompilerContextReservesLifecycleRestoreStagingForFrontend(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	proc.Base.IsFrontend = true
	ctx := &compilerContext{
		ctx:  context.Background(),
		proc: proc,
	}
	_, _, err := ctx.Resolve(
		"history",
		catalog.LifecycleRestoreTableNamePrefix+
			"0123456789abcdef0123456789abcdef",
		nil,
	)
	require.ErrorContains(t, err, "Lifecycle Restore staging")

	proc.Base.IsFrontend = false
	_, _, err = ctx.Resolve(
		"",
		catalog.LifecycleRestoreTableNamePrefix+
			"0123456789abcdef0123456789abcdef",
		nil,
	)
	require.NoError(t, err)
}

func TestInternalExecutorCommittedLogWaitHonorsCallerCancellation(t *testing.T) {
	ctrl := gomock.NewController(t)
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	commitTS := timestamp.Timestamp{PhysicalTime: 42}
	txnOperator.EXPECT().Txn().Return(txn.TxnMeta{CommitTS: commitTS})

	waitStarted := make(chan struct{})
	txnClient.EXPECT().WaitLogTailAppliedAt(gomock.Any(), commitTS).DoAndReturn(
		func(ctx context.Context, _ timestamp.Timestamp) (timestamp.Timestamp, error) {
			close(waitStarted)
			<-ctx.Done()
			return timestamp.Timestamp{}, ctx.Err()
		},
	)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	errC := make(chan error, 1)
	go func() {
		errC <- (&sqlExecutor{txnClient: txnClient}).maybeWaitCommittedLogApplied(
			ctx,
			executor.Options{}.
				WithTxn(txnOperator).
				WithWaitCommittedLogApplied(),
		)
	}()

	select {
	case <-waitStarted:
	case <-time.After(time.Second):
		t.Fatal("committed-log wait did not start")
	}
	cancel()
	select {
	case err := <-errC:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("caller cancellation did not release committed-log wait")
	}
}

func TestInternalExecutorCommittedLogWaitContract(t *testing.T) {
	waitErr := errors.New("logtail wait failed")
	commitTS := timestamp.Timestamp{PhysicalTime: 42}

	for _, tc := range []struct {
		name      string
		wait      bool
		commitTS  timestamp.Timestamp
		wantErr   error
		wantCalls int
	}{
		{name: "disabled", commitTS: commitTS},
		{name: "empty commit timestamp", wait: true},
		{name: "success", wait: true, commitTS: commitTS, wantCalls: 1},
		{name: "wait error", wait: true, commitTS: commitTS, wantErr: waitErr, wantCalls: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			txnClient := mock_frontend.NewMockTxnClient(ctrl)
			txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
			if tc.wait {
				txnOperator.EXPECT().Txn().Return(txn.TxnMeta{CommitTS: tc.commitTS})
			}
			if tc.wantCalls == 1 {
				txnClient.EXPECT().WaitLogTailAppliedAt(gomock.Any(), tc.commitTS).
					Return(timestamp.Timestamp{}, tc.wantErr)
			}
			opts := executor.Options{}.WithTxn(txnOperator)
			if tc.wait {
				opts = opts.WithWaitCommittedLogApplied()
			}

			err := (&sqlExecutor{txnClient: txnClient}).maybeWaitCommittedLogApplied(
				context.Background(), opts)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}

func TestNewInternalStatementContextPreservesRootAndClaimsStatsOnce(t *testing.T) {
	root := resource.NewRoot(resource.ConnExternal)
	parentStats := statistic.NewStatsInfo()
	parent := resource.ContextWithRoot(
		statistic.ContextWithStatsInfo(context.Background(), parentStats),
		root)

	child := newInternalStatementContext(parent)
	childStats := statistic.StatsInfoFromContext(child)
	childAgain := newInternalStatementContext(parent)
	childAgainStats := statistic.StatsInfoFromContext(childAgain)

	require.Same(t, root, resource.RootFromContext(child))
	require.NotNil(t, childStats)
	require.NotSame(t, parentStats, childStats)
	require.NotSame(t, childStats, childAgainStats)

	for _, stats := range []*statistic.StatsInfo{parentStats, childStats, childAgainStats} {
		_, ok := stats.ClaimRootPhaseResource()
		require.True(t, ok)
		_, ok = stats.ClaimRootPhaseResource()
		require.False(t, ok)
	}
}

func Test_panic(t *testing.T) {
	r := func() {
		err := recover()
		require.Equal(t, "not supported in internal sql executor", err)
	}

	c := &compilerContext{}

	func() {
		defer r()
		_ = c.CheckSubscriptionValid("", "", "")
	}()

	func() {
		defer r()
		_, _ = c.IsPublishing("")
	}()

	func() {
		defer r()
		c.SetQueryingSubscription(nil)
	}()

	func() {
		defer r()
		_, _ = c.ResolveUdf("", nil)
	}()

	func() {
		defer r()
		_, _ = c.ResolveAccountIds(nil)
	}()

	func() {
		defer r()
		_, _, _ = c.GetQueryResultMeta("")
	}()
}

func TestCompilerContext_Database(t *testing.T) {
	ctrl := gomock.NewController(t)
	database := mock_frontend.NewMockDatabase(ctrl)
	database.EXPECT().GetDatabaseId(gomock.Any()).Return("1")
	engine := mock_frontend.NewMockEngine(ctrl)
	engine.EXPECT().Database(gomock.Any(), "", nil).Return(database, nil).Times(2)

	c := &compilerContext{
		proc:   testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
		engine: engine,
	}

	exists := c.DatabaseExists("", &plan.Snapshot{})
	require.Equal(t, exists, true)

	_, err := c.GetDatabaseId("", &plan.Snapshot{})
	require.Nil(t, err)

	sql := c.GetRootSql()
	require.Equal(t, sql, "")
}

func TestCompilerContextBuildTableDefByMoColumns(t *testing.T) {
	ctrl := gomock.NewController(t)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	tableDef := &plan.TableDef{
		Name: "src",
		Cols: []*plan.ColDef{
			{Name: "a"},
			{Name: "b"},
		},
	}
	relation := mock_frontend.NewMockRelation(ctrl)
	relation.EXPECT().GetTableDef(gomock.Any()).Return(tableDef)
	relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(42))
	database := mock_frontend.NewMockDatabase(ctrl)
	database.EXPECT().Relation(gomock.Any(), "src", nil).Return(relation, nil)
	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().Database(gomock.Any(), "db", gomock.Any()).Return(database, nil)

	c := &compilerContext{
		defaultDB: "db",
		engine:    eng,
		proc:      proc,
	}
	actual, err := c.BuildTableDefByMoColumns("db", "src")
	require.NoError(t, err)
	require.Equal(t, tableDef.Cols, actual.Cols)
	require.NotSame(t, tableDef, actual)
}

func TestCompilerContextBuildTableDefByMoColumnsPropagatesRelationError(t *testing.T) {
	ctrl := gomock.NewController(t)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	relationErr := moerr.NewInternalErrorNoCtx("relation failed")

	database := mock_frontend.NewMockDatabase(ctrl)
	database.EXPECT().Relation(gomock.Any(), "src", nil).Return(nil, relationErr)
	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().Database(gomock.Any(), "db", gomock.Any()).Return(database, nil)

	c := &compilerContext{
		defaultDB: "db",
		engine:    eng,
		proc:      proc,
	}
	actual, err := c.BuildTableDefByMoColumns("db", "src")
	require.Nil(t, actual)
	require.ErrorIs(t, err, relationErr)
}

func TestCompilerContextBuildTableDefByMoColumnsNoSuchTable(t *testing.T) {
	ctrl := gomock.NewController(t)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())

	database := mock_frontend.NewMockDatabase(ctrl)
	database.EXPECT().Relation(gomock.Any(), "src", nil).Return(nil, nil)
	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().Database(gomock.Any(), "db", gomock.Any()).Return(database, nil)

	c := &compilerContext{
		defaultDB: "db",
		engine:    eng,
		proc:      proc,
	}
	actual, err := c.BuildTableDefByMoColumns("db", "src")
	require.Nil(t, actual)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrNoSuchTable))
}
