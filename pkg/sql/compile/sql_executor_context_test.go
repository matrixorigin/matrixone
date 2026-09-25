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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/process"

	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/util/resource"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
)

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

func TestInternalStatementContextSuppressesInheritedJSONMergeWarning(t *testing.T) {
	parent := plan.WithJSONMergeWarningContext(
		context.Background(), nil, plan.JSONMergeWarningUser)
	internal := markInternalJSONMergeWarningContext(
		newInternalStatementContext(parent))

	origin, ok := plan.JSONMergeWarningOriginFromContext(internal)
	require.True(t, ok)
	require.Equal(t, plan.JSONMergeWarningInternalReprepare, origin)
}

func TestCompilerContextUnsupportedOperations(t *testing.T) {
	r := func() {
		err := recover()
		require.Equal(t, "not supported in internal sql executor", err)
	}

	c := &compilerContext{}

	meta, err := c.GetSubscriptionMeta("", nil)
	require.NoError(t, err)
	require.Nil(t, meta)
	require.Error(t, c.CheckSubscriptionValid("", "", ""))
	_, err = c.IsPublishing("")
	require.Error(t, err)
	c.SetQueryingSubscription(nil)

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

type recordingSessionCompilerContext struct {
	*plan.MockCompilerContext
	snapshot             *plan.Snapshot
	subscription         *plan.SubscriptionMeta
	queryingSubscription *plan.SubscriptionMeta
	checkedSubscription  []string
	resolvedDatabase     string
	resolvedTable        string
	resolvedTableDef     *plan.TableDef
	proc                 *process.Process
}

func (c *recordingSessionCompilerContext) GetProcess() *process.Process { return c.proc }

func (c *recordingSessionCompilerContext) ResolveSnapshotWithSnapshotName(name string) (*plan.Snapshot, error) {
	if name != "daily" {
		return nil, moerr.NewInternalErrorNoCtx("unexpected snapshot")
	}
	return c.snapshot, nil
}

func (c *recordingSessionCompilerContext) GetSubscriptionMeta(
	dbName string,
	snapshot *plan.Snapshot,
) (*plan.SubscriptionMeta, error) {
	if dbName != "sub" || snapshot != c.snapshot {
		return nil, moerr.NewInternalErrorNoCtx("unexpected subscription binding")
	}
	return c.subscription, nil
}

func (c *recordingSessionCompilerContext) CheckSubscriptionValid(subName, accountName, pubName string) error {
	c.checkedSubscription = []string{subName, accountName, pubName}
	return nil
}

func (c *recordingSessionCompilerContext) SetQueryingSubscription(meta *plan.SubscriptionMeta) {
	c.queryingSubscription = meta
}

func (c *recordingSessionCompilerContext) GetQueryingSubscription() *plan.SubscriptionMeta {
	return c.queryingSubscription
}

func (c *recordingSessionCompilerContext) Resolve(
	databaseName string,
	tableName string,
	_ *plan.Snapshot,
) (*plan.ObjectRef, *plan.TableDef, error) {
	c.resolvedDatabase = databaseName
	c.resolvedTable = tableName
	return &plan.ObjectRef{}, c.resolvedTableDef, nil
}

func TestCompilerContextDelegatesSnapshotAndSubscriptionBinding(t *testing.T) {
	snapshot := &plan.Snapshot{}
	subscription := &plan.SubscriptionMeta{Name: "pub", SubName: "sub"}
	delegate := &recordingSessionCompilerContext{
		MockCompilerContext: plan.NewMockCompilerContext(false),
		snapshot:            snapshot, subscription: subscription,
		resolvedTableDef: &plan.TableDef{Name: "physical_source"},
	}
	proc := testutil.NewProcess(t)
	c := &compilerContext{
		ctx:  attachInternalExecutorCompilerContext(context.Background(), delegate),
		proc: proc,
	}

	actualSnapshot, err := c.ResolveSnapshotWithSnapshotName("daily")
	require.NoError(t, err)
	require.Same(t, snapshot, actualSnapshot)
	actualSubscription, err := c.GetSubscriptionMeta("sub", snapshot)
	require.NoError(t, err)
	require.Same(t, subscription, actualSubscription)
	require.NoError(t, c.CheckSubscriptionValid("sub", "publisher", "pub"))
	require.Equal(t, []string{"sub", "publisher", "pub"}, delegate.checkedSubscription)
	c.SetQueryingSubscription(subscription)
	require.Same(t, subscription, c.GetQueryingSubscription())
	_, resolved, err := c.Resolve("subscription_db", "source", snapshot)
	require.NoError(t, err)
	require.Same(t, delegate.resolvedTableDef, resolved)
	require.Equal(t, "subscription_db", delegate.resolvedDatabase)
	require.Equal(t, "source", delegate.resolvedTable)
	for _, skipMeta := range []bool{false, true} {
		indexRef, _, err := c.ResolveIndexTableByRef(&plan.ObjectRef{SchemaName: "db", NotLockMeta: skipMeta}, "hidden_index", nil)
		require.NoError(t, err)
		require.Equal(t, skipMeta, indexRef.NotLockMeta)
		require.Equal(t, "hidden_index", delegate.resolvedTable)
	}
}

type isolatedViewTestDelegate struct {
	*recordingSessionCompilerContext
	child *recordingSessionCompilerContext
	err   error
}

func (d *isolatedViewTestDelegate) NewViewDescriptionCompilerContext(
	_ context.Context,
) (plan.CompilerContext, func(), error) {
	if d.err != nil {
		return nil, nil, d.err
	}
	return d.child, func() {}, nil
}

func TestInternalExecutorViewChildDoesNotMutateParent(t *testing.T) {
	proc := testutil.NewProcess(t)
	original := proc.GetTopContext()
	parentSubscription := &plan.SubscriptionMeta{Name: "parent"}
	delegate := &isolatedViewTestDelegate{
		recordingSessionCompilerContext: &recordingSessionCompilerContext{
			MockCompilerContext:  plan.NewMockCompilerContext(false),
			queryingSubscription: parentSubscription,
		},
		child: &recordingSessionCompilerContext{MockCompilerContext: plan.NewMockCompilerContext(false)},
	}
	parent := &compilerContext{proc: proc, ctx: attachInternalExecutorCompilerContext(original, delegate)}
	childContext := context.WithValue(original, struct{}{}, "child")
	binding, cleanup, err := parent.NewViewDescriptionCompilerContext(childContext)
	require.NoError(t, err)
	defer cleanup()
	child := binding.(*compilerContext)
	require.NotSame(t, proc, child.GetProcess())
	require.NotSame(t, proc.Base, child.GetProcess().Base)
	require.Same(t, child, child.GetProcess().GetSessionInfo().CompilerContext)
	child.SetContext(context.WithValue(child.GetContext(), struct{}{}, "nested"))
	childSubscription := &plan.SubscriptionMeta{Name: "publisher"}
	child.SetQueryingSubscription(childSubscription)
	require.Same(t, childSubscription, child.GetQueryingSubscription())
	require.Same(t, childSubscription, delegate.child.GetQueryingSubscription())
	require.Same(t, parentSubscription, delegate.GetQueryingSubscription())
	require.Same(t, original, proc.GetTopContext())
	require.Same(t, child.GetContext(), child.GetProcess().GetTopContext())
	child.SetQueryingSubscription(nil)
	require.Same(t, parentSubscription, delegate.GetQueryingSubscription())
}

func TestInternalExecutorViewChildUsesDelegateProcess(t *testing.T) {
	proc := testutil.NewProcess(t)
	original := proc.GetTopContext()
	separate := proc.NewViewBindingProcess(original)
	defer separate.Free()
	delegate := &isolatedViewTestDelegate{
		recordingSessionCompilerContext: &recordingSessionCompilerContext{MockCompilerContext: plan.NewMockCompilerContext(false)},
		child: &recordingSessionCompilerContext{
			MockCompilerContext: plan.NewMockCompilerContext(false), proc: separate,
		},
	}
	parent := &compilerContext{proc: proc, ctx: attachInternalExecutorCompilerContext(original, delegate)}
	binding, cleanup, err := parent.NewViewDescriptionCompilerContext(original)
	require.NoError(t, err)
	defer cleanup()
	child := binding.(*compilerContext)
	require.Same(t, separate, child.GetProcess())
	child.SetContext(context.WithValue(original, struct{}{}, "nested"))
	require.Same(t, original, proc.GetTopContext())
	require.Same(t, child.GetContext(), separate.GetTopContext())
}

func TestInternalExecutorViewChildPropagatesDelegateFailure(t *testing.T) {
	proc := testutil.NewProcess(t)
	original := proc.GetTopContext()
	delegate := &isolatedViewTestDelegate{
		recordingSessionCompilerContext: &recordingSessionCompilerContext{MockCompilerContext: plan.NewMockCompilerContext(false)},
		err:                             errors.New("cannot create child"),
	}
	parent := &compilerContext{proc: proc, ctx: attachInternalExecutorCompilerContext(original, delegate)}
	binding, cleanup, err := parent.NewViewDescriptionCompilerContext(original)
	require.ErrorContains(t, err, "cannot create child")
	require.Nil(t, binding)
	require.Nil(t, cleanup)
	require.Same(t, original, proc.GetTopContext())
}

func TestInternalExecutorViewChildRejectsUnisolatedDelegate(t *testing.T) {
	proc := testutil.NewProcess(t)
	original := proc.GetTopContext()
	delegate := &recordingSessionCompilerContext{MockCompilerContext: plan.NewMockCompilerContext(false)}
	parent := &compilerContext{proc: proc, ctx: attachInternalExecutorCompilerContext(original, delegate)}
	binding, cleanup, err := parent.NewViewDescriptionCompilerContext(original)
	require.ErrorContains(t, err, "cannot isolate view binding")
	require.Nil(t, binding)
	require.Nil(t, cleanup)
	require.Same(t, original, proc.GetTopContext())
}

func TestInternalExecutorViewChildWithoutDelegateIsIsolated(t *testing.T) {
	proc := testutil.NewProcess(t)
	original := proc.GetTopContext()
	parent := &compilerContext{proc: proc, ctx: original}
	binding, cleanup, err := parent.NewViewDescriptionCompilerContext(original)
	require.NoError(t, err)
	defer cleanup()
	child := binding.(*compilerContext)
	require.NotSame(t, proc.Base, child.GetProcess().Base)
	child.SetContext(context.WithValue(original, struct{}{}, "child"))
	child.SetQueryingSubscription(&plan.SubscriptionMeta{Name: "publisher"})
	require.Equal(t, "publisher", child.GetQueryingSubscription().Name)
	require.Same(t, child.GetContext(), child.GetProcess().GetTopContext())
	require.Same(t, child.GetContext(), child.GetProcess().Ctx)
	require.Same(t, original, proc.GetTopContext())
	require.Nil(t, parent.GetQueryingSubscription())
}

func TestInternalExecutorSubscriptionViewWithoutFrontendDelegate(t *testing.T) {
	ctrl := gomock.NewController(t)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	relation := mock_frontend.NewMockRelation(ctrl)
	relation.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{Name: "v"})
	relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(42))
	database := mock_frontend.NewMockDatabase(ctrl)
	database.EXPECT().Relation(gomock.Any(), "v", nil).Return(relation, nil)
	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().GetNameById(gomock.Any(), nil, uint64(42)).Return("db", "v", nil)
	eng.EXPECT().Database(gomock.Any(), "db", nil).Return(database, nil)

	parent := &compilerContext{proc: proc, engine: eng, ctx: proc.GetTopContext()}
	binding, cleanup, err := parent.NewViewDescriptionCompilerContext(proc.GetTopContext())
	require.NoError(t, err)
	defer cleanup()
	child := binding.(*compilerContext)
	child.SetContext(defines.AttachAccountId(child.GetContext(), 23))
	obj, def, err := child.ResolveSubscriptionTableById(42, &plan.SubscriptionMeta{AccountId: 23})
	require.NoError(t, err)
	require.Equal(t, "v", obj.ObjName)
	require.Equal(t, "v", def.Name)
	require.Nil(t, parent.GetQueryingSubscription())
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

func TestCompilerContextResolveDatabaseErrors(t *testing.T) {
	for _, tc := range []struct {
		name        string
		databaseErr error
		wantMissing bool
	}{
		{
			name:        "ExpectedEOB is a missing database",
			databaseErr: moerr.GetOkExpectedEOB(),
			wantMissing: true,
		},
		{
			name:        "unexpected database error is preserved",
			databaseErr: moerr.NewInternalErrorNoCtx("database lookup failed"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			eng := mock_frontend.NewMockEngine(ctrl)
			eng.EXPECT().Database(gomock.Any(), "db", gomock.Any()).Return(nil, tc.databaseErr)

			c := &compilerContext{
				defaultDB: "db",
				engine:    eng,
				proc:      proc,
			}
			obj, tableDef, err := c.Resolve("db", "missing", nil)
			if tc.wantMissing {
				require.NoError(t, err)
				require.Nil(t, obj)
				require.Nil(t, tableDef)
				return
			}
			require.ErrorIs(t, err, tc.databaseErr)
			require.Nil(t, obj)
			require.Nil(t, tableDef)
		})
	}
}

func TestInternalCompilerContextDropTableIfExistsExpectedEOBNoop(t *testing.T) {
	ctrl := gomock.NewController(t)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().Database(gomock.Any(), "gone", gomock.Any()).
		Return(nil, moerr.GetOkExpectedEOB())
	c := &compilerContext{engine: eng, proc: proc}

	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL,
		"drop table if exists gone.__mo_tmp_table", 1)
	require.NoError(t, err)
	defer stmt.Free()

	p, err := plan.BuildPlan(c, stmt, false)
	require.NoError(t, err)
	drop := p.GetDdl().GetDropTable()
	require.Equal(t, "gone", drop.GetDatabase())
	require.Equal(t, "__mo_tmp_table", drop.GetTable())
	require.Nil(t, drop.GetTableDef())
}

// CTAS follow-up SQL is a replay of the user's own statement, so a variable
// that shaped the plan of that statement must shape the replay identically.
// Internal SQL with no attached frontend context has no user session whose
// variables could apply and keeps answering nil.
func TestCompilerContextResolveVariableDelegatesToAttachedSession(t *testing.T) {
	type resolved struct {
		name               string
		isSystem, isGlobal bool
	}
	var seen []resolved
	delegate := plan.NewMockCompilerContext(false)
	delegate.ResolveVariableFunc = func(name string, isSystemVar, isGlobalVar bool) (interface{}, error) {
		seen = append(seen, resolved{name, isSystemVar, isGlobalVar})
		if name == "sql_mode" {
			return "ONLY_FULL_GROUP_BY,ENABLE_BOOL_SUMAVG", nil
		}
		return nil, moerr.NewInternalErrorNoCtx("unexpected variable")
	}

	attached := &compilerContext{
		ctx:  attachInternalExecutorCompilerContext(context.Background(), delegate),
		proc: testutil.NewProcess(t),
	}
	value, err := attached.ResolveVariable("sql_mode", true, false)
	require.NoError(t, err)
	require.Equal(t, "ONLY_FULL_GROUP_BY,ENABLE_BOOL_SUMAVG", value)
	require.Equal(t, []resolved{{"sql_mode", true, false}}, seen)

	// An error from the session must reach the caller rather than being
	// flattened into the nil default, which would silently compile the replay
	// under different rules than the statement the user ran.
	_, err = attached.ResolveVariable("other", true, false)
	require.Error(t, err)

	detached := &compilerContext{ctx: context.Background(), proc: testutil.NewProcess(t)}
	value, err = detached.ResolveVariable("sql_mode", true, false)
	require.NoError(t, err)
	require.Nil(t, value)

	// A context attaching the same compilerContext must not recurse.
	selfAttached := &compilerContext{proc: testutil.NewProcess(t)}
	selfAttached.ctx = attachInternalExecutorCompilerContext(context.Background(), selfAttached)
	value, err = selfAttached.ResolveVariable("sql_mode", true, false)
	require.NoError(t, err)
	require.Nil(t, value)
}
