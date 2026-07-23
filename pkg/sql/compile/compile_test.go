// Copyright 2021 Matrix Origin
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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/system"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/buffer"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/shuffle"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func TestCompileRunPreservesBinaryPrepareParamAcrossRetries(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), catalog.System_Account)
	proc := testutil.NewProcess(t)
	proc.GetSessionInfo().Buf = buffer.New()
	proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) {
		return "STRICT_TRANS_TABLES", nil
	})
	compilerCtx := plan2.NewEmptyCompilerContext()
	compilerCtx.SetContext(ctx)
	stmts, err := mysql.Parse(ctx, "select ?", 1)
	require.NoError(t, err)
	query, err := plan2.NewPrepareOptimizer(compilerCtx).Optimize(stmts[0], true)
	require.NoError(t, err)
	pn := &plan.Plan{Plan: &plan.Plan_Query{Query: query}, IsPrepare: true}
	_, _, err = plan2.ResetPreparePlan(compilerCtx, pn)
	require.NoError(t, err)

	ctrl := gomock.NewController(t)
	txnCli, txnOp := newTestTxnClientAndOpWithIsolation(ctrl, txn.TxnIsolation_RC)
	proc.Base.TxnClient = txnCli
	proc.Base.TxnOperator = txnOp
	proc.Ctx = ctx
	proc.ReplaceTopCtx(ctx)

	want := []byte{'A', 'B', 0, 0}
	params := vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(params, want, false, proc.Mp()))
	proc.SetOwnedPrepareParamsWithIsBin(params, []bool{true})

	evaluations := 0
	fill := func(bat *batch.Batch, _ *perfcounter.CounterSet) error {
		if bat == nil {
			return nil
		}
		require.Len(t, bat.Vecs, 1)
		require.True(t, bat.Vecs[0].GetIsBin(), "binary semantics were lost on evaluation %d", evaluations+1)
		require.Equal(t, want, bat.Vecs[0].GetBytesAt(0))
		evaluations++
		if evaluations <= 2 {
			return moerr.NewTxnNeedRetryNoCtx()
		}
		return nil
	}

	c := NewCompile("test", "test", "select ?", "", "", newStubEngine(), proc, stmts[0], false, nil, time.Now())
	require.NoError(t, c.Compile(ctx, pn, fill))
	_, err = c.Run(0)
	require.NoError(t, err)
	require.Equal(t, 3, evaluations)
	require.Equal(t, 2, c.retryTimes)
	require.Zero(t, params.Length())
	require.Nil(t, params.GetData())
	require.Nil(t, params.GetArea())

	c.Release()
	proc.Free()
	proc.GetSessionInfo().Buf.Free()
}

func TestApplyExecutorLockWaitTimeout(t *testing.T) {
	ctrl := gomock.NewController(t)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	proc := process.NewTopProcess(
		context.Background(),
		mpool.MustNewZero(),
		nil,
		txnOp,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil)

	applyExecutorLockWaitTimeout(proc, executor.Options{}.WithLockWaitTimeout(1500*time.Millisecond))
	require.Equal(t, int64(2), proc.Base.SessionInfo.LockWaitTimeout)
	require.True(t, proc.Base.SessionInfo.LockWaitTimeoutSet)

	clearOpts := executor.Options{}.WithTxn(txnOp).WithLockWaitTimeout(0)
	require.True(t, clearOpts.HasExistsTxn())
	applyExecutorLockWaitTimeout(proc, clearOpts)
	require.Zero(t, proc.Base.SessionInfo.LockWaitTimeout)
	require.True(t, proc.Base.SessionInfo.LockWaitTimeoutSet,
		"an explicit zero must be distinguishable from an absent override")
}

type Ws struct {
	advanceSnapshot func(context.Context, timestamp.Timestamp) error
}

func (w *Ws) SetCloneTxn(snapshot int64) {}

func (w *Ws) SetCCPRTxn() {}

func (w *Ws) IsCCPRTxn() bool { return false }

func (w *Ws) SetCCPRTaskID(taskID string) {}

func (w *Ws) GetCCPRTaskID() string { return "" }

func (w *Ws) SetSyncProtectionJobID(jobID string) {}

func (w *Ws) GetSyncProtectionJobID() string { return "" }

func (w *Ws) Readonly() bool {
	return false
}

func (w *Ws) Snapshot() bool {
	return false
}

func (w *Ws) IncrStatementID(ctx context.Context, commit bool) error {
	return nil
}

func (w *Ws) AdvanceSnapshot(ctx context.Context, ts timestamp.Timestamp) error {
	if w.advanceSnapshot != nil {
		return w.advanceSnapshot(ctx, ts)
	}
	return nil
}

func (w *Ws) RollbackLastStatement(ctx context.Context) error {
	return nil
}

func (w *Ws) Commit(ctx context.Context) ([]txn.TxnRequest, error) {
	return nil, nil
}

func (w *Ws) FinalizeCommit(ctx context.Context) {
}

func (w *Ws) FinalizeCommitWithUnknownResult(ctx context.Context) {
}

func (w *Ws) Rollback(ctx context.Context) error {
	return nil
}

func (w *Ws) UpdateSnapshotWriteOffset() {
}

func (w *Ws) GetSnapshotWriteOffset() int {
	return 0
}

func (w *Ws) WriteOffset() uint64 {
	return 0
}

func (w *Ws) Adjust(_ uint64) error {
	return nil
}

func (w *Ws) StartStatement()     {}
func (w *Ws) EndStatement()       {}
func (w *Ws) IncrSQLCount()       {}
func (w *Ws) GetSQLCount() uint64 { return 0 }

func (w *Ws) CloneSnapshotWS() client.Workspace {
	return nil
}

func (w *Ws) BindTxnOp(op client.TxnOperator) {
}

func (w *Ws) SetHaveDDL(flag bool) {
}

func (w *Ws) GetHaveDDL() bool {
	return false
}

func (w *Ws) PPString() string {
	return ""
}

func NewMockCompile(t *testing.T) *Compile {
	return &Compile{
		proc: testutil.NewProcess(t),
		ncpu: system.GoMaxProcs(),
	}
}

func TestShouldPrePipelineLockTable(t *testing.T) {
	c := NewMockCompile(t)
	target := &plan.LockTarget{LockTable: true}

	c.pn = &plan.Plan{
		Plan: &plan.Plan_Query{
			Query: &plan.Query{StmtType: plan.Query_INSERT},
		},
	}
	require.False(t, c.shouldPrePipelineLockTable(target))
	require.True(t, target.LockTableAtTheEnd)

	target = &plan.LockTarget{LockTable: true}
	c.pn = &plan.Plan{
		Plan: &plan.Plan_Query{
			Query: &plan.Query{StmtType: plan.Query_INSERT, LoadTag: true},
		},
	}
	require.True(t, c.shouldPrePipelineLockTable(target))
	require.False(t, target.LockTableAtTheEnd)

	target = &plan.LockTarget{LockTable: true}
	c.pn = &plan.Plan{
		Plan: &plan.Plan_Query{
			Query: &plan.Query{StmtType: plan.Query_UPDATE},
		},
	}
	require.True(t, c.shouldPrePipelineLockTable(target))
	require.False(t, target.LockTableAtTheEnd)

	target = &plan.LockTarget{LockTable: true}
	c.pn = &plan.Plan{}
	require.True(t, c.shouldPrePipelineLockTable(target))
	require.False(t, target.LockTableAtTheEnd)

	target = &plan.LockTarget{LockTable: false, LockTableAtTheEnd: true}
	require.False(t, c.shouldPrePipelineLockTable(target))
	require.False(t, target.LockTableAtTheEnd)
}

func TestLockTableLocksAllPrePipelineTargets(t *testing.T) {
	runtime.RunTest(
		"",
		func(rt runtime.Runtime) {
			runtime.SetupServiceBasedRuntime("s1", rt)
			lockservice.RunLockServicesForTest(
				zap.DebugLevel,
				[]string{"s1"},
				time.Second,
				func(_ lockservice.LockTableAllocator, services []lockservice.LockService) {
					rt.SetGlobalVariables(runtime.LockService, services[0])

					ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
					defer cancel()

					sender, err := rpc.NewSender(rpc.Config{}, rt)
					require.NoError(t, err)

					txnClient := client.NewTxnClient("", sender, client.WithLockService(services[0]))
					txnClient.Resume()
					defer func() {
						require.NoError(t, txnClient.Close())
					}()

					txnOp, err := txnClient.New(ctx, timestamp.Timestamp{})
					require.NoError(t, err)
					defer func() {
						require.NoError(t, txnOp.Rollback(ctx))
					}()

					proc := process.NewTopProcess(
						ctx,
						mpool.MustNewZero(),
						txnClient,
						txnOp,
						nil,
						services[0],
						nil,
						nil,
						nil,
						nil,
						nil)
					c := &Compile{
						proc: proc,
						lockTables: map[uint64]*plan.LockTarget{
							10: {TableId: 10, PrimaryColTyp: plan.Type{Id: int32(types.T_int32)}},
							11: {TableId: 11, PrimaryColTyp: plan.Type{Id: int32(types.T_int32)}},
						},
					}

					require.NoError(t, c.lockTable())
					require.True(t, txnOp.HasLockTable(10))
					require.True(t, txnOp.HasLockTable(11))
				},
				nil,
			)
		},
	)
}
func newTestTxnClientAndOp(ctrl *gomock.Controller) (client.TxnClient, client.TxnOperator) {
	return newTestTxnClientAndOpWithIsolation(ctrl, txn.TxnIsolation_SI)
}

func newTestTxnClientAndOpWithIsolation(
	ctrl *gomock.Controller,
	isolation txn.TxnIsolation,
) (client.TxnClient, client.TxnOperator) {
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().GetWorkspace().Return(&Ws{}).AnyTimes()
	txnOperator.EXPECT().Txn().Return(txn.TxnMeta{Isolation: isolation}).AnyTimes()
	txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{}).AnyTimes()
	txnOperator.EXPECT().NextSequence().Return(uint64(0)).AnyTimes()
	txnOperator.EXPECT().TryEnterRunSqlWithTokenAndSQL(gomock.Any(), gomock.Any()).Return(uint64(1), nil).AnyTimes()
	txnOperator.EXPECT().ExitRunSqlWithToken(gomock.Any()).Return().AnyTimes()
	txnOperator.EXPECT().CheckLockTableBinds(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Snapshot().Return(txn.CNTxnSnapshot{}, nil).AnyTimes()
	txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()
	return txnClient, txnOperator
}

var (
	_ func(*Compile, client.TxnOperator)       = MarkQueryRunning
	_ func(*Compile, client.TxnOperator) error = TryMarkQueryRunning
)

func TestMarkQueryRunningPreservesLegacyContract(t *testing.T) {
	ctrl := gomock.NewController(t)
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().EnterRunSqlWithTokenAndSQL(gomock.Any(), "select 1").Return(uint64(0))
	txnOperator.EXPECT().ExitRunSqlWithToken(uint64(0))

	c := &Compile{
		proc:      testutil.NewProcess(t),
		originSQL: "select 1",
	}
	MarkQueryRunning(c, txnOperator)
	require.True(t, c.proc.GetBaseProcessRunningStatus())
	require.Zero(t, c.runSqlToken)

	MarkQueryDone(c, txnOperator)
	require.False(t, c.proc.GetBaseProcessRunningStatus())
}

func TestTryMarkQueryRunningRejectsSealedTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	expectedErr := moerr.NewTxnClosedNoCtx([]byte("sealed"))
	txnOperator.EXPECT().TryEnterRunSqlWithTokenAndSQL(gomock.Any(), "select 1").
		Return(uint64(0), expectedErr)

	c := &Compile{
		proc:      testutil.NewProcess(t),
		originSQL: "select 1",
	}
	err := TryMarkQueryRunning(c, txnOperator)
	require.ErrorIs(t, err, expectedErr)
	require.Zero(t, c.runSqlToken)
	require.False(t, c.proc.GetBaseProcessRunningStatus())
}

func newTestTxnClientAndOpWithPessimistic(ctrl *gomock.Controller) (client.TxnClient, client.TxnOperator) {
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().GetWorkspace().Return(&Ws{}).AnyTimes()
	txnOperator.EXPECT().Txn().Return(txn.TxnMeta{
		Mode: txn.TxnMode_Pessimistic,
	}).AnyTimes()
	txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{}).AnyTimes()
	txnOperator.EXPECT().NextSequence().Return(uint64(0)).AnyTimes()
	txnOperator.EXPECT().TryEnterRunSqlWithTokenAndSQL(gomock.Any(), gomock.Any()).Return(uint64(1), nil).AnyTimes()
	txnOperator.EXPECT().ExitRunSqlWithToken(gomock.Any()).Return().AnyTimes()
	txnOperator.EXPECT().CheckLockTableBinds(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Snapshot().Return(txn.CNTxnSnapshot{}, nil).AnyTimes()
	txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()
	return txnClient, txnOperator
}

func TestDebugLogFor19288(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		bsql      string
		originSQL string
	}{
		{
			name:      "Retry Error",
			err:       moerr.NewTxnNeedRetryNoCtx(),
			bsql:      "SELECT * FROM test_table",
			originSQL: "INSERT INTO test_table VALUES (1, 'test')",
		},
		{
			name:      "Non-Retry Error",
			err:       moerr.NewInternalErrorNoCtx("internal error"),
			bsql:      "SELECT * FROM test_table",
			originSQL: "INSERT INTO test_table VALUES (1, 'test')",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			c := NewMockCompile(t)
			txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
			txnOperator.EXPECT().Txn().Return(txn.TxnMeta{
				Isolation: txn.TxnIsolation_RC,
			}).AnyTimes()
			c.proc.Base.TxnOperator = txnOperator
			c.originSQL = tt.originSQL
			c.debugLogFor19288(tt.err, tt.bsql)
		})
	}
}

func TestLockMeta_doLock(t *testing.T) {
	lm := &LockMeta{
		database_table_id: 11230,
		table_table_id:    123123,
		metaTables: map[string]struct{}{
			"test1": {},
		},
		lockDbExe:    nil,
		lockTableExe: nil,
		lockMetaVecs: nil,
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc := testutil.NewProcess(t)
	proc.Base.SessionInfo.Buf = buffer.New()
	proc.Ctx = context.Background()
	eng := mock_frontend.NewMockEngine(ctrl)

	assert.Error(t, lm.doLock(eng, proc))
}

func TestLockMetaInitRetriesAfterPartialFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc := testutil.NewProcess(t)
	proc.Ctx = defines.AttachAccountId(context.Background(), catalog.System_Account)
	eng := mock_frontend.NewMockEngine(ctrl)
	database := mock_frontend.NewMockDatabase(ctrl)
	databaseRel := mock_frontend.NewMockRelation(ctrl)
	tableRel := mock_frontend.NewMockRelation(ctrl)
	lookupErr := moerr.NewInternalErrorNoCtx("lookup mo_tables")

	eng.EXPECT().Database(gomock.Any(), catalog.MO_CATALOG, gomock.Any()).Return(database, nil).Times(2)
	database.EXPECT().Relation(gomock.Any(), catalog.MO_DATABASE, gomock.Any()).Return(databaseRel, nil).Times(2)
	database.EXPECT().Relation(gomock.Any(), catalog.MO_TABLES, gomock.Any()).Return(nil, lookupErr).Times(1)
	database.EXPECT().Relation(gomock.Any(), catalog.MO_TABLES, gomock.Any()).Return(tableRel, nil).Times(1)
	databaseRel.EXPECT().GetTableID(gomock.Any()).Return(uint64(1)).Times(1)
	tableRel.EXPECT().GetTableID(gomock.Any()).Return(uint64(2)).Times(1)
	tableRel.EXPECT().Reset(gomock.Any()).Return(nil).Times(1)
	databaseRel.EXPECT().Reset(gomock.Any()).Return(nil).Times(1)

	lm := NewLockMeta()
	require.ErrorIs(t, lm.initLockExe(eng, proc), lookupErr)
	require.Nil(t, lm.lockDbExe)
	require.Nil(t, lm.lockTableExe)
	require.Nil(t, lm.database_rel)
	require.Nil(t, lm.table_rel)

	require.NoError(t, lm.initLockExe(eng, proc))
	require.NotNil(t, lm.lockDbExe)
	require.NotNil(t, lm.lockTableExe)
	require.Same(t, databaseRel, lm.database_rel)
	require.Same(t, tableRel, lm.table_rel)
	require.NoError(t, lm.initLockExe(eng, proc))

	lm.clear(proc)
	proc.Free()
}

func TestCompileClearReleasesLockMetaBeforeProcess(t *testing.T) {
	proc := testutil.NewProcess(t)
	c := allocateNewCompile(proc)
	c.lockMeta = NewLockMeta()
	c.lockMeta.lockMetaVecs = []*vector.Vector{vector.NewVec(types.T_uint32.ToType())}

	require.NotPanics(t, c.clear)
	require.Nil(t, c.proc)
	require.Nil(t, c.lockMeta)
}

func TestCompileShuffleGroupUsesDistributedPathWhenScopeMcpuDiffersFromDop(t *testing.T) {
	c := newCompileForShuffleGroupTest(t)
	aggNode, nodes := newShuffleGroupTestNodes(16)
	scope := newShuffleGroupInputScope(t, 1)

	result := c.compileShuffleGroup(aggNode, []*Scope{scope}, nodes)

	require.Len(t, result, 16)
	for _, resultScope := range result {
		require.IsType(t, &group.Group{}, resultScope.RootOp)
	}
	require.Len(t, result[0].PreScopes, 1)
	require.IsType(t, &shuffle.Shuffle{}, result[0].PreScopes[0].RootOp.GetOperatorBase().GetChildren(0))
}

func TestCompileShuffleGroupUsesDistributedPathWhenInputScopesNotSingle(t *testing.T) {
	c := newCompileForShuffleGroupTest(t)
	aggNode, nodes := newShuffleGroupTestNodes(16)
	scope1 := newShuffleGroupInputScope(t, 1)
	scope2 := newShuffleGroupInputScope(t, 1)

	result := c.compileShuffleGroup(aggNode, []*Scope{scope1, scope2}, nodes)

	require.Len(t, result, 16)
	for _, resultScope := range result {
		require.IsType(t, &group.Group{}, resultScope.RootOp)
	}
	require.Len(t, result[0].PreScopes, 1)
	for _, input := range result[0].PreScopes {
		require.IsType(t, &shuffle.Shuffle{}, input.RootOp.GetOperatorBase().GetChildren(0))
	}
}

func TestCompileShuffleGroupUsesLocalPathWhenScopeMcpuMatchesDop(t *testing.T) {
	c := newCompileForShuffleGroupTest(t)
	aggNode, nodes := newShuffleGroupTestNodes(16)
	scope := newShuffleGroupInputScope(t, 16)

	result := c.compileShuffleGroup(aggNode, []*Scope{scope}, nodes)

	require.Len(t, result, 1)
	require.Same(t, scope, result[0])
	require.IsType(t, &group.Group{}, result[0].RootOp)
	shuffleOp, ok := result[0].RootOp.GetOperatorBase().GetChildren(0).(*shuffle.Shuffle)
	require.True(t, ok)
	require.Equal(t, int32(16), shuffleOp.BucketNum)
	require.Equal(t, int32(0), shuffleOp.CurrentShuffleIdx)
}

func newCompileForShuffleGroupTest(t *testing.T) *Compile {
	c := NewMockCompile(t)
	c.execType = plan2.ExecTypeAP_ONECN
	c.anal = &AnalyzeModule{}
	return c
}

func newShuffleGroupInputScope(t *testing.T, mcpu int) *Scope {
	scope := newScope(Merge)
	scope.NodeInfo = engine.Node{Addr: "127.0.0.1:18000", Mcpu: mcpu}
	scope.Proc = testutil.NewProcess(t)
	scope.setRootOperator(colexec.NewMockOperator())
	return scope
}

func newShuffleGroupTestNodes(dop int32) (*plan.Node, []*plan.Node) {
	col := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_int64)},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{ColPos: 0},
		},
	}
	child := &plan.Node{
		NodeId:   3,
		NodeType: plan.Node_SORT,
		Stats:    &plan.Stats{Dop: dop},
		ProjectList: []*plan.Expr{
			col,
		},
	}
	agg := &plan.Node{
		NodeId:   4,
		NodeType: plan.Node_AGG,
		Stats: &plan.Stats{
			Dop: dop,
			HashmapStats: &plan.HashMapStats{
				Shuffle:       true,
				ShuffleColIdx: 0,
				ShuffleType:   plan.ShuffleType_Range,
				ShuffleMethod: plan.ShuffleMethod_Normal,
			},
		},
		Children: []int32{0},
		GroupBy:  []*plan.Expr{col},
	}
	return agg, []*plan.Node{child}
}

func TestDistributedShuffleJoinFallsBackFromPackedReuse(t *testing.T) {
	c := newCompileForShuffleJoinTest(t, engine.Nodes{{Addr: "cn1:6001", Mcpu: 4}})
	node := newShuffleJoinTestNode(4)
	probe := newShuffleJoinTestScope(t, c.cnList[0], 4)
	build := newShuffleJoinTestScope(t, c.cnList[0], 4)

	result := c.newShuffleJoinScopeList([]*Scope{probe}, []*Scope{build}, node)

	require.Len(t, result, 4)
	require.Equal(t, plan.ShuffleMethod_Reuse, node.Stats.HashmapStats.ShuffleMethod,
		"the physical fallback must not mutate reusable plan metadata")
	probeDispatch, ok := probe.RootOp.(*dispatch.Dispatch)
	require.True(t, ok)
	require.Len(t, probeDispatch.LocalRegs, 4)
	require.Equal(t, []int{0, 1, 2, 3}, probeDispatch.ShuffleRegIdxLocal)
	require.IsType(t, &shuffle.Shuffle{}, probeDispatch.GetOperatorBase().GetChildren(0))
	buildDispatch, ok := build.RootOp.(*dispatch.Dispatch)
	require.True(t, ok)
	require.Len(t, buildDispatch.LocalRegs, 4)
	require.Equal(t, []int{0, 1, 2, 3}, buildDispatch.ShuffleRegIdxLocal)
}

func TestDistributedShuffleJoinKeepsMaterializedReuse(t *testing.T) {
	c := newCompileForShuffleJoinTest(t, engine.Nodes{{Addr: "cn1:6001", Mcpu: 4}})
	node := newShuffleJoinTestNode(4)
	probes := make([]*Scope, 4)
	probeRoots := make([]vm.Operator, 4)
	for i := range probes {
		probes[i] = newShuffleJoinTestScope(t, c.cnList[0], 1)
		probeRoots[i] = probes[i].RootOp
	}
	build := newShuffleJoinTestScope(t, c.cnList[0], 4)

	result := c.newShuffleJoinScopeList(probes, []*Scope{build}, node)

	require.Len(t, result, 4)
	for i := range probes {
		require.Same(t, probes[i], result[i])
		require.Same(t, probeRoots[i], probes[i].RootOp,
			"valid reuse must not add a probe shuffle")
	}
	buildDispatch, ok := build.RootOp.(*dispatch.Dispatch)
	require.True(t, ok)
	require.Len(t, buildDispatch.LocalRegs, 4)
}

func TestDistributedShuffleJoinRejectsMisorderedReuse(t *testing.T) {
	nodes := engine.Nodes{
		{Addr: "cn1:6001", Mcpu: 2},
		{Addr: "cn2:6001", Mcpu: 2},
	}
	c := newCompileForShuffleJoinTest(t, nodes)
	c.execType = plan2.ExecTypeAP_MULTICN
	node := newShuffleJoinTestNode(2)
	probes := []*Scope{
		newShuffleJoinTestScope(t, nodes[0], 1),
		newShuffleJoinTestScope(t, nodes[1], 1),
		newShuffleJoinTestScope(t, nodes[0], 1),
		newShuffleJoinTestScope(t, nodes[1], 1),
	}
	builds := []*Scope{
		newShuffleJoinTestScope(t, nodes[0], 1),
		newShuffleJoinTestScope(t, nodes[1], 1),
	}

	result := c.newShuffleJoinScopeList(probes, builds, node)

	require.Len(t, result, 4)
	require.NotSame(t, probes[0], result[0],
		"noncanonical bucket order must materialize a new distributed shuffle layout")
}

func TestDistributedShuffleJoinRejectsMultiCNDedupReuse(t *testing.T) {
	nodes := engine.Nodes{
		{Addr: "cn1:6001", Mcpu: 2},
		{Addr: "cn2:6001", Mcpu: 2},
	}
	c := newCompileForShuffleJoinTest(t, nodes)
	c.execType = plan2.ExecTypeAP_MULTICN
	node := newShuffleJoinTestNode(2)
	node.JoinType = plan.Node_DEDUP
	probes := []*Scope{
		newShuffleJoinTestScope(t, nodes[0], 1),
		newShuffleJoinTestScope(t, nodes[0], 1),
		newShuffleJoinTestScope(t, nodes[1], 1),
		newShuffleJoinTestScope(t, nodes[1], 1),
	}
	builds := []*Scope{
		newShuffleJoinTestScope(t, nodes[0], 1),
		newShuffleJoinTestScope(t, nodes[1], 1),
	}

	result := c.newShuffleJoinScopeList(probes, builds, node)

	require.Len(t, result, 4,
		"multi-CN DEDUP normalization must force a physical reshuffle")
}

func newCompileForShuffleJoinTest(t *testing.T, nodes engine.Nodes) *Compile {
	c := NewMockCompile(t)
	c.addr = nodes[0].Addr
	c.cnList = nodes
	c.execType = plan2.ExecTypeAP_ONECN
	c.anal = &AnalyzeModule{}
	return c
}

func newShuffleJoinTestScope(t *testing.T, node engine.Node, mcpu int) *Scope {
	scope := newScope(Remote)
	scope.NodeInfo = scopeNodeWithMcpu(node, mcpu)
	scope.Proc = testutil.NewProcess(t)
	scope.setRootOperator(colexec.NewMockOperator())
	return scope
}

func newShuffleJoinTestNode(dop int32) *plan.Node {
	leftCol := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_int64)},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{
			RelPos: 1,
			ColPos: 0,
		}},
	}
	rightCol := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_int64)},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{
			RelPos: 2,
			ColPos: 0,
		}},
	}
	return &plan.Node{
		NodeType: plan.Node_JOIN,
		JoinType: plan.Node_INNER,
		Stats: &plan.Stats{
			Dop:      dop,
			TableCnt: 1000,
			HashmapStats: &plan.HashMapStats{
				Shuffle:       true,
				ShuffleColIdx: 0,
				ShuffleType:   plan.ShuffleType_Hash,
				ShuffleMethod: plan.ShuffleMethod_Reuse,
			},
		},
		OnList: []*plan.Expr{{
			Typ: plan.Type{Id: int32(types.T_bool)},
			Expr: &plan.Expr_F{F: &plan.Function{
				Args: []*plan.Expr{leftCol, rightCol},
			}},
		}},
	}
}

// TestNewCompileTxnOffsetForInternalSql verifies the statement-boundary
// contract of NewCompile and Compile.Reset (issue #25557): a compile of a
// user statement advances the workspace snapshot write offset, while an
// internal sub-sql compile (DisableIncrStatement, marked on the process)
// must not touch the shared boundary — it captures the current end of the
// workspace as its own TxnOffset instead.
func TestNewCompileTxnOffsetForInternalSql(t *testing.T) {
	t.Run("user statement advances the boundary", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ws := mock_frontend.NewMockWorkspace(ctrl)
		ws.EXPECT().UpdateSnapshotWriteOffset().Times(1)
		ws.EXPECT().GetSnapshotWriteOffset().Return(3).Times(1)
		txnOp := mock_frontend.NewMockTxnOperator(ctrl)
		txnOp.EXPECT().GetWorkspace().Return(ws).AnyTimes()

		proc := testutil.NewProcess(t)
		proc.Base.TxnOperator = txnOp

		c := NewCompile("test", "test", "select 1", "", "", nil, proc, nil, false, nil, time.Now())
		require.Equal(t, 3, c.TxnOffset)
	})

	t.Run("internal sub-sql must not advance the boundary", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ws := mock_frontend.NewMockWorkspace(ctrl)
		// no UpdateSnapshotWriteOffset expectation: the mock controller
		// fails the test if the internal compile advances the boundary
		ws.EXPECT().WriteOffset().Return(uint64(7)).Times(1)
		txnOp := mock_frontend.NewMockTxnOperator(ctrl)
		txnOp.EXPECT().GetWorkspace().Return(ws).AnyTimes()

		proc := testutil.NewProcess(t)
		proc.Base.TxnOperator = txnOp
		proc.SetIncrStatementDisabled(true)

		c := NewCompile("test", "test", "select 1", "", "", nil, proc, nil, false, nil, time.Now())
		require.Equal(t, 7, c.TxnOffset)
	})
}
