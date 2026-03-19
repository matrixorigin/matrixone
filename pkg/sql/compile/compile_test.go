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
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/system"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/txn/client"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/cnservice/cnclient"
	"github.com/matrixorigin/matrixone/pkg/common/buffer"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/testutil/testengine"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type compileTestCase struct {
	sql       string
	pn        *plan.Plan
	e         engine.Engine
	stmt      tree.Statement
	proc      *process.Process
	txnClient client.TxnClient // Store txnClient for truncating table with real transaction
}

func testPrint(_ *batch.Batch, crs *perfcounter.CounterSet) error {
	return nil
}

type Ws struct {
}

func (w *Ws) SetCloneTxn(snapshot int64) {}

func (w *Ws) Readonly() bool {
	return false
}

func (w *Ws) Snapshot() bool {
	return false
}

func (w *Ws) IncrStatementID(ctx context.Context, commit bool) error {
	return nil
}

func (w *Ws) RollbackLastStatement(ctx context.Context) error {
	return nil
}

func (w *Ws) Commit(ctx context.Context) ([]txn.TxnRequest, error) {
	return nil, nil
}

func (w *Ws) Rollback(ctx context.Context) error {
	return nil
}

func (w *Ws) UpdateSnapshotWriteOffset() {
}

func (w *Ws) GetSnapshotWriteOffset() int {
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

func TestCompile(t *testing.T) {
	c, err := cnclient.NewPipelineClient("", "test", &cnclient.PipelineConfig{})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close())
	}()

	ctrl := gomock.NewController(t)
	ctx := defines.AttachAccountId(context.TODO(), catalog.System_Account)
	txnCli, txnOp := newTestTxnClientAndOp(ctrl)

	// Generate test SQLs (avoiding engine creation in init)
	testSQLs := []string{
		"select 1",
		"select * from R",
		"select * from R where uid > 1",
		"select * from R order by uid",
		"select * from R order by uid limit 1",
		"select * from R limit 1",
		"select * from R limit 2, 1",
		"select count(*) from R",
		"select * from R join S on R.uid = S.uid",
		"select * from R left join S on R.uid = S.uid",
		"select * from R right join S on R.uid = S.uid",
		"select * from R join S on R.uid > S.uid",
		"select * from R limit 10",
		"select count(*) from R group by uid",
		"select count(distinct uid) from R",
		"select _wstart, _wend, max(b) from (select date_add('2021-01-12 00:00:00.000', interval 1 second) as ts, 1 as b) as t interval(ts, 2, second) sliding(1, second) fill(prev)",
		"select _wstart, sum(b) from (select date_add('2021-01-12 00:00:00.000', interval 1 second) as ts, 1 as b) as t interval(ts, 2, minute) sliding(1, minute) fill(none)",
		"select _wend, avg(b) from (select date_add('2021-01-12 00:00:00.000', interval 1 second) as ts, 1 as b) as t interval(ts, 2, hour) sliding(1, hour) fill(value, 1.2)",
		"select count(b) from (select date_add('2021-01-12 00:00:00.000', interval 1 second) as ts, 1 as b) as t interval(ts, 2, second) sliding(1, second)",
		"select _wstart, _wend, min(b) from (select date_add('2021-01-12 00:00:00.000', interval 1 second) as ts, 1 as b) as t interval(ts, 2, second)",
		"select _wstart, _wend, avg(b) from (select date_add('2021-01-12 00:00:00.000', interval 1 second) as ts, cast(1.222 as decimal(6, 2)) as b) as t interval(ts, 2, second)",
		"select _wstart, _wend, avg(b) from (select date_add('2021-01-12 00:00:00.000', interval 1 second) as ts, cast(1.222 as decimal(16, 2)) as b) as t interval(ts, 2, second)",
		fmt.Sprintf("load data infile {\"filepath\"=\"%s/../../../test/distributed/resources/load_data/parallel_1.txt.gz\", \"compression\"=\"gzip\"} into table pressTbl FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\\n' parallel 'true';", GetFilePath()),
	}

	// Create fresh test cases for each test run to avoid state persistence with --count > 1
	for _, sql := range testSQLs {
		// Create a fresh test case with a new engine for each SQL
		tc := newTestCase(sql, t)

		tc.proc.Base.TxnClient = txnCli
		tc.proc.Base.TxnOperator = txnOp
		tc.proc.Ctx = ctx
		tc.proc.ReplaceTopCtx(ctx)
		c := NewCompile("test", "test", tc.sql, "", "", tc.e, tc.proc, tc.stmt, false, nil, time.Now())
		err := c.Compile(ctx, tc.pn, testPrint)
		require.NoError(t, err)
		c.getAffectedRows()
		_, err = c.Run(0)
		require.NoError(t, err)
		// Enable memory check
		tc.proc.Free()
		//FIXME:
		//!!!GOD!!!
		//Sometimes it is 0.
		//Sometimes it is 24.
		//require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
		tc.proc.GetSessionInfo().Buf.Free()
	}
}

// TestCompileWithFaults tests compile behavior with fault injection.
//
// Test quality criteria:
// 1. No randomness: Fixed fault points and SQL
// 2. Fast execution: Uses testengine with mocks
// 3. Meaningful: Tests fault tolerance and error handling
// 4. Realistic: Tests real fault scenarios that can occur in production
func TestCompileWithFaults(t *testing.T) {
	var ctx = defines.AttachAccountId(context.Background(), catalog.System_Account)

	pc, err := cnclient.NewPipelineClient("", "test", &cnclient.PipelineConfig{})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, pc.Close())
	}()

	tests := []struct {
		name      string
		faultName string
		sql       string
	}{
		{
			name:      "panic_in_batch_append",
			faultName: "panic_in_batch_append",
			sql:       "select * from R join S on R.uid = S.uid",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fault.AddFaultPoint(ctx, tt.faultName, ":::", "panic", 0, "", false)
			tc := newTestCase(tt.sql, t)
			ctrl := gomock.NewController(t)
			txnCli, txnOp := newTestTxnClientAndOp(ctrl)
			tc.proc.Base.TxnClient = txnCli
			tc.proc.Base.TxnOperator = txnOp
			tc.proc.Ctx = ctx
			c := NewCompile("test", "test", tc.sql, "", "", tc.e, tc.proc, nil, false, nil, time.Now())
			err = c.Compile(ctx, tc.pn, testPrint)
			require.NoError(t, err, "compile should succeed even with fault point")
			c.getAffectedRows()
			_, err = c.Run(0)
			// Note: Run may succeed or fail depending on fault injection behavior
			// The key is that compile doesn't crash
			require.NoError(t, err, "run should complete without panic")
		})
	}
}

func newTestTxnClientAndOp(ctrl *gomock.Controller) (client.TxnClient, client.TxnOperator) {
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().GetWorkspace().Return(&Ws{}).AnyTimes()
	txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
	txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{}).AnyTimes()
	txnOperator.EXPECT().NextSequence().Return(uint64(0)).AnyTimes()
	txnOperator.EXPECT().EnterRunSqlWithTokenAndSQL(gomock.Any(), gomock.Any()).Return(uint64(0)).AnyTimes()
	txnOperator.EXPECT().ExitRunSqlWithToken(gomock.Any()).Return().AnyTimes()
	txnOperator.EXPECT().Snapshot().Return(txn.CNTxnSnapshot{}, nil).AnyTimes()
	txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()
	return txnClient, txnOperator
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
	txnOperator.EXPECT().EnterRunSqlWithTokenAndSQL(gomock.Any(), gomock.Any()).Return(uint64(0)).AnyTimes()
	txnOperator.EXPECT().ExitRunSqlWithToken(gomock.Any()).Return().AnyTimes()
	txnOperator.EXPECT().Snapshot().Return(txn.CNTxnSnapshot{}, nil).AnyTimes()
	txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()
	return txnClient, txnOperator
}

func newTestCase(sql string, t *testing.T) compileTestCase {
	proc := testutil.NewProcess(t)
	proc.GetSessionInfo().Buf = buffer.New()
	proc.SetResolveVariableFunc(func(varName string, isSystemVar, isGlobalVar bool) (interface{}, error) {
		return "STRICT_TRANS_TABLES", nil
	})
	catalog.SetupDefines("")
	e, txnClient, compilerCtx := testengine.New(defines.AttachAccountId(context.Background(), catalog.System_Account))
	stmts, err := mysql.Parse(compilerCtx.GetContext(), sql, 1)
	require.NoError(t, err)
	pn, err := plan2.BuildPlan(compilerCtx, stmts[0], false)
	if err != nil {
		panic(err)
	}
	require.NoError(t, err)
	return compileTestCase{
		e:         e,
		sql:       sql,
		proc:      proc,
		pn:        pn,
		stmt:      stmts[0],
		txnClient: txnClient,
	}
}

func GetFilePath() string {
	dir, _ := os.Getwd()
	return dir
}

// mockRPCClient is a test implementation of morpc.RPCClient for testing.
type mockRPCClient struct {
	pingErr error
}

func (m *mockRPCClient) Ping(ctx context.Context, backend string) error {
	return m.pingErr
}

func (m *mockRPCClient) Send(ctx context.Context, backend string, request morpc.Message) (*morpc.Future, error) {
	return nil, nil
}

func (m *mockRPCClient) NewStream(backend string, lock bool) (morpc.Stream, error) {
	return nil, nil
}

func (m *mockRPCClient) Close() error {
	return nil
}

func (m *mockRPCClient) CloseBackend() error {
	return nil
}

// TestIsAvailable tests CN availability check.
//
// Test quality criteria:
// 1. No randomness: Fixed RPC client behavior
// 2. Fast execution: Mocked Ping that returns immediately (no sleep)
// 3. Meaningful: Tests availability check logic with both success and failure cases
// 4. Realistic: Tests real scenario where CN ping can succeed or fail
func TestIsAvailable(t *testing.T) {
	// Test case 1: Ping fails - should return false
	mockClient := &mockRPCClient{pingErr: moerr.NewInternalErrorNoCtx("connection failed")}
	ret := isAvailable(mockClient, "127.0.0.1:6001")
	assert.False(t, ret, "should return false when ping fails")

	// Test case 2: Ping succeeds - should return true
	mockClient = &mockRPCClient{pingErr: nil}
	ret = isAvailable(mockClient, "127.0.0.1:6002")
	assert.True(t, ret, "should return true when ping succeeds")
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
