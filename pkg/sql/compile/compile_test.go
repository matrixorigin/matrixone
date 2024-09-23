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

	"github.com/matrixorigin/matrixone/pkg/txn/client"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"

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
	sql  string
	pn   *plan.Plan
	e    engine.Engine
	stmt tree.Statement
	proc *process.Process
}

var (
	tcs []compileTestCase
)

func init() {
	tcs = []compileTestCase{
		newTestCase("select 1", new(testing.T)),
		newTestCase("select * from R", new(testing.T)),
		newTestCase("select * from R where uid > 1", new(testing.T)),
		newTestCase("select * from R order by uid", new(testing.T)),
		newTestCase("select * from R order by uid limit 1", new(testing.T)),
		newTestCase("select * from R limit 1", new(testing.T)),
		newTestCase("select * from R limit 2, 1", new(testing.T)),
		newTestCase("select count(*) from R", new(testing.T)),
		newTestCase("select * from R join S on R.uid = S.uid", new(testing.T)),
		newTestCase("select * from R left join S on R.uid = S.uid", new(testing.T)),
		newTestCase("select * from R right join S on R.uid = S.uid", new(testing.T)),
		newTestCase("select * from R join S on R.uid > S.uid", new(testing.T)),
		newTestCase("select * from R limit 10", new(testing.T)),
		newTestCase("select count(*) from R group by uid", new(testing.T)),
		newTestCase("select count(distinct uid) from R", new(testing.T)),
		newTestCase("select _wstart, _wend, max(b) from (select date_add('2021-01-12 00:00:00.000', interval 1 second) as ts, 1 as b) as t interval(ts, 2, second) sliding(1, second) fill(prev)", new(testing.T)),
		newTestCase("select _wstart, sum(b) from (select date_add('2021-01-12 00:00:00.000', interval 1 second) as ts, 1 as b) as t interval(ts, 2, minute) sliding(1, minute) fill(none)", new(testing.T)),
		newTestCase("select _wend, avg(b) from (select date_add('2021-01-12 00:00:00.000', interval 1 second) as ts, 1 as b) as t interval(ts, 2, hour) sliding(1, hour) fill(value, 1.2)", new(testing.T)),
		newTestCase("select count(b) from (select date_add('2021-01-12 00:00:00.000', interval 1 second) as ts, 1 as b) as t interval(ts, 2, second) sliding(1, second)", new(testing.T)),
		newTestCase("select _wstart, _wend, min(b) from (select date_add('2021-01-12 00:00:00.000', interval 1 second) as ts, 1 as b) as t interval(ts, 2, second)", new(testing.T)),
		newTestCase("select _wstart, _wend, avg(b) from (select date_add('2021-01-12 00:00:00.000', interval 1 second) as ts, cast(1.222 as decimal(6, 2)) as b) as t interval(ts, 2, second)", new(testing.T)),
		newTestCase("select _wstart, _wend, avg(b) from (select date_add('2021-01-12 00:00:00.000', interval 1 second) as ts, cast(1.222 as decimal(16, 2)) as b) as t interval(ts, 2, second)", new(testing.T)),
		// xxx because memEngine can not handle Halloween Problem
		// newTestCase("insert into R values('991', '992', '993')", new(testing.T)),
		// newTestCase("insert into R select * from S", new(testing.T)),
		// newTestCase("update R set uid=110 where orderid='abcd'", new(testing.T)),
		newTestCase(fmt.Sprintf("load data infile {\"filepath\"=\"%s/../../../test/distributed/resources/load_data/parallel_1.txt.gz\", \"compression\"=\"gzip\"} into table pressTbl FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\n' parallel 'true';", GetFilePath()), new(testing.T)),
	}
}

func testPrint(_ *batch.Batch) error {
	return nil
}

type Ws struct {
}

func (w *Ws) Readonly() bool {
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

func TestCompile(t *testing.T) {
	c, err := cnclient.NewPipelineClient("", "test", &cnclient.PipelineConfig{})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close())
	}()

	ctrl := gomock.NewController(t)
	ctx := defines.AttachAccountId(context.TODO(), catalog.System_Account)
	txnCli, txnOp := newTestTxnClientAndOp(ctrl)
	for _, tc := range tcs {
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

func TestCompileWithFaults(t *testing.T) {
	// Enable this line to trigger the Hung.
	// fault.Enable()
	var ctx = defines.AttachAccountId(context.Background(), catalog.System_Account)

	pc, err := cnclient.NewPipelineClient("", "test", &cnclient.PipelineConfig{})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, pc.Close())
	}()

	fault.AddFaultPoint(ctx, "panic_in_batch_append", ":::", "panic", 0, "")
	tc := newTestCase("select * from R join S on R.uid = S.uid", t)
	ctrl := gomock.NewController(t)
	txnCli, txnOp := newTestTxnClientAndOp(ctrl)
	tc.proc.Base.TxnClient = txnCli
	tc.proc.Base.TxnOperator = txnOp
	tc.proc.Ctx = ctx
	c := NewCompile("test", "test", tc.sql, "", "", tc.e, tc.proc, nil, false, nil, time.Now())
	err = c.Compile(ctx, tc.pn, testPrint)
	require.NoError(t, err)
	c.getAffectedRows()
	_, err = c.Run(0)
	require.NoError(t, err)
}

func newTestTxnClientAndOp(ctrl *gomock.Controller) (client.TxnClient, client.TxnOperator) {
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().GetWorkspace().Return(&Ws{}).AnyTimes()
	txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
	txnOperator.EXPECT().ResetRetry(gomock.Any()).AnyTimes()
	txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{}).AnyTimes()
	txnOperator.EXPECT().NextSequence().Return(uint64(0)).AnyTimes()
	txnOperator.EXPECT().EnterRunSql().Return().AnyTimes()
	txnOperator.EXPECT().ExitRunSql().Return().AnyTimes()
	txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()
	return txnClient, txnOperator
}

func newTestCase(sql string, t *testing.T) compileTestCase {
	proc := testutil.NewProcess()
	proc.GetSessionInfo().Buf = buffer.New()
	proc.SetResolveVariableFunc(func(varName string, isSystemVar, isGlobalVar bool) (interface{}, error) {
		return "STRICT_TRANS_TABLES", nil
	})
	catalog.SetupDefines("")
	e, _, compilerCtx := testengine.New(defines.AttachAccountId(context.Background(), catalog.System_Account))
	stmts, err := mysql.Parse(compilerCtx.GetContext(), sql, 1)
	require.NoError(t, err)
	pn, err := plan2.BuildPlan(compilerCtx, stmts[0], false)
	if err != nil {
		panic(err)
	}
	require.NoError(t, err)
	return compileTestCase{
		e:    e,
		sql:  sql,
		proc: proc,
		pn:   pn,
		stmt: stmts[0],
	}
}

func GetFilePath() string {
	dir, _ := os.Getwd()
	return dir
}
