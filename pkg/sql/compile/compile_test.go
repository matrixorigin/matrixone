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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"

	"github.com/matrixorigin/matrixone/pkg/common/reuse"

	"github.com/golang/mock/gomock"
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
	"github.com/stretchr/testify/require"
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
		// xxx because memEngine can not handle Halloween Problem
		// newTestCase("insert into R values('991', '992', '993')", new(testing.T)),
		// newTestCase("insert into R select * from S", new(testing.T)),
		// newTestCase("update R set uid=110 where orderid='abcd'", new(testing.T)),
		newTestCase(fmt.Sprintf("load data infile {\"filepath\"=\"%s/../../../test/distributed/resources/load_data/parallel.txt.gz\", \"compression\"=\"gzip\"} into table pressTbl FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\n' parallel 'true';", GetFilePath()), new(testing.T)),
	}
}

func testPrint(_ interface{}, _ *batch.Batch) error {
	return nil
}

type Ws struct {
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

func (w *Ws) TransferRowID() {
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

func TestCompile(t *testing.T) {
	cnclient.NewCNClient("test", new(cnclient.ClientConfig))
	ctrl := gomock.NewController(t)
	ctx := defines.AttachAccountId(context.TODO(), catalog.System_Account)
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Rollback(ctx).Return(nil).AnyTimes()
	txnOperator.EXPECT().GetWorkspace().Return(&Ws{}).AnyTimes()
	txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
	txnOperator.EXPECT().ResetRetry(gomock.Any()).AnyTimes()
	txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{}).AnyTimes()
	txnOperator.EXPECT().NextSequence().Return(uint64(0)).AnyTimes()
	txnOperator.EXPECT().EnterRunSql().Return().AnyTimes()
	txnOperator.EXPECT().ExitRunSql().Return().AnyTimes()
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()
	for _, tc := range tcs {
		tc.proc.TxnClient = txnClient
		tc.proc.TxnOperator = txnOperator
		tc.proc.Ctx = ctx
		c := NewCompile("test", "test", tc.sql, "", "", ctx, tc.e, tc.proc, tc.stmt, false, nil, time.Now())
		err := c.Compile(ctx, tc.pn, nil, testPrint)
		require.NoError(t, err)
		c.getAffectedRows()
		_, err = c.Run(0)
		require.NoError(t, err)
		// Enable memory check
		tc.proc.FreeVectors()
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
		tc.proc.SessionInfo.Buf.Free()
	}
}

func TestCompileWithFaults(t *testing.T) {
	// Enable this line to trigger the Hung.
	// fault.Enable()
	var ctx = defines.AttachAccountId(context.Background(), catalog.System_Account)
	cnclient.NewCNClient("test", new(cnclient.ClientConfig))
	fault.AddFaultPoint(ctx, "panic_in_batch_append", ":::", "panic", 0, "")
	tc := newTestCase("select * from R join S on R.uid = S.uid", t)
	tc.proc.Ctx = ctx
	c := NewCompile("test", "test", tc.sql, "", "", ctx, tc.e, tc.proc, nil, false, nil, time.Now())
	err := c.Compile(ctx, tc.pn, nil, testPrint)
	require.NoError(t, err)
	c.getAffectedRows()
	_, err = c.Run(0)
	require.NoError(t, err)
}

func newTestCase(sql string, t *testing.T) compileTestCase {
	proc := testutil.NewProcess()
	proc.SessionInfo.Buf = buffer.New()
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

func TestCompileShouldReturnCtxError(t *testing.T) {
	{
		c := reuse.Alloc[Compile](nil)
		c.proc = &process.Process{}
		ctx, cancel := context.WithTimeout(context.TODO(), 100*time.Millisecond)
		c.proc.Ctx = ctx
		time.Sleep(time.Second)
		require.True(t, c.shouldReturnCtxErr())
		cancel()
		require.True(t, c.shouldReturnCtxErr())
	}

	{
		c := reuse.Alloc[Compile](nil)
		c.proc = &process.Process{}
		ctx, cancel := context.WithTimeout(context.TODO(), 500*time.Millisecond)
		c.proc.Ctx = ctx
		cancel()
		require.False(t, c.shouldReturnCtxErr())
		time.Sleep(time.Second)
		require.False(t, c.shouldReturnCtxErr())
	}
}

func GetFilePath() string {
	dir, _ := os.Getwd()
	return dir
}
