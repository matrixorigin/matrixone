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

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
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
		newTestCase("insert into R values('1', '2', '3')", new(testing.T)),
		newTestCase("insert into R select * from R", new(testing.T)),
		newTestCase(fmt.Sprintf("load data infile {\"filepath\"=\"%s/../../../test/distributed/resources/load_data/parallel.txt.gz\", \"compression\"=\"gzip\"} into table pressTbl FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\n' parallel 'true';", GetFilePath()), new(testing.T)),
	}
}

func testPrint(_ interface{}, _ *batch.Batch) error {
	return nil
}

func TestCompile(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx := context.TODO()
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Rollback(ctx).Return(nil).AnyTimes()

	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()
	for _, tc := range tcs {
		tc.proc.TxnClient = txnClient
		c := New("test", "test", tc.sql, "", "", context.TODO(), tc.e, tc.proc, tc.stmt, false, nil)
		err := c.Compile(ctx, tc.pn, nil, testPrint)
		require.NoError(t, err)
		c.GetAffectedRows()
		err = c.Run(0)
		require.NoError(t, err)
		// Enable memory check
		tc.proc.FreeVectors()
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	}
}

func TestCompileWithFaults(t *testing.T) {
	// Enable this line to trigger the Hung.
	// fault.Enable()
	var ctx = context.Background()
	fault.AddFaultPoint(ctx, "panic_in_batch_append", ":::", "panic", 0, "")
	tc := newTestCase("select * from R join S on R.uid = S.uid", t)
	c := New("test", "test", tc.sql, "", "", context.TODO(), tc.e, tc.proc, nil, false, nil)
	err := c.Compile(ctx, tc.pn, nil, testPrint)
	require.NoError(t, err)
	c.GetAffectedRows()
	err = c.Run(0)
	require.NoError(t, err)
}

func newTestCase(sql string, t *testing.T) compileTestCase {
	proc := testutil.NewProcess()
	e, _, compilerCtx := testengine.New(context.Background())
	stmts, err := mysql.Parse(compilerCtx.GetContext(), sql, 1)
	require.NoError(t, err)
	pn, err := plan2.BuildPlan(compilerCtx, stmts[0])
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
