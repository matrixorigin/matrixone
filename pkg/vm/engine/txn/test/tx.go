// Copyright 2022 Matrix Origin
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

package testtxnengine

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	txnengine "github.com/matrixorigin/matrixone/pkg/vm/engine/txn"
)

type Tx struct {
	operator client.TxnOperator
	engine   *txnengine.Engine
}

func (t *testEnv) NewTx() *Tx {
	operator := t.txnClient.New()
	tx := &Tx{
		operator: operator,
		engine:   t.engine,
	}
	return tx
}

func (t *Tx) Exec(ctx context.Context, stmtText string) error {

	stmts, err := mysql.Parse(stmtText)
	if err != nil {
		return err
	}
	stmt := stmts[0]

	proc := testutil.NewProcess()
	proc.Snapshot = txnengine.OperatorToSnapshot(t.operator) //TODO remove this
	compileCtx := compile.New("test", stmtText, "", ctx, t.engine, proc, stmt)

	//optimizer := plan.NewBaseOptimizer(t)
	//query, err := optimizer.Optimize(stmt)
	//if err != nil {
	//	return err
	//}

	exec := &Execution{
		tx:  t,
		ctx: ctx,
	}

	execPlan, err := plan.BuildPlan(exec, stmt)
	if err != nil {
		return err
	}

	err = compileCtx.Compile(execPlan, nil, nil)
	if err != nil {
		return err
	}

	err = compileCtx.Run(0)
	if err != nil {
		return err
	}

	return nil
}

func (t *Tx) Commit(ctx context.Context) error {
	return t.operator.Commit(ctx)
}
