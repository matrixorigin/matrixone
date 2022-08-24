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
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

type Tx struct {
	operator client.TxnOperator
	session  *Session
}

func (s *Session) NewTx() (*Tx, error) {
	operator, err := s.env.txnClient.New()
	if err != nil {
		return nil, err
	}
	tx := &Tx{
		operator: operator,
		session:  s,
	}
	return tx, nil
}

func (t *Tx) Exec(ctx context.Context, stmtText string, stmt tree.Statement) (err error) {

	switch stmt := stmt.(type) {

	case *tree.Use:
		t.session.currentDB = stmt.Name
		return

	case *tree.ExplainStmt:
		//TODO
		return

	}

	proc := testutil.NewProcess()
	proc.TxnOperator = t.operator
	proc.SessionInfo.TimeZone = time.Local
	compileCtx := compile.New(
		t.session.currentDB,
		stmtText,
		"",
		ctx,
		t.session.env.engine,
		proc,
		stmt,
	)

	exec := &Execution{
		ctx:  ctx,
		tx:   t,
		stmt: stmt,
	}

	var execPlan *plan.Plan
	switch stmt := stmt.(type) {

	case *tree.Select,
		*tree.ParenSelect,
		*tree.Update,
		*tree.Delete:
		optimizer := plan.NewBaseOptimizer(exec)
		query, err := optimizer.Optimize(stmt)
		if err != nil {
			return err
		}
		execPlan = &plan.Plan{
			Plan: &plan.Plan_Query{
				Query: query,
			},
		}

	default:
		var err error
		execPlan, err = plan.BuildPlan(exec, stmt)
		if err != nil {
			return err
		}
	}

	err = compileCtx.Compile(execPlan, nil, func(i any, batch *batch.Batch) error {
		//fmt.Printf("%v\n", batch) //TODO
		return nil
	})
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

func (t *Tx) Rollback(ctx context.Context) error {
	return t.operator.Rollback(ctx)
}
