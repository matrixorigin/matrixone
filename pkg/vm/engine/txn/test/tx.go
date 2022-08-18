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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	txnengine "github.com/matrixorigin/matrixone/pkg/vm/engine/txn"
)

type Tx struct {
	operator     client.TxnOperator
	engine       *txnengine.Engine
	databaseName string
}

func (t *testEnv) NewTx() *Tx {
	operator := t.txnClient.New()
	tx := &Tx{
		operator:     operator,
		engine:       t.engine,
		databaseName: defaultDatabase,
	}
	return tx
}

func (t *Tx) Exec(ctx context.Context, filePath string, stmtText string) error {

	stmts, err := mysql.Parse(stmtText)
	if err != nil {
		return err
	}

	for _, stmt := range stmts {

		stmtText := tree.String(stmt, dialect.MYSQL)
		println(stmtText)

		switch stmt := stmt.(type) {

		case *tree.Use:
			t.databaseName = stmt.Name
			continue

		}

		proc := testutil.NewProcess()
		proc.Snapshot = txnengine.OperatorToSnapshot(t.operator) //TODO remove this
		compileCtx := compile.New(t.databaseName, stmtText, "", ctx, t.engine, proc, stmt)

		//optimizer := plan.NewBaseOptimizer(t)
		//query, err := optimizer.Optimize(stmt)
		//if err != nil {
		//	return err
		//}

		exec := &Execution{
			ctx:  ctx,
			tx:   t,
			stmt: stmt,
		}

		execPlan, err := plan.BuildPlan(exec, stmt)
		if err != nil {
			return err
		}

		err = compileCtx.Compile(execPlan, nil, func(i any, batch *batch.Batch) error {
			fmt.Printf("%v\n", batch) //TODO
			return nil
		})
		if err != nil {
			return err
		}

		err = compileCtx.Run(0)
		if err != nil {

			sqlError, ok := err.(*errors.SqlError)
			if ok {
				fmt.Printf("%s\n", sqlError.Error())
				return nil
			}

			panic(err)
		}

	}

	return nil
}

func (t *Tx) Commit(ctx context.Context) error {
	return t.operator.Commit(ctx)
}
