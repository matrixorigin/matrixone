// Copyright 2024 Matrix Origin
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

package frontend

import (
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
)

func executeResultRowStmtInBack(backSes *backSession,
	execCtx *ExecCtx) (err error) {
	var columns []interface{}
	mrs := backSes.GetMysqlResultSet()
	// cw.Compile might rewrite sql, here we fetch the latest version
	columns, err = execCtx.cw.GetColumns(execCtx.reqCtx)
	if err != nil {
		logError(backSes, backSes.GetDebugString(),
			"Failed to get columns from computation handler",
			zap.Error(err))
		return
	}
	for _, c := range columns {
		mysqlc := c.(Column)
		mrs.AddColumn(mysqlc)
	}
	if c, ok := execCtx.cw.(*TxnComputationWrapper); ok {
		backSes.rs = &plan.ResultColDef{ResultCols: plan2.GetResultColumnsFromPlan(c.plan)}
	}
	runBegin := time.Now()
	if _, err = execCtx.runner.Run(0); err != nil {
		return
	}

	// only log if run time is longer than 1s
	if time.Since(runBegin) > time.Second {
		logInfo(backSes, backSes.GetDebugString(), fmt.Sprintf("time of Exec.Run : %s", time.Since(runBegin).String()))
	}
	return
}
