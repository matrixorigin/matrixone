// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table_function

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	executortest "github.com/matrixorigin/matrixone/pkg/util/executor/test"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Reuse the real borrowed-transaction boundary without starting services.
func mockIndexTxn(t *testing.T, proc *process.Process, fn func(string, executor.StatementOption) (executor.Result, error)) {
	t.Helper()
	ctrl := gomock.NewController(t)
	sqlExec := executortest.NewMockSQLExecutor(ctrl)
	txnExec := executortest.NewMockTxnExecutor(ctrl)
	rt := moruntime.ServiceRuntime(proc.GetService())
	prev, exists := rt.GetGlobalVariables(moruntime.InternalSQLExecutor)
	rt.SetGlobalVariables(moruntime.InternalSQLExecutor, sqlExec)
	t.Cleanup(func() {
		if exists {
			rt.SetGlobalVariables(moruntime.InternalSQLExecutor, prev)
		} else {
			rt.CompareAndDeleteGlobalVariables(moruntime.InternalSQLExecutor, sqlExec)
		}
	})
	sqlExec.EXPECT().ExecTxn(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, cb func(executor.TxnExecutor) error, _ executor.Options) error {
		return cb(txnExec)
	}).AnyTimes()
	sqlExec.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, sql string, opts executor.Options) (executor.Result, error) {
		return fn(sql, opts.StatementOption())
	}).AnyTimes()
	txnExec.EXPECT().Exec(gomock.Any(), gomock.Any()).DoAndReturn(fn).AnyTimes()
}
