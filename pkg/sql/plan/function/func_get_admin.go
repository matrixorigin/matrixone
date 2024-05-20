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

package function

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/functionUtil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func builtInInternalGetAdminName(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[int64](parameters[0])
	rs := vector.MustFunctionResult[types.Varlena](result)

	for i := uint64(0); i < uint64(length); i++ {
		accountId, null1 := p1.GetValue(i)
		if null1 {
			return moerr.NewInvalidInput(proc.Ctx, "unsupported parameter `null` for getAdminName")
		}

		v, ok := runtime.ProcessLevelRuntime().GetGlobalVariables(runtime.InternalSQLExecutor)
		if !ok {
			return moerr.NewNotSupported(proc.Ctx, "no implement sqlExecutor")
		}

		exec := v.(executor.SQLExecutor)
		opts := executor.Options{}.WithAccountID(uint32(accountId)).
			WithTxn(proc.TxnOperator).
			WithTimeZone(proc.SessionInfo.TimeZone)
		if proc.TxnOperator != nil {
			opts = opts.WithDisableIncrStatement() // this option always with WithTxn()
		}
		res, err := exec.Exec(proc.Ctx, "SELECT user_name FROM mo_catalog.mo_user ORDER BY user_id ASC LIMIT 1", opts)
		if err != nil {
			return err
		}
		defer res.Close()

		var adminNme string
		res.ReadRows(func(rows int, cols []*vector.Vector) bool {
			adminNme = cols[0].UnsafeGetStringAt(0)
			return true
		})
		if err = rs.AppendBytes(functionUtil.QuickStrToBytes(adminNme), false); err != nil {
			return err
		}
	}
	return nil
}
