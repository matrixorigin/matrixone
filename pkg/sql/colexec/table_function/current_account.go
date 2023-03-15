// Copyright 2022 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func currentAccountPrepare(proc *process.Process, arg *Argument) error {
	if len(arg.Args) > 0 {
		return moerr.NewInvalidInput(proc.Ctx, "current_account: no argument is required")
	}
	return nil
}

func getAccountName(proc *process.Process) *vector.Vector {
	return vector.NewConstBytes(types.T_varchar.ToType(), []byte(proc.SessionInfo.Account), 1, proc.Mp())
}

func getRoleName(proc *process.Process) *vector.Vector {
	return vector.NewConstBytes(types.T_varchar.ToType(), []byte(proc.SessionInfo.Role), 1, proc.Mp())
}

func getUserName(proc *process.Process) *vector.Vector {
	return vector.NewConstBytes(types.T_varchar.ToType(), []byte(proc.SessionInfo.User), 1, proc.Mp())
}

func getAccountId(proc *process.Process) *vector.Vector {
	return vector.NewConstFixed(types.T_uint32.ToType(), proc.SessionInfo.AccountId, 1, proc.Mp())
}

func getRoleId(proc *process.Process) *vector.Vector {
	return vector.NewConstFixed(types.T_uint32.ToType(), proc.SessionInfo.RoleId, 1, proc.Mp())
}

func getUserId(proc *process.Process) *vector.Vector {
	return vector.NewConstFixed(types.T_uint32.ToType(), proc.SessionInfo.UserId, 1, proc.Mp())
}

func currentAccountCall(_ int, proc *process.Process, arg *Argument) (bool, error) {
	rbat := batch.New(false, arg.Attrs)
	for i, attr := range arg.Attrs {
		switch attr {
		case "account_name":
			rbat.Vecs[i] = getAccountName(proc)
		case "account_id":
			rbat.Vecs[i] = getAccountId(proc)
		case "user_name":
			rbat.Vecs[i] = getUserName(proc)
		case "user_id":
			rbat.Vecs[i] = getUserId(proc)
		case "role_name":
			rbat.Vecs[i] = getRoleName(proc)
		case "role_id":
			rbat.Vecs[i] = getRoleId(proc)
		default:
			return false, moerr.NewInvalidInput(proc.Ctx, "%v is not supported by current_account()", attr)
		}
	}
	rbat.InitZsOne(1)
	proc.SetInputBatch(rbat)
	return true, nil
}
