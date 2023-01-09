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

func currentAccountCall(_ int, proc *process.Process, arg *Argument) (bool, error) {
	rbat := batch.New(false, arg.Attrs)
	rbat.Vecs[0] = vector.NewConstString(
		types.Type{Oid: types.T_varchar, Width: types.MaxVarcharLen},
		1,
		proc.SessionInfo.Account,
		proc.Mp(),
	)
	rbat.Vecs[1] = vector.NewConstFixed[uint32](types.Type{Oid: types.T_uint32}, 1, proc.SessionInfo.AccountId, proc.Mp())
	rbat.Vecs[2] = vector.NewConstString(
		types.Type{Oid: types.T_varchar, Width: types.MaxVarcharLen},
		1,
		proc.SessionInfo.User,
		proc.Mp(),
	)
	rbat.Vecs[3] = vector.NewConstFixed[uint32](types.Type{Oid: types.T_uint32}, 1, proc.SessionInfo.UserId, proc.Mp())
	rbat.Vecs[4] = vector.NewConstString(
		types.Type{Oid: types.T_varchar, Width: types.MaxVarcharLen},
		1,
		proc.SessionInfo.Role,
		proc.Mp(),
	)
	rbat.Vecs[5] = vector.NewConstFixed[uint32](types.Type{Oid: types.T_uint32}, 1, proc.SessionInfo.RoleId, proc.Mp())
	rbat.InitZsOne(1)
	proc.SetInputBatch(rbat)
	return true, nil
}
