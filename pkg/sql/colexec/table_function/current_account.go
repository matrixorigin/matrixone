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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type currentAccountState struct {
	simpleOneBatchState
}

func (s *currentAccountState) reset(tf *TableFunction, proc *process.Process) {
	s.called = false
	// do not reset batch
}

func currentAccountPrepare(_ *process.Process, _ *TableFunction) (tvfState, error) {
	return &currentAccountState{}, nil
}

func (s *currentAccountState) start(tf *TableFunction, proc *process.Process, nthRow int) error {
	// we do not call startPreamble here,
	// there are very specific way of constructing the batch, below.
	var err error
	if s.batch == nil {
		s.batch = tf.createResultBatch()
		for i, attr := range tf.Attrs {
			switch attr {
			case "account_name":
				s.batch.Vecs[i], err = vector.NewConstBytes(types.T_varchar.ToType(), []byte(proc.GetSessionInfo().Account), 1, proc.Mp())
			case "account_id":
				s.batch.Vecs[i], err = vector.NewConstFixed(types.T_uint32.ToType(), proc.GetSessionInfo().AccountId, 1, proc.Mp())
			case "user_name":
				s.batch.Vecs[i], err = vector.NewConstBytes(types.T_varchar.ToType(), []byte(proc.GetSessionInfo().User), 1, proc.Mp())
			case "user_id":
				s.batch.Vecs[i], err = vector.NewConstFixed(types.T_uint32.ToType(), proc.GetSessionInfo().UserId, 1, proc.Mp())
			case "role_name":
				s.batch.Vecs[i], err = vector.NewConstBytes(types.T_varchar.ToType(), []byte(proc.GetSessionInfo().Role), 1, proc.Mp())
			case "role_id":
				s.batch.Vecs[i], err = vector.NewConstFixed(types.T_uint32.ToType(), proc.GetSessionInfo().RoleId, 1, proc.Mp())
			default:
				err = moerr.NewInvalidInputf(proc.Ctx, "%v is not supported by current_account()", attr)
			}
			if err != nil {
				return err
			}
		}
		s.batch.SetRowCount(1)
	}
	s.called = false
	return nil
}
