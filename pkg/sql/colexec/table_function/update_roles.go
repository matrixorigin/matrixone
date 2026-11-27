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
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type updateRolesState struct{ simpleOneBatchState }

func updateRolesPrepare(proc *process.Process, tf *TableFunction) (tvfState, error) {
	if len(tf.Args) == 0 || len(tf.Args) > math.MaxInt32 || len(tf.Rets) != 2 || len(tf.Attrs) != 2 ||
		tf.Rets[0] == nil || tf.Rets[1] == nil ||
		types.T(tf.Rets[0].Typ.Id) != types.T_int32 || types.T(tf.Rets[1].Typ.Id) != types.T_Rowid {
		return nil, moerr.NewInternalErrorNoCtx("invalid repeated UPDATE role function")
	}
	for _, arg := range tf.Args {
		if arg == nil || types.T(arg.Typ.Id) != types.T_Rowid {
			return nil, moerr.NewInternalErrorNoCtx("invalid repeated UPDATE role identity")
		}
	}
	var err error
	tf.ctr.executorsForArgs, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, tf.Args)
	if err != nil {
		return nil, err
	}
	tf.ctr.argVecs = make([]*vector.Vector, len(tf.Args))
	return &updateRolesState{}, nil
}

func (s *updateRolesState) start(tf *TableFunction, proc *process.Process, row int, _ process.Analyzer) error {
	if row%256 == 0 {
		if err := context.Cause(proc.Ctx); err != nil {
			return err
		}
	}
	if len(tf.ctr.argVecs) != len(tf.Args) {
		return moerr.NewInternalErrorNoCtx("invalid repeated UPDATE role arguments")
	}
	s.startPreamble(tf, proc, row)
	for i, input := range tf.ctr.argVecs {
		if i > 0 && i%256 == 0 {
			if err := context.Cause(proc.Ctx); err != nil {
				return err
			}
		}
		if input == nil || row < 0 || row >= input.Length() {
			return moerr.NewInternalErrorNoCtx("invalid repeated UPDATE role input")
		}
		if input.IsNull(uint64(row)) {
			continue
		}
		if err := vector.AppendFixed(s.batch.Vecs[0], int32(i), false, proc.Mp()); err != nil {
			return err
		}
		if err := vector.AppendFixed(s.batch.Vecs[1], vector.GetFixedAtNoTypeCheck[types.Rowid](input, row), false, proc.Mp()); err != nil {
			return err
		}
		s.batch.AddRowCount(1)
	}
	return nil
}
