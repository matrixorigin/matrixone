// Copyright 2026 Matrix Origin
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

package colexec

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// SetOperationKeyEvaluator evaluates physical equality keys separately from
// the rows returned by a set operator. This lets PAD SPACE normalization affect
// membership without rewriting the user-visible representative row.
type SetOperationKeyEvaluator struct {
	eval ExprEvalVector
}

func (e *SetOperationKeyEvaluator) Prepare(proc *process.Process, exprs []*plan.Expr) error {
	if len(exprs) == 0 {
		e.eval.Free()
		return nil
	}
	if len(e.eval.Executor) == len(exprs) {
		e.eval.ResetForNextQuery()
		return nil
	}
	e.eval.Free()
	var err error
	e.eval, err = MakeEvalVector(proc, exprs)
	return err
}

func (e *SetOperationKeyEvaluator) Eval(proc *process.Process, bat *batch.Batch) ([]*vector.Vector, error) {
	if len(e.eval.Executor) == 0 {
		return bat.Vecs, nil
	}
	input := []*batch.Batch{bat}
	for i := range e.eval.Executor {
		vec, err := e.eval.Executor[i].Eval(proc, input, nil)
		if err != nil {
			return nil, err
		}
		e.eval.Vec[i] = vec
	}
	return e.eval.Vec, nil
}

func (e *SetOperationKeyEvaluator) Reset() {
	e.eval.ResetForNextQuery()
}

func (e *SetOperationKeyEvaluator) Free() {
	e.eval.Free()
}
