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

package colexec

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type Projection struct {
	projectBat       *batch.Batch
	ProjectList      []*plan.Expr
	ProjectExecutors []ExpressionExecutor
	uafs             []func(v, w *vector.Vector) error

	ProjectAllocSize int64
}

func (projection *Projection) PrepareProjection(proc *process.Process) (err error) {
	if len(projection.ProjectList) == 0 {
		return
	}
	projection.ProjectAllocSize = 0

	projection.ProjectExecutors, err = NewExpressionExecutorsFromPlanExpressions(proc, projection.ProjectList)
	if err != nil {
		return
	}

	projection.uafs = make([]func(v *vector.Vector, w *vector.Vector) error, len(projection.ProjectList))
	for i, e := range projection.ProjectList {
		if e.Typ.Id != 0 {
			typ := types.New(types.T(e.Typ.Id), e.Typ.Width, e.Typ.Scale)
			projection.uafs[i] = vector.GetUnionAllFunction(typ, proc.Mp())
		}
	}

	return
}

func (projection *Projection) EvalProjection(bat *batch.Batch, proc *process.Process) (*batch.Batch, error) {
	if bat == nil || bat.IsEmpty() || bat.Last() {
		return bat, nil
	}

	projection.projectBat = batch.NewWithSize(len(projection.ProjectList))
	projection.projectBat.ShuffleIDX = bat.ShuffleIDX

	for i := range projection.ProjectExecutors {
		vec, err := projection.ProjectExecutors[i].Eval(proc, []*batch.Batch{bat}, nil)
		if err != nil {
			for _, newV := range projection.projectBat.Vecs {
				if newV != nil {
					for k, oldV := range bat.Vecs {
						if oldV != nil && newV == oldV {
							bat.Vecs[k] = nil
						}
					}
				}
			}
			projection.projectBat = nil
			return nil, err
		}
		projection.projectBat.Vecs[i] = vec
	}
	newAlloc, err := FixProjectionResult(proc, projection.ProjectExecutors, projection.uafs, projection.projectBat, bat)
	if err != nil {
		return nil, err
	}
	projection.ProjectAllocSize = max(projection.ProjectAllocSize, int64(newAlloc))
	projection.projectBat.SetRowCount(bat.RowCount())
	return projection.projectBat, nil
}

func (projection *Projection) ResetProjection(proc *process.Process) {
}

func (projection *Projection) FreeProjection(proc *process.Process) {
	if projection != nil {
		for i := range projection.ProjectExecutors {
			if projection.ProjectExecutors[i] != nil {
				projection.ProjectExecutors[i].Free()
			}
		}
		if projection.projectBat != nil {
			projection.projectBat.Clean(proc.Mp())
			projection.projectBat = nil
		}
	}
}
