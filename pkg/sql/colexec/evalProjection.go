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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type Projection struct {
	projectBat       *batch.Batch
	ProjectList      []*plan.Expr
	ProjectExecutors []ExpressionExecutor
	ProjectAllocSize int64
}

func (projection *Projection) PrepareProjection(proc *process.Process) (err error) {
	if len(projection.ProjectList) == 0 {
		return
	}
	projection.ProjectAllocSize = 0
	if projection.ProjectExecutors == nil {
		projection.ProjectExecutors, err = NewExpressionExecutorsFromPlanExpressions(proc, projection.ProjectList)
		if err != nil {
			return
		}
	}
	if projection.projectBat == nil {
		projection.projectBat = batch.NewWithSize(len(projection.ProjectList))
	}

	return
}

func (projection *Projection) EvalProjection(bat *batch.Batch, proc *process.Process) (*batch.Batch, error) {
	if bat == nil || bat.IsEmpty() || bat.Last() {
		return bat, nil
	}
	if len(projection.ProjectExecutors) == 0 {
		return bat, nil
	}

	projection.projectBat.ShuffleIDX = bat.ShuffleIDX

	for i := range projection.ProjectExecutors {
		vec, err := projection.ProjectExecutors[i].Eval(proc, []*batch.Batch{bat}, nil)
		if err != nil {
			return nil, err
		}
		// for projection operator, all Vectors of projectBat come from executor.Eval
		// and will not be modified within projection operator. so we can used the result of executor.Eval directly.
		// (if operator will modify vector/agg of batch, you should make a copy)
		// however, it should be noted that since they directly come from executor.Eval
		// these vectors cannot be free by batch.Clean directly and must be handed over executor.Free
		projection.projectBat.Vecs[i] = vec
	}
	projection.ProjectAllocSize = max(projection.ProjectAllocSize, int64(projection.projectBat.Size()))
	projection.projectBat.SetRowCount(bat.RowCount())
	return projection.projectBat, nil
}

func (projection *Projection) ResetProjection(proc *process.Process) {
	for i := range projection.ProjectExecutors {
		if projection.ProjectExecutors[i] != nil {
			projection.ProjectExecutors[i].ResetForNextQuery()
		}
	}
	projection.ProjectAllocSize = 0
}

func (projection *Projection) FreeProjection(proc *process.Process) {
	for i := range projection.ProjectExecutors {
		if projection.ProjectExecutors[i] != nil {
			projection.ProjectExecutors[i].Free()
		}
	}
	projection.ProjectExecutors = nil

	if projection.projectBat != nil {
		// do not free projectBat's Vecs,  that will be free in ProjectExecutors[x].Free()
		projection.projectBat.Vecs = nil
		// allways call batch.Clean, even if there is no data
		projection.projectBat.Clean(proc.Mp())
		projection.projectBat = nil
	}
}
