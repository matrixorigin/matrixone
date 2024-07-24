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
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type Projection struct {
	ProjectList   []*plan.Expr
	ProjExecutors []ExpressionExecutor
	uafs          []func(v, w *vector.Vector) error

	MaxAllocSize int
}

func init() {
	reuse.CreatePool[Projection](
		func() *Projection {
			return &Projection{}
		},
		func(a *Projection) {
			*a = Projection{}
		},
		reuse.DefaultOptions[Projection]().
			WithEnableChecker(),
	)
}

func (projection Projection) TypeName() string {
	return "project"
}

func NewProjection(ProjectList []*plan.Expr) *Projection {
	projection := reuse.Alloc[Projection](nil)
	projection.ProjectList = ProjectList
	return projection
}

func (projection *Projection) Prepare(proc *process.Process) (err error) {
	if len(projection.ProjectList) == 0 {
		return
	}
	projection.ProjExecutors, err = NewExpressionExecutorsFromPlanExpressions(proc, projection.ProjectList)
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

func (projection *Projection) Eval(bat *batch.Batch, proc *process.Process) (*batch.Batch, error) {
	if bat == nil || bat.IsEmpty() || bat.Last() {
		return bat, nil
	}

	result := batch.NewWithSize(len(projection.ProjectList))
	result.ShuffleIDX = bat.ShuffleIDX

	for i := range projection.ProjExecutors {
		vec, err := projection.ProjExecutors[i].Eval(proc, []*batch.Batch{bat}, nil)
		if err != nil {
			for _, newV := range result.Vecs {
				if newV != nil {
					for k, oldV := range bat.Vecs {
						if oldV != nil && newV == oldV {
							bat.Vecs[k] = nil
						}
					}
				}
			}
			result = nil
			return nil, err
		}
		result.Vecs[i] = vec
	}
	newAlloc, err := FixProjectionResult(proc, projection.ProjExecutors, projection.uafs, result, bat)
	if err != nil {
		return nil, err
	}
	projection.MaxAllocSize = max(projection.MaxAllocSize, newAlloc)
	result.SetRowCount(bat.RowCount())
	return result, nil
}

func (projection *Projection) Free() {
	if projection != nil {
		for i := range projection.ProjExecutors {
			if projection.ProjExecutors[i] != nil {
				projection.ProjExecutors[i].Free()
			}
		}
		reuse.Free[Projection](projection, nil)
	}
}
