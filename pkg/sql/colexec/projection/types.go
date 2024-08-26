// Copyright 2021 Matrix Origin
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

package projection

import (
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(Projection)

type Projection struct {
	ctr         container
	ProjectList []*plan.Expr
	vm.OperatorBase

	maxAllocSize int
}

func (projection *Projection) GetOperatorBase() *vm.OperatorBase {
	return &projection.OperatorBase
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
	return opName
}

func NewArgument() *Projection {
	return reuse.Alloc[Projection](nil)
}

func (projection *Projection) Release() {
	if projection != nil {
		reuse.Free[Projection](projection, nil)
	}
}

type container struct {
	buf           *batch.Batch
	projExecutors []colexec.ExpressionExecutor
}

func (projection *Projection) Reset(proc *process.Process, pipelineFailed bool, err error) {
	for i := range projection.ctr.projExecutors {
		if projection.ctr.projExecutors[i] != nil {
			projection.ctr.projExecutors[i].ResetForNextQuery()
		}
	}
	//anal := proc.GetAnalyze(projection.GetIdx(), projection.GetParallelIdx(), projection.GetParallelMajor())
	//anal.Alloc(int64(projection.maxAllocSize))
	// 有可能发生这样的情况： pipeline还未执行prepare，就发生错误，触发defer pipeline.Cleanup(),
	// 执行reset和free，如果reset中有统计操作，就会发生空指针
	if projection.OpAnalyzer != nil {
		projection.OpAnalyzer.Alloc(int64(projection.maxAllocSize))
	}
	projection.maxAllocSize = 0
}

func (projection *Projection) Free(proc *process.Process, pipelineFailed bool, err error) {
	for i := range projection.ctr.projExecutors {
		if projection.ctr.projExecutors[i] != nil {
			projection.ctr.projExecutors[i].Free()
		}
	}
	projection.ctr.projExecutors = nil
}
