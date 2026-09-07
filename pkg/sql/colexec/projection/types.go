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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(Projection)

type Projection struct {
	ctr         container
	ProjectList []*plan.Expr

	// GroupingSetCount and GroupingFlags turn this projection into a
	// vector-level grouping-set expander. The first len(flags)/count columns are
	// grouping keys, the penultimate column marks runtime-empty synthetic rows,
	// and the last column is the generated set id.
	GroupingSetCount int
	GroupingFlags    []bool
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
	expandBuf     *batch.Batch
	projExecutors []colexec.ExpressionExecutor
	expandOwned   []*vector.Vector
	nextSet       int
	hasInput      bool
	inputSeen     bool
	childDone     bool
}

func (projection *Projection) Reset(proc *process.Process, pipelineFailed bool, err error) {
	projection.freeExpandOwned(proc)
	projection.ctr.nextSet = 0
	projection.ctr.hasInput = false
	projection.ctr.inputSeen = false
	projection.ctr.childDone = false
	for i := range projection.ctr.projExecutors {
		if projection.ctr.projExecutors[i] != nil {
			projection.ctr.projExecutors[i].ResetForNextQuery()
		}
	}

	if projection.OpAnalyzer != nil {
		projection.OpAnalyzer.Alloc(int64(projection.maxAllocSize))
	}
	projection.maxAllocSize = 0
}

func (projection *Projection) Free(proc *process.Process, pipelineFailed bool, err error) {
	projection.freeExpandOwned(proc)
	for i := range projection.ctr.projExecutors {
		if projection.ctr.projExecutors[i] != nil {
			projection.ctr.projExecutors[i].Free()
		}
	}
	projection.ctr.projExecutors = nil
	projection.ctr.buf = nil
	projection.ctr.expandBuf = nil
}

func (projection *Projection) freeExpandOwned(proc *process.Process) {
	for _, vec := range projection.ctr.expandOwned {
		if vec != nil {
			vec.Free(proc.Mp())
		}
	}
	projection.ctr.expandOwned = projection.ctr.expandOwned[:0]
}

func (projection *Projection) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	return input, nil
}
