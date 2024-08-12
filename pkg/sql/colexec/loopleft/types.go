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

package loopleft

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(LoopLeft)

const (
	Build = iota
	Probe
	End
)

type container struct {
	state    int
	probeIdx int
	bat      *batch.Batch
	rbat     *batch.Batch
	inBat    *batch.Batch
	joinBat  *batch.Batch
	expr     colexec.ExpressionExecutor
	cfs      []func(*vector.Vector, *vector.Vector, int64, int) error
}

type LoopLeft struct {
	ctr        *container
	Typs       []types.Type
	Cond       *plan.Expr
	Result     []colexec.ResultPos
	JoinMapTag int32

	vm.OperatorBase
	colexec.Projection
}

func (loopLeft *LoopLeft) GetOperatorBase() *vm.OperatorBase {
	return &loopLeft.OperatorBase
}
func init() {
	reuse.CreatePool[LoopLeft](
		func() *LoopLeft {
			return &LoopLeft{}
		},
		func(a *LoopLeft) {
			*a = LoopLeft{}
		},
		reuse.DefaultOptions[LoopLeft]().
			WithEnableChecker(),
	)
}

func (loopLeft LoopLeft) TypeName() string {
	return opName
}

func NewArgument() *LoopLeft {
	return reuse.Alloc[LoopLeft](nil)
}

func (loopLeft *LoopLeft) Release() {
	if loopLeft != nil {
		reuse.Free[LoopLeft](loopLeft, nil)
	}
}

func (loopLeft *LoopLeft) Reset(proc *process.Process, pipelineFailed bool, err error) {
	loopLeft.Free(proc, pipelineFailed, err)
}

func (loopLeft *LoopLeft) Free(proc *process.Process, pipelineFailed bool, err error) {
	if ctr := loopLeft.ctr; ctr != nil {
		ctr.cleanBatch(proc.Mp())
		ctr.cleanExprExecutor()
		loopLeft.ctr = nil
	}

	if loopLeft.ProjectList != nil {
		anal := proc.GetAnalyze(loopLeft.GetIdx(), loopLeft.GetParallelIdx(), loopLeft.GetParallelMajor())
		anal.Alloc(loopLeft.ProjectAllocSize)
		loopLeft.FreeProjection(proc)
	}
}

func (ctr *container) cleanBatch(mp *mpool.MPool) {
	if ctr.bat != nil {
		ctr.bat.Clean(mp)
		ctr.bat = nil
	}
	if ctr.rbat != nil {
		ctr.rbat.Clean(mp)
		ctr.rbat = nil
	}
	if ctr.joinBat != nil {
		ctr.joinBat.Clean(mp)
		ctr.joinBat = nil
	}
	if ctr.inBat != nil {
		ctr.inBat.Clean(mp)
		ctr.inBat = nil
	}
}

func (ctr *container) cleanExprExecutor() {
	if ctr.expr != nil {
		ctr.expr.Free()
		ctr.expr = nil
	}
}
