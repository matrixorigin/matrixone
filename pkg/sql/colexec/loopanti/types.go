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

package loopanti

import (
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(LoopAnti)

const (
	Build = iota
	Probe
	End
)

type container struct {
	state   int
	bat     *batch.Batch
	rbat    *batch.Batch
	joinBat *batch.Batch
	expr    colexec.ExpressionExecutor
	cfs     []func(*vector.Vector, *vector.Vector, int64, int) error
}

type LoopAnti struct {
	ctr        container
	Result     []int32
	Cond       *plan.Expr
	Typs       []types.Type
	JoinMapTag int32

	vm.OperatorBase
	colexec.Projection
}

func (loopAnti *LoopAnti) GetOperatorBase() *vm.OperatorBase {
	return &loopAnti.OperatorBase
}

func init() {
	reuse.CreatePool[LoopAnti](
		func() *LoopAnti {
			return &LoopAnti{}
		},
		func(a *LoopAnti) {
			*a = LoopAnti{}
		},
		reuse.DefaultOptions[LoopAnti]().
			WithEnableChecker(),
	)
}

func (loopAnti LoopAnti) TypeName() string {
	return opName
}

func NewArgument() *LoopAnti {
	return reuse.Alloc[LoopAnti](nil)
}

func (loopAnti *LoopAnti) Release() {
	if loopAnti != nil {
		reuse.Free[LoopAnti](loopAnti, nil)
	}
}

func (loopAnti *LoopAnti) Reset(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &loopAnti.ctr

	ctr.resetExprExecutor()
	ctr.state = Build
	ctr.bat.Clean(proc.Mp())
	ctr.bat = nil

	if loopAnti.ProjectList != nil {
		anal := proc.GetAnalyze(loopAnti.GetIdx(), loopAnti.GetParallelIdx(), loopAnti.GetParallelMajor())
		anal.Alloc(loopAnti.ProjectAllocSize)
		loopAnti.ResetProjection(proc)
	}
}

func (loopAnti *LoopAnti) Free(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &loopAnti.ctr

	ctr.cleanExprExecutor()
	ctr.cleanBatch(proc)

	if loopAnti.ProjectList != nil {
		loopAnti.FreeProjection(proc)
	}
}

func (ctr *container) resetExprExecutor() {
	if ctr.expr != nil {
		ctr.expr.ResetForNextQuery()
	}
}

func (ctr *container) cleanExprExecutor() {
	if ctr.expr != nil {
		ctr.expr.Free()
		ctr.expr = nil
	}
}

func (ctr *container) cleanBatch(proc *process.Process) {
	if ctr.rbat != nil {
		ctr.rbat.Clean(proc.Mp())
	}
	if ctr.bat != nil {
		ctr.bat.Clean(proc.Mp())
	}
}
