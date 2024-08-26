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

package loopsingle

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

var _ vm.Operator = new(LoopSingle)

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

type LoopSingle struct {
	ctr        container
	Cond       *plan.Expr
	Typs       []types.Type
	Result     []colexec.ResultPos
	JoinMapTag int32

	vm.OperatorBase
	colexec.Projection
}

func (loopSingle *LoopSingle) GetOperatorBase() *vm.OperatorBase {
	return &loopSingle.OperatorBase
}

func init() {
	reuse.CreatePool[LoopSingle](
		func() *LoopSingle {
			return &LoopSingle{}
		},
		func(a *LoopSingle) {
			*a = LoopSingle{}
		},
		reuse.DefaultOptions[LoopSingle]().
			WithEnableChecker(),
	)
}

func (loopSingle LoopSingle) TypeName() string {
	return opName
}

func NewArgument() *LoopSingle {
	return reuse.Alloc[LoopSingle](nil)
}

func (loopSingle *LoopSingle) Release() {
	if loopSingle != nil {
		reuse.Free[LoopSingle](loopSingle, nil)
	}
}

func (loopSingle *LoopSingle) Reset(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &loopSingle.ctr

	ctr.resetExprExecutor()
	ctr.state = Build
	if ctr.bat != nil {
		ctr.bat.Clean(proc.Mp())
		ctr.bat = nil
	}

	if loopSingle.ProjectList != nil {
		//anal := proc.GetAnalyze(loopSingle.GetIdx(), loopSingle.GetParallelIdx(), loopSingle.GetParallelMajor())
		//anal.Alloc(loopSingle.ProjectAllocSize)
		if loopSingle.OpAnalyzer != nil {
			loopSingle.OpAnalyzer.Alloc(loopSingle.ProjectAllocSize)
		}
		loopSingle.ResetProjection(proc)
	}
}

func (loopSingle *LoopSingle) Free(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &loopSingle.ctr

	ctr.cleanBatch(proc.Mp())
	ctr.cleanExprExecutor()

	loopSingle.FreeProjection(proc)
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
}

func (ctr *container) resetExprExecutor() {
	if ctr.expr != nil {
		ctr.expr.ResetForNextQuery()
	}
}

func (ctr *container) cleanExprExecutor() {
	if ctr.expr != nil {
		ctr.expr.Free()
	}
	ctr.expr = nil
}
