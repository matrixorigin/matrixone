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

package loopmark

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

var _ vm.Operator = new(LoopMark)

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

type LoopMark struct {
	ctr        container
	Cond       *plan.Expr
	Typs       []types.Type
	Result     []int32
	JoinMapTag int32

	vm.OperatorBase
	colexec.Projection
}

func (loopMark *LoopMark) GetOperatorBase() *vm.OperatorBase {
	return &loopMark.OperatorBase
}

func init() {
	reuse.CreatePool[LoopMark](
		func() *LoopMark {
			return &LoopMark{}
		},
		func(a *LoopMark) {
			*a = LoopMark{}
		},
		reuse.DefaultOptions[LoopMark]().
			WithEnableChecker(),
	)
}

func (loopMark LoopMark) TypeName() string {
	return opName
}

func NewArgument() *LoopMark {
	return reuse.Alloc[LoopMark](nil)
}

func (loopMark *LoopMark) Release() {
	if loopMark != nil {
		reuse.Free[LoopMark](loopMark, nil)
	}
}

func (loopMark *LoopMark) Reset(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &loopMark.ctr

	ctr.resetExprExecutor()
	ctr.state = Build

	if ctr.bat != nil {
		ctr.bat.Clean(proc.Mp())
		ctr.bat = nil
	}

	if loopMark.ProjectList != nil {
		anal := proc.GetAnalyze(loopMark.GetIdx(), loopMark.GetParallelIdx(), loopMark.GetParallelMajor())
		anal.Alloc(loopMark.ProjectAllocSize)
		loopMark.ResetProjection(proc)
	}
}

func (loopMark *LoopMark) Free(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &loopMark.ctr

	ctr.cleanBatch(proc.Mp())
	ctr.cleanExprExecutor()

	if loopMark.ProjectList != nil {
		loopMark.FreeProjection(proc)
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
