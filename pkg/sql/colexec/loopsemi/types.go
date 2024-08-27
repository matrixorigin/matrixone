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

package loopsemi

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(LoopSemi)

const (
	Build = iota
	Probe
	End
)

type container struct {
	state   int
	lastrow int
	bat     *batch.Batch
	rbat    *batch.Batch
	joinBat *batch.Batch
	buf     *batch.Batch
	expr    colexec.ExpressionExecutor
	cfs     []func(*vector.Vector, *vector.Vector, int64, int) error
}

type LoopSemi struct {
	ctr        container
	Result     []int32
	Cond       *plan.Expr
	JoinMapTag int32

	vm.OperatorBase
	colexec.Projection
}

func (loopSemi *LoopSemi) GetOperatorBase() *vm.OperatorBase {
	return &loopSemi.OperatorBase
}

func init() {
	reuse.CreatePool[LoopSemi](
		func() *LoopSemi {
			return &LoopSemi{}
		},
		func(a *LoopSemi) {
			*a = LoopSemi{}
		},
		reuse.DefaultOptions[LoopSemi]().
			WithEnableChecker(),
	)
}

func (loopSemi LoopSemi) TypeName() string {
	return opName
}

func NewArgument() *LoopSemi {
	return reuse.Alloc[LoopSemi](nil)
}

func (loopSemi *LoopSemi) Release() {
	if loopSemi != nil {
		reuse.Free[LoopSemi](loopSemi, nil)
	}
}

func (loopSemi *LoopSemi) Reset(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &loopSemi.ctr

	ctr.resetExprExecutor()
	ctr.state = Build
	ctr.lastrow = 0
	if ctr.bat != nil {
		ctr.bat.Clean(proc.Mp())
		ctr.bat = nil
	}

	if loopSemi.ProjectList != nil {
		//anal := proc.GetAnalyze(loopSemi.GetIdx(), loopSemi.GetParallelIdx(), loopSemi.GetParallelMajor())
		//anal.Alloc(loopSemi.ProjectAllocSize)
		if loopSemi.OpAnalyzer != nil {
			loopSemi.OpAnalyzer.Alloc(loopSemi.ProjectAllocSize)
		}
		loopSemi.ResetProjection(proc)
	}
}

func (loopSemi *LoopSemi) Free(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &loopSemi.ctr

	ctr.cleanBatch(proc.Mp())
	ctr.cleanExprExecutor()

	loopSemi.FreeProjection(proc)
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
