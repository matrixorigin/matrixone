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

package loopjoin

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

var _ vm.Operator = new(LoopJoin)

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
	joinBat  *batch.Batch
	expr     colexec.ExpressionExecutor
	cfs      []func(*vector.Vector, *vector.Vector, int64, int) error
}

type LoopJoin struct {
	ctr        container
	Cond       *plan.Expr
	Result     []colexec.ResultPos
	JoinMapTag int32

	vm.OperatorBase
	colexec.Projection
}

func (loopJoin *LoopJoin) GetOperatorBase() *vm.OperatorBase {
	return &loopJoin.OperatorBase
}

func init() {
	reuse.CreatePool[LoopJoin](
		func() *LoopJoin {
			return &LoopJoin{}
		},
		func(a *LoopJoin) {
			*a = LoopJoin{}
		},
		reuse.DefaultOptions[LoopJoin]().
			WithEnableChecker(),
	)
}

func (loopJoin LoopJoin) TypeName() string {
	return opName
}

func NewArgument() *LoopJoin {
	return reuse.Alloc[LoopJoin](nil)
}

func (loopJoin *LoopJoin) Release() {
	if loopJoin != nil {
		reuse.Free[LoopJoin](loopJoin, nil)
	}
}

func (loopJoin *LoopJoin) Reset(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &loopJoin.ctr

	ctr.resetExprExecutor()
	ctr.state = Build
	ctr.probeIdx = 0

	if ctr.bat != nil {
		ctr.bat.Clean(proc.Mp())
		ctr.bat = nil
	}

	if loopJoin.ProjectList != nil {
		//anal := proc.GetAnalyze(loopJoin.GetIdx(), loopJoin.GetParallelIdx(), loopJoin.GetParallelMajor())
		//anal.Alloc(loopJoin.ProjectAllocSize)
		if loopJoin.OpAnalyzer != nil {
			loopJoin.OpAnalyzer.Alloc(loopJoin.ProjectAllocSize)
		}
		loopJoin.ResetProjection(proc)
	}
}

func (loopJoin *LoopJoin) Free(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &loopJoin.ctr

	ctr.cleanBatch(proc.Mp())
	ctr.cleanExprExecutor()

	if loopJoin.ProjectList != nil {
		loopJoin.FreeProjection(proc)
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
	ctr.expr = nil
}
