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

package single

import (
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(SingleJoin)

const (
	Build = iota
	Probe
	End
)

type container struct {
	state int

	rbat *batch.Batch

	expr colexec.ExpressionExecutor

	joinBat1 *batch.Batch
	cfs1     []func(*vector.Vector, *vector.Vector, int64, int) error

	joinBat2 *batch.Batch
	cfs2     []func(*vector.Vector, *vector.Vector, int64, int) error

	executor []colexec.ExpressionExecutor
	vecs     []*vector.Vector

	mp *message.JoinMap

	maxAllocSize int64
}

type SingleJoin struct {
	ctr        container
	Typs       []types.Type
	Cond       *plan.Expr
	Conditions [][]*plan.Expr
	Result     []colexec.ResultPos

	HashOnPK           bool
	RuntimeFilterSpecs []*plan.RuntimeFilterSpec
	JoinMapTag         int32
	vm.OperatorBase
	colexec.Projection
}

func (singleJoin *SingleJoin) GetOperatorBase() *vm.OperatorBase {
	return &singleJoin.OperatorBase
}

func init() {
	reuse.CreatePool[SingleJoin](
		func() *SingleJoin {
			return &SingleJoin{}
		},
		func(a *SingleJoin) {
			*a = SingleJoin{}
		},
		reuse.DefaultOptions[SingleJoin]().
			WithEnableChecker(),
	)
}

func (singleJoin SingleJoin) TypeName() string {
	return opName
}

func NewArgument() *SingleJoin {
	return reuse.Alloc[SingleJoin](nil)
}

func (singleJoin *SingleJoin) Release() {
	if singleJoin != nil {
		reuse.Free[SingleJoin](singleJoin, nil)
	}
}

func (singleJoin *SingleJoin) Reset(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &singleJoin.ctr
	//anal := proc.GetAnalyze(singleJoin.GetIdx(), singleJoin.GetParallelIdx(), singleJoin.GetParallelMajor())

	ctr.resetExecutor()
	ctr.resetExprExecutor()
	ctr.cleanHashMap()
	ctr.state = Build

	if singleJoin.ProjectList != nil {
		//anal.Alloc(singleJoin.ProjectAllocSize + singleJoin.ctr.maxAllocSize)
		if singleJoin.OpAnalyzer != nil {
			singleJoin.OpAnalyzer.Alloc(singleJoin.ProjectAllocSize + singleJoin.ctr.maxAllocSize)
		}
		singleJoin.ctr.maxAllocSize = 0
		singleJoin.ResetProjection(proc)
	} else {
		//anal.Alloc(singleJoin.ctr.maxAllocSize)
		if singleJoin.OpAnalyzer != nil {
			singleJoin.OpAnalyzer.Alloc(singleJoin.ctr.maxAllocSize)
		}
		singleJoin.ctr.maxAllocSize = 0
	}
}

func (singleJoin *SingleJoin) Free(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &singleJoin.ctr

	ctr.cleanExecutor()
	ctr.cleanExprExecutor()
	ctr.cleanBatch(proc)

	singleJoin.FreeProjection(proc)
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
	if ctr.joinBat1 != nil {
		ctr.joinBat1.Clean(proc.Mp())
	}
	if ctr.joinBat2 != nil {
		ctr.joinBat2.Clean(proc.Mp())
	}
}

func (ctr *container) cleanHashMap() {
	if ctr.mp != nil {
		ctr.mp.Free()
		ctr.mp = nil
	}
}

func (ctr *container) resetExecutor() {
	for i := range ctr.executor {
		if ctr.executor[i] != nil {
			ctr.executor[i].ResetForNextQuery()
		}
	}
}

func (ctr *container) cleanExecutor() {
	for i := range ctr.executor {
		if ctr.executor[i] != nil {
			ctr.executor[i].Free()
		}
	}
}
