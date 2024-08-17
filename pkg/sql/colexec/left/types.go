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

package left

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

var _ vm.Operator = new(LeftJoin)

const (
	Build = iota
	Probe
	End
)

type container struct {
	state int

	batchRowCount int64
	lastrow       int
	inbat         *batch.Batch
	rbat          *batch.Batch

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

type LeftJoin struct {
	ctr        container
	Result     []colexec.ResultPos
	Typs       []types.Type
	Cond       *plan.Expr
	Conditions [][]*plan.Expr

	HashOnPK           bool
	IsShuffle          bool
	ShuffleIdx         int32
	RuntimeFilterSpecs []*plan.RuntimeFilterSpec
	JoinMapTag         int32

	vm.OperatorBase
	colexec.Projection
}

func (leftJoin *LeftJoin) GetOperatorBase() *vm.OperatorBase {
	return &leftJoin.OperatorBase
}

func init() {
	reuse.CreatePool[LeftJoin](
		func() *LeftJoin {
			return &LeftJoin{}
		},
		func(a *LeftJoin) {
			*a = LeftJoin{}
		},
		reuse.DefaultOptions[LeftJoin]().
			WithEnableChecker(),
	)
}

func (leftJoin LeftJoin) TypeName() string {
	return opName
}

func NewArgument() *LeftJoin {
	return reuse.Alloc[LeftJoin](nil)
}

func (leftJoin *LeftJoin) Release() {
	if leftJoin != nil {
		reuse.Free[LeftJoin](leftJoin, nil)
	}
}

func (leftJoin *LeftJoin) Reset(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &leftJoin.ctr
	anal := proc.GetAnalyze(leftJoin.GetIdx(), leftJoin.GetParallelIdx(), leftJoin.GetParallelMajor())

	ctr.resetExecutor()
	ctr.resetExprExecutor()
	ctr.cleanHashMap()
	ctr.inbat = nil
	ctr.lastrow = 0
	ctr.state = Build
	ctr.batchRowCount = 0

	if leftJoin.ProjectList != nil {
		anal.Alloc(leftJoin.ProjectAllocSize + leftJoin.ctr.maxAllocSize)
		leftJoin.ctr.maxAllocSize = 0
		leftJoin.ResetProjection(proc)
	} else {
		anal.Alloc(leftJoin.ctr.maxAllocSize)
		leftJoin.ctr.maxAllocSize = 0
	}
}

func (leftJoin *LeftJoin) Free(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &leftJoin.ctr

	ctr.cleanExecutor()
	ctr.cleanExprExecutor()
	ctr.cleanBatch(proc)

	leftJoin.FreeProjection(proc)
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
