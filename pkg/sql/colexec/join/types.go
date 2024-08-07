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

package join

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

var _ vm.Operator = new(InnerJoin)

const (
	Build = iota
	Probe
	End
)

type container struct {
	state int

	batches       []*batch.Batch
	batchRowCount int64
	lastrow       int
	rbat          *batch.Batch

	expr colexec.ExpressionExecutor

	joinBat1 *batch.Batch
	cfs1     []func(*vector.Vector, *vector.Vector, int64, int) error

	joinBat2 *batch.Batch
	cfs2     []func(*vector.Vector, *vector.Vector, int64, int) error

	executor []colexec.ExpressionExecutor
	vecs     []*vector.Vector

	mp  *message.JoinMap
	bat *batch.Batch

	maxAllocSize int64
}

type InnerJoin struct {
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

func (innerJoin *InnerJoin) GetOperatorBase() *vm.OperatorBase {
	return &innerJoin.OperatorBase
}

func init() {
	reuse.CreatePool[InnerJoin](
		func() *InnerJoin {
			return &InnerJoin{}
		},
		func(a *InnerJoin) {
			*a = InnerJoin{}
		},
		reuse.DefaultOptions[InnerJoin]().
			WithEnableChecker(),
	)
}

func (innerJoin InnerJoin) TypeName() string {
	return opName
}

func NewArgument() *InnerJoin {
	return reuse.Alloc[InnerJoin](nil)
}

func (innerJoin *InnerJoin) Release() {
	if innerJoin != nil {
		reuse.Free[InnerJoin](innerJoin, nil)
	}
}

func (innerJoin *InnerJoin) Reset(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &innerJoin.ctr
	anal := proc.GetAnalyze(innerJoin.GetIdx(), innerJoin.GetParallelIdx(), innerJoin.GetParallelMajor())

	ctr.resetExecutor()
	ctr.resetExprExecutor()
	ctr.cleanHashMap() // -> resetHashMap
	ctr.bat = nil      // children[0] clean
	ctr.batches = nil  // reset/free in hashmap
	ctr.lastrow = 0

	if innerJoin.ProjectList != nil {
		anal.Alloc(innerJoin.ProjectAllocSize + innerJoin.ctr.maxAllocSize)
		innerJoin.ctr.maxAllocSize = 0
		innerJoin.ResetProjection(proc)
	} else {
		anal.Alloc(innerJoin.ctr.maxAllocSize)
		innerJoin.ctr.maxAllocSize = 0
	}
}

func (innerJoin *InnerJoin) Free(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &innerJoin.ctr

	ctr.cleanExecutor()
	ctr.cleanExprExecutor()
	ctr.cleanBatch(proc)

	if innerJoin.ProjectList != nil {
		innerJoin.FreeProjection(proc)
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
