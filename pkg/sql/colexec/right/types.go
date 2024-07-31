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

package right

import (
	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(RightJoin)

const (
	Build = iota
	Probe
	SendLast
	End
)

type evalVector struct {
	executor colexec.ExpressionExecutor
	vec      *vector.Vector
}

type container struct {
	colexec.ReceiverOperator

	state   int
	lastpos int

	batches       []*batch.Batch
	batchRowCount int64
	rbat          *batch.Batch
	buf           *batch.Batch

	expr colexec.ExpressionExecutor

	joinBat1 *batch.Batch
	cfs1     []func(*vector.Vector, *vector.Vector, int64, int) error

	joinBat2 *batch.Batch
	cfs2     []func(*vector.Vector, *vector.Vector, int64, int) error

	evecs []evalVector
	vecs  []*vector.Vector

	mp *process.JoinMap

	matched *bitmap.Bitmap

	handledLast bool

	maxAllocSize int64
}

type RightJoin struct {
	ctr        *container
	Result     []colexec.ResultPos
	LeftTypes  []types.Type
	RightTypes []types.Type
	Cond       *plan.Expr
	Conditions [][]*plan.Expr
	rbat       []*batch.Batch

	Channel chan *bitmap.Bitmap
	NumCPU  uint64

	HashOnPK           bool
	IsShuffle          bool
	ShuffleIdx         int32
	IsMerger           bool
	RuntimeFilterSpecs []*plan.RuntimeFilterSpec
	JoinMapTag         int32
	vm.OperatorBase
}

func (rightJoin *RightJoin) GetOperatorBase() *vm.OperatorBase {
	return &rightJoin.OperatorBase
}

func init() {
	reuse.CreatePool[RightJoin](
		func() *RightJoin {
			return &RightJoin{}
		},
		func(a *RightJoin) {
			*a = RightJoin{}
		},
		reuse.DefaultOptions[RightJoin]().
			WithEnableChecker(),
	)
}

func (rightJoin RightJoin) TypeName() string {
	return opName
}

func NewArgument() *RightJoin {
	return reuse.Alloc[RightJoin](nil)
}

func (rightJoin *RightJoin) Release() {
	if rightJoin != nil {
		reuse.Free[RightJoin](rightJoin, nil)
	}
}

func (rightJoin *RightJoin) Reset(proc *process.Process, pipelineFailed bool, err error) {
	rightJoin.Free(proc, pipelineFailed, err)
}

func (rightJoin *RightJoin) Free(proc *process.Process, pipelineFailed bool, err error) {
	ctr := rightJoin.ctr
	if ctr != nil {
		if !ctr.handledLast && rightJoin.NumCPU > 1 && !rightJoin.IsMerger {
			rightJoin.Channel <- nil
		}
		ctr.cleanBatch(proc)
		ctr.cleanHashMap()
		ctr.cleanExprExecutor()
		ctr.cleanEvalVectors()

		anal := proc.GetAnalyze(rightJoin.GetIdx(), rightJoin.GetParallelIdx(), rightJoin.GetParallelMajor())
		anal.Alloc(ctr.maxAllocSize)
		if rightJoin.ctr.buf != nil {
			proc.PutBatch(rightJoin.ctr.buf)
			rightJoin.ctr.buf = nil
		}
		rightJoin.ctr = nil
	}

}

func (ctr *container) cleanExprExecutor() {
	if ctr.expr != nil {
		ctr.expr.Free()
		ctr.expr = nil
	}
}

func (ctr *container) cleanBatch(proc *process.Process) {
	ctr.batches = nil
	if ctr.rbat != nil {
		proc.PutBatch(ctr.rbat)
		ctr.rbat = nil
	}
	if ctr.joinBat1 != nil {
		proc.PutBatch(ctr.joinBat1)
		ctr.joinBat1 = nil
	}
	if ctr.joinBat2 != nil {
		proc.PutBatch(ctr.joinBat2)
		ctr.joinBat2 = nil
	}
}

func (ctr *container) cleanHashMap() {
	if ctr.mp != nil {
		ctr.mp.Free()
		ctr.mp = nil
	}
}

func (ctr *container) cleanEvalVectors() {
	for i := range ctr.evecs {
		if ctr.evecs[i].executor != nil {
			ctr.evecs[i].executor.Free()
		}
		ctr.evecs[i].vec = nil
	}
	ctr.evecs = nil
}
