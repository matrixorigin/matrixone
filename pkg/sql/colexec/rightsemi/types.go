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

package rightsemi

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

var _ vm.Operator = new(RightSemi)

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

	tmpBatches []*batch.Batch // for reuse

	buf []*batch.Batch

	maxAllocSize int64
}

type RightSemi struct {
	ctr        *container
	Result     []int32
	RightTypes []types.Type
	Cond       *plan.Expr
	Conditions [][]*plan.Expr

	Channel chan *bitmap.Bitmap
	NumCPU  uint64

	IsMerger           bool
	HashOnPK           bool
	IsShuffle          bool
	ShuffleIdx         int32
	RuntimeFilterSpecs []*plan.RuntimeFilterSpec
	JoinMapTag         int32
	vm.OperatorBase
}

func (rightSemi *RightSemi) GetOperatorBase() *vm.OperatorBase {
	return &rightSemi.OperatorBase
}

func init() {
	reuse.CreatePool[RightSemi](
		func() *RightSemi {
			return &RightSemi{}
		},
		func(a *RightSemi) {
			*a = RightSemi{}
		},
		reuse.DefaultOptions[RightSemi]().
			WithEnableChecker(),
	)
}

func (rightSemi RightSemi) TypeName() string {
	return opName
}

func NewArgument() *RightSemi {
	return reuse.Alloc[RightSemi](nil)
}

func (rightSemi *RightSemi) Release() {
	if rightSemi != nil {
		reuse.Free[RightSemi](rightSemi, nil)
	}
}

func (rightSemi *RightSemi) Reset(proc *process.Process, pipelineFailed bool, err error) {
	rightSemi.Free(proc, pipelineFailed, err)
}

func (rightSemi *RightSemi) Free(proc *process.Process, pipelineFailed bool, err error) {
	ctr := rightSemi.ctr
	if ctr != nil {
		if !ctr.handledLast && rightSemi.NumCPU > 1 && !rightSemi.IsMerger {
			rightSemi.Channel <- nil
		}
		ctr.cleanBatch(proc)
		ctr.cleanEvalVectors()
		ctr.cleanHashMap()
		ctr.cleanExprExecutor()

		ctr.tmpBatches = nil

		anal := proc.GetAnalyze(rightSemi.GetIdx(), rightSemi.GetParallelIdx(), rightSemi.GetParallelMajor())
		anal.Alloc(ctr.maxAllocSize)

		rightSemi.ctr = nil
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
