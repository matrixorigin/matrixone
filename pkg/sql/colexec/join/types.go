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

type evalVector struct {
	executor colexec.ExpressionExecutor
	vec      *vector.Vector
}

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

	evecs []evalVector
	vecs  []*vector.Vector

	mp  *message.JoinMap
	bat *batch.Batch

	maxAllocSize int64
}

type InnerJoin struct {
	ctr        *container
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
	innerJoin.Free(proc, pipelineFailed, err)
}

func (innerJoin *InnerJoin) Free(proc *process.Process, pipelineFailed bool, err error) {
	ctr := innerJoin.ctr
	anal := proc.GetAnalyze(innerJoin.GetIdx(), innerJoin.GetParallelIdx(), innerJoin.GetParallelMajor())
	if ctr != nil {
		ctr.cleanBatch(proc)
		ctr.cleanEvalVectors()
		ctr.cleanHashMap()
		ctr.cleanExprExecutor()

		anal.Alloc(ctr.maxAllocSize)

		if innerJoin.ctr.bat != nil {
			innerJoin.ctr.bat = nil
		}
		innerJoin.ctr.lastrow = 0
		innerJoin.ctr = nil
	}
	if innerJoin.ProjectList != nil {
		anal.Alloc(innerJoin.ProjectAllocSize)
		innerJoin.FreeProjection(proc)
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
