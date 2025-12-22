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

package hashjoin

import (
	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
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

var _ vm.Operator = new(HashJoin)

const (
	Build = iota
	Probe
	Finalize
	End
)

type evalVector struct {
	executor colexec.ExpressionExecutor
	vec      *vector.Vector
}

type container struct {
	state   int
	itr     hashmap.Iterator
	lastPos int

	batches       []*batch.Batch
	batchRowCount int64
	inbat         *batch.Batch
	rbat          *batch.Batch

	lastRow int

	expr colexec.ExpressionExecutor

	joinBat1 *batch.Batch
	cfs1     []func(*vector.Vector, *vector.Vector, int64, int) error

	joinBat2 *batch.Batch
	cfs2     []func(*vector.Vector, *vector.Vector, int64, int) error

	evecs []evalVector
	vecs  []*vector.Vector

	mp *message.JoinMap

	matched *bitmap.Bitmap

	handledLast bool

	maxAllocSize int64
}

type HashJoin struct {
	ctr container

	JoinType    plan.Node_JoinType
	IsRightJoin bool

	ResultCols []colexec.ResultPos
	LeftTypes  []types.Type
	RightTypes []types.Type
	NonEqCond  *plan.Expr
	EqConds    [][]*plan.Expr

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

func (hashJoin *HashJoin) GetOperatorBase() *vm.OperatorBase {
	return &hashJoin.OperatorBase
}

func init() {
	reuse.CreatePool[HashJoin](
		func() *HashJoin {
			return &HashJoin{}
		},
		func(a *HashJoin) {
			*a = HashJoin{}
		},
		reuse.DefaultOptions[HashJoin]().
			WithEnableChecker(),
	)
}

func (hashJoin HashJoin) TypeName() string {
	return opName
}

func NewArgument() *HashJoin {
	return reuse.Alloc[HashJoin](nil)
}

func (hashJoin *HashJoin) Release() {
	if hashJoin != nil {
		reuse.Free[HashJoin](hashJoin, nil)
	}
}

func (hashJoin *HashJoin) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	return input, nil
}

func (hashJoin *HashJoin) Reset(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &hashJoin.ctr
	ctr.itr = nil
	if !ctr.handledLast && hashJoin.NumCPU > 1 && !hashJoin.IsMerger {
		hashJoin.Channel <- nil
	}
	ctr.cleanHashMap()
	ctr.resetExprExecutor()
	ctr.resetEvalVectors()
	ctr.matched = nil
	ctr.handledLast = false
	ctr.state = Build
	ctr.lastPos = 0

	if hashJoin.OpAnalyzer != nil {
		hashJoin.OpAnalyzer.Alloc(ctr.maxAllocSize)
	}
	hashJoin.ctr.inbat = nil
	ctr.maxAllocSize = 0
}

func (hashJoin *HashJoin) Free(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &hashJoin.ctr
	ctr.cleanBatch(proc)
	ctr.cleanHashMap()
	ctr.cleanExprExecutor()
	ctr.cleanEvalVectors()
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
	ctr.batches = nil
	if ctr.rbat != nil {
		ctr.rbat.Clean(proc.GetMPool())
		ctr.rbat = nil
	}
	if ctr.joinBat1 != nil {
		ctr.joinBat1.Clean(proc.GetMPool())
		ctr.joinBat1 = nil
	}
	if ctr.joinBat2 != nil {
		ctr.joinBat2.Clean(proc.GetMPool())
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

func (ctr *container) resetEvalVectors() {
	for i := range ctr.evecs {
		if ctr.evecs[i].executor != nil {
			ctr.evecs[i].executor.ResetForNextQuery()
		}
	}
}
