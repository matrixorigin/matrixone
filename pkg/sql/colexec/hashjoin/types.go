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

type probeState int

const (
	psNextBatch probeState = iota
	psSelsForOneRow
	psBatchRow
)

type finalizeState int

const (
	fsSyncMatched finalizeState = iota
	fsSendBatches
)

type container struct {
	state       int
	itr         hashmap.Iterator
	rightBats   []*batch.Batch
	rightRowCnt int64

	leftBat *batch.Batch
	resBat  *batch.Batch

	lastIdx int
	// process idx for zvs and vs, which returned by hashmap.Iterator.Find()
	// guarantee: vs[ctr.vsidx] is the result of inbat[ctr.lastRow]
	vsidx int
	zvs   []int64
	vs    []uint64
	sels  []int32

	leftRowMatched bool

	probeState probeState

	nonEqCondExec colexec.ExpressionExecutor

	joinBats []*batch.Batch
	cfs1     []func(*vector.Vector, *vector.Vector, int64, int) error
	cfs2     []func(*vector.Vector, *vector.Vector, int64, int) error

	eqCondExecs []colexec.ExpressionExecutor
	eqCondVecs  []*vector.Vector

	mp *message.JoinMap

	rightRowsMatched   *bitmap.Bitmap
	rightJoinFinalized bool

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
	CanSkipProbe       bool
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
	if !ctr.rightJoinFinalized && hashJoin.NumCPU > 1 && !hashJoin.IsMerger {
		hashJoin.Channel <- nil
	}
	ctr.cleanHashMap()
	ctr.resetNonEqCondExecutor()
	ctr.resetEqCondExecutors()
	ctr.rightRowsMatched = nil
	ctr.rightJoinFinalized = false
	ctr.state = Build
	ctr.lastIdx = 0

	if hashJoin.OpAnalyzer != nil {
		hashJoin.OpAnalyzer.Alloc(ctr.maxAllocSize)
	}
	hashJoin.ctr.leftBat = nil
	ctr.maxAllocSize = 0
}

func (hashJoin *HashJoin) Free(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &hashJoin.ctr
	ctr.cleanBatch(proc)
	ctr.cleanHashMap()
	ctr.cleanNonEqCondExecutor()
	ctr.cleanEqCondExecutors()
}

func (ctr *container) resetNonEqCondExecutor() {
	if ctr.nonEqCondExec != nil {
		ctr.nonEqCondExec.ResetForNextQuery()
	}
}

func (ctr *container) cleanNonEqCondExecutor() {
	if ctr.nonEqCondExec != nil {
		ctr.nonEqCondExec.Free()
		ctr.nonEqCondExec = nil
	}
}

func (ctr *container) cleanBatch(proc *process.Process) {
	ctr.rightBats = nil
	if ctr.resBat != nil {
		ctr.resBat.Clean(proc.GetMPool())
		ctr.resBat = nil
	}
	for i := range ctr.joinBats {
		if ctr.joinBats[i] != nil {
			ctr.joinBats[i].Clean(proc.Mp())
			ctr.joinBats[i] = nil
		}
	}
}

func (ctr *container) cleanHashMap() {
	if ctr.mp != nil {
		ctr.mp.Free()
		ctr.mp = nil
	}
}

func (ctr *container) cleanEqCondExecutors() {
	for i := range ctr.eqCondExecs {
		if ctr.eqCondExecs[i] != nil {
			ctr.eqCondExecs[i].Free()
		}
	}
	ctr.eqCondExecs = nil
}

func (ctr *container) resetEqCondExecutors() {
	for i := range ctr.eqCondExecs {
		if ctr.eqCondExecs[i] != nil {
			ctr.eqCondExecs[i].ResetForNextQuery()
		}
	}
}

func (hashJoin *HashJoin) IsInner() bool {
	return hashJoin.JoinType == plan.Node_INNER
}

func (hashJoin *HashJoin) IsLeftOuter() bool {
	return hashJoin.JoinType == plan.Node_LEFT
}

func (hashJoin *HashJoin) IsRightOuter() bool {
	return hashJoin.JoinType == plan.Node_RIGHT
}

func (hashJoin *HashJoin) IsFullOuter() bool {
	return hashJoin.JoinType == plan.Node_OUTER
}

func (hashJoin *HashJoin) IsSemi() bool {
	return hashJoin.JoinType == plan.Node_SEMI
}

func (hashJoin *HashJoin) IsLeftSemi() bool {
	return !hashJoin.IsRightJoin && hashJoin.JoinType == plan.Node_SEMI
}

func (hashJoin *HashJoin) IsRightSemi() bool {
	return hashJoin.IsRightJoin && hashJoin.JoinType == plan.Node_SEMI
}

func (hashJoin *HashJoin) IsAnti() bool {
	return hashJoin.JoinType == plan.Node_ANTI
}

func (hashJoin *HashJoin) IsLeftAnti() bool {
	return !hashJoin.IsRightJoin && hashJoin.JoinType == plan.Node_ANTI
}

func (hashJoin *HashJoin) IsRightAnti() bool {
	return hashJoin.IsRightJoin && hashJoin.JoinType == plan.Node_ANTI
}

func (hashJoin *HashJoin) IsSingle() bool {
	return hashJoin.JoinType == plan.Node_SINGLE
}

func (hashJoin *HashJoin) IsLeftSingle() bool {
	return !hashJoin.IsRightJoin && hashJoin.JoinType == plan.Node_SINGLE
}

func (hashJoin *HashJoin) IsRightSingle() bool {
	return hashJoin.IsRightJoin && hashJoin.JoinType == plan.Node_SINGLE
}
