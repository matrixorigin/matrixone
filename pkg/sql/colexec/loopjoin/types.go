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
	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
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

var _ vm.Operator = new(LoopJoin)

const (
	Build = iota
	Probe
	Finalize
	End
)

const (
	loopJoinAllocationSiteMatched mpool.AllocationSite = iota + 92
	loopJoinAllocationSiteBatchOffsets
)

type container struct {
	state    int
	probeIdx int
	batIdx   int
	inBat    *batch.Batch
	resBat   *batch.Batch
	joinBat  *batch.Batch
	expr     colexec.ExpressionExecutor
	cfs      []func(*vector.Vector, *vector.Vector, int64, int) error
	mp       *message.JoinMap

	// FULL OUTER JOIN bookkeeping. rightRowsMatched is a flat bitmap over
	// all build rows; bit i is set when build row i matched at least one
	// probe row. rightBatchOffset[k] is the flat index of build batch k's
	// first row. After probe drains, Negate() makes set bits = unmatched
	// build rows; finalize() walks the iterator to emit them.
	rightRowsMatched *bitmap.Bitmap
	rightBatchOffset []uint64
	rightMatchedIter bitmap.Iterator
	rightMatchedBat  int // monotonically advancing batIdx during finalize
}

type LoopJoin struct {
	ctr               container
	LeftTypes         []types.Type
	RightTypes        []types.Type
	NonEqCond         *plan.Expr
	ResultCols        []colexec.ResultPos
	JoinMapTag        int32
	JoinType          plan.Node_JoinType
	MarkPos           int
	allocationAccount *mpool.AllocationAccount

	vm.OperatorBase
}

func (loopJoin *LoopJoin) SetAllocationAccount(
	account *mpool.AllocationAccount,
) error {
	if account == nil || account.Handle() == 0 {
		return mpool.ErrAllocationAccountInvalid
	}
	if loopJoin.allocationAccount != nil &&
		loopJoin.allocationAccount != account {
		return mpool.ErrAllocationAccountMismatch
	}
	if loopJoin.allocationAccount == account {
		return nil
	}
	loopJoin.allocationAccount = account
	return nil
}

func (loopJoin *LoopJoin) ClearAllocationAccount(
	account *mpool.AllocationAccount,
) error {
	if loopJoin.allocationAccount == nil {
		return nil
	}
	if loopJoin.allocationAccount != account {
		return mpool.ErrAllocationAccountMismatch
	}
	ctr := &loopJoin.ctr
	if ctr.mp != nil || ctr.expr != nil || ctr.rightRowsMatched != nil ||
		len(ctr.rightBatchOffset) != 0 {
		return mpool.ErrAllocationAccountInvariant
	}
	loopJoin.allocationAccount = nil
	return nil
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

	// The executor owns allocations from this execution generation. Prepared
	// statements must rebuild it after the next account is installed instead
	// of carrying generation-bound storage across Reset.
	ctr.cleanNonEqCondExecutor()
	ctr.cleanHashMap()
	ctr.state = Build
	ctr.inBat = nil
	ctr.cleanRightMatchState(proc)
	ctr.rightMatchedIter = nil
	ctr.rightMatchedBat = 0
}

func (loopJoin *LoopJoin) Free(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &loopJoin.ctr

	ctr.cleanBatch(proc.Mp())
	ctr.cleanNonEqCondExecutor()
	ctr.cleanRightMatchState(proc)

}

func (ctr *container) cleanRightMatchState(proc *process.Process) {
	colexec.FreeAccountedBitmap(ctr.rightRowsMatched, proc.Mp())
	ctr.rightRowsMatched = nil
	if cap(ctr.rightBatchOffset) > 0 {
		mpool.FreeSlice(proc.Mp(), ctr.rightBatchOffset)
	}
	ctr.rightBatchOffset = nil
	ctr.rightMatchedIter = nil
	ctr.rightMatchedBat = 0
}

func (loopJoin *LoopJoin) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	return input, nil
}

func (ctr *container) cleanBatch(mp *mpool.MPool) {
	if ctr.resBat != nil {
		ctr.resBat.Clean(mp)
		ctr.resBat = nil
	}
	if ctr.joinBat != nil {
		ctr.joinBat.Clean(mp)
		ctr.joinBat = nil
	}
}

func (ctr *container) cleanNonEqCondExecutor() {
	if ctr.expr != nil {
		ctr.expr.Free()
		ctr.expr = nil
	}
	ctr.expr = nil
}

func (ctr *container) cleanHashMap() {
	if ctr.mp != nil {
		ctr.mp.Free()
		ctr.mp = nil
	}
}
