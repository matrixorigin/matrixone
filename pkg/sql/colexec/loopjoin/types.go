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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
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

const (
	loopJoinAllocationSiteResultData mpool.AllocationSite = iota + 106
	loopJoinAllocationSiteResultArea
	loopJoinAllocationSiteResultNulls
	loopJoinAllocationSiteResultGrouping
)

const (
	loopJoinAllocationSiteConditionData mpool.AllocationSite = iota + 118
	loopJoinAllocationSiteConditionArea
	loopJoinAllocationSiteConditionNulls
	loopJoinAllocationSiteConditionGrouping
)

const defaultLoopJoinResultBatchBytes = 64 * mpool.MB

const (
	// A non-equi condition returns a boolean vector. Cap it at one million rows
	// and also budget copied null/grouping bitmaps by bytes for unusually wide
	// batches. Ordinary batches stay on the original single-Eval fast path.
	loopJoinConditionMaxWindowRows   = 1 << 20
	loopJoinConditionWindowByteLimit = 8 * mpool.MB
)

type container struct {
	state    int
	probeIdx int
	batIdx   int
	// batRowIdx is the next row to consume from batIdx. A JoinMap batch is not
	// required to fit in DefaultBatchSize, so loop join must be able to yield
	// in the middle of one instead of materializing the whole batch at once.
	batRowIdx int
	// resultBatchByteLimit complements DefaultBatchSize for wide rows. Prepare
	// installs the default unless the execution already has a tighter limit.
	resultBatchByteLimit int
	// probeMatched is part of the resume cursor. It cannot be inferred from the
	// batch/row cursor: a condition may have consumed an arbitrary number of
	// false rows before a result batch fills.
	probeMatched bool
	// condVec is the read-only result for exactly one
	// (probeIdx, batIdx, [condStart,condEnd)) window. Expression results stay
	// valid until the next Eval call, so retaining this pointer prevents a split
	// result batch from evaluating the same condition window again.
	condVec      *vector.Vector
	condProbeIdx int
	condBatIdx   int
	condStart    int
	condEnd      int
	// condWindow owns only its range bitmaps; vector data/area borrow the build
	// batch. It must live as long as condVec because a column expression may
	// return one of the window vectors directly.
	condWindow   *batch.Batch
	condWindowMP *mpool.MPool
	// Bounded-cardinality joins scan the build before emitting their one result
	// row. Keep that completed scan across a byte-limit yield so admission never
	// causes the same condition to be evaluated again.
	probeScanValid      bool
	probeScanIdx        int
	probeScanMatches    int
	probeScanFirstBatch int
	probeScanFirstRow   int
	probeScanHasNull    bool
	// FULL OUTER finalization consumes an iterator. If the next right row does
	// not fit a non-empty result batch, keep it here rather than losing it.
	rightPendingRow bool
	rightPending    uint64
	inBat           *batch.Batch
	resBat          *batch.Batch
	joinBat         *batch.Batch
	expr            colexec.ExpressionExecutor
	cfs             []func(*vector.Vector, *vector.Vector, int64, int) error
	mp              *message.JoinMap

	// FULL OUTER JOIN bookkeeping. rightRowsMatched is a flat bitmap over
	// all build rows; bit i is set when build row i matched at least one
	// probe row. rightBatchOffset[k] is the flat index of build batch k's
	// first row. After probe drains, Negate() makes set bits = unmatched
	// build rows; finalize() walks the iterator to emit them.
	rightRowsMatched *bitmap.Bitmap
	rightBatchOffset []uint64
	rightMatchedIter bitmap.Iterator
	rightMatchedBat  int // monotonically advancing batIdx during finalize

	// Byte-admission diagnostics are deliberately kept local to an operator
	// execution.  Sampling avoids putting a clock read on every output row,
	// while the counters make an EXPLAIN ANALYZE run identify whether the byte
	// bound is material on a production-sized input.
	byteAdmissionRowChecks       int64
	byteAdmissionSizeChecks      int64
	byteAdmissionRejectedRows    int64
	byteAdmissionEstimatedBytes  int64
	byteAdmissionRowSampleNanos  int64
	byteAdmissionSizeSampleNanos int64
	byteAdmissionRowSamples      int64
	byteAdmissionSizeSamples     int64
	byteAdmissionStatsFlushed    bool
}

type LoopJoin struct {
	ctr                 container
	LeftTypes           []types.Type
	RightTypes          []types.Type
	NonEqCond           *plan.Expr
	ResultCols          []colexec.ResultPos
	JoinMapTag          int32
	JoinType            plan.Node_JoinType
	MarkPos             int
	allocationAccount   *mpool.AllocationAccount
	resultAllocation    *vector.AllocationAccountSelection
	conditionAllocation *vector.AllocationAccountSelection
	// recursiveProbe is derived from the operator tree during Prepare. An empty
	// INNER/SEMI build must still drain a recursive probe until its round marker.
	recursiveProbe bool

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
	selection, err := vector.NewAllocationAccountSelection(
		account,
		hashbuild.HashBuildAllocationOwner,
		loopJoinAllocationSiteResultData,
		loopJoinAllocationSiteResultArea,
		loopJoinAllocationSiteResultNulls,
		loopJoinAllocationSiteResultGrouping,
	)
	if err != nil {
		return err
	}
	conditionSelection, err := vector.NewAllocationAccountSelection(
		account,
		hashbuild.HashBuildAllocationOwner,
		loopJoinAllocationSiteConditionData,
		loopJoinAllocationSiteConditionArea,
		loopJoinAllocationSiteConditionNulls,
		loopJoinAllocationSiteConditionGrouping,
	)
	if err != nil {
		return err
	}
	loopJoin.allocationAccount = account
	loopJoin.resultAllocation = selection
	loopJoin.conditionAllocation = conditionSelection
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
		len(ctr.rightBatchOffset) != 0 || ctr.resBat != nil ||
		ctr.condWindow != nil {
		return mpool.ErrAllocationAccountInvariant
	}
	loopJoin.allocationAccount = nil
	loopJoin.resultAllocation = nil
	loopJoin.conditionAllocation = nil
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

	ctr.clearProbeResume()
	// The executor owns allocations from this execution generation. Prepared
	// statements must rebuild it after the next account is installed instead
	// of carrying generation-bound storage across Reset.
	ctr.cleanNonEqCondExecutor()
	ctr.cleanHashMap()
	if ctr.resBat != nil {
		ctr.resBat.Clean(proc.GetMPool())
		ctr.resBat = nil
	}
	ctr.state = Build
	ctr.inBat = nil
	ctr.probeIdx = 0
	ctr.batIdx = 0
	ctr.batRowIdx = 0
	ctr.cleanRightMatchState(proc)
	ctr.rightMatchedIter = nil
	ctr.rightMatchedBat = 0
	ctr.byteAdmissionRowChecks = 0
	ctr.byteAdmissionSizeChecks = 0
	ctr.byteAdmissionRejectedRows = 0
	ctr.byteAdmissionEstimatedBytes = 0
	ctr.byteAdmissionRowSampleNanos = 0
	ctr.byteAdmissionSizeSampleNanos = 0
	ctr.byteAdmissionRowSamples = 0
	ctr.byteAdmissionSizeSamples = 0
	ctr.byteAdmissionStatsFlushed = false
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
	ctr.rightPendingRow = false
	ctr.rightPending = 0
}

func (ctr *container) clearConditionResult() {
	if ctr.condWindow != nil {
		ctr.condWindow.Clean(ctr.condWindowMP)
	}
	ctr.condVec = nil
	ctr.condProbeIdx = 0
	ctr.condBatIdx = 0
	ctr.condStart = 0
	ctr.condEnd = 0
	ctr.condWindow = nil
	ctr.condWindowMP = nil
}

func (ctr *container) clearProbeResume() {
	ctr.probeMatched = false
	ctr.clearConditionResult()
	ctr.probeScanValid = false
	ctr.probeScanIdx = 0
	ctr.probeScanMatches = 0
	ctr.probeScanFirstBatch = 0
	ctr.probeScanFirstRow = 0
	ctr.probeScanHasNull = false
}

func (loopJoin *LoopJoin) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	return input, nil
}

func (ctr *container) cleanBatch(mp *mpool.MPool) {
	ctr.clearProbeResume()
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
