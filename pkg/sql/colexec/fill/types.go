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

package fill

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/spillutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(Fill)

var _ interface {
	SetAllocationAccount(*mpool.AllocationAccount) error
	ClearAllocationAccount(*mpool.AllocationAccount) error
} = new(Fill)

const (
	// Sites 32-43 and 60 are shared spillutil sites. Fill-specific storage uses
	// a disjoint range under the Fill owner.
	fillAllocationSiteRetainedData mpool.AllocationSite = iota + 64
	fillAllocationSiteRetainedArea
	fillAllocationSiteRetainedNulls
	fillAllocationSiteRetainedGrouping
	fillAllocationSiteExpressionData
	fillAllocationSiteExpressionArea
	fillAllocationSiteExpressionNulls
	fillAllocationSiteExpressionGrouping
	fillAllocationSiteOutputData
	fillAllocationSiteOutputArea
	fillAllocationSiteOutputNulls
	fillAllocationSiteOutputGrouping
	fillAllocationSiteCoordinates
	fillAllocationSitePartitionSnapshot
	fillAllocationSiteSpillWriteBuffer
)

// fillCoord addresses one buffered row by the batch's absolute sequence number
// (stable across FIFO popping) and its row within that batch.
type fillCoord struct {
	seq int
	row int
}

type container struct {

	// value
	valVecs []*vector.Vector

	// prev
	prevVecs []*vector.Vector
	// prevValid marks which prevVecs hold a value from the current partition.
	// A partition boundary invalidates them without freeing the vectors.
	prevValid []bool
	// prevPart snapshots the partition key of the last row of
	// the previous batch, so the first row of the next batch can detect a
	// boundary without keeping the old batch alive.
	prevPart spillPartitionSnapshot

	// next / linear incremental engine. bats is a FIFO of still-pending child
	// batches; baseSeq is the absolute sequence number of bats[0], so a
	// fillCoord captured as an absolute seq stays valid after the FIFO pops its
	// head (local index = seq - baseSeq). toFree holds the batch handed to the
	// caller on the previous Call, released at the top of the next one.
	// flushable counts the resolved prefix of bats that may be emitted;
	// childDone records child EOF. bats is structurally capped at
	// maxFillPendingBatches; an unresolved suffix spills before another child
	// batch can be retained.
	bats      []*batch.Batch
	baseSeq   int
	toFree    *batch.Batch
	flushable int
	childDone bool
	// pendingBytes accounts for duplicated batches retained in bats. Once it
	// crosses spillThreshold while no prefix is flushable, spill owns the
	// unresolved suffix and keeps only one batch resident at a time.
	pendingBytes   int64
	pendingRows    int64
	spillThreshold int64
	spill          *fillSpill

	allocationAccount    *mpool.AllocationAccount
	retainedAllocation   *vector.AllocationAccountSelection
	expressionAllocation *vector.AllocationAccountSelection
	outputAllocation     *vector.AllocationAccountSelection
	spillAllocation      *spillutil.SpillAllocationAccount
	budget               *process.ExecutionResourceGeneration
	// next: per fill-column list of NULL rows still waiting for a following
	// value of the same partition.
	nextRun [][]fillCoord
	// linear: linPre is the last non-NULL row per column (seq < 0 means none in
	// the current partition), linRun the NULL run waiting to be interpolated
	// between linPre and the next non-NULL.
	linPre []fillCoord
	linRun [][]fillCoord
	// linSeed carries the last original non-NULL value across a completed spill
	// segment without pinning the segment's final output batch in memory.
	linSeed      []*vector.Vector
	linSeedValid []bool
	// linEntry is the endpoint immediately before bats[0]. Unlike linSeed,
	// which follows the currently consumed partition and may be cleared when a
	// right endpoint arrives, linEntry advances only when a resolved batch is
	// emitted. A spill therefore always starts with the endpoint that belongs
	// to the beginning of its persisted suffix.
	linEntry      []*vector.Vector
	linEntryValid []bool
	linEntryPart  spillPartitionSnapshot

	buf *batch.Batch

	// linear
	exes []colexec.ExpressionExecutor

	process func(ctr *container, ap *Fill, proc *process.Process, anal process.Analyzer) (vm.CallResult, error)
}

type Fill struct {
	ctr container

	ColLen   int
	FillType plan.Node_FillType
	FillVal  []*plan.Expr
	// SpillThreshold follows the shared colexec convention: zero selects the
	// CN-local default, small positive values are row-oriented test thresholds,
	// and larger values are bytes.
	SpillThreshold int64
	// PartitionColIdx locates the time window's partition keys inside the
	// input batch. fill(prev/next/linear) treats a change in these columns as
	// a hard boundary: values never cross it in either direction.
	PartitionColIdx []int32

	vm.OperatorBase
	colexec.Projection
}

func (fill *Fill) GetOperatorBase() *vm.OperatorBase {
	return &fill.OperatorBase
}

func init() {
	reuse.CreatePool[Fill](
		func() *Fill {
			return &Fill{}
		},
		func(a *Fill) {
			*a = Fill{}
		},
		reuse.DefaultOptions[Fill]().
			WithEnableChecker(),
	)
}

func (fill Fill) TypeName() string {
	return opName
}

func NewArgument() *Fill {
	return reuse.Alloc[Fill](nil)
}

func (fill *Fill) Release() {
	if fill != nil {
		reuse.Free[Fill](fill, nil)
	}
}

func (fill *Fill) SetAllocationAccount(account *mpool.AllocationAccount) error {
	if fill == nil || account == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	if fill.ctr.allocationAccount != nil {
		if fill.ctr.allocationAccount == account {
			return nil
		}
		return mpool.ErrAllocationAccountMismatch
	}
	if fill.HasPreparedProjection() {
		return mpool.ErrAllocationAccountInvariant
	}
	return fill.ctr.setAllocationAccount(account)
}

func (fill *Fill) ClearAllocationAccount(account *mpool.AllocationAccount) error {
	if fill == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	if fill.ctr.allocationAccount == nil {
		return nil
	}
	if fill.ctr.allocationAccount != account {
		return mpool.ErrAllocationAccountMismatch
	}
	if fill.HasPreparedProjection() {
		return mpool.ErrAllocationAccountInvariant
	}
	return fill.ctr.clearAllocationAccount(account)
}

func (ctr *container) setAllocationAccount(account *mpool.AllocationAccount) error {
	if ctr == nil || account == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	if ctr.allocationAccount != nil {
		if ctr.allocationAccount == account {
			return nil
		}
		return mpool.ErrAllocationAccountMismatch
	}
	if ctr.buf != nil || ctr.spill != nil || ctr.toFree != nil ||
		len(ctr.bats) != 0 || len(ctr.exes) != 0 || len(ctr.valVecs) != 0 ||
		len(ctr.prevVecs) != 0 ||
		len(ctr.linSeed) != 0 || len(ctr.linEntry) != 0 {
		return mpool.ErrAllocationAccountInvariant
	}
	retained, err := vector.NewAllocationAccountSelection(
		account,
		mpool.AllocationOwnerFill,
		fillAllocationSiteRetainedData,
		fillAllocationSiteRetainedArea,
		fillAllocationSiteRetainedNulls,
		fillAllocationSiteRetainedGrouping,
	)
	if err != nil {
		return err
	}
	expression, err := vector.NewAllocationAccountSelection(
		account,
		mpool.AllocationOwnerFill,
		fillAllocationSiteExpressionData,
		fillAllocationSiteExpressionArea,
		fillAllocationSiteExpressionNulls,
		fillAllocationSiteExpressionGrouping,
	)
	if err != nil {
		return err
	}
	output, err := vector.NewAllocationAccountSelection(
		account,
		mpool.AllocationOwnerFill,
		fillAllocationSiteOutputData,
		fillAllocationSiteOutputArea,
		fillAllocationSiteOutputNulls,
		fillAllocationSiteOutputGrouping,
	)
	if err != nil {
		return err
	}
	spill, err := spillutil.NewSpillAllocationAccount(
		account,
		mpool.AllocationOwnerFill,
	)
	if err != nil {
		return err
	}
	ctr.allocationAccount = account
	ctr.retainedAllocation = retained
	ctr.expressionAllocation = expression
	ctr.outputAllocation = output
	ctr.spillAllocation = spill
	return nil
}

func (ctr *container) clearAllocationAccount(account *mpool.AllocationAccount) error {
	if ctr == nil || ctr.allocationAccount == nil {
		return nil
	}
	if ctr.allocationAccount != account {
		return mpool.ErrAllocationAccountMismatch
	}
	if ctr.buf != nil || ctr.spill != nil || ctr.toFree != nil ||
		len(ctr.bats) != 0 || len(ctr.exes) != 0 || len(ctr.valVecs) != 0 ||
		len(ctr.prevVecs) != 0 ||
		ctr.hasCoordinateCapacity() || ctr.prevPart.hasCapacity() ||
		ctr.linEntryPart.hasCapacity() || ctr.budget != nil {
		return mpool.ErrAllocationAccountInvariant
	}
	ctr.prevPart.free()
	ctr.linEntryPart.free()
	ctr.allocationAccount = nil
	ctr.retainedAllocation = nil
	ctr.expressionAllocation = nil
	ctr.outputAllocation = nil
	ctr.spillAllocation = nil
	return nil
}

func (ctr *container) hasCoordinateCapacity() bool {
	for i := range ctr.nextRun {
		if cap(ctr.nextRun[i]) != 0 {
			return true
		}
	}
	for i := range ctr.linRun {
		if cap(ctr.linRun[i]) != 0 {
			return true
		}
	}
	return false
}

func (fill *Fill) Reset(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &fill.ctr
	ctr.cleanupSpill(proc)
	if ctr.allocationAccount != nil {
		ctr.freeVectors(proc.Mp())
	} else {
		ctr.clearLinearSeeds(proc.Mp())
		ctr.clearLinearEntries(proc.Mp())
	}
	ctr.resetCtrParma(proc.Mp())
	if ctr.allocationAccount != nil {
		ctr.freeExes()
		ctr.valVecs = nil
	} else {
		ctr.resetExes()
	}
	if ctr.buf != nil {
		if ctr.buf.HasAllocationAccount() {
			ctr.buf.Clean(proc.Mp())
			ctr.buf = nil
		} else {
			ctr.buf.CleanOnlyData()
		}
	}
	for _, b := range ctr.bats {
		if b != nil {
			b.Clean(proc.GetMPool())
		}
	}
	clear(ctr.bats)
	if ctr.allocationAccount != nil {
		ctr.bats = nil
	} else {
		ctr.bats = ctr.bats[:0]
	}
	// toFree was popped out of bats, so the loop above does not cover it.
	if ctr.toFree != nil {
		ctr.toFree.Clean(proc.GetMPool())
		ctr.toFree = nil
	}

	if fill.ProjectList != nil {
		if ctr.allocationAccount != nil {
			fill.FreeProjection(proc)
		} else {
			if fill.OpAnalyzer != nil {
				fill.OpAnalyzer.Alloc(fill.ProjectAllocSize)
			}
			fill.ResetProjection(proc)
		}
	}
	ctr.budget = nil
}

func (fill *Fill) Free(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &fill.ctr
	ctr.cleanupSpill(proc)
	ctr.freeBatch(proc.Mp())
	ctr.freeExes()
	ctr.valVecs = nil
	ctr.freeVectors(proc.Mp())
	ctr.freeCoordRuns(proc.Mp())
	ctr.prevPart.free()
	ctr.linEntryPart.free()
	ctr.budget = nil

	fill.FreeProjection(proc)
}

func (fill *Fill) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	batch := input
	var err error
	if fill.ProjectList != nil {
		batch, err = fill.EvalProjection(input, proc)
	}
	return batch, err
}

func (ctr *container) freeBatch(mp *mpool.MPool) {
	for _, b := range ctr.bats {
		if b != nil {
			b.Clean(mp)
		}
	}
	ctr.bats = nil
	if ctr.toFree != nil {
		ctr.toFree.Clean(mp)
		ctr.toFree = nil
	}
	if ctr.buf != nil {
		ctr.buf.Clean(mp)
		ctr.buf = nil
	}
}

func (ctr *container) freeVectors(mp *mpool.MPool) {
	for _, vec := range ctr.prevVecs {
		if vec != nil {
			vec.Free(mp)
		}
	}
	ctr.prevVecs = nil
	ctr.prevValid = nil
	ctr.clearLinearSeeds(mp)
	ctr.clearLinearEntries(mp)
	ctr.linSeed = nil
	ctr.linSeedValid = nil
	ctr.linEntry = nil
	ctr.linEntryValid = nil
}

func (ctr *container) clearLinearSeeds(mp *mpool.MPool) {
	for i, vec := range ctr.linSeed {
		if vec != nil {
			vec.Free(mp)
			ctr.linSeed[i] = nil
		}
	}
	for i := range ctr.linSeedValid {
		ctr.linSeedValid[i] = false
	}
}

func (ctr *container) clearLinearEntries(mp *mpool.MPool) {
	for i, vec := range ctr.linEntry {
		if vec != nil {
			vec.Free(mp)
			ctr.linEntry[i] = nil
		}
	}
	for i := range ctr.linEntryValid {
		ctr.linEntryValid[i] = false
	}
	ctr.linEntryPart.free()
}

func (ctr *container) freeExes() {
	for i := range ctr.exes {
		if ctr.exes[i] != nil {
			ctr.exes[i].Free()
		}
	}
	ctr.exes = nil
}

func (ctr *container) resetExes() {
	for i := range ctr.exes {
		if ctr.exes[i] != nil {
			ctr.exes[i].ResetForNextQuery()
		}
	}
}

func (ctr *container) resetCtrParma(mp *mpool.MPool) {
	ctr.baseSeq = 0
	ctr.flushable = 0
	ctr.childDone = false
	ctr.pendingBytes = 0
	ctr.pendingRows = 0
	for i := range ctr.prevValid {
		ctr.prevValid[i] = false
	}
	ctr.prevPart.free()
	if ctr.allocationAccount != nil {
		ctr.freeCoordRuns(mp)
	} else {
		for i := range ctr.nextRun {
			ctr.nextRun[i] = ctr.nextRun[i][:0]
		}
		for i := range ctr.linRun {
			ctr.linRun[i] = ctr.linRun[i][:0]
		}
	}
	for i := range ctr.linPre {
		ctr.linPre[i] = fillCoord{seq: -1, row: -1}
	}
}

func (ctr *container) freeCoordRuns(mp *mpool.MPool) {
	if ctr.allocationAccount != nil {
		for i := range ctr.nextRun {
			spillutil.FreeAccountedSlice(ctr.nextRun[i], mp)
			ctr.nextRun[i] = nil
		}
		for i := range ctr.linRun {
			spillutil.FreeAccountedSlice(ctr.linRun[i], mp)
			ctr.linRun[i] = nil
		}
		return
	}
	for i := range ctr.nextRun {
		ctr.nextRun[i] = nil
	}
	for i := range ctr.linRun {
		ctr.linRun[i] = nil
	}
}
