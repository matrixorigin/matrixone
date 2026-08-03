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
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(Fill)

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
	// prevPartKey / prevPartNull snapshot the partition key of the last row of
	// the previous batch, so the first row of the next batch can detect a
	// boundary without keeping the old batch alive.
	prevPartKey  [][]byte
	prevPartNull []bool
	prevPartSet  bool

	// next / linear incremental engine. bats is a FIFO of still-pending child
	// batches; baseSeq is the absolute sequence number of bats[0], so a
	// fillCoord captured as an absolute seq stays valid after the FIFO pops its
	// head (local index = seq - baseSeq). toFree holds the batch handed to the
	// caller on the previous Call, released at the top of the next one.
	// flushable counts the resolved prefix of bats that may be emitted;
	// childDone records child EOF.
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

func (fill *Fill) Reset(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &fill.ctr
	ctr.cleanupSpill(proc)
	ctr.clearLinearSeeds(proc.Mp())
	ctr.clearLinearEntries(proc.Mp())
	ctr.resetCtrParma()
	ctr.resetExes()
	if ctr.buf != nil {
		ctr.buf.CleanOnlyData()
	}
	for _, b := range ctr.bats {
		if b != nil {
			b.Clean(proc.GetMPool())
		}
	}
	ctr.bats = ctr.bats[:0]
	// toFree was popped out of bats, so the loop above does not cover it.
	if ctr.toFree != nil {
		ctr.toFree.Clean(proc.GetMPool())
		ctr.toFree = nil
	}

	if fill.ProjectList != nil {
		if fill.OpAnalyzer != nil {
			fill.OpAnalyzer.Alloc(fill.ProjectAllocSize)
		}
		fill.ResetProjection(proc)
	}
}

func (fill *Fill) Free(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &fill.ctr
	ctr.cleanupSpill(proc)
	ctr.freeBatch(proc.Mp())
	ctr.freeExes()
	ctr.freeVectors(proc.Mp())

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
	ctr.clearLinearSeeds(mp)
	ctr.clearLinearEntries(mp)
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
	ctr.linEntryPart = spillPartitionSnapshot{}
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

func (ctr *container) resetCtrParma() {
	ctr.baseSeq = 0
	ctr.flushable = 0
	ctr.childDone = false
	ctr.pendingBytes = 0
	ctr.pendingRows = 0
	for i := range ctr.prevValid {
		ctr.prevValid[i] = false
	}
	ctr.prevPartKey = nil
	ctr.prevPartNull = nil
	ctr.prevPartSet = false
	for i := range ctr.nextRun {
		ctr.nextRun[i] = ctr.nextRun[i][:0]
	}
	for i := range ctr.linRun {
		ctr.linRun[i] = ctr.linRun[i][:0]
	}
	for i := range ctr.linPre {
		ctr.linPre[i] = fillCoord{seq: -1, row: -1}
	}
}
