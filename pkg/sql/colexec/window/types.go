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

package window

import (
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(Window)

const (
	receive = iota
	eval
	emit
	done
	receiveAll
)

type container struct {
	status int

	bat     *batch.Batch
	batAggs []aggexec.AggFuncExec

	// runningAgg retains the one-group aggregate for cumulative and bounded
	// sliding frames between output chunks. runningNextRow guards against
	// accidentally reusing the state out of order; runningLeft/runningRight
	// describe the current half-open sliding frame. For RANGE frames,
	// runningPeerEnd lets every row in one peer group reuse the same boundaries.
	runningAgg       aggexec.AggFuncExec
	runningNextRow   int
	runningPartition int
	runningLeft      int
	runningRight     int
	runningPeerEnd   int

	desc      []bool
	nullsLast []bool
	orderVecs []colexec.ExprEvalVector
	sels      []int64

	ps      []int64 // index of partition by
	os      []int64 // Sorted partitions
	aggVecs []colexec.ExprEvalVector

	prepareParamKind aggexec.PrepareParamKindStates

	emitOffset int
	rBat       *batch.Batch

	runtimeFrames []*plan.FrameClause

	// timestampCivilOrder caches the monotonic civil-time spans of a sorted
	// TIMESTAMP partition. It is scoped to one materialized input generation
	// and is cleared before order vectors are reused for the next input batch.
	// A fold query can then binary-search each span instead of rescanning the
	// partition for every frame row.
	timestampCivilOrder map[timestampCivilOrderKey]*timestampCivilOrderIndex
}

type timestampCivilOrderKey struct {
	vec        *vector.Vector
	loc        *time.Location
	start, end int
	desc       bool
}

type timestampCivilOrderIndex struct {
	hasFold         bool
	nullPrefixEnd   int
	nullSuffixStart int
	spans           []timestampCivilOrderSpan
}

type timestampCivilOrderSpan struct {
	start, end int
}

// timestampRangeSelection preserves window order while allowing a civil-time
// frame to consist of several disjoint instant-sorted spans around a fold.
type timestampRangeSelection struct {
	spans []timestampCivilOrderSpan
}

type Window struct {
	ctr         container
	WinSpecList []*plan.Expr
	// sort and partition
	Fs []*plan.OrderBySpec
	// agg func
	Aggs []aggexec.AggFuncExecExpression
	// PartitionTopN allows the bounded ROW_NUMBER path to coalesce complete
	// candidate partitions and evaluate their explicit boundaries once.
	PartitionTopN bool

	vm.OperatorBase
}

func (window *Window) GetOperatorBase() *vm.OperatorBase {
	return &window.OperatorBase
}

func init() {
	reuse.CreatePool[Window](
		func() *Window {
			return &Window{}
		},
		func(a *Window) {
			*a = Window{}
		},
		reuse.DefaultOptions[Window]().
			WithEnableChecker(),
	)
}

func (window Window) TypeName() string {
	return opName
}

func NewArgument() *Window {
	return reuse.Alloc[Window](nil)
}

func (window *Window) Release() {
	if window != nil {
		reuse.Free[Window](window, nil)
	}
}

func (window *Window) Reset(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &window.ctr

	ctr.cleanOutput(proc.Mp())
	ctr.resetParam()
	ctr.prepareParamKind.Reset(nil)
	ctr.resetVectors()
	// Release aggregators here too: on an error exit from Call the normal
	// freeAggFun() at the end of the eval loop is skipped, so batAggs would
	// otherwise keep their accumulated state (e.g. json payloads, distinct
	// hashes) in the mpool until the next reuse.
	ctr.freeAggFun()
	ctr.freeRunningAgg()
	if ctr.hasAccountedBufferedData() {
		// AppendWithCopy and Dup preserve a source vector's allocation
		// selection. Release inherited backing at the prepared-statement
		// generation boundary; only unaccounted buffers may be reused.
		ctr.freeBatch(proc.Mp())
		ctr.freeVector(proc.Mp())
	} else if ctr.bat != nil {
		ctr.bat.CleanOnlyData()
	}
}

func (window *Window) Free(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &window.ctr

	ctr.cleanOutput(proc.Mp())
	ctr.runtimeFrames = nil
	ctr.timestampCivilOrder = nil
	// Free aggregators before the batch so an error exit from Call (which skips
	// the normal freeAggFun()) does not leak their mpool-held state.
	ctr.freeAggFun()
	ctr.freeRunningAgg()
	ctr.freeBatch(proc.Mp())
	ctr.freeExes()
	ctr.freeVector(proc.Mp())
	ctr.prepareParamKind.Reset(nil)
}

func (window *Window) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	return input, nil
}

func (ctr *container) resetParam() {
	ctr.status = receive
	ctr.emitOffset = 0
	ctr.desc = nil
	ctr.nullsLast = nil
	ctr.sels = nil
	ctr.ps = nil
	ctr.os = nil
	ctr.runtimeFrames = nil
	ctr.timestampCivilOrder = nil
}

// cleanOutput releases the batch returned by the previous Call. Input-column
// vectors in rBat are borrowed windows into ctr.bat; the appended window-result
// vector is owned by rBat. The pipeline contract keeps a returned batch valid
// until the next Call or Reset, so this is the earliest safe release point.
func (ctr *container) cleanOutput(mp *mpool.MPool) {
	if ctr.rBat != nil {
		ctr.rBat.Clean(mp)
		ctr.rBat = nil
	}
}

func (ctr *container) resetVectors() {
	for i := range ctr.orderVecs {
		ctr.orderVecs[i].ResetForNextQuery()
	}

	for i := range ctr.aggVecs {
		ctr.aggVecs[i].ResetForNextQuery()
	}
}

func (ctr *container) freeBatch(mp *mpool.MPool) {
	if ctr.bat != nil {
		ctr.bat.Clean(mp)
		ctr.bat = nil
	}
}

func (ctr *container) freeAggFun() {
	for i, a := range ctr.batAggs {
		if a != nil {
			a.Free()
			ctr.batAggs[i] = nil
		}
	}
	ctr.batAggs = nil
}

func (ctr *container) freeRunningAgg() {
	if ctr.runningAgg != nil {
		ctr.runningAgg.Free()
		ctr.runningAgg = nil
	}
	ctr.runningNextRow = 0
	ctr.runningPartition = 0
	ctr.runningLeft = 0
	ctr.runningRight = 0
	ctr.runningPeerEnd = 0
}

func (ctr *container) freeExes() {
	for i := range ctr.orderVecs {
		ctr.orderVecs[i].Free()
	}

	for i := range ctr.aggVecs {
		ctr.aggVecs[i].Free()
	}
}

func (ctr *container) freeVector(mp *mpool.MPool) {
	for i := range ctr.orderVecs {
		for j, vec := range ctr.orderVecs[i].Vec {
			if vec != nil {
				vec.Free(mp)
				ctr.orderVecs[i].Vec[j] = nil
			}
		}
	}

	for i := range ctr.aggVecs {
		for j, vec := range ctr.aggVecs[i].Vec {
			if vec != nil {
				vec.Free(mp)
				ctr.aggVecs[i].Vec[j] = nil
			}
		}
	}

}

func (ctr *container) hasAccountedBufferedData() bool {
	if ctr == nil {
		return false
	}
	if ctr.bat != nil && ctr.bat.HasAllocationAccount() {
		return true
	}
	for _, eval := range ctr.orderVecs {
		for _, vec := range eval.Vec {
			if vec != nil && vec.AllocationAccountSelection() != nil {
				return true
			}
		}
	}
	for _, eval := range ctr.aggVecs {
		for _, vec := range eval.Vec {
			if vec != nil && vec.AllocationAccountSelection() != nil {
				return true
			}
		}
	}
	return false
}
