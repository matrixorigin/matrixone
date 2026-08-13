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

package top

import (
	"io"
	"os"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/compare"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/spillutil"
	"github.com/matrixorigin/matrixone/pkg/sql/internal/topsites"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(Top)

var _ interface {
	SetAllocationAccount(*mpool.AllocationAccount) error
	ClearAllocationAccount(*mpool.AllocationAccount) error
} = new(Top)

const topSpillThreshold uint64 = 8192 * 2

type rowRef struct {
	offset int64
	size   int64
	rowIdx int64
}

type spillRecordRef struct {
	offset int64
	size   int64
}

type spillWriteFlusher interface {
	io.Writer
	Flush() error
	Free()
}

type container struct {
	n     int // result vector number
	state vm.CtrState
	sels  []int64
	poses []int32 // sorted list of attributes
	cmps  []compare.Compare

	limit         uint64
	limitExecutor colexec.ExpressionExecutor

	executorsForOrderColumn []colexec.ExpressionExecutor
	desc                    bool
	topValueZM              objectio.ZoneMap
	bat                     *batch.Batch
	buildBat                *batch.Batch //temp batch, do not need free or reset

	allocationAccount    *mpool.AllocationAccount
	retainedAllocation   *vector.AllocationAccountSelection
	expressionAllocation *vector.AllocationAccountSelection
	outputAllocation     *vector.AllocationAccountSelection
	spillAllocation      *spillutil.SpillAllocationAccount
	budget               *process.ExecutionResourceGeneration

	spilling       bool
	spillFile      *os.File
	spillWriter    spillWriteFlusher
	spillOffset    int64
	spillFDToken   *process.ExecutionSpillFDReservation
	spillDiskToken *process.ExecutionSpillDiskReservation
	rowRefs        []rowRef

	// streaming eval state for spill mode
	spillOrdered bool         // sels backing contains the final ascending order
	evalCursor   int          // next row index to output in sels
	spillOutBat  *batch.Batch // current chunk output batch, freed on next call
}

type Top struct {
	Limit       *plan.Expr
	TopValueTag int32
	ctr         container
	Fs          []*plan.OrderBySpec

	vm.OperatorBase
}

func (top *Top) GetOperatorBase() *vm.OperatorBase {
	return &top.OperatorBase
}

func init() {
	reuse.CreatePool(
		func() *Top {
			return &Top{}
		},
		func(a *Top) {
			*a = Top{}
		},
		reuse.DefaultOptions[Top]().
			WithEnableChecker(),
	)
}

func (top Top) TypeName() string {
	return opName
}

func NewArgument() *Top {
	return reuse.Alloc[Top](nil)
}

func (top *Top) WithLimit(limit *plan.Expr) *Top {
	top.Limit = limit
	return top
}

func (top *Top) WithFs(fs []*plan.OrderBySpec) *Top {
	top.Fs = fs
	return top
}

func (top *Top) Release() {
	if top != nil {
		reuse.Free(top, nil)
	}
}

func (top *Top) Reset(proc *process.Process, pipelineFailed bool, err error) {
	top.ctr.reset(proc)
}

func (top *Top) Free(proc *process.Process, pipelineFailed bool, err error) {
	top.ctr.free(proc)
}

func (top *Top) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	return input, nil
}

func (top *Top) SetAllocationAccount(
	account *mpool.AllocationAccount,
) error {
	if top == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	return top.ctr.setAllocationAccount(account)
}

func (top *Top) ClearAllocationAccount(
	account *mpool.AllocationAccount,
) error {
	if top == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	return top.ctr.clearAllocationAccount(account)
}

func (ctr *container) setAllocationAccount(
	account *mpool.AllocationAccount,
) error {
	if ctr == nil || account == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	if ctr.allocationAccount != nil {
		if ctr.allocationAccount == account {
			return nil
		}
		return mpool.ErrAllocationAccountMismatch
	}
	if ctr.bat != nil || ctr.spillOutBat != nil || ctr.limitExecutor != nil ||
		len(ctr.executorsForOrderColumn) != 0 || len(ctr.sels) != 0 ||
		len(ctr.rowRefs) != 0 || ctr.spillFile != nil || ctr.spillWriter != nil {
		return mpool.ErrAllocationAccountInvariant
	}
	retained, err := vector.NewAllocationAccountSelection(
		account,
		mpool.AllocationOwnerTop,
		topsites.TopRetainedData,
		topsites.TopRetainedArea,
		topsites.TopRetainedNulls,
		topsites.TopRetainedGrouping,
	)
	if err != nil {
		return err
	}
	expression, err := vector.NewAllocationAccountSelection(
		account,
		mpool.AllocationOwnerTop,
		topsites.TopExpressionData,
		topsites.TopExpressionArea,
		topsites.TopExpressionNulls,
		topsites.TopExpressionGrouping,
	)
	if err != nil {
		return err
	}
	output, err := vector.NewAllocationAccountSelection(
		account,
		mpool.AllocationOwnerTop,
		topsites.TopOutputData,
		topsites.TopOutputArea,
		topsites.TopOutputNulls,
		topsites.TopOutputGrouping,
	)
	if err != nil {
		return err
	}
	spill, err := spillutil.NewSpillAllocationAccount(
		account,
		mpool.AllocationOwnerTop,
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

func (ctr *container) clearAllocationAccount(
	account *mpool.AllocationAccount,
) error {
	if ctr == nil || ctr.allocationAccount == nil {
		return nil
	}
	if ctr.allocationAccount != account {
		return mpool.ErrAllocationAccountMismatch
	}
	if ctr.bat != nil || ctr.spillOutBat != nil || ctr.limitExecutor != nil ||
		len(ctr.executorsForOrderColumn) != 0 || len(ctr.sels) != 0 ||
		len(ctr.rowRefs) != 0 || ctr.spillFile != nil || ctr.spillWriter != nil ||
		ctr.spillFDToken != nil || ctr.spillDiskToken != nil {
		return mpool.ErrAllocationAccountInvariant
	}
	ctr.allocationAccount = nil
	ctr.retainedAllocation = nil
	ctr.expressionAllocation = nil
	ctr.outputAllocation = nil
	ctr.spillAllocation = nil
	ctr.budget = nil
	return nil
}

func (ctr *container) reset(proc *process.Process) {
	if ctr.bat != nil {
		ctr.bat.Clean(proc.Mp())
		ctr.bat = nil
	}
	if ctr.spillOutBat != nil {
		ctr.spillOutBat.Clean(proc.Mp())
		ctr.spillOutBat = nil
	}
	ctr.cleanupSpill(proc)

	ctr.n = 0
	ctr.state = 0
	ctr.poses = nil
	ctr.cmps = nil
	ctr.limit = 0
	if ctr.limitExecutor != nil {
		if ctr.allocationAccount != nil {
			ctr.limitExecutor.Free()
			ctr.limitExecutor = nil
		} else {
			ctr.limitExecutor.ResetForNextQuery()
		}
	}
	for i, executor := range ctr.executorsForOrderColumn {
		if executor != nil {
			if ctr.allocationAccount != nil {
				executor.Free()
				ctr.executorsForOrderColumn[i] = nil
			} else {
				executor.ResetForNextQuery()
			}
		}
	}
	if ctr.allocationAccount != nil {
		ctr.executorsForOrderColumn = nil
	}
	ctr.desc = false
	ctr.topValueZM = nil
	ctr.buildBat = nil
	ctr.budget = nil
}

func (ctr *container) free(proc *process.Process) {
	if ctr.bat != nil {
		ctr.bat.Clean(proc.Mp())
		ctr.bat = nil
	}
	if ctr.spillOutBat != nil {
		ctr.spillOutBat.Clean(proc.Mp())
		ctr.spillOutBat = nil
	}
	ctr.cleanupSpill(proc)
	for i, executor := range ctr.executorsForOrderColumn {
		if executor != nil {
			executor.Free()
			ctr.executorsForOrderColumn[i] = nil
		}
	}
	ctr.executorsForOrderColumn = nil
	if ctr.limitExecutor != nil {
		ctr.limitExecutor.Free()
		ctr.limitExecutor = nil
	}
	ctr.buildBat = nil
	ctr.poses = nil
	ctr.cmps = nil
	ctr.budget = nil
}

func (ctr *container) cleanupSpill(proc *process.Process) {
	if ctr.spillWriter != nil {
		ctr.spillWriter.Free()
		ctr.spillWriter = nil
	}
	_ = ctr.closeSpillFile()
	if ctr.allocationAccount != nil && proc != nil {
		spillutil.FreeAccountedSlice(ctr.sels, proc.Mp())
		spillutil.FreeAccountedSlice(ctr.rowRefs, proc.Mp())
	}
	ctr.sels = nil
	ctr.rowRefs = nil
	ctr.spilling = false
	ctr.spillOffset = 0
	ctr.spillOrdered = false
	ctr.evalCursor = 0
}

func (ctr *container) closeSpillFile() error {
	var err error
	if ctr.spillFile != nil {
		err = ctr.spillFile.Close()
		ctr.spillFile = nil
	}
	if ctr.spillDiskToken != nil {
		ctr.spillDiskToken.Release()
		ctr.spillDiskToken = nil
	}
	if ctr.spillFDToken != nil {
		ctr.spillFDToken.Release()
		ctr.spillFDToken = nil
	}
	return err
}

func (ctr *container) compare(vi, vj int, i, j int64) int {
	if ctr.spilling {
		for pos := range ctr.cmps {
			if r := ctr.cmps[pos].Compare(vi, vj, i, j); r != 0 {
				return r
			}
		}
		return 0
	}
	for _, pos := range ctr.poses {
		if r := ctr.cmps[pos].Compare(vi, vj, i, j); r != 0 {
			return r
		}
	}
	return 0
}

func (ctr *container) Len() int {
	return len(ctr.sels)
}

func (ctr *container) Less(i, j int) bool {
	return ctr.compare(0, 0, ctr.sels[i], ctr.sels[j]) > 0
}

func (ctr *container) Swap(i, j int) {
	ctr.sels[i], ctr.sels[j] = ctr.sels[j], ctr.sels[i]
}

func (ctr *container) Push(x interface{}) {
	ctr.sels = append(ctr.sels, x.(int64))
}

func (ctr *container) Pop() interface{} {
	n := len(ctr.sels) - 1
	x := ctr.sels[n]
	ctr.sels = ctr.sels[:n]
	return x
}
