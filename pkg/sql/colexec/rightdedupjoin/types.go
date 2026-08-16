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

package rightdedupjoin

import (
	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/spillutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(RightDedupJoin)

const (
	Build = iota
	Probe
	Finalize
	End
)

const rightDedupJoinAllocationSiteMatched mpool.AllocationSite = 90

const (
	rightDedupJoinAllocationSiteResultData mpool.AllocationSite = iota + 114
	rightDedupJoinAllocationSiteResultArea
	rightDedupJoinAllocationSiteResultNulls
	rightDedupJoinAllocationSiteResultGrouping
)

type evalVector struct {
	executor colexec.ExpressionExecutor
	vec      *vector.Vector
}

type container struct {
	state int
	itr   hashmap.Iterator

	exprExecs []colexec.ExpressionExecutor

	evecs []evalVector
	vecs  []*vector.Vector

	mp *message.JoinMap

	matched *bitmap.Bitmap

	maxAllocSize int64

	groupCount      uint64
	buildGroupCount uint64

	spillEngine    *spillutil.SpillEngine
	spillThreshold int64
	resultBatch    *batch.Batch
}

type RightDedupJoin struct {
	ctr        container
	Result     []colexec.ResultPos
	LeftTypes  []types.Type
	RightTypes []types.Type
	Conditions [][]*plan.Expr

	IsShuffle          bool
	ShuffleIdx         int32
	RuntimeFilterSpecs []*plan.RuntimeFilterSpec
	JoinMapTag         int32

	OnDuplicateAction plan.Node_OnDuplicateAction
	// InputKeysUnique means the probe stream is proven unique on the dedup key.
	// The operator then performs lookup-only conflict checks and never inserts
	// probe keys into the target hashmap.
	InputKeysUnique   bool
	DedupColName      string
	SpillThreshold    int64
	DedupColTypes     []plan.Type
	DelColIdx         int32
	UpdateColIdxList  []int32
	UpdateColExprList []*plan.Expr
	allocationAccount *mpool.AllocationAccount
	resultAllocation  *vector.AllocationAccountSelection

	vm.OperatorBase
}

func (rightDedupJoin *RightDedupJoin) SetAllocationAccount(
	account *mpool.AllocationAccount,
) error {
	if account == nil || account.Handle() == 0 {
		return mpool.ErrAllocationAccountInvalid
	}
	if rightDedupJoin.allocationAccount != nil &&
		rightDedupJoin.allocationAccount != account {
		return mpool.ErrAllocationAccountMismatch
	}
	if rightDedupJoin.allocationAccount == account {
		return nil
	}
	selection, err := vector.NewAllocationAccountSelection(
		account,
		mpool.AllocationOwnerHashBuild,
		rightDedupJoinAllocationSiteResultData,
		rightDedupJoinAllocationSiteResultArea,
		rightDedupJoinAllocationSiteResultNulls,
		rightDedupJoinAllocationSiteResultGrouping,
	)
	if err != nil {
		return err
	}
	rightDedupJoin.allocationAccount = account
	rightDedupJoin.resultAllocation = selection
	return nil
}

func (rightDedupJoin *RightDedupJoin) ClearAllocationAccount(
	account *mpool.AllocationAccount,
) error {
	if rightDedupJoin.allocationAccount == nil {
		return nil
	}
	if rightDedupJoin.allocationAccount != account {
		return mpool.ErrAllocationAccountMismatch
	}
	if rightDedupJoin.ctr.mp != nil ||
		rightDedupJoin.ctr.spillEngine != nil ||
		len(rightDedupJoin.ctr.evecs) != 0 ||
		len(rightDedupJoin.ctr.exprExecs) != 0 ||
		rightDedupJoin.ctr.matched != nil ||
		rightDedupJoin.ctr.resultBatch != nil {
		return mpool.ErrAllocationAccountInvariant
	}
	rightDedupJoin.allocationAccount = nil
	rightDedupJoin.resultAllocation = nil
	return nil
}

func (rightDedupJoin *RightDedupJoin) GetOperatorBase() *vm.OperatorBase {
	return &rightDedupJoin.OperatorBase
}

func init() {
	reuse.CreatePool(
		func() *RightDedupJoin {
			return &RightDedupJoin{}
		},
		func(a *RightDedupJoin) {
			*a = RightDedupJoin{}
		},
		reuse.DefaultOptions[RightDedupJoin]().
			WithEnableChecker(),
	)
}

func (rightDedupJoin RightDedupJoin) TypeName() string {
	return opName
}

func NewArgument() *RightDedupJoin {
	return reuse.Alloc[RightDedupJoin](nil)
}

func (rightDedupJoin *RightDedupJoin) Release() {
	if rightDedupJoin != nil {
		reuse.Free[RightDedupJoin](rightDedupJoin, nil)
	}
}

func (rightDedupJoin *RightDedupJoin) Reset(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &rightDedupJoin.ctr
	if rightDedupJoin.OpAnalyzer != nil {
		rightDedupJoin.OpAnalyzer.Alloc(ctr.maxAllocSize)
	}
	ctr.maxAllocSize = 0
	hashmap.IteratorClearOwner(ctr.itr)
	ctr.itr = nil
	ctr.groupCount = 0
	ctr.buildGroupCount = 0

	ctr.cleanBitmap(proc)
	ctr.cleanHashMap()
	ctr.cleanResultBatch(proc)
	ctr.cleanExprExecutor()
	if ctr.spillEngine != nil {
		ctr.spillEngine.Cleanup(proc)
		ctr.spillEngine = nil
	}
	ctr.cleanEvalVectors()
	ctr.state = Build
}

func (rightDedupJoin *RightDedupJoin) Free(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &rightDedupJoin.ctr
	ctr.cleanBitmap(proc)
	ctr.cleanHashMap()
	ctr.cleanResultBatch(proc)
	ctr.cleanExprExecutor()
	if ctr.spillEngine != nil {
		ctr.spillEngine.Cleanup(proc)
		ctr.spillEngine = nil
	}
	ctr.cleanEvalVectors()
}

func (rightDedupJoin *RightDedupJoin) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	return input, nil
}

func (ctr *container) cleanExprExecutor() {
	for i := range ctr.exprExecs {
		if ctr.exprExecs[i] != nil {
			ctr.exprExecs[i].Free()
		}
	}
	ctr.exprExecs = nil
}

func (ctr *container) cleanHashMap() {
	hashmap.IteratorClearOwner(ctr.itr)
	ctr.itr = nil
	if ctr.mp != nil {
		ctr.mp.Free()
		ctr.mp = nil
	}
}

func (ctr *container) cleanBitmap(proc *process.Process) {
	colexec.FreeAccountedBitmap(ctr.matched, proc.Mp())
	ctr.matched = nil
}

func (ctr *container) resetResultBatch() {
	if ctr.resultBatch == nil {
		return
	}
	ctr.resultBatch.CleanOnlyData()
	for _, vec := range ctr.resultBatch.Vecs {
		vec.SetClass(vector.FLAT)
		vec.SetLength(0)
	}
}

func (rightDedupJoin *RightDedupJoin) resetResultBatch() error {
	ctr := &rightDedupJoin.ctr
	ctr.resetResultBatch()
	if ctr.resultBatch != nil {
		return nil
	}
	ctr.resultBatch = batch.NewOffHeapWithSize(len(rightDedupJoin.Result))
	for i, rp := range rightDedupJoin.Result {
		if rp.Rel == 0 {
			ctr.resultBatch.Vecs[i] = vector.NewOffHeapVecWithType(
				rightDedupJoin.LeftTypes[rp.Pos],
			)
		} else {
			ctr.resultBatch.Vecs[i] = vector.NewOffHeapVecWithType(
				rightDedupJoin.RightTypes[rp.Pos],
			)
		}
	}
	if err := ctr.resultBatch.SetAllocationAccount(rightDedupJoin.resultAllocation); err != nil {
		ctr.resultBatch.Clean(nil)
		ctr.resultBatch = nil
		return err
	}
	return nil
}

func (ctr *container) cleanResultBatch(proc *process.Process) {
	if ctr.resultBatch != nil {
		ctr.resultBatch.Clean(proc.Mp())
		ctr.resultBatch = nil
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
	ctr.vecs = nil
}
