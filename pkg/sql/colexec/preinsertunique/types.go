// Copyright 2022 Matrix Origin
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

package preinsertunique

import (
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/util"

	"github.com/matrixorigin/matrixone/pkg/common/reuse"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(PreInsertUnique)

type container struct {
	buf          *batch.Batch
	acceptedKeys []odkuKeyIndex
	actionRows   *odkuActionRows
	actions      *odkuActions
	lastOrdinal  int64
	failed       bool
}

// odkuKeyIndex owns one constraint's mapping from a physical key to a stable
// logical row ordinal. Negative ordinals represent vacated hash groups.
type odkuKeyIndex struct {
	keyType   types.Type
	key       [1]*vector.Vector
	index     hashmap.HashMap
	iterator  hashmap.Iterator
	fixedKeys *vector.Vector
	rows      *vector.Vector
	dead      uint64
}
type PreInsertUnique struct {
	ctr          container
	PreInsertCtx *plan.PreInsertUkCtx

	// Bound by Compile for the current attempt; Compile closes the view after
	// scope/remote-reader quiescence, including before rollback or retry.
	PriorDataView engine.StatementKeyView
	LockKeys      func(*process.Process, int, *vector.Vector) error

	packers            util.PackerList
	allocationAccount  *mpool.AllocationAccount
	hashAllocation     *hashtable.AllocationAccountSelection
	iteratorAllocation *hashmap.IteratorAllocation
	retainedAllocation *vector.AllocationAccountSelection

	vm.OperatorBase
}

func (preInsertUnique *PreInsertUnique) GetOperatorBase() *vm.OperatorBase {
	return &preInsertUnique.OperatorBase
}

func init() {
	reuse.CreatePool[PreInsertUnique](
		func() *PreInsertUnique {
			return &PreInsertUnique{}
		},
		func(a *PreInsertUnique) {
			*a = PreInsertUnique{}
		},
		reuse.DefaultOptions[PreInsertUnique]().
			WithEnableChecker(),
	)
}

func (preInsertUnique PreInsertUnique) TypeName() string {
	return opName
}

func NewArgument() *PreInsertUnique {
	return reuse.Alloc[PreInsertUnique](nil)
}

func (preInsertUnique *PreInsertUnique) Release() {
	if preInsertUnique != nil {
		reuse.Free[PreInsertUnique](preInsertUnique, nil)
	}
}

func (preInsertUnique *PreInsertUnique) Reset(proc *process.Process, pipelineFailed bool, err error) {
	preInsertUnique.freeAcceptedState(proc)
	if preInsertUnique.ctr.buf != nil {
		if preInsertUnique.PreInsertCtx.GetOrderedOdku() {
			preInsertUnique.ctr.buf.Clean(proc.Mp())
			preInsertUnique.ctr.buf = nil
		} else {
			preInsertUnique.ctr.buf.CleanOnlyData()
		}
	}
	if preInsertUnique.packers.PackerCount() > 10 {
		preInsertUnique.packers.Free()
	}
}

func (preInsertUnique *PreInsertUnique) Free(proc *process.Process, pipelineFailed bool, err error) {
	preInsertUnique.freeAcceptedState(proc)
	if preInsertUnique.ctr.buf != nil {
		preInsertUnique.ctr.buf.Clean(proc.Mp())
		preInsertUnique.ctr.buf = nil
	}
	preInsertUnique.packers.Free()
}

func (preInsertUnique *PreInsertUnique) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	return input, nil
}

const (
	preInsertUniqueAllocationSiteHashCell mpool.AllocationSite = iota + 1
	preInsertUniqueAllocationSiteHashDescriptor
	preInsertUniqueAllocationSiteRetainedData
	preInsertUniqueAllocationSiteRetainedArea
	preInsertUniqueAllocationSiteRetainedNulls
	preInsertUniqueAllocationSiteRetainedGrouping
	preInsertUniqueAllocationSiteIterator
)

// ActivatesAllocationAccountLifecycle limits the new statement-retained
// accounting requirement to ordered ODKU actions. Ordinary unique-index
// preprocessing and INSERT IGNORE retain their existing lifecycle.
func (preInsertUnique *PreInsertUnique) ActivatesAllocationAccountLifecycle() bool {
	return preInsertUnique != nil && preInsertUnique.PreInsertCtx != nil &&
		preInsertUnique.PreInsertCtx.GetOrderedOdku()
}

func (preInsertUnique *PreInsertUnique) SetAllocationAccount(
	account *mpool.AllocationAccount,
) error {
	if preInsertUnique == nil || account == nil || account.Handle() == 0 {
		return mpool.ErrAllocationAccountInvalid
	}
	if preInsertUnique.allocationAccount != nil {
		if preInsertUnique.allocationAccount == account {
			return nil
		}
		return mpool.ErrAllocationAccountMismatch
	}
	if len(preInsertUnique.ctr.acceptedKeys) != 0 || preInsertUnique.ctr.actionRows != nil {
		return mpool.ErrAllocationAccountInvariant
	}
	hashAllocation, err := hashtable.NewAllocationAccountSelection(
		account,
		mpool.AllocationOwnerMin,
		preInsertUniqueAllocationSiteHashCell,
		preInsertUniqueAllocationSiteHashDescriptor,
	)
	if err != nil {
		return err
	}
	iteratorAllocation, err := hashmap.NewIteratorAllocation(account, mpool.AllocationOwnerMin, preInsertUniqueAllocationSiteIterator)
	if err != nil {
		return err
	}
	retainedAllocation, err := vector.NewAllocationAccountSelection(
		account,
		mpool.AllocationOwnerMin,
		preInsertUniqueAllocationSiteRetainedData,
		preInsertUniqueAllocationSiteRetainedArea,
		preInsertUniqueAllocationSiteRetainedNulls,
		preInsertUniqueAllocationSiteRetainedGrouping,
	)
	if err != nil {
		return err
	}
	preInsertUnique.allocationAccount = account
	preInsertUnique.hashAllocation = hashAllocation
	preInsertUnique.iteratorAllocation = iteratorAllocation
	preInsertUnique.retainedAllocation = retainedAllocation
	return nil
}

func (preInsertUnique *PreInsertUnique) ClearAllocationAccount(
	account *mpool.AllocationAccount,
) error {
	if preInsertUnique == nil || preInsertUnique.allocationAccount == nil {
		return nil
	}
	if preInsertUnique.allocationAccount != account {
		return mpool.ErrAllocationAccountMismatch
	}
	if len(preInsertUnique.ctr.acceptedKeys) != 0 || preInsertUnique.ctr.actionRows != nil {
		return mpool.ErrAllocationAccountInvariant
	}
	preInsertUnique.allocationAccount = nil
	preInsertUnique.hashAllocation = nil
	preInsertUnique.iteratorAllocation = nil
	preInsertUnique.retainedAllocation = nil
	return nil
}

func (preInsertUnique *PreInsertUnique) freeAcceptedState(proc *process.Process) {
	if preInsertUnique.ctr.actions != nil {
		preInsertUnique.ctr.actions.free(proc)
		preInsertUnique.ctr.actions = nil
	}
	if preInsertUnique.ctr.actionRows != nil {
		preInsertUnique.ctr.actionRows.free(proc)
		preInsertUnique.ctr.actionRows = nil
	}
	for i := range preInsertUnique.ctr.acceptedKeys {
		preInsertUnique.ctr.acceptedKeys[i].free(proc)
	}
	preInsertUnique.ctr.acceptedKeys = nil
	preInsertUnique.ctr.lastOrdinal = 0
	preInsertUnique.ctr.failed = false
}
