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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/util"

	"github.com/matrixorigin/matrixone/pkg/common/reuse"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(PreInsertUnique)

type container struct {
	buf              *batch.Batch
	updateIgnoreKeys []updateIgnoreKeyState
}
type PreInsertUnique struct {
	ctr          container
	PreInsertCtx *plan.PreInsertUkCtx

	packers util.PackerList

	allocationAccount  *mpool.AllocationAccount
	hashAllocation     *hashtable.AllocationAccountSelection
	vectorAllocation   *vector.AllocationAccountSelection
	iteratorAllocation *hashmap.IteratorAllocation

	vm.OperatorBase
}

const updateIgnoreAllocationOwner mpool.AllocationOwner = 1

const (
	updateIgnoreHashCell mpool.AllocationSite = iota + 1
	updateIgnoreHashDescriptor
	updateIgnoreVectorData
	updateIgnoreVectorArea
	updateIgnoreVectorNulls
	updateIgnoreVectorGrouping
	updateIgnoreIteratorScratch
)

// Only the ordered UPDATE IGNORE mode retains state across input batches.
func (preInsertUnique *PreInsertUnique) ActivatesAllocationAccountLifecycle() bool {
	return preInsertUnique != nil && preInsertUnique.PreInsertCtx != nil && preInsertUnique.PreInsertCtx.UpdateIgnoreArbitration
}

func (preInsertUnique *PreInsertUnique) SetAllocationAccount(account *mpool.AllocationAccount) error {
	if preInsertUnique == nil || account == nil || account.Handle() == 0 {
		return mpool.ErrAllocationAccountInvalid
	}
	if preInsertUnique.allocationAccount != nil {
		if preInsertUnique.allocationAccount == account {
			return nil
		}
		return mpool.ErrAllocationAccountMismatch
	}
	if len(preInsertUnique.ctr.updateIgnoreKeys) != 0 {
		return mpool.ErrAllocationAccountInvariant
	}
	hashAllocation, err := hashtable.NewAllocationAccountSelection(account, updateIgnoreAllocationOwner, updateIgnoreHashCell, updateIgnoreHashDescriptor)
	if err != nil {
		return err
	}
	vectorAllocation, err := vector.NewAllocationAccountSelection(account, updateIgnoreAllocationOwner, updateIgnoreVectorData, updateIgnoreVectorArea, updateIgnoreVectorNulls, updateIgnoreVectorGrouping)
	if err != nil {
		return err
	}
	iteratorAllocation, err := hashmap.NewIteratorAllocation(account, updateIgnoreAllocationOwner, updateIgnoreIteratorScratch)
	if err != nil {
		return err
	}
	preInsertUnique.allocationAccount, preInsertUnique.hashAllocation, preInsertUnique.vectorAllocation, preInsertUnique.iteratorAllocation = account, hashAllocation, vectorAllocation, iteratorAllocation
	return nil
}

func (preInsertUnique *PreInsertUnique) ClearAllocationAccount(account *mpool.AllocationAccount) error {
	if preInsertUnique == nil || preInsertUnique.allocationAccount == nil {
		return nil
	}
	if preInsertUnique.allocationAccount != account {
		return mpool.ErrAllocationAccountMismatch
	}
	if len(preInsertUnique.ctr.updateIgnoreKeys) != 0 || (preInsertUnique.ActivatesAllocationAccountLifecycle() && preInsertUnique.ctr.buf != nil) {
		return mpool.ErrAllocationAccountInvariant
	}
	preInsertUnique.allocationAccount, preInsertUnique.hashAllocation, preInsertUnique.vectorAllocation, preInsertUnique.iteratorAllocation = nil, nil, nil, nil
	return nil
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
	preInsertUnique.freeUpdateIgnore(proc)
	if preInsertUnique.ctr.buf != nil {
		if preInsertUnique.ActivatesAllocationAccountLifecycle() {
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
	preInsertUnique.freeUpdateIgnore(proc)
	if preInsertUnique.ctr.buf != nil {
		preInsertUnique.ctr.buf.Clean(proc.Mp())
		preInsertUnique.ctr.buf = nil
	}
	preInsertUnique.packers.Free()
}

func (preInsertUnique *PreInsertUnique) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	return input, nil
}
