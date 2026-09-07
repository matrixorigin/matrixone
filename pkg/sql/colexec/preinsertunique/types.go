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

const (
	preInsertUniqueAllocationSiteHashCell mpool.AllocationSite = iota + 1
	preInsertUniqueAllocationSiteHashDescriptor
	preInsertUniqueAllocationSiteRetainedData
	preInsertUniqueAllocationSiteRetainedArea
	preInsertUniqueAllocationSiteRetainedNulls
	preInsertUniqueAllocationSiteRetainedGrouping
)

type container struct {
	buf             *batch.Batch
	acceptedMaps    []*hashmap.StrHashMap
	acceptedIters   []hashmap.Iterator
	acceptedKeyVecs [][]*vector.Vector
	acceptedTarget  *vector.Vector
	acceptedRows    []*vector.Vector
}
type PreInsertUnique struct {
	ctr          container
	PreInsertCtx *plan.PreInsertUkCtx

	packers util.PackerList

	allocationAccount  *mpool.AllocationAccount
	hashAllocation     *hashtable.AllocationAccountSelection
	retainedAllocation *vector.AllocationAccountSelection

	vm.OperatorBase
}

// ActivatesAllocationAccountLifecycle limits the new statement-retained
// accounting requirement to ordered ODKU arbitration. Ordinary unique-index
// preprocessing and INSERT IGNORE retain their existing lifecycle.
func (preInsertUnique *PreInsertUnique) ActivatesAllocationAccountLifecycle() bool {
	return preInsertUnique != nil && preInsertUnique.PreInsertCtx != nil &&
		preInsertUnique.PreInsertCtx.GetOdkuTargetArbitration()
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
	if len(preInsertUnique.ctr.acceptedMaps) != 0 ||
		preInsertUnique.ctr.acceptedTarget != nil || len(preInsertUnique.ctr.acceptedRows) != 0 {
		return mpool.ErrAllocationAccountInvariant
	}
	hashAllocation, err := hashtable.NewAllocationAccountSelection(
		account,
		mpool.AllocationOwnerDML,
		preInsertUniqueAllocationSiteHashCell,
		preInsertUniqueAllocationSiteHashDescriptor,
	)
	if err != nil {
		return err
	}
	retainedAllocation, err := vector.NewAllocationAccountSelection(
		account,
		mpool.AllocationOwnerDML,
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
	if len(preInsertUnique.ctr.acceptedMaps) != 0 ||
		preInsertUnique.ctr.acceptedTarget != nil || len(preInsertUnique.ctr.acceptedRows) != 0 {
		return mpool.ErrAllocationAccountInvariant
	}
	preInsertUnique.allocationAccount = nil
	preInsertUnique.hashAllocation = nil
	preInsertUnique.retainedAllocation = nil
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
	if preInsertUnique.ctr.buf != nil {
		preInsertUnique.ctr.buf.CleanOnlyData()
	}
	if preInsertUnique.packers.PackerCount() > 10 {
		preInsertUnique.packers.Free()
	}
	preInsertUnique.freeAcceptedState(proc)
}

func (preInsertUnique *PreInsertUnique) Free(proc *process.Process, pipelineFailed bool, err error) {
	if preInsertUnique.ctr.buf != nil {
		preInsertUnique.ctr.buf.Clean(proc.Mp())
		preInsertUnique.ctr.buf = nil
	}
	preInsertUnique.packers.Free()
	preInsertUnique.freeAcceptedState(proc)
}

func (preInsertUnique *PreInsertUnique) freeAcceptedState(proc *process.Process) {
	for i := range preInsertUnique.ctr.acceptedMaps {
		if preInsertUnique.ctr.acceptedMaps[i] != nil {
			preInsertUnique.ctr.acceptedMaps[i].Free()
		}
	}
	preInsertUnique.ctr.acceptedMaps = nil
	preInsertUnique.ctr.acceptedIters = nil
	preInsertUnique.ctr.acceptedKeyVecs = nil
	if preInsertUnique.ctr.acceptedTarget != nil {
		preInsertUnique.ctr.acceptedTarget.Free(proc.Mp())
	}
	preInsertUnique.ctr.acceptedTarget = nil
	for i := range preInsertUnique.ctr.acceptedRows {
		if preInsertUnique.ctr.acceptedRows[i] != nil {
			preInsertUnique.ctr.acceptedRows[i].Free(proc.Mp())
		}
	}
	preInsertUnique.ctr.acceptedRows = nil
}

func (preInsertUnique *PreInsertUnique) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	return input, nil
}
