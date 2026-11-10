// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package projection

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (projection *Projection) ActivatesAllocationAccountLifecycle() bool {
	if projection.GroupingSetCount != 0 || len(projection.ProjectList) == 0 || len(projection.Children) == 0 {
		return false
	}
	_, possible := projection.GetChildren(0).(vm.DictionaryProducer)
	return possible
}
func (projection *Projection) SetAllocationAccount(account *mpool.AllocationAccount) error {
	if account == nil || account.Handle() == 0 {
		return mpool.ErrAllocationAccountInvalid
	}
	if projection.ctr.outputAllocation != nil {
		if projection.ctr.outputAllocation.UsesAccount(account) {
			return nil
		}
		return mpool.ErrAllocationAccountMismatch
	}
	if !projection.ActivatesAllocationAccountLifecycle() {
		return nil
	}
	if projection.ctr.buf != nil || len(projection.ctr.projExecutors) > 0 {
		return mpool.ErrAllocationAccountInvariant
	}
	selection, err := vector.NewAllocationAccountSelection(account, mpool.AllocationOwnerSelection, 9, 10, 11, 12)
	if err != nil {
		return err
	}
	projection.ctr.outputAllocation = selection
	return nil
}
func (projection *Projection) ClearAllocationAccount(account *mpool.AllocationAccount) error {
	if projection.ctr.outputAllocation == nil {
		return nil
	}
	if !projection.ctr.outputAllocation.UsesAccount(account) {
		return mpool.ErrAllocationAccountMismatch
	}
	if projection.ctr.buf != nil || len(projection.ctr.projExecutors)+len(projection.ctr.expandOwned) > 0 {
		return mpool.ErrAllocationAccountLive
	}
	projection.ctr.outputAllocation = nil
	return nil
}
func (projection *Projection) AcceptsDictionary(child int) bool {
	if child != 0 || projection.GroupingSetCount != 0 || projection.ctr.outputAllocation == nil ||
		!colexec.ExpressionsAcceptDictionary(projection.ctr.projExecutors) {
		return false
	}
	if projection.ctr.dictionaryBoundary.AllowsDictionaryOutput() {
		return true
	}
	// A pure forwarder must not enable a source just to flatten it immediately.
	for _, expr := range projection.ProjectList {
		if expr.GetCol() == nil {
			return true
		}
	}
	return false
}
func (projection *Projection) DictionaryOutput() (bool, *vector.AllocationAccountSelection) {
	if projection.ctr.outputAllocation == nil || projection.GroupingSetCount != 0 || len(projection.Children) == 0 ||
		!colexec.ExpressionsAcceptDictionary(projection.ctr.projExecutors) {
		return false, projection.ctr.outputAllocation
	}
	if child, ok := projection.GetChildren(0).(vm.DictionaryProducer); ok {
		possible, _ := child.DictionaryOutput()
		return possible, projection.ctr.outputAllocation
	}
	return false, projection.ctr.outputAllocation
}
func (projection *Projection) DictionaryOutputState() *vm.DictionaryOutputState {
	return &projection.ctr.dictionaryBoundary
}
func (projection *Projection) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	return projection.ctr.dictionaryBoundary.Normalize(input, proc.Mp())
}
