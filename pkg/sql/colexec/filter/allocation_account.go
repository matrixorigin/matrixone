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

package filter

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func (filter *Filter) ActivatesAllocationAccountLifecycle() bool {
	return !filter.IsAssert && !filter.OwnsConstantCastWarnings && len(filter.FilterExprs)+len(filter.RuntimeFilterExprs) > 0
}
func (filter *Filter) SetAllocationAccount(account *mpool.AllocationAccount) error {
	if account == nil || account.Handle() == 0 {
		return mpool.ErrAllocationAccountInvalid
	}
	if filter.ctr.outputAllocation != nil {
		if filter.ctr.outputAllocation.UsesAccount(account) {
			return nil
		}
		return mpool.ErrAllocationAccountMismatch
	}
	if !filter.ActivatesAllocationAccountLifecycle() {
		return nil
	}
	if filter.ctr.buf != nil || len(filter.ctr.executors)+len(filter.ctr.runtimeExecutors) > 0 {
		return mpool.ErrAllocationAccountInvariant
	}
	selection, err := vector.NewAllocationAccountSelection(account, mpool.AllocationOwnerSelection, 5, 6)
	if err != nil {
		return err
	}
	filter.ctr.outputAllocation = selection
	return nil
}
func (filter *Filter) ClearAllocationAccount(account *mpool.AllocationAccount) error {
	if filter.ctr.outputAllocation == nil {
		return nil
	}
	if !filter.ctr.outputAllocation.UsesAccount(account) {
		return mpool.ErrAllocationAccountMismatch
	}
	if filter.ctr.buf != nil || len(filter.ctr.executors)+len(filter.ctr.runtimeExecutors) > 0 {
		return mpool.ErrAllocationAccountLive
	}
	filter.ctr.outputAllocation = nil
	return nil
}
