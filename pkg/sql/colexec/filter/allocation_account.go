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
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (filter *Filter) ActivatesAllocationAccountLifecycle() bool {
	return !filter.IsAssert && !filter.OwnsConstantCastWarnings && len(filter.FilterExprs)+len(filter.RuntimeFilterExprs) > 0
}
func (filter *Filter) SetAllocationAccount(account *mpool.AllocationAccount) error {
	if account == nil || account.Handle() == 0 {
		return mpool.ErrAllocationAccountInvalid
	}
	if filter.ctr.account != nil {
		if filter.ctr.account == account {
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
	filter.ctr.account = account
	filter.ctr.outputAllocation = nil
	return nil
}
func (filter *Filter) ClearAllocationAccount(account *mpool.AllocationAccount) error {
	if filter.ctr.account == nil {
		return nil
	}
	if filter.ctr.account != account {
		return mpool.ErrAllocationAccountMismatch
	}
	if filter.ctr.buf != nil || len(filter.ctr.executors)+len(filter.ctr.runtimeExecutors) > 0 {
		return mpool.ErrAllocationAccountLive
	}
	filter.ctr.account = nil
	filter.ctr.outputAllocation = nil
	return nil
}

// bindOutputAllocation builds the accounted view of proc's pool that the
// filter's executors and output buffer allocate through. Binding the account
// happens before the operator knows its process pool, so the view is built on
// first use.
func (ctr *container) bindOutputAllocation(proc *process.Process) error {
	if ctr.account == nil || ctr.outputAllocation != nil {
		return nil
	}
	view, err := proc.Mp().WithAccount(
		ctr.account, mpool.AllocationOwnerSelection, 5, mpool.AllocationCapacityClassDefault)
	if err != nil {
		return err
	}
	ctr.outputAllocation = view
	return nil
}

// mp returns the pool the filter output allocates from: the accounted view
// when an account is bound, otherwise proc's pool.
func (ctr *container) mp(proc *process.Process) *mpool.MPool {
	if ctr.outputAllocation != nil {
		return ctr.outputAllocation
	}
	return proc.Mp()
}
