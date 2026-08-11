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

package indexbuild

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(IndexBuild)

const (
	ReceiveBatch = iota
	HandleRuntimeFilter
	End
)

const indexBuildAllocationOwner mpool.AllocationOwner = 1

const (
	indexBuildAllocationSiteRuntimeFilterData mpool.AllocationSite = iota + 1
	indexBuildAllocationSiteRuntimeFilterArea
	indexBuildAllocationSiteRuntimeFilterNulls
	indexBuildAllocationSiteRuntimeFilterGrouping
	indexBuildAllocationSiteRuntimeFilterPayload
)

type container struct {
	state               int
	buf                 *batch.Batch
	runtimeFilterUsable bool
	runtimeFilterDone   bool
}

type IndexBuild struct {
	ctr                     container
	RuntimeFilterSpec       *plan.RuntimeFilterSpec
	allocationAccount       *mpool.AllocationAccount
	runtimeFilterAllocation *vector.AllocationAccountSelection
	vm.OperatorBase
}

func (indexBuild *IndexBuild) SetAllocationAccount(
	account *mpool.AllocationAccount,
) error {
	if account == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	if indexBuild.allocationAccount != nil {
		if indexBuild.allocationAccount == account {
			return nil
		}
		return mpool.ErrAllocationAccountMismatch
	}
	selection, err := vector.NewAllocationAccountSelection(
		account,
		indexBuildAllocationOwner,
		indexBuildAllocationSiteRuntimeFilterData,
		indexBuildAllocationSiteRuntimeFilterArea,
		indexBuildAllocationSiteRuntimeFilterNulls,
		indexBuildAllocationSiteRuntimeFilterGrouping,
	)
	if err != nil {
		return err
	}
	indexBuild.allocationAccount = account
	indexBuild.runtimeFilterAllocation = selection
	return nil
}

func (indexBuild *IndexBuild) ClearAllocationAccount(
	account *mpool.AllocationAccount,
) error {
	if indexBuild.allocationAccount == nil {
		return nil
	}
	if indexBuild.allocationAccount != account {
		return mpool.ErrAllocationAccountMismatch
	}
	if indexBuild.ctr.buf != nil {
		return mpool.ErrAllocationAccountInvariant
	}
	indexBuild.allocationAccount = nil
	indexBuild.runtimeFilterAllocation = nil
	return nil
}

func (indexBuild *IndexBuild) GetOperatorBase() *vm.OperatorBase {
	return &indexBuild.OperatorBase
}

func init() {
	reuse.CreatePool[IndexBuild](
		func() *IndexBuild {
			return &IndexBuild{}
		},
		func(a *IndexBuild) {
			*a = IndexBuild{}
		},
		reuse.DefaultOptions[IndexBuild]().
			WithEnableChecker(),
	)
}

func (indexBuild IndexBuild) TypeName() string {
	return opName
}

func NewArgument() *IndexBuild {
	return reuse.Alloc[IndexBuild](nil)
}

func (indexBuild *IndexBuild) Release() {
	if indexBuild != nil {
		reuse.Free[IndexBuild](indexBuild, nil)
	}
}

func (indexBuild *IndexBuild) Reset(proc *process.Process, pipelineFailed bool, err error) {
	runtimeSucceed := indexBuild.ctr.state > HandleRuntimeFilter

	if !indexBuild.ctr.runtimeFilterDone {
		if !runtimeSucceed && (pipelineFailed || err != nil) {
			message.FinalizeRuntimeFilterOnBuildError(
				indexBuild.RuntimeFilterSpec, proc.GetMessageBoard())
		} else {
			message.FinalizeRuntimeFilter(
				indexBuild.RuntimeFilterSpec, runtimeSucceed, proc.GetMessageBoard())
		}
		indexBuild.ctr.runtimeFilterDone =
			indexBuild.RuntimeFilterSpec != nil
	}
	indexBuild.ctr.state = ReceiveBatch
	indexBuild.ctr.runtimeFilterUsable = false
	if indexBuild.ctr.buf != nil {
		indexBuild.ctr.buf.Clean(proc.Mp())
		indexBuild.ctr.buf = nil
	}
}

func (indexBuild *IndexBuild) Free(proc *process.Process, pipelineFailed bool, err error) {
	if indexBuild.ctr.buf != nil {
		indexBuild.ctr.buf.Clean(proc.Mp())
		indexBuild.ctr.buf = nil
	}
}

func (indexBuild *IndexBuild) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	return input, nil
}
