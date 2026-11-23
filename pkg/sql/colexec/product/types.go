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

package product

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(Product)

const (
	Build = iota
	Probe
	End
)

type container struct {
	state       int
	buildBatIdx int
	buildRowIdx int
	rbat        *batch.Batch
	// rbatMP is the pool rbat was created with (the result view).
	rbatMP *mpool.MPool
	inBat  *batch.Batch
	mp     *message.JoinMap
}

const (
	productAllocationSiteResultData mpool.AllocationSite = iota + 94
	productAllocationSiteResultArea
	_ // reserved: was productAllocationSiteResultNulls
	_ // reserved: was productAllocationSiteResultGrouping
)

type Product struct {
	ctr        container
	Result     []colexec.ResultPos
	IsShuffle  bool
	JoinMapTag int32

	allocationAccount *mpool.AllocationAccount
	resultAllocation  *mpool.AccountBinding

	vm.OperatorBase
}

func (product *Product) GetOperatorBase() *vm.OperatorBase {
	return &product.OperatorBase
}

func (product *Product) SetAllocationAccount(
	account *mpool.AllocationAccount,
) error {
	if account == nil || account.Handle() == 0 {
		return mpool.ErrAllocationAccountInvalid
	}
	if product.allocationAccount != nil &&
		product.allocationAccount != account {
		return mpool.ErrAllocationAccountMismatch
	}
	if product.allocationAccount == account {
		return nil
	}
	selection, err := mpool.NewAccountBinding(
		account,
		mpool.AllocationOwnerHashBuild,
		productAllocationSiteResultData,
	)
	if err != nil {
		return err
	}
	product.allocationAccount = account
	product.resultAllocation = selection
	return nil
}

func (product *Product) ClearAllocationAccount(
	account *mpool.AllocationAccount,
) error {
	if product.allocationAccount == nil {
		return nil
	}
	if product.allocationAccount != account {
		return mpool.ErrAllocationAccountMismatch
	}
	if product.ctr.mp != nil || product.ctr.rbat != nil {
		return mpool.ErrAllocationAccountInvariant
	}
	product.allocationAccount = nil
	product.resultAllocation = nil
	return nil
}

func init() {
	reuse.CreatePool[Product](
		func() *Product {
			return &Product{}
		},
		func(a *Product) {
			*a = Product{}
		},
		reuse.DefaultOptions[Product]().
			WithEnableChecker(),
	)
}

func (product Product) TypeName() string {
	return opName
}

func NewArgument() *Product {
	return reuse.Alloc[Product](nil)
}

func (product *Product) Release() {
	if product != nil {
		reuse.Free[Product](product, nil)
	}
}

func (product *Product) Reset(proc *process.Process, pipelineFailed bool, err error) {
	product.ctr.cleanBatch(proc.Mp())
	product.ctr.state = Build
}

func (product *Product) Free(proc *process.Process, pipelineFailed bool, err error) {
	product.ctr.cleanBatch(proc.Mp())
}

func (ctr *container) cleanBatch(mp *mpool.MPool) {
	if ctr.rbat != nil {
		ctr.rbat.Clean(ctr.rbatMP)
		ctr.rbat = nil
		ctr.rbatMP = nil
	}
	if ctr.mp != nil {
		ctr.mp.Free()
		ctr.mp = nil
	}
	ctr.inBat = nil
	ctr.buildBatIdx = 0
	ctr.buildRowIdx = 0
}
