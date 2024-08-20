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

package order

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(Order)

const maxBatchSizeToSort = 64 * mpool.MB

type Order struct {
	ctr container

	OrderBySpec []*plan.OrderBySpec

	vm.OperatorBase
}

func (order *Order) GetOperatorBase() *vm.OperatorBase {
	return &order.OperatorBase
}

func init() {
	reuse.CreatePool[Order](
		func() *Order {
			return &Order{}
		},
		func(a *Order) {
			*a = Order{}
		},
		reuse.DefaultOptions[Order]().
			WithEnableChecker(),
	)
}

func (order Order) TypeName() string {
	return opName
}

func NewArgument() *Order {
	return reuse.Alloc[Order](nil)
}

func (order *Order) Release() {
	if order != nil {
		reuse.Free[Order](order, nil)
	}
}

type container struct {
	state          vm.CtrState
	batWaitForSort *batch.Batch

	desc      []bool // ds[i] == true: the attrs[i] are in descending order
	nullsLast []bool

	sortExprExecutor []colexec.ExpressionExecutor
	sortVectors      []*vector.Vector
	resultOrderList  []int64
}

func (order *Order) Reset(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &order.ctr
	if ctr.batWaitForSort != nil {
		if ctr.batWaitForSort.RowCount() > colexec.DefaultBatchSize {
			ctr.batWaitForSort.Clean(proc.Mp())
			ctr.batWaitForSort = nil
		} else {
			ctr.batWaitForSort.CleanOnlyData()
		}
	}
	ctr.state = vm.Build
	for i := range ctr.sortExprExecutor {
		if ctr.sortExprExecutor[i] != nil {
			ctr.sortExprExecutor[i].ResetForNextQuery()
		}
	}
	ctr.resultOrderList = nil
}

func (order *Order) Free(proc *process.Process, _ bool, err error) {
	order.cleanBatch(proc)
	ctr := &order.ctr
	for i := range ctr.sortExprExecutor {
		if ctr.sortExprExecutor[i] != nil {
			ctr.sortExprExecutor[i].Free()
		}
	}
	ctr.sortExprExecutor = nil
	ctr.resultOrderList = nil
}

func (order *Order) cleanBatch(proc *process.Process) {
	//big memory, just clean
	ctr := &order.ctr
	if ctr.batWaitForSort != nil {
		ctr.batWaitForSort.Clean(proc.Mp())
		ctr.batWaitForSort = nil
	}
}
