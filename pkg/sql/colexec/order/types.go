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

var _ vm.Operator = new(Argument)

const maxBatchSizeToSort = 64 * mpool.MB

type Argument struct {
	ctr *container

	OrderBySpec []*plan.OrderBySpec

	vm.OperatorBase
}

func (arg *Argument) GetOperatorBase() *vm.OperatorBase {
	return &arg.OperatorBase
}

func init() {
	reuse.CreatePool[Argument](
		func() *Argument {
			return &Argument{}
		},
		func(a *Argument) {
			*a = Argument{}
		},
		reuse.DefaultOptions[Argument]().
			WithEnableChecker(),
	)
}

func (arg Argument) TypeName() string {
	return argName
}

func NewArgument() *Argument {
	return reuse.Alloc[Argument](nil)
}

func (arg *Argument) Release() {
	if arg != nil {
		reuse.Free[Argument](arg, nil)
	}
}

type container struct {
	state          vm.CtrState
	batWaitForSort *batch.Batch
	rbat           *batch.Batch

	desc      []bool // ds[i] == true: the attrs[i] are in descending order
	nullsLast []bool

	sortExprExecutor []colexec.ExpressionExecutor
	sortVectors      []*vector.Vector
	resultOrderList  []int64
	flatFn           []func(v, w *vector.Vector) error // method to flat const vector
}

func (arg *Argument) Free(proc *process.Process, _ bool, err error) {
	ctr := arg.ctr
	if ctr != nil {
		for i := range ctr.sortExprExecutor {
			if ctr.sortExprExecutor[i] != nil {
				ctr.sortExprExecutor[i].Free()
			}
		}

		ctr.sortExprExecutor = nil

		if ctr.batWaitForSort != nil {
			ctr.batWaitForSort.Clean(proc.Mp())
			ctr.batWaitForSort = nil
		}
		if ctr.rbat != nil {
			ctr.rbat.Clean(proc.Mp())
			ctr.rbat = nil
		}
		ctr.resultOrderList = nil

		arg.ctr = nil
	}
}
