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

package mergeorder

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/compare"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const maxBatchSizeToSend = 64 * mpool.MB

var _ vm.Operator = new(Argument)

const (
	receiving = iota
	normalSending
	pickUpSending
)

type Argument struct {
	ctr *container

	OrderBySpecs []*plan.OrderBySpec

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
	colexec.ReceiverOperator

	// operator status
	status int

	// batchList is the data structure to store the all the received batches
	batchList []*batch.Batch
	orderCols [][]*vector.Vector
	// indexList[i] = k means the number of rows before k in batchList[i] has been merged and send.
	indexList []int64

	// expression executors for order columns.
	executors []colexec.ExpressionExecutor
	compares  []compare.Compare

	buf *batch.Batch
}

func (arg *Argument) Reset(proc *process.Process, pipelineFailed bool, err error) {
	arg.Free(proc, pipelineFailed, err)
}

func (arg *Argument) Free(proc *process.Process, pipelineFailed bool, err error) {
	if ctr := arg.ctr; ctr != nil {
		mp := proc.Mp()
		for i := range ctr.batchList {
			if ctr.batchList[i] != nil {
				ctr.batchList[i].Clean(mp)
			}
		}
		for i := range ctr.orderCols {
			if ctr.orderCols[i] != nil {
				for j := range ctr.orderCols[i] {
					if ctr.orderCols[i][j] != nil {
						ctr.orderCols[i][j].Free(mp)
					}
				}
			}
		}
		for i := range ctr.executors {
			if ctr.executors[i] != nil {
				ctr.executors[i].Free()
			}
		}
		ctr.executors = nil

		if ctr.buf != nil {
			ctr.buf.Clean(proc.Mp())
			ctr.buf = nil
		}

		arg.ctr = nil
	}
}
