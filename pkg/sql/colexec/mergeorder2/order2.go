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

package mergeorder2

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type Argument struct {
	ctr *container

	OrderInformation []*plan.OrderBySpec
}

type container struct {
	colexec.ReceiverOperator

	// receiveOver and sendOver are the flags to indicate whether
	// this operator has received all the data from the child pipelines.
	// and send all the data to the next operator.
	receiveOver bool
	sendOver    bool

	// batch list and order list to store the intermediate data.
	batchList []*batch.Batch
	orderList []itemPosition

	// expression executors for order columns.
	executors []colexec.ExpressionExecutor
}

type itemPosition struct {
	batIdx int
	rowIdx int
}

func (ctr *container) mergeAndSort(bat *batch.Batch) error {
	ctr.batchList = append(ctr.batchList, bat)
	// IF ONLY ONE BATCH, DO NOT SORT.
	if len(ctr.batchList) == 1 {

		return nil
	}

	return nil
}

func (ctr *container) pickAndSend(proc *process.Process) error {
	proc.SetInputBatch(batch.EmptyBatch)

	if len(ctr.orderList) == 0 {
		ctr.sendOver = true
	}
	return nil
}

func Prepare(proc *process.Process, arg any) (err error) {
	return nil
}

func Call(idx int, proc *process.Process, arg any, isFirst bool, isLast bool) (bool, error) {
	ap := arg.(*Argument)
	ctr := ap.ctr

	anal := proc.GetAnalyze(idx)
	anal.Start()
	defer anal.Stop()

	// get batch from receiver and do merge sort.
	// but do not really sort, just get the order.
	if !ctr.receiveOver {
		for {
			bat, end, err := ctr.ReceiveFromAllRegs(anal)
			if err != nil {
				return false, err
			}
			if end {
				ctr.receiveOver = true
				break
			}
			if err = ctr.mergeAndSort(bat); err != nil {
				return false, err
			}
		}
	}

	// then use the order to get the data from the batch.
	err := ctr.pickAndSend(proc)
	return ctr.sendOver, err
}
