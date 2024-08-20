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
const defaultCacheBatchSize = 16

var _ vm.Operator = new(MergeOrder)

const (
	receiving = iota
	normalSending
	pickUpSending
)

type MergeOrder struct {
	ctr container

	OrderBySpecs []*plan.OrderBySpec

	vm.OperatorBase
}

func (mergeOrder *MergeOrder) GetOperatorBase() *vm.OperatorBase {
	return &mergeOrder.OperatorBase
}

func init() {
	reuse.CreatePool[MergeOrder](
		func() *MergeOrder {
			return &MergeOrder{}
		},
		func(a *MergeOrder) {
			*a = MergeOrder{}
		},
		reuse.DefaultOptions[MergeOrder]().
			WithEnableChecker(),
	)
}

func (mergeOrder MergeOrder) TypeName() string {
	return opName
}

func NewArgument() *MergeOrder {
	return reuse.Alloc[MergeOrder](nil)
}

func (mergeOrder *MergeOrder) Release() {
	if mergeOrder != nil {
		reuse.Free[MergeOrder](mergeOrder, nil)
	}
}

type container struct {
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

func (mergeOrder *MergeOrder) Reset(proc *process.Process, pipelineFailed bool, err error) {
	mergeOrder.cleanBatchAndCol(proc)
	ctr := &mergeOrder.ctr
	ctr.batchList = ctr.batchList[:0]
	ctr.orderCols = ctr.orderCols[:0]
	ctr.indexList = nil
	ctr.status = receiving

	for i := range ctr.executors {
		if ctr.executors[i] != nil {
			ctr.executors[i].ResetForNextQuery()
		}
	}
	if ctr.buf != nil {
		ctr.buf.CleanOnlyData()
	}
}

func (mergeOrder *MergeOrder) Free(proc *process.Process, pipelineFailed bool, err error) {
	mergeOrder.cleanBatchAndCol(proc)
	ctr := &mergeOrder.ctr
	ctr.batchList = nil
	ctr.orderCols = nil
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

}

func (mergeOrder *MergeOrder) cleanBatchAndCol(proc *process.Process) {
	mp := proc.Mp()
	ctr := &mergeOrder.ctr
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
}
