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

package partition

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/compare"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(Partition)

const (
	receive = iota
	eval
)

type Partition struct {
	ctr container

	OrderBySpecs []*plan.OrderBySpec

	vm.OperatorBase
}

func (partition *Partition) GetOperatorBase() *vm.OperatorBase {
	return &partition.OperatorBase
}

func init() {
	reuse.CreatePool[Partition](
		func() *Partition {
			return &Partition{}
		},
		func(a *Partition) {
			*a = Partition{}
		},
		reuse.DefaultOptions[Partition]().
			WithEnableChecker(),
	)
}

func (partition Partition) TypeName() string {
	return opName
}

func NewArgument() *Partition {
	return reuse.Alloc[Partition](nil)
}

func (partition *Partition) Release() {
	if partition != nil {
		reuse.Free[Partition](partition, nil)
	}
}

type container struct {

	// operator status
	status int

	// batchList is the data structure to store the all the received batches
	batchList []*batch.Batch
	orderCols [][]*vector.Vector
	i         int
	// indexList[i] = k means the number of rows before k in batchList[i] has been merged and send.
	indexList []int64

	// expression executors for order columns.
	executors []colexec.ExpressionExecutor
	compares  []compare.Compare

	buf *batch.Batch
}

func (partition *Partition) Reset(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &partition.ctr

	ctr.resetExes()
	if ctr.buf != nil {
		ctr.buf.CleanOnlyData()
	}
	ctr.resetParam()
}

func (partition *Partition) Free(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &partition.ctr

	ctr.freeExes()
	ctr.freeBatch(proc.Mp())
	ctr.freeVector(proc.Mp())
}

func (ctr *container) freeBatch(mp *mpool.MPool) {
	if ctr.buf != nil {
		ctr.buf.Clean(mp)
		ctr.buf = nil
	}
	ctr.batchList = nil
}

func (ctr *container) freeVector(mp *mpool.MPool) {
	for i := range ctr.orderCols {
		if ctr.orderCols[i] != nil {
			for j := range ctr.orderCols[i] {
				if ctr.orderCols[i][j] != nil {
					ctr.orderCols[i][j].Free(mp)
				}
			}
		}
	}
	ctr.orderCols = nil
}

func (ctr *container) freeExes() {
	for i := range ctr.executors {
		if ctr.executors[i] != nil {
			ctr.executors[i].Free()
		}
	}
	ctr.executors = nil
}

func (ctr *container) resetExes() {
	for i := range ctr.executors {
		if ctr.executors[i] != nil {
			ctr.executors[i].ResetForNextQuery()
		}
	}
}

func (ctr *container) resetParam() {
	ctr.i = 0
	ctr.indexList = ctr.indexList[:0]
	ctr.status = receive
	ctr.batchList = ctr.batchList[:0]
}
