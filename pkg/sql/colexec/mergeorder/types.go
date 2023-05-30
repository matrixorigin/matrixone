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
	"github.com/matrixorigin/matrixone/pkg/compare"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type container struct {
	colexec.ReceiverOperator

	n     int               // result vector number
	poses []int32           // sorted list of attributes
	cmps  []compare.Compare // compare structures used to do sort work for attrs

	bat *batch.Batch // bat store the result of merge-order

	// some reused memory
	unionFlag                    []uint8
	compare0Index, compare1Index []int32
	finalSelectList              []int64

	// executors for order column.
	executorsForOrderList []colexec.ExpressionExecutor
}

type Argument struct {
	ctr *container          // ctr stores the attributes needn't do Serialization work
	Fs  []*plan.OrderBySpec // Fields store the order information
}

func (arg *Argument) Free(proc *process.Process, pipelineFailed bool) {
	ctr := arg.ctr
	if ctr != nil {
		mp := proc.Mp()
		ctr.cleanBatch(mp)
		ctr.cleanExecutors()
		ctr.FreeMergeTypeOperator(pipelineFailed)
	}
}

func (ctr *container) cleanBatch(mp *mpool.MPool) {
	if ctr.bat != nil {
		ctr.bat.Clean(mp)
		ctr.bat = nil
	}
}

func (ctr *container) cleanExecutors() {
	for i := range ctr.executorsForOrderList {
		ctr.executorsForOrderList[i].Free()
	}
}
