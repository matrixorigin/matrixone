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

package window

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type evalVector struct {
	vec      *vector.Vector
	executor colexec.ExpressionExecutor
}

type container struct {
	colexec.ReceiverOperator

	bat *batch.Batch

	desc      []bool
	nullsLast []bool
	orderVecs []evalVector

	ps      []int64 // index of partition by
	os      []int64 // Sorted partitions
	aggVecs []evalVector
}

type Argument struct {
	ctr         *container
	WinSpecList []*plan.Expr
	// sort and partition
	Fs []*plan.OrderBySpec
	// agg func
	Types []types.Type
	Aggs  []agg.Aggregate
}

func (arg *Argument) Free(proc *process.Process, pipelineFailed bool) {
	ctr := arg.ctr
	if ctr != nil {
		mp := proc.Mp()
		ctr.FreeMergeTypeOperator(pipelineFailed)
		ctr.cleanBatch(mp)
		ctr.cleanAggVectors(mp)
	}
}

func (ctr *container) cleanBatch(mp *mpool.MPool) {
	if ctr.bat != nil {
		ctr.bat.Clean(mp)
		ctr.bat = nil
	}
}

func (ctr *container) cleanOrderVectors(_ *mpool.MPool) {
	for i := range ctr.orderVecs {
		if ctr.orderVecs[i].executor != nil {
			ctr.orderVecs[i].executor.Free()
		}
		ctr.orderVecs[i].vec = nil
	}
}

func (ctr *container) cleanAggVectors(_ *mpool.MPool) {
	for i := range ctr.aggVecs {
		if ctr.aggVecs[i].executor != nil {
			ctr.aggVecs[i].executor.Free()
		}
		ctr.aggVecs[i].vec = nil
	}
}
