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
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(Argument)

type evalVector struct {
	vec      *vector.Vector
	executor colexec.ExpressionExecutor
}

const (
	receive = iota
	eval
	done
	receiveAll
)

type container struct {
	colexec.ReceiverOperator
	status int

	bat *batch.Batch

	desc      []bool
	nullsLast []bool
	orderVecs []evalVector
	sels      []int64

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

func (arg *Argument) Free(proc *process.Process, pipelineFailed bool, err error) {
	ctr := arg.ctr
	if ctr != nil {
		mp := proc.Mp()
		ctr.FreeMergeTypeOperator(pipelineFailed)
		ctr.cleanBatch(mp)
		ctr.cleanAggVectors(mp)
		ctr.cleanOrderVectors(mp)
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
