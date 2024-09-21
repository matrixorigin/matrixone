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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(Window)

const (
	receive = iota
	eval
	done
	receiveAll
)

type container struct {
	status int

	bat *batch.Batch

	desc      []bool
	nullsLast []bool
	orderVecs []group.ExprEvalVector
	sels      []int64

	ps      []int64 // index of partition by
	os      []int64 // Sorted partitions
	aggVecs []group.ExprEvalVector

	vec  *vector.Vector
	rBat *batch.Batch
}

type Window struct {
	ctr         container
	WinSpecList []*plan.Expr
	// sort and partition
	Fs []*plan.OrderBySpec
	// agg func
	Types []types.Type
	Aggs  []aggexec.AggFuncExecExpression

	vm.OperatorBase
}

func (window *Window) GetOperatorBase() *vm.OperatorBase {
	return &window.OperatorBase
}

func init() {
	reuse.CreatePool[Window](
		func() *Window {
			return &Window{}
		},
		func(a *Window) {
			*a = Window{}
		},
		reuse.DefaultOptions[Window]().
			WithEnableChecker(),
	)
}

func (window Window) TypeName() string {
	return opName
}

func NewArgument() *Window {
	return reuse.Alloc[Window](nil)
}

func (window *Window) Release() {
	if window != nil {
		reuse.Free[Window](window, nil)
	}
}

func (window *Window) Reset(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &window.ctr

	ctr.resetParam()
	ctr.resetVectors()
	if ctr.bat != nil {
		ctr.bat.CleanOnlyData()
	}
	// It needs to free, because the result of agg eval is not reuse the vector
	if ctr.vec != nil {
		ctr.vec.Free(proc.Mp())
		ctr.vec = nil
	}
}

func (window *Window) Free(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &window.ctr

	ctr.freeBatch(proc.Mp())
	ctr.freeExes()
	ctr.freeVector(proc.Mp())
}

func (ctr *container) resetParam() {
	ctr.status = receive
	ctr.desc = nil
	ctr.nullsLast = nil
	ctr.sels = nil
	ctr.ps = nil
	ctr.os = nil
}

func (ctr *container) resetVectors() {
	for i := range ctr.orderVecs {
		ctr.orderVecs[i].ResetForNextQuery()
	}

	for i := range ctr.aggVecs {
		ctr.aggVecs[i].ResetForNextQuery()
	}
}

func (ctr *container) freeBatch(mp *mpool.MPool) {
	if ctr.bat != nil {
		ctr.bat.Clean(mp)
		ctr.bat = nil
	}
}

func (ctr *container) freeAggFun() {
	if ctr.bat != nil {
		for _, a := range ctr.bat.Aggs {
			if a != nil {
				a.Free()
			}
		}
		ctr.bat.Aggs = nil
	}
}

func (ctr *container) freeExes() {
	for i := range ctr.orderVecs {
		ctr.orderVecs[i].Free()
	}

	for i := range ctr.aggVecs {
		ctr.aggVecs[i].Free()
	}
}

func (ctr *container) freeVector(mp *mpool.MPool) {
	for _, e := range ctr.orderVecs {
		for _, vec := range e.Vec {
			if vec != nil {
				vec.Free(mp)
			}
		}
	}

	for _, e := range ctr.aggVecs {
		for _, vec := range e.Vec {
			if vec != nil {
				vec.Free(mp)
			}
		}
	}

	if ctr.vec != nil {
		ctr.vec.Free(mp)
	}
}
