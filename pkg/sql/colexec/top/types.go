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

package top

import (
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/compare"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(Top)

type container struct {
	n     int // result vector number
	state vm.CtrState
	sels  []int64
	poses []int32 // sorted list of attributes
	cmps  []compare.Compare

	limit         uint64
	limitExecutor colexec.ExpressionExecutor

	executorsForOrderColumn []colexec.ExpressionExecutor
	desc                    bool
	topValueZM              objectio.ZoneMap
	bat                     *batch.Batch
	buildBat                *batch.Batch //temp batch, do not need free or reset
}

type Top struct {
	Limit       *plan.Expr
	TopValueTag int32
	ctr         container
	Fs          []*plan.OrderBySpec

	vm.OperatorBase
}

func (top *Top) GetOperatorBase() *vm.OperatorBase {
	return &top.OperatorBase
}

func init() {
	reuse.CreatePool(
		func() *Top {
			return &Top{}
		},
		func(a *Top) {
			*a = Top{}
		},
		reuse.DefaultOptions[Top]().
			WithEnableChecker(),
	)
}

func (top Top) TypeName() string {
	return opName
}

func NewArgument() *Top {
	return reuse.Alloc[Top](nil)
}

func (top *Top) WithLimit(limit *plan.Expr) *Top {
	top.Limit = limit
	return top
}

func (top *Top) WithFs(fs []*plan.OrderBySpec) *Top {
	top.Fs = fs
	return top
}

func (top *Top) Release() {
	if top != nil {
		reuse.Free(top, nil)
	}
}

func (top *Top) Reset(proc *process.Process, pipelineFailed bool, err error) {
	top.ctr.reset()
}

func (top *Top) Free(proc *process.Process, pipelineFailed bool, err error) {
	top.ctr.free(proc)
}

func (ctr *container) reset() {

	ctr.n = 0
	ctr.state = 0
	ctr.sels = nil
	ctr.poses = nil
	ctr.cmps = nil

	ctr.limit = 0
	if ctr.limitExecutor != nil {
		ctr.limitExecutor.ResetForNextQuery()
	}

	for _, executor := range ctr.executorsForOrderColumn {
		if executor != nil {
			executor.ResetForNextQuery()
		}
	}
	ctr.desc = false
	ctr.topValueZM = nil
	if ctr.bat != nil {
		ctr.bat.CleanOnlyData()
	}

}

func (ctr *container) free(proc *process.Process) {
	if ctr.bat != nil {
		ctr.bat.Clean(proc.Mp())
	}
	for _, executor := range ctr.executorsForOrderColumn {
		if executor != nil {
			executor.Free()
		}
	}
	if ctr.limitExecutor != nil {
		ctr.limitExecutor.Free()
	}
}

func (ctr *container) compare(vi, vj int, i, j int64) int {
	for _, pos := range ctr.poses {
		if r := ctr.cmps[pos].Compare(vi, vj, i, j); r != 0 {
			return r
		}
	}
	return 0
}

func (ctr *container) Len() int {
	return len(ctr.sels)
}

func (ctr *container) Less(i, j int) bool {
	return ctr.compare(0, 0, ctr.sels[i], ctr.sels[j]) > 0
}

func (ctr *container) Swap(i, j int) {
	ctr.sels[i], ctr.sels[j] = ctr.sels[j], ctr.sels[i]
}

func (ctr *container) Push(x interface{}) {
	ctr.sels = append(ctr.sels, x.(int64))
}

func (ctr *container) Pop() interface{} {
	n := len(ctr.sels) - 1
	x := ctr.sels[n]
	ctr.sels = ctr.sels[:n]
	return x
}
