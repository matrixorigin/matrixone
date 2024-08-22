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

package filter

import (
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(Filter)

type Filter struct {
	ctr     container
	E       *plan.Expr
	exeExpr *plan.Expr
	IsEnd   bool

	vm.OperatorBase
}

func (filter *Filter) GetOperatorBase() *vm.OperatorBase {
	return &filter.OperatorBase
}

func init() {
	reuse.CreatePool[Filter](
		func() *Filter {
			return &Filter{}
		},
		func(a *Filter) {
			*a = Filter{}
		},
		reuse.DefaultOptions[Filter]().
			WithEnableChecker(),
	)
}

func (filter Filter) TypeName() string {
	return opName
}

func NewArgument() *Filter {
	return reuse.Alloc[Filter](nil)
}

func (filter *Filter) Release() {
	if filter != nil {
		reuse.Free[Filter](filter, nil)
	}
}

type container struct {
	buf       *batch.Batch
	executors []colexec.ExpressionExecutor
}

func (filter *Filter) SetExeExpr(e *plan.Expr) {
	filter.exeExpr = e
}

func (filter *Filter) GetExeExpr() *plan.Expr {
	return filter.exeExpr
}

func (filter *Filter) Reset(proc *process.Process, pipelineFailed bool, err error) {
	filter.ctr.cleanExecutor() //todo need fix performance issue for executor mem reuse
	filter.exeExpr = nil
}

func (filter *Filter) Free(proc *process.Process, pipelineFailed bool, err error) {
	filter.ctr.cleanExecutor()
	if filter.ctr.buf != nil {
		filter.ctr.buf.Clean(proc.Mp())
	}
}

func (ctr *container) cleanExecutor() {
	for i := range ctr.executors {
		if ctr.executors[i] != nil {
			ctr.executors[i].Free()
		}
	}
	ctr.executors = nil
}

// func (ctr *container) resetExecutor() {
// 	for i := range ctr.executors {
// 		if ctr.executors[i] != nil {
// 			ctr.executors[i].ResetForNextQuery()
// 		}
// 	}
// 	ctr.executors = nil
// }
