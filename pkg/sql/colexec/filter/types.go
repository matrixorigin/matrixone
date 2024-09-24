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
	ctr   container
	E     *plan.Expr
	IsEnd bool

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
		filter.ctr.cleanRuntimeExecutor()
		reuse.Free[Filter](filter, nil)
	}
}

type container struct {
	buf       *batch.Batch
	executors []colexec.ExpressionExecutor

	runtimeExecutors []colexec.ExpressionExecutor
	allExecutors     []colexec.ExpressionExecutor // = executors + runtimeExecutor, do not free this executors
}

func (filter *Filter) SetRuntimeExpr(proc *process.Process, exes []*plan.Expr) (err error) {
	filter.ctr.cleanRuntimeExecutor()
	filter.ctr.runtimeExecutors, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, exes)
	return
}

func (filter *Filter) Reset(proc *process.Process, pipelineFailed bool, err error) {
	filter.ctr.resetExecutor()
	filter.ctr.cleanRuntimeExecutor()
}

func (filter *Filter) Free(proc *process.Process, pipelineFailed bool, err error) {
	filter.ctr.cleanExecutor()
	filter.ctr.cleanRuntimeExecutor()
	filter.ctr.allExecutors = nil
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

func (ctr *container) cleanRuntimeExecutor() {
	for i := range ctr.runtimeExecutors {
		if ctr.runtimeExecutors[i] != nil {
			ctr.runtimeExecutors[i].Free()
		}
	}
	ctr.runtimeExecutors = nil
}

func (ctr *container) resetExecutor() {
	for i := range ctr.executors {
		if ctr.executors[i] != nil {
			ctr.executors[i].ResetForNextQuery()
		}
	}
}
