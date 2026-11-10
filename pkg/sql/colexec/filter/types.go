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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(Filter)

type Filter struct {
	ctr                      container
	FilterExprs              []*plan.Expr
	RuntimeFilterExprs       []*plan.Expr
	IsEnd                    bool
	IsAssert                 bool
	OwnsConstantCastWarnings bool

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
	dictionaryInputs          colexec.DictionaryInputFrame
	dictionaryBoundary        vm.DictionaryOutputState
	viewOutput                *batch.Batch
	selectedRows              []int64
	outputAllocation          *vector.AllocationAccountSelection
	buf                       *batch.Batch
	executors                 []colexec.ExpressionExecutor
	bs                        vector.FunctionParameterWrapper[bool]
	runtimeExecutors          []colexec.ExpressionExecutor
	allExecutors              []colexec.ExpressionExecutor // = executors + runtimeExecutor, do not free this executors
	subjectReuseAdmissionDone bool
}

func (filter *Filter) SetRuntimeFilterExprs(proc *process.Process, exes []*plan.Expr) (err error) {
	filter.RuntimeFilterExprs = exes
	return
}

func (filter *Filter) Reset(proc *process.Process, pipelineFailed bool, err error) {
	filter.ctr.resetExecutor()
	filter.ctr.cleanRuntimeExecutor()
	filter.RuntimeFilterExprs = nil
	if filter.ctr.outputAllocation != nil {
		filter.Free(proc, pipelineFailed, err)
	}
}

func (filter *Filter) Free(proc *process.Process, pipelineFailed bool, err error) {
	filter.ctr.dictionaryInputs.Free(proc.Mp())
	if filter.ctr.viewOutput != nil {
		filter.ctr.viewOutput.Clean(proc.Mp())
		filter.ctr.viewOutput = nil
	}
	filter.ctr.selectedRows = nil
	filter.ctr.cleanExecutor()
	filter.ctr.cleanRuntimeExecutor()
	filter.ctr.allExecutors = nil
	if filter.ctr.buf != nil {
		filter.ctr.buf.Clean(proc.Mp())
		filter.ctr.buf = nil
	}
	filter.RuntimeFilterExprs = nil
}

func (ctr *container) cleanExecutor() {
	for i := range ctr.executors {
		if ctr.executors[i] != nil {
			ctr.executors[i].Free()
		}
	}
	ctr.executors = nil
	ctr.subjectReuseAdmissionDone = false
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
