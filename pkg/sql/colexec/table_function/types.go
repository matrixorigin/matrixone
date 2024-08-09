// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table_function

import (
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(TableFunction)

const (
	dataProducing = iota
	dataFinished
)

type TableFunction struct {
	ctr container

	Rets     []*plan.ColDef
	Args     []*plan.Expr
	Attrs    []string
	Params   []byte
	FuncName string

	vm.OperatorBase
}

func (tableFunction *TableFunction) GetOperatorBase() *vm.OperatorBase {
	return &tableFunction.OperatorBase
}

func init() {
	reuse.CreatePool[TableFunction](
		func() *TableFunction {
			return &TableFunction{}
		},
		func(a *TableFunction) {
			*a = TableFunction{}
		},
		reuse.DefaultOptions[TableFunction]().
			WithEnableChecker(),
	)
}

func (tableFunction TableFunction) TypeName() string {
	return opName
}

func NewArgument() *TableFunction {
	return reuse.Alloc[TableFunction](nil)
}

func (tableFunction *TableFunction) Release() {
	if tableFunction != nil {
		reuse.Free[TableFunction](tableFunction, nil)
	}
}

type tvfState interface {
	reset(tf *TableFunction, proc *process.Process)
	start(tf *TableFunction, proc *process.Process, nthRow int) error
	call(tf *TableFunction, proc *process.Process) (vm.CallResult, error)
	free(tf *TableFunction, proc *process.Process, pipelineFailed bool, err error)
}

type container struct {
	// schema
	retSchema        []types.Type
	executorsForArgs []colexec.ExpressionExecutor

	// next row cursor.   inputBatch holds batch from children cross
	// calls, we do not own it and do not free it.
	nextRow    int
	inputBatch *batch.Batch
	// hold arg vectors, we do not own them and do not free them.
	argVecs []*vector.Vector

	// opaque, each table function has its own state.
	state tvfState
}

func (tableFunction *TableFunction) Reset(proc *process.Process, pipelineFailed bool, err error) {
	tableFunction.ctr.nextRow = 0
	tableFunction.ctr.inputBatch = nil
	for i := range tableFunction.ctr.executorsForArgs {
		if tableFunction.ctr.executorsForArgs[i] != nil {
			tableFunction.ctr.argVecs[i] = nil
			tableFunction.ctr.executorsForArgs[i].ResetForNextQuery()
		}
	}
	tableFunction.ctr.state.reset(tableFunction, proc)
}

func (tableFunction *TableFunction) Free(proc *process.Process, pipelineFailed bool, err error) {
	tableFunction.ctr.cleanExecutors()
	tableFunction.ctr.state.free(tableFunction, proc, pipelineFailed, err)
}

func (ctr *container) cleanExecutors() {
	for i := range ctr.executorsForArgs {
		if ctr.executorsForArgs[i] != nil {
			ctr.argVecs[i] = nil
			ctr.executorsForArgs[i].Free()
		}
	}
	ctr.executorsForArgs = nil
}

// simple state for table function that only produce one batch.
// but we do not know how to generate the batch, so leave start to each impl.
type simpleOneBatchState struct {
	called bool
	// result batch, we own it.
	batch *batch.Batch
}

func (s *simpleOneBatchState) reset(tf *TableFunction, proc *process.Process) {
	if s.batch != nil {
		s.batch.CleanOnlyData()
	}
}

func (s *simpleOneBatchState) free(tf *TableFunction, proc *process.Process, pipelineFailed bool, err error) {
	if s.batch != nil {
		s.batch.Clean(proc.Mp())
	}
}

func (s *simpleOneBatchState) call(tf *TableFunction, proc *process.Process) (vm.CallResult, error) {
	if s.called {
		return vm.CancelResult, nil
	}
	s.called = true
	if s.batch.RowCount() == 0 {
		return vm.CancelResult, nil
	}
	return vm.CallResult{Status: vm.ExecNext, Batch: s.batch}, nil
}

func (s *simpleOneBatchState) startPreamble(tf *TableFunction, proc *process.Process, nthRow int) {
	s.called = false
	if s.batch == nil {
		s.batch = tf.createResultBatch()
	} else {
		s.batch.CleanOnlyData()
	}
}
