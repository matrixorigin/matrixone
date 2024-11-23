// Copyright 2021-2023 Matrix Origin
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

package value_scan

import (
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	plan2 "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const thisOperator = "value_scan"

type ValueScan struct {
	vm.OperatorBase
	colexec.Projection

	runningCtx container

	ColCount int
	NodeType plan2.Node_NodeType

	Batchs        []*batch.Batch
	RowsetData    *plan2.RowsetData
	ExprExecLists [][]colexec.ExpressionExecutor
}

type container struct {
	// nowIdx indicates which data should send to next operator now.
	nowIdx int
	start  int
	end    int
}

func init() {
	reuse.CreatePool[ValueScan](
		func() *ValueScan {
			return &ValueScan{}
		},
		func(a *ValueScan) {
			*a = ValueScan{}
		},
		reuse.DefaultOptions[ValueScan]().
			WithEnableChecker(),
	)
}

func NewArgument() *ValueScan {
	return reuse.Alloc[ValueScan](nil)
}

func (valueScan *ValueScan) Release() {
	if valueScan != nil {
		reuse.Free[ValueScan](valueScan, nil)
	}
}

func (valueScan *ValueScan) Reset(proc *process.Process, _ bool, _ error) {
	valueScan.runningCtx.nowIdx = 0
	valueScan.runningCtx.start = 0
	valueScan.runningCtx.end = 0

	//for prepare stmt, valuescan batch vecs do not need to reset, when next execute, prepare just copy data to vecs, length is same to last execute
	for i := 0; i < valueScan.ColCount; i++ {
		exprExecList := valueScan.ExprExecLists[i]
		for _, expr := range exprExecList {
			expr.ResetForNextQuery()
		}
	}
	valueScan.ResetProjection(proc)
}

func (valueScan *ValueScan) Free(proc *process.Process, _ bool, _ error) {
	valueScan.FreeProjection(proc)
	if valueScan.Batchs != nil {
		valueScan.cleanBatchs(proc)
	}
	for i := range valueScan.ExprExecLists {
		exprExecList := valueScan.ExprExecLists[i]
		for i, expr := range exprExecList {
			if expr != nil {
				expr.Free()
				exprExecList[i] = nil
			}
		}
	}
}

func (valueScan *ValueScan) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	var err error
	batch := input
	if valueScan.ProjectList != nil {
		batch, err = valueScan.EvalProjection(input, proc)
	}
	return batch, err
}

func (valueScan *ValueScan) cleanBatchs(proc *process.Process) {
	for _, bat := range valueScan.Batchs {
		if bat != nil {
			bat.Clean(proc.Mp())
		}
	}
	valueScan.Batchs = nil
}

// TypeName implement the `reuse.ReusableObject` interface.
func (valueScan ValueScan) TypeName() string {
	return thisOperator
}

func (valueScan *ValueScan) GetOperatorBase() *vm.OperatorBase {
	return &valueScan.OperatorBase
}

func (valueScan *ValueScan) OpType() vm.OpType {
	return vm.ValueScan
}
