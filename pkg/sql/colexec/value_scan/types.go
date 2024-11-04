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
	// if dataInProcess is true,
	// this means all the batches were saved other place.
	// there is no need clean them after operator done.
	dataInProcess bool
	ColCount      int
	NodeType      plan2.Node_NodeType

	Batchs        []*batch.Batch
	RowsetData    *plan2.RowsetData
	ExprExecLists [][]colexec.ExpressionExecutor
}

type container struct {
	// nowIdx indicates which data should send to next operator now.
	nowIdx int
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

func NewValueScanFromProcess() *ValueScan {
	vs := getFromReusePool()
	vs.dataInProcess = true
	return vs
}

func NewValueScanFromItSelf() *ValueScan {
	vs := getFromReusePool()
	vs.dataInProcess = false
	return vs
}

func getFromReusePool() *ValueScan {
	return reuse.Alloc[ValueScan](nil)
}

func (valueScan *ValueScan) Release() {
	if valueScan != nil {
		if valueScan.dataInProcess {
			for i := range valueScan.Batchs {
				valueScan.Batchs[i] = nil
			}
			valueScan.Batchs = valueScan.Batchs[:0]
		} else {
			valueScan.Batchs = nil
		}

		reuse.Free[ValueScan](valueScan, nil)
	}
}

func (valueScan *ValueScan) Reset(proc *process.Process, _ bool, _ error) {
	valueScan.runningCtx.nowIdx = 0
	// valueScan.doBatchClean(proc)
	if valueScan.Batchs != nil {
		valueScan.resetBatchs()
	}
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
	// valueScan.doBatchClean(proc)
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

func (valueScan *ValueScan) cleanBatchs(proc *process.Process) {
	for _, bat := range valueScan.Batchs {
		if bat != nil {
			bat.Clean(proc.Mp())
		}
	}
	valueScan.Batchs = nil
}

func (valueScan *ValueScan) doBatchClean(proc *process.Process) {
	// If data was stored in the process, do not clean it.
	// process's free will clean them.
	if valueScan.dataInProcess {
		valueScan.Batchs = valueScan.Batchs[:0]
		return
	}

	for i := range valueScan.Batchs {
		if valueScan.Batchs[i] != nil {
			valueScan.Batchs[i].Clean(proc.Mp())
		}
		valueScan.Batchs[i] = nil
	}
	valueScan.Batchs = nil
}

func (valueScan *ValueScan) resetBatchs() {
	for _, bat := range valueScan.Batchs {
		if bat != nil {
			for _, vec := range bat.Vecs {
				vec.CleanOnlyData()
			}
		}
	}
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
