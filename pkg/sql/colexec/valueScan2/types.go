// Copyright 2024 Matrix Origin
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

package valueScan2

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
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

	Batchs     []*batch.Batch
	RowsetData *plan.RowsetData
	ColCount   int
	Uuid       []byte
}

type container struct {
	// nowIdx indicates which data should send to next operator now.
	nowIdx int
}

func (valueScan *ValueScan) Reset(proc *process.Process, pipelineFailed bool, err error) {
	valueScan.runningCtx.nowIdx = 0
	valueScan.doBatchClean(proc)
	valueScan.ResetProjection(proc)
	return
}

func (valueScan *ValueScan) Free(proc *process.Process, pipelineFailed bool, err error) {
	valueScan.FreeProjection(proc)
	return
}

func (valueScan *ValueScan) doBatchClean(proc *process.Process) {
	// If data was stored in the process, do not clean it.
	// process's free will clean them.
	if valueScan.dataInProcess {
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

// TypeName implement the `reuse.ReusableObject` interface.
func (ValueScan *ValueScan) TypeName() string {
	return thisOperator
}

func (valueScan *ValueScan) GetOperatorBase() *vm.OperatorBase {
	return &valueScan.OperatorBase
}

func (ValueScan *ValueScan) String(buf *bytes.Buffer) {
	buf.WriteString(thisOperator + ": value_scan")
}

func (ValueScan *ValueScan) OpType() vm.OpType {
	return vm.ValueScan
}
