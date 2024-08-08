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
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "value_scan"

func (valueScan *ValueScan) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": value_scan ")
}

func (valueScan *ValueScan) OpType() vm.OpType {
	return vm.ValueScan
}

func (valueScan *ValueScan) Prepare(proc *process.Process) (err error) {
	//@todo need move make batchs function from Scope.run to value_scan.Prepare
	err = valueScan.PrepareProjection(proc)
	return
}

func (valueScan *ValueScan) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	anal := proc.GetAnalyze(valueScan.GetIdx(), valueScan.GetParallelIdx(), valueScan.GetParallelMajor())
	anal.Start()
	defer func() {
		anal.Stop()
	}()

	result := vm.NewCallResult()
	if valueScan.ctr.idx < len(valueScan.Batchs) {
		result.Batch = valueScan.Batchs[valueScan.ctr.idx]
		if valueScan.ctr.idx > 0 {
			valueScan.Batchs[valueScan.ctr.idx-1].Clean(proc.GetMPool())
			valueScan.Batchs[valueScan.ctr.idx-1] = nil
		}
		valueScan.ctr.idx += 1
	}
	anal.Input(result.Batch, valueScan.IsFirst)
	var err error
	result.Batch, err = valueScan.EvalProjection(result.Batch, proc)

	return result, err

}
