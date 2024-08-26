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

package connector

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "connector"

func (connector *Connector) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": pipe connector")
}

func (connector *Connector) Prepare(_ *process.Process) error {
	connector.OpAnalyzer = process.NewAnalyzer(connector.GetIdx(), connector.IsFirst, connector.IsLast, "connector")
	return nil
}

func (connector *Connector) OpType() vm.OpType {
	return vm.Connector
}

func (connector *Connector) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	reg := connector.Reg

	result, err := connector.Children[0].Call(proc)
	if err != nil {
		return result, err
	}

	if result.Batch == nil {
		result.Status = vm.ExecStop
		return result, nil
	}

	if result.Batch.IsEmpty() {
		result.Batch = batch.EmptyBatch
		return result, nil
	}

	sendBat, err := result.Batch.Dup(proc.GetMPool())
	if err != nil {
		return vm.CancelResult, nil
	}
	sendBat.Aggs = result.Batch.Aggs
	result.Batch.Aggs = nil
	sendBat.SetRowCount(result.Batch.RowCount())

	// there is no need to log anything here.
	// because the context is already canceled means the pipeline closed normally.
	select {
	case <-proc.Ctx.Done():
		proc.PutBatch(sendBat)
		result.Status = vm.ExecStop
		return result, nil

	case <-reg.Ctx.Done():
		proc.PutBatch(sendBat)
		result.Status = vm.ExecStop
		return result, nil

	case reg.Ch <- process.NewRegMsg(sendBat):
		return result, nil
	}
}
