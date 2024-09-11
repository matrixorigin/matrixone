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
	"github.com/matrixorigin/matrixone/pkg/container/pSpool"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "connector"

func (connector *Connector) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": pipe connector")
}

func (connector *Connector) Prepare(proc *process.Process) error {
	if connector.ctr.sp == nil {
		connector.ctr.sp = pSpool.InitMyPipelineSpool(proc.Mp(), 1)
	}

	if connector.OpAnalyzer == nil {
		connector.OpAnalyzer = process.NewAnalyzer(connector.GetIdx(), connector.IsFirst, connector.IsLast, "connector")
	} else {
		connector.OpAnalyzer.Reset()
	}
	return nil
}

func (connector *Connector) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	analyzer := connector.OpAnalyzer
	analyzer.Start()
	defer analyzer.Stop()

	result, err := vm.ChildrenCall(connector.GetChildren(0), proc, analyzer)
	if err != nil {
		return result, err
	}

	// pipeline ends normally.
	if result.Batch == nil {
		result.Status = vm.ExecStop
		return result, nil
	}
	// batch with no data, no need to send.
	if result.Batch.IsEmpty() {
		result.Batch = batch.EmptyBatch
		return result, nil
	}

	var queryDone bool
	queryDone, err = connector.ctr.sp.SendBatch(proc.Ctx, 0, result.Batch, nil)
	if queryDone || err != nil {
		return result, err
	}
	connector.Reg.Ch2 <- process.NewPipelineSignalToGetFromSpool(connector.ctr.sp, 0)
	return result, nil
}
