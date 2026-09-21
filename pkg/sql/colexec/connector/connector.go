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
	"github.com/matrixorigin/matrixone/pkg/container/pSpool"
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
	result := vm.NewCallResult()
	if connector.ctr.pendingBatch == nil {
		if connector.Reg != nil && len(connector.Reg.Ch2) >= cap(connector.Reg.Ch2) {
			result.Status = vm.ExecWaiting
			result.OnReady = connector.Reg.RegisterCapacityReady
			return result, nil
		}
		childResult, err := vm.ChildrenCall(connector.GetChildren(0), proc, connector.OpAnalyzer)
		if err != nil {
			return childResult, err
		}
		if childResult.Batch == nil {
			childResult.Status = vm.ExecStop
			return childResult, nil
		}
		if childResult.Batch.IsEmpty() {
			childResult.Batch = batch.EmptyBatch
			return childResult, nil
		}
		connector.ctr.pendingBatch = childResult.Batch
		connector.ctr.spoolSent = false
		result = childResult
	} else {
		result.Batch = connector.ctr.pendingBatch
	}

	if !connector.ctr.spoolSent {
		queryDone, sent, err := connector.ctr.sp.TrySendBatch(0, connector.ctr.pendingBatch, nil)
		if err != nil {
			return result, err
		}
		if queryDone {
			result.Status = vm.ExecStop
			return result, nil
		}
		if !sent {
			result.Status = vm.ExecWaiting
			result.OnReady = connector.ctr.sp.RegisterSendReady
			return result, nil
		}
		connector.ctr.spoolSent = true
	}
	if !connector.Reg.TrySendData(connector.ctr.sp, 0) {
		result.Status = vm.ExecWaiting
		result.OnReady = connector.Reg.RegisterCapacityReady
		return result, nil
	}
	connector.ctr.pendingBatch = nil
	connector.ctr.spoolSent = false
	return result, nil
}
