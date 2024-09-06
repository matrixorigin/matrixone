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

package merge

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/vm"

	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "merge"

func (merge *Merge) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": union all ")
}

func (merge *Merge) OpType() vm.OpType {
	return vm.Merge
}

func (merge *Merge) Prepare(proc *process.Process) error {
	if merge.Partial {
		merge.ctr.receiver = process.InitPipelineSignalReceiver(proc.Ctx, proc.Reg.MergeReceivers[merge.StartIDX:merge.EndIDX])
	} else {
		merge.ctr.receiver = process.InitPipelineSignalReceiver(proc.Ctx, proc.Reg.MergeReceivers)
	}
	return nil
}

func (merge *Merge) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	anal := proc.GetAnalyze(merge.GetIdx(), merge.GetParallelIdx(), merge.GetParallelMajor())
	anal.Start()
	defer anal.Stop()

	var info error
	result := vm.NewCallResult()
	for {
		result.Batch, info = merge.ctr.receiver.GetNextBatch()
		if info != nil {
			return vm.CancelResult, info
		}

		if result.Batch == nil {
			result.Status = vm.ExecStop
			return result, nil
		}
		if merge.SinkScan && result.Batch.Last() {
			continue
		}
		break
	}

	anal.Input(result.Batch, merge.GetIsFirst())
	anal.Output(result.Batch, merge.GetIsLast())
	return result, nil
}
