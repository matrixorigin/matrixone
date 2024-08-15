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
	merge.ctr = new(container)
	if merge.Partial {
		merge.ctr.InitReceiver(proc, proc.Reg.MergeReceivers[merge.StartIDX:merge.EndIDX])
	} else {
		merge.ctr.InitReceiver(proc, proc.Reg.MergeReceivers)
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
	var msg *process.RegisterMessage
	result := vm.NewCallResult()
	if merge.ctr.buf != nil {
		proc.PutBatch(merge.ctr.buf)
		merge.ctr.buf = nil
	}
	var err error
	for {
		msg = merge.ctr.ReceiveFromAllRegs(anal)
		if msg.Err != nil {
			result.Status = vm.ExecStop
			return result, msg.Err
		}
		if msg.Batch == nil {
			result.Status = vm.ExecStop
			return result, nil
		}
		if msg.Batch.Last() && merge.SinkScan {
			continue
		}

		merge.ctr.buf, err = msg.Batch.Dup(proc.GetMPool())
		if err != nil {
			return vm.CancelResult, err
		}
		break
	}

	anal.Input(merge.ctr.buf, merge.GetIsFirst())
	anal.Output(merge.ctr.buf, merge.GetIsLast())
	result.Batch = merge.ctr.buf
	return result, nil
}
