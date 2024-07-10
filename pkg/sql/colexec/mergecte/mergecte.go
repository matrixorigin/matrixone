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

package mergecte

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "merge_cte"

func (mergeCte *MergeCTE) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": merge cte ")
}

func (mergeCte *MergeCTE) Prepare(proc *process.Process) error {
	mergeCte.ctr = new(container)
	mergeCte.ctr.InitReceiver(proc, true)
	mergeCte.ctr.nodeCnt = int32(len(proc.Reg.MergeReceivers)) - 1
	mergeCte.ctr.curNodeCnt = mergeCte.ctr.nodeCnt
	mergeCte.ctr.status = sendInitial
	return nil
}

func (mergeCte *MergeCTE) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	anal := proc.GetAnalyze(mergeCte.GetIdx(), mergeCte.GetParallelIdx(), mergeCte.GetParallelMajor())
	anal.Start()
	defer anal.Stop()
	var msg *process.RegisterMessage
	result := vm.NewCallResult()
	if mergeCte.ctr.buf != nil {
		proc.PutBatch(mergeCte.ctr.buf)
		mergeCte.ctr.buf = nil
	}
	switch mergeCte.ctr.status {
	case sendInitial:
		msg = mergeCte.ctr.ReceiveFromSingleReg(0, anal)
		if msg.Err != nil {
			result.Status = vm.ExecStop
			return result, msg.Err
		}
		mergeCte.ctr.buf = msg.Batch
		if mergeCte.ctr.buf == nil {
			mergeCte.ctr.status = sendLastTag
		}
		fallthrough
	case sendLastTag:
		if mergeCte.ctr.status == sendLastTag {
			mergeCte.ctr.status = sendRecursive
			mergeCte.ctr.buf = makeRecursiveBatch(proc)
			mergeCte.ctr.RemoveChosen(1)
		}
	case sendRecursive:
		for {
			msg = mergeCte.ctr.ReceiveFromAllRegs(anal)
			if msg.Batch == nil {
				result.Batch = nil
				result.Status = vm.ExecStop
				return result, nil
			}
			mergeCte.ctr.buf = msg.Batch
			if !mergeCte.ctr.buf.Last() {
				break
			}

			mergeCte.ctr.buf.SetLast()
			mergeCte.ctr.curNodeCnt--
			if mergeCte.ctr.curNodeCnt == 0 {
				mergeCte.ctr.curNodeCnt = mergeCte.ctr.nodeCnt
				break
			} else {
				proc.PutBatch(mergeCte.ctr.buf)
			}
		}
	}

	anal.Input(mergeCte.ctr.buf, mergeCte.GetIsFirst())
	anal.Output(mergeCte.ctr.buf, mergeCte.GetIsLast())
	result.Batch = mergeCte.ctr.buf
	return result, nil
}

func makeRecursiveBatch(proc *process.Process) *batch.Batch {
	b := batch.NewWithSize(1)
	b.Attrs = []string{
		"recursive_col",
	}
	b.SetVector(0, proc.GetVector(types.T_varchar.ToType()))
	vector.AppendBytes(b.GetVector(0), []byte("check recursive status"), false, proc.GetMPool())
	batch.SetLength(b, 1)
	b.SetLast()
	return b
}
