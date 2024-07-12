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

package mergelimit

import (
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "merge_limit"

func (mergeLimit *MergeLimit) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	ap := mergeLimit
	buf.WriteString(fmt.Sprintf("mergeLimit(%v)", ap.Limit))
}

func (mergeLimit *MergeLimit) OpType() vm.OpType {
	return vm.MergeLimit
}

func (mergeLimit *MergeLimit) Prepare(proc *process.Process) error {
	mergeLimit.ctr = new(container)
	mergeLimit.ctr.seen = 0
	mergeLimit.ctr.InitReceiver(proc, true)
	var err error
	if mergeLimit.ctr.limitExecutor == nil {
		mergeLimit.ctr.limitExecutor, err = colexec.NewExpressionExecutor(proc, mergeLimit.Limit)
		if err != nil {
			return err
		}
	}
	vec, err := mergeLimit.ctr.limitExecutor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
	if err != nil {
		return err
	}
	mergeLimit.ctr.limit = uint64(vector.MustFixedCol[uint64](vec)[0])
	return nil
}

func (mergeLimit *MergeLimit) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	anal := proc.GetAnalyze(mergeLimit.GetIdx(), mergeLimit.GetParallelIdx(), mergeLimit.GetParallelMajor())
	anal.Start()
	defer anal.Stop()

	result := vm.NewCallResult()
	var msg *process.RegisterMessage
	if mergeLimit.ctr.buf != nil {
		proc.PutBatch(mergeLimit.ctr.buf)
		mergeLimit.ctr.buf = nil
	}

	for {
		msg = mergeLimit.ctr.ReceiveFromAllRegs(anal)
		if msg.Err != nil {
			result.Status = vm.ExecStop
			return result, msg.Err
		}
		if msg.Batch == nil {
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
		mergeLimit.ctr.buf = msg.Batch
		if mergeLimit.ctr.buf.Last() {
			result.Batch = mergeLimit.ctr.buf
			return result, nil
		}

		anal.Input(mergeLimit.ctr.buf, mergeLimit.GetIsFirst())
		if mergeLimit.ctr.seen >= mergeLimit.ctr.limit {
			proc.PutBatch(mergeLimit.ctr.buf)
			continue
		}
		newSeen := mergeLimit.ctr.seen + uint64(mergeLimit.ctr.buf.RowCount())
		if newSeen < mergeLimit.ctr.limit {
			mergeLimit.ctr.seen = newSeen
			anal.Output(mergeLimit.ctr.buf, mergeLimit.GetIsLast())
			result.Batch = mergeLimit.ctr.buf
			return result, nil
		} else {
			num := int(newSeen - mergeLimit.ctr.limit)
			batch.SetLength(mergeLimit.ctr.buf, mergeLimit.ctr.buf.RowCount()-num)
			mergeLimit.ctr.seen = newSeen
			anal.Output(mergeLimit.ctr.buf, mergeLimit.GetIsLast())
			result.Batch = mergeLimit.ctr.buf
			return result, nil
		}
	}
}
