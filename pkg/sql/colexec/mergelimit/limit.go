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

const argName = "merge_limit"

func (arg *Argument) String(buf *bytes.Buffer) {
	buf.WriteString(argName)
	ap := arg
	buf.WriteString(fmt.Sprintf("mergeLimit(%v)", ap.Limit))
}

func (arg *Argument) Prepare(proc *process.Process) error {
	arg.ctr = new(container)
	arg.ctr.seen = 0
	arg.ctr.InitReceiver(proc, true)
	var err error
	if arg.ctr.limitExecutor == nil {
		arg.ctr.limitExecutor, err = colexec.NewExpressionExecutor(proc, arg.Limit)
		if err != nil {
			return err
		}
	}
	vec, err := arg.ctr.limitExecutor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch})
	if err != nil {
		return err
	}
	arg.ctr.limit = uint64(vector.MustFixedCol[uint64](vec)[0])
	return nil
}

func (arg *Argument) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	anal := proc.GetAnalyze(arg.GetIdx(), arg.GetParallelIdx(), arg.GetParallelMajor())
	anal.Start()
	defer anal.Stop()

	result := vm.NewCallResult()
	var msg *process.RegisterMessage
	if arg.buf != nil {
		proc.PutBatch(arg.buf)
		arg.buf = nil
	}

	for {
		msg = arg.ctr.ReceiveFromAllRegs(anal)
		if msg.Err != nil {
			result.Status = vm.ExecStop
			return result, msg.Err
		}
		if msg.Batch == nil {
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
		arg.buf = msg.Batch
		if arg.buf.Last() {
			result.Batch = arg.buf
			return result, nil
		}

		anal.Input(arg.buf, arg.GetIsFirst())
		if arg.ctr.seen >= arg.ctr.limit {
			proc.PutBatch(arg.buf)
			continue
		}
		newSeen := arg.ctr.seen + uint64(arg.buf.RowCount())
		if newSeen < arg.ctr.limit {
			arg.ctr.seen = newSeen
			anal.Output(arg.buf, arg.GetIsLast())
			result.Batch = arg.buf
			return result, nil
		} else {
			num := int(newSeen - arg.ctr.limit)
			batch.SetLength(arg.buf, arg.buf.RowCount()-num)
			arg.ctr.seen = newSeen
			anal.Output(arg.buf, arg.GetIsLast())
			result.Batch = arg.buf
			return result, nil
		}
	}
}
