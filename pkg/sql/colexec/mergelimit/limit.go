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
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (arg *Argument) String(buf *bytes.Buffer) {
	ap := arg
	buf.WriteString(fmt.Sprintf("mergeLimit(%d)", ap.Limit))
}

func (arg *Argument) Prepare(proc *process.Process) error {
	ap := arg
	ap.ctr = new(container)
	ap.ctr.seen = 0
	ap.ctr.InitReceiver(proc, true)
	return nil
}

func (arg *Argument) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	anal := proc.GetAnalyze(arg.info.Idx, arg.info.ParallelIdx, arg.info.ParallelMajor)
	anal.Start()
	defer anal.Stop()

	result := vm.NewCallResult()
	var end bool
	var err error
	if arg.buf != nil {
		proc.PutBatch(arg.buf)
		arg.buf = nil
	}

	for {
		arg.buf, end, err = arg.ctr.ReceiveFromAllRegs(anal)
		if err != nil {
			result.Status = vm.ExecStop
			return result, nil
		}
		if end {
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
		if arg.buf.Last() {
			result.Batch = arg.buf
			return result, nil
		}

		anal.Input(arg.buf, arg.info.IsFirst)
		if arg.ctr.seen >= arg.Limit {
			proc.PutBatch(arg.buf)
			continue
		}
		newSeen := arg.ctr.seen + uint64(arg.buf.RowCount())
		if newSeen < arg.Limit {
			arg.ctr.seen = newSeen
			anal.Output(arg.buf, arg.info.IsLast)
			result.Batch = arg.buf
			return result, nil
		} else {
			num := int(newSeen - arg.Limit)
			batch.SetLength(arg.buf, arg.buf.RowCount()-num)
			arg.ctr.seen = newSeen
			anal.Output(arg.buf, arg.info.IsLast)
			result.Batch = arg.buf
			return result, nil
		}
	}
}
