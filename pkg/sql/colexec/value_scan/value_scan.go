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

func (arg *Argument) String(buf *bytes.Buffer) {
	buf.WriteString(" table_scan ")
}

func (arg *Argument) Prepare(proc *process.Process) (err error) {
	return nil
}

func (arg *Argument) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	result := vm.NewCallResult()

	//select {
	//case <-proc.Ctx.Done():
	//	result.Status = vm.ExecStop
	//	return result, proc.Ctx.Err()
	//default:
	//}

	if arg.idx < len(arg.Batchs) {
		result.Batch = arg.Batchs[arg.idx]
		if arg.idx > 0 {
			proc.PutBatch(arg.Batchs[arg.idx-1])
			arg.Batchs[arg.idx-1] = nil
		}
		arg.idx += 1
	}

	return result, nil
}
