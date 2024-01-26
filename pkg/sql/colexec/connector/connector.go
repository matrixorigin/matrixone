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

func (arg *Argument) String(buf *bytes.Buffer) {
	buf.WriteString("pipe connector")
}

func (arg *Argument) Prepare(_ *process.Process) error {
	return nil
}

func (arg *Argument) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	reg := arg.Reg

	result, err := arg.Children[0].Call(proc)
	if err != nil {
		return result, err
	}

	if result.Batch == nil {
		result.Status = vm.ExecStop
		return result, nil
	}

	bat := result.Batch
	if bat.IsEmpty() {
		result.Batch = batch.EmptyBatch
		return result, nil
	}
	bat.AddCnt(1)

	// there is no need to log anything here.
	// because the context is already canceled means the pipeline closed normally.
	select {
	case <-proc.Ctx.Done():
		proc.PutBatch(bat)
		result.Status = vm.ExecStop
		return result, nil

	case <-reg.Ctx.Done():
		proc.PutBatch(bat)
		result.Status = vm.ExecStop
		return result, nil

	case reg.Ch <- bat:
		return result, nil
	}
}
