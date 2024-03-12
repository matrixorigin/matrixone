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

package limit

import (
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const argName = "limit"

func (arg *Argument) String(buf *bytes.Buffer) {
	buf.WriteString(argName)
	buf.WriteString(fmt.Sprintf("limit(%v)", arg.Limit))
}

func (arg *Argument) Prepare(_ *process.Process) error {
	return nil
}

// Call returning only the first n tuples from its input
func (arg *Argument) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	ap := arg
	anal := proc.GetAnalyze(arg.GetIdx(), arg.GetParallelIdx(), arg.GetParallelMajor())
	if ap.Limit == 0 {
		result := vm.NewCallResult()
		result.Batch = nil
		result.Status = vm.ExecStop
		return result, nil
	}

	result, err := arg.GetChildren(0).Call(proc)
	if err != nil {
		return result, err
	}

	anal.Start()
	defer anal.Stop()

	if result.Batch == nil || result.Batch.IsEmpty() || result.Batch.Last() {
		return result, nil
	}
	bat := result.Batch
	anal.Input(bat, arg.GetIsFirst())

	if ap.Seen >= ap.Limit {
		result.Batch = nil
		result.Status = vm.ExecStop
		return result, nil
	}
	length := bat.RowCount()
	newSeen := ap.Seen + uint64(length)
	if newSeen >= ap.Limit { // limit - seen
		batch.SetLength(bat, int(ap.Limit-ap.Seen))
		ap.Seen = newSeen
		anal.Output(bat, arg.GetIsLast())

		result.Status = vm.ExecStop
		return result, nil
	}
	anal.Output(bat, arg.GetIsLast())
	ap.Seen = newSeen
	return result, nil
}
