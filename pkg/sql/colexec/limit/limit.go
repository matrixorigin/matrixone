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

func String(ins *vm.Instruction, buf *bytes.Buffer) {
	n := ins.Arg.(*Argument)
	buf.WriteString(fmt.Sprintf("limit(%v)", n.Limit))
}

func Prepare(*vm.Instruction, *process.Process) error {
	return nil
}

// Call returning only the first n tuples from its input
func Call(ins *vm.Instruction, proc *process.Process) (*batch.Batch, error) {
	ap := ins.Arg.(*Argument)
	child := ins.Children[0]
	anal := proc.GetAnalyze(ins.Idx)
	anal.Start()
	defer anal.Stop()

	// done
	if ap.Seen >= ap.Limit {
		return nil, nil
	}

	bat, err := vm.InstructionCall(child, proc)
	if err != nil {
		return nil, err
	}
	if bat == nil {
		return nil, nil
	}

	length := bat.RowCount()
	newSeen := ap.Seen + uint64(length)
	isLast := false
	if newSeen >= ap.Limit { // limit - seen
		batch.SetLength(bat, int(ap.Limit-ap.Seen))
		isLast = true
	}
	ap.Seen = newSeen
	anal.Output(bat, isLast)
	return bat, nil
}
