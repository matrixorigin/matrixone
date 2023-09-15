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

package vm

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (ins Instruction) MarshalBinary() ([]byte, error) {
	return nil, nil
}

func (ins Instruction) UnmarshalBinary(_ []byte) error {
	return nil
}

// String range instructions and call each operator's string function to show a query plan
func String(ins Instructions, buf *bytes.Buffer) {
	for i, in := range ins {
		if i > 0 {
			buf.WriteString(" -> ")
		}
		instructionVFT[in.Op].fnString(&in, buf)
	}
}

// Prepare range instructions and do init work for each operator's argument by calling its prepare function
func Prepare(ins Instructions, proc *process.Process) error {
	for _, in := range ins {
		if err := instructionVFT[in.Op].fnPrepare(&in, proc); err != nil {
			return err
		}
	}
	return nil
}

func Run(ins Instructions, proc *process.Process) (end bool, err error) {
	defer func() {
		if e := recover(); e != nil {
			err = moerr.ConvertPanicError(proc.Ctx, e)
		}
	}()

	// XXX Instructions is linearized.  This is simply wrong.
	// Wait for people to unscrew this.
	for i := 1; i < len(ins); i++ {
		ins[i].Children = append(ins[i].Children, &ins[i-1])
	}

	rootIdx := len(ins) - 1
	root := &ins[rootIdx]
	end = false
	for !end {
		bat, err := InstructionCall(root, proc)
		if err != nil {
			return false, err
		}
		end = (bat == nil)
	}
	return end, err
}
