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
		stringFunc[in.Op](in.Arg, buf)
	}
}

// Prepare range instructions and do init work for each operator's argument by calling its prepare function
func Prepare(ins Instructions, proc *process.Process) error {
	for _, in := range ins {
		if err := prepareFunc[in.Op](proc, in.Arg); err != nil {
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

	return fubarRun(ins, proc, 0)
}

func fubarRun(ins Instructions, proc *process.Process, start int) (end bool, err error) {
	var fubarStack []int
	var ok process.ExecStatus

	for i := start; i < len(ins); i++ {
		if ok, err = execFunc[ins[i].Op](i, proc, ins[i].Arg, ins[i].IsFirst, ins[i].IsLast); err != nil {
			// error handling weirdness
			return ok == process.ExecStop || end, err
		}

		if ok == process.ExecStop {
			end = true
		} else if ok == process.ExecHasMore {
			fubarStack = append(fubarStack, i)
		}
	}

	// run the stack backwards.
	for i := len(fubarStack) - 1; i >= 0; i-- {
		end, err = fubarRun(ins, proc, fubarStack[i])
		if end || err != nil {
			return end, err
		}
	}
	return end, err
}
