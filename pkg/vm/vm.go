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
	"github.com/matrixorigin/matrixone/pkg/logutil"
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
		in.Arg.String(buf)
	}
}

// Prepare range instructions and do init work for each operator's argument by calling its prepare function
func Prepare(ins Instructions, proc *process.Process) error {
	for _, in := range ins {
		if err := in.Arg.Prepare(proc); err != nil {
			return err
		}
	}
	return nil
}

func Run(ins Instructions, proc *process.Process) (end bool, err error) {
	defer func() {
		if e := recover(); e != nil {
			err = moerr.ConvertPanicError(proc.Ctx, e)
			logutil.Errorf("panic in vm.Run: %v", err)
		}
	}()

	for i := 0; i < len(ins); i++ {
		info := &OperatorInfo{
			Idx:     ins[i].Idx,
			IsFirst: ins[i].IsFirst,
			IsLast:  ins[i].IsLast,
		}
		ins[i].Arg.SetInfo(info)

		if i > 0 {
			ins[i].Arg.AppendChild(ins[i-1].Arg)
		}
	}

	root := ins[len(ins)-1].Arg
	end = false
	for !end {
		result, err := root.Call(proc)
		if err != nil {
			return true, err
		}
		end = result.Status == ExecStop || result.Batch == nil
	}
	return end, nil

	// return fubarRun(ins, proc, 0)
}

// func fubarRun(ins Instructions, proc *process.Process, start int) (end bool, err error) {
// 	var fubarStack []int
// 	var result CallResult

// 	for i := start; i < len(ins); i++ {
// 		if result, err = ins[i].Arg.Call(proc); err != nil {
// 			return result.Status == ExecStop || end, err
// 		}

// 		if result.Status == process.ExecStop {
// 			end = true
// 		} else if result.Status == process.ExecHasMore {
// 			fubarStack = append(fubarStack, i)
// 		}
// 	}

// 	// run the stack backwards.
// 	for i := len(fubarStack) - 1; i >= 0; i-- {
// 		end, err = fubarRun(ins, proc, fubarStack[i])
// 		if end || err != nil {
// 			return end, err
// 		}
// 	}
// 	return end, err
// }
