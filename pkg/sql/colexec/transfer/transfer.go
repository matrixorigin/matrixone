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

package transfer

import (
	"bytes"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/vm/process"
)

func String(_ interface{}, buf *bytes.Buffer) {
	buf.WriteString("transfer")
}

func Prepare(_ *process.Process, _ interface{}) error {
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	n := arg.(*Argument)
	reg := n.Reg
	if reg.Ch == nil {
		if proc.Reg.Ax != nil {
			if bat := proc.Reg.Ax.(*batch.Batch); bat != nil {
				bat.Clean(proc)
			}
		}
		return true, nil
	}
	reg.Wg.Add(1)
	reg.Ch <- proc.Reg.Ax
	n.Mmu.Alloc(proc.Size())
	proc.Gm.Free(proc.Size())
	reg.Wg.Wait()
	return false, nil
}
