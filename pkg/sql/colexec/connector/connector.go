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
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/vm/mheap"
	"matrixone/pkg/vm/process"
)

func String(arg interface{}, buf *bytes.Buffer) {
	buf.WriteString("pipe connector")
}

func Prepare(_ *process.Process, _ interface{}) error {
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	n := arg.(*Argument)
	reg := n.Reg
	bat := proc.Reg.InputBatch
	if bat == nil || len(bat.Zs) == 0 {
		select {
		case <-reg.Ctx.Done():
			process.FreeRegisters(proc)
			return true, nil
		case reg.Ch <- bat:
			return false, nil
		}
	}
	vecs := n.vecs[:0]
	for i := range bat.Vecs {
		if bat.Vecs[i].Or {
			vec, err := vector.Dup(bat.Vecs[i], proc.Mp)
			if err != nil {
				return false, err
			}
			vecs = append(vecs, vec)
		}
	}
	for i := range bat.Vecs {
		if bat.Vecs[i].Or {
			bat.Vecs[i] = vecs[0]
			vecs = vecs[1:]
		}
	}
	size := mheap.Size(proc.Mp)
	select {
	case <-reg.Ctx.Done():
		batch.Clean(bat, proc.Mp)
		process.FreeRegisters(proc)
		return true, nil
	case reg.Ch <- bat:
		n.Mmu.Alloc(size)
		proc.Mp.Gm.Free(size)
		return false, nil
	}
}
