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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/matrixorigin/matrixone/pkg/vm/register"
)

func String(_ interface{}, buf *bytes.Buffer) {
	buf.WriteString("=>")
}

func Prepare(_ *process.Process, _ interface{}) error {
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	n := arg.(*Argument)
	reg := n.Reg
	if reg.Ch == nil {
		if proc.Reg.InputBatch != nil {
			if bat := proc.Reg.InputBatch.(*batch.Batch); bat != nil {
				bat.Clean(proc)
			}
		}
		register.FreeRegisters(proc)
		return true, nil
	}
	if proc.Reg.InputBatch == nil {
		reg.Wg.Add(1)
		reg.Ch <- nil
		reg.Wg.Wait()
		register.FreeRegisters(proc)
		return true, nil
	}
	bat := proc.Reg.InputBatch.(*batch.Batch)
	if bat == nil || bat.Attrs == nil {
		reg.Wg.Add(1)
		reg.Ch <- bat
		reg.Wg.Wait()
		return false, nil
	}
	{
		for i := 0; i < len(bat.Attrs); i++ {
			if bat.Vecs[i] == nil {
				bat.Vecs = append(bat.Vecs[:i], bat.Vecs[i+1:]...)
				bat.Attrs = append(bat.Attrs[:i], bat.Attrs[i+1:]...)
				i--
			}
		}
	}
	size := int64(0)
	vecs := n.vecs[:0]
	for i := range bat.Vecs {
		if bat.Vecs[i].Or {
			vec, err := bat.Vecs[i].Dup(proc)
			if err != nil {
				clean(vecs, n.Proc)
				return false, err
			}
			vecs = append(vecs, vec)
		} else {
			size += int64(cap(bat.Vecs[i].Data))
		}
	}
	for i := range bat.Vecs {
		if bat.Vecs[i].Or {
			bat.Vecs[i] = vecs[0]
			vecs = vecs[1:]
		}
	}
	reg.Wg.Add(1)
	reg.Ch <- bat
	n.Proc.Gm.Alloc(size)
	proc.Gm.Free(size)
	reg.Wg.Wait()
	return false, nil
}

func clean(vecs []*vector.Vector, proc *process.Process) {
	for _, vec := range vecs {
		vec.Clean(proc)
	}
}
