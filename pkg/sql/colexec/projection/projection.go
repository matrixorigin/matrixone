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

package projection

import (
	"bytes"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/sql/colexec/extend"
	"matrixone/pkg/vm/process"
	"matrixone/pkg/vm/register"
	"reflect"
	"unsafe"
)

func String(arg interface{}, buf *bytes.Buffer) {
	n := arg.(*Argument)
	buf.WriteString("π(")
	for i, e := range n.Es {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(e.String())
	}
	buf.WriteString(")")
}

func Prepare(_ *process.Process, _ interface{}) error {
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	var err error

	if proc.Reg.Ax == nil {
		return false, nil
	}
	n := arg.(*Argument)
	bat := proc.Reg.Ax.(*batch.Batch)
	if bat == nil || bat.Attrs == nil {
		return false, nil
	}
	rbat := batch.New(true, n.Attrs)
	for i := range n.Attrs {
		if rbat.Vecs[i], _, err = n.Es[i].Eval(bat, proc); err != nil {
			rbat.Vecs = rbat.Vecs[:i]
			clean(bat, rbat, proc)
			return false, err
		}
		if _, ok := n.Es[i].(*extend.Attribute); !ok {
			count := n.Refer[n.Attrs[i]]
			hp := reflect.SliceHeader{Data: uintptr(unsafe.Pointer(&count)), Len: 8, Cap: 8}
			copy(rbat.Vecs[i].Data, *(*[]byte)(unsafe.Pointer(&hp)))
		}
	}
	if bat.SelsData != nil {
		proc.Free(bat.SelsData)
		bat.Sels = nil
		bat.SelsData = nil
	}
	{
		for _, e := range n.Es {
			if _, ok := e.(*extend.Attribute); !ok {
				bat.Reduce(e.Attributes(), proc)
			}
		}
	}
	proc.Reg.Ax = rbat
	register.FreeRegisters(proc)
	return false, nil
}

func clean(bat, rbat *batch.Batch, proc *process.Process) {
	bat.Clean(proc)
	rbat.Clean(proc)
	register.FreeRegisters(proc)
}
