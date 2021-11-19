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
	"fmt"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/sql/colexec/extend"
	"matrixone/pkg/vm/process"
)

func String(arg interface{}, buf *bytes.Buffer) {
	n := arg.(*Argument)
	buf.WriteString("π(")
	for i, e := range n.Es {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(fmt.Sprintf("%s -> %s:%v", e, n.As[i], n.Rs[i]))
	}
	buf.WriteString(")")
}

func Prepare(_ *process.Process, _ interface{}) error {
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	var err error

	bat := proc.Reg.InputBatch
	if bat == nil || len(bat.Zs) == 0 {
		return false, nil
	}
	n := arg.(*Argument)
	rbat := batch.New(true, n.As)
	for i, e := range n.Es {
		if attr, ok := e.(*extend.Attribute); ok { // vector reuse
			vec := batch.GetVector(bat, attr.Name)
			rbat.Vecs[i] = &vector.Vector{
				Or:   vec.Or,
				Data: vec.Data,
				Typ:  vec.Typ,
				Col:  vec.Col,
				Nsp:  vec.Nsp,
			}
			vec.Link++
		} else {
			if rbat.Vecs[i], _, err = e.Eval(bat, proc); err != nil {
				rbat.Vecs = rbat.Vecs[:i]
				batch.Clean(bat, proc.Mp)
				batch.Clean(rbat, proc.Mp)
				proc.Reg.InputBatch = &batch.Batch{}
				return false, err
			}
		}
		rbat.Vecs[i].Ref = n.Rs[i]
	}
	if bat.Ro {
		batch.Cow(bat)
	}
	for i := range rbat.Vecs {
		bat.Vecs = append(bat.Vecs, rbat.Vecs[i])
		bat.Attrs = append(bat.Attrs, rbat.Attrs[i])
	}
	for _, e := range n.Es {
		batch.Reduce(bat, e.Attributes(), proc.Mp)
	}
	proc.Reg.InputBatch = bat
	return false, nil
}
