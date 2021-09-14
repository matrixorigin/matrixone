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

package restrict

import (
	"bytes"
	"fmt"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/sql/colexec/extend"
	"matrixone/pkg/vm/process"
	"matrixone/pkg/vm/register"
)

func String(arg interface{}, buf *bytes.Buffer) {
	n := arg.(*Argument)
	buf.WriteString(fmt.Sprintf("σ(%s)", n.E))
}

func Prepare(_ *process.Process, arg interface{}) error {
	n := arg.(*Argument)
	n.Attrs = n.E.Attributes()
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	if proc.Reg.InputBatch == nil {
		return false, nil
	}
	bat := proc.Reg.InputBatch.(*batch.Batch)
	if bat == nil || bat.Attrs == nil {
		return false, nil
	}
	n := arg.(*Argument)
	if _, ok := n.E.(*extend.Attribute); ok { // mysql treats any attribute as true
		proc.Reg.InputBatch = bat
		return false, nil
	}
	if es := extend.AndExtends(n.E, []extend.Extend{}); len(es) > 0 {
		for _, e := range es {
			vec, _, err := e.Eval(bat, proc)
			if err != nil {
				bat.Clean(proc)
				return false, nil
			}
			sels := vec.Col.([]int64)
			if len(sels) == 0 {
				bat.Clean(proc)
				bat.Attrs = nil
				proc.Reg.InputBatch = bat
				register.Put(proc, vec)
				return false, nil
			}
			for i, vec := range bat.Vecs {
				if bat.Vecs[i], err = vec.Shuffle(sels, proc); err != nil {
					return false, err
				}
			}
			register.Put(proc, vec)
		}
		for _, vec := range bat.Vecs { // reset reference count of vector
			if vec.Ref == 0 {
				vec.Ref = 2
			}
		}
		bat.Reduce(n.Attrs, proc)
		proc.Reg.InputBatch = bat
		return false, nil
	}
	vec, _, err := n.E.Eval(bat, proc)
	if err != nil {
		bat.Clean(proc)
		return false, err
	}
	sels := vec.Col.([]int64)
	if len(sels) == 0 {
		bat.Clean(proc)
		bat.Attrs = nil
		proc.Reg.InputBatch = bat
		register.Put(proc, vec)
		return false, nil
	}
	for i, vec := range bat.Vecs {
		if bat.Vecs[i], err = vec.Shuffle(sels, proc); err != nil {
			return false, err
		}
	}
	register.Put(proc, vec)
	bat.Reduce(n.Attrs, proc)
	proc.Reg.InputBatch = bat
	return false, nil
}
