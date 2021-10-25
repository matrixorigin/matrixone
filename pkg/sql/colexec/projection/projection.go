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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
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

	if proc.Reg.InputBatch == nil {
		return false, nil
	}
	bat := proc.Reg.InputBatch.(*batch.Batch)
	if bat == nil || bat.Attrs == nil {
		return false, nil
	}
	bat.Shuffle(proc)
	n := arg.(*Argument)
	rbat := batch.New(true, n.Attrs)
	for i := range n.Attrs {
		if rbat.Vecs[i], _, err = n.Es[i].Eval(bat, proc); err != nil {
			rbat.Vecs = rbat.Vecs[:i]
			bat.Clean(proc)
			rbat.Clean(proc)
			return false, err
		}
	}
	for i, e := range n.Es {
		if _, ok := e.(*extend.Attribute); !ok {
			bat.Reduce(e.Attributes(), proc)
		}
		if name, ok := e.(*extend.Attribute); !ok || name.Name != n.Attrs[i] {
			rbat.Vecs[i].Ref = n.Refer[n.Attrs[i]]
		}
	}
	proc.Reg.InputBatch = rbat
	return false, nil
}
