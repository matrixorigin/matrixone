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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
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

func Call(_ int, proc *process.Process, arg interface{}) (bool, error) {
	bat := proc.Reg.InputBatch
	if bat == nil {
		return true, nil
	}
	if len(bat.Zs) == 0 {
		return false, nil
	}
	ap := arg.(*Argument)
	rbat := batch.NewWithSize(len(ap.Es))
	for i, e := range ap.Es {
		vec, err := colexec.EvalExpr(bat, proc, e)
		if err != nil || vec.ConstExpand(proc.Mp) == nil {
			bat.Clean(proc.Mp)
			rbat.Clean(proc.Mp)
			return false, err
		}
		rbat.Vecs[i] = vec
	}
	for _, vec := range rbat.Vecs {
		for k := 0; k < len(bat.Vecs); k++ {
			if vec == bat.Vecs[k] {
				bat.Vecs = append(bat.Vecs[:k], bat.Vecs[k+1:]...)
				break
			}
		}
	}
	rbat.Zs = bat.Zs
	bat.Zs = nil
	bat.Clean(proc.Mp)
	proc.Reg.InputBatch = rbat
	return false, nil
}
