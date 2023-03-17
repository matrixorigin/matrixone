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

func String(arg any, buf *bytes.Buffer) {
	n := arg.(*Argument)
	buf.WriteString("projection(")
	for i, e := range n.Es {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(e.String())
	}
	buf.WriteString(")")
}

func Prepare(_ *process.Process, _ any) error {
	return nil
}

func Call(idx int, proc *process.Process, arg any, isFirst bool, isLast bool) (bool, error) {
	anal := proc.GetAnalyze(idx)
	anal.Start()
	defer anal.Stop()

	bat := proc.InputBatch()
	if bat == nil {
		proc.SetInputBatch(nil)
		return true, nil
	}
	if bat.Length() == 0 {
		return false, nil
	}
	anal.Input(bat, isFirst)
	ap := arg.(*Argument)
	rbat := batch.NewWithSize(len(ap.Es))
	for i, e := range ap.Es {
		vec, err := colexec.EvalExpr(bat, proc, e)
		if err != nil {
			bat.Clean(proc.Mp())
			rbat.Clean(proc.Mp())
			return false, err
		}
		rbat.Vecs[i] = vec
	}
	for i, vec := range bat.Vecs {
		isSame := false
		for _, rVec := range rbat.Vecs {
			if vec == rVec {
				bat.Vecs[i] = nil
				isSame = true
				break
			}
		}
		if !isSame && vec != nil {
			anal.Alloc(int64(vec.Size()))
		}
	}
	rbat.Zs = bat.Zs
	bat.Zs = nil
	bat.Clean(proc.Mp())
	anal.Output(rbat, isLast)
	proc.SetInputBatch(rbat)
	return false, nil
}
