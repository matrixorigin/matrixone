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
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (arg *Argument) String(buf *bytes.Buffer) {
	n := arg
	buf.WriteString("projection(")
	for i, e := range n.Es {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(e.String())
	}
	buf.WriteString(")")
}

func (arg *Argument) Prepare(proc *process.Process) (err error) {
	ap := arg
	ap.ctr = new(container)
	ap.ctr.projExecutors, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, ap.Es)

	return err
}

func (arg *Argument) Call(proc *process.Process) (vm.CallResult, error) {
	anal := proc.GetAnalyze(arg.info.Idx)
	anal.Start()
	defer anal.Stop()

	result, err := arg.children[0].Call(proc)
	if err != nil {
		return result, err
	}
	if result.Batch == nil || result.Batch.IsEmpty() || result.Batch.Last() {
		return result, nil
	}
	bat := result.Batch

	anal.Input(bat, arg.info.IsFirst)
	ap := arg
	rbat := batch.NewWithSize(len(ap.Es))

	// do projection.
	for i := range ap.ctr.projExecutors {
		vec, err := ap.ctr.projExecutors[i].Eval(proc, []*batch.Batch{bat})
		if err != nil {
			return result, err
		}
		rbat.Vecs[i] = vec
	}

	newAlloc, err := colexec.FixProjectionResult(proc, ap.ctr.projExecutors, rbat, bat)
	if err != nil {
		bat.Clean(proc.Mp())
		return result, err
	}
	anal.Alloc(int64(newAlloc))

	rbat.SetRowCount(bat.RowCount())

	proc.PutBatch(bat)
	anal.Output(rbat, arg.info.IsLast)
	result.Batch = rbat
	return result, nil
}
