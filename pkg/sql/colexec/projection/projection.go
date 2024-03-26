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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
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
	ap.ctr.uafs = make([]func(v *vector.Vector, w *vector.Vector) error, len(ap.Es))
	for i, e := range ap.Es {
		if e.Typ.Id != 0 {
			ap.ctr.uafs[i] = vector.GetUnionAllFunction(plan.MakeTypeByPlan2Expr(e), proc.Mp())
		}
	}
	return err
}

func (arg *Argument) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	result, err := arg.children[0].Call(proc)
	if err != nil {
		return result, err
	}

	anal := proc.GetAnalyze(arg.info.Idx, arg.info.ParallelIdx, arg.info.ParallelMajor)
	anal.Start()
	defer anal.Stop()

	if result.Batch == nil || result.Batch.IsEmpty() || result.Batch.Last() {
		return result, nil
	}
	bat := result.Batch
	anal.Input(bat, arg.info.IsFirst)

	if arg.buf != nil {
		proc.PutBatch(arg.buf)
		arg.buf = nil
	}

	arg.buf = batch.NewWithSize(len(arg.Es))
	// keep shuffleIDX unchanged
	arg.buf.ShuffleIDX = bat.ShuffleIDX
	// do projection.
	for i := range arg.ctr.projExecutors {
		vec, err := arg.ctr.projExecutors[i].Eval(proc, []*batch.Batch{bat})
		if err != nil {
			return result, err
		}
		arg.buf.Vecs[i] = vec
	}

	newAlloc, err := colexec.FixProjectionResult(proc, arg.ctr.projExecutors, arg.ctr.uafs, arg.buf, bat)
	if err != nil {
		return result, err
	}
	if newAlloc > arg.maxAllocSize {
		arg.maxAllocSize = newAlloc
	}
	arg.buf.SetRowCount(bat.RowCount())

	anal.Output(arg.buf, arg.info.IsLast)
	result.Batch = arg.buf
	return result, nil
}
