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

const opName = "projection"

func (projection *Projection) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": projection(")
	for i, e := range projection.Es {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(e.String())
	}
	buf.WriteString(")")
}

func (projection *Projection) OpType() vm.OpType {
	return vm.Projection
}

func (projection *Projection) Prepare(proc *process.Process) (err error) {
	projection.ctr = new(container)
	projection.ctr.projExecutors, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, projection.Es)
	projection.ctr.uafs = make([]func(v *vector.Vector, w *vector.Vector) error, len(projection.Es))
	for i, e := range projection.Es {
		if e.Typ.Id != 0 {
			projection.ctr.uafs[i] = vector.GetUnionAllFunction(plan.MakeTypeByPlan2Expr(e), proc.Mp())
		}
	}
	return err
}

func (projection *Projection) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	anal := proc.GetAnalyze(projection.GetIdx(), projection.GetParallelIdx(), projection.GetParallelMajor())
	anal.Start()
	defer anal.Stop()

	result, err := vm.ChildrenCall(projection.GetChildren(0), proc, anal)
	if err != nil {
		return result, err
	}

	if result.Batch == nil || result.Batch.IsEmpty() || result.Batch.Last() {
		return result, nil
	}
	bat := result.Batch
	anal.Input(bat, projection.GetIsFirst())

	if projection.ctr.buf != nil {
		proc.PutBatch(projection.ctr.buf)
		projection.ctr.buf = nil
	}

	projection.ctr.buf = batch.NewWithSize(len(projection.Es))
	// keep shuffleIDX unchanged
	projection.ctr.buf.ShuffleIDX = bat.ShuffleIDX
	// do projection.
	for i := range projection.ctr.projExecutors {
		vec, err := projection.ctr.projExecutors[i].Eval(proc, []*batch.Batch{bat}, nil)
		if err != nil {
			for _, newV := range projection.ctr.buf.Vecs {
				if newV != nil {
					for k, oldV := range bat.Vecs {
						if oldV != nil && newV == oldV {
							bat.Vecs[k] = nil
						}
					}
				}
			}
			projection.ctr.buf = nil
			return result, err
		}
		projection.ctr.buf.Vecs[i] = vec
	}

	newAlloc, err := colexec.FixProjectionResult(proc, projection.ctr.projExecutors, projection.ctr.uafs, projection.ctr.buf, bat)
	if err != nil {
		return result, err
	}
	projection.maxAllocSize = max(projection.maxAllocSize, newAlloc)
	projection.ctr.buf.SetRowCount(bat.RowCount())

	anal.Output(projection.ctr.buf, projection.GetIsLast())
	result.Batch = projection.ctr.buf
	return result, nil
}
