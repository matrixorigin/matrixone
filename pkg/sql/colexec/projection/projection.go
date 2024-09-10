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

const opName = "projection"

func (projection *Projection) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": projection(")
	for i, e := range projection.ProjectList {
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
	if len(projection.ctr.projExecutors) == 0 {
		projection.ctr.projExecutors, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, projection.ProjectList)

		projection.ctr.buf = batch.NewWithSize(len(projection.ProjectList))
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

	// keep shuffleIDX unchanged
	projection.ctr.buf.ShuffleIDX = bat.ShuffleIDX
	for i := range projection.ctr.projExecutors {
		vec, err := projection.ctr.projExecutors[i].Eval(proc, []*batch.Batch{bat}, nil)
		if err != nil {
			return vm.CancelResult, err
		}
		// for projection operator, all Vectors of projectBat come from executor.Eval
		// and will not be modified within projection operator. so we can used the result of executor.Eval directly.
		// (if operator will modify vector/agg of batch, you should make a copy)
		// however, it should be noted that since they directly come from executor.Eval
		// these vectors cannot be free by batch.Clean directly and must be handed over executor.Free
		projection.ctr.buf.Vecs[i] = vec
	}
	projection.maxAllocSize = max(projection.maxAllocSize, projection.ctr.buf.Size())
	projection.ctr.buf.SetRowCount(bat.RowCount())

	anal.Output(projection.ctr.buf, projection.GetIsLast())
	result.Batch = projection.ctr.buf
	return result, nil
}
