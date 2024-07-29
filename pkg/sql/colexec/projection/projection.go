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
	projection.projExecutors, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, projection.Es)
	return err
}

func (projection *Projection) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	input, err := projection.GetChildren(0).Call(proc)
	if err != nil || input.Done() {
		return input, err
	}

	anal := proc.GetAnalyze(projection.GetIdx(), projection.GetParallelIdx(), projection.GetParallelMajor())
	anal.Start()
	defer anal.Stop()

	anal.Input(input.Batch, projection.GetIsFirst())

	// initialze projection.buf
	if projection.buf == nil {
		projection.buf = batch.NewWithSize(len(projection.Es))
		projection.buf.SetBorrowed()
	}
	// keep shuffleIDX unchanged
	projection.buf.ShuffleIDX = input.Batch.ShuffleIDX

	// do projection.
	for i := range projection.projExecutors {
		vec, err := projection.projExecutors[i].Eval(proc, []*batch.Batch{input.Batch}, nil)
		if err != nil {
			return vm.CallResult{}, err
		}
		projection.buf.Vecs[i] = vec
	}
	projection.buf.SetRowCount(input.Batch.RowCount())

	// The folowing is useless.
	// newAlloc, err := colexec.FixProjectionResult(proc, projection.projExecutors, projection.uafs, projection.buf, input.Batch)
	// Note conviced this maxAllocSize make sense any more.
	// projection.maxAllocSize = max(projection.maxAllocSize, newAlloc)
	anal.Output(projection.buf, projection.GetIsLast())

	// Keep input status
	return vm.CallResult{Status: input.Status, Batch: projection.buf}, nil
}
