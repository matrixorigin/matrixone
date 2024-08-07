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
	if projection.ProjectList != nil {
		err = projection.PrepareProjection(proc)
	}
	return
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
	anal.Input(result.Batch, projection.GetIsFirst())

	if projection.ProjectList != nil {
		result.Batch, err = projection.EvalProjection(result.Batch, proc)
		if err != nil {
			return result, err
		}
	}

	anal.Output(result.Batch, projection.GetIsLast())
	return result, nil
}
