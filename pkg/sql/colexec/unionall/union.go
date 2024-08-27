// Copyright 2024 Matrix Origin
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

package unionall

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "unionall"

func (unionall *UnionAll) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": union all ")
}

func (unionall *UnionAll) OpType() vm.OpType {
	return vm.UnionAll
}

func (unionall *UnionAll) Prepare(proc *process.Process) error {
	unionall.OpAnalyzer = process.NewAnalyzer(unionall.GetIdx(), unionall.IsFirst, unionall.IsLast, "unionall")
	return nil
}

func (unionall *UnionAll) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	//anal := proc.GetAnalyze(unionall.GetIdx(), unionall.GetParallelIdx(), unionall.GetParallelMajor())
	//anal.Start()
	//defer anal.Stop()
	analyzer := unionall.OpAnalyzer
	analyzer.Start()
	defer analyzer.Stop()

	//result, err := vm.ChildrenCall(unionall.GetChildren(0), proc, anal)
	result, err := vm.ChildrenCall(unionall.GetChildren(0), proc, analyzer)
	if err != nil {
		return result, err
	}

	//anal.Input(result.Batch, unionall.IsFirst)
	//anal.Output(result.Batch, unionall.IsLast)
	analyzer.Output(result.Batch)
	return result, nil
}
