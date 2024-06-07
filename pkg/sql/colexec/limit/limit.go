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

package limit

import (
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const argName = "limit"

func (arg *Argument) String(buf *bytes.Buffer) {
	buf.WriteString(argName)
	buf.WriteString(fmt.Sprintf("limit(%v)", arg.LimitExpr))
}

func (arg *Argument) Prepare(proc *process.Process) error {
	var err error
	if arg.limitExecutor == nil {
		arg.limitExecutor, err = colexec.NewExpressionExecutor(proc, arg.LimitExpr)
		if err != nil {
			return err
		}
	}
	vec, err := arg.limitExecutor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch})
	if err != nil {
		return err
	}
	arg.limit = uint64(vector.MustFixedCol[uint64](vec)[0])

	return nil
}

// Call returning only the first n tuples from its input
func (arg *Argument) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	anal := proc.GetAnalyze(arg.GetIdx(), arg.GetParallelIdx(), arg.GetParallelMajor())
	if arg.limit == 0 {
		result := vm.NewCallResult()
		result.Batch = nil
		result.Status = vm.ExecStop
		return result, nil
	}

	result, err := arg.GetChildren(0).Call(proc)
	if err != nil {
		return result, err
	}

	anal.Start()
	defer anal.Stop()

	if result.Batch == nil || result.Batch.IsEmpty() || result.Batch.Last() {
		return result, nil
	}
	bat := result.Batch
	anal.Input(bat, arg.GetIsFirst())

	if arg.Seen >= arg.limit {
		result.Batch = nil
		result.Status = vm.ExecStop
		return result, nil
	}
	length := bat.RowCount()
	newSeen := arg.Seen + uint64(length)
	if newSeen >= arg.limit { // limit - seen
		batch.SetLength(bat, int(arg.limit-arg.Seen))
		arg.Seen = newSeen
		anal.Output(bat, arg.GetIsLast())

		result.Status = vm.ExecStop
		return result, nil
	}
	anal.Output(bat, arg.GetIsLast())
	arg.Seen = newSeen
	return result, nil
}
