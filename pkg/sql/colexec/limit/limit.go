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

const opName = "limit"

func (limit *Limit) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(fmt.Sprintf("limit(%v)", limit.LimitExpr))
}

func (limit *Limit) OpType() vm.OpType {
	return vm.Limit
}

func (limit *Limit) Prepare(proc *process.Process) error {
	var err error
	limit.ctr = new(container)
	if limit.ctr.limitExecutor == nil {
		limit.ctr.limitExecutor, err = colexec.NewExpressionExecutor(proc, limit.LimitExpr)
		if err != nil {
			return err
		}
	}
	vec, err := limit.ctr.limitExecutor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
	if err != nil {
		return err
	}
	limit.ctr.limit = uint64(vector.MustFixedCol[uint64](vec)[0])

	return nil
}

// Call returning only the first n tuples from its input
func (limit *Limit) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	anal := proc.GetAnalyze(limit.GetIdx(), limit.GetParallelIdx(), limit.GetParallelMajor())
	anal.Start()
	defer anal.Stop()

	if limit.ctr.limit == 0 {
		result := vm.NewCallResult()
		result.Batch = nil
		result.Status = vm.ExecStop
		return result, nil
	}

	result, err := vm.ChildrenCall(limit.GetChildren(0), proc, anal)
	if err != nil {
		return result, err
	}
	anal.Input(result.Batch, limit.GetIsFirst())

	if result.Batch == nil || result.Batch.IsEmpty() || result.Batch.Last() {
		return result, nil
	}
	bat := result.Batch

	if limit.ctr.seen >= limit.ctr.limit {
		result.Batch = nil
		result.Status = vm.ExecStop
		return result, nil
	}
	length := bat.RowCount()
	newSeen := limit.ctr.seen + uint64(length)
	if newSeen >= limit.ctr.limit { // limit - seen
		batch.SetLength(bat, int(limit.ctr.limit-limit.ctr.seen))
		limit.ctr.seen = newSeen
		anal.Output(bat, limit.GetIsLast())

		result.Status = vm.ExecStop
		return result, nil
	}
	anal.Output(bat, limit.GetIsLast())
	limit.ctr.seen = newSeen
	return result, nil
}
