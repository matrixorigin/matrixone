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
	if limit.limitExecutor == nil {
		limit.limitExecutor, err = colexec.NewExpressionExecutor(proc, limit.LimitExpr)
		if err != nil {
			return err
		}
	}

	vec, err := limit.limitExecutor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
	if err != nil {
		return err
	}
	limit.limit = uint64(vector.MustFixedCol[uint64](vec)[0])
	limit.seen = 0

	return nil
}

// Call returning only the first n tuples from its input
func (limit *Limit) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	anal := proc.GetAnalyze(limit.GetIdx(), limit.GetParallelIdx(), limit.GetParallelMajor())
	if limit.seen >= limit.limit {
		return vm.CallResult{}, nil
	}

	// Get the input
	result, err := limit.GetChildren(0).Call(proc)
	if err != nil || result.Done() {
		return result, err
	}

	anal.Start()
	defer anal.Stop()

	anal.Input(result.Batch, limit.GetIsFirst())

	length := result.Batch.RowCount()
	newSeen := limit.seen + uint64(length)
	if newSeen >= limit.limit { // limit - seen
		// Truncate the batch, and set result to stop.
		// Note that truncate batch is OK, because we did not touch the data in vectors
		// even though we do touch the length of vectors.
		batch.SetLength(result.Batch, int(limit.limit-limit.seen))
		result.Status = vm.ExecStop
	}

	limit.seen = newSeen
	anal.Output(result.Batch, limit.GetIsLast())
	return result, nil
}
