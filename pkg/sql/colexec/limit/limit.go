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
	if limit.OpAnalyzer == nil {
		limit.OpAnalyzer = process.NewAnalyzer(limit.GetIdx(), limit.IsFirst, limit.IsLast, "limit")
	} else {
		limit.OpAnalyzer.Reset()
	}

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
	limit.ctr.limit = uint64(vector.MustFixedColWithTypeCheck[uint64](vec)[0])
	// do not free the vector from executor.Eval after used.
	// should use executor.Free to free it in Operator.Free()

	return nil
}

// Call returning only the first n tuples from its input
func (limit *Limit) Call(proc *process.Process) (vm.CallResult, error) {
	drainForFoundRows := limit.drainInputForFoundRows && proc.IsSqlCalcFoundRows()
	if drainForFoundRows && limit.ctr.draining {
		return limit.drainForFoundRows(proc)
	}
	if limit.ctr.seen >= limit.ctr.limit {
		if drainForFoundRows {
			limit.ctr.draining = true
			return limit.drainForFoundRows(proc)
		}
		result := vm.NewCallResult()
		result.Status = vm.ExecStop
		return result, nil
	}

	analyzer := limit.OpAnalyzer

	result, err := vm.ChildrenCall(limit.GetChildren(0), proc, analyzer)
	if err != nil {
		return result, err
	}

	if result.Batch == nil {
		if limit.calcFoundRows && proc.IsSqlCalcFoundRows() && !proc.FoundRowsRecorded() {
			proc.SetFoundRows(limit.ctr.seen)
		}
		return result, nil
	}
	if result.Batch.IsEmpty() || result.Batch.Last() {
		return result, nil
	}
	bat := result.Batch
	length := bat.RowCount()
	newSeen := limit.ctr.seen + uint64(length)
	if newSeen >= limit.ctr.limit { // limit - seen
		if limit.ctr.buf != nil {
			limit.ctr.buf.CleanOnlyData()
		}
		limit.ctr.buf, err = limit.ctr.buf.AppendWithCopy(proc.Ctx, proc.Mp(), bat)
		if err != nil {
			return vm.CancelResult, err
		}
		limit.ctr.buf.Attrs = append(limit.ctr.buf.Attrs[:0], bat.Attrs...)
		limit.ctr.buf.Recursive = bat.Recursive
		limit.ctr.buf.ShuffleIDX = bat.ShuffleIDX
		limit.ctr.buf.SetRowCount(bat.RowCount())
		batch.SetLength(limit.ctr.buf, int(limit.ctr.limit-limit.ctr.seen))
		result.Batch = limit.ctr.buf
		if drainForFoundRows {
			limit.ctr.draining = true
			result.Status = vm.ExecNext
		} else {
			result.Status = vm.ExecStop
		}
	}
	limit.ctr.seen = newSeen
	return result, nil
}

func (limit *Limit) drainForFoundRows(proc *process.Process) (vm.CallResult, error) {
	for {
		result, err := vm.ChildrenCall(limit.GetChildren(0), proc, limit.OpAnalyzer)
		if err != nil {
			return result, err
		}
		if result.Batch == nil {
			if limit.calcFoundRows && !proc.FoundRowsRecorded() {
				proc.SetFoundRows(limit.ctr.seen)
			}
			result.Status = vm.ExecStop
			return result, nil
		}
		if result.Batch.Last() {
			if limit.calcFoundRows && !proc.FoundRowsRecorded() {
				proc.SetFoundRows(limit.ctr.seen)
			}
			return result, nil
		}
		if result.Batch.IsEmpty() {
			continue
		}
		limit.ctr.seen += uint64(result.Batch.RowCount())
	}
}
