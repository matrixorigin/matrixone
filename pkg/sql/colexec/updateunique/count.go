// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package updateunique

import (
	"bytes"
	"context"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Count owns the logical affected-row count for one normalized target. It is
// placed on the target's sole local producer before its writer fan-out. The
// callback records a provisional count in the current compilation attempt;
// it runs once after successful input EOF, never on reset or failure.
type Count struct {
	Column  int
	Publish func(uint64)

	total    uint64
	finished bool
	vm.OperatorBase
}

var _ vm.Operator = (*Count)(nil)

func (op *Count) GetOperatorBase() *vm.OperatorBase { return &op.OperatorBase }
func (op *Count) OpType() vm.OpType                 { return vm.UpdateCount }
func (op *Count) TypeName() string                  { return "update_count" }
func (op *Count) String(buf *bytes.Buffer)          { buf.WriteString("update_count") }
func (op *Count) Release()                          {}
func (op *Count) ExecProjection(_ *process.Process, input *batch.Batch) (*batch.Batch, error) {
	return input, nil
}

func (op *Count) Prepare(_ *process.Process) error {
	if op.Publish == nil || op.Column < 0 {
		return moerr.NewInternalErrorNoCtx("invalid repeated UPDATE count owner")
	}
	if op.OpAnalyzer == nil {
		op.OpAnalyzer = process.NewAnalyzer(op.GetIdx(), op.IsFirst, op.IsLast, "update_count")
	} else {
		op.OpAnalyzer.Reset()
	}
	return nil
}

func (op *Count) Call(proc *process.Process) (vm.CallResult, error) {
	if op.finished {
		return vm.CallResult{Status: vm.ExecStop}, nil
	}
	input, err := vm.ChildrenCall(op.GetChildren(0), proc, op.OpAnalyzer)
	if err != nil {
		return vm.CancelResult, err
	}
	if input.Batch == nil {
		if err = context.Cause(proc.Ctx); err != nil {
			return vm.CancelResult, err
		}
		op.finished = true
		op.Publish(op.total)
		return input, nil
	}
	if input.Batch.Last() {
		return vm.CancelResult, moerr.NewInternalErrorNoCtx("unexpected recursive batch in update count")
	}
	if input.Batch.IsEmpty() {
		return input, nil
	}
	if op.Column >= len(input.Batch.Vecs) || input.Batch.Vecs[op.Column] == nil ||
		input.Batch.Vecs[op.Column].GetType().Oid != types.T_uint64 || input.Batch.Vecs[op.Column].Length() < input.Batch.RowCount() {
		return vm.CancelResult, moerr.NewInternalErrorNoCtx("invalid repeated UPDATE count column")
	}
	counts := input.Batch.Vecs[op.Column]
	for i := 0; i < input.Batch.RowCount(); i++ {
		if i%1024 == 0 {
			if err = context.Cause(proc.Ctx); err != nil {
				return vm.CancelResult, err
			}
		}
		if counts.IsNull(uint64(i)) {
			return vm.CancelResult, moerr.NewInternalErrorNoCtx("null repeated UPDATE count")
		}
		count := vector.GetFixedAtNoTypeCheck[uint64](counts, i)
		if count > math.MaxUint64-op.total {
			return vm.CancelResult, moerr.NewInternalErrorNoCtx("repeated UPDATE count overflow")
		}
		op.total += count
	}
	return input, nil
}

func (op *Count) Reset(_ *process.Process, _ bool, _ error) { op.total, op.finished = 0, false }
func (op *Count) Free(_ *process.Process, _ bool, _ error)  { op.total, op.finished = 0, false }
