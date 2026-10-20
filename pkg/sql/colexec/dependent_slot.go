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

package colexec

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type dependentSlotExecutor struct {
	slot  plan.CorrelationSlot
	value *vector.Vector
	mp    *mpool.MPool
}

func newDependentSlotExecutor(proc *process.Process, slot *plan.CorrelationSlot, typ types.Type, allocation *vector.AllocationAccountSelection) (ExpressionExecutor, error) {
	if slot == nil {
		return nil, moerr.NewInternalErrorNoCtx("dependent slot requires allocation owner")
	}
	if allocation == nil {
		// Native operators may use the ordinary executor constructor. Their
		// private slot still belongs to the active invocation's captured value.
		source, err := proc.DependentSlot(slot.ProgramId, slot.Ordinal)
		if err != nil {
			return nil, err
		}
		allocation = source.AllocationAccountSelection()
		if allocation == nil {
			return nil, moerr.NewInternalErrorNoCtx("dependent slot requires allocation owner")
		}
	}
	value, err := vector.NewOffHeapVecWithTypeAndAllocation(typ, allocation)
	if err != nil {
		return nil, err
	}
	return &dependentSlotExecutor{slot: *slot, value: value, mp: proc.Mp()}, nil
}

func (e *dependentSlotExecutor) Eval(proc *process.Process, batches []*batch.Batch, selection []bool) (*vector.Vector, error) {
	if e.value == nil {
		return nil, moerr.NewInternalErrorNoCtx("dependent slot executor is closed")
	}
	n := 1
	if len(batches) > 0 && batches[0] != nil {
		n = batches[0].RowCount()
	}
	if noRowsSelected(selection, n) {
		if err := vector.SetConstNull(e.value, n, e.mp); err != nil {
			return nil, err
		}
		return e.value, nil
	}
	source, err := proc.DependentSlot(e.slot.ProgramId, e.slot.Ordinal)
	if err != nil {
		return nil, err
	}
	if !source.GetType().Eq(*e.value.GetType()) {
		return nil, moerr.NewInternalErrorNoCtx("dependent slot type changed")
	}
	set := vector.GetConstSetFunction(*source.GetType(), e.mp)
	if err = set(e.value, source, 0, n); err != nil {
		return nil, err
	}
	return e.value, nil
}
func (e *dependentSlotExecutor) EvalWithoutResultReusing(proc *process.Process, batches []*batch.Batch, selection []bool) (*vector.Vector, error) {
	value, err := e.Eval(proc, batches, selection)
	if err != nil {
		return nil, err
	}
	return value.DupOffHeapWithAllocation(e.mp, e.value.AllocationAccountSelection())
}
func (e *dependentSlotExecutor) ResetForNextQuery() {
	if e.value != nil {
		e.value.CleanOnlyData()
	}
}
func (e *dependentSlotExecutor) Free() {
	if e.value != nil {
		e.value.Free(e.mp)
		e.value = nil
	}
}
func (e *dependentSlotExecutor) IsColumnExpr() bool { return false }
func (e *dependentSlotExecutor) TypeName() string   { return "dependent slot" }
