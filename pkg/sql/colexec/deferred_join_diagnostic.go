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
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// DeferredJoinDiagnostic owns diagnostics from statement-constant build keys.
// HashBuild may compute those keys before the probe side has any rows. The
// coordinator HashJoin publishes them only after observing both input sides.
// It is used by one non-shuffled join execution at a time.
type DeferredJoinDiagnostic struct {
	mu       sync.Mutex
	warnings process.WarningAccumulator
	budget   *process.WarningDiagnosticBudget
	limit    int
	err      error
	active   bool
	ready    bool
}

func (d *DeferredJoinDiagnostic) Prepare(proc *process.Process) {
	budget := process.WarningDiagnosticBudgetForProcess(proc)
	limit := process.WarningDiagnosticRetentionLimitForProcess(proc)
	d.mu.Lock()
	defer d.mu.Unlock()
	d.warnings.Reset()
	d.budget = budget
	d.limit = limit
	d.warnings.SetWarningRetentionLimit(limit)
	d.warnings.SetWarningBudget(budget)
	d.err = nil
	d.active = false
	d.ready = true
}

func (d *DeferredJoinDiagnostic) Reset() {
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.warnings.Reset()
	d.budget = nil
	d.err = nil
	d.active = false
	d.ready = false
}

func (d *DeferredJoinDiagnostic) AppendWarningDiagnostic(code uint16, message string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.ready || d.active {
		return
	}
	d.warnings.Add(code, message)
}

func (d *DeferredJoinDiagnostic) AppendWarningCount(total uint64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.ready || d.active {
		return
	}
	d.appendWarningCount(total)
}

func (d *DeferredJoinDiagnostic) appendWarningCount(total uint64) {
	if ^uint64(0)-d.warnings.Total < total {
		d.warnings.Total = ^uint64(0)
	} else {
		d.warnings.Total += total
	}
}

func (d *DeferredJoinDiagnostic) AppendWarningBatch(total uint64, codes []uint16, messages []string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.ready || d.active {
		return
	}
	count := min(len(codes), len(messages))
	if uint64(count) > total {
		count = int(total)
	}
	for i := 0; i < count; i++ {
		d.warnings.Add(codes[i], messages[i])
	}
	d.appendWarningCount(total - uint64(count))
}

func (d *DeferredJoinDiagnostic) GetWarningDiagnosticBudget() *process.WarningDiagnosticBudget {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.budget
}

func (d *DeferredJoinDiagnostic) GetWarningRetentionLimit() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.limit
}

func (d *DeferredJoinDiagnostic) captureProcess(proc *process.Process) *process.Process {
	child := proc.NewNoContextChildProc(0)
	child.Ctx = proc.Ctx
	child.WarningSink = d
	return child
}

func (d *DeferredJoinDiagnostic) recordError(err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.ready && !d.active && d.err == nil {
		d.err = err
	}
}

func (d *DeferredJoinDiagnostic) canDeferError() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.ready && !d.active
}

func (d *DeferredJoinDiagnostic) isActive() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.active
}

// Activate is called once the join knows the build side and the current probe
// batch both contain rows. A NULL key or zero matches do not suppress the
// logical evaluation of a selected constant ON operand.
func (d *DeferredJoinDiagnostic) Activate(proc *process.Process) error {
	if d == nil {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.ready || d.active {
		return nil
	}
	d.active = true
	if d.err != nil {
		d.warnings.Reset()
		return d.err
	}
	d.warnings.Flush(proc)
	return nil
}

// Only SQL evaluation errors may wait for logical ON activation. Resource,
// cancellation, and internal failures must still fail the build immediately.
func deferJoinExpressionError(err error) bool {
	for _, code := range [...]uint16{
		moerr.ErrDivByZero,
		moerr.ErrOutOfRange,
		moerr.ErrDataTruncated,
		moerr.ErrInvalidArg,
		moerr.ErrTruncatedWrongValueForField,
		moerr.ErrTruncatedWrongValue,
		moerr.ErrInvalidInput,
		moerr.ErrWrongDatetimeSpec,
		moerr.ErrWrongArguments,
	} {
		if moerr.IsMoErrCode(err, code) {
			return true
		}
	}
	return false
}

// deferredJoinConstantExecutor isolates only a statement-constant subtree.
// Row-dependent build-key conversions keep their ordinary diagnostic path.
type deferredJoinConstantExecutor struct {
	executor  ExpressionExecutor
	owner     *DeferredJoinDiagnostic
	typ       types.Type
	selection *vector.AllocationAccountSelection
	null      *vector.Vector
	captured  *process.Process
	failed    bool
}

func (e *deferredJoinConstantExecutor) capturedProcess(proc *process.Process) *process.Process {
	// Residual expressions can first be selected after the JOIN has activated.
	// Their diagnostics belong to the live statement sink at that point.
	if e.owner.isActive() {
		return proc
	}
	if e.captured == nil {
		e.captured = e.owner.captureProcess(proc)
	}
	return e.captured
}

func (e *deferredJoinConstantExecutor) nullResult(rows int) (*vector.Vector, error) {
	if e.null == nil {
		var err error
		e.null, err = newExpressionConstNull(e.typ, rows, e.selection)
		if err != nil {
			return nil, err
		}
	} else {
		e.null.SetLength(rows)
	}
	return e.null, nil
}

func (e *deferredJoinConstantExecutor) Eval(
	proc *process.Process, batches []*batch.Batch, selectList []bool,
) (*vector.Vector, error) {
	rows := expressionRowCount(batches)
	if e.failed || noRowsSelected(selectList, rows) {
		return e.nullResult(rows)
	}
	value, err := e.executor.Eval(e.capturedProcess(proc), batches, selectList)
	if err == nil || !deferJoinExpressionError(err) || !e.owner.canDeferError() {
		return value, err
	}
	e.owner.recordError(err)
	e.failed = true
	return e.nullResult(rows)
}

func (e *deferredJoinConstantExecutor) EvalWithoutResultReusing(
	proc *process.Process, batches []*batch.Batch, selectList []bool,
) (*vector.Vector, error) {
	rows := expressionRowCount(batches)
	if e.failed || noRowsSelected(selectList, rows) {
		value, err := e.nullResult(rows)
		if err != nil {
			return nil, err
		}
		return value.Dup(proc.Mp())
	}
	value, err := e.executor.EvalWithoutResultReusing(e.capturedProcess(proc), batches, selectList)
	if err == nil || !deferJoinExpressionError(err) || !e.owner.canDeferError() {
		return value, err
	}
	e.owner.recordError(err)
	e.failed = true
	value, err = e.nullResult(rows)
	if err != nil {
		return nil, err
	}
	return value.Dup(proc.Mp())
}

func (e *deferredJoinConstantExecutor) ResetForNextQuery() {
	e.failed = false
	e.captured = nil
	e.executor.ResetForNextQuery()
}

func (e *deferredJoinConstantExecutor) Free() {
	e.executor.Free()
	e.captured = nil
	if e.null != nil {
		e.null.Free(nil)
		e.null = nil
	}
}

func (e *deferredJoinConstantExecutor) IsColumnExpr() bool { return false }
func (e *deferredJoinConstantExecutor) TypeName() string   { return e.executor.TypeName() }
