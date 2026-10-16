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
	"context"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// DependentInvocation exposes a provisional, owning result to its caller before
// Close releases child buffers. The expression frame copies it in Accept.
type DependentInvocation interface {
	vm.Continuation
	Result() *batch.Batch
}

type DependentProgramFactory interface {
	Open(*process.Process, *plan.DependentExpr, []*vector.Vector, []*vector.Vector) (DependentInvocation, error)
}

type dependentExprInstruction struct {
	expr   *plan.Expr
	args   []int
	typ    types.Type
	fid    int32
	native func(*process.Process, *vector.AllocationAccountSelection, []ExpressionExecutor) (ExpressionExecutor, error)
}

// DependentExpressionProgram contains only immutable bound instructions. Native
// overload selection happens here, never when a row requests a subprogram.
type DependentExpressionProgram struct {
	instructions []dependentExprInstruction
	roots        []int
}

func CompileDependentExpressions(ctx context.Context, roots []*plan.Expr) (*DependentExpressionProgram, error) {
	result := &DependentExpressionProgram{}
	indexes := make(map[*plan.Expr]int)
	memos := make(map[int32]int)
	active := make(map[*plan.Expr]bool)
	type cursor struct {
		expr *plan.Expr
		args []*plan.Expr
		next int
	}
	for _, root := range roots {
		if root == nil {
			return nil, moerr.NewInternalError(ctx, "nil dependent expression")
		}
		pending := []cursor{{expr: root}}
		for len(pending) > 0 {
			top := &pending[len(pending)-1]
			if _, ok := indexes[top.expr]; ok {
				pending = pending[:len(pending)-1]
				continue
			}
			if top.expr.AuxId < 0 {
				if index, ok := memos[top.expr.AuxId]; ok {
					indexes[top.expr] = index
					pending = pending[:len(pending)-1]
					continue
				}
			}
			if !active[top.expr] {
				active[top.expr] = true
				if f := top.expr.GetF(); f != nil {
					top.args = f.Args
				}
				if l := top.expr.GetList(); l != nil {
					top.args = l.List
				}
				if d := top.expr.GetDependent(); d != nil {
					top.args = append(append([]*plan.Expr(nil), d.Inputs...), d.Lhs...)
				}
			}
			if top.next < len(top.args) {
				child := top.args[top.next]
				top.next++
				if child == nil {
					return nil, moerr.NewInternalError(ctx, "nil dependent operand")
				}
				if _, ok := indexes[child]; ok {
					continue
				}
				if active[child] {
					return nil, moerr.NewInternalError(ctx, "dependent expression cycle")
				}
				pending = append(pending, cursor{expr: child})
				continue
			}
			instruction := dependentExprInstruction{expr: top.expr, fid: -1, args: make([]int, len(top.args))}
			for i, arg := range top.args {
				instruction.args[i] = indexes[arg]
			}
			typ, err := types.TypeFromPlan(top.expr.Typ)
			if err != nil {
				return nil, err
			}
			instruction.typ = typ
			if f := top.expr.GetF(); f != nil {
				if f.Func == nil {
					return nil, moerr.NewInternalError(ctx, "missing native function")
				}
				overload, err := function.GetFunctionById(ctx, f.Func.Obj)
				if err != nil {
					return nil, err
				}
				fid, _ := function.DecodeOverloadID(f.Func.Obj)
				instruction.fid = fid
				parameterTypes := make([]types.Type, len(f.Args))
				for i, arg := range f.Args {
					parameterTypes[i], err = types.TypeFromPlan(arg.Typ)
					if err != nil {
						return nil, err
					}
				}
				overloadID := f.Func.Obj
				stringToNumericCast := !overload.CannotFold() && isStringToNumericCast(top.expr, false)
				instruction.native = func(proc *process.Process, allocation *vector.AllocationAccountSelection, args []ExpressionExecutor) (ExpressionExecutor, error) {
					executor := NewFunctionExpressionExecutor()
					executor.overloadID = overloadID
					executor.fid = fid
					executor.stringToNumericCast = stringToNumericCast
					executor.timeDependent = overload.IsRealTimeRelated()
					executor.volatile = true // Frame demand already owns memoization.
					executor.folded.needFoldingCheck = false
					executor.evalFn, executor.resetFn, executor.freeFn, executor.retainedBytesFn = overload.GetExecuteMethod()
					if err := executor.init(proc, len(args), typ, allocation); err != nil {
						executor.Free()
						return nil, err
					}
					executor.parameterTypes = parameterTypes
					copy(executor.parameterExecutor, args)
					return executor, nil
				}
			} else if top.expr.GetDependent() == nil && top.expr.GetCol() == nil {
				if top.expr.GetList() != nil || top.expr.GetSub() != nil || top.expr.GetCorr() != nil || top.expr.GetW() != nil {
					return nil, moerr.NewInternalError(ctx, "unlowered dependent scalar expression")
				}
				expr := top.expr
				instruction.native = func(proc *process.Process, allocation *vector.AllocationAccountSelection, _ []ExpressionExecutor) (ExpressionExecutor, error) {
					return NewExpressionExecutorWithAllocation(proc, expr, allocation)
				}
			}
			index := len(result.instructions)
			indexes[top.expr] = index
			if top.expr.AuxId < 0 {
				memos[top.expr.AuxId] = index
			}
			result.instructions = append(result.instructions, instruction)
			delete(active, top.expr)
			pending = pending[:len(pending)-1]
		}
		result.roots = append(result.roots, indexes[root])
	}
	return result, nil
}

// CompileDependentSingleInputExpressions adapts the native single-batch ABI.
// Callers must supply exactly one input batch when creating a frame.
// Relation coordinates are ignored, but column positions remain strict.
// Compile first so malformed/cyclic expressions retain normal validation.
// Copy only column instructions; never mutate the bound query or a reducer's
// tuple comparison. Already-normalized columns retain their original AST.
// Adaptation allocates only during compilation, not for each execution row.
func CompileDependentSingleInputExpressions(ctx context.Context, roots []*plan.Expr) (*DependentExpressionProgram, error) {
	program, err := CompileDependentExpressions(ctx, roots)
	if err != nil {
		return nil, err
	}
	for i := range program.instructions {
		instruction := &program.instructions[i]
		if col := instruction.expr.GetCol(); col != nil && col.RelPos != 0 {
			expr, reference := *instruction.expr, *col
			reference.RelPos = 0
			expr.Expr = &plan.Expr_Col{Col: &reference}
			instruction.expr = &expr
		}
	}
	return program, nil
}

// DependentInput names a real stage row, including each side of a JOIN pair.
// Its batch is borrowed under the stage's input lease until Frame.Close.
type DependentInput struct {
	Batch *batch.Batch
	Row   int
}

type dependentExprCell struct {
	value    *vector.Vector
	owned    bool
	ready    bool
	executor ExpressionExecutor
}

type DependentExpressionFrame struct {
	program    *DependentExpressionProgram
	proc       *process.Process
	factory    DependentProgramFactory
	allocation *vector.AllocationAccountSelection
	budget     *process.ExecutionResourceGeneration
	inputs     []DependentInput
	cells      []dependentExprCell
	charged    uint64
	failure    error
	closed     bool
}

func (p *DependentExpressionProgram) NewFrame(proc *process.Process, inputs []DependentInput, factory DependentProgramFactory, allocation *vector.AllocationAccountSelection) (*DependentExpressionFrame, error) {
	if p == nil || proc == nil || allocation == nil {
		return nil, moerr.NewInternalErrorNoCtx("dependent frame requires owner")
	}
	budget, err := proc.GetExecutionResourceBudget()
	if err != nil {
		return nil, err
	}
	size := uint64(unsafe.Sizeof(DependentExpressionFrame{})) + uint64(len(p.instructions))*uint64(unsafe.Sizeof(dependentExprCell{})) + uint64(len(inputs))*uint64(unsafe.Sizeof(DependentInput{}))
	if err = budget.AcquireAllocationCapacity(size); err != nil {
		return nil, err
	}
	return &DependentExpressionFrame{program: p, proc: proc, factory: factory, allocation: allocation, budget: budget, inputs: append([]DependentInput(nil), inputs...), cells: make([]dependentExprCell, len(p.instructions)), charged: size}, nil
}

func (f *DependentExpressionFrame) Task(root int) (vm.Continuation, error) {
	if f.closed || root < 0 || root >= len(f.program.roots) {
		return nil, moerr.NewInternalErrorNoCtx("invalid dependent root")
	}
	if f.failure != nil {
		return nil, f.failure
	}
	return f.task(f.program.roots[root])
}
func (f *DependentExpressionFrame) task(index int) (vm.Continuation, error) {
	size := uint64(unsafe.Sizeof(dependentExpressionTask{}))
	if err := f.budget.AcquireAllocationCapacity(size); err != nil {
		return nil, err
	}
	return &dependentExpressionTask{frame: f, index: index, charged: size}, nil
}
func (f *DependentExpressionFrame) Value(root int) (*vector.Vector, error) {
	if f.closed || f.failure != nil || root < 0 || root >= len(f.program.roots) {
		return nil, moerr.NewInternalErrorNoCtx("dependent frame has no successful result")
	}
	cell := &f.cells[f.program.roots[root]]
	if !cell.ready {
		return nil, moerr.NewInternalErrorNoCtx("dependent result not ready")
	}
	return cell.value, nil
}
func (f *DependentExpressionFrame) Close() {
	if f == nil || f.closed {
		return
	}
	f.closed = true
	for i := range f.cells {
		c := &f.cells[i]
		if c.executor != nil {
			c.executor.Free()
		}
		if c.owned && c.value != nil {
			c.value.Free(f.proc.Mp())
		}
	}
	f.cells = nil
	f.inputs = nil
	f.budget.ReleaseAllocationCapacity(f.charged)
	f.charged = 0
}

type dependentExpressionTask struct {
	frame    *DependentExpressionFrame
	index    int
	next     int
	waiting  bool
	selected bool
	charged  uint64
	captures []*vector.Vector
}

func (t *dependentExpressionTask) Step(ctx context.Context) (vm.ContinuationStep, error) {
	f := t.frame
	instruction := &f.program.instructions[t.index]
	cell := &f.cells[t.index]
	if cell.ready {
		return vm.ContinuationStep{Done: true}, nil
	}
	if f.failure != nil {
		return vm.ContinuationStep{}, f.failure
	}
	// The selected branch is recorded in this PC before it yields. Resuming
	// never repeats a condition, native function or dependent invocation.
	for t.next < len(instruction.args) {
		arg := instruction.args[t.next]
		if !f.cells[arg].ready {
			child, err := f.task(arg)
			return vm.ContinuationStep{Child: child}, err
		}
		value := f.cells[arg].value
		switch instruction.fid {
		case function.IFF:
			if t.next == 0 {
				truth, err := function.IffConditionTruthyAt(value, 0, function.CompatibilityModeFromProcess(f.proc))
				if err != nil {
					return vm.ContinuationStep{}, err
				}
				t.next = 2
				if truth {
					t.next = 1
				}
				t.selected = true
			} else {
				t.next = len(instruction.args)
			}
		case function.CASE:
			if t.selected || t.next == len(instruction.args)-1 {
				t.next = len(instruction.args)
				break
			}
			truth := !value.IsNull(0) && vector.GetFixedAtNoTypeCheck[bool](value, 0)
			if truth {
				t.selected = true
				t.next++
			} else {
				t.next += 2
			}
		case function.COALESCE:
			if !value.IsNull(0) {
				t.next = len(instruction.args)
			} else {
				t.next++
			}
		default:
			t.next++
		}
	}
	if call := instruction.expr.GetDependent(); call != nil {
		if t.waiting {
			return vm.ContinuationStep{}, moerr.NewInternalError(ctx, "dependent child completed without result")
		}
		if f.factory == nil {
			return vm.ContinuationStep{}, moerr.NewInternalError(ctx, "missing dependent program factory")
		}
		size := uint64(len(instruction.args)) * uint64(unsafe.Sizeof((*vector.Vector)(nil)))
		if err := f.budget.AcquireAllocationCapacity(size); err != nil {
			return vm.ContinuationStep{}, err
		}
		t.charged += size
		t.captures = make([]*vector.Vector, len(instruction.args))
		inputs := t.captures[:len(call.Inputs)]
		lhs := t.captures[len(call.Inputs):]
		for i := range inputs {
			inputs[i] = f.cells[instruction.args[i]].value
		}
		for i := range lhs {
			lhs[i] = f.cells[instruction.args[len(inputs)+i]].value
		}
		child, err := f.factory.Open(f.proc, call, inputs, lhs)
		t.waiting = err == nil
		return vm.ContinuationStep{Child: child}, err
	}
	if col := instruction.expr.GetCol(); col != nil {
		if col.RelPos < 0 || int(col.RelPos) >= len(f.inputs) {
			return vm.ContinuationStep{}, moerr.NewInternalError(ctx, "dependent input relation")
		}
		input := f.inputs[col.RelPos]
		if input.Batch == nil || input.Row < 0 || input.Row >= input.Batch.RowCount() || col.ColPos < 0 || int(col.ColPos) >= len(input.Batch.Vecs) {
			return vm.ContinuationStep{}, moerr.NewInternalError(ctx, "dependent input row or column")
		}
		typ := instruction.typ
		if err := f.reserve(uint64(unsafe.Sizeof(vector.Vector{}))); err != nil {
			return vm.ContinuationStep{}, err
		}
		var err error
		cell.value, err = vector.NewOffHeapVecWithTypeAndAllocation(typ, f.allocation)
		if err != nil {
			return vm.ContinuationStep{}, err
		}
		cell.owned = true
		set := vector.GetConstSetFunction(typ, f.proc.Mp())
		if err = set(cell.value, input.Batch.Vecs[col.ColPos], int64(input.Row), 1); err != nil {
			return vm.ContinuationStep{}, err
		}
	} else {
		// Native argument wrappers live until frame cleanup. Reserve both the
		// adapter objects and the native executor's argument arrays up front.
		size := uint64(len(instruction.args)) * uint64(unsafe.Sizeof(dependentValueReference{})+unsafe.Sizeof(vector.Vector{})+2*unsafe.Sizeof(ExpressionExecutor(nil))+unsafe.Sizeof((*vector.Vector)(nil)))
		size += uint64(unsafe.Sizeof(FunctionExpressionExecutor{}))
		if err := f.reserve(size); err != nil {
			return vm.ContinuationStep{}, err
		}
		args := make([]ExpressionExecutor, len(instruction.args))
		transferred := false
		defer func() {
			if !transferred {
				for _, arg := range args {
					if arg != nil {
						arg.Free()
					}
				}
			}
		}()
		for i, index := range instruction.args {
			typ := f.program.instructions[index].typ
			null, err := vector.NewConstNullWithAllocation(typ, 1, f.allocation)
			if err != nil {
				return vm.ContinuationStep{}, err
			}
			args[i] = &dependentValueReference{frame: f, index: index, null: null}
		}
		executor, err := instruction.native(f.proc, f.allocation, args)
		if err != nil {
			return vm.ContinuationStep{}, err
		}
		cell.executor = executor
		transferred = true
		cell.value, err = executor.Eval(f.proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
		if err != nil {
			return vm.ContinuationStep{}, err
		}
	}
	cell.ready = true
	return vm.ContinuationStep{Done: true}, nil
}

func (t *dependentExpressionTask) Accept(child vm.Continuation) error {
	invocation, ok := child.(DependentInvocation)
	if !ok {
		return nil
	} // An expression child published into the same frame.
	f := t.frame
	cell := &f.cells[t.index]
	result := invocation.Result()
	if result == nil || result.RowCount() != 1 || len(result.Vecs) != 1 {
		return moerr.NewInternalErrorNoCtx("invalid dependent invocation result")
	}
	typ := f.program.instructions[t.index].typ
	if !typ.Eq(*result.Vecs[0].GetType()) {
		return moerr.NewInternalErrorNoCtx("dependent invocation result type")
	}
	if err := f.reserve(uint64(unsafe.Sizeof(vector.Vector{}))); err != nil {
		return err
	}
	var err error
	cell.value, err = vector.NewOffHeapVecWithTypeAndAllocation(typ, f.allocation)
	if err != nil {
		return err
	}
	cell.owned = true
	if err = cell.value.UnionOne(result.Vecs[0], 0, f.proc.Mp()); err != nil {
		return err
	}
	cell.ready = true
	return nil
}
func (t *dependentExpressionTask) Close(err error) error {
	if err != nil && t.frame.failure == nil {
		t.frame.failure = err
	}
	t.frame.budget.ReleaseAllocationCapacity(t.charged)
	t.charged = 0
	t.captures = nil
	return nil
}

func (f *DependentExpressionFrame) reserve(size uint64) error {
	if err := f.budget.AcquireAllocationCapacity(size); err != nil {
		return err
	}
	f.charged += size
	return nil
}

type dependentValueReference struct {
	frame *DependentExpressionFrame
	index int
	null  *vector.Vector
}

func (r *dependentValueReference) Eval(_ *process.Process, _ []*batch.Batch, mask []bool) (*vector.Vector, error) {
	if noRowsSelected(mask, 1) {
		return r.null, nil
	}
	cell := &r.frame.cells[r.index]
	if !cell.ready {
		return nil, moerr.NewInternalErrorNoCtx("native function demanded an unselected operand")
	}
	return cell.value, nil
}
func (r *dependentValueReference) EvalWithoutResultReusing(proc *process.Process, b []*batch.Batch, mask []bool) (*vector.Vector, error) {
	return r.Eval(proc, b, mask)
}
func (r *dependentValueReference) Free() {
	if r.null != nil {
		r.null.Free(r.frame.proc.Mp())
		r.null = nil
	}
}
func (r *dependentValueReference) ResetForNextQuery() {}
func (r *dependentValueReference) IsColumnExpr() bool { return true }
func (r *dependentValueReference) TypeName() string   { return "dependent frame value" }

// NewScalarExecutor instances a bound comparison DAG without overload lookup.
// Every instruction has one owner; parameter edges borrow it, including shared
// tuple operands. This entry point deliberately excludes relational demand.
func (p *DependentExpressionProgram) NewScalarExecutor(proc *process.Process, allocation *vector.AllocationAccountSelection) (ExpressionExecutor, error) {
	if len(p.roots) != 1 {
		return nil, moerr.NewInternalErrorNoCtx("scalar program requires one root")
	}
	budget, err := proc.GetExecutionResourceBudget()
	if err != nil {
		return nil, err
	}
	size := uint64(unsafe.Sizeof(dependentScalarExecutor{}))
	for _, instruction := range p.instructions {
		if instruction.expr.GetDependent() != nil || instruction.expr.GetSlot() != nil {
			return nil, moerr.NewInternalErrorNoCtx("comparison contains relational demand")
		}
		size += uint64(unsafe.Sizeof(FunctionExpressionExecutor{})) + uint64(unsafe.Sizeof(dependentScalarReference{})) + uint64(unsafe.Sizeof(ExpressionExecutor(nil)))
		size += uint64(len(instruction.args)) * 2 * uint64(unsafe.Sizeof(ExpressionExecutor(nil)))
	}
	if err = budget.AcquireAllocationCapacity(size); err != nil {
		return nil, err
	}
	owner := &dependentScalarExecutor{budget: budget, charged: size, executors: make([]ExpressionExecutor, len(p.instructions))}
	refs := make([]dependentScalarReference, len(p.instructions))
	for i, instruction := range p.instructions {
		args := make([]ExpressionExecutor, len(instruction.args))
		for j, index := range instruction.args {
			args[j] = &refs[index]
		}
		if instruction.native != nil {
			owner.executors[i], err = instruction.native(proc, allocation, args)
		} else {
			owner.executors[i], err = NewExpressionExecutorWithAllocation(proc, instruction.expr, allocation)
		}
		if err != nil {
			owner.Free()
			return nil, err
		}
		refs[i].ExpressionExecutor = owner.executors[i]
	}
	owner.ExpressionExecutor = owner.executors[p.roots[0]]
	return owner, nil
}

type dependentScalarReference struct{ ExpressionExecutor }

func (*dependentScalarReference) Free()              {}
func (*dependentScalarReference) ResetForNextQuery() {}

type dependentScalarExecutor struct {
	ExpressionExecutor
	executors []ExpressionExecutor
	budget    *process.ExecutionResourceGeneration
	charged   uint64
}

func (e *dependentScalarExecutor) Free() {
	for i := len(e.executors) - 1; i >= 0; i-- {
		if e.executors[i] != nil {
			e.executors[i].Free()
		}
	}
	e.executors = nil
	e.ExpressionExecutor = nil
	e.budget.ReleaseAllocationCapacity(e.charged)
	e.charged = 0
}
func (e *dependentScalarExecutor) ResetForNextQuery() {
	for _, executor := range e.executors {
		executor.ResetForNextQuery()
	}
}
