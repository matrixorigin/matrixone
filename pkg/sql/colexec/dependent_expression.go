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

	"github.com/gogo/protobuf/proto"
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
	expr                 *plan.Expr
	args                 []int
	typ                  types.Type
	fid                  int32
	numericKindDemand    bool
	inferNumericKind     bool
	numericKindFrom      int
	privateValueAlias    bool
	privateValueFrom     int
	privateProgramScope  int64
	dependentKindCapture bool
	native               func(*process.Process, *vector.AllocationAccountSelection, []ExpressionExecutor) (ExpressionExecutor, error)
}

// DependentExpressionProgram contains only immutable bound instructions. Native
// overload selection happens here, never when a row requests a subprogram.
type DependentExpressionProgram struct {
	instructions              []dependentExprInstruction
	roots                     []int
	numericKindRefs           map[int]int
	numericProgramDefinitions map[int64]*plan.Expr
}

type dependentNumericProgramOwner struct {
	scope    int64
	raw      int
	kind     int
	value    int
	explicit bool
}

func CompileDependentExpressions(ctx context.Context, roots []*plan.Expr) (*DependentExpressionProgram, error) {
	return compileDependentExpressions(ctx, roots, nil)
}

// CompileDependentExpressionsWithCorrelation binds scalar dependent result
// layouts together with their expression instructions. A marked program has
// a physical BOOL passenger in addition to its logical scalar value.
func CompileDependentExpressionsWithCorrelation(
	ctx context.Context, roots []*plan.Expr, programs []*plan.CorrelationProgram,
) (*DependentExpressionProgram, error) {
	return compileDependentExpressions(ctx, roots, programs)
}

func compileDependentExpressions(
	ctx context.Context, roots []*plan.Expr, programs []*plan.CorrelationProgram,
) (*DependentExpressionProgram, error) {
	type programOwner struct {
		definition *plan.Expr
		raw        *plan.Expr
		explicit   *plan.Expr
		valueType  *plan.Type
		state      uint8
		valueIndex int
		kindIndex  int
	}
	result := &DependentExpressionProgram{
		numericKindRefs:           make(map[int]int),
		numericProgramDefinitions: make(map[int64]*plan.Expr),
	}
	owners := make(map[int64]*programOwner)
	var refs []*plan.Expr
	for _, root := range roots {
		if root == nil {
			return nil, moerr.NewInternalError(ctx, "nil dependent expression")
		}
		if err := plan.VisitExprTree(root, func(expr *plan.Expr) error {
			f := expr.GetF()
			if f == nil || f.Func == nil {
				return nil
			}
			fid, _ := function.DecodeOverloadID(f.Func.Obj)
			if fid != function.SCALAR_PROGRAM_REF {
				return nil
			}
			scope, slot, definition, err := decodeDependentNumericProgramRef(ctx, expr)
			if err != nil {
				return err
			}
			refs = append(refs, expr)
			if slot != 0 {
				return nil
			}
			defFn := definition.GetF()
			defArgs := defFn.Args
			owner := &programOwner{
				definition: definition,
				raw:        defArgs[1],
				valueType:  &expr.Typ,
				valueIndex: -1,
				kindIndex:  -1,
			}
			if len(defArgs) == 3 {
				owner.explicit = defArgs[2]
			}
			if previous := owners[scope]; previous != nil {
				if !proto.Equal(previous.definition, definition) || previous.valueType == nil ||
					!proto.Equal(previous.valueType, &expr.Typ) {
					return moerr.NewInvalidInput(ctx, "conflicting dependent numeric program definition")
				}
			} else {
				owners[scope] = owner
				result.numericProgramDefinitions[scope] = proto.Clone(definition).(*plan.Expr)
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}
	for _, expr := range refs {
		scope, _, _, err := decodeDependentNumericProgramRef(ctx, expr)
		if err != nil {
			return nil, err
		}
		if owners[scope] == nil {
			return nil, moerr.NewInvalidInput(ctx, "scalar numeric program reference has no lexical definition")
		}
	}

	indexes := make(map[*plan.Expr]int)
	memos := make(map[int32]int)
	active := make(map[*plan.Expr]bool)
	type cursor struct {
		expr *plan.Expr
		args []*plan.Expr
		next int
	}
	var compileExpression func(*plan.Expr) (int, error)
	var compileOwner func(int64) (*programOwner, error)
	compileExpression = func(root *plan.Expr) (int, error) {
		if root == nil {
			return 0, moerr.NewInternalError(ctx, "nil dependent expression")
		}
		if index, ok := indexes[root]; ok {
			return index, nil
		}
		if active[root] {
			return 0, moerr.NewInternalError(ctx, "dependent expression cycle")
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
				if f := top.expr.GetF(); f != nil && f.Func != nil {
					fid, _ := function.DecodeOverloadID(f.Func.Obj)
					if fid == function.SCALAR_PROGRAM_REF {
						scope, slot, _, err := decodeDependentNumericProgramRef(ctx, top.expr)
						if err != nil {
							return 0, err
						}
						owner, err := compileOwner(scope)
						if err != nil {
							return 0, err
						}
						index := owner.valueIndex
						if slot == 1 {
							index = owner.kindIndex
						}
						if index < 0 {
							return 0, moerr.NewInvalidInput(ctx, "incomplete scalar numeric program output")
						}
						indexes[top.expr] = index
						if top.expr.AuxId < 0 {
							memos[top.expr.AuxId] = index
						}
						delete(active, top.expr)
						pending = pending[:len(pending)-1]
						continue
					}
					if fid == function.SCALAR_PROGRAM {
						return 0, moerr.NewInvalidInput(ctx, "scalar numeric program definition must be anchored by output zero")
					}
					top.args = f.Args
				} else if top.expr.GetF() != nil {
					return 0, moerr.NewInternalError(ctx, "missing native function")
				} else if f := top.expr.GetF(); f != nil {
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
					return 0, moerr.NewInternalError(ctx, "nil dependent operand")
				}
				if _, ok := indexes[child]; ok {
					continue
				}
				if active[child] {
					return 0, moerr.NewInternalError(ctx, "dependent expression cycle")
				}
				pending = append(pending, cursor{expr: child})
				continue
			}
			instruction := dependentExprInstruction{
				expr: top.expr, fid: -1, args: make([]int, len(top.args)),
				numericKindFrom: -1, privateValueFrom: -1,
			}
			for i, arg := range top.args {
				instruction.args[i] = indexes[arg]
			}
			typ, err := types.TypeFromPlan(top.expr.Typ)
			if err != nil {
				return 0, err
			}
			instruction.typ = typ
			if f := top.expr.GetF(); f != nil {
				if f.Func == nil {
					return 0, moerr.NewInternalError(ctx, "missing native function")
				}
				overload, err := function.GetFunctionById(ctx, f.Func.Obj)
				if err != nil {
					return 0, err
				}
				fid, _ := function.DecodeOverloadID(f.Func.Obj)
				instruction.fid = fid
				parameterTypes := make([]types.Type, len(f.Args))
				for i, arg := range f.Args {
					parameterTypes[i], err = types.TypeFromPlan(arg.Typ)
					if err != nil {
						return 0, err
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
					return 0, moerr.NewInternalError(ctx, "unlowered dependent scalar expression")
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
			if call := top.expr.GetDependent(); call != nil && call.Reducer == plan.SubqueryRef_SCALAR && call.Comparison == nil &&
				call.ProgramId > 0 && int(call.ProgramId) <= len(programs) && programs[call.ProgramId-1] != nil &&
				programs[call.ProgramId-1].ScalarResultKindColumn == 1 {
				kindIndex := len(result.instructions)
				result.instructions = append(result.instructions, dependentExprInstruction{
					expr: &plan.Expr{Typ: plan.Type{Id: int32(types.T_bool)}},
					fid:  -1, typ: types.T_bool.ToType(), numericKindFrom: -1, privateValueFrom: -1,
					dependentKindCapture: true, privateProgramScope: int64(call.ProgramId),
				})
				result.numericKindRefs[index] = kindIndex
			}
			delete(active, top.expr)
			pending = pending[:len(pending)-1]
		}
		return indexes[root], nil
	}
	compileOwner = func(scope int64) (*programOwner, error) {
		owner := owners[scope]
		if owner == nil {
			return nil, moerr.NewInvalidInput(ctx, "scalar numeric program reference has no lexical definition")
		}
		if owner.state == 1 {
			return nil, moerr.NewInvalidInput(ctx, "cyclic scalar numeric program definition")
		}
		if owner.state == 2 {
			return owner, nil
		}
		owner.state = 1
		rawIndex, err := compileExpression(owner.raw)
		if err != nil {
			owner.state = 0
			return nil, err
		}
		if err = plan.VisitExprTree(owner.raw, func(expr *plan.Expr) error {
			if index, ok := indexes[expr]; ok && index >= 0 && index < len(result.instructions) {
				result.instructions[index].numericKindDemand = true
			}
			return nil
		}); err != nil {
			owner.state = 0
			return nil, err
		}
		kindIndex := -1
		if owner.explicit != nil {
			if owner.explicit.Typ.Id != 10 {
				owner.state = 0
				return nil, moerr.NewInvalidInput(ctx, "scalar numeric program kind source must be BOOL")
			}
			kindIndex, err = compileExpression(owner.explicit)
			if err != nil {
				owner.state = 0
				return nil, err
			}
		} else {
			boolType := types.T_bool.ToType()
			definition := &plan.Expr{Typ: plan.Type{Id: int32(types.T_bool)}}
			kindIndex = len(result.instructions)
			result.instructions = append(result.instructions, dependentExprInstruction{
				expr: definition, args: []int{rawIndex}, typ: boolType, fid: -1,
				inferNumericKind: true, numericKindFrom: rawIndex, privateValueFrom: -1,
				privateProgramScope: scope,
			})
		}
		valueType, err := types.TypeFromPlan(*owner.valueType)
		if err != nil {
			owner.state = 0
			return nil, err
		}
		valueIndex := len(result.instructions)
		result.instructions = append(result.instructions, dependentExprInstruction{
			expr: owner.definition, args: []int{rawIndex, kindIndex}, typ: valueType, fid: -1,
			privateValueAlias: true, privateValueFrom: rawIndex, numericKindFrom: -1,
			privateProgramScope: scope,
		})
		owner.valueIndex, owner.kindIndex, owner.state = valueIndex, kindIndex, 2
		result.numericKindRefs[valueIndex] = kindIndex
		return owner, nil
	}

	for _, root := range roots {
		index, err := compileExpression(root)
		if err != nil {
			return nil, err
		}
		result.roots = append(result.roots, index)
	}
	return result, nil
}

func decodeDependentNumericProgramRef(ctx context.Context, expr *plan.Expr) (int64, int, *plan.Expr, error) {
	f := expr.GetF()
	if f == nil || f.Func == nil {
		return 0, 0, nil, moerr.NewInvalidInput(ctx, "invalid scalar numeric program reference")
	}
	fid, refOverload := function.DecodeOverloadID(f.Func.Obj)
	args := f.Args
	if fid != function.SCALAR_PROGRAM_REF || refOverload != 0 || (len(args) != 2 && len(args) != 3) {
		return 0, 0, nil, moerr.NewInvalidInput(ctx, "invalid scalar numeric program reference arity")
	}
	var definition, scopeExpr, slotExpr *plan.Expr
	if len(args) == 3 {
		definition, scopeExpr, slotExpr = args[0], args[1], args[2]
	} else {
		scopeExpr, slotExpr = args[0], args[1]
	}
	readInt := func(expr *plan.Expr) (int64, bool) {
		if expr == nil || expr.GetLit() == nil || expr.GetLit().Isnull {
			return 0, false
		}
		value, ok := expr.GetLit().Value.(*plan.Literal_I64Val)
		if !ok {
			return 0, false
		}
		return value.I64Val, true
	}
	scope, scopeOK := readInt(scopeExpr)
	slot, slotOK := readInt(slotExpr)
	if !scopeOK || scope <= 0 || !slotOK || (slot != 0 && slot != 1) {
		return 0, 0, nil, moerr.NewInvalidInput(ctx, "invalid scalar numeric program scope or slot")
	}
	if slot == 0 {
		if definition == nil || expr.Typ.Id == 10 {
			return 0, 0, nil, moerr.NewInvalidInput(ctx, "invalid scalar numeric program value reference")
		}
		defFn := definition.GetF()
		if defFn == nil || defFn.Func == nil || len(defFn.Args) < 2 || len(defFn.Args) > 3 {
			return 0, 0, nil, moerr.NewInvalidInput(ctx, "invalid scalar numeric program definition")
		}
		defID, defOverload := function.DecodeOverloadID(defFn.Func.Obj)
		defScope, defOK := readInt(defFn.Args[0])
		if defID != function.SCALAR_PROGRAM || defOverload != 0 || !defOK || defScope != scope || defFn.Args[1] == nil ||
			!proto.Equal(&expr.Typ, &defFn.Args[1].Typ) || (len(defFn.Args) == 3 && (defFn.Args[2] == nil || defFn.Args[2].Typ.Id != 10)) {
			return 0, 0, nil, moerr.NewInvalidInput(ctx, "invalid scalar numeric program definition")
		}
		return scope, 0, definition, nil
	}
	if definition != nil || expr.Typ.Id != 10 {
		return 0, 0, nil, moerr.NewInvalidInput(ctx, "invalid scalar numeric program kind reference")
	}
	return scope, 1, nil, nil
}

// CompileDependentSingleInputExpressions adapts the native single-batch ABI.
// Callers must supply exactly one input batch when creating a frame.
// Relation coordinates are ignored, but column positions remain strict.
// Compile first so malformed/cyclic expressions retain normal validation.
// Copy only column instructions; never mutate the bound query or a reducer's
// tuple comparison. Already-normalized columns retain their original AST.
// Adaptation allocates only during compilation, not for each execution row.
func CompileDependentSingleInputExpressions(ctx context.Context, roots []*plan.Expr) (*DependentExpressionProgram, error) {
	return CompileDependentSingleInputExpressionsWithCorrelation(ctx, roots, nil)
}

func CompileDependentSingleInputExpressionsWithCorrelation(
	ctx context.Context, roots []*plan.Expr, programs []*plan.CorrelationProgram,
) (*DependentExpressionProgram, error) {
	program, err := compileDependentExpressions(ctx, roots, programs)
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
	if instruction.dependentKindCapture {
		return vm.ContinuationStep{}, moerr.NewInternalError(ctx, "dependent kind passenger has no invocation owner")
	}
	// A four-operand IFF has an ordinary BOOL companion for its selected
	// condition kind. Evaluate that companion before truth conversion, while
	// leaving both value arms lazy. The value and companion share their
	// lexical program owner, so this does not replay the selector.
	if instruction.fid == function.IFF && len(instruction.args) == 4 && !t.selected {
		for _, position := range []int{3, 0} {
			arg := instruction.args[position]
			if !f.cells[arg].ready {
				child, err := f.task(arg)
				return vm.ContinuationStep{Child: child}, err
			}
		}
		kind := f.cells[instruction.args[3]].value
		truth, err := function.IffConditionTruthyAt(
			f.cells[instruction.args[0]].value,
			0,
			function.CompatibilityModeFromProcess(f.proc),
			kind,
		)
		if err != nil {
			return vm.ContinuationStep{}, err
		}
		t.next = 2
		if truth {
			t.next = 1
		}
		t.selected = true
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
	if instruction.inferNumericKind {
		if instruction.numericKindFrom < 0 || instruction.numericKindFrom >= len(f.cells) ||
			!f.cells[instruction.numericKindFrom].ready {
			return vm.ContinuationStep{}, moerr.NewInternalError(ctx, "numeric kind source is not ready")
		}
		value := f.cells[instruction.numericKindFrom].value
		kind := value != nil && !value.IsNull(0) && f.numericValueKindAt(instruction.numericKindFrom, 0)
		if err := f.reserve(uint64(unsafe.Sizeof(vector.Vector{}))); err != nil {
			return vm.ContinuationStep{}, err
		}
		kindVector, err := newExpressionConstFixed(types.T_bool.ToType(), kind, 1, f.proc.Mp(), f.allocation)
		if err != nil {
			return vm.ContinuationStep{}, err
		}
		cell.value = kindVector
		cell.owned = true
		cell.ready = true
		return vm.ContinuationStep{Done: true}, nil
	}
	if instruction.privateValueAlias {
		if instruction.privateValueFrom < 0 || instruction.privateValueFrom >= len(f.cells) ||
			!f.cells[instruction.privateValueFrom].ready {
			return vm.ContinuationStep{}, moerr.NewInternalError(ctx, "numeric program value source is not ready")
		}
		cell.value = f.cells[instruction.privateValueFrom].value
		cell.ready = true
		return vm.ContinuationStep{Done: true}, nil
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
		if functionExecutor, ok := executor.(*FunctionExpressionExecutor); ok && instruction.numericKindDemand {
			functionExecutor.numericValueKindDemand = true
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
	kindColumn := uint32(0)
	if layout, ok := child.(interface{ ScalarResultKindColumn() uint32 }); ok {
		kindColumn = layout.ScalarResultKindColumn()
	}
	if kindColumn > 1 {
		return moerr.NewInternalErrorNoCtx("invalid dependent scalar kind column marker")
	}
	wantWidth := 1
	if kindColumn == 1 {
		wantWidth = 2
	}
	if result == nil || result.RowCount() != 1 || len(result.Vecs) != wantWidth {
		return moerr.NewInternalErrorNoCtx("invalid dependent invocation result")
	}
	typ := f.program.instructions[t.index].typ
	if result.Vecs[0] == nil || !typ.Eq(*result.Vecs[0].GetType()) {
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
	if kindColumn == 1 {
		kindIndex, ok := f.program.numericKindRefs[t.index]
		if !ok || kindIndex < 0 || kindIndex >= len(f.cells) || result.Vecs[1] == nil ||
			!types.T_bool.ToType().Eq(*result.Vecs[1].GetType()) {
			return moerr.NewInternalErrorNoCtx("invalid dependent scalar kind layout")
		}
		kindValue := false
		if !result.Vecs[0].IsNull(0) && !result.Vecs[1].IsNull(0) {
			kindValue = vector.GetFixedAtNoTypeCheck[bool](result.Vecs[1], 0)
		}
		if err = f.reserve(uint64(unsafe.Sizeof(vector.Vector{}))); err != nil {
			return err
		}
		kindCell := &f.cells[kindIndex]
		if kindCell.ready || kindCell.value != nil {
			return moerr.NewInternalErrorNoCtx("dependent scalar kind was already captured")
		}
		kindCell.value, err = newExpressionConstFixed(types.T_bool.ToType(), kindValue, 1, f.proc.Mp(), f.allocation)
		if err != nil {
			return err
		}
		kindCell.owned = true
		kindCell.ready = true
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
func (r *dependentValueReference) numericValueKindAt(row int) bool {
	if r == nil || r.frame == nil {
		return false
	}
	return r.frame.numericValueKindAt(r.index, row)
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

func (f *DependentExpressionFrame) numericValueKindAt(index, row int) bool {
	if f == nil || f.program == nil || index < 0 || index >= len(f.cells) || row < 0 {
		return false
	}
	value := f.cells[index].value
	if value == nil || value.IsNull(uint64(row)) {
		return false
	}
	if kindIndex, ok := f.program.numericKindRefs[index]; ok {
		if kindIndex < 0 || kindIndex >= len(f.cells) || !f.cells[kindIndex].ready {
			return false
		}
		kind := f.cells[kindIndex].value
		if kind == nil || kind.IsNull(0) {
			return false
		}
		kindRow := row
		if kind.IsConst() {
			kindRow = 0
		}
		if kindRow >= kind.Length() {
			return false
		}
		return vector.GetFixedAtNoTypeCheck[bool](kind, kindRow)
	}
	return numericValueKindAt(f.cells[index].executor, row)
}

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
	programOwners := make([]dependentNumericProgramOwner, 0)
	for valueIndex, instruction := range p.instructions {
		if !instruction.privateValueAlias {
			continue
		}
		if len(instruction.args) != 2 || instruction.privateValueFrom < 0 || instruction.privateValueFrom >= valueIndex {
			return nil, moerr.NewInvalidInputNoCtx("invalid scalar numeric program value owner")
		}
		kindIndex := instruction.args[1]
		if kindIndex < 0 || kindIndex >= valueIndex || kindIndex == instruction.privateValueFrom ||
			kindIndex >= len(p.instructions) || p.instructions[kindIndex].typ.Oid != types.T_bool {
			return nil, moerr.NewInvalidInputNoCtx("invalid scalar numeric program kind owner")
		}
		definition := p.numericProgramDefinitions[instruction.privateProgramScope]
		if definition == nil || instruction.privateProgramScope <= 0 {
			return nil, moerr.NewInvalidInputNoCtx("scalar numeric program owner has no definition")
		}
		defArgs := definition.GetF().GetArgs()
		if len(defArgs) != 2 && len(defArgs) != 3 {
			return nil, moerr.NewInvalidInputNoCtx("invalid scalar numeric program definition")
		}
		explicit := len(defArgs) == 3
		kindInstruction := p.instructions[kindIndex]
		if explicit == kindInstruction.inferNumericKind ||
			(!explicit && (kindInstruction.numericKindFrom != instruction.privateValueFrom || kindInstruction.privateProgramScope != instruction.privateProgramScope)) {
			return nil, moerr.NewInvalidInputNoCtx("scalar numeric program kind definition does not match its owner")
		}
		programOwners = append(programOwners, dependentNumericProgramOwner{
			scope: instruction.privateProgramScope, raw: instruction.privateValueFrom,
			kind: kindIndex, value: valueIndex, explicit: explicit,
		})
	}
	size := uint64(unsafe.Sizeof(dependentScalarExecutor{}))
	for _, instruction := range p.instructions {
		if instruction.expr.GetDependent() != nil || instruction.expr.GetSlot() != nil {
			return nil, moerr.NewInternalErrorNoCtx("comparison contains relational demand")
		}
		size += uint64(unsafe.Sizeof(FunctionExpressionExecutor{})) + uint64(unsafe.Sizeof(dependentScalarReference{})) + uint64(unsafe.Sizeof(ExpressionExecutor(nil)))
		size += uint64(len(instruction.args)) * 2 * uint64(unsafe.Sizeof(ExpressionExecutor(nil)))
	}
	size += uint64(len(programOwners)) * uint64(unsafe.Sizeof(scalarNumericProgramState{}))
	if err = budget.AcquireAllocationCapacity(size); err != nil {
		return nil, err
	}
	owner := &dependentScalarExecutor{
		budget: budget, charged: size, executors: make([]ExpressionExecutor, len(p.instructions)),
		programs: make([]*scalarNumericProgramState, 0, len(programOwners)),
	}
	refs := make([]dependentScalarReference, len(p.instructions))
	programOwnerAt := make(map[int]dependentNumericProgramOwner, len(programOwners))
	for _, programOwner := range programOwners {
		programOwnerAt[programOwner.value] = programOwner
	}
	for i, instruction := range p.instructions {
		if instruction.inferNumericKind {
			// The matching value alias below creates and publishes the shared
			// program state, including this BOOL output.
			continue
		}
		if instruction.privateValueAlias {
			programOwner, ok := programOwnerAt[i]
			if !ok || owner.executors[programOwner.raw] == nil ||
				(programOwner.explicit && owner.executors[programOwner.kind] == nil) ||
				(!programOwner.explicit && !p.instructions[programOwner.kind].inferNumericKind) {
				owner.Free()
				return nil, moerr.NewInvalidInputNoCtx("scalar numeric program inputs were not compiled")
			}
			state := &scalarNumericProgramState{
				definition: p.numericProgramDefinitions[programOwner.scope],
				value:      owner.executors[programOwner.raw],
				selection:  allocation,
				mp:         proc.Mp(),
			}
			if programOwner.explicit {
				state.kindSource = owner.executors[programOwner.kind]
			}
			owner.programs = append(owner.programs, state)
			valueRef := &scalarNumericProgramRefExecutor{state: state, slot: 0}
			kindRef := &scalarNumericProgramRefExecutor{state: state, slot: 1}
			state.refs = 3
			owner.executors[programOwner.raw] = valueRef
			owner.executors[programOwner.kind] = kindRef
			refs[programOwner.raw].ExpressionExecutor = valueRef
			refs[programOwner.kind].ExpressionExecutor = kindRef
			owner.executors[programOwner.value] = &scalarNumericProgramRefExecutor{state: state, slot: 0}
			refs[programOwner.value].ExpressionExecutor = owner.executors[programOwner.value]
			continue
		}
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
		if functionExecutor, ok := owner.executors[i].(*FunctionExpressionExecutor); ok && instruction.numericKindDemand {
			functionExecutor.numericValueKindDemand = true
		}
		refs[i].ExpressionExecutor = owner.executors[i]
	}
	owner.ExpressionExecutor = owner.executors[p.roots[0]]
	return owner, nil
}

type dependentScalarReference struct{ ExpressionExecutor }

func (r *dependentScalarReference) numericValueKindAt(row int) bool {
	return r != nil && numericValueKindAt(r.ExpressionExecutor, row)
}

func (*dependentScalarReference) Free()              {}
func (*dependentScalarReference) ResetForNextQuery() {}

type dependentScalarExecutor struct {
	ExpressionExecutor
	executors []ExpressionExecutor
	programs  []*scalarNumericProgramState
	budget    *process.ExecutionResourceGeneration
	charged   uint64
}

func (e *dependentScalarExecutor) Eval(proc *process.Process, batches []*batch.Batch, selectList []bool) (*vector.Vector, error) {
	e.beginBatch()
	return e.ExpressionExecutor.Eval(proc, batches, selectList)
}

func (e *dependentScalarExecutor) EvalWithoutResultReusing(proc *process.Process, batches []*batch.Batch, selectList []bool) (*vector.Vector, error) {
	e.beginBatch()
	return e.ExpressionExecutor.EvalWithoutResultReusing(proc, batches, selectList)
}

func (e *dependentScalarExecutor) beginBatch() {
	for _, state := range e.programs {
		state.resetEvaluation()
	}
}

func (e *dependentScalarExecutor) Free() {
	for i := len(e.executors) - 1; i >= 0; i-- {
		if e.executors[i] != nil {
			e.executors[i].Free()
		}
	}
	e.executors = nil
	e.programs = nil
	e.ExpressionExecutor = nil
	e.budget.ReleaseAllocationCapacity(e.charged)
	e.charged = 0
}
func (e *dependentScalarExecutor) ResetForNextQuery() {
	for _, executor := range e.executors {
		executor.ResetForNextQuery()
	}
	for _, state := range e.programs {
		state.resetForNextQuery()
	}
}
