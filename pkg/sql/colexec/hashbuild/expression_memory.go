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

package hashbuild

import (
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type expressionMemoryLeaseSlot struct {
	tokens       []*process.HashBuildReservation
	admittedPeak uint64
	variableSize bool
}

// ExpressionMemoryLease couples one stable expression-executor set to the
// HashBuild reservation that covers its retained vector capacity. It is not
// safe for concurrent use.
//
// Run retains the largest admitted expression bound independently for every
// root executor. Re-evaluating a root for an equal or smaller batch therefore
// reuses its existing high-water reservation. Growth admits only the part of
// that root's allocate-copy-free peak not covered by the old reservation, then
// reconciles the transient charge into the new high water. Unrelated roots are
// never double-charged.
//
// The owner must Free every executor and duplicate vector covered by the lease
// before calling Release. A lease cannot cross statement budget generations.
type ExpressionMemoryLease struct {
	budget    *process.HashBuildBudgetGeneration
	exprs     []*plan.Expr
	executors []colexec.ExpressionExecutor
	duplicate bool
	slots     []expressionMemoryLeaseSlot
	released  bool
}

// NewAllocationAccountedExpressionExecutors constructs only expression trees
// whose complete retained and call-scoped allocation ledger is closed. The
// exact MPool leases are the sole capacity charge; unsupported function
// families continue through NewBudgetedExpressionExecutors until their own
// scratch owner is migrated.
func NewAllocationAccountedExpressionExecutors(
	proc *process.Process,
	exprs []*plan.Expr,
	allocation *colexec.ExpressionAllocationAccount,
) ([]colexec.ExpressionExecutor, error) {
	if allocation == nil || !expressionSetAllocationClosed(exprs) {
		return nil, process.ErrHashBuildBudgetInvalid
	}
	return colexec.NewExpressionExecutorsFromPlanExpressionsWithAllocation(
		proc,
		exprs,
		allocation,
	)
}

func expressionSetAllocationClosed(exprs []*plan.Expr) bool {
	for _, expr := range exprs {
		if !expressionAllocationClosed(expr) {
			return false
		}
	}
	return true
}

// AllocationAccountedExpressionSetSupported reports whether every execution
// path in the expression set has a closed physical-allocation ledger. Spill
// rebuild uses this gate before selecting the shared exact account.
func AllocationAccountedExpressionSetSupported(exprs []*plan.Expr) bool {
	return expressionSetAllocationClosed(exprs)
}

func expressionAllocationClosed(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	switch node := expr.Expr.(type) {
	case *plan.Expr_Col, *plan.Expr_Lit, *plan.Expr_T,
		*plan.Expr_P, *plan.Expr_V, *plan.Expr_Vec, *plan.Expr_Fold:
		return true
	case *plan.Expr_F:
		if node.F == nil || node.F.Func == nil {
			return false
		}
		// Keep this as an implementation audit list, not a semantic function
		// list. CONCAT writes directly into admitted result storage, CASE owns
		// its row selections through ExpressionAllocationAccount, and varchar
		// equality has no row-scaled scratch. Integer string casts use a
		// stack-backed formatter; inserted casts of literals are plan-bounded.
		functionID, _ := function.DecodeOverloadID(node.F.Func.Obj)
		switch functionID {
		case function.CONCAT, function.CASE:
		case function.EQUAL:
			if !closedHashBuildEqual(node.F.Args) {
				return false
			}
		case function.CAST:
			if !closedHashBuildCast(expr, node.F.Args) {
				return false
			}
		default:
			return false
		}
		for _, arg := range node.F.Args {
			if !expressionAllocationClosed(arg) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func closedHashBuildEqual(args []*plan.Expr) bool {
	if len(args) != 2 || args[0] == nil || args[1] == nil {
		return false
	}
	for _, arg := range args {
		oid := types.T(arg.Typ.Id)
		if oid != types.T_char && oid != types.T_varchar {
			return false
		}
	}
	return true
}

func closedHashBuildCast(result *plan.Expr, args []*plan.Expr) bool {
	if result == nil || len(args) == 0 || args[0] == nil {
		return false
	}
	source := types.T(args[0].Typ.Id)
	target := types.T(result.Typ.Id)
	if !source.ToType().IsIntOrUint() {
		if (source == types.T_char || source == types.T_varchar) &&
			(target == types.T_char || target == types.T_varchar) {
			_, literal := args[0].Expr.(*plan.Expr_Lit)
			return literal
		}
		return false
	}
	return target == types.T_char || target == types.T_varchar
}

// NewBudgetedExpressionExecutors admits the mpool-backed capacity owned by
// constant children before constructing them. The returned lease adopts those
// reservations, so construction and later evaluation have one continuous
// budget lifetime.
func NewBudgetedExpressionExecutors(
	proc *process.Process,
	budget *process.HashBuildBudgetGeneration,
	exprs []*plan.Expr,
	duplicate bool,
) ([]colexec.ExpressionExecutor, *ExpressionMemoryLease, error) {
	if budget == nil {
		executors, err := colexec.NewExpressionExecutorsFromPlanExpressions(proc, exprs)
		if err != nil {
			return nil, nil, err
		}
		lease, err := NewExpressionMemoryLease(nil, exprs, executors, duplicate)
		if err != nil {
			for _, executor := range executors {
				executor.Free()
			}
			return nil, nil, err
		}
		return executors, lease, err
	}

	executors := make([]colexec.ExpressionExecutor, len(exprs))
	lease := &ExpressionMemoryLease{
		budget:    budget,
		exprs:     exprs,
		executors: executors,
		duplicate: duplicate,
		slots:     make([]expressionMemoryLeaseSlot, len(exprs)),
	}
	cleanup := func() {
		for _, executor := range executors {
			if executor != nil {
				executor.Free()
			}
		}
		lease.Release()
	}

	for i, expr := range exprs {
		initial, err := expressionInitialOwnedBytes(expr)
		if err != nil {
			cleanup()
			return nil, nil, err
		}
		if initial > 0 {
			token, err := budget.Reserve(initial)
			if err != nil {
				cleanup()
				return nil, nil, err
			}
			lease.slots[i].tokens = append(lease.slots[i].tokens, token)
			lease.slots[i].admittedPeak = initial
		}

		executor, err := colexec.NewExpressionExecutor(proc, expr)
		if err != nil {
			cleanup()
			return nil, nil, err
		}
		executors[i] = executor

		retained, ok := colexec.ExpressionExecutorRetainedBytes(executor)
		if !ok || retained > initial {
			cleanup()
			return nil, nil, process.ErrHashBuildBudgetInvalid
		}
		slot := &lease.slots[i]
		slot.variableSize = expressionExecutorMayGrowWithinBound(expr)
		if len(slot.tokens) > 0 {
			token := slot.tokens[0]
			if retained == 0 {
				token.Release()
				slot.tokens = nil
			} else if _, err = token.ReconcileDown(retained); err != nil {
				cleanup()
				return nil, nil, err
			}
		}
		slot.admittedPeak = retained
	}
	return executors, lease, nil
}

func NewExpressionMemoryLease(
	budget *process.HashBuildBudgetGeneration,
	exprs []*plan.Expr,
	executors []colexec.ExpressionExecutor,
	duplicate bool,
) (*ExpressionMemoryLease, error) {
	if len(exprs) != len(executors) {
		return nil, process.ErrHashBuildBudgetInvalid
	}
	lease := &ExpressionMemoryLease{
		budget:    budget,
		exprs:     exprs,
		executors: executors,
		duplicate: duplicate,
		slots:     make([]expressionMemoryLeaseSlot, len(executors)),
	}
	if budget == nil {
		return lease, nil
	}

	for i, executor := range executors {
		retained, ok := colexec.ExpressionExecutorRetainedBytes(executor)
		if !ok {
			lease.Release()
			return nil, process.ErrHashBuildBudgetInvalid
		}
		lease.slots[i].admittedPeak = retained
		lease.slots[i].variableSize = expressionExecutorMayGrowWithinBound(exprs[i])
		if retained == 0 {
			continue
		}
		token, err := budget.Reserve(retained)
		if err != nil {
			lease.Release()
			return nil, err
		}
		lease.slots[i].tokens = append(lease.slots[i].tokens, token)
	}
	return lease, nil
}

func expressionInitialOwnedBytes(expr *plan.Expr) (uint64, error) {
	if expr == nil {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	switch typed := expr.Expr.(type) {
	case *plan.Expr_Lit:
		if typed.Lit == nil || typed.Lit.GetIsnull() {
			return 0, nil
		}
		return literalInitialOwnedBytes(types.T(expr.Typ.Id), typed.Lit)
	case *plan.Expr_List:
		return expressionListInitialOwnedBytes(typed.List.GetList())
	case *plan.Expr_F:
		return expressionListInitialOwnedBytes(typed.F.GetArgs())
	default:
		return 0, nil
	}
}

func expressionListInitialOwnedBytes(exprs []*plan.Expr) (uint64, error) {
	var total uint64
	for _, expr := range exprs {
		size, err := expressionInitialOwnedBytes(expr)
		if err != nil || total > math.MaxUint64-size {
			return 0, process.ErrHashBuildBudgetInvalid
		}
		total += size
	}
	return total, nil
}

func expressionExecutorMayGrowWithinBound(expr *plan.Expr) bool {
	if expr == nil {
		return true
	}
	switch typed := expr.Expr.(type) {
	case *plan.Expr_Col, *plan.Expr_Lit, *plan.Expr_T, *plan.Expr_Vec:
		return false
	case *plan.Expr_F:
		if types.T(expr.Typ.Id).FixedLength() < 0 {
			return true
		}
		for _, arg := range typed.F.GetArgs() {
			if expressionExecutorMayGrowWithinBound(arg) {
				return true
			}
		}
		return false
	case *plan.Expr_List:
		if types.T(expr.Typ.Id).FixedLength() < 0 {
			return true
		}
		for _, item := range typed.List.GetList() {
			if expressionExecutorMayGrowWithinBound(item) {
				return true
			}
		}
		return false
	case *plan.Expr_P, *plan.Expr_V:
		return types.T(expr.Typ.Id).FixedLength() < 0
	default:
		return true
	}
}

func literalInitialOwnedBytes(oid types.T, literal *plan.Literal) (uint64, error) {
	var dataBytes uint64
	var payloadBytes uint64
	switch value := literal.GetValue().(type) {
	case *plan.Literal_Bval, *plan.Literal_I8Val, *plan.Literal_U8Val, *plan.Literal_Defaultval:
		dataBytes = 1
	case *plan.Literal_I16Val, *plan.Literal_U16Val, *plan.Literal_EnumVal:
		dataBytes = 2
	case *plan.Literal_I32Val, *plan.Literal_U32Val, *plan.Literal_Fval, *plan.Literal_Dateval:
		dataBytes = 4
	case *plan.Literal_I64Val, *plan.Literal_U64Val, *plan.Literal_Dval,
		*plan.Literal_Timeval, *plan.Literal_Datetimeval,
		*plan.Literal_Decimal64Val, *plan.Literal_Timestampval:
		dataBytes = 8
	case *plan.Literal_Decimal128Val:
		dataBytes = 16
	case *plan.Literal_Sval:
		dataBytes = types.VarlenaSize
		switch oid {
		case types.T_array_float32:
			if uint64(len(value.Sval)) > math.MaxUint64/4 {
				return 0, process.ErrHashBuildBudgetInvalid
			}
			// The textual representation has at least one byte per element.
			// Reserve a parsing-allocation-free upper bound; construction later
			// reconciles it to the actual binary payload.
			payloadBytes = uint64(len(value.Sval)) * 4
		case types.T_array_float64:
			if uint64(len(value.Sval)) > math.MaxUint64/8 {
				return 0, process.ErrHashBuildBudgetInvalid
			}
			payloadBytes = uint64(len(value.Sval)) * 8
		default:
			payloadBytes = uint64(len(value.Sval))
		}
	case *plan.Literal_VecVal:
		dataBytes = types.VarlenaSize
		payloadBytes = uint64(len(value.VecVal))
	default:
		// Unsupported literal kinds are rejected by the expression factory
		// before they can own an mpool-backed vector.
		return 0, nil
	}

	dataCapacity, err := initialAllocationCapacity(dataBytes)
	if err != nil {
		return 0, err
	}
	if payloadBytes <= types.VarlenaInlineSize {
		return dataCapacity, nil
	}
	areaCapacity, err := initialAllocationCapacity(payloadBytes)
	if err != nil || dataCapacity > math.MaxUint64-areaCapacity {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	return dataCapacity + areaCapacity, nil
}

func initialAllocationCapacity(required uint64) (uint64, error) {
	if required == 0 {
		return 0, nil
	}
	if required > math.MaxInt64 {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	capacity, ok := mpool.GrowCapacity(0, int64(required))
	if !ok || capacity < 0 {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	return uint64(capacity), nil
}

// Run admits and evaluates each root in index order. Growth keeps the root's
// old retained charge live, reserves only the uncovered allocate-copy-free
// overlap, and reconciles that overlap into the new high-water charge.
func (l *ExpressionMemoryLease) Run(
	proc *process.Process,
	rows int,
	fn func(index int) error,
) (err error) {
	if fn == nil {
		return process.ErrHashBuildBudgetInvalid
	}
	if l == nil {
		return process.ErrHashBuildBudgetInvalid
	}
	if l.released {
		return process.ErrHashBuildReservationInactive
	}
	if rows < 0 {
		return process.ErrHashBuildBudgetInvalid
	}

	for i, expr := range l.exprs {
		if l.budget == nil {
			if err := fn(i); err != nil {
				return err
			}
			continue
		}

		peak, peakErr := expressionVectorPeak(proc, expr, rows, l.duplicate)
		if peakErr != nil {
			return peakErr
		}
		slot := &l.slots[i]
		if peak <= slot.admittedPeak && !slot.variableSize {
			if err := fn(i); err != nil {
				return err
			}
			continue
		}

		retained, ok := colexec.ExpressionExecutorRetainedBytes(l.executors[i])
		if !ok || retained > slot.admittedPeak {
			return process.ErrHashBuildBudgetInvalid
		}
		var transient uint64
		if retained > math.MaxUint64-peak {
			return process.ErrHashBuildBudgetInvalid
		}
		physicalPeak := retained + peak
		if physicalPeak > slot.admittedPeak {
			transient = physicalPeak - slot.admittedPeak
		}
		if transient == 0 {
			if err := fn(i); err != nil {
				return err
			}
			continue
		}

		candidate, reserveErr := l.budget.Reserve(transient)
		if reserveErr != nil {
			return reserveErr
		}
		evalErr := fn(i)
		if l.released {
			candidate.Release()
			return evalErr
		}
		if peak > slot.admittedPeak {
			growth := peak - slot.admittedPeak
			if _, reconcileErr := candidate.ReconcileDown(growth); reconcileErr != nil {
				slot.tokens = append(slot.tokens, candidate)
				slot.admittedPeak += transient
				if evalErr != nil {
					return evalErr
				}
				return reconcileErr
			}
			slot.tokens = append(slot.tokens, candidate)
			slot.admittedPeak = peak
		} else {
			candidate.Release()
		}
		if evalErr != nil {
			return evalErr
		}
	}
	return nil
}

// Eval evaluates the executors owned by the lease in the same per-root order
// used for admission. consume receives each successfully evaluated vector
// before the next root is admitted.
func (l *ExpressionMemoryLease) Eval(
	proc *process.Process,
	bats []*batch.Batch,
	rows int,
	consume func(index int, vec *vector.Vector) error,
) error {
	if l == nil || consume == nil {
		return process.ErrHashBuildBudgetInvalid
	}
	return l.Run(proc, rows, func(index int) error {
		vec, err := l.executors[index].Eval(proc, bats, nil)
		if err != nil {
			return err
		}
		return consume(index, vec)
	})
}

func (l *ExpressionMemoryLease) Reserved() uint64 {
	if l == nil || l.released {
		return 0
	}
	var total uint64
	for i := range l.slots {
		for _, token := range l.slots[i].tokens {
			size := token.Size()
			if total > math.MaxUint64-size {
				return math.MaxUint64
			}
			total += size
		}
	}
	return total
}

func (l *ExpressionMemoryLease) Len() int {
	if l == nil || l.released {
		return 0
	}
	return len(l.executors)
}

// Retained returns the current mpool-backed capacity physically owned by the
// executor set. Reserved may be larger: the documented delta is the retained
// high-water admission bound kept available for safe executor reuse.
func (l *ExpressionMemoryLease) Retained() (uint64, bool) {
	if l == nil {
		return 0, true
	}
	if l.released {
		return 0, false
	}
	return colexec.ExpressionExecutorsRetainedBytes(l.executors)
}

func (l *ExpressionMemoryLease) Release() {
	if l == nil || l.released {
		return
	}
	l.released = true
	for i := range l.slots {
		for _, token := range l.slots[i].tokens {
			token.Release()
		}
		l.slots[i].tokens = nil
		l.slots[i].admittedPeak = 0
	}
	l.slots = nil
	l.exprs = nil
	l.executors = nil
	l.budget = nil
}
