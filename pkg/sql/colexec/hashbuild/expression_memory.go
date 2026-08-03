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
	"errors"
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
	tokens                   []*process.HashBuildReservation
	admittedPeak             uint64
	mayReplaceWithinBound    bool
	recoveryPeak             uint64
	recoveryMayReplace       bool
	recoveryCandidate        uint64
	recoveryCandidateReplace bool
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
	budget              *process.HashBuildBudgetGeneration
	exprs               []*plan.Expr
	executors           []colexec.ExpressionExecutor
	duplicate           bool
	slots               []expressionMemoryLeaseSlot
	recoveryReservation *process.HashBuildReservation
	recoveryRows        int
	recoveryReady       bool
	recoveryReconcile   bool
	released            bool
}

type expressionRecoveryCheckpoint struct {
	reservation              *process.HashBuildReservation
	reservationSize          uint64
	recoveryRows             int
	recoveryReady            bool
	recoveryReconcile        bool
	recoveryPeak             []uint64
	recoveryMayReplace       []bool
	recoveryCandidate        []uint64
	recoveryCandidateReplace []bool
}

func (l *ExpressionMemoryLease) checkpointRecovery(rows int) expressionRecoveryCheckpoint {
	if l == nil || l.budget == nil || (l.recoveryReady && rows <= l.recoveryRows) {
		return expressionRecoveryCheckpoint{}
	}
	checkpoint := expressionRecoveryCheckpoint{
		reservation:              l.recoveryReservation,
		recoveryRows:             l.recoveryRows,
		recoveryReady:            l.recoveryReady,
		recoveryReconcile:        l.recoveryReconcile,
		recoveryPeak:             make([]uint64, len(l.slots)),
		recoveryMayReplace:       make([]bool, len(l.slots)),
		recoveryCandidate:        make([]uint64, len(l.slots)),
		recoveryCandidateReplace: make([]bool, len(l.slots)),
	}
	if checkpoint.reservation != nil {
		checkpoint.reservationSize = checkpoint.reservation.Size()
	}
	for i := range l.slots {
		checkpoint.recoveryPeak[i] = l.slots[i].recoveryPeak
		checkpoint.recoveryMayReplace[i] = l.slots[i].recoveryMayReplace
		checkpoint.recoveryCandidate[i] = l.slots[i].recoveryCandidate
		checkpoint.recoveryCandidateReplace[i] = l.slots[i].recoveryCandidateReplace
	}
	return checkpoint
}

func (l *ExpressionMemoryLease) rollbackRecovery(checkpoint expressionRecoveryCheckpoint) error {
	if l == nil || checkpoint.recoveryPeak == nil {
		return nil
	}
	if len(l.slots) != len(checkpoint.recoveryPeak) ||
		len(l.slots) != len(checkpoint.recoveryMayReplace) ||
		len(l.slots) != len(checkpoint.recoveryCandidate) ||
		len(l.slots) != len(checkpoint.recoveryCandidateReplace) {
		return process.ErrHashBuildBudgetInvalid
	}
	if checkpoint.reservation == nil {
		if l.recoveryReservation != nil {
			if !l.recoveryReservation.Release() {
				return process.ErrHashBuildReservationInactive
			}
			l.recoveryReservation = nil
		}
	} else {
		if l.recoveryReservation != checkpoint.reservation {
			return process.ErrHashBuildBudgetInvalid
		}
		if _, err := l.recoveryReservation.ReconcileDown(checkpoint.reservationSize); err != nil {
			return err
		}
	}
	for i := range l.slots {
		l.slots[i].recoveryPeak = checkpoint.recoveryPeak[i]
		l.slots[i].recoveryMayReplace = checkpoint.recoveryMayReplace[i]
		l.slots[i].recoveryCandidate = checkpoint.recoveryCandidate[i]
		l.slots[i].recoveryCandidateReplace = checkpoint.recoveryCandidateReplace[i]
	}
	l.recoveryRows = checkpoint.recoveryRows
	l.recoveryReady = checkpoint.recoveryReady
	l.recoveryReconcile = checkpoint.recoveryReconcile
	return nil
}

// EnsureRunRecoveryWith admits expression recovery and one dependent recovery
// owner as a transaction. The dependent admission must either succeed without
// a partial state change or return an error. On failure, the expression lease
// restores its prior reservation and prepared row high water before the caller
// can fall back to a smaller recovery window.
func (l *ExpressionMemoryLease) EnsureRunRecoveryWith(
	proc *process.Process,
	rows int,
	ensureDependent func() error,
) error {
	if ensureDependent == nil {
		return process.ErrHashBuildBudgetInvalid
	}
	checkpoint := l.checkpointRecovery(rows)
	if err := l.EnsureRunRecovery(proc, rows); err != nil {
		if rollbackErr := l.rollbackRecovery(checkpoint); rollbackErr != nil {
			return errors.Join(err, rollbackErr)
		}
		return err
	}
	if err := ensureDependent(); err != nil {
		if rollbackErr := l.rollbackRecovery(checkpoint); rollbackErr != nil {
			return errors.Join(err, rollbackErr)
		}
		return err
	}
	return nil
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
		slot.mayReplaceWithinBound = expressionExecutorMayGrowWithinBound(expr)
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
		lease.slots[i].mayReplaceWithinBound = expressionExecutorMayGrowWithinBound(exprs[i])
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
		children, err := expressionListInitialOwnedBytes(typed.F.GetArgs())
		if err != nil {
			return 0, err
		}
		own := expressionFunctionInitialOwnedBytes(typed.F)
		if children > math.MaxUint64-own {
			return 0, process.ErrHashBuildBudgetInvalid
		}
		return children + own, nil
	default:
		return 0, nil
	}
}

func expressionFunctionInitialOwnedBytes(fn *plan.Function) uint64 {
	if fn == nil || fn.Func == nil {
		return 0
	}
	fid, _ := function.DecodeOverloadID(fn.Func.Obj)
	if fid == function.SERIAL || fid == function.SERIAL_FULL {
		return types.DefaultPackerCapacity()
	}
	return 0
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
	return expressionExecutorMayGrowWithinSelection(expr, false)
}

// expressionExecutorMayGrowWithinSelection reports whether an executor tree
// can replace retained mpool capacity without increasing its input row bound.
// Varlena values can change size at the same row count. A function evaluated
// through a partial flow-control mask can likewise grow its cached compacted
// parameters and selected result when only the mask distribution changes.
func expressionExecutorMayGrowWithinSelection(
	expr *plan.Expr,
	mayReceivePartialSelection bool,
) bool {
	if expr == nil {
		return true
	}
	switch typed := expr.Expr.(type) {
	case *plan.Expr_Col, *plan.Expr_Lit, *plan.Expr_T, *plan.Expr_Vec:
		return false
	case *plan.Expr_F:
		if typed.F == nil || types.T(expr.Typ.Id).FixedLength() < 0 ||
			mayReceivePartialSelection {
			return true
		}
		fid := int32(-1)
		if typed.F.Func != nil {
			fid, _ = function.DecodeOverloadID(typed.F.Func.Obj)
		}
		for i, arg := range typed.F.GetArgs() {
			if expressionExecutorMayGrowWithinSelection(
				arg,
				expressionChildMayReceivePartialSelection(
					fid, i, mayReceivePartialSelection),
			) {
				return true
			}
		}
		return false
	case *plan.Expr_List:
		if types.T(expr.Typ.Id).FixedLength() < 0 {
			return true
		}
		for _, item := range typed.List.GetList() {
			if expressionExecutorMayGrowWithinSelection(
				item, mayReceivePartialSelection) {
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

func expressionChildMayReceivePartialSelection(
	fid int32,
	argument int,
	parentMayReceivePartialSelection bool,
) bool {
	switch fid {
	case function.IFF, function.CASE, function.COALESCE:
		// The first argument inherits the caller's mask. Every later argument
		// can receive a mask narrowed by an earlier condition/value.
		return parentMayReceivePartialSelection || argument > 0
	default:
		return parentMayReceivePartialSelection
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

// EnsureRunRecovery admits the complete expression-executor peak needed to
// evaluate batches up to rows without another budget reservation. HashBuild
// calls it before retaining a spillable batch, so spill can always evaluate a
// key and release the first retained batch even when sibling workers consume
// all remaining query headroom.
//
// Root executors run sequentially but retain their result capacities. The
// first-run bound therefore replaces each root's old admitted capacity with
// its target peak in evaluation order. Executors whose capacity can still
// vary inside the same row bound (varlena values or flow-control selected
// scratch) may always overlap one old result with one replacement. A stable
// fixed-width root needs the same protection only until it has successfully
// reached this row high water. One aggregate standby token covers the largest
// such overlap instead of pessimistically reserving two copies for every root.
func (l *ExpressionMemoryLease) EnsureRunRecovery(
	proc *process.Process,
	rows int,
) error {
	if l == nil {
		return process.ErrHashBuildBudgetInvalid
	}
	if l.released {
		return process.ErrHashBuildReservationInactive
	}
	if rows < 0 || len(l.exprs) != len(l.executors) || len(l.slots) != len(l.executors) {
		return process.ErrHashBuildBudgetInvalid
	}
	if l.budget == nil || (l.recoveryReady && rows <= l.recoveryRows) {
		return nil
	}

	var running uint64
	for i := range l.slots {
		retained, ok := colexec.ExpressionExecutorRetainedBytes(l.executors[i])
		if !ok || retained > l.slots[i].admittedPeak {
			return process.ErrHashBuildBudgetInvalid
		}
		if running > math.MaxUint64-l.slots[i].admittedPeak {
			return process.ErrHashBuildBudgetInvalid
		}
		running += l.slots[i].admittedPeak
	}

	target := running
	var replacementOverlap uint64
	for i, expr := range l.exprs {
		peak, err := expressionVectorPeak(proc, expr, rows, l.duplicate)
		if err != nil {
			return err
		}
		slot := &l.slots[i]
		slot.recoveryCandidate = peak
		slot.recoveryCandidateReplace = slot.mayReplaceWithinBound

		allocation := peak
		nextRetained := peak
		if slot.admittedPeak > nextRetained {
			// Executors retain reusable high-water capacity; evaluating a
			// smaller variable-width value does not prove that old capacity was
			// returned to mpool.
			nextRetained = slot.admittedPeak
		}
		if !slot.mayReplaceWithinBound && peak <= slot.admittedPeak {
			// A fixed-width executor whose high water already covers this row
			// bound reuses its vectors and retains the existing capacity.
			allocation = 0
			nextRetained = slot.admittedPeak
		} else if !slot.mayReplaceWithinBound {
			// Until this exact high water has executed successfully, a smaller
			// fixed-width result can still overlap its later replacement.
			slot.recoveryCandidateReplace = true
		}
		if running > math.MaxUint64-allocation {
			return process.ErrHashBuildBudgetInvalid
		}
		if candidate := running + allocation; candidate > target {
			target = candidate
		}
		if running < slot.admittedPeak ||
			running-slot.admittedPeak > math.MaxUint64-nextRetained {
			return process.ErrHashBuildBudgetInvalid
		}
		running = running - slot.admittedPeak + nextRetained
		if slot.recoveryCandidateReplace && peak > replacementOverlap {
			replacementOverlap = peak
		}
	}
	if running > math.MaxUint64-replacementOverlap {
		return process.ErrHashBuildBudgetInvalid
	}
	if repeated := running + replacementOverlap; repeated > target {
		target = repeated
	}

	reserved := l.Reserved()
	if target > reserved {
		growth := target - reserved
		if l.recoveryReservation == nil {
			reservation, err := l.budget.Reserve(growth)
			if err != nil {
				return err
			}
			l.recoveryReservation = reservation
		} else if err := l.recoveryReservation.Grow(growth); err != nil {
			return err
		}
	}

	for i := range l.slots {
		l.slots[i].recoveryPeak = l.slots[i].recoveryCandidate
		l.slots[i].recoveryMayReplace = l.slots[i].recoveryCandidateReplace
	}
	l.recoveryRows = rows
	l.recoveryReady = true
	return nil
}

func (l *ExpressionMemoryLease) reconcileRecoveryAfterRun() error {
	if l == nil || l.recoveryReservation == nil {
		return nil
	}
	var steady uint64
	var replacementOverlap uint64
	for i := range l.slots {
		slot := &l.slots[i]
		retained := slot.admittedPeak
		if slot.recoveryPeak > retained {
			retained = slot.recoveryPeak
		}
		if steady > math.MaxUint64-retained {
			return process.ErrHashBuildBudgetInvalid
		}
		steady += retained
		if slot.recoveryMayReplace && slot.recoveryPeak > replacementOverlap {
			replacementOverlap = slot.recoveryPeak
		}
	}
	if steady > math.MaxUint64-replacementOverlap {
		return process.ErrHashBuildBudgetInvalid
	}
	steady += replacementOverlap
	reserved := l.Reserved()
	if reserved <= steady {
		return nil
	}
	release := reserved - steady
	recoverySize := l.recoveryReservation.Size()
	if release > recoverySize {
		release = recoverySize
	}
	_, err := l.recoveryReservation.ReconcileDown(recoverySize - release)
	return err
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
	if l.budget != nil && l.recoveryReady && rows <= l.recoveryRows {
		for i, expr := range l.exprs {
			peak := l.slots[i].recoveryPeak
			if rows < l.recoveryRows {
				var peakErr error
				peak, peakErr = expressionVectorPeak(proc, expr, rows, l.duplicate)
				if peakErr != nil {
					return peakErr
				}
			}
			evalErr := fn(i)
			if l.released {
				return evalErr
			}
			if peak > l.slots[i].admittedPeak {
				l.slots[i].admittedPeak = peak
			}
			if evalErr == nil && !l.slots[i].mayReplaceWithinBound && rows == l.recoveryRows &&
				l.slots[i].recoveryMayReplace {
				l.slots[i].recoveryMayReplace = false
				l.recoveryReconcile = true
			}
			if evalErr != nil {
				return evalErr
			}
		}
		if l.recoveryReconcile {
			// Return only replacement headroom whose fixed-width root has reached
			// the prepared row high water. Keep every retained capacity, every root
			// still growing toward that high water, and variable replacement owned.
			if err := l.reconcileRecoveryAfterRun(); err != nil {
				return err
			}
			l.recoveryReconcile = false
		}
		return nil
	}
	if l.recoveryReady && rows > l.recoveryRows {
		// A larger unprepared evaluation can change the retained-capacity
		// state. Keep the standby token owned, but require the next recovery
		// preflight to recompute its guarantee from that new state.
		l.recoveryReady = false
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
		if peak <= slot.admittedPeak && !slot.mayReplaceWithinBound {
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
	if l.recoveryReservation != nil {
		size := l.recoveryReservation.Size()
		if total > math.MaxUint64-size {
			return math.MaxUint64
		}
		total += size
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
		l.slots[i].recoveryPeak = 0
		l.slots[i].recoveryMayReplace = false
		l.slots[i].recoveryCandidate = 0
		l.slots[i].recoveryCandidateReplace = false
	}
	if l.recoveryReservation != nil {
		l.recoveryReservation.Release()
		l.recoveryReservation = nil
	}
	l.recoveryRows = 0
	l.recoveryReady = false
	l.recoveryReconcile = false
	l.slots = nil
	l.exprs = nil
	l.executors = nil
	l.budget = nil
}
