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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// UserVarAssignmentExpressionExecutor evaluates := one logical row at a time.
// The row-wise loop is intentional: a later projection (or a later row in a
// self-referential assignment) must see the values staged by earlier rows.
// Publication is owned by the frontend statement boundary, not by this
// executor.
type UserVarAssignmentExpressionExecutor struct {
	mp         *mpool.MPool
	allocation *vector.AllocationAccountSelection
	child      ExpressionExecutor
	name       string
	typ        types.Type
	result     *vector.Vector
	maskedNull *vector.Vector
}

func (expr *UserVarAssignmentExpressionExecutor) Eval(
	proc *process.Process, batches []*batch.Batch, selectList []bool,
) (*vector.Vector, error) {
	if proc == nil || proc.Base == nil || !proc.Base.IsFrontend {
		return nil, moerr.NewNotSupported(context.TODO(),
			"user-variable assignment is supported only in frontend SELECT projections")
	}
	rowCount := expressionRowCount(batches)
	previousRow := proc.GetUserVariableAssignmentCurrentRow()
	nested := previousRow >= 0
	if noRowsSelected(selectList, rowCount) {
		if nested {
			// A skipped CASE/IF arm still represents the value visible before
			// this row. Walk the complete assignment subtree so nested targets
			// are also recorded; evaluating the subtree would execute side
			// effects, especially when a wrapper function surrounds it.
			if err := snapshotSkippedUserVariableAssignments(
				proc, expr, rowCount, selectList); err != nil {
				return nil, err
			}
		}
		if expr.maskedNull == nil {
			var err error
			expr.maskedNull, err = newExpressionConstNull(expr.typ, 1, expr.allocation)
			if err != nil {
				return nil, err
			}
		}
		expr.maskedNull.SetLength(rowCount)
		return expr.maskedNull, nil
	}
	if !allRowsSelected(selectList, rowCount) {
		return nil, moerr.NewNotSupported(proc.Ctx,
			"user-variable assignment does not support a filtered projection")
	}

	if expr.result != nil {
		expr.result.CleanOnlyData()
		expr.result.SetLength(0)
	} else {
		var err error
		expr.result, err = newExpressionOffHeapVector(expr.typ, expr.allocation)
		if err != nil {
			return nil, err
		}
	}
	proc.BeginUserVariableAssignment(expr.name, rowCount)
	defer proc.SetUserVariableAssignmentCurrentRow(previousRow)

	for row := 0; row < rowCount; row++ {
		if proc.Ctx != nil {
			if err := proc.Ctx.Err(); err != nil {
				return nil, err
			}
		}
		logicalRow := row
		if nested {
			// The parent assignment has already selected the original input
			// row. The nested executor sees a one-row scratch batch, but its
			// staged value belongs in the parent's statement-sized row slot.
			logicalRow = previousRow
		}
		proc.SetUserVariableAssignmentCurrentRow(logicalRow)
		value, err := expr.evalAssignmentRow(proc, batches, row)
		if err != nil {
			return nil, err
		}
		proc.StageUserVariableAssignment(value, logicalRow)
	}
	return expr.result, nil
}

func (expr *UserVarAssignmentExpressionExecutor) evalAssignmentRow(
	proc *process.Process, batches []*batch.Batch, row int,
) (process.UserVariableAssignment, error) {
	rowBatches, err := cloneUserVariableAssignmentRowBatches(
		batches, row, proc.Mp(), expr.allocation)
	if err != nil {
		return process.UserVariableAssignment{}, err
	}
	releaseRowBatches := func() {
		for i := range rowBatches {
			rowBatch := rowBatches[i]
			rowBatches[i] = nil
			if rowBatch != nil {
				rowBatch.Clean(proc.Mp())
			}
		}
	}
	defer releaseRowBatches()

	childVec, err := expr.child.Eval(proc, rowBatches, nil)
	if err != nil {
		return process.UserVariableAssignment{}, err
	}
	if childVec == nil || childVec.Length() == 0 {
		return process.UserVariableAssignment{}, moerr.NewInternalError(proc.Ctx,
			"user-variable assignment expression returned an invalid row")
	}
	if err = expr.result.UnionOne(childVec, 0, proc.Mp()); err != nil {
		return process.UserVariableAssignment{}, err
	}
	value := process.UserVariableAssignment{
		Name:         expr.name,
		Type:         childVec.GetType().PlanType(),
		IsBin:        childVec.GetIsBinaryStringAt(0),
		StringDomain: childVec.GetRuntimeStringDomainAt(0),
	}
	if childVec.IsNull(0) {
		value.Value = nil
	} else {
		value.Value = userVariableAssignmentValue(childVec, 0)
	}
	releaseRowBatches()
	return value, nil
}

// userVariableAssignmentValue converts the physical vector representation to
// the statement-overlay representation. Decimals are stored as scaled
// coefficients in vectors, while JSON vectors contain encoded bytejson; both
// must be converted to their textual form before the generic variable reader
// reconstructs a typed vector.
func userVariableAssignmentValue(vec *vector.Vector, row int) any {
	switch vec.GetType().Oid {
	case types.T_decimal64:
		return vector.GetFixedAtNoTypeCheck[types.Decimal64](vec, row).Format(vec.GetType().Scale)
	case types.T_decimal128:
		return vector.GetFixedAtNoTypeCheck[types.Decimal128](vec, row).Format(vec.GetType().Scale)
	case types.T_decimal256:
		return vector.GetFixedAtNoTypeCheck[types.Decimal256](vec, row).Format(vec.GetType().Scale)
	case types.T_json:
		return types.DecodeJson(vec.GetBytesAt(row)).String()
	default:
		return vector.GetAny(vec, row, true)
	}
}

func (expr *UserVarAssignmentExpressionExecutor) currentUserVariableAssignment(
	proc *process.Process,
) (process.UserVariableAssignment, error) {
	if value, ok := proc.ResolveUserVariableAssignment(expr.name, -1); ok {
		return value, nil
	}
	value := process.UserVariableAssignment{
		Name: expr.name,
		Type: expr.typ.PlanType(),
	}
	if resolve := proc.GetResolveVariableFunc(); resolve != nil {
		resolved, err := resolve(expr.name, false, false)
		if err != nil {
			return process.UserVariableAssignment{}, err
		}
		value.Value = resolved
	}
	if resolve := proc.GetResolveVariableIsBinFunc(); resolve != nil {
		isBin, err := resolve(expr.name, false, false)
		if err != nil {
			return process.UserVariableAssignment{}, err
		}
		value.IsBin = isBin
	}
	if resolve := proc.GetResolveVariableStringDomainFunc(); resolve != nil {
		domain, err := resolve(expr.name, false, false)
		if err != nil {
			return process.UserVariableAssignment{}, err
		}
		value.StringDomain = domain
	}
	return value, nil
}

// cloneUserVariableAssignmentRowBatches gives the child executor a genuine
// one-row input. Calling an ordinary vector executor with the original batch
// for every logical row would evaluate the whole RHS batch repeatedly (and
// would repeat volatile functions or sequence allocation). A copied row also
// makes the row number seen by column executors unambiguous while the process
// overlay keeps the original logical row for user-variable reads.
func cloneUserVariableAssignmentRowBatches(
	batches []*batch.Batch,
	row int,
	mp *mpool.MPool,
	allocation *vector.AllocationAccountSelection,
) ([]*batch.Batch, error) {
	if row < 0 {
		return nil, moerr.NewInvalidInputNoCtx("invalid user-variable assignment row")
	}
	rowBatches := make([]*batch.Batch, len(batches))
	for i, source := range batches {
		if source == nil || row >= source.RowCount() {
			for _, cloned := range rowBatches {
				if cloned != nil {
					cloned.Clean(mp)
				}
			}
			return nil, moerr.NewInternalErrorNoCtx(
				"user-variable assignment input row is out of range")
		}
		cloned := batch.NewWithSize(len(source.Vecs))
		cloned.Attrs = source.Attrs
		cloned.ShuffleIDX = source.ShuffleIDX
		for col, sourceVec := range source.Vecs {
			if sourceVec == nil {
				continue
			}
			vec, err := sourceVec.CloneWindowWithAllocation(row, row+1, mp, allocation)
			if err != nil {
				cloned.Clean(mp)
				for _, prior := range rowBatches {
					if prior != nil {
						prior.Clean(mp)
					}
				}
				return nil, err
			}
			cloned.Vecs[col] = vec
		}
		cloned.SetRowCount(1)
		rowBatches[i] = cloned
	}
	return rowBatches, nil
}

func (expr *UserVarAssignmentExpressionExecutor) EvalWithoutResultReusing(
	proc *process.Process, batches []*batch.Batch, selectList []bool,
) (*vector.Vector, error) {
	vec, err := expr.Eval(proc, batches, selectList)
	if err == nil && vec == expr.result {
		expr.result = nil
	}
	if err == nil && vec == expr.maskedNull {
		expr.maskedNull = nil
	}
	if err != nil && expr.maskedNull != nil {
		expr.maskedNull.Free(expr.mp)
		expr.maskedNull = nil
	}
	return vec, err
}

func (expr *UserVarAssignmentExpressionExecutor) ResetForNextQuery() {
	if expr.child != nil {
		expr.child.ResetForNextQuery()
	}
	if expr.result != nil {
		expr.result.Free(expr.mp)
		expr.result = nil
	}
	if expr.maskedNull != nil {
		expr.maskedNull.Free(expr.mp)
		expr.maskedNull = nil
	}
}

func (expr *UserVarAssignmentExpressionExecutor) Free() {
	if expr == nil {
		return
	}
	if expr.child != nil {
		expr.child.Free()
		expr.child = nil
	}
	if expr.result != nil {
		expr.result.Free(expr.mp)
		expr.result = nil
	}
	if expr.maskedNull != nil {
		expr.maskedNull.Free(expr.mp)
		expr.maskedNull = nil
	}
}

func (expr *UserVarAssignmentExpressionExecutor) IsColumnExpr() bool { return false }

func (expr *UserVarAssignmentExpressionExecutor) TypeName() string {
	return "user variable assignment " + strings.ToLower(expr.name)
}

func allRowsSelected(selectList []bool, rowCount int) bool {
	if len(selectList) == 0 {
		return true
	}
	if len(selectList) < rowCount {
		return false
	}
	for i := 0; i < rowCount; i++ {
		if !selectList[i] {
			return false
		}
	}
	return true
}

// evalStagedUserVariableValue materializes a scalar overlay value using the
// same conversion path as ordinary user-variable reads.
func evalStagedUserVariableValue(
	proc *process.Process,
	typ types.Type,
	value process.UserVariableAssignment,
	allocation *vector.AllocationAccountSelection,
	rowCount int,
) (*vector.Vector, error) {
	vec, err := util.GenVectorByVarValueWithAllocation(proc, typ, value.Value, allocation)
	if err != nil {
		return nil, err
	}
	vec.SetLength(rowCount)
	vec.SetIsBin(value.IsBin)
	if err = vec.SetRuntimeStringDomainWithMP(value.StringDomain, proc.Mp()); err != nil {
		vec.Free(proc.Mp())
		return nil, err
	}
	if err = vec.SetStringSource(types.StringSourceUserVariable); err != nil {
		vec.Free(proc.Mp())
		return nil, err
	}
	return vec, nil
}

func evalStagedUserVariableRows(
	proc *process.Process,
	typ types.Type,
	rows []process.UserVariableAssignment,
	allocation *vector.AllocationAccountSelection,
) (*vector.Vector, error) {
	result, err := newExpressionOffHeapVector(typ, allocation)
	if err != nil {
		return nil, err
	}
	for _, value := range rows {
		one, oneErr := evalStagedUserVariableValue(proc, typ, value, allocation, 1)
		if oneErr != nil {
			result.Free(proc.Mp())
			return nil, oneErr
		}
		oneErr = result.UnionOne(one, 0, proc.Mp())
		one.Free(proc.Mp())
		if oneErr != nil {
			result.Free(proc.Mp())
			return nil, oneErr
		}
	}
	return result, nil
}
