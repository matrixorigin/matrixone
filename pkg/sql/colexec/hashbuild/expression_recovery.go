// Copyright 2026 Matrix Origin
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

package hashbuild

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// expressionRecoveryBytes bounds all retained expression roots plus one
// allocate-copy-free replacement. Roots run sequentially, so one largest-root
// overlap is sufficient even when several roots retain reusable vectors.
func expressionRecoveryBytes(
	proc *process.Process,
	exprs []*plan.Expr,
	rows int,
	duplicate bool,
) (uint64, error) {
	if rows < 0 || len(exprs) == 0 {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	var total, replacement uint64
	for _, expr := range exprs {
		peak, err := expressionVectorPeak(proc, expr, rows, duplicate)
		if err != nil {
			return 0, err
		}
		total, err = recoveryCheckedAdd(total, peak)
		if err != nil {
			return 0, err
		}
		if peak > replacement {
			replacement = peak
		}
	}
	return recoveryCheckedAdd(total, replacement)
}

// expressionVectorPeak is an execution-before-allocation upper bound for one
// expression executor tree based on the declared SQL result types.
func expressionVectorPeak(
	proc *process.Process,
	expr *plan.Expr,
	rows int,
	duplicate bool,
) (uint64, error) {
	if expr == nil || rows < 0 {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	total, root, err := expressionTreePeakWithSelection(
		proc, expr, uint64(rows), false)
	if err != nil {
		return 0, err
	}
	if duplicate {
		return recoveryCheckedAdd(total, root)
	}
	return total, nil
}

func expressionTreePeakWithSelection(
	proc *process.Process,
	expr *plan.Expr,
	rows uint64,
	mayReceivePartialSelection bool,
) (total uint64, output uint64, err error) {
	if expr == nil {
		return 0, 0, process.ErrHashBuildBudgetInvalid
	}
	switch node := expr.Expr.(type) {
	case *plan.Expr_Col:
		return 0, 0, nil
	case *plan.Expr_F:
		if node.F == nil {
			return 0, 0, process.ErrHashBuildBudgetInvalid
		}
		fid := int32(-1)
		if node.F.Func != nil {
			fid, _ = function.DecodeOverloadID(node.F.Func.Obj)
		}
		for index, arg := range node.F.Args {
			child, _, childErr := expressionTreePeakWithSelection(
				proc,
				arg,
				rows,
				expressionChildMayReceivePartialSelection(
					fid, index, mayReceivePartialSelection),
			)
			if childErr != nil {
				return 0, 0, childErr
			}
			total, err = recoveryCheckedAdd(total, child)
			if err != nil {
				return 0, 0, err
			}
		}
	case *plan.Expr_P:
		if node.P == nil || proc == nil || proc.GetPrepareParams() == nil {
			return 0, 0, process.ErrHashBuildBudgetInvalid
		}
		paramPeak, paramErr := expressionParamPeak(proc, node.P.Pos)
		if paramErr != nil {
			return 0, 0, paramErr
		}
		typePeak, typeErr := expressionTypePeak(expr.Typ, 1)
		if typeErr != nil {
			return 0, 0, typeErr
		}
		output = max(paramPeak, typePeak)
		return output, output, nil
	case *plan.Expr_Lit, *plan.Expr_V, *plan.Expr_Raw, *plan.Expr_Vec,
		*plan.Expr_Fold, *plan.Expr_T:
		// Leaf executors may materialize their declared output below.
	default:
		return 0, 0, process.ErrHashBuildBudgetInvalid
	}

	output, err = expressionResultPeak(expr, rows)
	if err != nil {
		return 0, 0, err
	}
	total, err = recoveryCheckedAdd(total, output)
	if err != nil {
		return 0, 0, err
	}
	private, err := expressionFunctionPrivatePeak(expr)
	if err != nil {
		return 0, 0, err
	}
	total, err = recoveryCheckedAdd(total, private)
	if err != nil {
		return 0, 0, err
	}

	if _, isFunction := expr.Expr.(*plan.Expr_F); mayReceivePartialSelection && isFunction {
		total, err = recoveryCheckedAdd(total, output)
		if err != nil {
			return 0, 0, err
		}
		for _, arg := range nodeFunctionArgs(expr) {
			switch arg.Expr.(type) {
			case *plan.Expr_Col, *plan.Expr_F:
				selected, selectedErr := expressionTypePeak(arg.Typ, rows)
				if selectedErr != nil {
					return 0, 0, selectedErr
				}
				total, err = recoveryCheckedAdd(total, selected)
				if err != nil {
					return 0, 0, err
				}
			}
		}
	}
	return total, output, nil
}

func expressionResultPeak(expr *plan.Expr, rows uint64) (uint64, error) {
	fn, ok := expr.Expr.(*plan.Expr_F)
	if !ok || fn.F == nil || fn.F.Func == nil {
		return expressionTypePeak(expr.Typ, rows)
	}
	fid, _ := function.DecodeOverloadID(fn.F.Func.Obj)
	if fid != function.SERIAL && fid != function.SERIAL_FULL {
		return expressionTypePeak(expr.Typ, rows)
	}
	payload, _, supported, err := serialExpressionPackerBounds(fn.F)
	if err != nil {
		return 0, err
	}
	if !supported {
		return expressionTypePeak(expr.Typ, rows)
	}
	return expressionVarlenaWidthPeak(payload, rows)
}

func expressionFunctionPrivatePeak(expr *plan.Expr) (uint64, error) {
	fn, ok := expr.Expr.(*plan.Expr_F)
	if !ok || fn.F == nil || fn.F.Func == nil {
		return 0, nil
	}
	fid, _ := function.DecodeOverloadID(fn.F.Func.Obj)
	if fid != function.SERIAL && fid != function.SERIAL_FULL {
		return 0, nil
	}
	payload, maxAppend, supported, err := serialExpressionPackerBounds(fn.F)
	if err != nil {
		return 0, err
	}
	if !supported {
		return types.DefaultPackerCapacity(), nil
	}
	capacity, ok := types.PackerCapacityUpperBound(payload, maxAppend)
	if !ok {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	return capacity, nil
}

func serialExpressionPackerBounds(fn *plan.Function) (
	payload uint64,
	maxAppend uint64,
	supported bool,
	err error,
) {
	if fn == nil {
		return 0, 0, false, process.ErrHashBuildBudgetInvalid
	}
	for _, arg := range fn.Args {
		if arg == nil {
			return 0, 0, false, process.ErrHashBuildBudgetInvalid
		}
		component, ok := function.SerialEncodedTypeSizeBound(types.New(
			types.T(arg.Typ.Id), arg.Typ.Width, arg.Typ.Scale,
		))
		if !ok {
			return 0, 0, false, nil
		}
		payload, err = recoveryCheckedAdd(payload, component)
		if err != nil {
			return 0, 0, false, err
		}
		maxAppend = max(maxAppend, component)
	}
	return payload, maxAppend, true, nil
}

func nodeFunctionArgs(expr *plan.Expr) []*plan.Expr {
	if node, ok := expr.Expr.(*plan.Expr_F); ok && node.F != nil {
		return node.F.Args
	}
	return nil
}

func expressionChildMayReceivePartialSelection(
	fid int32,
	argument int,
	parentMayReceivePartialSelection bool,
) bool {
	switch fid {
	case function.IFF, function.CASE, function.COALESCE:
		return parentMayReceivePartialSelection || argument > 0
	default:
		return parentMayReceivePartialSelection
	}
}

func expressionParamPeak(proc *process.Process, pos int32) (uint64, error) {
	value, err := proc.GetPrepareParamsAt(int(pos))
	if err != nil {
		return 0, err
	}
	if value == nil {
		return 0, nil
	}
	header, ok := mpool.GrowCapacity(0, int64(types.VarlenaSize))
	if !ok || header < 0 {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	peak := uint64(header)
	if len(value) <= types.VarlenaInlineSize {
		return peak, nil
	}
	area, ok := mpool.GrowCapacity(0, int64(len(value)))
	if !ok || area < 0 {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	return recoveryCheckedAdd(peak, uint64(area))
}

func expressionTypePeak(typ plan.Type, rows uint64) (uint64, error) {
	oid := types.T(typ.Id)
	width := int64(oid.FixedLength())
	if width >= 0 {
		return expressionFixedWidthPeak(uint64(max(width, 1)), rows)
	}
	width = int64(typ.Width)
	hardMax := int64(types.MaxVarcharLen)
	if oid.IsArrayRelate() {
		elementWidth := int64(oid.ToType().GetArrayElementSize())
		width *= elementWidth
		hardMax = int64(types.MaxArrayDimension) * elementWidth
	} else {
		switch oid {
		case types.T_blob, types.T_text, types.T_json, types.T_datalink,
			types.T_geometry, types.T_geometry32:
			hardMax = int64(types.MaxBlobLen)
		}
	}
	if width > hardMax {
		hardMax = width
	}
	return expressionVarlenaWidthPeak(uint64(max(hardMax, 1)), rows)
}

const (
	expressionPerRowAllowance = uint64(32)
	expressionAllocationSlack = uint64(64 << 10)
)

func expressionAllocationCapacityUpperBound(required uint64) (uint64, error) {
	if required == 0 {
		return 0, nil
	}
	if mpool.CapLimit <= 0 {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	limit := uint64(mpool.CapLimit)
	if required >= limit {
		return limit, nil
	}
	capacity, ok := mpool.GrowCapacity(int64(required-1), int64(required))
	if !ok || capacity < 0 {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	return uint64(capacity), nil
}

func expressionFixedWidthPeak(width, rows uint64) (uint64, error) {
	data, err := recoveryCheckedMul(max(width, 1), rows)
	if err != nil {
		return 0, err
	}
	data, err = expressionAllocationCapacityUpperBound(data)
	if err != nil {
		return 0, err
	}
	allowance, err := recoveryCheckedMul(rows, expressionPerRowAllowance)
	if err != nil {
		return 0, err
	}
	total, err := recoveryCheckedAdd(data, allowance)
	if err != nil {
		return 0, err
	}
	return recoveryCheckedAdd(total, expressionAllocationSlack)
}

func expressionVarlenaWidthPeak(width, rows uint64) (uint64, error) {
	descriptors, err := recoveryCheckedMul(rows, uint64(types.VarlenaSize))
	if err != nil {
		return 0, err
	}
	descriptors, err = expressionAllocationCapacityUpperBound(descriptors)
	if err != nil {
		return 0, err
	}
	var area uint64
	if width > uint64(types.VarlenaInlineSize) {
		area, err = recoveryCheckedMul(width, rows)
		if err != nil {
			return 0, err
		}
		area, err = expressionAllocationCapacityUpperBound(area)
		if err != nil {
			return 0, err
		}
	}
	metadata, err := recoveryCheckedMul(
		rows, expressionPerRowAllowance-uint64(types.VarlenaSize))
	if err != nil {
		return 0, err
	}
	total, err := recoveryCheckedAdd(descriptors, area)
	if err != nil {
		return 0, err
	}
	total, err = recoveryCheckedAdd(total, metadata)
	if err != nil {
		return 0, err
	}
	return recoveryCheckedAdd(total, expressionAllocationSlack)
}
