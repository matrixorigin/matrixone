// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"
	"reflect"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// Refresh the typed edges in producer order: caller captures, callee slots and
// result, then the caller's comparison. This runs on the execute-time plan copy;
// neither the prepared template nor another execution owns these mutations.
func refreshPreparedCorrelationExpr(
	ctx context.Context, query *plan.Query, expr *plan.Expr,
	refreshCaller func(*plan.Expr) (bool, error), refreshNode func(int32) error,
	rule *ResetParamRefRule,
) (bool, error) {
	spec := query.Correlation
	programID := expr.GetSlot().GetProgramId()
	if call := expr.GetDependent(); call != nil {
		programID = call.ProgramId
	}
	if spec == nil || programID == 0 || int(programID) > len(spec.Programs) {
		return false, moerr.NewInternalError(ctx, "invalid prepared correlation program")
	}
	program := spec.Programs[programID-1]
	if slot := expr.GetSlot(); slot != nil {
		if int(slot.Ordinal) >= len(program.InputTypes) {
			return false, moerr.NewInternalError(ctx, "invalid prepared correlation slot")
		}
		typ := program.InputTypes[slot.Ordinal]
		changed := !reflect.DeepEqual(expr.Typ, typ)
		expr.Typ = *DeepCopyType(&typ)
		return changed, nil
	}
	call := expr.GetDependent()
	if len(call.Inputs) != len(program.InputTypes) {
		return false, moerr.NewInternalError(ctx, "invalid prepared correlation inputs")
	}
	changed := false
	for _, operands := range [][]*plan.Expr{call.Inputs, call.Lhs} {
		for _, operand := range operands {
			updated, err := refreshCaller(operand)
			if err != nil {
				return false, err
			}
			changed = changed || updated
		}
	}
	for i, input := range call.Inputs {
		changed = changed || !reflect.DeepEqual(program.InputTypes[i], input.Typ)
		program.InputTypes[i] = *DeepCopyType(&input.Typ)
	}
	if err := refreshNode(program.RootNode); err != nil {
		return false, err
	}
	outputs := query.Nodes[program.RootNode].ProjectList
	if len(outputs) != len(program.OutputTypes) {
		return false, moerr.NewInternalError(ctx, "invalid prepared correlation outputs")
	}
	for i, output := range outputs {
		changed = changed || !reflect.DeepEqual(program.OutputTypes[i], output.Typ)
		program.OutputTypes[i] = *DeepCopyType(&output.Typ)
	}
	if call.Comparison != nil {
		resolve := func(col *plan.ColRef, _ plan.Type) (plan.Type, bool) {
			if col.ColPos < 0 {
				return plan.Type{}, false
			}
			if col.RelPos == 0 && int(col.ColPos) < len(call.Lhs) {
				return call.Lhs[col.ColPos].Typ, true
			}
			if col.RelPos == 1 && int(col.ColPos) < len(program.OutputTypes) {
				return program.OutputTypes[col.ColPos], true
			}
			return plan.Type{}, false
		}
		updated, err := refreshPreparedPlanProjectionExprType(ctx, call.Comparison, resolve,
			func(*plan.ColRef) (int32, bool, bool) { return 0, false, false }, rule)
		if err != nil {
			return false, err
		}
		changed = changed || updated
	} else if call.Reducer == plan.SubqueryRef_SCALAR {
		if len(program.OutputTypes) != 1 {
			return false, moerr.NewInternalError(ctx, "invalid prepared scalar outputs")
		}
		typ := *DeepCopyType(&program.OutputTypes[0])
		typ.NotNullable = false
		changed = changed || !reflect.DeepEqual(expr.Typ, typ)
		expr.Typ = typ
	}
	return changed, nil
}
