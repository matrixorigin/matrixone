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

package plan

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type temporalCostScalar struct {
	typ   types.Type
	value []byte
	null  bool
}

func spendTemporalCostWork(ctx context.Context, budget *int, work int) bool {
	if ctx == nil || budget == nil || work < 0 || *budget < work {
		if budget != nil {
			*budget = 0
		}
		return false
	}
	before := *budget
	*budget -= work
	if before>>6 != *budget>>6 && context.Cause(ctx) != nil {
		*budget = 0
		return false
	}
	return true
}

// Bound input payloads before invoking the scalar evaluator, which otherwise
// could allocate a large vector before an optional observation declines it.
func temporalCostLiteralFits(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	literal := expr.GetLit()
	if literal == nil || literal.Src != nil {
		return false
	}
	if types.T(expr.Typ.Id) == types.T_json || types.T(expr.Typ.Id).IsArrayRelate() {
		return false
	}
	return len(literal.GetSval()) <= 4096 && len(literal.GetJsonval()) <= 4096 && len(literal.GetVecVal()) <= 4096
}

// The caller proves eager evaluation safe before this bounded copy. Warnings
// decline only the optional cost observation; real resource/stop errors retain
// the planner's established error ownership.
func evaluateTemporalCostScalar(ctx context.Context, proc *process.Process, expr *plan.Expr, budget *int) (temporalCostScalar, bool, error) {
	if ctx == nil || proc == nil || expr == nil || !spendTemporalCostWork(ctx, budget, 1) {
		return temporalCostScalar{}, false, temporalCostStop(ctx)
	}
	if err := context.Cause(ctx); err != nil {
		return temporalCostScalar{}, false, err
	}
	if expr.GetLit() != nil && !temporalCostLiteralFits(expr) {
		return temporalCostScalar{}, false, nil
	}
	probe := proc.NewNoContextChildProc(0)
	probe.Ctx = ctx
	vec, free, warned, err := rule.EvaluateConstantExpression(probe, DeepCopyExpr(expr), batch.EmptyForConstFoldBatch)
	if free != nil {
		defer free()
	}
	if stop := context.Cause(ctx); stop != nil {
		return temporalCostScalar{}, false, stop
	}
	if err != nil {
		return temporalCostScalar{}, false, temporalCandidateCostEvaluationError(err)
	}
	if warned || vec == nil || vec.Length() != 1 {
		return temporalCostScalar{}, false, nil
	}
	scalar := temporalCostScalar{typ: *vec.GetType(), null: vec.IsConstNull() || vec.IsNull(0)}
	if scalar.null {
		return scalar, true, nil
	}
	value := vec.GetRawBytesAt(0)
	if len(value) > 4096 || !spendTemporalCostWork(ctx, budget, len(value)) {
		return temporalCostScalar{}, false, nil
	}
	scalar.value = append([]byte(nil), value...)
	return scalar, true, nil
}

func temporalCostStop(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return context.Cause(ctx)
}

func temporalCandidateCostEvaluationError(err error) error {
	if isStatementConversionError(err) {
		return nil
	}
	return err
}
