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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// buildSetOperationPhysicalKeys refines membership without changing the visible
// row or the shared GROUP BY/DISTINCT equality domain. Both initial planning and
// prepared-plan specialization must derive keys from the resolved output types.
func buildSetOperationPhysicalKeys(ctx context.Context, exprs []*planpb.Expr, padSpace []bool) ([]*planpb.Expr, error) {
	if len(exprs) != len(padSpace) {
		return nil, moerr.NewInternalError(ctx, "physical equality key shape does not match projected row")
	}
	required := false
	for i, expr := range exprs {
		if expr == nil {
			return nil, moerr.NewInternalErrorf(ctx, "set-operation output has a nil projection at column %d", i)
		}
		oid := types.T(expr.Typ.Id)
		required = required || padSpace[i] || oid == types.T_float32 || oid == types.T_float64
	}
	if !required {
		return nil, nil
	}
	keys := make([]*planpb.Expr, len(exprs))
	for i, expr := range exprs {
		key := DeepCopyExpr(expr)
		var err error
		switch types.T(expr.Typ.Id) {
		case types.T_float32, types.T_float64:
			key, err = BindFuncExprImplByPlanExpr(ctx, "set_operation_float_key", []*planpb.Expr{key})
		default:
			if padSpace[i] {
				key, err = appendSetOperationCastBeforeExpr(ctx, key, key.Typ)
			}
		}
		if err != nil {
			return nil, err
		}
		keys[i] = key
	}
	return keys, nil
}
