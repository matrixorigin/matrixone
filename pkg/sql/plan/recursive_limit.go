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

package plan

import (
	"context"
	"math"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// recursiveCTEPrefixLimit bounds generation without removing rows needed by
// recursive feedback. OFFSET is applied only by the final result consumer.
func recursiveCTEPrefixLimit(ctx context.Context, limit, offset *planpb.Expr) (*planpb.Expr, error) {
	if limit == nil || offset == nil {
		return limit, nil
	}
	remaining, err := BindFuncExprImplByPlanExpr(ctx, "-", []*planpb.Expr{
		MakePlan2Uint64ConstExprWithType(math.MaxUint64), DeepCopyExpr(limit),
	})
	if err != nil {
		return nil, err
	}
	extra, err := BindFuncExprImplByPlanExpr(ctx, "least", []*planpb.Expr{DeepCopyExpr(offset), remaining})
	if err != nil {
		return nil, err
	}
	prefix, err := BindFuncExprImplByPlanExpr(ctx, "+", []*planpb.Expr{DeepCopyExpr(limit), extra})
	if err != nil {
		return nil, err
	}
	zero := MakePlan2Uint64ConstExprWithType(0)
	isZero, err := BindFuncExprImplByPlanExpr(ctx, "=", []*planpb.Expr{DeepCopyExpr(limit), zero})
	if err != nil {
		return nil, err
	}
	return BindFuncExprImplByPlanExpr(ctx, "case", []*planpb.Expr{isZero, DeepCopyExpr(zero), prefix})
}
